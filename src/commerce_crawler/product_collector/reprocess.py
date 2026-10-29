"""Repair extraction from saved HTML without issuing any HTTP request.

Records are re-extracted one at a time into staged files, so a whole run never sits in memory."""
from __future__ import annotations

from collections.abc import Sequence
import copy
import json
import re
import shutil
import tempfile
import weakref
from pathlib import Path
from uuid import uuid4

from .core import FieldCoverage, payload_hash, raw_projection, run_result, utc_now
from .danawa import DanawaAdapter
from . import quality
from .storage import load_detail_html, save_detail_html


class StagedRecords(Sequence):
    """Rows of a staged jsonl file, read back one at a time. save_run streams them to the run.

    projection: the small per-product values the report was computed from.
    The staging directory is removed when the last sequence that uses it is gone."""

    def __init__(self, path: Path, projection: list, owner):
        self.path, self.projection, self._owner = path, projection, owner
        self._offsets = None

    def _positions(self):
        if self._offsets is None:
            self._offsets, offset = [], 0
            with self.path.open("rb") as stream:
                for line in stream:
                    if line.strip():
                        self._offsets.append(offset)
                    offset += len(line)
        return self._offsets

    def __len__(self):
        return len(self.projection)

    def __getitem__(self, index):
        if isinstance(index, slice):
            return [self[i] for i in range(*index.indices(len(self)))]
        offset = self._positions()[index]
        with self.path.open("rb") as stream:
            stream.seek(offset)
            return json.loads(stream.readline().decode("utf-8"))

    def __iter__(self):
        with self.path.open(encoding="utf-8") as stream:
            for line in stream:
                if line.strip():
                    yield json.loads(line)


class _Staging:
    def __init__(self, parent: Path | None, name: str):
        if parent is not None:
            self.directory = Path(parent) / f".staging-{name}"
            self.directory.mkdir(parents=True)
        else:
            self.directory = Path(tempfile.mkdtemp(prefix="reprocess-"))
        self.cleanup = weakref.finalize(self, shutil.rmtree, str(self.directory), True)


def saved_records(path: Path):
    """Rows of a saved jsonl file, one at a time."""
    with Path(path).open(encoding="utf-8-sig") as stream:
        for line in stream:
            if line.strip():
                yield json.loads(line)


def apply_detail(record, adapter, html, collected_at=None):
    """Shared detail identity check and extraction for saved runs.

    Another product's page keeps only its reference, as in a live collection."""
    listing = record["payload"]["listings"][0]
    detail = {"url": listing["product_url"], "collected_at": collected_at or utc_now()}
    error = adapter.identity_error(html, listing)
    if error:
        record["payload"]["detail"] = {**detail, "status": "identity_mismatch"}
        record["detail_status"], record["detail_error"] = "identity_mismatch", error
        return
    parsed = adapter.parse_detail(html, listing)
    record["payload"]["detail"] = {**detail, **parsed}
    record["detail_status"], record["detail_error"] = parsed["status"], None


def reprocess_run(directory: Path, output_root: Path | None = None,
                  quality_mode: str = "warn") -> tuple[StagedRecords, StagedRecords, dict]:
    """output_root: where save_run will write this result. Pages embedded in a pre-0.4 payload are
    then kept in its details/, since the reprocessed payload no longer carries them.

    The results are staged files (beside output_root, else in the temp directory) read back on demand."""
    original = json.loads((directory / "run_report.json").read_text(encoding="utf-8"))
    adapter = DanawaAdapter()
    report = copy.deepcopy(original)
    report.update(job_id=str(uuid4()), original_run_id=original["job_id"], started_at=utc_now(),
                  execution_mode="offline_reprocess", original_request_count=original["request_count"],
                  original_request_log=original["requests"], request_count=0, requests=[])
    # The reprocessed run is saved elsewhere, so it points back to the evidence it used.
    evidence = [str(Path(directory).resolve())] + original.get("detail_evidence_dirs", [])
    target = Path(output_root) / report["job_id"] if output_root is not None else None
    report["detail_evidence_dirs"] = ([str(target.resolve())] if target else []) + evidence
    category_names = {}
    for category in report["categories"]:
        title = category.get("diagnostics", {}).get("page_title", "")
        name = re.fullmatch(r"(.+?)\s*:\s*다나와 가격비교", title or "")
        category["observed_category_name"] = name.group(1).strip() if name else None
        category_names[category["category_id"]] = category["observed_category_name"]
    def reprocessed(record):
        if record["source"] != adapter.source:
            raise ValueError("only Danawa saved runs can be reprocessed")
        record["job_id"] = report["job_id"]
        record["reprocessed_at"] = utc_now()
        for category in record["categories"]:
            category["observed_category_name"] = category_names.get(category["category_id"])
        for listing in record["payload"]["listings"]:
            if listing.get("format") == "action_product":
                rows = adapter.parse_api_products([listing["api_product"]], 1)
            else:
                rows, _ = adapter.parse_listing('<div id="productListArea">' + listing["html"] + '</div>', 1)
            if not rows or rows[0]["product_id"] != record["product_id"]:
                raise ValueError(f"saved listing identity mismatch: {record['product_id']}")
            listing.update(rows[0])
            listing["category"]["observed_category_name"] = category_names.get(listing["category"]["category_id"])
        detail = record["payload"]["detail"]
        if detail is not None:
            # Runs before 0.4 kept the page in the payload; later runs keep it in details/.
            at = detail.get("collected_at") or record["collected_at"]
            html = detail.get("html") or detail.get("unrecognized_html")
            if html and target is not None:
                save_detail_html(target, record["product_id"], detail["url"], html, at)
            if not html:
                saved = load_detail_html(evidence, str(record["product_id"]), detail["url"], at)
                html = saved and saved["html"]
            if not html:
                raise ValueError(f"saved full detail HTML is missing: {record['product_id']}")
            apply_detail(record, adapter, html, at)
            if (detail.get("html") or detail.get("unrecognized_html")) and target is None:
                # Nowhere to keep the embedded page: leave it in the payload so the result stays reprocessable.
                record["payload"]["detail"]["html"] = html
        record["payload_hash"] = payload_hash(record["payload"])
        record["schema_version"] = "1.2"
        record["extractor_version"] = "0.4"
        return record

    staging = _Staging(output_root, report["job_id"])
    raw_path, normalized_path = staging.directory / "product_raw.jsonl", staging.directory / "products.jsonl"
    coverage, raw_rows, quality_rows = FieldCoverage(), [], []
    try:
        with raw_path.open("w", encoding="utf-8", newline="\n") as raw_out, \
                normalized_path.open("w", encoding="utf-8", newline="\n") as normalized_out:
            for record in saved_records(directory / "product_raw.jsonl"):
                record = reprocessed(record)
                normalized = adapter.normalize(record)
                raw_out.write(json.dumps(record, ensure_ascii=False, allow_nan=False) + "\n")
                normalized_out.write(json.dumps(normalized, ensure_ascii=False, allow_nan=False) + "\n")
                coverage.add(normalized)
                raw_rows.append(raw_projection(record))
                quality_rows.append(quality.project(normalized))
    except BaseException:
        staging.cleanup()
        raise
    report.update(finished_at=utc_now(), product_count=len(raw_rows), field_coverage=coverage.result(),
                  result=run_result(report, raw_rows, any(c["status"] == "not_requested" for c in report["categories"]),
                                    include_reused=True))
    # Quality is judged again on the new extraction; the original run's verdict is not carried over.
    quality.apply(report, quality_rows, quality_mode)
    return StagedRecords(raw_path, raw_rows, staging), StagedRecords(normalized_path, quality_rows, staging), report
