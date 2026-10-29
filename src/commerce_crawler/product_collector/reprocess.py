"""Repair extraction from saved HTML without issuing any HTTP request."""
from __future__ import annotations

import copy
import json
import re
from pathlib import Path
from uuid import uuid4

from .core import field_coverage, payload_hash, run_result, utc_now
from .danawa import DanawaAdapter
from . import quality
from .storage import load_detail_html, read_jsonl, save_detail_html


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
                  quality_mode: str = "warn") -> tuple[list[dict], list[dict], dict]:
    """output_root: where save_run will write this result. Pages embedded in a pre-0.4 payload are
    then kept in its details/, since the reprocessed payload no longer carries them."""
    original = json.loads((directory / "run_report.json").read_text(encoding="utf-8"))
    raw = read_jsonl(directory / "product_raw.jsonl")
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
    for record in raw:
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
    normalized = [adapter.normalize(record) for record in raw]
    report.update(finished_at=utc_now(), product_count=len(raw), field_coverage=field_coverage(normalized),
                  result=run_result(report, raw, any(c["status"] == "not_requested" for c in report["categories"])))
    # Quality is judged again on the new extraction; the original run's verdict is not carried over.
    quality.apply(report, normalized, quality_mode)
    return raw, normalized, report
