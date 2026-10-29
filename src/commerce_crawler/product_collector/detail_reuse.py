"""Detail refresh "changed": reuse unchanged details of the previous completed run without a request.

Only the base run's product index is kept in memory; a reused record is read from its line on demand.
The reused page is hard-linked (or copied) into this run's details/, so every run keeps complete
reprocess evidence of its own and never depends on an older run that prune may remove."""
from __future__ import annotations

import copy
import hashlib
import json
from pathlib import Path

from .storage import atomic_json, detail_html_path, link_file, load_detail_html

REUSE_FILE = "detail_reuse.json"
MODES = ("all", "changed")


def _read(path: Path):
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None


def find_base(output_root: Path, current: Path, source: str) -> Path | None:
    """Newest completed run in the same output root that collected details of the same source."""
    current, found = Path(current).resolve(), []
    for directory in Path(output_root).iterdir():
        if not directory.is_dir() or directory.name.startswith(".") or directory.resolve() == current:
            continue
        state, config = _read(directory / "checkpoint_state.json"), _read(directory / "run_config.json")
        settings = config.get("config") if isinstance(config, dict) else None
        if (isinstance(state, dict) and state.get("status") == "completed" and isinstance(settings, dict)
                and settings.get("source") == source and settings.get("collect_details") is True
                and (directory / "product_raw.jsonl").is_file()):
            # updated_at has second precision; the state file's write time breaks a tie between quick runs.
            found.append((str(state.get("updated_at") or ""), (directory / "checkpoint_state.json").stat().st_mtime_ns,
                          directory))
    return max(found)[2] if found else None


class DetailReuse:
    def __init__(self, base: Path | None, job_id: str, sample_size: int):
        if type(sample_size) is not int or sample_size < 0:
            raise ValueError("refresh sample must be zero or a positive integer")
        self.base, self.job_id, self.sample_size = (Path(base) if base else None), job_id, sample_size
        self.index, self.base_job_id, self.evidence, self.base_missing = {}, None, [], None
        if self.base is None:
            return
        report = _read(self.base / "run_report.json") or {}
        self.base_job_id = report.get("job_id") or self.base.name
        self.evidence = [self.base] + [Path(d) for d in report.get("detail_evidence_dirs", []) if Path(d) != self.base]
        offset = 0
        with (self.base / "product_raw.jsonl").open("rb") as stream:
            for line in stream:
                if line.strip():
                    record = json.loads(line)
                    detail = record["payload"].get("detail") or {}
                    self.index[str(record["product_id"])] = {
                        "offset": offset, "status": record.get("detail_status"),
                        "fingerprint": record.get("listing_fingerprint"),
                        "extractor_version": record.get("extractor_version"),
                        "collected_at": detail.get("collected_at"), "gallery": len(detail.get("gallery_images") or [])}
                offset += len(line)

    @classmethod
    def open(cls, run_dir: Path, output_root: Path, source: str, job_id: str, sample_size: int) -> "DetailReuse":
        """The base is chosen once, on the first start, and read back on resume."""
        path = Path(run_dir) / REUSE_FILE
        saved = _read(path)
        if isinstance(saved, dict):
            base = Path(saved["base_dir"]) if saved.get("base_dir") else None
            if base is not None and not (base / "product_raw.jsonl").is_file():
                # Pruned while this run was interrupted. Details reused so far have their own evidence
                # here; the remaining products are simply requested.
                reuse = cls(None, job_id, sample_size)
                reuse.base_job_id, reuse.base_missing = saved.get("base_job_id"), str(base)
                return reuse
        else:
            base = find_base(output_root, run_dir, source)
            atomic_json(path, {"base_dir": str(base.resolve()) if base else None,
                               "base_job_id": (_read(base / "run_report.json") or {}).get("job_id") if base else None})
        return cls(base, job_id, sample_size)

    def sampled(self, product_id: str) -> bool:
        """Deterministic per run: a resumed run draws the same sample."""
        if not self.index:
            return False
        point = int(hashlib.sha256(f"{self.job_id}:{product_id}".encode()).hexdigest()[:12], 16) / 16 ** 12
        return point < self.sample_size / len(self.index)

    def reuse(self, record: dict, fingerprint: str | None, target: Path | None) -> dict | None:
        """The base detail when this product is unchanged and its saved page checks out, else None."""
        code = str(record["product_id"])
        entry = self.index.get(code)
        if (entry is None or fingerprint is None or entry["fingerprint"] != fingerprint
                or entry["status"] != "ok" or entry["gallery"] < 1 or not entry["collected_at"]
                or entry["extractor_version"] != record.get("extractor_version") or self.sampled(code)):
            return None
        with (self.base / "product_raw.jsonl").open("rb") as stream:
            stream.seek(entry["offset"])
            detail = json.loads(stream.readline())["payload"]["detail"]
        url, at = record["payload"]["listings"][0]["product_url"], entry["collected_at"]
        if detail.get("url") != url:
            return None
        # Only this response's gzip page counts; an older per-product details/<id>.json is no evidence of it.
        source = next((path for path in (detail_html_path(d, code, at) for d in self.evidence) if path.is_file()), None)
        if source is None:
            return None
        try:
            load_detail_html([source.parents[2]], code, url, at)
        except ValueError:
            return None  # damaged or foreign page: request this product again
        if target is not None:
            link_file(source, detail_html_path(target, code, at), hashlib.sha256(source.read_bytes()).hexdigest())
        return copy.deepcopy(detail)

    def summary(self, raw: list[dict], mode: str = "changed") -> dict:
        reused = sum(bool(r.get("detail_reused_from")) for r in raw)
        return {"mode": mode, "base_job_id": self.base_job_id, "base_dir": str(self.base) if self.base else None,
                "base_missing": self.base_missing,
                "sample_size": self.sample_size, "reused": reused,
                "sampled": sum(str(r["product_id"]) in self.index and self.sampled(str(r["product_id"])) for r in raw)}
