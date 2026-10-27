"""Repair extraction from saved HTML without issuing any HTTP request."""
from __future__ import annotations

import copy
import json
import re
from pathlib import Path
from uuid import uuid4

from .core import field_coverage, payload_hash, utc_now
from .danawa import DanawaAdapter, validate_identity
from .storage import read_jsonl


def apply_detail(record, adapter, html, collected_at=None):
    """Shared detail identity check and extraction for saved runs."""
    validate_identity(html, str(record["product_id"]))
    listing = record["payload"]["listings"][0]
    parsed = adapter.parse_detail(html, listing)
    record["payload"]["detail"] = {"url": listing["product_url"], "collected_at": collected_at or utc_now(), **parsed}
    record["detail_status"], record["detail_error"] = parsed["status"], None


def reprocess_run(directory: Path) -> tuple[list[dict], list[dict], dict]:
    original = json.loads((directory / "run_report.json").read_text(encoding="utf-8"))
    raw = read_jsonl(directory / "product_raw.jsonl")
    adapter = DanawaAdapter()
    report = copy.deepcopy(original)
    report.update(job_id=str(uuid4()), original_run_id=original["job_id"], started_at=utc_now(),
                  execution_mode="offline_reprocess", original_request_count=original["request_count"],
                  original_request_log=original["requests"], request_count=0, requests=[])
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
            html = detail.get("html") or detail.get("unrecognized_html")
            if not html:
                raise ValueError(f"saved full detail HTML is missing: {record['product_id']}")
            apply_detail(record, adapter, html, detail.get("collected_at") or record["collected_at"])
        record["payload_hash"] = payload_hash(record["payload"])
        record["schema_version"] = "1.2"
        record["extractor_version"] = "0.3"
    normalized = [adapter.normalize(record) for record in raw]
    report.update(finished_at=utc_now(), product_count=len(raw), field_coverage=field_coverage(normalized),
                  result="partial" if report["errors"] or any(r["detail_status"] in {
                      "budget_exhausted", "request_failed", "unrecognized"} for r in raw)
                  or any(c["status"] == "not_requested" for c in report["categories"]) else "completed")
    return raw, normalized, report
