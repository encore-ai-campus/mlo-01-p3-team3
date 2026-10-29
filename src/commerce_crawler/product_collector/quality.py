"""Run-level quality gate: detects site structure changes from values already extracted.

Structure rates count only details that came back as a page (ok / unrecognized / identity_mismatch).
Failed details are judged by rate in detail_failures, which the collector uses for report["result"]."""
from __future__ import annotations

from statistics import median

VERSION = 1
MODES = ("warn", "enforce", "off")
# Per-product detail failures decide completeness in every mode: within these rates the run stays
# completed and only the failed products are left out by the loader (their detail_status is not ok).
FAILURE_LIMITS = {"request_failed": 0.01, "unrecognized": 0.01, "identity_mismatch": 0.005}
# Structure signals: bounds before calibration against normal runs; a breach fails only in enforce mode.
LIMITS = {"no_images": 0.30, "no_specifications": 0.50, "no_priced_offer": 0.50, "listing_rejected": 0.05}
MIN_SAMPLE = 50           # fewer details: any failure is incomplete and structure rates are not judged
MIN_CATEGORY_SAMPLE = 20  # per-category alerts keep one broken category from hiding in the total
RECEIVED = {"ok", "unrecognized", "identity_mismatch"}


def detail_failures(raw: list[dict]) -> dict:
    """Failed details among requested ones. Budget stops are not here: they leave the run incomplete."""
    requested = [r for r in raw if r["detail_status"] in RECEIVED | {"request_failed"}]
    total = len(requested)
    counts = {status: sum(r["detail_status"] == status for r in requested) for status in FAILURE_LIMITS}
    if total < MIN_SAMPLE:
        passed = not any(counts.values())
    else:
        passed = all(counts[status] / total <= limit for status, limit in FAILURE_LIMITS.items())
    return {"requested": total, "counts": counts, "limits": FAILURE_LIMITS, "passed": passed,
            "failed_product_ids": sorted(str(r["product_id"]) for r in requested if r["detail_status"] in FAILURE_LIMITS)}


def _signals(row: dict) -> dict:
    status = row["detail_status"]
    specs = [s for s in row.get("specifications") or [] if s.get("name") and s.get("value") not in (None, "")]
    priced = [o for o in row.get("offers") or []
              if o.get("price") is not None or o.get("low_price") is not None]
    return {"identity_mismatch": status == "identity_mismatch", "unrecognized": status == "unrecognized",
            "no_images": not row.get("images"), "no_specifications": not specs, "no_priced_offer": not priced}


def _rates(rows: list[dict], names=("no_images", "no_specifications", "no_priced_offer")) -> dict:
    flags = [_signals(row) for row in rows]
    total = len(flags)
    return {name: {"count": sum(f[name] for f in flags), "total": total,
                   "rate": round(sum(f[name] for f in flags) / total, 4) if total else None}
            for name in names}


def evaluate(normalized: list[dict], report: dict, mode: str = "warn") -> dict:
    if mode not in MODES:
        raise ValueError(f"quality mode must be one of {MODES}")
    received = [row for row in normalized if row["detail_status"] in RECEIVED]
    metrics = _rates(received)
    responded = rejected = 0
    alerts = []
    for category in report.get("categories", []):
        pages = category.get("pages", [])
        for page in pages:
            diagnostics = page.get("diagnostics") or {}
            responded += diagnostics.get("response_product_count") or 0
            rejected += sum((diagnostics.get("rejected_products") or {}).get(k, 0) for k in ("invalid_code", "missing_name"))
        if category.get("reason") == "repeated_products":
            alerts.append({"signal": "listing_repeated_products", "category_id": category["category_id"]})
        if pages and pages[0].get("status") == "empty" and not category.get("selected"):
            alerts.append({"signal": "listing_first_page_empty", "category_id": category["category_id"]})
    metrics["listing_rejected"] = {"count": rejected, "total": responded,
                                   "rate": round(rejected / responded, 4) if responded else None}
    for name, metric in metrics.items():
        metric["limit"] = LIMITS[name]
    counts = [len(row.get("images") or []) for row in received if row["detail_status"] == "ok"]
    gallery_median = median(counts) if counts else None

    by_category = {}
    for row in received:
        for membership in row.get("categories") or []:
            by_category.setdefault(membership["category_id"], []).append(row)
    for category_id, rows in by_category.items():
        if len(rows) < MIN_CATEGORY_SAMPLE:
            continue
        limits = {**LIMITS, **FAILURE_LIMITS}
        for name, metric in _rates(rows, ("identity_mismatch", "unrecognized", *_rates([]))).items():
            if metric["rate"] > limits[name]:
                alerts.append({"signal": f"category_{name}", "category_id": category_id, **metric})

    judged = len(received) >= MIN_SAMPLE
    # Listing rejection is judged on its own denominator, which can exist without details (--listing-only).
    violations = [{"signal": name, **metric} for name, metric in metrics.items()
                  if metric["rate"] is not None and metric["rate"] > metric["limit"]
                  and (judged or name == "listing_rejected" and responded >= MIN_SAMPLE)]
    return {"version": VERSION, "mode": mode, "received_details": len(received), "judged": judged,
            "gallery_count_median": gallery_median, "metrics": metrics, "violations": violations,
            "alerts": alerts, "passed": not violations}


def apply(report: dict, normalized: list[dict], mode: str = "warn") -> dict:
    """Record report["quality"]; in enforce mode a failed gate replaces a completed result.

    An incomplete (partial) run stays partial: an accepted quality never promotes missing data."""
    if mode == "off":
        report.pop("quality", None)
        return report
    report["quality"] = evaluate(normalized, report, mode)
    if mode == "enforce" and not report["quality"]["passed"] and report.get("result") == "completed":
        report["result"] = "quality_failed"
    return report
