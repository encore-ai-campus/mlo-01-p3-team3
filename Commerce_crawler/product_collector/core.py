"""Site-independent request budget, category orchestration and local storage."""
from __future__ import annotations

import hashlib
import json
import math
import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Protocol
from urllib.parse import urlparse
from uuid import uuid4

from .storage import atomic_json, write_jsonl

import requests
from requests.adapters import HTTPAdapter


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


@dataclass(frozen=True)
class Category:
    category_id: str
    category_name: str | None = None
    limit: int = 2
    start_page: int = 1
    max_pages: int = 1

    def __post_init__(self) -> None:
        if not isinstance(self.category_id, str) or not re.fullmatch(r"[0-9]+", self.category_id):
            raise ValueError("category_id must be a nonempty ASCII digit string (not fixed to 8 digits)")
        if self.category_name is not None and not isinstance(self.category_name, str):
            raise ValueError("category_name must be a string or null")
        if type(self.limit) is not int or self.limit < 1:
            raise ValueError("category limit must be a positive integer")
        if any(type(value) is not int or value < 1 for value in (self.start_page, self.max_pages)):
            raise ValueError("start_page/max_pages must be positive integers")

    def as_dict(self) -> dict:
        return {"category_id": self.category_id, "category_name": self.category_name}


def load_categories(path: Path) -> list[Category]:
    values = json.loads(path.read_text(encoding="utf-8-sig"))
    if not isinstance(values, list) or not values:
        raise ValueError("categories must be a nonempty JSON list")
    categories = [Category(**value) for value in values]
    if len({c.category_id for c in categories}) != len(categories):
        raise ValueError("duplicate category IDs in configuration")
    return categories


def payload_hash(payload: dict) -> str:
    """Hash source extraction, excluding run timestamps and category configuration."""
    content = {"listings": [{k: v for k, v in item.items() if k not in {"collected_at", "category"}}
                            for item in payload["listings"]],
               "detail": {k: v for k, v in (payload["detail"] or {}).items() if k != "collected_at"}}
    return hashlib.sha256(json.dumps(content, ensure_ascii=False, sort_keys=True).encode("utf-8")).hexdigest()


def field_coverage(rows: list[dict]) -> dict:
    fields = sorted({field for row in rows for field in row.get("field_status", {})})
    return {field: {"observed": sum(row.get(field) is not None for row in rows), "total": len(rows)}
            for field in fields}


class BudgetExhausted(Exception):
    pass


class RequestClient:
    """One get attempt = one budget unit, including failures. No redirects/retries."""

    def __init__(self, max_requests: int = 15, interval: float = 1.0,
                 timeout: float = 20.0, session: Any = None,
                 clock=time.monotonic, sleep=time.sleep):
        if type(max_requests) is not int or max_requests < 1:
            raise ValueError("max_requests must be a positive integer")
        if not math.isfinite(interval) or interval < 0 or not math.isfinite(timeout) or timeout <= 0:
            raise ValueError("interval must be finite and nonnegative; timeout must be finite and positive")
        self.max_requests, self.interval, self.timeout = max_requests, interval, timeout
        self.session = session if session is not None else requests.Session()
        self.session.mount("https://", HTTPAdapter(max_retries=0))
        self.session.headers.update({
            "User-Agent": "DanawaStudyCollector/0.2 (low-volume educational request)",
            "Accept-Language": "ko-KR,ko;q=0.9,en;q=0.7",
        })
        self.clock, self.sleep = clock, sleep
        self.last_started: float | None = None
        self.attempts: list[dict] = []

    def get(self, url: str) -> str:
        return self.request("GET", url)

    def post(self, url: str, payload: list, headers: dict) -> str:
        return self.request("POST", url, payload=payload, headers=headers)

    def request(self, method: str, url: str, payload=None, headers=None) -> str:
        parsed = urlparse(url)
        if parsed.scheme != "https" or parsed.netloc != "prod.danawa.com":
            raise ValueError("only https://prod.danawa.com requests are allowed in this collector")
        if len(self.attempts) >= self.max_requests:
            raise BudgetExhausted("request_limit")
        if self.last_started is not None:
            self.sleep(max(0.0, self.interval - (self.clock() - self.last_started)))
        self.last_started = self.clock()
        attempt = {"method": method, "url": url, "requested_at": utc_now(), "status_code": None, "error": None}
        if payload is not None:
            attempt["payload"] = payload
        self.attempts.append(attempt)
        try:
            if method == "POST":
                response = self.session.post(url, data=json.dumps(payload, ensure_ascii=False), headers=headers,
                                             timeout=self.timeout, allow_redirects=False)
            else:
                response = self.session.get(url, timeout=self.timeout, allow_redirects=False)
            attempt["status_code"] = response.status_code
            response.raise_for_status()
            if 300 <= response.status_code < 400:
                raise requests.HTTPError("redirect not followed", response=response)
            # requests defaults to Latin-1 for text/html without a charset.
            if not response.encoding or response.encoding.lower() == "iso-8859-1":
                response.encoding = response.apparent_encoding
            return response.text
        except requests.RequestException as exc:
            attempt["error"] = f"{type(exc).__name__}: {exc}"
            raise

    def close(self) -> None:
        self.session.close()


class Adapter(Protocol):
    source: str
    def category_url(self, category_id: str) -> str: ...
    def parse_listing(self, html: str, limit: int) -> tuple[list[dict], dict]: ...
    def needs_detail(self, listing: dict) -> bool: ...
    def parse_detail(self, html: str, listing: dict) -> dict: ...
    def normalize(self, raw: dict) -> dict: ...
    def validate_categories(self, categories: list[Category]) -> None: ...
    def fetch_listing_page(self, client: RequestClient, category: Category, page: int) -> tuple[list[dict], dict]: ...


def collect(adapter: Adapter, client: RequestClient, categories: list[Category],
            total_limit: int = 10, collect_details: bool = True) -> tuple[list[dict], list[dict], dict]:
    if type(total_limit) is not int or total_limit < 1:
        raise ValueError("total_limit must be a positive integer")
    adapter.validate_categories(categories)
    job_id = str(uuid4())
    report = {"job_id": job_id, "started_at": utc_now(), "source": adapter.source, "execution_mode": "live",
              "limits": {"total_products": total_limit, "requests": client.max_requests,
                         "interval_seconds": client.interval, "timeout_seconds": client.timeout,
                         "collect_details": collect_details},
              "categories": [], "errors": [], "stop_reason": None}
    records: dict[tuple[str, str], dict] = {}
    # Listings first: default budgets leave room for all five categories before details.
    for category in categories:
        url = adapter.category_url(category.category_id)
        outcome = {**category.as_dict(), "limit": category.limit, "url": url,
                   "status": "not_requested", "selected": 0, "new_products": 0,
                   "start_page": category.start_page, "max_pages": category.max_pages, "pages": []}
        report["categories"].append(outcome)
        if len(records) >= total_limit:
            outcome["reason"] = "total_limit"
            report["stop_reason"] = "total_limit"
            continue
        seen_category = set()
        for page in range(category.start_page, category.start_page + category.max_pages):
            if len(seen_category) >= category.limit or len(records) >= total_limit:
                outcome["reason"] = "category_limit" if len(seen_category) >= category.limit else "total_limit"
                break
            page_outcome = {"page": page, "status": "not_requested", "selected": 0}
            outcome["pages"].append(page_outcome)
            try:
                rows, diagnostics = adapter.fetch_listing_page(client, category, page)
            except BudgetExhausted:
                page_outcome["reason"] = outcome["reason"] = "request_limit"
                report["stop_reason"] = "request_limit"
                break
            except (requests.RequestException, ValueError) as exc:
                outcome.update(status="request_failed" if isinstance(exc, requests.RequestException) else "parse_failed", reason=str(exc))
                page_outcome.update(status=outcome["status"], reason=str(exc))
                report["errors"].append({"stage": "listing", "url": url, "page": page, "reason": str(exc)})
                break
            outcome["diagnostics"] = diagnostics
            outcome["observed_category_name"] = diagnostics.get("observed_category_name")
            page_outcome["diagnostics"] = diagnostics
            if not rows:
                page_outcome["status"] = "empty"
                outcome["reason"] = "empty_page"
                if diagnostics.get("format") == "action_products" and diagnostics.get("response_product_count") == 0:
                    outcome["status"] = "ok"
                else:
                    outcome["status"] = "no_products"
                    report["errors"].append({"stage": "listing", "url": url, "page": page,
                                             "reason": "no_products_or_parser_mismatch"})
                break
            page_outcome["status"] = outcome["status"] = "ok"
            new_ids = {row["product_id"] for row in rows} - seen_category
            if not new_ids:
                outcome["reason"] = page_outcome["reason"] = "repeated_products"
                break
            for row in rows:
                if row["product_id"] in seen_category or len(seen_category) >= category.limit:
                    continue
                key = (adapter.source, row["product_id"])
                if key not in records:
                    if len(records) >= total_limit:
                        report["stop_reason"] = "total_limit"
                        continue
                    records[key] = {"source": adapter.source, "product_id": row["product_id"],
                                    "collected_at": utc_now(), "job_id": job_id,
                                    "schema_version": "1.2", "extractor_version": "0.3",
                                    "categories": [], "payload": {"listings": [], "detail": None},
                                    "detail_status": "not_requested", "detail_error": None}
                    outcome["new_products"] += 1
                record = records[key]
                membership = {**category.as_dict(), "observed_category_name": diagnostics.get("observed_category_name")}
                if not any(c["category_id"] == category.category_id for c in record["categories"]):
                    record["categories"].append(membership)
                record["payload"]["listings"].append({"category": membership,
                                                        "url": url, "page": page, "collected_at": utc_now(), **row})
                seen_category.add(row["product_id"])
                outcome["selected"] += 1
                page_outcome["selected"] += 1
        else:
            outcome["reason"] = "max_pages"

    for record in records.values():
        listing = record["payload"]["listings"][0]
        if not collect_details:
            record["detail_status"] = "disabled"
            continue
        if not adapter.needs_detail(listing):
            record["detail_status"] = "not_needed"
            continue
        try:
            html = client.get(listing["product_url"])
            record["payload"]["detail"] = {"url": listing["product_url"], "collected_at": utc_now(),
                                               **adapter.parse_detail(html, listing)}
            record["detail_status"] = record["payload"]["detail"]["status"]
        except BudgetExhausted:
            record["detail_status"] = "budget_exhausted"
            report["stop_reason"] = "request_limit"
        except requests.RequestException as exc:
            record["detail_status"] = "request_failed"
            record["detail_error"] = str(exc)
            report["errors"].append({"stage": "detail", "url": listing["product_url"], "reason": str(exc)})

    raw = list(records.values())
    normalized = []
    for record in raw:
        record["payload_hash"] = payload_hash(record["payload"])
        normalized.append(adapter.normalize(record))
    report.update(finished_at=utc_now(), product_count=len(raw), request_count=len(client.attempts),
                  requests=list(client.attempts), field_coverage=field_coverage(normalized),
                  result="partial" if report["errors"] or any(
                      r["detail_status"] in {"budget_exhausted", "request_failed", "unrecognized"} for r in raw
                  ) or any(c["status"] == "not_requested" or c.get("reason") == "request_limit"
                           for c in report["categories"]) else "completed")
    return raw, normalized, report


def save_run(output_root: Path, raw: list[dict], normalized: list[dict], report: dict) -> Path:
    destination = output_root / report["job_id"]
    destination.mkdir(parents=True, exist_ok=False)
    write_jsonl(destination / "product_raw.jsonl", raw)
    write_jsonl(destination / "products.jsonl", normalized)
    atomic_json(destination / "run_report.json", report)
    return destination
