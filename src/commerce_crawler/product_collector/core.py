"""Site-independent request budget, category orchestration and local storage."""
from __future__ import annotations

import copy
import hashlib
import json
import math
import re
import time
import threading
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Protocol
from urllib.parse import urlparse
from uuid import uuid4

from . import quality
from .storage import atomic_json, detail_html_path, save_detail_html, write_jsonl

import requests
from requests.adapters import HTTPAdapter


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


@dataclass(frozen=True)
class Category:
    category_id: str
    category_name: str | None = None
    limit: int | None = 2
    start_page: int = 1
    max_pages: int | None = 1

    def __post_init__(self) -> None:
        if not isinstance(self.category_id, str) or not re.fullmatch(r"[0-9]+", self.category_id):
            raise ValueError("category_id must be a nonempty ASCII digit string (not fixed to 8 digits)")
        if self.category_name is not None and not isinstance(self.category_name, str):
            raise ValueError("category_name must be a string or null")
        if self.limit is not None and (type(self.limit) is not int or self.limit < 1):
            raise ValueError("category limit must be a positive integer")
        if type(self.start_page) is not int or self.start_page < 1 or (self.max_pages is not None and (type(self.max_pages) is not int or self.max_pages < 1)):
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


class FieldCoverage:
    """field_coverage accumulated one row at a time."""

    def __init__(self):
        self.fields, self.observed, self.total = set(), {}, 0

    def add(self, row: dict) -> None:
        self.total += 1
        self.fields.update(row.get("field_status", {}))
        for field, value in row.items():
            if value is not None:
                self.observed[field] = self.observed.get(field, 0) + 1

    def result(self) -> dict:
        return {field: {"observed": self.observed.get(field, 0), "total": self.total}
                for field in sorted(self.fields)}


def field_coverage(rows) -> dict:
    coverage = FieldCoverage()
    for row in rows:
        coverage.add(row)
    return coverage.result()


def raw_projection(raw: dict) -> dict:
    """What run_result, detail_failures and the reuse summary read of a raw record."""
    return {"product_id": raw["product_id"], "detail_status": raw["detail_status"],
            "detail_reused_from": raw.get("detail_reused_from")}


class StoredRecords(Sequence):
    """Records of a checkpointed run, read back one at a time instead of held in memory.

    projection: the small per-product values the run's aggregates were computed from."""

    def __init__(self, checkpoint, keys: list, part: str, projection: list):
        self.checkpoint, self.keys, self.projection = checkpoint, keys, projection
        self._load = checkpoint.load if part == "raw" else checkpoint.load_normalized

    def __len__(self):
        return len(self.keys)

    def __getitem__(self, index):
        if isinstance(index, slice):
            return [self._load(key) for key in self.keys[index]]
        return self._load(self.keys[index])

    def __iter__(self):
        return (self._load(key) for key in self.keys)


class MemoryRecords:
    """A run without a checkpoint (tests, saved-run tools): records stay plain objects."""

    def __init__(self):
        self.records = {}

    def __len__(self):
        return len(self.records)

    def __contains__(self, key):
        return key in self.records

    def keys(self):
        return list(self.records)

    def detail_status(self, key):
        return self.records[key]["detail_status"]

    def category_ids(self, key):
        return tuple(c["category_id"] for c in self.records[key]["categories"])

    def load(self, key):
        return self.records[key]

    def add(self, key, record):
        self.records[key] = record

    def save(self, changed, report, attempts):
        pass


class CheckpointRecords:
    """The checkpoint's index plus the products created on the page being merged.

    A product is read when it is about to change and saved before the next one is read,
    so memory holds one page of records, not the whole collection."""

    def __init__(self, checkpoint):
        self.checkpoint, self.pending = checkpoint, {}

    def __len__(self):
        return len(self.checkpoint) + len(self.pending)

    def __contains__(self, key):
        return key in self.pending or key in self.checkpoint

    def keys(self):
        return self.checkpoint.keys() + list(self.pending)

    def detail_status(self, key):
        return self.checkpoint.detail_status(key)

    def category_ids(self, key):
        return self.checkpoint.category_ids(key)

    def load(self, key):
        return self.pending[key] if key in self.pending else self.checkpoint.load(key)

    def add(self, key, record):
        self.pending[key] = record

    def save(self, changed, report, attempts):
        self.checkpoint.save(changed, report, attempts)
        self.pending.clear()


def run_result(report: dict, raw: list[dict], categories_incomplete: bool, include_reused: bool = False) -> str:
    """Completeness: listing errors, budget stops or missing categories make a partial run.

    Failed details only do so above their rates; below them the loader leaves out those products.
    Reused details were not requested in this run, so their rate excludes them unless re-extracted."""
    report["detail_failures"] = quality.detail_failures(raw, include_reused)
    return "partial" if (categories_incomplete or any(e.get("stage") != "detail" for e in report["errors"])
                         or any(r["detail_status"] == "budget_exhausted" for r in raw)
                         or not report["detail_failures"]["passed"]) else "completed"


def limit_reached(current: int, maximum: int | None) -> bool:
    return maximum is not None and current >= maximum


def parse_limit(value: str) -> int | None:
    """CLI: zero means unlimited; positive values restore a finite cap."""
    result = int(value)
    if result < 0:
        raise ValueError("limit must be zero (unlimited) or positive")
    return result or None


class BudgetExhausted(Exception):
    pass


class AttemptLog(list):
    """Request attempts, plus which of them are finished (all fields final).

    The marks are kept beside the entries so the saved request log has no extra field."""

    def __init__(self):
        super().__init__()
        self._finished = set()

    def mark_finished(self, index: int) -> None:
        self._finished.add(index)

    def finished(self, index: int) -> bool:
        return index in self._finished


class RequestClient:
    """One get attempt = one budget unit, including failures. No redirects/retries."""

    def __init__(self, max_requests: int | None = 15, interval: float = 1.0,
                 timeout: float = 20.0, session: Any = None,
                 clock=time.monotonic, sleep=time.sleep, progress=None, session_factory=None):
        if max_requests is not None and (type(max_requests) is not int or max_requests < 1):
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
        self.attempts: AttemptLog = AttemptLog()
        self.halt_reason: str | None = None
        self.progress = progress
        self._request_lock = threading.Lock()
        self._local = threading.local()
        self._owner = threading.get_ident()
        self._session_factory = session_factory or requests.Session
        self._sessions = [self.session]
        self._cancelled = threading.Event()

    def get(self, url: str) -> str:
        return self.request("GET", url)

    def post(self, url: str, payload: list, headers: dict) -> str:
        return self.request("POST", url, payload=payload, headers=headers)

    def request(self, method: str, url: str, payload=None, headers=None) -> str:
        parsed = urlparse(url)
        if parsed.scheme != "https" or parsed.netloc != "prod.danawa.com":
            raise ValueError("only https://prod.danawa.com requests are allowed in this collector")
        with self._request_lock:
            if self.halt_reason or self._cancelled.is_set():
                raise BudgetExhausted(self.halt_reason or "interrupted")
            if limit_reached(len(self.attempts), self.max_requests):
                raise BudgetExhausted("request_limit")
            if self.last_started is not None:
                self.sleep(max(0.0, self.interval - (self.clock() - self.last_started)))
            if self.halt_reason or self._cancelled.is_set():
                raise BudgetExhausted(self.halt_reason or "interrupted")
            self.last_started = self.clock()
            attempt = {"method": method, "url": url, "requested_at": utc_now(), "status_code": None, "error": None}
            if payload is not None:
                attempt["payload"] = copy.deepcopy(payload)  # the log keeps what was sent
            self.attempts.append(attempt)
            index = len(self.attempts) - 1
            if self.progress:
                self.progress({"stage": "request", "number": len(self.attempts), "method": method, "url": url})
        try:
            return self._send(attempt, method, url, payload, headers)
        finally:
            # After every field of the attempt is final; a checkpoint then stops re-encoding it.
            with self._request_lock:
                self.attempts.mark_finished(index)

    def _send(self, attempt: dict, method: str, url: str, payload, headers) -> str:
        session = self._thread_session()
        try:
            if method == "POST":
                response = session.post(url, data=json.dumps(payload, ensure_ascii=False), headers=headers,
                                             timeout=self.timeout, allow_redirects=False)
            else:
                response = session.get(url, timeout=self.timeout, allow_redirects=False)
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
            if isinstance(exc, requests.HTTPError) and exc.response is not None and exc.response.status_code in {403, 429}:
                self.halt_reason = f"http_{exc.response.status_code}"
                self._cancelled.set()
            raise

    def _thread_session(self):
        if threading.get_ident() == self._owner:
            return self.session
        if not hasattr(self._local, 'session'):
            session = self._session_factory()
            session.mount("https://", HTTPAdapter(max_retries=0))
            session.headers.update(self.session.headers)
            self._local.session = session
            with self._request_lock:
                self._sessions.append(session)
        return self._local.session

    def cancel(self):
        self._cancelled.set()

    def close(self) -> None:
        for session in self._sessions:
            session.close()


class Adapter(Protocol):
    source: str
    def category_url(self, category_id: str) -> str: ...
    def parse_listing(self, html: str, limit: int | None) -> tuple[list[dict], dict]: ...
    def needs_detail(self, listing: dict) -> bool: ...
    def parse_detail(self, html: str, listing: dict) -> dict: ...
    def normalize(self, raw: dict) -> dict: ...
    def listing_fingerprint(self, listings: list[dict]) -> str | None: ...
    def validate_categories(self, categories: list[Category]) -> None: ...
    def fetch_listing_page(self, client: RequestClient, category: Category, page: int) -> tuple[list[dict], dict]: ...


def collect(adapter: Adapter, client: RequestClient, categories: list[Category],
            total_limit: int | None = 10, collect_details: bool = True, progress=None, checkpoint=None,
            listing_interval: float | None = None, workers: int = 1,
            detail_reuse=None) -> tuple[list[dict], list[dict], dict]:
    if total_limit is not None and (type(total_limit) is not int or total_limit < 1):
        raise ValueError("total_limit must be a positive integer")
    if listing_interval is not None and (not math.isfinite(listing_interval) or listing_interval < 0):
        raise ValueError("listing_interval must be finite and nonnegative")
    if type(workers) is not int or workers < 1:
        raise ValueError("workers must be a positive integer")
    detail_interval = client.interval
    adapter.validate_categories(categories)
    # Products are read when they change and saved right away; only the checkpoint's index stays.
    records = CheckpointRecords(checkpoint) if checkpoint else MemoryRecords()
    previous = checkpoint.current_report() if checkpoint else {}
    previous_requests = previous.get("requests", [])
    previous_categories = {c["category_id"]: c for c in previous.get("categories", [])}
    job_id = checkpoint.job_id if checkpoint else str(uuid4())
    report = {"job_id": job_id, "started_at": utc_now(), "source": adapter.source, "execution_mode": "live",
              "limits": {"total_products": total_limit, "requests": client.max_requests,
                         "interval_seconds": client.interval, "timeout_seconds": client.timeout,
                         "listing_interval_seconds": listing_interval if listing_interval is not None else client.interval,
                         "listing_workers": workers,
                         "collect_details": collect_details},
              "categories": [], "errors": [], "stop_reason": None}
    if previous.get('categories') or len(records):
        report["resumed_at"] = utc_now()
        report["started_at"] = previous.get("started_at", report["started_at"])
        report["previous_errors"] = previous.get("previous_errors", []) + previous.get("errors", [])

    def persist(changed=()):
        if checkpoint:
            report["product_count"] = len(records)
            records.save(changed, report, client.attempts)

    for category in categories:
        outcome = copy.deepcopy(previous_categories.get(category.category_id))
        if outcome is None:
            outcome = {**category.as_dict(), "limit": category.limit, "url": adapter.category_url(category.category_id),
                       "status": "not_requested", "selected": 0, "new_products": 0,
                       "start_page": category.start_page, "max_pages": category.max_pages, "pages": []}
        report["categories"].append(outcome)
    persist()
    if listing_interval is not None:
        client.interval = listing_interval
    # Fetchers own no product/report/checkpoint state. This thread is the only writer.
    from .parallel import page_results
    category_order = {c.category_id: i for i, c in enumerate(categories)}
    states = []
    for index, category in enumerate(categories):
        outcome = report["categories"][index]
        if outcome.get("reason") in {"empty_page", "repeated_products", "max_pages", "category_limit", "total_limit"}:
            continue
        outcome["pages"] = [p for p in outcome["pages"] if p["status"] in {"ok", "empty"}]
        outcome.pop("reason", None)
        seen = {key[1] for key in records.keys() if category.category_id in records.category_ids(key)}
        next_page = max((p["page"] for p in outcome["pages"]), default=category.start_page - 1) + 1
        state = {"category": category, "index": index, "outcome": outcome, "seen": seen,
                 "page": next_page, "done": False}
        if limit_reached(len(seen), category.limit):
            outcome['reason'], state['done'] = 'category_limit', True
        elif category.max_pages is not None and next_page >= category.start_page + category.max_pages:
            outcome['reason'], state['done'] = 'max_pages', True
        states.append(state)

    def stop_reason():
        if client.halt_reason:
            return client.halt_reason
        if limit_reached(len(records), total_limit):
            return 'total_limit'
        if limit_reached(len(client.attempts), client.max_requests):
            return 'request_limit'
        return None

    def started(state):
        if progress:
            c = state['category']
            progress({'stage': 'category', 'index': state['index'] + 1, 'total': len(categories),
                      'category_id': c.category_id, 'name': c.category_name})

    stream = page_results(adapter, client, [s for s in states if not s['done']], workers, stop_reason, started)
    try:
        for state, result, error in stream:
            category, outcome, page = state['category'], state['outcome'], state['page']
            page_outcome = {'page': page, 'status': 'not_requested', 'selected': 0}
            outcome['pages'].append(page_outcome)
            changed = []
            if error is not None:
                state['done'] = True
                if isinstance(error, BudgetExhausted):
                    outcome['reason'] = page_outcome['reason'] = report['stop_reason'] = str(error)
                elif isinstance(error, (requests.RequestException, ValueError)):
                    outcome.update(status='request_failed' if isinstance(error, requests.RequestException) else 'parse_failed', reason=str(error))
                    page_outcome.update(status=outcome['status'], reason=str(error))
                    report['errors'].append({'stage': 'listing', 'url': outcome['url'], 'page': page, 'reason': str(error)})
                    if progress:
                        progress({'stage': 'error', 'category_id': category.category_id, 'page': page, 'reason': str(error)})
                else:
                    raise error
            else:
                rows, diagnostics = result
                outcome['diagnostics'] = diagnostics
                outcome['observed_category_name'] = diagnostics.get('observed_category_name')
                page_outcome['diagnostics'] = diagnostics
                if not rows:
                    page_outcome['status'] = 'empty'
                    outcome['reason'], state['done'] = 'empty_page', True
                    outcome['status'] = 'ok' if diagnostics.get('format') == 'action_products' and diagnostics.get('response_product_count') == 0 else 'no_products'
                    if outcome['status'] != 'ok':
                        report['errors'].append({'stage': 'listing', 'url': outcome['url'], 'page': page, 'reason': 'no_products_or_parser_mismatch'})
                else:
                    outcome['status'] = page_outcome['status'] = 'ok'
                    if not ({r['product_id'] for r in rows} - state['seen']):
                        outcome['reason'] = page_outcome['reason'] = 'repeated_products'
                        state['done'] = True
                    else:
                        for row in rows:
                            if row['product_id'] in state['seen'] or limit_reached(len(state['seen']), category.limit):
                                continue
                            key = (adapter.source, row['product_id'])
                            if key not in records:
                                if limit_reached(len(records), total_limit):
                                    continue
                                record = {'source': adapter.source, 'product_id': row['product_id'],
                                    'collected_at': utc_now(), 'job_id': job_id, 'schema_version': '1.2', 'extractor_version': '0.4',
                                    'categories': [], 'payload': {'listings': [], 'detail': None},
                                    'detail_status': 'not_requested' if collect_details else 'disabled', 'detail_error': None}
                                records.add(key, record)
                                outcome['new_products'] += 1
                            else:
                                record = records.load(key)
                            membership = {**category.as_dict(), 'observed_category_name': diagnostics.get('observed_category_name')}
                            if not any(c['category_id'] == category.category_id for c in record['categories']):
                                record['categories'].append(membership)
                            record['payload']['listings'].append({'category': membership, 'url': outcome['url'],
                                                                 'page': page, 'collected_at': utc_now(), **row})
                            record['categories'].sort(key=lambda c: category_order[c['category_id']])
                            record['payload']['listings'].sort(key=lambda r: (category_order[r['category']['category_id']], r['page']))
                            state['seen'].add(row['product_id'])
                            outcome['selected'] += 1
                            page_outcome['selected'] += 1
                            changed.append(record)
                        if limit_reached(len(state['seen']), category.limit):
                            outcome['reason'], state['done'] = 'category_limit', True
                if not state['done']:
                    state['page'] += 1
                    if category.max_pages is not None and state['page'] >= category.start_page + category.max_pages:
                        outcome['reason'], state['done'] = 'max_pages', True
                persist(changed)
                if progress:
                    progress({'stage': 'page', 'category_id': category.category_id, 'page': page, 'products': len(rows)})
            if error is not None:
                persist()
    finally:
        stream.close()
    reason = stop_reason()
    if reason:
        report['stop_reason'] = reason
        for state in states:
            if not state['done']:
                state['outcome']['reason'] = reason
    persist()

    client.interval = detail_interval
    if progress and collect_details:
        progress({"stage": "details_start", "total": len(records)})
    keys = records.keys()
    for detail_index, key in enumerate(keys, 1):
        status = records.detail_status(key)
        if checkpoint and status in {"ok", "not_needed"}:
            if progress:
                progress({"stage": "detail", "index": detail_index, "total": len(keys),
                          "product_id": key[1], "status": status, "cached": True})
            continue
        if not collect_details:
            if status != 'disabled':
                record = records.load(key)
                record['detail_status'] = 'disabled'
                persist([record])
            continue
        record = records.load(key)
        listing = record["payload"]["listings"][0]
        if not adapter.needs_detail(listing):
            record["detail_status"] = "not_needed"
            persist([record])
            continue
        # Recorded in every mode so the next run can tell whether this product changed.
        fingerprint = record["listing_fingerprint"] = adapter.listing_fingerprint(record["payload"]["listings"])
        record.pop("detail_reused_from", None)
        reused = detail_reuse.reuse(record, fingerprint, checkpoint.directory if checkpoint else None) if detail_reuse else None
        if reused is not None:
            record["payload"]["detail"] = reused
            record["detail_status"], record["detail_error"] = "ok", None
            record["detail_reused_from"] = {"job_id": detail_reuse.base_job_id, "collected_at": reused["collected_at"]}
            persist([record])
            if progress:
                progress({"stage": "detail", "index": detail_index, "total": len(keys),
                          "product_id": record["product_id"], "status": "ok", "reused": True})
            continue
        try:
            html = client.get(listing["product_url"])
            collected_at = utc_now()
            # The page file is keyed by this second-precision time; a retry in the same second waits for the next.
            while checkpoint and detail_html_path(checkpoint.directory, record["product_id"], collected_at).exists():
                time.sleep(0.2)
                collected_at = utc_now()
            if checkpoint:
                # Saved before the record so a resumed run that skips this detail still has its HTML.
                save_detail_html(checkpoint.directory, record["product_id"], listing["product_url"], html, collected_at)
            identity_error = adapter.identity_error(html, listing)
            if identity_error:
                # Another product's page: keep only the reference to the saved page so a reprocess can re-check it.
                record["payload"]["detail"] = {"url": listing["product_url"], "collected_at": collected_at,
                                               "status": "identity_mismatch"}
                record["detail_status"], record["detail_error"] = "identity_mismatch", identity_error
            else:
                record["payload"]["detail"] = {"url": listing["product_url"], "collected_at": collected_at,
                                               **adapter.parse_detail(html, listing)}
                record["detail_status"] = record["payload"]["detail"]["status"]
                record["detail_error"] = None
        except BudgetExhausted as exc:
            record["detail_status"] = "budget_exhausted"
            report["stop_reason"] = str(exc)
        except requests.RequestException as exc:
            record["detail_status"] = "request_failed"
            record["detail_error"] = str(exc)
            report["errors"].append({"stage": "detail", "url": listing["product_url"], "reason": str(exc)})
        persist([record])
        if progress:
            progress({"stage": "detail", "index": detail_index, "total": len(keys),
                      "product_id": record["product_id"], "status": record["detail_status"]})

    # One pass for the run's aggregates. A checkpointed run reads each product back with the
    # normalization saved beside it; nothing else of the record is kept.
    coverage, raw_rows, quality_rows = FieldCoverage(), [], []
    if checkpoint:
        for key in keys:
            row = checkpoint.row(key)
            coverage.add(row["normalized"])
            raw_rows.append(raw_projection(row["raw"]))
            quality_rows.append(quality.project(row["normalized"]))
        raw = StoredRecords(checkpoint, keys, "raw", raw_rows)
        normalized = StoredRecords(checkpoint, keys, "normalized", quality_rows)
    else:
        raw, normalized = [records.load(key) for key in keys], []
        for record in raw:
            record["payload_hash"] = payload_hash(record["payload"])
            normalized.append(adapter.normalize(record))
            coverage.add(normalized[-1])
            raw_rows.append(raw_projection(record))
    if detail_reuse is not None:
        report["detail_reuse"] = detail_reuse.summary(raw_rows)
    report.update(finished_at=utc_now(), product_count=len(keys), request_count=len(previous_requests) + len(client.attempts),
                  requests=previous_requests + list(client.attempts), field_coverage=coverage.result(),
                  result=run_result(report, raw_rows, any(c["status"] == "not_requested" or c.get("reason") == "request_limit"
                                                          for c in report["categories"])))
    persist()
    return raw, normalized, report


def save_run(output_root: Path, raw: list[dict], normalized: list[dict], report: dict) -> Path:
    destination = output_root / report["job_id"]
    # A reprocess may already have written this job's details/; anything else is another run.
    if destination.exists() and {p.name for p in destination.iterdir()} - {"details"}:
        raise FileExistsError(f"run directory already exists: {destination}")
    destination.mkdir(parents=True, exist_ok=True)
    write_jsonl(destination / "product_raw.jsonl", raw)
    write_jsonl(destination / "products.jsonl", normalized)
    atomic_json(destination / "run_report.json", report)
    return destination
