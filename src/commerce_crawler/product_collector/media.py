"""Download observed product galleries; preserve identity, bytes and resume history."""
import argparse
import hashlib
import math
import re
import time
from contextlib import ExitStack
from uuid import uuid4
from pathlib import Path
from urllib.parse import urlparse

import requests
from requests.adapters import HTTPAdapter
from PIL import UnidentifiedImageError

from .core import utc_now, limit_reached, parse_limit
from .storage import atomic_json, append_record, history, link_file, read_jsonl, write_jsonl
from .validation import cached_valid, stored_metadata, verified_image
from .image_store import object_key
from .media_resume import ValidationMemo, download_history, read_products
from .checkpoint import writer_lock


def product_images(product: dict) -> list[dict]:
    # Compatibility is input-only. New output stores a single images array.
    entries = product.get("images") or [
        {"url": url, "order": i, "role": "gallery", "origin": "legacy_image_urls"}
        for i, url in enumerate(product.get("image_urls") or [])]
    seen, result = set(), []
    for entry in entries:
        if isinstance(entry, dict) and entry.get("url") and entry["url"] not in seen:
            seen.add(entry["url"])
            result.append(entry)
    return result


DELIVERED = {"downloaded", "stored"}  # statuses that give the pipeline the image
GIVE_UP_AFTER = 3  # consecutive permanent failures before a URL is skipped without --retry-failed
RETRY_POLICY_VERSION = 1  # GIVE_UP_AFTER and permanent_failure; fixed per run in image_settings.json


def permanent_failure(exc) -> bool:
    """Not-found and non-image answers repeat; timeouts, connection/DNS errors and 5xx pass."""
    if isinstance(exc, requests.HTTPError):
        code = exc.response.status_code if exc.response is not None else None
        return code is not None and 400 <= code < 500 and code not in {403, 408, 429}
    # Pillow's "not an image" is an OSError subclass, unlike network and file errors it repeats.
    return isinstance(exc, (ValueError, UnidentifiedImageError))


def success_part(entry: dict) -> dict | None:
    """The last verified download of a URL, kept apart from later failed attempts."""
    if entry.get("status") == "downloaded":
        return {k: v for k, v in entry.items()
                if k not in {"last_success", "consecutive_permanent_failures", "gave_up"}}
    return entry.get("last_success")


def compact_journal(journal: Path) -> None:
    """Keep the newest record per URL. Called with the store lock held; replaced atomically."""
    if not journal.exists():
        return
    latest, lines = download_history(journal), 0
    with journal.open("rb") as stream:
        for line in stream:
            lines += bool(line.strip())
    if lines > 2 * len(latest):
        write_jsonl(journal, latest.values())


class ImageFetcher:
    def __init__(self, interval=1.0, timeout=20.0, max_bytes=20 * 1024 * 1024,
                 session=None, sleep=time.sleep, clock=time.monotonic):
        if not math.isfinite(interval) or interval < 0 or not math.isfinite(timeout) or timeout <= 0:
            raise ValueError("invalid interval/timeout")
        if type(max_bytes) is not int or max_bytes < 1:
            raise ValueError("max_bytes must be positive")
        self.interval, self.timeout, self.max_bytes = interval, timeout, max_bytes
        self.session = session if session is not None else requests.Session()
        for scheme in ("https://", "http://"):
            self.session.mount(scheme, HTTPAdapter(max_retries=0))
        self.session.headers.update({"User-Agent": "DanawaStudyCollector/0.4", "Referer": "https://prod.danawa.com/"})
        self.sleep, self.clock, self.last_start = sleep, clock, None

    def fetch(self, url):
        parsed = urlparse(url)
        if parsed.scheme not in {"https", "http"} or not parsed.netloc or parsed.username or parsed.password:
            raise ValueError("invalid image URL")
        if self.last_start is not None:
            self.sleep(max(0, self.interval - (self.clock() - self.last_start)))
        self.last_start = self.clock()
        self.last_headers = None
        with self.session.get(url, timeout=self.timeout, stream=True, allow_redirects=False) as response:
            response.raise_for_status()
            if 300 <= response.status_code < 400:
                raise requests.HTTPError("image redirect not followed", response=response)
            chunks, count = [], 0
            for chunk in response.iter_content(chunk_size=64 * 1024):
                count += len(chunk)
                if count > self.max_bytes:
                    raise ValueError("image response exceeds max_bytes")
                chunks.append(chunk)
            # Recorded only, to judge later whether conditional requests are worth it.
            self.last_headers = {key: response.headers.get(name) for key, name in (
                ("etag", "ETag"), ("last_modified", "Last-Modified"), ("content_length", "Content-Length"))}
            return b"".join(chunks)

    def close(self):
        self.session.close()


def localize_images(products_path: Path, destination: Path, fetcher, max_requests=None,
                    progress=None, cache_dir: Path | None = None, *,
                    retry_failed=False, verify_cache=False, progress_every=25,
                    export_interval=300.0, retry_transient=False, index_dir: Path | None = None,
                    object_store=None, import_from=()) -> dict:
    """cache_dir: a local blob store shared by runs (saved-run tools).
    index_dir: the collector CLI's shared download history without image bytes. A URL whose
    last verified download is already in object_storage (HEAD size and sha256 metadata) is
    recorded as status "stored" and not downloaded; without object_store everything downloads.
    retry_transient: retry a failed URL once per run unless it failed permanently
    GIVE_UP_AFTER times in a row.
    import_from: other runs' media directories whose downloads seed a new index."""
    if max_requests is not None and (type(max_requests) is not int or max_requests < 1):
        raise ValueError("max_requests must be positive")
    if type(progress_every) is not int or progress_every < 1:
        raise ValueError("progress_every must be positive")
    if not math.isfinite(export_interval) or export_interval < 0:
        raise ValueError("export_interval must be finite and nonnegative")
    if index_dir is not None and cache_dir is not None:
        raise ValueError("index_dir and cache_dir cannot be combined")
    if object_store is not None and index_dir is None:
        raise ValueError("object_store requires index_dir")
    products_path, destination = Path(products_path), Path(destination)
    store = Path(cache_dir) if cache_dir is not None else destination
    with ExitStack() as stack:
        for root in sorted({store.resolve(), destination.resolve()}, key=str):
            stack.enter_context(writer_lock(root / '.image-download.lock'))
        index = None
        if index_dir is not None:
            index = Path(index_dir)
            index.mkdir(parents=True, exist_ok=True)
            stack.enter_context(writer_lock(index / '.lock'))
            if not (index / "image_downloads.jsonl").exists():
                seed_index(index / "image_downloads.jsonl", import_from, destination, stack)
        memo = stack.enter_context(ValidationMemo(store, verify_all=verify_cache))
        report = _localize_images(products_path, destination, store, fetcher, max_requests,
                                  progress, retry_failed, progress_every, memo, export_interval,
                                  retry_transient, index, object_store)
        if index is not None:
            compact_journal(index / "image_downloads.jsonl")
        return report


def seed_index(journal: Path, sources, destination: Path, stack) -> list[str]:
    """Create the shared history from other runs' successful downloads (partial runs too).

    Only metadata is imported; whether the bytes still exist is asked of object storage.
    The first listed source wins per URL, so list the newest run first."""
    found, used = {}, []
    for source in sources:
        source = Path(source)
        if source.resolve() == destination.resolve() or not (source / "image_downloads.jsonl").is_file():
            continue
        try:
            stack.enter_context(writer_lock(source / '.image-download.lock'))
        except (ValueError, OSError):
            continue  # another image step owns it; not imported now
        used.append(str(source))
        for url, entry in download_history(source / "image_downloads.jsonl").items():
            success = success_part(entry)
            if url not in found and stored_metadata(success):
                found[url] = {**{k: v for k, v in success.items() if k != 'local_path'},
                              "imported_from": str(source)}
    write_jsonl(journal, found.values())
    return used


STORE_ERROR_LIMIT = 20  # consecutive HEAD errors before the run stops asking object storage


def _localize_images(products_path, destination, store, fetcher, max_requests,
                     progress, retry_failed, progress_every, memo, export_interval,
                     retry_transient=False, index=None, object_store=None):
    products, input_hash = read_products(products_path)
    for product in products:
        if not all(re.fullmatch(r"[A-Za-z0-9_-]+", str(product.get(k, ""))) for k in ("source", "product_id")):
            raise ValueError("unsafe source/product_id")
        for category in product.get('categories') or []:
            if not isinstance(category, dict) or not isinstance(category.get('category_id'), str) or not re.fullmatch(r'[0-9]+', category['category_id']):
                raise ValueError('category_id must be a numeric string')
    plans = [product_images(p) for p in products]
    urls = list(dict.fromkeys(e["url"] for plan in plans for e in plan))
    destination.mkdir(parents=True, exist_ok=True)
    # One blob store per cache; output only contains named product links.
    (store / "images").mkdir(parents=True, exist_ok=True)
    journal = store / "image_downloads.jsonl"
    previous = download_history(journal)
    # Import old destination history without modifying existing data.
    legacy = download_history(destination / "image_downloads.jsonl") if store.resolve() != destination.resolve() else previous
    shared_journal = index / "image_downloads.jsonl" if index is not None else None
    shared = download_history(shared_journal) if index is not None else {}
    current = {url: {"url": url, "status": "pending", "local_path": None} for url in urls}
    report = {"started_at": utc_now(), "product_count": len(products), "unique_urls": len(urls),
              "requests": 0, "cached": 0, "downloaded": 0, "failed": 0, "budget_exhausted": 0}
    stopped = False
    run_id = str(uuid4())
    report.update(run_id=run_id, input_sha256=input_hash, skipped_failed=0, gave_up=0,
                  full_exports=0, retry_failed=retry_failed, retry_transient=retry_transient,
                  index=str(index) if index is not None else None, object_store=object_store is not None,
                  stored=0, store_checks=0, store_missing=0, store_mismatch=0, store_errors=0)
    store_errors_in_row = 0

    def in_storage(meta):
        nonlocal object_store, store_errors_in_row
        if object_store is None:
            return False
        report['store_checks'] += 1
        try:
            found = object_store.head(object_key(meta))
        except Exception as exc:  # any client failure only means "download again"
            report['store_errors'] += 1
            report['store_last_error'] = f"{type(exc).__name__}: {exc}"[:300]
            store_errors_in_row += 1
            if store_errors_in_row >= STORE_ERROR_LIMIT:
                object_store = None
                report['store_disabled'] = 'consecutive_errors'
            return False
        store_errors_in_row = 0
        if found is None:
            report['store_missing'] += 1
            return False
        if found.get('bytes') != meta['bytes'] or found.get('sha256') != meta['sha256']:
            report['store_mismatch'] += 1
            return False
        return True

    def record(entry):
        # Success/failure is published before a lightweight progress marker.
        append_record(journal, entry)
        if shared_journal is not None:
            # Paths belong to this run's media/; the index keeps what can be asked of storage.
            shared[entry['url']] = {k: v for k, v in entry.items() if k != 'local_path'}
            append_record(shared_journal, shared[entry['url']])

    def save_progress(phase):
        memo.flush()
        report.update(updated_at=utc_now(), validation_full_hashes=memo.full_hashes,
                      validation_stat_reuses=memo.stat_reuses, phase=phase)
        processed = (report['cached'] + report['stored'] + report['downloaded'] + report['failed']
                     + report['skipped_failed'] + report['budget_exhausted'])
        state = dict(contract_version=1, run_id=run_id, input_sha256=input_hash,
                     resume_strategy='url_history_and_verified_file_signature',
                     journal_bytes=journal.stat().st_size if journal.exists() else 0,
                     processed=processed, total=len(urls), **{'report': dict(report)})
        atomic_json(destination / 'image_resume.json', state)
        atomic_json(destination / 'download_report.json', report)
        if progress:
            progress(dict(report, processed=processed, total=len(urls)))

    def checkpoint():
        rows = []
        for product, plan in zip(products, plans):
            images = []
            for ordinal, observed in enumerate(plan, 1):
                entry = current[observed["url"]]
                image = {**observed, **entry, "source": product["source"], "product_id": product["product_id"],
                         "filename": None, "local_path": None, "category_paths": {}}
                image.pop("storage_path", None)  # compatibility input must not duplicate output paths
                if entry["status"] == "downloaded":
                    key = hashlib.sha256(entry["url"].encode()).hexdigest()[:12]
                    name = f"{product['source']}_{product['product_id']}_{ordinal:03d}_{key}.{entry['extension']}"
                    codes = list(dict.fromkeys(c['category_id'] for c in product.get('categories') or [])) or ['_uncategorized']
                    for code in codes:
                        relative = Path('categories') / code / product['source'] / str(product['product_id']) / name
                        source, target = store / entry['local_path'], destination / relative
                        # Existing hard links already point to the verified bytes.
                        if not (target.is_file() and source.samefile(target)):
                            link_file(source, target, entry['sha256'])
                        image['category_paths'][code] = relative.as_posix()
                    image.update(filename=name, local_path=next(iter(image['category_paths'].values())))
                images.append(image)
            row = {k: v for k, v in product.items() if k not in {"images", "image_urls", "local_images", "storage_path"}}
            for key in ("provenance", "field_status"):
                if isinstance(row.get(key), dict):
                    row[key] = {k: v for k, v in row[key].items() if k not in {"image_urls", "local_images"}}
            rows.append({**row, "schema_version": "1.2", "images": images,
                         "image_download_status": "completed" if images and all(e["status"] in DELIVERED for e in images)
                                                  else "partial" if images else "gallery_missing"})
        # Creating a hard link changes NTFS ChangeTime on the source blob.
        # Refresh the proof only after links are complete, before publishing.
        checked = set()
        for entry in current.values():
            if entry['status'] != 'downloaded' or entry['local_path'] in checked:
                continue
            if not memo.valid(entry):
                raise ValueError('image changed during category linkage')
            checked.add(entry['local_path'])
        write_jsonl(destination / "products_with_local_images.jsonl", rows)
        report["full_exports"] += 1
        report.update(products_with_local_images=sum(any(e["status"] == "downloaded" for e in r["images"]) for r in rows),
                      products_complete=sum(r["image_download_status"] == "completed" for r in rows),
                      products_with_stored_images=sum(any(e["status"] == "stored" for e in r["images"]) for r in rows),
                      products_without_gallery=sum(not plan for plan in plans),
                      unique_files=len({e["sha256"] for e in current.values() if e.get("status") == "downloaded"}),
                      updated_at=utc_now())
        report["result"] = "completed" if products and report["products_complete"] == len(products) else "partial"
        atomic_json(destination / "download_report.json", report)

    queue = []
    prepared = False
    last_export = time.monotonic()
    try:
        # Import old successful history once. A valid prior mapping is never
        # replaced with pending merely because its URL has not been visited yet.
        for position, url in enumerate(urls, 1):
            entry = previous.get(url, {})
            valid = memo.valid(entry)
            if not valid and entry.get('status') not in {'downloaded', 'stored'}:
                # A failed retry keeps the last verified download of this URL usable.
                kept = success_part(entry)
                if kept and memo.valid(kept):
                    entry, valid = kept, True
            if not valid and store.resolve() != destination.resolve():
                old = legacy.get(url, {})
                if cached_valid(old, destination):
                    link_file(destination / old['local_path'], store / old['local_path'], old['sha256'])
                    entry = old.copy()
                    valid = memo.valid(entry)
                    if valid:
                        append_record(journal, entry)
            if valid:
                current[url] = entry.copy()
                report['cached'] += 1
                if index is not None and url not in shared:
                    # A run started before the index existed: its downloads become known to later runs.
                    shared[url] = {k: v for k, v in entry.items() if k not in {'local_path', 'last_success'}}
                    append_record(shared_journal, shared[url])
            elif entry.get('status') == 'stored' and stored_metadata(entry):
                current[url] = entry.copy()  # this run already found it in storage (resume)
                report['stored'] += 1
            elif entry.get('status') == 'failed' and not retry_failed and (
                    not retry_transient or entry.get('gave_up') or index is not None):
                # With an index, a failure of this run is retried in the next run, not on resume.
                current[url] = entry.copy()
                report['skipped_failed'] += 1
                report['gave_up'] += bool(entry.get('gave_up'))
            elif index is not None:
                known = shared.get(url, {})
                success = success_part(known)
                meta = stored_metadata(success)
                if meta and in_storage(meta):
                    current[url] = dict(url=url, status='stored', **meta, local_path=None,
                                        downloaded_at=success.get('downloaded_at'),
                                        checked_at=utc_now(), run_id=run_id)
                    append_record(journal, current[url])
                    report['stored'] += 1
                elif known.get('status') == 'failed' and known.get('gave_up') and not retry_failed:
                    current[url] = {k: v for k, v in known.items() if k != 'last_success'}
                    current[url]['local_path'] = None
                    report['skipped_failed'] += 1
                    report['gave_up'] += 1
                else:
                    queue.append(url)
            else:
                queue.append(url)
            if position % 1000 == 0:
                save_progress('checking_cache')
        prepared = True
        save_progress('downloading')
        for position, url in enumerate(queue, 1):
            if stopped or limit_reached(report['requests'], max_requests):
                for pending in queue[position - 1:]:
                    current[pending]['status'] = 'stopped_after_block' if stopped else 'budget_exhausted'
                if not stopped:
                    report['budget_exhausted'] += len(queue) - position + 1
                break
            report['requests'] += 1
            attempted_at = utc_now()
            before = (shared if index is not None else previous).get(url, {})
            attempts = int(before.get('attempts', 0)) + 1
            try:
                data = fetcher.fetch(url)
                info = verified_image(data)
                relative = f"images/{info['sha256']}.{info['extension']}"
                target = store / relative
                if not memo.valid({'status': 'downloaded', 'local_path': relative, **info}):
                    temporary = target.with_suffix(target.suffix + '.tmp')
                    temporary.write_bytes(data)
                    temporary.replace(target)
                entry = dict(url=url, status='downloaded', local_path=relative,
                             downloaded_at=utc_now(), run_id=run_id, attempts=attempts,
                             response_headers=getattr(fetcher, 'last_headers', None), **info)
                if not memo.valid(entry):
                    raise ValueError('saved image checksum unconfirmed')
                report['downloaded'] += 1
            except (requests.RequestException, ValueError, OSError) as exc:
                response = exc.response if isinstance(exc, requests.HTTPError) else None
                status_code = response.status_code if response is not None else None
                permanent = permanent_failure(exc)
                streak = int(before.get('consecutive_permanent_failures', 0)) + 1 if permanent else 0
                entry = dict(url=url, status='failed', local_path=None, error=str(exc),
                             error_type=type(exc).__name__, http_status=status_code,
                             attempted_at=attempted_at, failed_at=utc_now(), run_id=run_id, attempts=attempts,
                             permanent=permanent, consecutive_permanent_failures=streak,
                             gave_up=streak >= GIVE_UP_AFTER, last_success=success_part(before))
                report['failed'] += 1
                if status_code in {403, 429}:
                    stopped = True
                    report['stop_reason'] = f'http_{status_code}'
            record(entry)
            current[url] = entry
            export_at = time.monotonic()
            if export_interval and export_at - last_export >= export_interval:
                checkpoint()
                last_export = export_at
            if position % progress_every == 0:
                save_progress('downloading')
    finally:
        report['finished_at'] = utc_now()
        # Complete export at the end/interruption, plus optional timed exports.
        # Hard termination replays the per-URL journal; the cursor is not proof.
        if prepared:
            checkpoint()
        else:
            report['result'] = 'partial'
        save_progress('cache_check_interrupted' if not prepared else 'stopped' if stopped else 'exported')
    return report


def main():
    parser = argparse.ArgumentParser(description="확보한 상품 갤러리 다운로드")
    parser.add_argument("--products", type=Path, required=True)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--cache-dir", type=Path)
    parser.add_argument("--max-requests", type=parse_limit, default=None)
    parser.add_argument("--interval", type=float, default=1)
    parser.add_argument("--timeout", type=float, default=20)
    parser.add_argument("--retry-failed", action="store_true", help="실패 이력 URL도 명시적으로 재시도")
    parser.add_argument("--verify-cache", action="store_true", help="기존 파일의 전체 SHA-256 재검사")
    parser.add_argument("--export-interval", type=float, default=300,
                        help="전체 상품 매핑 저장 주기(초), 기본 300; 0은 종료 시만 저장")
    args = parser.parse_args()
    fetcher = ImageFetcher(args.interval, args.timeout)
    try:
        report = localize_images(args.products, args.output or args.products.parent / "media", fetcher,
                                 args.max_requests, cache_dir=args.cache_dir,
                                 retry_failed=args.retry_failed, verify_cache=args.verify_cache,
                                 export_interval=args.export_interval)
    finally:
        fetcher.close()
    import json
    print(json.dumps(report, ensure_ascii=False))
    return 0 if report["result"] == "completed" else 2


if __name__ == "__main__":
    raise SystemExit(main())
