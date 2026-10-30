"""Download observed product galleries; preserve identity, bytes and resume history."""
import argparse
import hashlib
import io
import json
import math
import queue
import re
import threading
import time
from collections import deque
from contextlib import ExitStack
from uuid import uuid4
from pathlib import Path
from urllib.parse import parse_qsl, urlencode, urlparse

import requests
from requests.adapters import HTTPAdapter
from PIL import Image, UnidentifiedImageError

from .core import utc_now, limit_reached, parse_limit
from .storage import atomic_json, append_record, history, images_pruned, link_file, read_jsonl, write_jsonl
from .validation import cached_valid, stored_metadata, verified_image
from .image_store import object_key
from .media_resume import ValidationMemo, download_history
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
# GIVE_UP_AFTER, permanent_failure, the retry passes, the failure rate and storage_unverified;
# fixed per run in image_settings.json (crawler-optimization/image-retry.md).
RETRY_POLICY_VERSION = 2
RETRY_WAITS = (60.0, 300.0)  # seconds before each extra pass over this run's transient failures
FAILURE_LIMIT = 0.01         # transient and internal failures left among the run's unique image URLs
FAILURE_MIN_SAMPLE = 50      # fewer URLs: any such failure leaves the step partial
EXTERNAL_ALERT_LIMIT = 0.05  # images the site no longer serves: recorded, an alert above this rate
UNFINISHED = {"pending", "budget_exhausted", "stopped_after_block"}  # never tried: the step is partial
# Image key rules, fixed per run in image_settings.json with the replacement sample
# (crawler-optimization/image-cache.md, stage 2). 2: catalog images are matched without _v.
IMAGE_KEY_VERSION = 2
REPLACEMENT_SAMPLE = 1000         # images matched without _v fetched again per run to catch replacements
REPLACEMENT_ALERT_LIMIT = 0.01    # share of the checked sample that turned out another picture
REENCODE_LIMIT = 2.0              # mean difference (0-255) of 32x32 grayscale copies of a re-encoding
MATCH_FIELDS = ("matched_url", "identity", "replaced_sha256")  # how an image was matched, run journal only
CATALOG_IMAGE = re.compile(r"/catalog-image/\d{3}/\d{3}/\d{3}/[0-9a-f]{32}\.(?:jpg|jpeg|png|webp|gif)")


def image_identity(url: str) -> str | None:
    """Path and query without _v of an img.danuri.io catalog image; None for any other URL.

    _v is when the site last re-published the picture: it changes between runs and may
    re-encode the same picture. Other hosts keep the whole URL as the key (names may be reused)."""
    parsed = urlparse(url)
    if parsed.hostname != "img.danuri.io" or not CATALOG_IMAGE.fullmatch(parsed.path):
        return None
    query = sorted((k, v) for k, v in parse_qsl(parsed.query, keep_blank_values=True) if k != "_v")
    return parsed.path + ("?" + urlencode(query) if query else "")


def same_picture(old: bytes, new: bytes) -> bool:
    """A re-encoding: same format and pixel size, and small grayscale copies nearly equal."""
    with Image.open(io.BytesIO(old)) as first, Image.open(io.BytesIO(new)) as second:
        if first.format != second.format or first.size != second.size:
            return False
        small = [image.convert("L").resize((32, 32), Image.Resampling.BOX).tobytes() for image in (first, second)]
    return sum(abs(a - b) for a, b in zip(*small)) / len(small[0]) <= REENCODE_LIMIT


class ImageTooLarge(ValueError):
    """The response is bigger than max_bytes; the same URL answers the same next time."""


def failure_kind(entry: dict) -> str:
    """blocked (403/429), external (the site does not serve a usable image), internal (this
    collector could not use what it got) or transient (worth another try in this run).

    Read from a recorded failure, so failures kept from earlier attempts are classified alike."""
    code = entry.get("http_status")
    if code in {403, 429}:
        return "blocked"
    if code is not None and 400 <= code < 500 and code != 408:
        return "external"
    kind, message = entry.get("error_type"), entry.get("error") or ""
    if (kind in {"UnidentifiedImageError", "ImageTooLarge", "UnsupportedImageFormat"}
            or "exceeds max_bytes" in message or "unsupported downloaded image format" in message):
        return "external"
    if kind == "ValueError":  # an invalid URL, an unconfirmed checksum: not the site's answer
        return "internal"
    return "transient"


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
        self.cancel = None  # an ImageStep's event: the interval wait ends early and nothing is sent

    def fetch(self, url):
        parsed = urlparse(url)
        if parsed.scheme not in {"https", "http"} or not parsed.netloc or parsed.username or parsed.password:
            raise ValueError("invalid image URL")
        if self.last_start is not None:
            delay = max(0, self.interval - (self.clock() - self.last_start))
            if self.cancel is None:
                self.sleep(delay)
            elif self.cancel.wait(delay):
                raise StepCancelled()
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
                    raise ImageTooLarge("image response exceeds max_bytes")
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
                    object_store=None, import_from=(), identity_keys=False, replacement_sample=0) -> dict:
    """cache_dir: a local blob store shared by runs (saved-run tools).
    index_dir: the collector CLI's shared download history without image bytes. A URL whose
    last verified download is already in object_storage (HEAD size and sha256 metadata) is
    recorded as status "stored" and not downloaded; without object_store everything downloads.
    retry_transient: retry a failed URL once per run unless it failed permanently
    GIVE_UP_AFTER times in a row.
    import_from: other runs' media directories whose downloads seed a new index.
    identity_keys: with object_store, a catalog image whose URL differs only in _v stands for the
    earlier download; replacement_sample of those are fetched again (image_identity).

    Batch order: every product is checked and every URL judged before the first download."""
    step = ImageStep(destination, fetcher, max_requests, progress, cache_dir, retry_failed=retry_failed,
                     verify_cache=verify_cache, progress_every=progress_every, export_interval=export_interval,
                     retry_transient=retry_transient, index_dir=index_dir, object_store=object_store,
                     import_from=import_from, identity_keys=identity_keys,
                     replacement_sample=replacement_sample).open()
    completed = False
    try:
        report = step.run_batch(Path(products_path))
        completed = True
        return report
    finally:
        step.close(completed)


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


def scan_products(path: Path):
    """(rows, digest): products of a jsonl file one at a time, and the sha256 of the bytes read so far."""
    digest = hashlib.sha256()

    def rows():
        with Path(path).open('rb') as stream:
            for line in stream:
                digest.update(line)
                if line.strip():
                    yield json.loads(line.decode('utf-8-sig'))
    return rows(), digest


def check_product(product: dict) -> None:
    if not all(re.fullmatch(r"[A-Za-z0-9_-]+", str(product.get(k, ""))) for k in ("source", "product_id")):
        raise ValueError("unsafe source/product_id")
    for category in product.get('categories') or []:
        if not isinstance(category, dict) or not isinstance(category.get('category_id'), str) or not re.fullmatch(r'[0-9]+', category['category_id']):
            raise ValueError('category_id must be a numeric string')


class StepCancelled(Exception):
    """The image step was asked to stop between URLs (Ctrl+C or an error in the collector)."""


class ImageStep:
    """The image step of one run: judge each URL (history, storage, give-up), download the rest.

    Batch (localize_images, --image-overlap off): run_batch() checks every product and judges
    every URL before the first download, as the step always did. Overlap: begin() before details,
    then plan() and drain() per product while details are still fetched, and finish() once
    products.jsonl is exported. One thread uses a step at a time; cancel stops it between URLs."""

    def __init__(self, destination: Path, fetcher, max_requests=None, progress=None,
                 cache_dir: Path | None = None, *, retry_failed=False, verify_cache=False,
                 progress_every=25, export_interval=300.0, retry_transient=False,
                 index_dir: Path | None = None, object_store=None, import_from=(),
                 identity_keys=False, replacement_sample=0):
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
        if type(replacement_sample) is not int or replacement_sample < 0:
            raise ValueError("replacement_sample must be a nonnegative integer")
        self.destination = Path(destination)
        self.store = Path(cache_dir) if cache_dir is not None else self.destination
        self.fetcher, self.max_requests, self.progress = fetcher, max_requests, progress
        self.retry_failed, self.verify_cache, self.progress_every = retry_failed, verify_cache, progress_every
        self.export_interval, self.retry_transient = export_interval, retry_transient
        self.index_dir, self.object_store, self.import_from = index_dir, object_store, import_from
        # Matching without _v needs storage: an image is only reused when its object is there.
        self.identity_keys = identity_keys and object_store is not None
        self.replacement_sample = replacement_sample
        self.index = None
        self.stack = ExitStack()
        self.cancel = threading.Event()
        self.report, self.prepared = None, False  # set by begin(); a stop may come before it ends

    def open(self):
        stack = self.stack
        try:
            for root in sorted({self.store.resolve(), self.destination.resolve()}, key=str):
                stack.enter_context(writer_lock(root / '.image-download.lock'))
            if self.index_dir is not None:
                self.index = Path(self.index_dir)
                self.index.mkdir(parents=True, exist_ok=True)
                stack.enter_context(writer_lock(self.index / '.lock'))
                if not (self.index / "image_downloads.jsonl").exists():
                    seed_index(self.index / "image_downloads.jsonl", self.import_from, self.destination, stack)
            self.memo = stack.enter_context(ValidationMemo(self.store, verify_all=self.verify_cache))
        except BaseException:
            stack.close()
            raise
        return self

    def close(self, completed=False):
        """Release the locks; a completed step first compacts the shared history."""
        try:
            if completed and self.index is not None:
                compact_journal(self.index / "image_downloads.jsonl")
        finally:
            self.stack.close()

    def run_batch(self, products_path: Path) -> dict:
        # One pass for the URL list and the input hash; products are read again when the manifest is written.
        rows, digest = scan_products(products_path)
        urls, product_count, without_gallery = {}, 0, 0
        for product in rows:
            check_product(product)
            plan = product_images(product)
            product_count += 1
            without_gallery += not plan
            urls.update(dict.fromkeys(e["url"] for e in plan))
        self.begin(list(urls), product_count, without_gallery, digest.hexdigest(), products_path)
        try:
            # Import old successful history once. A valid prior mapping is never
            # replaced with pending merely because its URL has not been visited yet.
            for position, url in enumerate(urls, 1):
                self.judge(url)
                if position % 1000 == 0:
                    self.save_progress('checking_cache')
            self.prepared = True
            self.save_progress('downloading')
            self.drain()
            self.settle()
        finally:
            self.end()
        return self.report

    def begin(self, urls=(), product_count=0, without_gallery=0, input_hash=None, products_path=None):
        """State of this run's step. Overlap starts empty and publishes a report without finished_at."""
        destination, store = self.destination, self.store
        if images_pruned(destination) or images_pruned(store):
            raise ValueError(f"사진 파일을 정리한(prune) 실행에는 이미지 단계를 다시 돌릴 수 없습니다: {destination}")
        destination.mkdir(parents=True, exist_ok=True)
        # One blob store per cache; output only contains named product links.
        (store / "images").mkdir(parents=True, exist_ok=True)
        self.journal = store / "image_downloads.jsonl"
        self.previous = download_history(self.journal)
        # Import old destination history without modifying existing data.
        self.legacy = (download_history(destination / "image_downloads.jsonl")
                       if store.resolve() != destination.resolve() else self.previous)
        self.shared_journal = self.index / "image_downloads.jsonl" if self.index is not None else None
        self.shared = download_history(self.shared_journal) if self.index is not None else {}
        self.by_identity = {}  # identity -> newest real download; matches made in a run are never added
        if self.identity_keys:
            for entry in self.shared.values():
                self.remember(success_part(entry))
        self.current = {url: {"url": url, "status": "pending", "local_path": None} for url in urls}
        self.product_count, self.without_gallery = product_count, without_gallery
        self.input_hash, self.products_path = input_hash, products_path
        report = {"started_at": utc_now(), "product_count": product_count, "unique_urls": len(self.current),
                  "requests": 0, "cached": 0, "downloaded": 0, "failed": 0, "budget_exhausted": 0}
        self.stopped = False
        self.run_id = str(uuid4())
        report.update(run_id=self.run_id, input_sha256=input_hash, skipped_failed=0, gave_up=0,
                      full_exports=0, retry_failed=self.retry_failed, retry_transient=self.retry_transient,
                      index=str(self.index) if self.index is not None else None,
                      object_store=self.object_store is not None,
                      stored=0, store_checks=0, store_missing=0, store_mismatch=0, store_errors=0,
                      stored_by_identity=0)
        self.report = report
        self.store_errors_in_row = 0
        self.storage_checks = {}  # url -> found/missing/mismatch/error, this attempt's HEAD answers
        # Retries belong to this run directory: its attempts (run ids) and the tries per URL.
        path = destination / 'image_retry.json'
        state = json.loads(path.read_text(encoding='utf-8')) if path.is_file() else {}
        if state.get('retry_policy_version') != RETRY_POLICY_VERSION:
            state = {'retry_policy_version': RETRY_POLICY_VERSION, 'runs': [], 'rounds': {}, 'ready_at': {}}
        state.setdefault('runs', []).append(self.run_id)
        atomic_json(path, state)
        self.retry_state, self.settled = state, False
        # Without storage settings no image may be "stored": before the pipeline loads it,
        # a stored image would arrive as an image without bytes.
        self.store_configured = self.object_store is not None
        self.queue = deque()
        self.prepared = False  # every URL judged and the input known: a manifest can be written
        self.judged = self.downloads = 0
        self.last_export = time.monotonic()
        if products_path is None:
            # Overlap: a finished report left by an earlier attempt of this run is replaced at once.
            self.save_progress('overlap')

    def plan(self, product: dict) -> None:
        """Overlap: judge the product's URLs not seen yet; the ones to download wait for drain()."""
        check_product(product)
        images = product_images(product)
        self.product_count += 1
        self.without_gallery += not images
        self.report["product_count"] = self.product_count
        for entry in images:
            url = entry["url"]
            if url in self.current:
                continue
            self.check_cancel()
            self.current[url] = {"url": url, "status": "pending", "local_path": None}
            self.report["unique_urls"] = len(self.current)
            self.judge(url)
            self.judged += 1
            if self.judged % 1000 == 0:
                self.save_progress('overlap')

    def finish(self, products_path: Path, expected_sha256: str) -> dict:
        """Overlap: after every product was planned and drained and products.jsonl was exported.

        The manifest is published only if the exported bytes are the products that were handed over."""
        self.products_path, self.input_hash = Path(products_path), expected_sha256
        self.report["input_sha256"] = expected_sha256
        self.prepared = True
        try:
            self.settle()
        finally:
            self.end()
        return self.report

    def abort(self) -> None:
        """Overlap stopped before every product was planned: no manifest, the report stays unfinished."""
        if self.report is not None:
            self.save_progress('overlap_interrupted')

    def check_cancel(self):
        if self.cancel.is_set():
            raise StepCancelled()

    def in_storage(self, meta, url=None):
        """Ask object storage; the answer per URL (found/missing/mismatch/error) is kept for settle()."""
        state = self.storage_state(meta)
        if url is not None and state != 'not_checked':
            self.storage_checks[url] = state
        return state == 'found'

    def storage_state(self, meta):
        report = self.report
        if self.object_store is None:
            # Configured but given up after repeated errors: the object is unknown, not missing.
            return 'error' if self.store_configured else 'not_checked'
        report['store_checks'] += 1
        try:
            found = self.object_store.head(object_key(meta))
        except Exception as exc:  # any client failure only means "download again"
            report['store_errors'] += 1
            report['store_last_error'] = f"{type(exc).__name__}: {exc}"[:300]
            self.store_errors_in_row += 1
            if self.store_errors_in_row >= STORE_ERROR_LIMIT:
                self.object_store = None
                report['store_disabled'] = 'consecutive_errors'
            return 'error'
        self.store_errors_in_row = 0
        if found is None:
            report['store_missing'] += 1
            return 'missing'
        if found.get('bytes') != meta['bytes'] or found.get('sha256') != meta['sha256']:
            report['store_mismatch'] += 1
            return 'mismatch'
        return 'found'

    def record(self, entry):
        # Success/failure is published before a lightweight progress marker.
        append_record(self.journal, entry)
        if self.shared_journal is not None:
            # Paths belong to this run's media/; the index keeps what can be asked of storage.
            self.shared[entry['url']] = {k: v for k, v in entry.items() if k != 'local_path'}
            append_record(self.shared_journal, self.shared[entry['url']])
            if self.identity_keys and entry.get('status') == 'downloaded':
                self.remember(self.shared[entry['url']])

    def remember(self, entry):
        key = image_identity(entry['url']) if stored_metadata(entry) and entry.get('url') else None
        if key and (entry.get('downloaded_at') or '') >= (self.by_identity.get(key, {}).get('downloaded_at') or ''):
            self.by_identity[key] = entry

    def match_identity(self, url):
        """Same catalog path and query but another _v: the earlier download stands if storage has it.

        The match is kept in this run's journal only (with matched_url); the shared history keeps
        real downloads, so aliases do not pile up per _v."""
        key = image_identity(url) if self.identity_keys else None
        match = self.by_identity.get(key) if key else None
        meta = stored_metadata(match)
        if not meta or match['url'] == url or not self.in_storage(meta, url):
            return False
        self.current[url] = dict(url=url, status='stored', **meta, local_path=None,
                                 downloaded_at=match.get('downloaded_at'), checked_at=utc_now(),
                                 run_id=self.run_id, matched_url=match['url'], identity=key)
        append_record(self.journal, self.current[url])
        self.report['stored'] += 1
        self.report['stored_by_identity'] += 1
        return True

    def judge(self, url):
        memo, store, destination, report = self.memo, self.store, self.destination, self.report
        current, shared, index = self.current, self.shared, self.index
        entry = self.previous.get(url, {})
        valid = memo.valid(entry)
        if not valid and entry.get('status') not in {'downloaded', 'stored'}:
            # A failed retry keeps the last verified download of this URL usable.
            kept = success_part(entry)
            if kept and memo.valid(kept):
                entry, valid = kept, True
        if not valid and store.resolve() != destination.resolve():
            old = self.legacy.get(url, {})
            if cached_valid(old, destination):
                link_file(destination / old['local_path'], store / old['local_path'], old['sha256'])
                entry = old.copy()
                valid = memo.valid(entry)
                if valid:
                    append_record(self.journal, entry)
        if valid:
            current[url] = entry.copy()
            report['cached'] += 1
            if index is not None and url not in shared:
                # A run started before the index existed: its downloads become known to later runs.
                shared[url] = {k: v for k, v in entry.items() if k not in {'local_path', 'last_success'}}
                append_record(self.shared_journal, shared[url])
        elif self.store_configured and entry.get('status') == 'stored' and stored_metadata(entry):
            current[url] = entry.copy()  # this run already found it in storage (resume)
            report['stored'] += 1
            report['stored_by_identity'] += bool(entry.get('matched_url'))
        elif entry.get('status') == 'failed' and not self.retry_failed and (
                not self.retry_transient or entry.get('gave_up') or index is not None):
            # With an index, a failure of this run is retried in the next run, not on resume.
            current[url] = entry.copy()
            report['skipped_failed'] += 1
            report['gave_up'] += bool(entry.get('gave_up'))
        elif index is not None:
            known = shared.get(url, {})
            success = success_part(known)
            meta = stored_metadata(success)
            if meta and self.in_storage(meta, url):
                current[url] = dict(url=url, status='stored', **meta, local_path=None,
                                    downloaded_at=success.get('downloaded_at'),
                                    checked_at=utc_now(), run_id=self.run_id)
                append_record(self.journal, current[url])
                report['stored'] += 1
            elif known.get('status') == 'failed' and known.get('gave_up') and not self.retry_failed:
                current[url] = {k: v for k, v in known.items() if k != 'last_success'}
                current[url]['local_path'] = None
                report['skipped_failed'] += 1
                report['gave_up'] += 1
            elif not self.match_identity(url):
                self.queue.append(url)
        else:
            self.queue.append(url)

    def drain(self):
        """Download the waiting URLs. After a 403/429 or the budget the rest are only marked."""
        report = self.report
        while self.queue:
            url = self.queue[0]
            if self.stopped or limit_reached(report['requests'], self.max_requests):
                self.queue.popleft()
                self.current[url]['status'] = 'stopped_after_block' if self.stopped else 'budget_exhausted'
                if not self.stopped:
                    report['budget_exhausted'] += 1
                continue
            self.check_cancel()
            self.download(url)
            self.queue.popleft()
            self.downloads += 1
            export_at = time.monotonic()
            if (self.products_path is not None and self.export_interval
                    and export_at - self.last_export >= self.export_interval):
                self.checkpoint()
                self.last_export = export_at
            if self.downloads % self.progress_every == 0:
                self.save_progress('downloading' if self.products_path is not None else 'overlap')

    def download(self, url, retry_round=0):
        report, store, memo, fetcher = self.report, self.store, self.memo, self.fetcher
        report['requests'] += 1
        attempted_at = utc_now()
        if retry_round:
            before = self.current[url]  # this run's failure: its attempts and last success go on
        else:
            before = (self.shared if self.index is not None else self.previous).get(url, {})
        attempts = int(before.get('attempts', 0)) + 1
        try:
            data = fetcher.fetch(url)
            info = verified_image(data)
            relative = self.keep(data, info)
            entry = dict(url=url, status='downloaded', local_path=relative,
                         downloaded_at=utc_now(), run_id=self.run_id, attempts=attempts,
                         response_headers=getattr(fetcher, 'last_headers', None), **info)
            if not memo.valid(entry):
                raise ValueError('saved image checksum unconfirmed')
            report['downloaded'] += 1
        except StepCancelled:
            report['requests'] -= 1  # stopped in the request interval: nothing was sent
            raise
        except (requests.RequestException, ValueError, OSError) as exc:
            response = exc.response if isinstance(exc, requests.HTTPError) else None
            status_code = response.status_code if response is not None else None
            permanent = permanent_failure(exc)
            streak = int(before.get('consecutive_permanent_failures', 0)) + 1 if permanent else 0
            entry = dict(url=url, status='failed', local_path=None, error=str(exc),
                         error_type=type(exc).__name__, http_status=status_code,
                         attempted_at=attempted_at, failed_at=utc_now(), run_id=self.run_id, attempts=attempts,
                         permanent=permanent, consecutive_permanent_failures=streak,
                         gave_up=streak >= GIVE_UP_AFTER, last_success=success_part(before))
            report['failed'] += 1
            if status_code in {403, 429}:
                self.stopped = True
                report['stop_reason'] = f'http_{status_code}'
        if retry_round:
            entry['retry_round'] = retry_round
        check = self.storage_checks.get(url) or (before.get('storage_check') if retry_round else None)
        if check:
            entry['storage_check'] = check
        self.record(entry)
        self.current[url] = entry

    def keep(self, data, info):
        """Write verified bytes into the blob store under their sha256; the relative path."""
        relative = f"images/{info['sha256']}.{info['extension']}"
        target = self.store / relative
        if not self.memo.valid({'status': 'downloaded', 'local_path': relative, **info}):
            temporary = target.with_suffix(target.suffix + '.tmp')
            temporary.write_bytes(data)
            temporary.replace(target)
        return relative

    def settle(self):
        """Normal end of the downloads, before the final manifest: retry this run's transient
        failures, then let an unconfirmed but earlier stored image stand in for a failed one.

        Never on an exception or a stop: end() then publishes what there is."""
        self.retry_passes()
        self.replacement_check()
        self.unverified_stand_ins()
        self.settled = True

    def retry_passes(self):
        """Up to len(RETRY_WAITS) extra tries per URL in this run directory, kept across resumes.

        image_retry.json counts the tries before each request, so a crash costs a try, never adds one."""
        report, current = self.report, self.current
        path, state = self.destination / 'image_retry.json', self.retry_state
        tries, ready = state['rounds'], state['ready_at']
        summary = report.setdefault('retry', {'waits': list(RETRY_WAITS), 'passes': []})
        for number, wait in enumerate(RETRY_WAITS, 1):
            targets = [url for url, entry in current.items()
                       if entry.get('status') == 'failed' and failure_kind(entry) == 'transient'
                       and not entry.get('gave_up') and tries.get(url, 0) < number and self.own(entry)]
            if self.stopped or limit_reached(report['requests'], self.max_requests):
                break
            if not targets:
                continue  # a resume after a try of this pass: only later passes are left
            key = str(number)
            if key not in ready:
                ready[key] = time.time() + wait
                atomic_json(path, state)
            if self.cancel.wait(max(0.0, ready[key] - time.time())):
                raise StepCancelled()
            done = dict(round=number, targets=len(targets), attempted=0, recovered=0)
            summary['passes'].append(done)
            self.save_progress('retrying')
            for url in targets:
                self.check_cancel()
                if self.stopped or limit_reached(report['requests'], self.max_requests):
                    done['stopped'] = report.get('stop_reason') or 'budget'
                    break
                tries[url] = number
                atomic_json(path, state)
                self.download(url, retry_round=number)
                done['attempted'] += 1
                done['recovered'] += current[url]['status'] == 'downloaded'

    def replacement_check(self):
        """Fetch a sample of the images matched without _v again. Same bytes or a re-encoding of
        the same picture keep the earlier sha256; another picture is recorded as this run's
        download (new sha256) and counted, with an alert above REPLACEMENT_ALERT_LIMIT.

        Half the sample is the matches confirmed longest ago, half random. The targets and the
        verdicts are kept in image_replacement.json, so a resume goes on with the same sample.
        A failed check leaves the image stored and is not an image failure; 403/429 stops as usual."""
        if not self.identity_keys or not self.replacement_sample:
            return
        report, path = self.report, self.destination / 'image_replacement.json'
        state = json.loads(path.read_text(encoding='utf-8')) if path.is_file() else {}
        if state.get('image_key_version') != IMAGE_KEY_VERSION or state.get('size') != self.replacement_sample:
            state = dict(image_key_version=IMAGE_KEY_VERSION, size=self.replacement_sample,
                         targets=self.sample_targets(), done={})
            if state['targets']:  # nothing matched: no file, a resume finds the same nothing
                atomic_json(path, state)
        summary = report['replacement_check'] = dict(size=self.replacement_sample, targets=len(state['targets']),
                                                     limit=REPLACEMENT_ALERT_LIMIT)
        for url in state['targets']:
            if url in state['done']:
                continue
            self.check_cancel()
            if self.stopped or limit_reached(report['requests'], self.max_requests) or self.object_store is None:
                summary['stopped'] = report.get('stop_reason') or ('budget' if self.object_store else 'store_disabled')
                break
            entry = self.current.get(url) or {}
            if entry.get('status') == 'downloaded' and entry.get('replaced_sha256'):
                verdict = 'changed'  # journaled before a stop, the verdict was not saved yet
            elif entry.get('matched_url') and entry.get('status') == 'stored':
                verdict = self.check_again(url, entry)
            else:
                verdict = 'skipped'
            if verdict is None:
                summary['stopped'] = report['stop_reason']
                break
            state['done'][url] = verdict
            atomic_json(path, state)
        verdicts = list(state['done'].values())
        counts = {verdict: verdicts.count(verdict) for verdict in ('same', 'reencoded', 'changed', 'failed', 'skipped')}
        checked = counts['same'] + counts['reencoded'] + counts['changed']
        summary.update(counts, checked=checked, rate=round(counts['changed'] / checked, 6) if checked else 0.0)

    def sample_targets(self):
        matched = [url for url, entry in self.current.items()
                   if entry.get('status') == 'stored' and entry.get('matched_url')]

        def confirmed(url):
            entry = self.current[url]
            return self.shared.get(entry['matched_url'], {}).get('confirmed_at') or entry.get('downloaded_at') or ''
        oldest = sorted(matched, key=lambda url: (confirmed(url), url))[:self.replacement_sample // 2]
        chosen = set(oldest)
        rest = sorted((url for url in matched if url not in chosen),
                      key=lambda url: hashlib.sha256(f"{self.run_id}:{url}".encode()).hexdigest())
        return oldest + rest[:self.replacement_sample - len(oldest)]

    def check_again(self, url, entry):
        report = self.report
        report['requests'] += 1
        try:
            data = self.fetcher.fetch(url)
            info = verified_image(data)
            if info['sha256'] == entry['sha256']:
                verdict = 'same'
            else:
                verdict = 'reencoded' if same_picture(self.earlier_bytes(entry), data) else 'changed'
        except StepCancelled:
            report['requests'] -= 1
            raise
        except Exception as exc:  # a check that cannot finish only leaves the earlier image
            response = exc.response if isinstance(exc, requests.HTTPError) else None
            if response is not None and response.status_code in {403, 429}:
                self.stopped = True
                report['stop_reason'] = f'http_{response.status_code}'
                return None  # blocked: not a verdict, a resume asks again
            return 'failed'
        if verdict != 'changed':
            # The earlier bytes stand either way; only when they were last confirmed changes.
            matched = self.shared.get(entry['matched_url'])
            if matched is not None:
                matched['confirmed_at'] = utc_now()
                append_record(self.shared_journal, matched)
            return verdict
        replaced = dict(url=url, status='downloaded', local_path=self.keep(data, info), downloaded_at=utc_now(),
                        run_id=self.run_id, attempts=1, response_headers=getattr(self.fetcher, 'last_headers', None),
                        replaced_sha256=entry['sha256'], matched_url=entry['matched_url'], **info)
        if not self.memo.valid(replaced):
            return 'failed'
        self.record(replaced)
        self.current[url] = replaced
        report['downloaded'] += 1
        report['stored'] -= 1
        report['stored_by_identity'] -= 1
        return verdict

    def earlier_bytes(self, entry):
        """The bytes the match stands for: this store's copy, or the object in storage."""
        local = self.store / f"images/{entry['sha256']}.{entry['extension']}"
        data = local.read_bytes() if local.is_file() else self.object_store.get(object_key(entry))
        if hashlib.sha256(data).hexdigest() != entry['sha256']:
            raise ValueError('earlier image bytes do not match their sha256')
        return data

    def own(self, entry):
        """A failure of this run directory, not one a shared cache keeps from another run."""
        return entry.get('run_id') in self.retry_state['runs']

    def unverified_stand_ins(self):
        """With storage checks on, a URL whose last download is known, whose HEAD failed (or was
        given up) and whose new download still failed is handed over as that earlier stored image.

        Only in the manifest: the history keeps the failure and the last success. Ingestion checks
        the object itself and fails just that slot if it is gone."""
        if not self.store_configured or self.stopped:
            return
        count = 0
        for url, entry in self.current.items():
            if (entry.get('status') != 'failed' or entry.get('storage_check') != 'error'
                    or failure_kind(entry) != 'transient' or not self.own(entry)):
                continue
            success = success_part(entry)
            meta = stored_metadata(success)
            if not meta:
                continue
            self.current[url] = dict(url=url, status='stored', **meta, local_path=None,
                                     downloaded_at=success.get('downloaded_at'), run_id=self.run_id,
                                     storage_unverified=True, unverified_reason=entry.get('error'),
                                     failed_at=entry.get('failed_at'))
            count += 1
        self.report['storage_unverified'] = count

    def failure_summary(self):
        """Final state of every unique image URL of the input; the request counters are attempts."""
        report, current = self.report, self.current
        kinds = {'transient': 0, 'internal': 0, 'external': 0, 'blocked': 0}
        unfinished, samples = 0, []
        for url, entry in current.items():
            status = entry.get('status')
            if status in UNFINISHED:
                unfinished += 1
            elif status == 'failed':
                kind = failure_kind(entry)
                kinds[kind] += 1
                if kind != 'external' and len(samples) < 50:
                    samples.append(url)
        total = len(current)
        counted = kinds['transient'] + kinds['internal'] + kinds['blocked']
        rate = counted / total if total else 0.0
        passed = (counted == 0) if total < FAILURE_MIN_SAMPLE else rate <= FAILURE_LIMIT
        external_rate = kinds['external'] / total if total else 0.0
        blocked_by = [reason for reason, hit in (
            ('not_prepared', not self.prepared), (report.get('stop_reason') or 'blocked', self.stopped),
            # A 403/429 kept from before a resume was never tried again: still blocked.
            ('blocked_failures', kinds['blocked'] > 0 and not self.stopped), ('unfinished', unfinished > 0),
            # Cancelled or failed before the retries ended: what there is gets published, not passed.
            ('interrupted', self.prepared and not self.settled)) if hit]
        summary = dict(unique_urls=total, failed=counted, rate=round(rate, 6), limit=FAILURE_LIMIT,
                       min_sample=FAILURE_MIN_SAMPLE, passed=passed and not blocked_by, by_kind=kinds,
                       unfinished=unfinished, blocked_by=blocked_by,
                       external_rate=round(external_rate, 6), external_alert_limit=EXTERNAL_ALERT_LIMIT,
                       storage_unverified=report.get('storage_unverified', 0), failed_url_sample=samples)
        alerts = ([{'signal': 'image_permanent_failures', 'count': kinds['external'], 'total': total,
                    'rate': round(external_rate, 6), 'limit': EXTERNAL_ALERT_LIMIT}]
                  if external_rate > EXTERNAL_ALERT_LIMIT else [])
        check = report.get('replacement_check') or {}
        if check.get('rate', 0) > REPLACEMENT_ALERT_LIMIT:
            alerts.append({'signal': 'image_replaced_at_same_path', 'count': check['changed'],
                           'total': check['checked'], 'rate': check['rate'], 'limit': REPLACEMENT_ALERT_LIMIT})
        return summary, alerts

    def end(self):
        self.report['finished_at'] = utc_now()
        # Complete export at the end/interruption, plus optional timed exports.
        # Hard termination replays the per-URL journal; the cursor is not proof.
        if self.prepared:
            self.checkpoint()
        else:
            self.report['result'] = 'partial'
        self.save_progress('cache_check_interrupted' if not self.prepared else 'stopped' if self.stopped else 'exported')

    def save_progress(self, phase):
        memo, report = self.memo, self.report
        memo.flush()
        report.update(updated_at=utc_now(), validation_full_hashes=memo.full_hashes,
                      validation_stat_reuses=memo.stat_reuses, phase=phase)
        processed = (report['cached'] + report['stored'] + report['downloaded'] + report['failed']
                     + report['skipped_failed'] + report['budget_exhausted'])
        state = dict(contract_version=1, run_id=self.run_id, input_sha256=self.input_hash,
                     resume_strategy='url_history_and_verified_file_signature',
                     journal_bytes=self.journal.stat().st_size if self.journal.exists() else 0,
                     processed=processed, total=len(self.current), **{'report': dict(report)})
        atomic_json(self.destination / 'image_resume.json', state)
        atomic_json(self.destination / 'download_report.json', report)
        if self.progress:
            self.progress(dict(report, processed=processed, total=len(self.current)))

    def checkpoint(self):
        # The manifest is written while products.jsonl is read again, one product at a time. The
        # bytes read must be the ones the URL list came from, or the manifest is not published.
        report, current, memo = self.report, self.current, self.memo
        target = self.destination / "products_with_local_images.jsonl"
        counts = dict(products_with_local_images=0, products_complete=0, products_with_stored_images=0,
                      products_affected=0, products_without_delivered_images=0)

        def manifest():
            products, reread = scan_products(self.products_path)
            for product in products:
                row = self.manifest_row(product)
                counts["products_with_local_images"] += any(e["status"] == "downloaded" for e in row["images"])
                counts["products_complete"] += row["image_download_status"] == "completed"
                counts["products_with_stored_images"] += any(e["status"] == "stored" for e in row["images"])
                counts["products_affected"] += row["image_download_status"] == "partial"
                counts["products_without_delivered_images"] += bool(row["images"]) and not any(
                    e["status"] in DELIVERED for e in row["images"])
                yield row
            # Raised here, before write_jsonl replaces the published file.
            if reread.hexdigest() != self.input_hash:
                raise ValueError("products.jsonl changed during the image step")
            # Creating a hard link changes NTFS ChangeTime on the source blob.
            # Refresh the proof only after links are complete, before publishing.
            checked = set()
            for entry in current.values():
                if entry['status'] != 'downloaded' or entry['local_path'] in checked:
                    continue
                if not memo.valid(entry):
                    raise ValueError('image changed during category linkage')
                checked.add(entry['local_path'])

        try:
            write_jsonl(target, manifest())
        except BaseException:
            target.with_suffix(target.suffix + ".tmp").unlink(missing_ok=True)
            raise
        report["full_exports"] += 1
        report.update(**counts, products_without_gallery=self.without_gallery,
                      unique_files=len({e["sha256"] for e in current.values() if e.get("status") == "downloaded"}),
                      updated_at=utc_now())
        # A few failed images or galleries the site has none of leave the step completed; the
        # quality gate judges missing galleries. Blocks, budget stops and too many failures do not.
        report["failures"], report["alerts"] = self.failure_summary()
        report["result"] = "completed" if self.product_count and report["failures"]["passed"] else "partial"
        atomic_json(self.destination / "download_report.json", report)

    def manifest_row(self, product):
        check_product(product)
        current, store, destination = self.current, self.store, self.destination
        images = []
        for ordinal, observed in enumerate(product_images(product), 1):
            if observed["url"] not in current:
                raise ValueError("products.jsonl changed during the image step")
            entry = current[observed["url"]]
            image = {**observed, **entry, "source": product["source"], "product_id": product["product_id"],
                     "filename": None, "local_path": None, "category_paths": {}}
            image.pop("storage_path", None)  # compatibility input must not duplicate output paths
            for key in MATCH_FIELDS:  # journal only, until the pipeline contract (policy 2) wants them
                image.pop(key, None)
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
        return {**row, "schema_version": "1.2", "images": images,
                "image_download_status": "completed" if images and all(e["status"] in DELIVERED for e in images)
                                         else "partial" if images else "gallery_missing"}


GALLERY_FIELDS = ("source", "product_id", "images", "image_urls", "categories")  # what plan() reads


class ImageWorker:
    """Overlap: one thread plans and downloads the galleries the collector hands over, in order.

    The collector (one producer) offers each product once; this thread (one consumer) takes them
    first in, first out, so the requests follow products.jsonl order. The queue is bounded: when
    images fall behind, details wait. digest is the sha256 of the products as products.jsonl
    will hold them, for ImageStep.finish."""

    def __init__(self, step: ImageStep, capacity=10000, poll=0.2):
        self.step, self.poll = step, poll
        self.queue = queue.Queue(capacity)
        self.digest = hashlib.sha256()
        self.error = None
        self.planned_all = False
        self.thread = threading.Thread(target=self._run, name="image-step", daemon=True)
        self.thread.start()

    def _run(self):
        try:
            while not self.step.cancel.is_set():
                try:
                    item = self.queue.get(timeout=self.poll)
                except queue.Empty:
                    continue
                if item is None:
                    self.planned_all = True
                    return
                self.step.plan(item)
                self.step.drain()
        except StepCancelled:
            pass
        except BaseException as exc:  # handed to the collector at its next offer or at close()
            self.error = exc
            self.step.cancel.set()

    def offer(self, product: dict) -> None:
        self.digest.update((json.dumps(product, ensure_ascii=False, allow_nan=False) + "\n").encode("utf-8"))
        self._put({key: product[key] for key in GALLERY_FIELDS if key in product})

    def _put(self, item):
        # Never an unbounded wait: a dead worker would leave a full queue behind.
        while True:
            self._raise()
            try:
                self.queue.put(item, timeout=self.poll)
                return
            except queue.Full:
                continue

    def _raise(self):
        if self.error is not None:
            raise self.error

    def close(self) -> None:
        """Every product was offered: wait until the rest of the queue is planned and downloaded."""
        self._put(None)
        while self.thread.is_alive():
            self.thread.join(self.poll)
        self._raise()

    def stop(self) -> None:
        """Ctrl+C or a collector error: the request in flight finishes, then the thread ends."""
        self.step.cancel.set()
        while self.thread.is_alive():
            self.thread.join(self.poll)


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
