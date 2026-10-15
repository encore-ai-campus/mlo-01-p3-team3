"""Download observed product galleries; preserve identity, bytes and resume history."""
import argparse
import hashlib
import math
import re
import time
from pathlib import Path
from urllib.parse import urlparse

import requests
from requests.adapters import HTTPAdapter

from .core import utc_now, limit_reached, parse_limit
from .storage import atomic_json, append_record, history, link_file, read_jsonl, write_jsonl
from .validation import cached_valid, verified_image


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
            return b"".join(chunks)

    def close(self):
        self.session.close()


def localize_images(products_path: Path, destination: Path, fetcher, max_requests=None,
                    progress=None, cache_dir: Path | None = None) -> dict:
    if max_requests is not None and (type(max_requests) is not int or max_requests < 1):
        raise ValueError("max_requests must be positive")
    products = read_jsonl(products_path)
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
    store = cache_dir or destination
    (store / "images").mkdir(parents=True, exist_ok=True)
    journal = store / "image_downloads.jsonl"
    previous = history(journal)
    # Import old destination history without modifying existing data.
    legacy = history(destination / "image_downloads.jsonl") if store != destination else previous
    current = {url: {"url": url, "status": "pending", "local_path": None} for url in urls}
    report = {"started_at": utc_now(), "product_count": len(products), "unique_urls": len(urls),
              "requests": 0, "cached": 0, "downloaded": 0, "failed": 0, "budget_exhausted": 0}
    stopped = False

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
                        link_file(store / entry['local_path'], destination / relative, entry['sha256'])
                        image['category_paths'][code] = relative.as_posix()
                    image.update(filename=name, local_path=next(iter(image['category_paths'].values())))
                images.append(image)
            row = {k: v for k, v in product.items() if k not in {"images", "image_urls", "local_images", "storage_path"}}
            for key in ("provenance", "field_status"):
                if isinstance(row.get(key), dict):
                    row[key] = {k: v for k, v in row[key].items() if k not in {"image_urls", "local_images"}}
            rows.append({**row, "schema_version": "1.2", "images": images,
                         "image_download_status": "completed" if images and all(e["status"] == "downloaded" for e in images)
                                                  else "partial" if images else "gallery_missing"})
        write_jsonl(destination / "products_with_local_images.jsonl", rows)
        report.update(products_with_local_images=sum(any(e["status"] == "downloaded" for e in r["images"]) for r in rows),
                      products_complete=sum(r["image_download_status"] == "completed" for r in rows),
                      products_without_gallery=sum(not plan for plan in plans),
                      unique_files=len({e["sha256"] for e in current.values() if e.get("status") == "downloaded"}),
                      updated_at=utc_now())
        report["result"] = "completed" if products and report["products_complete"] == len(products) else "partial"
        atomic_json(destination / "download_report.json", report)

    try:
        for index, url in enumerate(urls, 1):
            cached = previous.get(url, {})
            if not cached_valid(cached, store) and cached_valid(legacy.get(url, {}), destination):
                cached = legacy[url]
                link_file(destination / cached["local_path"], store / cached["local_path"], cached["sha256"])
            if cached_valid(cached, store):
                entry = cached.copy()
                report["cached"] += 1
            elif stopped or limit_reached(report["requests"], max_requests):
                entry = {"url": url, "status": "stopped_after_block" if stopped else "budget_exhausted", "local_path": None}
                report["budget_exhausted"] += not stopped
            else:
                report["requests"] += 1
                try:
                    data = fetcher.fetch(url)
                    info = verified_image(data)
                    relative = f"images/{info['sha256']}.{info['extension']}"
                    target = store / relative
                    if not cached_valid({"status": "downloaded", "local_path": relative, **info}, store):
                        temporary = target.with_suffix(target.suffix + ".tmp")
                        temporary.write_bytes(data)
                        temporary.replace(target)
                    entry = {"url": url, "status": "downloaded", "local_path": relative, "downloaded_at": utc_now(), **info}
                    report["downloaded"] += 1
                except (requests.RequestException, ValueError, OSError) as exc:
                    entry = {"url": url, "status": "failed", "local_path": None, "error": str(exc)}
                    report["failed"] += 1
                    if isinstance(exc, requests.HTTPError) and exc.response is not None and exc.response.status_code in {403, 429}:
                        stopped = True
                        report["stop_reason"] = f"http_{exc.response.status_code}"
            current[url] = entry
            append_record(journal, entry)
            if index % 25 == 0:
                checkpoint()
                if progress:
                    progress({"processed": index, "total": len(urls), **report})
    finally:
        # Graceful interruption keeps mappings; a hard kill resumes from the journal.
        report["finished_at"] = utc_now()
        checkpoint()
    return report


def main():
    parser = argparse.ArgumentParser(description="확보한 상품 갤러리 다운로드")
    parser.add_argument("--products", type=Path, required=True)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--cache-dir", type=Path)
    parser.add_argument("--max-requests", type=parse_limit, default=None)
    parser.add_argument("--interval", type=float, default=1)
    parser.add_argument("--timeout", type=float, default=20)
    args = parser.parse_args()
    fetcher = ImageFetcher(args.interval, args.timeout)
    try:
        report = localize_images(args.products, args.output or args.products.parent / "media", fetcher,
                                 args.max_requests, cache_dir=args.cache_dir)
    finally:
        fetcher.close()
    import json
    print(json.dumps(report, ensure_ascii=False))
    return 0 if report["result"] == "completed" else 2


if __name__ == "__main__":
    raise SystemExit(main())
