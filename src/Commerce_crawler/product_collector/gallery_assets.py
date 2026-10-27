"""Resume saved Danawa products: detail gallery -> local files -> verified linkage.

Never refetch listings. Full detail evidence and per-image checkpoints persist
under the chosen output. Re-running reuses those successful observations.
"""
from __future__ import annotations

import argparse
import copy
import json
from pathlib import Path

import requests

from .core import BudgetExhausted, RequestClient, payload_hash, utc_now, parse_limit
from .danawa import DanawaAdapter
from .reprocess import apply_detail
from .media import ImageFetcher, localize_images
from .storage import atomic_json, read_jsonl, write_jsonl
from .validation import verify_assets


def enrich_saved_run(run: Path, output: Path, client, progress=None, cache_dir: Path | None = None) -> dict:
    adapter = DanawaAdapter()
    source = read_jsonl(run / "product_raw.jsonl")
    output.mkdir(parents=True, exist_ok=True)
    cache = output / "details"
    cache.mkdir(exist_ok=True)
    if cache_dir is not None:
        (cache_dir / "details").mkdir(parents=True, exist_ok=True)
    # Preserve all category memberships even if input contains repeated product records.
    grouped = {}
    for original in source:
        if original["source"] != "danawa" or not str(original["product_id"]).isascii() or not str(original["product_id"]).isdigit():
            raise ValueError("only Danawa numeric product IDs are supported")
        code = str(original["product_id"])
        if code not in grouped:
            grouped[code] = copy.deepcopy(original)
        else:
            existing = grouped[code]
            for category in original["categories"]:
                if category not in existing["categories"]:
                    existing["categories"].append(category)
            existing["payload"]["listings"].extend(copy.deepcopy(original["payload"]["listings"]))
    raw = list(grouped.values())
    report = {"started_at": utc_now(), "input_run": str(run.resolve()), "products": len(raw),
              "detail_requests": 0, "detail_cached": 0, "errors": [], "result": "partial"}
    stopped = False

    def checkpoint():
        normalized = []
        for record in raw:
            record["payload_hash"] = payload_hash(record["payload"])
            row = adapter.normalize(record)
            row["gallery_status"] = (record["payload"].get("detail") or {}).get("gallery_status", "not_collected")
            normalized.append(row)
        write_jsonl(output / "product_raw.jsonl", raw)
        write_jsonl(output / "products.jsonl", normalized)
        report.update(updated_at=utc_now(), detail_requests=len(client.attempts),
                      with_gallery=sum(bool(p["images"]) for p in normalized), requests=client.attempts)
        report["result"] = "completed" if report["with_gallery"] == len(raw) else "partial"
        atomic_json(output / "gallery_report.json", report)

    for index, record in enumerate(raw, 1):
        code = str(record["product_id"])
        listing = record["payload"]["listings"][0]
        url = f"https://prod.danawa.com/info/?pcode={code}"
        if listing["product_url"] != url:
            raise ValueError(f"saved product URL does not match ID: {code}")
        path = cache / f"{code}.json"
        saved = None
        if path.exists():
            saved = json.loads(path.read_text(encoding="utf-8"))
        elif cache_dir is not None and (cache_dir / "details" / f"{code}.json").exists():
            saved = json.loads((cache_dir / "details" / f"{code}.json").read_text(encoding="utf-8"))
        if saved is not None:
            if saved.get("product_id") != code or saved.get("url") != url:
                raise ValueError(f"detail cache identity mismatch: {code}")
        detail = record["payload"].get("detail") or {}
        html = (saved or {}).get("html") or detail.get("html")
        try:
            if html:
                report["detail_cached"] += 1
            elif stopped:
                record["detail_status"] = "not_requested"
                continue
            else:
                html = client.get(url)
            collected_at = (saved or detail).get("collected_at") or utc_now()
            apply_detail(record, adapter, html, collected_at)
            evidence = {"product_id": code, "url": url, "html": html, "collected_at": collected_at}
            atomic_json(path, evidence)
            if cache_dir is not None:
                atomic_json(cache_dir / "details" / f"{code}.json", evidence)
        except BudgetExhausted:
            record["detail_status"] = "budget_exhausted"
            stopped = True
            report["stop_reason"] = "request_limit"
        except (requests.RequestException, ValueError) as exc:
            record["detail_status"] = "request_failed" if isinstance(exc, requests.RequestException) else "unrecognized"
            record["detail_error"] = str(exc)
            report["errors"].append({"product_id": code, "error": str(exc)})
            if isinstance(exc, requests.HTTPError) and exc.response is not None and exc.response.status_code in {403, 429}:
                stopped = True
                report["stop_reason"] = f"http_{exc.response.status_code}"
        # Persist each detail response immediately; full snapshots every ten products.
        if index % 10 == 0 or index == len(raw):
            checkpoint()
            if progress:
                progress({"stage": "detail", "processed": index, "total": len(raw),
                          "requests": len(client.attempts), "with_gallery": report["with_gallery"]})
    report["finished_at"] = utc_now()
    checkpoint()
    return report


def main():
    parser = argparse.ArgumentParser(description="기존 상품의 상세 갤러리 전체 수집·다운로드·연결 검증")
    parser.add_argument("--run", type=Path, required=True)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--max-detail-requests", type=parse_limit, default=None)
    parser.add_argument("--max-image-requests", type=parse_limit, default=None)
    parser.add_argument("--interval", type=float, default=1)
    parser.add_argument("--timeout", type=float, default=20)
    parser.add_argument("--cache-dir", type=Path, help="기본값은 수집 실행들의 상위 디렉터리/_asset_cache")
    args = parser.parse_args()
    output = args.output or args.run / "gallery_assets"
    cache_dir = args.cache_dir or args.run.parent / "_asset_cache"
    output.mkdir(parents=True, exist_ok=True)
    # A single writer also prevents concurrent runs from requesting the same URL twice.
    lock = output / "pipeline.lock"
    try:
        with lock.open("x", encoding="utf-8") as stream:
            import os
            stream.write(str(os.getpid()))
    except FileExistsError:
        parser.error(f"another writer or stale lock exists: {lock}")
    progress = lambda value: print(json.dumps(value, ensure_ascii=False), flush=True)
    try:
        client = RequestClient(args.max_detail_requests, args.interval, args.timeout)
        try:
            details = enrich_saved_run(args.run, output, client, progress, cache_dir=cache_dir)
        finally:
            client.close()
        if str(details.get("stop_reason", "")).startswith("http_"):
            progress(details)
            return 2
        fetcher = ImageFetcher(args.interval, args.timeout)
        try:
            images = localize_images(output / "products.jsonl", output / "media", fetcher,
                                     args.max_image_requests, progress=progress, cache_dir=cache_dir / "media")
        finally:
            fetcher.close()
        verification = verify_assets(output / "media" / "products_with_local_images.jsonl", output / "media")
        progress({"output": str(output.resolve()), "details": details["result"], "images": images["result"],
                  "verification": verification})
        return 0 if verification["result"] == "completed" and details["result"] == "completed" else 2
    finally:
        lock.unlink()


if __name__ == "__main__":
    raise SystemExit(main())
