from __future__ import annotations

import argparse
import json
from pathlib import Path
from dataclasses import replace

from .core import RequestClient, collect, load_categories, save_run
from .danawa import DanawaAdapter
from .reprocess import reprocess_run
from .pagination import PaginationTemplates

PROJECT_ROOT = Path(__file__).resolve().parent.parent


def main() -> int:
    parser = argparse.ArgumentParser(description="다나와 카테고리·페이지 수집과 상품 갤러리 보강")
    parser.add_argument("--categories", type=Path, default=PROJECT_ROOT / "config/danawa_categories.json")
    parser.add_argument("--output", type=Path, default=PROJECT_ROOT / "data/danawa")
    parser.add_argument("--total-limit", type=int, default=10)
    parser.add_argument("--max-requests", type=int, default=15)
    parser.add_argument("--interval", type=float, default=1.0)
    parser.add_argument("--timeout", type=float, default=20.0)
    parser.add_argument("--reprocess-from", type=Path, help="저장된 실행 디렉터리를 HTTP 요청 없이 재추출")
    parser.add_argument("--pagination-template", type=Path, help="카테고리별 캡처한 POST 본문·헤더 JSON")
    parser.add_argument("--start-page", type=int, help="설정의 시작 페이지를 일괄 재정의")
    parser.add_argument("--max-pages", type=int, help="설정의 최대 페이지 수를 일괄 재정의")
    parser.add_argument("--listing-only", action="store_true", help="상세 요청 없이 목록만 수집")
    parser.add_argument("--download-images", action="store_true", help="상세 갤러리 다운로드와 상품·파일 연결 검증까지 실행")
    parser.add_argument("--max-image-requests", type=int, default=3000)
    args = parser.parse_args()
    if args.download_images and (args.listing_only or args.reprocess_from):
        parser.error("--download-images requires live detail collection; use gallery_assets for saved runs")
    if args.max_image_requests < 1:
        parser.error("max-image-requests must be positive")
    if args.reprocess_from:
        try:
            raw, normalized, report = reprocess_run(args.reprocess_from)
            destination = save_run(args.output, raw, normalized, report)
        except (ValueError, TypeError, OSError) as exc:
            parser.error(str(exc))
        print(json.dumps({"output": str(destination.resolve()), "products": len(raw), "requests": 0,
                          "result": report["result"], "execution_mode": report["execution_mode"]}, ensure_ascii=False))
        return 0 if report["result"] == "completed" else 2
    try:
        categories = load_categories(args.categories)
        overrides = {key: value for key, value in {"start_page": args.start_page, "max_pages": args.max_pages}.items()
                     if value is not None}
        if overrides:
            categories = [replace(category, **overrides) for category in categories]
        templates = PaginationTemplates.load(args.pagination_template) if args.pagination_template else None
        adapter = DanawaAdapter(templates)
        adapter.validate_categories(categories)
        if args.total_limit < 1:
            raise ValueError("total-limit must be positive")
        client = RequestClient(args.max_requests, args.interval, args.timeout)
    except (ValueError, TypeError, OSError) as exc:
        parser.error(str(exc))
    try:
        raw, normalized, report = collect(adapter, client, categories, args.total_limit, collect_details=not args.listing_only)
        destination = save_run(args.output, raw, normalized, report)
    finally:
        client.close()
    if args.download_images:
        from .media import ImageFetcher, localize_images
        from .validation import verify_assets
        fetcher = ImageFetcher(args.interval, args.timeout)
        try:
            media_report = localize_images(destination / "products.jsonl", destination / "media", fetcher,
                                           args.max_image_requests)
        finally:
            fetcher.close()
        verified = verify_assets(destination / "media/products_with_local_images.jsonl", destination / "media")
        from .storage import atomic_json
        report["image_download"] = {"result": media_report["result"], "verification": verified["result"]}
        if media_report["result"] != "completed" or verified["result"] != "completed":
            report["result"] = "partial"
        atomic_json(destination / "run_report.json", report)
    print(json.dumps({"output": str(destination.resolve()), "products": len(raw),
                      "requests": report["request_count"], "result": report["result"]}, ensure_ascii=False))
    return 0 if report["result"] == "completed" else 2


if __name__ == "__main__":
    raise SystemExit(main())
