from __future__ import annotations

import argparse
import json
import math
import threading
from pathlib import Path
from dataclasses import replace, asdict
from .checkpoint import open_collection, export_checkpoint

from .core import RequestClient, collect, load_categories, save_run, parse_limit
from .danawa import DanawaAdapter
from .reprocess import reprocess_run
from .pagination import PaginationTemplates

PROJECT_ROOT = Path(__file__).resolve().parent.parent
PRINT_LOCK = threading.Lock()


def show_progress(event):
    stage = event['stage']
    if stage == 'request':
        text = f"[요청 {event['number']}] {event['method']} {event['url']}"
    elif stage == 'category':
        text = f"[카테고리 {event['index']}/{event['total']}] {event['name'] or ''} ({event['category_id']})"
    elif stage == 'page':
        text = f"[목록] 카테고리 {event['category_id']} / {event['page']}페이지 / 상품 {event['products']}개"
    elif stage == 'details_start':
        text = f"[상세 보강] 목록 순회 완료, 상품 {event['total']}개 처리 시작"
    elif stage == 'detail':
        text = f"[상세 {event['index']}/{event['total']}] 상품 {event['product_id']} / {event['status']}"
    else:
        text = f"[오류] {event.get('category_id', '')}: {event.get('reason', '')}"
    with PRINT_LOCK:
        print(text, flush=True)


def main() -> int:
    parser = argparse.ArgumentParser(description="다나와 카테고리·페이지 수집과 상품 갤러리 보강")
    parser.add_argument("--categories", type=Path, default=PROJECT_ROOT / "config/danawa_categories_71.json")
    parser.add_argument("--output", type=Path, default=PROJECT_ROOT / "data/danawa")
    parser.add_argument("--total-limit", type=parse_limit, default=None, help="전체 상품 상한. 기본값과 0은 무제한")
    parser.add_argument("--max-requests", type=parse_limit, default=None, help="목록·상세 요청 상한. 기본값과 0은 무제한")
    parser.add_argument("--interval", type=float, default=1.0, help="상세 요청·이미지 다운로드 간격(초)")
    parser.add_argument("--listing-interval", type=float, default=0.0, help="목록 요청 간격(초). 기본 0초")
    parser.add_argument("--workers", type=int, default=2, help="동시에 수집할 목록 카테고리 수. 1은 순차 처리")
    parser.add_argument("--timeout", type=float, default=20.0)
    parser.add_argument("--reprocess-from", type=Path, help="저장된 실행 디렉터리를 HTTP 요청 없이 재추출")
    parser.add_argument("--pagination-template", type=Path, default=PROJECT_ROOT / "config/danawa_pagination.local.json", help="카테고리별 캡처한 POST 본문·헤더 JSON")
    parser.add_argument("--start-page", type=int, help="설정의 시작 페이지를 일괄 재정의")
    parser.add_argument("--max-pages", type=parse_limit, default=argparse.SUPPRESS, help="카테고리별 페이지 상한. 0은 끝까지")
    parser.add_argument("--listing-only", action="store_true", help="상세 요청 없이 목록만 수집")
    parser.add_argument("--download-images", action="store_true", help="상세 갤러리 다운로드와 상품·파일 연결 검증까지 실행")
    parser.add_argument("--max-image-requests", type=parse_limit, default=None)
    parser.add_argument("--resume-from", type=Path, help="지정한 작업의 저장 지점에서 재개")
    parser.add_argument("--new-run", action="store_true", help="미완료 작업 재개 대신 새 작업 시작")
    parser.add_argument("--export-from", type=Path, help="진행 중/중단된 체크포인트를 JSONL로 내보내기. 수집 요청 없음")
    args = parser.parse_args()
    if args.export_from:
        print(json.dumps({"export": str(export_checkpoint(args.export_from)), "requests": 0}, ensure_ascii=False))
        return 0
    if args.resume_from and args.new_run:
        parser.error("--resume-from and --new-run cannot be combined")
    if args.download_images and (args.listing_only or args.reprocess_from):
        parser.error("--download-images requires live detail collection; use gallery_assets for saved runs")
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
        if args.workers < 1:
            raise ValueError("workers must be positive")
        if not math.isfinite(args.listing_interval) or args.listing_interval < 0:
            raise ValueError("listing-interval must be finite and nonnegative")
        categories = load_categories(args.categories)
        overrides = {"start_page": args.start_page} if args.start_page is not None else {}
        if hasattr(args, "max_pages"):
            overrides["max_pages"] = args.max_pages
        if overrides:
            categories = [replace(category, **overrides) for category in categories]
        templates = PaginationTemplates.load(args.pagination_template) if args.pagination_template and args.pagination_template.is_file() else None
        adapter = DanawaAdapter(templates, discover_categories=True)
        adapter.validate_categories(categories)
        client = RequestClient(args.max_requests, args.interval, args.timeout, progress=show_progress)
    except (ValueError, TypeError, OSError) as exc:
        parser.error(str(exc))
    config = {"source": adapter.source, "categories": [asdict(c) for c in categories],
              "total_limit": args.total_limit, "collect_details": not args.listing_only,
              "pagination_payloads": {key: value["payload"] for key, value in templates.categories.items()} if templates else {}}
    try:
        with open_collection(args.output, adapter, config, args.resume_from, args.new_run) as checkpoint:
            destination = checkpoint.directory
            _, previous = checkpoint.restore()
            adapter.restore_pagination(previous)
            print(f"[시작] 카테고리 {len(categories)}개 / 목록 작업자 {args.workers}개 / 상품 한도 {args.total_limit or '없음'} / 요청 한도 {args.max_requests or '없음'} / 목록 간격 {args.listing_interval}초 / 상세·다운로드 간격 {args.interval}초", flush=True)
            print(f"[실시간 저장] {destination.resolve()}", flush=True)
            print(f"[재개] python run_collector.py --resume-from \"{destination.resolve()}\"", flush=True)
            try:
                raw, normalized, report = collect(adapter, client, categories, args.total_limit,
                    collect_details=not args.listing_only, progress=show_progress, checkpoint=checkpoint,
                    listing_interval=args.listing_interval, workers=args.workers)
                checkpoint.save([], report, client.attempts)
                checkpoint.state(report['result'])
                checkpoint.export()
            except KeyboardInterrupt:
                checkpoint.interrupt(client.attempts)
                print(f"[중단·저장 완료] {destination.resolve()}", flush=True)
                return 130
            except Exception as exc:
                checkpoint.interrupt(client.attempts, 'failed', str(exc))
                raise
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
