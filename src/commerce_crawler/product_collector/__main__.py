from __future__ import annotations

import argparse
import json
import math
import threading
from pathlib import Path
from dataclasses import replace, asdict
from .checkpoint import open_collection, export_checkpoint

from .core import RequestClient, collect, load_categories, save_run, parse_limit
from .danawa import FINGERPRINT_VERSION, DanawaAdapter
from . import quality
from .detail_reuse import MODES as REFRESH_MODES, DetailReuse
from .reprocess import reprocess_run
from .pagination import PaginationTemplates

PROJECT_ROOT = Path(__file__).resolve().parent.parent
PRINT_LOCK = threading.Lock()


def show_progress(event):
    stage = event['stage']
    if stage == 'checkpoint_read':
        total = event['total']
        percent = 100 * event['completed'] / total if total else 100
        text = f"[이력 복원] {percent:.1f}% / {event['completed'] / 1048576:.1f}/{total / 1048576:.1f} MB / 상품 {event['products']}개 / {event['elapsed']:.1f}초"
    elif stage == 'checkpoint_files':
        text = (f"[상품 파일 복구] {event['completed']}/{event['total']}개 / {event['elapsed']:.1f}초"
                if event['total'] else '[상품 파일 복구] 대상 없음 / 저장 완료한 파일 검사 생략')
    elif stage == 'checkpoint_ready':
        text = f"[복원 완료] 상품 {event['products']}개 / {event['elapsed']:.1f}초"
    elif stage == 'request':
        text = f"[요청 {event['number']}] {event['method']} {event['url']}"
    elif stage == 'category':
        text = f"[카테고리 {event['index']}/{event['total']}] {event['name'] or ''} ({event['category_id']})"
    elif stage == 'page':
        text = f"[목록] 카테고리 {event['category_id']} / {event['page']}페이지 / 상품 {event['products']}개"
    elif stage == 'details_start':
        text = f"[상세 보강] 목록 순회 완료, 상품 {event['total']}개 처리 시작"
    elif stage == 'detail':
        text = f"[상세 {event['index']}/{event['total']}] 상품 {event['product_id']} / {event['status']}" + (
            " (이전 실행 재사용)" if event.get('reused') else "")
    else:
        text = f"[오류] {event.get('category_id', '')}: {event.get('reason', '')}"
    with PRINT_LOCK:
        print(text, flush=True)


def show_quality(report):
    summary = report.get("quality")
    if not summary:
        return
    head = "통과" if summary["passed"] else "상한 초과"
    if not summary["judged"]:
        head += f" (상세 {summary['received_details']}건, 표본 {quality.MIN_SAMPLE}건 미만은 상세 비율 판정 생략)"
    print(f"[품질 관문:{summary['mode']}] {head}", flush=True)
    for item in summary["violations"]:
        print(f"[품질 초과] {item['signal']} {item['count']}/{item['total']} = {item['rate']} > {item['limit']}", flush=True)
    for item in summary["alerts"]:
        print(f"[품질 경보] {item['signal']} 카테고리 {item.get('category_id')}", flush=True)


def image_index(output: Path, destination: Path, policy_version: int, store_config: dict | None):
    """Shared download history of the output root (no image bytes) and the runs to seed it from.

    The settings are fixed in the run's media/ so a resumed image step uses the same history,
    the same object storage (endpoint and bucket, never the keys) and the same retry rules."""
    from .image_store import describe
    from .storage import atomic_json
    index = (Path(output) / "_image_index").resolve()
    settings = {"index_dir": str(index), "object_store": describe(store_config) if store_config else None,
                "retry_policy_version": policy_version}
    path = destination / "media" / "image_settings.json"
    if path.is_file():
        saved = json.loads(path.read_text(encoding="utf-8"))
        if saved != settings:
            raise ValueError(f"이미지 단계 설정이 이전 실행과 다릅니다: {saved} != {settings}")
    else:
        path.parent.mkdir(parents=True, exist_ok=True)
        atomic_json(path, settings)
    runs = [p / "media" for p in Path(output).iterdir()
            if p.is_dir() and p.resolve() != destination.resolve() and (p / "run_config.json").is_file()
            and (p / "media" / "image_downloads.jsonl").is_file()]
    # Newest first: a URL found in several runs is imported from the most recent download.
    runs.sort(key=lambda m: (m / "image_downloads.jsonl").stat().st_mtime_ns, reverse=True)
    return index, runs


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
    parser.add_argument("--retry-failed", action="store_true",
                        help="이미지: 연속 실패로 포기한 사진 주소도 다시 시도")
    parser.add_argument("--image-store-config", type=Path,
                        help="이미지 버킷 읽기 전용 접속 설정(Git 밖 파일). 있으면 이미 저장된 사진은 다시 받지 않음")
    parser.add_argument("--resume-from", type=Path, help="지정한 작업의 저장 지점에서 재개")
    parser.add_argument("--new-run", action="store_true", help="미완료 작업 재개 대신 새 작업 시작")
    parser.add_argument("--repair-files", action="store_true", help="개별 상품 JSON 전체 검사·복구. 외부 삭제·손상 시 사용하며 재개 준비가 오래 걸릴 수 있음")
    parser.add_argument("--export-from", type=Path, help="진행 중/중단된 체크포인트를 JSONL로 내보내기. 수집 요청 없음")
    parser.add_argument("--detail-refresh", choices=REFRESH_MODES, default="all",
                        help="all은 상세 전부 요청, changed는 직전 완료 실행과 목록 지문이 같은 상품의 상세 재사용")
    parser.add_argument("--refresh-sample", type=int, default=1000,
                        help="changed에서 무변경이어도 다시 요청할 표본 수(구조 변경 감지용). 0은 표본 없음")
    parser.add_argument("--quality-gate", choices=quality.MODES, default="warn",
                        help="종료 품질 관문. warn은 기록만, enforce는 상한 초과 시 quality_failed")
    args = parser.parse_args()
    if args.export_from:
        print(json.dumps({"export": str(export_checkpoint(args.export_from)), "requests": 0}, ensure_ascii=False))
        return 0
    if args.resume_from and args.new_run:
        parser.error("--resume-from and --new-run cannot be combined")
    if args.refresh_sample < 0:
        parser.error("--refresh-sample must be zero or positive")
    if args.detail_refresh == "changed" and (args.listing_only or args.reprocess_from):
        parser.error("--detail-refresh changed requires live detail collection")
    if args.download_images and (args.listing_only or args.reprocess_from):
        parser.error("--download-images requires live detail collection; use gallery_assets for saved runs")
    if args.image_store_config and not args.download_images:
        parser.error("--image-store-config requires --download-images")
    store_config = None
    if args.download_images:
        try:
            from .media import ImageFetcher, RETRY_POLICY_VERSION, localize_images
            from .validation import verify_assets
            from .image_store import S3ImageStore, read_store_config
            if args.image_store_config:
                import boto3  # noqa: F401  checked before collection begins
        except ModuleNotFoundError as exc:
            parser.error(f"이미지 다운로드 의존성이 없습니다: {exc.name}. 실행에 사용하는 Python으로 python -m pip install -r requirements-collector.txt 를 먼저 실행해주세요.")
        if args.image_store_config:
            try:
                store_config = read_store_config(args.image_store_config)
            except (ValueError, OSError) as exc:
                parser.error(f"이미지 저장소 설정을 읽을 수 없습니다: {exc}")
    if args.reprocess_from:
        try:
            raw, normalized, report = reprocess_run(args.reprocess_from, args.output, args.quality_gate)
            destination = save_run(args.output, raw, normalized, report)
        except (ValueError, TypeError, OSError) as exc:
            parser.error(str(exc))
        show_quality(report)
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
    if args.detail_refresh == "changed":
        # Fixed for the run: a resume with another mode or sample is a different job. Absent in "all" for older runs.
        config["detail_refresh"] = {"mode": "changed", "sample": args.refresh_sample,
                                    "fingerprint_version": FINGERPRINT_VERSION}
    try:
        with open_collection(args.output, adapter, config, args.resume_from, args.new_run,
                             progress=show_progress, repair_files=args.repair_files) as checkpoint:
            destination = checkpoint.directory
            previous = checkpoint.current_report()
            adapter.restore_pagination(previous)
            print(f"[시작] 카테고리 {len(categories)}개 / 목록 작업자 {args.workers}개 / 상품 한도 {args.total_limit or '없음'} / 요청 한도 {args.max_requests or '없음'} / 목록 간격 {args.listing_interval}초 / 상세·다운로드 간격 {args.interval}초", flush=True)
            print(f"[실시간 저장] {destination.resolve()}", flush=True)
            print(f"[재개] python run_collector.py --resume-from \"{destination.resolve()}\"", flush=True)
            reuse = None
            if args.detail_refresh == "changed":
                reuse = DetailReuse.open(destination, args.output, adapter.source, checkpoint.job_id, args.refresh_sample)
                print(f"[상세 재사용] 기준 실행 {reuse.base_job_id} / 상품 {len(reuse.index)}개 / 표본 {args.refresh_sample}개"
                      if reuse.base else f"[상세 재사용] 기준 실행이 정리되어 남은 상세를 모두 요청합니다: {reuse.base_missing}"
                      if reuse.base_missing else "[상세 재사용] 완료된 이전 실행이 없어 모든 상세를 요청합니다", flush=True)
            try:
                raw, normalized, report = collect(adapter, client, categories, args.total_limit,
                    collect_details=not args.listing_only, progress=show_progress, checkpoint=checkpoint,
                    listing_interval=args.listing_interval, workers=args.workers, detail_reuse=reuse)
                checkpoint.save([], report, client.attempts)
                checkpoint.export()  # products.jsonl for the image step; the status stays running until delivery is decided
                if args.download_images:
                    index, sources = image_index(args.output, destination, RETRY_POLICY_VERSION, store_config)
                    fetcher = ImageFetcher(args.interval, args.timeout)
                    object_store = S3ImageStore(store_config) if store_config else None
                    try:
                        media_report = localize_images(destination / "products.jsonl", destination / "media", fetcher,
                                                       args.max_image_requests, retry_failed=args.retry_failed,
                                                       retry_transient=True, index_dir=index,
                                                       object_store=object_store, import_from=sources)
                    finally:
                        fetcher.close()
                        if object_store:
                            object_store.close()
                    verified = verify_assets(destination / "media/products_with_local_images.jsonl", destination / "media")
                    report["image_download"] = {"result": media_report["result"], "verification": verified["result"],
                                                **{k: media_report[k] for k in ("downloaded", "stored", "failed", "gave_up",
                                                                                "store_checks", "store_errors")}}
                    if media_report["result"] != "completed" or verified["result"] != "completed":
                        report["result"] = "partial"
                # The per-product values collect kept; the records themselves stay in the journal.
                quality.apply(report, getattr(normalized, "projection", normalized), args.quality_gate,
                              {str(r["product_id"]) for r in getattr(raw, "projection", raw) if r.get("detail_reused_from")})
                # One delivery status for the journal, checkpoint_state, run_report, prune and the exit code.
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
    show_quality(report)
    print(json.dumps({"output": str(destination.resolve()), "products": len(raw),
                      "requests": report["request_count"], "result": report["result"]}, ensure_ascii=False))
    return 0 if report["result"] == "completed" else 2


if __name__ == "__main__":
    raise SystemExit(main())
