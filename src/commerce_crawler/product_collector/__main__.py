from __future__ import annotations

import argparse
import json
import math
import signal
import sys
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

# Exit codes the server launcher reads (crawler-optimization/operations.md).
EXIT_COMPLETED = 0
EXIT_FAILED = 1          # an exception during the run; checkpoint_state is "failed"
EXIT_PARTIAL = 2         # partial or quality_failed; run_report.result tells which
EXIT_NOT_STARTED = 3     # command line, settings or resume refused before collecting; no run state changed
EXIT_INTERRUPTED = 130   # Ctrl+C or SIGTERM; checkpoint_state is "interrupted"
# --check-pagination
CHECK_OK, CHECK_BLOCKED, CHECK_REQUEST_FAILED, CHECK_PARSE_FAILED = 0, 4, 5, 6


class Parser(argparse.ArgumentParser):
    """Usage and settings errors exit with EXIT_NOT_STARTED, never with partial's 2."""

    def error(self, message):
        finish_line(result="not_started", exit_code=EXIT_NOT_STARTED, error=message)
        self.print_usage(sys.stderr)
        self.exit(EXIT_NOT_STARTED, f"{self.prog}: error: {message}\n")


def finish_line(**values):
    """The last stdout line of every outcome, for the launcher (no secrets, no headers)."""
    with PRINT_LOCK:
        print(json.dumps(values, ensure_ascii=False), flush=True)


def check_pagination(path: Path, timeout: float) -> int:
    """One listing POST of the template's first category: does the captured Next-Action still work?

    No run directory is made and nothing is saved."""
    import requests
    from .core import Category
    from .pagination import ListingResponseError
    try:
        templates = PaginationTemplates.load(path)
        category_id = next(iter(templates.categories))
        client = RequestClient(1, 0.0, timeout)
    except (OSError, ValueError, StopIteration) as exc:
        finish_line(check="pagination", status="not_started", error=f"{type(exc).__name__}: {exc}")
        return EXIT_NOT_STARTED
    status_code = None
    try:
        rows, diagnostics = DanawaAdapter(templates).fetch_listing_page(client, Category(category_id, limit=None), 1)
        count = diagnostics.get("response_product_count", len(rows))
        status, code, error = ("ok", CHECK_OK, None) if rows else ("parse_failed", CHECK_PARSE_FAILED, "no products")
    except requests.HTTPError as exc:
        status_code = exc.response.status_code if exc.response is not None else None
        count = 0
        status, code = (("blocked", CHECK_BLOCKED) if status_code in {403, 429}
                        else ("request_failed", CHECK_REQUEST_FAILED))
        error = f"{type(exc).__name__}: {exc}"
    except requests.RequestException as exc:
        count, status, code, error = 0, "request_failed", CHECK_REQUEST_FAILED, f"{type(exc).__name__}: {exc}"
    except (ListingResponseError, ValueError) as exc:
        count, status, code, error = 0, "parse_failed", CHECK_PARSE_FAILED, f"{type(exc).__name__}: {exc}"
    finally:
        client.close()
    if status_code is None and client.attempts:
        status_code = client.attempts[0].get("status_code")
    finish_line(check="pagination", status=status, category_id=category_id, http_status=status_code,
                products=count, error=error)
    return code


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


def image_index(output: Path, destination: Path, policy_version: int, store_config: dict | None,
                replacement_sample: int | None = None):
    """Shared download history of the output root (no image bytes) and the runs to seed it from.

    The settings are fixed in the run's media/ so a resumed image step uses the same history,
    the same object storage (endpoint and bucket, never the keys) and the same retry rules.
    With storage, the image key rules and the replacement sample are fixed too."""
    from .image_store import describe
    from .media import IMAGE_KEY_VERSION
    from .storage import atomic_json
    index = (Path(output) / "_image_index").resolve()
    settings = {"index_dir": str(index), "object_store": describe(store_config) if store_config else None,
                "retry_policy_version": policy_version}
    if store_config:
        settings.update(image_key_version=IMAGE_KEY_VERSION, replacement_sample=replacement_sample)
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


class ImageResources:
    """The image step's thread, locks and clients; closed before the run's writer.lock is released."""

    def __init__(self):
        self.fetcher = self.object_store = self.step = self.worker = self.options = None

    def close(self, completed=False):
        """Idempotent. Without completed, an overlap step that had not finished stays unfinished."""
        try:
            if self.worker is not None:
                self.worker.stop()
                self.worker = None
            if self.step is not None:
                step, self.step = self.step, None
                try:
                    if not completed and not step.prepared:
                        step.abort()
                finally:
                    step.close(completed)
        finally:
            for client in ("fetcher", "object_store"):
                if getattr(self, client) is not None:
                    getattr(self, client).close()
                    setattr(self, client, None)


def stop_on_signal(signum, frame):
    """Ctrl+C, or SIGTERM from systemd stopping NiFi: save the run as interrupted.

    Only the first signal counts. systemd signals the supervisor and the collector together and the
    supervisor passes SIGINT on, so a second signal must not break the save that the first one started."""
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    raise KeyboardInterrupt


def main() -> int:
    if threading.current_thread() is threading.main_thread():
        signal.signal(signal.SIGINT, stop_on_signal)
        signal.signal(signal.SIGTERM, stop_on_signal)
    parser = Parser(description="다나와 카테고리·페이지 수집과 상품 갤러리 보강")
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
    parser.add_argument("--image-overlap", choices=("on", "off"), default="on",
                        help="on은 상세가 끝난 상품의 사진을 상세 수집과 함께 받음, off는 상세 전체 뒤에 사진 전체")
    parser.add_argument("--image-store-config", type=Path,
                        help="이미지 버킷 읽기 전용 접속 설정(Git 밖 파일). 있으면 이미 저장된 사진은 다시 받지 않음")
    parser.add_argument("--image-replacement-sample", type=int, default=None,
                        help="저장소 설정이 있을 때 주소의 _v만 다른 사진 중 매 실행 다시 받아 교체 여부를 볼 수. 기본 1000")
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
    parser.add_argument("--check-pagination", action="store_true",
                        help="--pagination-template의 첫 카테고리 1페이지 POST 1회로 템플릿이 유효한지만 확인. "
                             "종료 코드 0 정상, 4 차단(403/429), 5 통신 실패, 6 응답 해석 실패, 3 템플릿 없음·형식 오류")
    args = parser.parse_args()
    if args.check_pagination:
        return check_pagination(args.pagination_template, args.timeout)
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
    if args.image_replacement_sample is not None and not args.image_store_config:
        parser.error("--image-replacement-sample requires --image-store-config")
    if args.image_replacement_sample is not None and args.image_replacement_sample < 0:
        parser.error("--image-replacement-sample must be nonnegative")
    store_config = None
    if args.download_images:
        try:
            from .media import (ImageFetcher, ImageStep, ImageWorker, REPLACEMENT_SAMPLE, RETRY_POLICY_VERSION,
                                localize_images)
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
    opened = False
    try:
        with open_collection(args.output, adapter, config, args.resume_from, args.new_run,
                             progress=show_progress, repair_files=args.repair_files,
                             image_policy_version=RETRY_POLICY_VERSION if args.download_images else None) as checkpoint:
            opened = True
            destination = checkpoint.directory
            previous = checkpoint.current_report()
            adapter.restore_pagination(previous)
            print(f"[시작] 카테고리 {len(categories)}개 / 목록 작업자 {args.workers}개 / 상품 한도 {args.total_limit or '없음'} / 요청 한도 {args.max_requests or '없음'} / 목록 간격 {args.listing_interval}초 / 상세·다운로드 간격 {args.interval}초", flush=True)
            print(f"[실시간 저장] {destination.resolve()}", flush=True)
            print(f"[재개] python run_collector.py --resume-from \"{destination.resolve()}\"", flush=True)
            reuse = None
            if args.detail_refresh == "changed" or not args.listing_only:
                # "all" opens it too: nothing is reused, but the report counts what "changed" would reuse.
                reuse = DetailReuse.open(destination, args.output, adapter.source, checkpoint.job_id, args.refresh_sample,
                                         args.detail_refresh)
                label = "상세 재사용" if args.detail_refresh == "changed" else "상세 재사용 측정(all: 재사용 안 함)"
                print(f"[{label}] 기준 실행 {reuse.base_job_id} / 상품 {len(reuse.index)}개 / 이전 실행 {len(reuse.runs) - 1}개에서 "
                      f"{len(reuse.fallback)}개 / 표본 {args.refresh_sample}개"
                      if reuse.base else f"[{label}] 기준 실행이 정리되어 남은 상세를 모두 요청합니다: {reuse.base_missing}"
                      if reuse.base_missing else f"[{label}] 완료된 이전 실행이 없어 모든 상세를 요청합니다", flush=True)
            images = ImageResources()

            def image_options():
                """Settings, history and clients of the image step, made when it starts."""
                if images.options is None:
                    sample = (REPLACEMENT_SAMPLE if args.image_replacement_sample is None
                              else args.image_replacement_sample) if store_config else None
                    index, sources = image_index(args.output, destination, RETRY_POLICY_VERSION, store_config, sample)
                    images.fetcher = ImageFetcher(args.interval, args.timeout)
                    images.object_store = S3ImageStore(store_config) if store_config else None
                    images.options = dict(retry_failed=args.retry_failed, retry_transient=True, index_dir=index,
                                          object_store=images.object_store, import_from=sources,
                                          identity_keys=bool(store_config), replacement_sample=sample or 0)
                return images.options

            def start_overlap():
                # Started with the first finished product, so a stop while listing leaves no image step.
                if images.worker is None:
                    images.step = ImageStep(destination / "media", None, args.max_image_requests,
                                            **image_options()).open()
                    images.step.fetcher = images.fetcher
                    images.fetcher.cancel = images.step.cancel
                    images.step.begin()
                    images.worker = ImageWorker(images.step)
                return images.worker

            try:
                on_product = None
                if args.download_images and args.image_overlap == "on":
                    # Galleries are downloaded while details are fetched: another host, its own interval.
                    on_product = lambda key: start_overlap().offer(checkpoint.load_normalized(key))
                raw, normalized, report = collect(adapter, client, categories, args.total_limit,
                    collect_details=not args.listing_only, progress=show_progress, checkpoint=checkpoint,
                    listing_interval=args.listing_interval, workers=args.workers, detail_reuse=reuse,
                    on_product=on_product)
                checkpoint.save([], report, client.attempts)
                checkpoint.export()  # products.jsonl for the image step; the status stays running until delivery is decided
                if args.download_images:
                    if on_product:
                        start_overlap().close()
                        media_report = images.step.finish(destination / "products.jsonl",
                                                          images.worker.digest.hexdigest())
                        images.close(completed=True)
                    else:
                        options = image_options()
                        media_report = localize_images(destination / "products.jsonl", destination / "media",
                                                       images.fetcher, args.max_image_requests, **options)
                    images.close()
                    verified = verify_assets(destination / "media/products_with_local_images.jsonl", destination / "media")
                    report["image_download"] = {"result": media_report["result"], "verification": verified["result"],
                                                **{k: media_report[k] for k in ("downloaded", "stored", "failed", "gave_up",
                                                                                "store_checks", "store_errors")}}
                    # Final URL states and alerts of the image step (crawler-optimization/image-retry.md).
                    for key in ("failures", "alerts", "storage_unverified"):
                        if media_report.get(key):
                            report["image_download"][key] = media_report[key]
                    if (media_report.get("retry") or {}).get("passes"):
                        report["image_download"]["retry"] = media_report["retry"]
                    if media_report.get("stop_reason"):
                        # A 403/429 from the image host, so one report tells the launcher about any block.
                        report["image_download"]["stop_reason"] = media_report["stop_reason"]
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
                images.close()  # the image thread stops before the run's lock is released
                checkpoint.interrupt(client.attempts)
                print(f"[중단·저장 완료] {destination.resolve()}", flush=True)
                finish_line(output=str(destination.resolve()), job_id=checkpoint.job_id, result="interrupted",
                            exit_code=EXIT_INTERRUPTED)
                return EXIT_INTERRUPTED
            except Exception as exc:
                try:
                    images.close()
                finally:
                    checkpoint.interrupt(client.attempts, 'failed', str(exc))
                    finish_line(output=str(destination.resolve()), job_id=checkpoint.job_id, result="failed",
                                exit_code=EXIT_FAILED, error=f"{type(exc).__name__}: {exc}")
                raise
            finally:
                images.close()
    except (ValueError, OSError) as exc:
        if opened:
            raise
        # Busy run, changed settings, a run that is gone or abandoned: nothing was collected or saved.
        finish_line(output=str(Path(args.output).resolve()), result="not_started", exit_code=EXIT_NOT_STARTED,
                    error=f"{type(exc).__name__}: {exc}")
        return EXIT_NOT_STARTED
    finally:
        client.close()
    show_quality(report)
    code = EXIT_COMPLETED if report["result"] == "completed" else EXIT_PARTIAL
    finish_line(output=str(destination.resolve()), products=len(raw), requests=report["request_count"],
                result=report["result"], job_id=report.get("job_id"), exit_code=code)
    return code


if __name__ == "__main__":
    raise SystemExit(main())
