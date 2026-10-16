"""CLI: prepare / plan / rehearse / preflight / ingest. No implicit live writes."""
import argparse
from contextlib import ExitStack
import json
import os
import time
from pathlib import Path

from .config import Settings
from .errors import PipelineError
from .files import atomic_json, lock, parse, read_json
from .snapshot import prepare, verify


def emit(value):
    print(json.dumps(value, ensure_ascii=False, allow_nan=False), flush=True)


def _env(name):
    value = os.environ.get(name)
    if not value:
        raise PipelineError("REQUIRED_ENV_MISSING", name)
    return value


def _live(settings, stack):
    from .mongo import MongoLedger
    from .objects import S3Objects
    from .publisher import KafkaPublisher
    if any(v.startswith("replace-with-") for v in (settings.environment_id, settings.mongo_database, settings.image_bucket)):
        raise PipelineError("EXAMPLE_CONFIG_NOT_DEPLOYABLE")
    # Read every required configuration before opening a network connection.
    uri = _env(settings.mongo_uri_env)
    endpoint = _env(settings.s3_endpoint_env)
    kafka = parse(_env(settings.kafka_config_env))
    if endpoint != "aws" and not endpoint.startswith("https://"):
        raise PipelineError("S3_TLS_ENDPOINT_REQUIRED")
    ledger = MongoLedger(settings, uri)
    stack.callback(ledger.close)
    objects = S3Objects(settings.image_bucket, None if endpoint == "aws" else endpoint)
    publisher = KafkaPublisher(settings.kafka_topic, kafka, max_attempts=settings.max_attempts)
    stack.callback(publisher.close)
    return ledger, objects, publisher


def main(argv=None):
    parser = argparse.ArgumentParser(description="다나와 FULL_EXPORT/LIVE_DELTA 인계·적재·복구")
    commands = parser.add_subparsers(dest="command", required=True)
    create = commands.add_parser("prepare", help="중단/완료된 export를 불변 스냅샷으로 검증·복사")
    create.add_argument("--export-dir", type=Path, required=True)
    create.add_argument("--output", type=Path, required=True)
    create.add_argument("--media-manifest", type=Path)
    plan = commands.add_parser("plan", help="봉인 스냅샷 검증. 외부 연결·쓰기 없음")
    plan.add_argument("--snapshot", type=Path, required=True)
    capture_cmd = commands.add_parser('capture', help='수집 checkpoint commit 이하만 읽기 전용 캡처')
    capture_cmd.add_argument('--source', type=Path, required=True)
    capture_cmd.add_argument('--spool', type=Path, required=True)
    capture_cmd.add_argument('--max-spool-bytes', type=int, required=True)
    capture_cmd.add_argument('--min-free-bytes', type=int, required=True)
    capture_cmd.add_argument('--max-records', type=int, default=100)
    capture_cmd.add_argument('--media-manifest', type=Path)
    capture_cmd.add_argument('--watch', action='store_true')
    capture_cmd.add_argument('--poll-seconds', type=float, default=2)
    for command in ('send', 'receive'):
        cmd = commands.add_parser(command, help='S3 ready-last 인계 '+command)
        cmd.add_argument('--config', type=Path, required=True)
        if command == 'send':
            cmd.add_argument('--snapshot', type=Path, required=True)
        else:
            cmd.add_argument('--ready-key', required=True)
            cmd.add_argument('--output', type=Path, required=True)
            cmd.add_argument('--max-total-bytes', type=int, required=True)
    drain = commands.add_parser('drain', help='확정 spool 배치를 순서대로 적재. NiFi 단발 호출 가능')
    drain.add_argument('--spool', type=Path, required=True)
    drain.add_argument('--state-dir', type=Path, required=True)
    drain.add_argument('--config', type=Path)
    drain.add_argument('--rehearse', action='store_true')
    for name in ("rehearse", "ingest"):
        cmd = commands.add_parser(name, help="파일 기반 로컬 리허설" if name == "rehearse" else "실제 MongoDB·S3·Kafka 적재")
        cmd.add_argument("--snapshot", type=Path, required=True)
        cmd.add_argument("--state-dir", type=Path, required=True, help="스냅샷과 별도인 감사/리허설 디렉터리")
        cmd.add_argument("--config", type=Path, required=name == "ingest")
        cmd.add_argument("--replay-events", action="store_true", help="기존 ID·본문으로 확인된 이벤트도 재발행")
        cmd.add_argument('--repair-raw', action='store_true', help='확인된 원천도 대조·복구. 정상 재개에서는 생략')
    preflight = commands.add_parser("preflight", help="인프라 읽기 전용 확인. 객체·컬렉션·토픽 생성 없음")
    preflight.add_argument("--config", type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        if args.command == 'capture':
            from .live import CaptureSession, capture
            if not 0.1 <= args.poll_seconds <= 3600:
                raise PipelineError('INVALID_CAPTURE_INTERVAL')
            session = CaptureSession() if args.watch else None
            while True:
                started = time.monotonic()
                result = capture(args.source, args.spool, max_spool_bytes=args.max_spool_bytes,
                    min_free_bytes=args.min_free_bytes, max_records=args.max_records,
                    media_manifest=args.media_manifest, session=session)
                emit({**result, 'duration_ms': round((time.monotonic()-started)*1000)})
                if not args.watch:
                    return 2 if result['result'] == 'BACKPRESSURE' else 0
                if result['result'] != 'CAPTURED_WAITING_INGEST':
                    time.sleep(args.poll_seconds)
        if args.command == 'drain':
            from .handoff import drain_spool
            if args.rehearse == bool(args.config):
                raise PipelineError('SELECT_REHEARSAL_OR_LIVE_CONFIG')
            result = drain_spool(args.spool, args.state_dir, args.config, args.rehearse, emit)
            emit(result)
            return 0 if result['result'] == 'DRAINED' else 2
        if args.command in {'send', 'receive'}:
            from .objects import S3Objects
            from .transfer import send_snapshot, receive_snapshot, MAX_READY_BYTES, CHUNK_BYTES
            import tempfile
            settings = Settings.load(args.config)
            endpoint = _env(settings.s3_endpoint_env)
            if endpoint != 'aws' and not endpoint.startswith('https://'):
                raise PipelineError('S3_TLS_ENDPOINT_REQUIRED')
            # Transport uses a dedicated configured handoff bucket, never infer an image/lake role.
            config = read_json(args.config)
            bucket = config.get('handoff_bucket')
            if not bucket or bucket.startswith('replace-with-'):
                raise PipelineError('HANDOFF_BUCKET_REQUIRED')
            store = S3Objects(bucket, None if endpoint == 'aws' else endpoint)
            store.preflight()
            if args.command == 'send':
                result = send_snapshot(args.snapshot, store)
            else:
                if not args.ready_key.startswith('handoff/ready/') or not args.ready_key.endswith('.ready.json'):
                    raise PipelineError('INVALID_READY_KEY')
                def fetch(key, limit=CHUNK_BYTES):
                    response = store.client.get_object(Bucket=bucket, Key=key)
                    try:
                        data = response['Body'].read(limit+1)
                    finally:
                        response['Body'].close()
                    if len(data) > limit:
                        raise PipelineError('TRANSPORT_OBJECT_TOO_LARGE')
                    return data
                with tempfile.TemporaryDirectory(prefix='handoff-ready-') as tmp:
                    ready = Path(tmp)/'ready.json'
                    ready.write_bytes(fetch(args.ready_key, MAX_READY_BYTES))
                    result = receive_snapshot(ready, fetch, args.output, max_total_bytes=args.max_total_bytes)
            emit(result)
            return 0
        if args.command == "prepare":
            result = prepare(args.export_dir, args.output, args.media_manifest)
            emit({"result": "prepared", "snapshot": str(args.output.resolve()), **result})
            return 0
        if args.command == "plan":
            manifest = verify(args.snapshot)
            emit({"result": "validated", "external_connections": 0, "external_writes": 0,
                  "snapshot_id": manifest["snapshot_id"], "products": manifest["product_count"],
                  "verified_image_relations": manifest["verified_image_relations"]})
            return 0
        settings = Settings.load(args.config) if args.config else Settings("rehearsal", "rehearsal", "rehearsal-images")
        with ExitStack() as stack:
            if args.command == "preflight":
                ledger, objects, publisher = _live(settings, stack)
                objects.preflight()
                publisher.preflight()
                emit({"result": "read_preflight_passed", "writes": 0,
                      "remaining_acceptance": ["majority-journal write test", "conditional S3 write test",
                                               "delivery and failure injection"]})
                return 0
            # Malformed input must fail before live connection/schema setup.
            verify(args.snapshot)
            state = args.state_dir.resolve()
            if state.is_relative_to(args.snapshot.resolve()) or args.snapshot.resolve().is_relative_to(state):
                raise PipelineError("STATE_DIRECTORY_NOT_SEPARATE")
            marker = state / "mode.json"
            expected = {"mode": args.command, "target_fingerprint": settings.fingerprint}
            if any((state / name).exists() for name in ("run_config.json", "product_raw.jsonl", "snapshot.json")):
                raise PipelineError("STATE_DIRECTORY_CONTAINS_SOURCE_DATA")
            with lock(state / "mode.lock"):
                if marker.exists() and read_json(marker) != expected:
                    raise PipelineError("STATE_TARGET_OR_MODE_MISMATCH")
                atomic_json(marker, expected)
            if args.command == "rehearse":
                from .local import FileLedger, FileObjects, FilePublisher
                ledger = FileLedger(state / "ledger", settings)
                objects = FileObjects(state / "objects")
                publisher = FilePublisher(state / "events")
            else:
                ledger, objects, publisher = _live(settings, stack)
                objects.preflight()
                publisher.preflight()
            from .runner import ingest
            result = ingest(args.snapshot, settings, ledger, objects, publisher, state / "audit",
                            replay_events=args.replay_events, progress=emit, repair_raw=args.repair_raw)
            emit({"mode": args.command, **result})
            return 0 if result["result"] == "completed" else 2
    except KeyboardInterrupt:
        emit({"result": "interrupted"})
        return 130
    except Exception as exc:
        # Do not render SDK errors, URIs, tokens or arbitrary source values.
        emit({"result": "failed", "error_code": exc.code if isinstance(exc, PipelineError) else type(exc).__name__})
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
