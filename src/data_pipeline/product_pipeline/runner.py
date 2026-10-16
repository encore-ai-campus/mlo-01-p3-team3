"""Single-observation commit/recovery. Safe to repeat after every external boundary."""
from pathlib import Path
import time
import random
from uuid import uuid4

from .contracts import digest, envelope, event_for, now, registration_identity
from .errors import Conflict, LeaseLost, PipelineError
from .files import atomic_json, confined, read_json, rows
from .snapshot import verify


def retryable(exc):
    if isinstance(exc, (Conflict, LeaseLost)):
        return False
    if isinstance(exc, PipelineError):
        return exc.code in {"OBJECT_READ_FAILED", "OBJECT_WRITE_UNCONFIRMED", "RAW_UNCONFIRMED",
                            "REGISTRATION_UNCONFIRMED", "ATTEMPT_LOG_UNCONFIRMED"}
    try:
        from pymongo.errors import ConnectionFailure, ExecutionTimeout, WriteConcernError
        if isinstance(exc, (ConnectionFailure, ExecutionTimeout, WriteConcernError)):
            return True
    except ImportError:
        pass
    return isinstance(exc, (OSError, TimeoutError))


def error_code(exc):
    return exc.code if isinstance(exc, PipelineError) else type(exc).__name__


def check_registration(row, saved, settings):
    doc = saved["document"]
    # Lineage and first input snapshot are deliberately frozen on first registration.
    expected = envelope({**row, "lineage": doc["lineage"]}, doc["_id"], doc["input_snapshot_id"], settings.image_bucket)
    if (doc != expected or saved["target_fingerprint"] != settings.fingerprint
            or saved["observation_key"] != row["observation_key"] or saved["identity"] != registration_identity(row)):
        raise Conflict("REGISTRATION_DOCUMENT_CONFLICT")
    if saved["event"] != event_for(doc, saved["event"]["event_id"], saved["event"]["occurred_at"]):
        raise Conflict("REGISTRATION_EVENT_CONFLICT")


def ingest(snapshot_dir, settings, ledger, objects, publisher, audit_dir, replay_events=False,
           progress=None, sleep=time.sleep, hook=None):
    snapshot_dir, audit_dir = Path(snapshot_dir).resolve(), Path(audit_dir).resolve()
    if audit_dir.is_relative_to(snapshot_dir) or snapshot_dir.is_relative_to(audit_dir):
        raise PipelineError("AUDIT_DIRECTORY_NOT_SEPARATE")
    record_proofs = {}
    manifest = verify(snapshot_dir, record_proofs)  # before opening any mutating backend
    run_id = str(uuid4())
    report = {"ingestion_run_id": run_id, "snapshot_id": manifest["snapshot_id"], "started_at": now(),
              "products": manifest["product_count"], "raw_confirmed": 0, "raw_created": 0,
              "events_sent": 0, "events_reused": 0, "objects_written": 0,
              "image_relations_available": 0, "image_relations_unavailable": 0,
              "gallery_missing": 0, "failed": 0, "errors": [], "result": "running"}
    audit_dir.mkdir(parents=True, exist_ok=True)
    report_path = audit_dir / "runs" / (run_id + ".json")

    def notify(stage, key=None, **extra):
        if progress:
            progress({"stage": stage, "observation_key": key, **extra})

    def point(stage):
        if hook:
            hook(stage)

    def perform(key, stage, operation, token=None, attempts=None):
        attempts = attempts or settings.max_attempts
        for index in range(attempts):
            execution = str(uuid4())
            started = time.monotonic()
            try:
                if token is not None:
                    ledger.renew(key, token)
                result = operation()
                ledger.attempt(key, stage, "SUCCEEDED", execution, round((time.monotonic() - started) * 1000))
                return result
            except Exception as exc:
                code = error_code(exc)
                # A local attempt is recoverable evidence when DB logging itself is unavailable.
                atomic_json(audit_dir / "attempts" / (execution + ".json"), {
                    "ingestion_run_id": run_id, "execution_id": execution, "observation_key": key,
                    "stage": stage, "attempt": index + 1, "status": "FAILED", "error_code": code,
                    "duration_ms": round((time.monotonic() - started) * 1000), "recorded_at": now()})
                try:
                    ledger.attempt(key, stage, "FAILED", execution, round((time.monotonic() - started) * 1000), error=code)
                except Exception:
                    pass  # local evidence above remains until Mongo can be reconciled
                if index + 1 >= attempts or not retryable(exc):
                    raise
                sleep(random.uniform(0, min(60, 2 ** index)))

    try:
        with ledger.exclusive():
            ledger.snapshot(manifest)
            processed_keys = set()
            for row in rows(snapshot_dir / "observations.jsonl"):
                key, token = row["observation_key"], None
                if key in processed_keys or record_proofs.get(key) != digest(row):
                    raise PipelineError("SNAPSHOT_CHANGED_DURING_INGEST")
                processed_keys.add(key)
                try:
                    archive_path = audit_dir / "registrations" / (key + ".json")
                    archived = read_json(archive_path) if archive_path.exists() else None
                    if archived:
                        check_registration(row, archived, settings)
                    saved = perform(key, "REGISTER", lambda: ledger.register(row, manifest, archived))
                    check_registration(row, saved, settings)
                    if archived is None:
                        atomic_json(archive_path, saved)
                    elif archived["document"] != saved["document"] or archived["event"] != saved["event"]:
                        raise Conflict("REGISTRATION_ARCHIVE_CONFLICT")
                    token = ledger.acquire(key)
                    point("REGISTERED")
                    vector = []
                    if not row["images"]:
                        report["gallery_missing"] += 1
                    for image in row["images"]:
                        slot = "representative" if image["role"] == "representative" else f"gallery:{image['order']}"
                        state = {"image_slot": slot, "checksum": image.get("sha256"),
                                 "bucket": settings.image_bucket if image.get("object_key") else None,
                                 "object_key": image.get("object_key"), "availability": "UNCONFIRMED",
                                 "verification": "sha256-bytes-v1"}
                        if image.get("object_key"):
                            source = confined(snapshot_dir, image["object_key"])
                            try:
                                written = perform(key, "IMAGE", lambda: objects.ensure(image["object_key"], source,
                                    image["sha256"], image["bytes"]), token)
                                report["objects_written"] += bool(written)
                                state["availability"] = "AVAILABLE"
                            except Conflict:
                                raise
                            except LeaseLost:
                                raise
                            except Exception as exc:
                                state["availability"] = "FAILED"
                                atomic_json(audit_dir / "image_errors" / (key + "-" + digest(slot) + ".json"), {
                                    "observation_key": key, "image_slot": slot, "error_code": error_code(exc)})
                        report["image_relations_available" if state["availability"] == "AVAILABLE"
                               else "image_relations_unavailable"] += 1
                        vector.append(state)
                    vector.sort(key=lambda i: i["image_slot"])
                    perform(key, "IMAGE_STATE", lambda: ledger.images(key, token, vector), token)
                    ledger.update(key, token, stage="IMAGES_RESOLVED")
                    point("IMAGES_RESOLVED")
                    created = perform(key, "RAW", lambda: ledger.ensure_raw(saved["document"]), token)
                    point("RAW_INSERTED_BEFORE_MARK")
                    ledger.update(key, token, stage="RAW_CONFIRMED")
                    report["raw_created"] += bool(created)
                    report["raw_confirmed"] += 1
                    point("RAW_CONFIRMED")
                    if not saved.get("event_confirmed") or replay_events:
                        ledger.update(key, token, stage="EVENT_PENDING")
                        try:
                            # Kafka performs bounded protocol retries itself, never multiply them here.
                            perform(key, "PUBLISH", lambda: publisher.send(saved["event"], saved["document"]["source_record_key"]),
                                    token, attempts=1)
                            point("EVENT_SENT_BEFORE_MARK")
                        except BaseException:
                            ledger.update(key, token, stage="EVENT_UNKNOWN")
                            raise
                        ledger.update(key, token, stage="EVENT_CONFIRMED", event_confirmed=True, last_error=None)
                        report["events_sent"] += 1
                    else:
                        ledger.update(key, token, stage="EVENT_CONFIRMED", last_error=None)
                        report["events_reused"] += 1
                    notify("observation_complete", key, raw_confirmed=report["raw_confirmed"], total=report["products"])
                except Exception as exc:
                    code = error_code(exc)
                    report["failed"] += 1
                    report["errors"].append({"observation_key": key, "error_code": code})
                    terminal = isinstance(exc, Conflict) or code in {"RAW_TOO_LARGE", "REGISTRATION_TOO_LARGE", "BSON_UNENCODABLE_INPUT"}
                    atomic_json(audit_dir / ("quarantine" if terminal else "failures") / (key + ".json"), {
                        "snapshot_id": manifest["snapshot_id"], "observation_key": key, "error_code": code,
                        "classification": "TERMINAL_INPUT" if terminal else "RETRY_REQUIRED",
                        "recoverable_input": "observations.jsonl", "recorded_at": now()})
                    if token is not None:
                        try:
                            ledger.update(key, token, last_error=code)
                        except Exception:
                            pass  # neither successful state nor a false completion is written
                    notify("observation_failed", key, error_code=code)
                finally:
                    if token is not None:
                        try:
                            ledger.release(key, token)
                        except Exception:
                            report["errors"].append({"observation_key": key, "error_code": "LEASE_RELEASE_UNCONFIRMED"})
                atomic_json(report_path, report)
            if len(processed_keys) != len(record_proofs):
                raise PipelineError("SNAPSHOT_CHANGED_DURING_INGEST")
        report["result"] = "partial" if (report["failed"] or report["errors"] or report["image_relations_unavailable"]
                                          or report["gallery_missing"]) else "completed"
    except KeyboardInterrupt:
        report["result"] = "interrupted"
        raise
    except BaseException:
        report["result"] = "failed"
        raise
    finally:
        report["finished_at"] = now()
        atomic_json(report_path, report)
    return report
