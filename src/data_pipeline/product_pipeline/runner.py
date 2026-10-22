"""Snapshot batch orchestration; each observation is independently recoverable."""
from pathlib import Path
import time
from uuid import uuid4

from .contracts import digest, now
from .errors import PipelineError
from .files import atomic_json, rows
from .snapshot import verify
from .ports import IngestionLedger, ImageStore, EventPublisher
from .retry import retryable, error_code
from .infrastructure.ingestion_audit import FileAudit
from .ingestion.attempts import Attempts
from .ingestion.observation import check_registration, process


def ingest(snapshot_dir, settings, ledger: IngestionLedger, objects: ImageStore, publisher: EventPublisher, audit_dir, replay_events=False,
           progress=None, sleep=time.sleep, hook=None, repair_raw=False):
    snapshot_dir, audit_dir = Path(snapshot_dir).resolve(), Path(audit_dir).resolve()
    if audit_dir.is_relative_to(snapshot_dir) or snapshot_dir.is_relative_to(audit_dir):
        raise PipelineError("AUDIT_DIRECTORY_NOT_SEPARATE")
    record_proofs = {}
    manifest = verify(snapshot_dir, record_proofs)  # before opening any mutating backend
    run_id = str(uuid4())
    report = {"ingestion_run_id": run_id, "snapshot_id": manifest["snapshot_id"], "started_at": now(),
              "products": manifest["product_count"], "raw_confirmed": 0, "raw_created": 0,
              "events_sent": 0, "events_reused": 0, "objects_written": 0,
              "observations_reused": 0, "stages_reused": 0, "raw_reused": 0,
              "image_relations_reused": 0,
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

    audit = FileAudit(audit_dir)
    perform = Attempts(settings, ledger, audit, run_id, sleep).perform

    try:
        with ledger.exclusive():
            ledger.snapshot(manifest)
            processed_keys = set()
            for row in rows(snapshot_dir / "observations.jsonl"):
                key = row["observation_key"]
                if key in processed_keys or record_proofs.get(key) != digest(row):
                    raise PipelineError("SNAPSHOT_CHANGED_DURING_INGEST")
                processed_keys.add(key)
                process(row, snapshot_dir=snapshot_dir, settings=settings, ledger=ledger,
                        objects=objects, publisher=publisher, audit=audit, manifest=manifest,
                        report=report, perform=perform, point=point, notify=notify,
                        repair_raw=repair_raw, replay_events=replay_events)
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
