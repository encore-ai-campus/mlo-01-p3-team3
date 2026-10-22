"""Drain sealed capture batches independently of the read-only source cursor."""
from contextlib import ExitStack
from pathlib import Path
import time

from .config import Settings
from .contracts import now
from .errors import PipelineError
from .files import atomic_json, lock, read_json
from .runner import ingest
from .snapshot import verify


def drain_spool(spool, state, config=None, rehearse=False, progress=None):
    spool, state = Path(spool).resolve(), Path(state).resolve()
    if spool.is_relative_to(state) or state.is_relative_to(spool):
        raise PipelineError('HANDOFF_STATE_NOT_SEPARATE')
    if rehearse == bool(config):
        raise PipelineError('SELECT_REHEARSAL_OR_LIVE_CONFIG')
    settings = Settings.load(config) if config else Settings('rehearsal', 'rehearsal', 'rehearsal-images')
    started = time.monotonic()
    result = dict(result='DRAINED', batches_ingested=0, batches_reused=0, failed=0,
                  image_relations_unavailable=0, target_fingerprint=settings.fingerprint)
    state.mkdir(parents=True, exist_ok=True)
    # A scheduler overlap is rejected by an OS lock; Mongo fences across different hosts.
    with lock(state/'drain.lock'), ExitStack() as stack:
        mode = dict(mode='rehearse' if rehearse else 'ingest', target_fingerprint=settings.fingerprint)
        marker = state/'mode.json'
        if marker.exists() and read_json(marker) != mode:
            raise PipelineError('STATE_TARGET_OR_MODE_MISMATCH')
        atomic_json(marker, mode)
        if rehearse:
            from .local import FileLedger, FileObjects, FilePublisher
            ledger, objects, publisher = FileLedger(state/'ledger', settings), FileObjects(state/'objects'), FilePublisher(state/'events')
        else:
            from .bootstrap import open_live as _live
            ledger, objects, publisher = _live(settings, stack)
            objects.preflight()
            publisher.preflight()
        cursor = read_json(spool/'cursor.json')  # fixed committed capture range for this run
        predecessor = None
        for snapshot_id in cursor['chain']:
            directory = spool/'batches'/snapshot_id
            manifest = verify(directory)
            if (manifest['snapshot_id'] != snapshot_id or manifest['predecessor_snapshot_id'] != predecessor
                    or manifest['subscription'] != cursor['subscription']):
                raise PipelineError('HANDOFF_CHAIN_INVALID')
            predecessor = snapshot_id
            receipt = state/'receipts'/(snapshot_id+'.json')
            expected = dict(snapshot_id=snapshot_id, target_fingerprint=settings.fingerprint, status='INGEST_DRAINED')
            if receipt.exists():
                saved = read_json(receipt)
                if any(saved.get(k) != v for k, v in expected.items()):
                    raise PipelineError('HANDOFF_RECEIPT_CONFLICT')
                result['batches_reused'] += 1
                continue
            report = ingest(directory, settings, ledger, objects, publisher, state/'audit', progress=progress)
            result['batches_ingested'] += 1
            result['image_relations_unavailable'] += report['image_relations_unavailable']
            if (report['failed'] or report['errors'] or report['raw_confirmed'] != manifest['product_count']
                    or report['events_sent']+report['events_reused'] != manifest['product_count']):
                result.update(result='RETRY_REQUIRED', failed=result['failed']+1)
                break
            # Metadata-only partial images do not imply DB/event failure or recommendation approval.
            atomic_json(receipt, dict(expected, image_relations_unavailable=report['image_relations_unavailable'],
                                     recorded_at=now(), ingestion_run_id=report['ingestion_run_id']))
        result['duration_ms'] = round((time.monotonic()-started)*1000)
        atomic_json(state/'drain_report.json', result)
    return result
