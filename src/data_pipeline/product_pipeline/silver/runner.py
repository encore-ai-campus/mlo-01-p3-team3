"""Commit outputs first, immutable evaluation receipt last. No cross-table transaction."""
import time
from uuid import uuid4

from ..contracts import canonical, digest, now
from ..errors import Conflict, PipelineError
from .inputs import image_state
from .normalize import KEYS, TABLES, normalize, semantic_hash
from .schemas import COMMON, complete


def process(doc, states, sink, inputs, *, processing_run_id, as_of, bronze_version, bronze_path,
            progress=None, hook=None, archive=None):
    started,execution= time.monotonic(),str(uuid4())
    raw_id=doc['_id']
    try:
        state=states.get(raw_id,doc['observation_key'])
        if state is None:raise PipelineError('SILVER_IMAGE_STATE_UNCONFIRMED')
        state_ref=archive(state) if archive is not None else None
        result=normalize(doc,state,inputs,processing_run_id=processing_run_id,as_of=as_of)
        sink.assert_revision(raw_id,result['image_state']['revision'],result['image_state']['fingerprint'])
        key=result['evaluation_key']
        receipts=sink.existing('pipeline_evaluations',{key})
        if receipts:
            receipt=receipts[key]
            if receipt['record_hash']!=semantic_hash(receipt) or receipt['processing_input_fingerprint']!=result['processing_input_fingerprint']:
                raise Conflict('SILVER_RECEIPT_CONFLICT')
            from ..files import parse
            manifest=parse(receipt['output_manifest_json'])
            for table,proof in manifest.items():
                expected={r[KEYS[table]]:r['record_hash'] for r in result['tables'][table]}
                if proof['records']!=expected:raise Conflict('SILVER_RECEIPT_OUTPUT_CONFLICT')
                sink.verify(table,expected,proof['version'])
            current=image_state(doc,states.get(raw_id,doc['observation_key']))
            if current['revision']!=result['image_state']['revision'] or current['fingerprint']!=result['image_state']['fingerprint']:
                raise PipelineError('SILVER_INPUT_CHANGED')
            return dict(status=receipt['status'],evaluation_key=key,reused=True,raw_document_id=raw_id,
                        output_counts={t:len(v) for t,v in result['tables'].items()})
        current=image_state(doc,states.get(raw_id,doc['observation_key']))
        if current['revision']!=result['image_state']['revision'] or current['fingerprint']!=result['image_state']['fingerprint']:
            raise PipelineError('SILVER_INPUT_CHANGED')
        manifest={}
        for table in TABLES:
            rows=result['tables'][table]
            sink.insert(table,rows)
            manifest[table]=dict(version=sink.version(table),records={r[KEYS[table]]:r['record_hash'] for r in rows})
            sink.verify(table,manifest[table]['records'],manifest[table]['version'])
            if hook:hook(table)
        # A changed image input may leave idempotent partial outputs, never a successful receipt.
        current=image_state(doc,states.get(raw_id,doc['observation_key']))
        if current['revision']!=result['image_state']['revision'] or current['fingerprint']!=result['image_state']['fingerprint']:
            raise PipelineError('SILVER_INPUT_CHANGED')
        base={k:v for k,v in result['tables']['product_observations'][0].items() if k in COMMON}
        receipt=complete('pipeline_evaluations',dict(base,stage='SILVER',bronze_version=bronze_version,
            bronze_path=bronze_path,input_snapshot_refs_json=canonical({**{k:v['content_hash'] for k,v in inputs.items()},'image_state_ref':state_ref}),
            related_hashes_json=canonical(result['related_hashes']),output_manifest_json=canonical(manifest),
            status=result['status'],quality_reason_codes=result['quality_reason_codes'],completed_at=now()))
        receipt['record_hash']=semantic_hash(receipt)
        if hook:hook('BEFORE_RECEIPT')
        sink.insert('pipeline_evaluations',[receipt])
        return dict(status=result['status'],evaluation_key=key,reused=False,raw_document_id=raw_id,
                    output_counts={t:len(v) for t,v in result['tables'].items()})
    except Exception as exc:
        if progress:progress(dict(stage='SILVER',status='RETRY_WAIT',raw_document_id=raw_id,
            error_code=exc.code if isinstance(exc,PipelineError) else type(exc).__name__,execution_id=execution,
            duration_ms=round((time.monotonic()-started)*1000)))
        raise
    finally:
        if progress:progress(dict(stage='SILVER_ATTEMPT',raw_document_id=raw_id,execution_id=execution,
            duration_ms=round((time.monotonic()-started)*1000)))
