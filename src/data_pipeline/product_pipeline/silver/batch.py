"""Bounded output batches; every receipt still follows verified durable outputs."""
from ..contracts import canonical,now
from ..errors import Conflict,PipelineError
from ..files import parse
from .inputs import image_state
from .normalize import normalize,evaluation_input,TABLES,KEYS,semantic_hash
from .schemas import COMMON,complete
from ..spark_session import bump,measured,timed


@measured('silver_batch')
def process_many(docs,states,sink,inputs,*,processing_run_id,as_of,bronze_version,bronze_path,archive=None,hook=None,reuse_choices=None,archive_many=None):
    from contextlib import nullcontext
    with sink.batch_scope() if hasattr(sink,'batch_scope') else nullcontext():
        return _process_many(docs,states,sink,inputs,processing_run_id=processing_run_id,as_of=as_of,
            bronze_version=bronze_version,bronze_path=bronze_path,archive=archive,hook=hook,reuse_choices=reuse_choices,archive_many=archive_many)


def _process_many(docs,states,sink,inputs,*,processing_run_id,as_of,bronze_version,bronze_path,archive=None,hook=None,reuse_choices=None,archive_many=None):
    if not 1<=len(docs)<=100:raise PipelineError('SILVER_BATCH_LIMIT')
    prepared=[];keys=set();reused=[];pending=[]
    captured=[]
    for doc in docs:
        with timed('silver_input_state_read'):
            state=states.get(doc['_id'],doc['observation_key'])
        candidate=(reuse_choices or {}).get(doc['_id'])
        result=None
        if candidate:
            if (candidate['observation_key']!=doc['observation_key'] or candidate['product_key']!=doc['source_code']+':'+doc['source_product_id']):
                raise Conflict('SILVER_REUSE_OBSERVATION_CHANGED')
            with timed('silver_reuse_identity_check'):
                identity,_,_=evaluation_input(doc,state,inputs,as_of)
            if identity['evaluation_key']==candidate['evaluation_key']:
                result=dict(identity,tables=None)
        if result is None:
            with timed('silver_normalization'):
                result=normalize(doc,state,inputs,processing_run_id=processing_run_id,as_of=as_of)
        key=result['evaluation_key']
        if key in keys:raise Conflict('SILVER_DUPLICATE_BATCH_INPUT')
        keys.add(key)
        captured.append(state);prepared.append((doc,result,None))
    bump('silver_input_products',len(docs))
    with timed('silver_input_archive'):
        refs=(archive_many(captured) if archive_many else
              [archive(state) if archive else None for state in captured])
    bump('silver_archived_input_states',len(captured) if archive_many or archive else 0)
    if len(refs)!=len(prepared):raise PipelineError('SILVER_ARCHIVE_BATCH_INCOMPLETE')
    prepared=[(doc,result,ref) for (doc,result,_),ref in zip(prepared,refs)]
    revisions=[(doc['_id'],r['image_state']['revision'],r['image_state']['fingerprint']) for doc,r,_ in prepared]
    with timed('silver_input_revision_check'):
        if hasattr(sink,'assert_revisions'):sink.assert_revisions(revisions)
        else:
            for values in revisions:sink.assert_revision(*values)
    with timed('silver_receipt_lookup'):
        existing=sink.existing('pipeline_evaluations',keys)
    bump('silver_receipt_lookup_keys',len(keys))
    proofs={}
    with timed('silver_existing_receipt_validation'):
        for doc,result,state_ref in prepared:
            key=result['evaluation_key'];old=existing.get(key)
            if not old:
                if result['tables'] is None:raise Conflict('SILVER_REUSE_RECEIPT_MISSING')
                pending.append((doc,result,state_ref));continue
            if old['record_hash']!=semantic_hash(old) or old['processing_input_fingerprint']!=result['processing_input_fingerprint']:
                raise Conflict('SILVER_RECEIPT_CONFLICT')
            manifest=parse(old['output_manifest_json'])
            if set(manifest)!=set(TABLES):raise Conflict('SILVER_RECEIPT_OUTPUT_CONFLICT')
            if result['tables'] is None:
                # A candidate is only a hint. Durable receipt hash, exact input
                # identity, output proofs and live image state still authorize reuse.
                if any(old.get(k)!=v for k,v in {'raw_document_id':doc['_id'],'observation_key':doc['observation_key'],
                        'product_key':doc['source_code']+':'+doc['source_product_id'],'evaluation_key':key}.items()):
                    raise Conflict('SILVER_REUSE_RECEIPT_CONFLICT')
                result.update(status=old['status'],output_counts={t:len(p['records']) for t,p in manifest.items()})
            for table,proof in manifest.items():
                expected=(proof['records'] if result['tables'] is None else {r[KEYS[table]]:r['record_hash'] for r in result['tables'][table]})
                if proof['records']!=expected:raise Conflict('SILVER_RECEIPT_OUTPUT_CONFLICT')
                proofs.setdefault((table,proof['version']),{}).update(expected)
            reused.append(key)
    prior=[(t,v,e) for (t,v),e in proofs.items()]
    with timed('silver_prior_output_proof'):
        if hasattr(sink,'verify_many'):sink.verify_many(prior)
        else:
            for table,version,expected in prior:sink.verify(table,expected,version)
    bump('silver_reused_products',len(reused));bump('silver_pending_products',len(pending))
    with timed('silver_output_rows_prepare'):
        manifests={r['evaluation_key']:{} for _,r,_ in pending}
        output_rows={table:[row for _,result,_ in pending for row in result['tables'][table]] for table in TABLES}
    for table,rows in output_rows.items():bump('silver_output_rows_'+table,len(rows))
    with timed('silver_output_persist'):
        if pending and hasattr(sink,'insert_many'):sink.insert_many(output_rows,hook=hook)
        else:
            for table,rows in output_rows.items():
                if pending:sink.insert(table,rows)
                if hook:hook(table)
    if pending:
        with timed('silver_output_version_lookup'):
            versions=sink.versions_many(TABLES) if hasattr(sink,'versions_many') else {t:sink.version(t) for t in TABLES}
        proofs=[]
        for table,rows in output_rows.items():
            version=versions[table]
            proofs.append((table,version,{r[KEYS[table]]:r['record_hash'] for r in rows}))
            for _,result,_ in pending:
                manifests[result['evaluation_key']][table]=dict(version=version,
                    records={r[KEYS[table]]:r['record_hash'] for r in result['tables'][table]})
        with timed('silver_new_output_proof'):
            if hasattr(sink,'verify_many'):sink.verify_many(proofs)
            else:
                for table,version,expected in proofs:sink.verify(table,expected,version)
    # A changed state invalidates the batch before any new successful receipt.
    with timed('silver_live_input_recheck'):
        for doc,result,_ in prepared:
            current=image_state(doc,states.get(doc['_id'],doc['observation_key']))
            if any(current[k]!=result['image_state'][k] for k in ('revision','fingerprint')):
                raise PipelineError('SILVER_INPUT_CHANGED')
    receipts=[]
    with timed('silver_receipt_build'):
        for doc,result,state_ref in pending:
            base={k:v for k,v in result['tables']['product_observations'][0].items() if k in COMMON}
            receipt=complete('pipeline_evaluations',dict(base,stage='SILVER',bronze_version=bronze_version,bronze_path=bronze_path,
                input_snapshot_refs_json=canonical({**{k:v['content_hash'] for k,v in inputs.items()},'image_state_ref':state_ref}),
                related_hashes_json=canonical(result['related_hashes']),output_manifest_json=canonical(manifests[result['evaluation_key']]),
                status=result['status'],quality_reason_codes=result['quality_reason_codes'],completed_at=now()))
            receipt['record_hash']=semantic_hash(receipt);receipts.append(receipt)
    bump('silver_pending_receipts',len(receipts))
    with timed('silver_receipt_persist'):
        if hook:hook('BEFORE_RECEIPT')
        if receipts:
            sink.insert('pipeline_evaluations',receipts)
            sink.verify('pipeline_evaluations',{r['evaluation_key']:r['record_hash'] for r in receipts},sink.version('pipeline_evaluations'))
    skipped=sum(r['tables'] is None for _,r,_ in prepared)
    bump('silver_normalization_skipped',skipped);bump('silver_normalizations',len(prepared)-skipped)
    return [dict(status=r['status'],evaluation_key=r['evaluation_key'],reused=r['evaluation_key'] in reused,
        normalization_skipped=r['tables'] is None,raw_document_id=doc['_id'],
        output_counts=r['output_counts'] if r['tables'] is None else {t:len(v) for t,v in r['tables'].items()}) for doc,r,_ in prepared]
