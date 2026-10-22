"""One verified Delta write per table for a bounded product batch."""
from ..contracts import canonical
from ..errors import Conflict,PipelineError
from ..files import parse
from .build import build
from .contracts import semantic_hash
from .storage import columns
from .identity import binding
from ..spark_session import bump,measured,timed


@measured('gold_batch')
def process_many(groups,inputs,versions,sink,*,processing_run_id,as_of,input_refs=None,hook=None):
    from contextlib import nullcontext
    with sink.batch_scope() if hasattr(sink,'batch_scope') else nullcontext():
        return _process_many(groups,inputs,versions,sink,processing_run_id=processing_run_id,as_of=as_of,input_refs=input_refs,hook=hook)


def _process_many(groups,inputs,versions,sink,*,processing_run_id,as_of,input_refs=None,hook=None):
    if not 1<=len(groups)<=100:raise PipelineError('GOLD_BATCH_LIMIT')
    with timed('gold_build_results'):
        results=[build(g,inputs,versions,processing_run_id=processing_run_id,as_of=as_of) for g in groups]
    bump('gold_input_products',len(groups))
    with timed('gold_output_prepare'):
        keys={r['evaluation_key'] for r in results}
        if len(keys)!=len(results):raise Conflict('GOLD_DUPLICATE_BATCH_INPUT')
        for result in results:
            for table,rows in result['tables'].items():
                for row in rows:
                    for name in columns(table):row.setdefault(name,None)
                    row['record_hash']=semantic_hash(row)
    with timed('gold_receipt_lookup'):
        existing=sink.existing('pipeline_evaluations',keys)
    bump('gold_receipt_lookup_keys',len(keys))
    pending=[];proofs={}
    with timed('gold_existing_receipt_validation'):
        for result in results:
            old=existing.get(result['evaluation_key'])
            if not old:pending.append(result);continue
            if old['record_hash']!=semantic_hash(old) or old['input_fingerprint']!=result['input_fingerprint']:
                raise Conflict('GOLD_RECEIPT_CONFLICT')
            manifest=parse(old['output_manifest_json'])
            if set(manifest)!=set(result['tables']):raise Conflict('GOLD_RECEIPT_TABLE_CONFLICT')
            for table,proof in manifest.items():
                expected={r['row_key']:r['record_hash'] for r in result['tables'][table]}
                if proof['records']!=expected:raise Conflict('GOLD_RECEIPT_OUTPUT_CONFLICT')
                proofs.setdefault((table,proof['version']),{}).update(expected)
    prior=[(table,version,expected) for (table,version),expected in proofs.items()]
    with timed('gold_prior_output_proof'):
        if hasattr(sink,'verify_many'):sink.verify_many(prior)
        else:
            for table,version,expected in prior:sink.verify(table,expected,version)
    bump('gold_reused_products',len(existing));bump('gold_pending_products',len(pending))
    with timed('gold_output_rows_prepare'):
        manifests={r['evaluation_key']:{} for r in pending}
        output_rows={table:[row for result in pending for row in result['tables'][table]] for table in results[0]['tables']}
    for table,rows in output_rows.items():bump('gold_output_rows_'+table,len(rows))
    with timed('gold_output_persist'):
        if pending and hasattr(sink,'insert_many'):sink.insert_many(output_rows,hook=hook)
        else:
            for table,rows in output_rows.items():
                if pending:sink.insert(table,rows)
                if hook:hook(table)
    if pending:
        tables=list(results[0]['tables'])
        with timed('gold_output_version_lookup'):
            versions_out=sink.versions_many(tables) if hasattr(sink,'versions_many') else {t:sink.version(t) for t in tables}
        proofs=[]
        for table in tables:
            rows=output_rows[table];version=versions_out[table]
            proofs.append((table,version,{r['row_key']:r['record_hash'] for r in rows}))
            for result in pending:manifests[result['evaluation_key']][table]=dict(version=version,
                records={r['row_key']:r['record_hash'] for r in result['tables'][table]})
        with timed('gold_new_output_proof'):
            if hasattr(sink,'verify_many'):sink.verify_many(proofs)
            else:
                for table,version,expected in proofs:sink.verify(table,expected,version)
    receipts=[]
    with timed('gold_receipt_build'):
        for result in pending:
            catalog=result['tables']['product_catalog'][0]
            receipt={k:catalog[k] for k in columns('pipeline_evaluations') if k in catalog}
            receipt.update(row_key=result['evaluation_key'],output_manifest_json=canonical(manifests[result['evaluation_key']]),
                input_refs_json=canonical(input_refs or {}),status=result['status'],reason_codes=result['reason_codes'])
            for name in columns('pipeline_evaluations'):receipt.setdefault(name,None)
            receipt['record_hash']=semantic_hash(receipt);receipts.append(receipt)
    bump('gold_pending_receipts',len(receipts))
    with timed('gold_receipt_persist'):
        if hook:hook('BEFORE_RECEIPT')
        if receipts:
            sink.insert('pipeline_evaluations',receipts)
            sink.verify('pipeline_evaluations',{r['row_key']:r['record_hash'] for r in receipts},sink.version('pipeline_evaluations'))
    all_receipts={**existing,**{r['row_key']:r for r in receipts}}
    return [dict(evaluation_key=r['evaluation_key'],status=r['status'],reused=r['evaluation_key'] in existing,
        receipt_binding=binding(all_receipts[r['evaluation_key']],input_refs),next_due_at=r['next_due_at']) for r in results]
