"""Gold output confirmation precedes receipt; partial commits are replayable."""
from ..contracts import canonical
from ..errors import Conflict
from ..files import parse
from .build import build
from .contracts import semantic_hash
from .storage import columns
from .identity import binding


def process(groups, inputs, versions, sink, *, processing_run_id, as_of, input_refs=None, hook=None):
    result=build(groups,inputs,versions,processing_run_id=processing_run_id,as_of=as_of)
    # Fill nullable schema columns before hashing so persisted rows have exactly the same meaning.
    for table,rows in result['tables'].items():
        for row in rows:
            for name in columns(table): row.setdefault(name,None)
            row['record_hash']=semantic_hash(row)
    key=result['evaluation_key']
    old=sink.existing('pipeline_evaluations',{key})
    if old:
        receipt=old[key]
        if receipt['record_hash']!=semantic_hash(receipt) or receipt['input_fingerprint']!=result['input_fingerprint']:
            raise Conflict('GOLD_RECEIPT_CONFLICT')
        manifest=parse(receipt['output_manifest_json'])
        if set(manifest)!=set(result['tables']):raise Conflict('GOLD_RECEIPT_TABLE_CONFLICT')
        for table,proof in manifest.items():
            expected={r['row_key']:r['record_hash'] for r in result['tables'][table]}
            if proof['records']!=expected:raise Conflict('GOLD_RECEIPT_OUTPUT_CONFLICT')
            sink.verify(table,expected,proof['version'])
        return dict(evaluation_key=key,status=receipt['status'],reused=True,receipt_binding=binding(receipt,input_refs),next_due_at=result['next_due_at'])
    manifest={}
    for table,rows in result['tables'].items():
        sink.insert(table,rows)
        expected={r['row_key']:r['record_hash'] for r in rows}
        version=sink.version(table);sink.verify(table,expected,version)
        manifest[table]=dict(version=version,records=expected)
        if hook:hook(table)
    catalog=result['tables']['product_catalog'][0]
    receipt={k:catalog[k] for k in columns('pipeline_evaluations') if k in catalog}
    receipt.update(row_key=key,output_manifest_json=canonical(manifest),input_refs_json=canonical(input_refs or {}),
                   status=result['status'],reason_codes=result['reason_codes'])
    for name in columns('pipeline_evaluations'): receipt.setdefault(name,None)
    receipt['record_hash']=semantic_hash(receipt)
    if hook:hook('BEFORE_RECEIPT')
    sink.insert('pipeline_evaluations',[receipt])
    sink.verify('pipeline_evaluations',{key:receipt['record_hash']},sink.version('pipeline_evaluations'))
    return dict(evaluation_key=key,status=result['status'],reused=False,receipt_binding=binding(receipt,input_refs),next_due_at=result['next_due_at'])
