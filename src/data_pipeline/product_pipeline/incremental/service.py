"""Capture -> durable enqueue -> verified scoped execution -> exact acknowledgement."""
from pathlib import Path
from ..contracts import now
from ..errors import Conflict,PipelineError
from ..files import read_json,sha_file,atomic_json
from .policy import batches,STAGES


def discover(source,state,queue,*,at=None,full=False):
    at=at or now();full=full or state.reconciliation_due(at)
    captures=[];scheduled=0
    def flush(values):
        nonlocal scheduled
        for batch in batches(values):
            path=state.archive_work(batch)
            task=queue.enqueue('REEVALUATE',dict(work=str(path),work_sha256=sha_file(path)))
            state.queued(batch,task['task_id'])
            source.acknowledge(batch)  # only after both the work and queue are durable
            scheduled+=len(batch)
    for snapshot in source.snapshots(full=full,due_keys=state.due_keys(at),state=state,at=at):
        capture=state.capture(snapshot,at=at,full=full)
        if capture:captures.append(capture)
        else:source.acknowledge([dict(snapshot=snapshot)])
        if len(captures)>=100:flush(captures);captures=[]
    flush(captures)
    source.discovery_completed()
    if full:state.reconciled(at)
    return dict(scheduled=scheduled,full_reconciliation=full)


def execute(spec,state,backend):
    work=Path(spec['work'])
    if not work.resolve().is_relative_to(state.root/'work') or sha_file(work)!=spec['work_sha256']:
        raise Conflict('INCREMENTAL_WORK_CHANGED')
    job=read_json(work);captures=job['captures']
    if job['target']!=state.target:raise Conflict('INCREMENTAL_WORK_TARGET_CHANGED')
    directory=state.root/'executions'/work.stem
    directory.mkdir(parents=True,exist_ok=True)
    if not state.current(captures) or not backend.current(captures):
        state.superseded(captures);return dict(status='SUPERSEDED')
    stage=backend.freeze(directory,captures)
    for name in STAGES[STAGES.index(stage):]:
        if name=='publish' and (not state.current(captures) or not backend.current(captures)):
            state.superseded(captures);return dict(status='SUPERSEDED')
        path=directory/(name+'-result.json')
        if path.exists():
            if read_json(path).get('status')!='COMPLETED':raise Conflict('INCREMENTAL_STAGE_RECEIPT_INVALID')
        else:
            result=backend.run(name,directory)
            if result.get('status')!='COMPLETED':raise PipelineError('INCREMENTAL_STAGE_UNCONFIRMED')
            atomic_json(path,result)
    receipts=backend.verify(directory,captures)
    if set(receipts)!={c['product_key'] for c in captures}:raise Conflict('INCREMENTAL_RECEIPT_SCOPE_CHANGED')
    # Even a completed QUALITY_WAIT is acknowledged once. Only a dependency
    # change, expiry or a reconciliation repair schedules it again.
    for capture in captures:state.complete(capture,receipts[capture['product_key']])
    return dict(status='COMPLETED',products=len(captures),execution_root=str(directory))
