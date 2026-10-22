"""Finish the paused window using its proven Silver choices, then stay paused."""
import os,sys,shutil
from pathlib import Path
ROOT=Path('/var/lib/pj3/product-pipeline/spark/production-pending-20261019')
sys.path.insert(0,str(ROOT));sys.path.insert(0,str(ROOT/'source'))
from product_pipeline.files import read_json,atomic_json,sha_file,lock
from product_pipeline.contracts import now
from product_pipeline.silver.selection import recover
from product_pipeline.spark_session import SessionScope
from product_pipeline.windows import run_windows
from production_work import setup,run_at


def main():
    os.environ['PIPELINE_CONTINUOUS']='1'
    parent=ROOT/'bounded-downstream-v1';cut=parent/'windows/000000';old=cut/'activation';repair=cut/'activation-repair-v1'
    with lock(parent/'windows.lock'):
        if (cut/'activation-result.json').exists():return 0
        if read_json(old/'silver-result.json')['status']!='COMPLETED':raise RuntimeError('SILVER_SUCCESS_REQUIRED')
        scope=read_json(cut/'selection.json');repair.mkdir(exist_ok=True,mode=0o700)
        setup()
        from bson import ObjectId
        from pymongo import MongoClient,ReadPreference
        from pymongo.read_concern import ReadConcern
        from product_pipeline.files import parse
        text=(old/'silver.private.log').read_text()
        rows=[parse(x) for x in text.splitlines() if x.startswith('{') and x.endswith('}')]
        ids={r['raw_document_id'] for r in rows if r.get('stage')=='silver_observation'}
        client=MongoClient(os.environ['PIPELINE_MONGO_URI'],tz_aware=True,serverSelectionTimeoutMS=5000)
        try:
            db=client.get_database('pj3_products_raw',read_preference=ReadPreference.PRIMARY,read_concern=ReadConcern('majority'))
            raw=list(db.raw_products.find({'_id':{'$in':[ObjectId(x) for x in ids]}},
                {'source_code':1,'source_product_id':1,'observation_key':1}))
        finally:client.close()
        choices=recover(text,raw,scope)
        for name,source in [('selection.json',cut),('silver-versions.json',old),('silver-result.json',old)]:
            p=repair/name
            if p.exists() and p.read_bytes()!=(source/name).read_bytes():raise RuntimeError('RECOVERY_INPUT_CHANGED')
            if not p.exists():shutil.copyfile(source/name,p)
        choice_path=repair/'silver-state/evaluation-selection.json'
        if choice_path.exists() and read_json(choice_path)!=choices:raise RuntimeError('RECOVERY_SELECTION_CHANGED')
        if not choice_path.exists():atomic_json(choice_path,choices)
        atomic_json(repair/'recovery-evidence.json',dict(window_hash=scope['content_hash'],
            silver_log_sha256=sha_file(old/'silver.private.log'),silver_result_sha256=sha_file(old/'silver-result.json'),
            selection_hash=choices['content_hash'],mode='COMPLETED_SILVER_RECEIPT_SELECTION'))
        with SessionScope() as runtime:
            try:
                for stage in ['gold','publish']:
                    result=repair/(stage+'-result.json')
                    if result.exists() and read_json(result).get('status')=='COMPLETED':continue
                    run_at(stage,repair)
            finally:atomic_json(repair/'batch-runtime.json',dict(runtime.metrics))
        published=read_json(repair/'publish-result.json')
        atomic_json(cut/'activation-result.json',dict(status='COMPLETED',stage='activation',window_hash=scope['content_hash'],
            reason='EXPLICIT_COMPLETED_SILVER_SELECTION_RECOVERED',reviewed=published.get('reviewed'),
            repair_result_ref=str(repair/'publish-result.json'),finished_at=now()))
        def no_stage(*args):raise RuntimeError('UNEXPECTED_NEW_STAGE_DURING_PAUSE')
        state=run_windows(parent,scope['bronze_version'],no_stage,base_version=scope['base_bronze_version'],pause_requested=lambda state:True)
        atomic_json(ROOT/'pause-after-batch.json',dict(status=state['status'],completed_window=0,next_window_started=False,counts=published['counts']))
        progress=read_json(ROOT/'downstream-progress.json');progress.update(status=state['status'],updated_at=now())
        atomic_json(ROOT/'downstream-progress.json',progress)
        return 0


if __name__=='__main__':
    try:raise SystemExit(main())
    except Exception as exc:
        atomic_json(ROOT/'batch-repair-failure.json',dict(error_code=getattr(exc,'code',type(exc).__name__),at=now()))
        raise
