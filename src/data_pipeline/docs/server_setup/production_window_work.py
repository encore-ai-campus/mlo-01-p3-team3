"""Advance at most 100 committed source observations through publication at a time."""
import os,sys,subprocess
from pathlib import Path
CODE_ROOT=Path(__file__).resolve().parent
sys.path.insert(0,str(CODE_ROOT/'source'))
from production_work import setup,pinned,LAKE,ROOT
import production_work as work
from product_pipeline.files import read_json,atomic_json,lock
from product_pipeline.windows import run_windows
from product_pipeline.contracts import now
from product_pipeline.spark_session import SessionScope
from product_pipeline.spark_session import scope_active,metrics


def main():
    if scope_active():
        before=metrics()
        try:return run()
        finally:atomic_json(ROOT/'batch-runtime.json',{k:v-before.get(k,0) for k,v in metrics().items()})
    with SessionScope() as runtime:
        try:return run()
        finally:atomic_json(ROOT/'batch-runtime.json',dict(runtime.metrics))


def run_at(directory):
    """Borrow the coordinator Driver only after Bronze query quiescence."""
    global ROOT
    previous,previous_work=ROOT,work.ROOT
    ROOT=Path(directory);work.ROOT=ROOT
    try:return main()
    finally:ROOT=previous;work.ROOT=previous_work


def run():
    config,options=setup()
    with lock(ROOT/'windows.lock'):
        scope_path=ROOT/'window-range.json'
        if not scope_path.exists():
            atomic_json(scope_path,dict(bronze_version=pinned(options,LAKE+'/bronze',['products'])['products'],
                base_bronze_version=int(os.environ.get('PIPELINE_BASE_BRONZE_VERSION','-1'))))
        scope=read_json(scope_path)
        def execute(stage,cut):
            result=work.run_at(stage,cut)
            if stage!='plan':
                result['window_hash']=read_json(cut/'selection.json')['content_hash'];atomic_json(cut/(stage+'-result.json'),result)
            return result
        def progress(state):
            atomic_json(ROOT/'window-status.json',dict(state,updated_at=now()))
        result=run_windows(ROOT,scope['bronze_version'],execute,progress=progress,base_version=scope['base_bronze_version'],
            pause_requested=lambda state:(CODE_ROOT/'pause-after-window.json').exists())
        from product_pipeline.publication.mysql import MySQL
        db=MySQL(config)
        try:
            counts=db.one("SELECT COUNT(*) AS products,SUM(status='ACTIVE') AS active,SUM(status='PENDING_REVIEW') AS pending,SUM(recommendation_ready) AS recommendation_ready FROM products WHERE source_code='danawa'")
            result['counts']={k:int(v or 0) for k,v in counts.items()}
        finally:db.close()
        atomic_json(ROOT/'windows-result.json',dict(**result,finished_at=now()))
        return 75 if result['status']=='PAUSED_AFTER_BATCH' else 0


if __name__=='__main__':raise SystemExit(main())
