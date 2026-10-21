"""Reevaluate saved Bronze for missing seller offers; preserve original runs and raw."""
import os
import sys
from pathlib import Path

CODE_ROOT = Path(__file__).resolve().parent
os.environ['PIPELINE_EXECUTION_ROOT'] = str(CODE_ROOT / 'offer-recovery-v1')
os.environ['PIPELINE_CONTINUOUS'] = '1'
os.environ['PIPELINE_BASE_BRONZE_VERSION'] = '-1'
sys.path.insert(0, str(CODE_ROOT))
sys.path.insert(0, str(CODE_ROOT / 'source'))

if __name__ == '__main__':
    import production_work as work
    from product_pipeline.files import lock, read_json, atomic_json
    from product_pipeline.contracts import now
    from product_pipeline.windows import run_windows, seal_window
    from product_pipeline.spark_session import SessionScope
    from product_pipeline.silver.source import BronzeSource
    from product_pipeline.silver.config import SilverSettings
    root = Path(os.environ['PIPELINE_EXECUTION_ROOT'])
    root.mkdir(exist_ok=True, mode=0o700)
    with lock(CODE_ROOT / 'offer-recovery.lock'):
        with SessionScope() as runtime:
            _, options = work.setup()
            scope = root / 'window-range.json'
            if not scope.exists():
                atomic_json(scope, dict(bronze_version=work.pinned(options,work.LAKE+'/bronze',['products'])['products']))
            version = read_json(scope)['bronze_version']
            def execute(stage, cut):
                if stage != 'plan':
                    return work.run_at(stage,cut)
                # First ten establish actual MySQL publication, then continue in 100-row windows.
                spark = work.session(options)
                try:
                    source = BronzeSource(spark, SilverSettings(environment_id='pj3-danawa',
                        mongo_database='pj3_products_raw',bronze_path=work.LAKE+'/bronze/products',
                        silver_root=work.LAKE+'/silver',checkpoint_path=work.LAKE+'/checkpoints/silver',
                        max_records=10 if cut.name=='000000' else 100), version)
                    after = read_json(cut/'scope.json')['after']
                    docs = source.page(after)
                    if not docs: return dict(status='COMPLETED',drained=True)
                    value = seal_window(version,after,docs)
                    atomic_json(cut/'selection.json',value)
                    return dict(status='COMPLETED',observations=len(docs),products=len(value['product_keys']))
                finally: work.release_spark(spark)
            def progress(state):
                atomic_json(CODE_ROOT/'offer-recovery-progress.json',dict(state,updated_at=now()))
            try:
                result = run_windows(root,version,execute,progress=progress)
                atomic_json(root/'result.json',dict(result,finished_at=now()))
            finally:
                atomic_json(root/'batch-runtime.json',dict(runtime.metrics))
