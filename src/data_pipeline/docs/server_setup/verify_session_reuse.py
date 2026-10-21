"""Read-only real-cluster session and pinned input reuse measurement."""
import sys,time
from pathlib import Path
ROOT=Path('/var/lib/pj3/product-pipeline/spark/production-pending-20261019')
sys.path.insert(0,str(ROOT));sys.path.insert(0,str(ROOT/'source'))
from production_work import setup,session,pinned,LAKE
from product_pipeline.spark_session import SessionScope,release_spark,metrics
from product_pipeline.files import atomic_json
from product_pipeline.contracts import digest
from product_pipeline.gold.config import GoldSettings
from product_pipeline.gold.contracts import INPUT_TABLES
from product_pipeline.gold.source import SilverSource


def main():
    output=Path(sys.argv[1]);output.mkdir(parents=True,exist_ok=True)
    began=time.monotonic();result=dict(status='FAILED',production_writes=False,kafka_offsets_modified=False)
    try:
        _,options=setup()
        with SessionScope() as runtime:
            versions=pinned(options,LAKE+'/silver',INPUT_TABLES)
            spark=session(options);app=spark.sparkContext.applicationId
            settings=GoldSettings(environment_id='pj3-danawa',silver_root=LAKE+'/silver',gold_root=LAKE+'/gold',checkpoint_path=LAKE+'/checkpoints/gold',max_records=2)
            source=SilverSource(spark,settings,versions);keys=source.products();source.restrict_products(keys)
            stamp=time.monotonic();first=source.groups_many(keys);first_ms=round((time.monotonic()-stamp)*1000)
            reads=metrics().get('silver_table_read_batches',0);release_spark(spark)
            second=session(options)
            stamp=time.monotonic();repeated=SilverSource(second,settings,versions).groups_many(keys);second_ms=round((time.monotonic()-stamp)*1000)
            if app!=second.sparkContext.applicationId or digest(first)!=digest(repeated):raise RuntimeError('SESSION_OR_PINNED_INPUT_CHANGED')
            if metrics().get('silver_table_read_batches',0)!=reads or runtime.metrics.get('spark_sessions_started')!=1:raise RuntimeError('REPEATED_TABLE_READ')
            release_spark(second)
            result.update(status='VERIFIED',master=spark.sparkContext.master,application_id=app,
                first_read_ms=first_ms,reused_read_ms=second_ms,products=len(keys),groups=len(first),metrics=metrics(),
                executor_hosts=spark.sparkContext._jsc.sc().getExecutorMemoryStatus().keySet().toString())
    except Exception as exc:
        import traceback
        result.update(status='FAILED',error_code=getattr(exc,'code',str(exc) if isinstance(exc,RuntimeError) else type(exc).__name__),
            frames=[dict(file=Path(f.filename).name,line=f.lineno,function=f.name) for f in traceback.extract_tb(exc.__traceback__)[-6:]])
    result['duration_ms']=round((time.monotonic()-began)*1000);atomic_json(output/'result.json',result)
    return 0 if result['status']=='VERIFIED' else 2


if __name__=='__main__':raise SystemExit(main())
