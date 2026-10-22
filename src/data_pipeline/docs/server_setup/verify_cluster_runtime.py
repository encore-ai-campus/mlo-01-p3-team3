"""Read-only Spark cluster/Delta check. Does not consume Kafka or publish products."""
import json,os,sys,time
from pathlib import Path
ROOT=Path('/var/lib/pj3/product-pipeline/spark/production-pending-20261019')
sys.path.insert(0,str(ROOT));sys.path.insert(0,str(ROOT/'source'))
from production_work import setup,session,pinned,LAKE
from product_pipeline.files import atomic_json,read_json
from product_pipeline.gold.config import GoldSettings
from product_pipeline.gold.source import SilverSource,iter_groups
from product_pipeline.gold.contracts import INPUT_TABLES


def main():
    output=Path(sys.argv[1]);output.mkdir(parents=True,exist_ok=True)
    began=time.monotonic();spark=None
    result=dict(status='FAILED',production_writes=False,kafka_offsets_modified=False)
    try:
        _,options=setup()
        spark=session(options)
        if spark.sparkContext.master.startswith('local'):raise RuntimeError('LOCAL_MASTER_FORBIDDEN')
        deadline=time.monotonic()+45
        while spark.sparkContext._jsc.sc().getExecutorMemoryStatus().size()<4 and time.monotonic()<deadline:time.sleep(1)
        result['registered_executor_hosts']=spark.sparkContext._jsc.sc().getExecutorMemoryStatus().keySet().toString()
        partition_rows=spark.range(0,3000,1,3).selectExpr('spark_partition_id() as partition',
            "reflect('java.net.InetAddress', 'getLocalHost') as executor_host").distinct().collect()
        workers=sorted({r['executor_host'] for r in partition_rows})
        result['worker_hosts']=workers;result['partition_count']=len({r['partition'] for r in partition_rows})
        if len(workers)!=3:raise RuntimeError('THREE_WORKER_TASK_EXECUTION_NOT_CONFIRMED')
        version_file=Path(read_json(ROOT/'downstream-progress.json')['execution_root'])/'silver-versions.json'
        versions=read_json(version_file)
        source=SilverSource(spark,GoldSettings(environment_id='pj3-danawa',silver_root=LAKE+'/silver',
            gold_root=LAKE+'/gold',checkpoint_path=LAKE+'/checkpoints/gold',max_records=2),versions)
        keys=source.products();source.restrict_products(keys)
        before=time.monotonic();groups=list(iter_groups(source))
        if not groups:raise RuntimeError('SILVER_READ_EMPTY')
        result.update(status='VERIFIED',master=spark.sparkContext.master,worker_hosts=workers,
            selected_products=len(keys),verified_silver_groups=len(groups),table_read_ms=round((time.monotonic()-before)*1000),
            executor_memory=spark.sparkContext.getConf().get('spark.executor.memory'))
    except Exception as exc:
        import traceback
        result.update(error_code=getattr(exc,'code',str(exc) if isinstance(exc,RuntimeError) else type(exc).__name__),
            frames=[dict(file=Path(f.filename).name,line=f.lineno,function=f.name) for f in traceback.extract_tb(exc.__traceback__)[-6:]])
    finally:
        if spark is not None:
            try:spark.stop()
            except Exception:result.update(status='FAILED',error_code='SPARK_SHUTDOWN_UNCONFIRMED')
        result['duration_ms']=round((time.monotonic()-began)*1000)
        atomic_json(output/'result.json',result)
    return 0 if result['status']=='VERIFIED' else 2


if __name__=='__main__':raise SystemExit(main())
