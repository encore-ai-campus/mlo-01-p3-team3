"""Execute bounded product Gold snapshots; never connect to MySQL or embedding APIs."""
import argparse
from contextlib import ExitStack
from dataclasses import asdict
from pathlib import Path
from types import SimpleNamespace
from urllib.parse import unquote,urlsplit
from uuid import uuid4
import os
import signal
import threading
import time

from ..cli import emit,_env
from ..contracts import now
from ..errors import PipelineError
from ..files import atomic_json,read_json,parse,lock
from .config import GoldSettings
from .contracts import registration,validate_inputs,validate_versions


def main(argv=None):
    parser=argparse.ArgumentParser(description='고정 Silver 버전의 상품 Gold 생성')
    for name in ('config','versions','inputs','registration','state-dir'):
        parser.add_argument('--'+name,required=True,type=Path)
    parser.add_argument('--as-of',required=True,help='UTC 업무 평가 기준 시각. 재개 때 동일 값 사용')
    parser.add_argument('--max-products',type=int,default=0)
    parser.add_argument('--rehearse',action='store_true')
    args=parser.parse_args(argv)
    spark=lease=None;previous=None;started=time.monotonic();execution=str(uuid4());code=2
    result=dict(status='FAILED',execution_id=execution,processed=0,reused=0,quality_wait=0)
    protected=[args.config.resolve(),args.versions.resolve(),args.inputs.resolve(),args.registration.resolve()]
    try:
        settings=GoldSettings.load(args.config)
        if args.max_products<0:raise PipelineError('GOLD_INVALID_LIMIT')
        from ..contracts import utc
        as_of=utc(args.as_of)
        if any(p.is_relative_to(args.state_dir.resolve()) for p in protected):raise PipelineError('GOLD_AUDIT_NOT_SEPARATE')
        for path in protected:
            if path.stat().st_size>32*1024*1024:raise PipelineError('GOLD_INPUT_SNAPSHOT_TOO_LARGE')
        versions=read_json(args.versions);validate_versions(versions)
        inputs=read_json(args.inputs);validate_inputs(inputs)
        run=read_json(args.registration);run_id=registration(run,args.rehearse)
        if args.rehearse and any(not p.startswith('file:') for p in (settings.silver_root,settings.gold_root,settings.checkpoint_path)):
            raise PipelineError('GOLD_REHEARSAL_LOCAL_ONLY')
        options=parse(_env(settings.spark_options_env))
        if not isinstance(options,dict) or any(not isinstance(k,str) or not k.startswith('spark.') or not isinstance(v,str) for k,v in options.items()):
            raise PipelineError('GOLD_SPARK_CONFIG_INVALID')
        if not args.rehearse and settings.environment_id.startswith('replace-with-'):raise PipelineError('EXAMPLE_CONFIG_NOT_DEPLOYABLE')
        if threading.current_thread() is threading.main_thread():
            def terminate(*args):raise KeyboardInterrupt
            previous=signal.signal(signal.SIGTERM,terminate)
        if args.rehearse:
            stack=ExitStack()
            try:
                for path in sorted({settings.gold_root,settings.checkpoint_path}):
                    parsed=urlsplit(path)
                    target=Path(unquote(parsed.path.lstrip('/') if os.name=='nt' else parsed.path))
                    stack.enter_context(lock(target.with_name(target.name+'.gold.lock')))
            except BaseException:stack.close();raise
            class LocalLease:
                def check(self,*args):pass
                def close(self,release=True):stack.close()
            lease=LocalLease()
        else:
            from ..bronze.lease import MongoLease
            proxy=SimpleNamespace(**{**asdict(settings),'bronze_path':settings.gold_root,'events_path':settings.gold_root,
                'quarantine_path':settings.gold_root,'fingerprint':settings.fingerprint})
            lease=MongoLease(proxy,_env(settings.lease_uri_env));lease.acquire()
        from pyspark.sql import SparkSession
        builder=SparkSession.builder.appName('product-gold-'+settings.environment_id)
        for key,value in options.items():builder=builder.config(key,value)
        spark=(builder.config('spark.sql.session.timeZone','UTC')
            .config('spark.sql.extensions','io.delta.sql.DeltaSparkSessionExtension')
            .config('spark.sql.catalog.spark_catalog','org.apache.spark.sql.delta.catalog.DeltaCatalog').getOrCreate())
        if not args.rehearse:lease.start_heartbeat(lambda:spark.sparkContext.cancelAllJobs())
        from .source import SilverSource
        from .storage import Checkpoint,DeltaSink
        from .runner import process
        source=SilverSource(spark,settings,versions)
        checkpoint=Checkpoint(spark,settings,lease)
        refs=dict(inputs=checkpoint.archive(inputs),registration=checkpoint.archive(run),
                  versions=checkpoint.archive(versions),silver_root=settings.silver_root,as_of=as_of)
        sink=DeltaSink(spark,settings,lease=lease)
        after=None;drained=False;selection_pages=[]
        # Each invocation scans the pinned range. Receipts, not a local cursor, authorize reuse.
        while True:
            lease.check()
            page=source.products(after)
            if not page:drained=True;break
            selections=[]
            for key in page:
                out=process(source.groups(key),inputs,versions,sink,processing_run_id=run_id,as_of=as_of,input_refs=refs)
                result['processed']+=1;result['reused']+=out['reused'];result['quality_wait']+=out['status']=='QUALITY_WAIT'
                emit(dict(stage='GOLD',product_key=key,**out));after=key
                selections.append(dict(product_key=key,evaluation_key=out['evaluation_key']))
                if args.max_products and result['processed']>=args.max_products:break
            selection_pages.append(checkpoint.archive(dict(selections=selections)))
            if args.max_products and result['processed']>=args.max_products:break
        lease.check()
        from .export import finalize
        if selection_pages:
            result.update(finalize(spark,sink,checkpoint,selection_pages,as_of=as_of,source_drained=drained,input_refs=refs))
        result.update(status='COMPLETED',source_drained=drained,silver_versions=versions,as_of=as_of,
            mode='LOCAL_REHEARSAL' if args.rehearse else 'OWNER_EXPORT_REGISTERED',mysql_published=False)
        code=0
    except KeyboardInterrupt:result['status']='INTERRUPTED';code=130
    except Exception as exc:result['error_code']=exc.code if isinstance(exc,PipelineError) else type(exc).__name__
    finally:
        stopped=True
        if spark is not None:
            try:spark.stop()
            except Exception:stopped=False
        if lease is not None:
            try:lease.close(release=stopped)
            except Exception:stopped=False
        if previous is not None:signal.signal(signal.SIGTERM,previous)
        if not stopped:result.update(status='FAILED',error_code='GOLD_SHUTDOWN_UNCONFIRMED');code=2
        result['duration_ms']=round((time.monotonic()-started)*1000)
        if not any(p.is_relative_to(args.state_dir.resolve()) for p in protected):
            try:atomic_json(args.state_dir/'runs'/(execution+'.json'),result)
            except Exception:result.update(status='FAILED',error_code='GOLD_AUDIT_WRITE_FAILED');code=2
        emit(result)
    return code
