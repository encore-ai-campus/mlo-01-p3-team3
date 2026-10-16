"""Silver only: pinned Bronze + owner snapshots + confirmed image state -> Delta."""
import argparse
from dataclasses import asdict
from pathlib import Path
from types import SimpleNamespace
import os
import signal
import threading
import time
from uuid import uuid4

from ..contracts import now
from ..errors import PipelineError
from ..files import atomic_json,parse,read_json
from ..cli import emit,_env
from .config import SilverSettings
from .inputs import registration,validate_inputs


def main(argv=None):
    parser=argparse.ArgumentParser(description='상품 Silver 정규화·품질·Delta 평가')
    parser.add_argument('--config',required=True)
    parser.add_argument('--bronze-version',required=True,type=int)
    parser.add_argument('--inputs',required=True,type=Path)
    parser.add_argument('--registration',required=True,type=Path)
    parser.add_argument('--state-dir',required=True,type=Path,help='지역 실행 로그. 공유 discovery checkpoint와 별도')
    parser.add_argument('--rehearse',action='store_true')
    parser.add_argument('--image-states',type=Path,help='rehearsal 전용 명시 상태 export')
    parser.add_argument('--max-observations',type=int,default=0,help='0=고정 Bronze 전체, 양수=소량 검증')
    args=parser.parse_args(argv)
    spark=lease=states=None
    started,execution=time.monotonic(),str(uuid4())
    result=dict(status='FAILED',execution_id=execution,processed=0,reused=0,quality_wait=0)
    code=2;previous=None
    try:
        settings=SilverSettings.load(args.config)
        if args.bronze_version<0 or args.max_observations<0:raise PipelineError('SILVER_INVALID_LIMIT')
        if args.rehearse:
            if not args.image_states or any(not p.startswith('file:') for p in (settings.bronze_path,settings.silver_root,settings.checkpoint_path)):
                raise PipelineError('SILVER_REHEARSAL_LOCAL_ONLY')
        elif args.image_states:raise PipelineError('SILVER_STATE_EXPORT_NOT_LIVE')
        for path in [args.inputs,args.registration,*([args.image_states] if args.image_states else [])]:
            if path.stat().st_size>32*1024*1024:raise PipelineError('SILVER_INPUT_SNAPSHOT_TOO_LARGE')
            if path.resolve().is_relative_to(args.state_dir.resolve()):raise PipelineError('SILVER_AUDIT_NOT_SEPARATE')
        inputs=read_json(args.inputs);validate_inputs(inputs)
        run=read_json(args.registration);run_id=registration(run,args.rehearse)
        spark_options=parse(_env(settings.spark_options_env))
        if not isinstance(spark_options,dict) or any(not isinstance(k,str) or not k.startswith('spark.') or not isinstance(v,str) for k,v in spark_options.items()):
            raise PipelineError('SILVER_SPARK_CONFIG_INVALID')
        if not args.rehearse:
            if any(v.startswith('replace-with-') for v in (settings.environment_id,settings.mongo_database)):
                raise PipelineError('EXAMPLE_CONFIG_NOT_DEPLOYABLE')
            uri,lease_uri=_env(settings.mongo_uri_env),_env(settings.lease_uri_env)
        if threading.current_thread() is threading.main_thread():
            def terminate(*args):raise KeyboardInterrupt
            previous=signal.signal(signal.SIGTERM,terminate)
        from pyspark.sql import SparkSession
        if args.rehearse:
            from ..files import lock
            from contextlib import ExitStack
            local_locks=ExitStack()
            from urllib.parse import unquote,urlsplit
            # Lock each output/checkpoint target, even if callers choose different state dirs.
            try:
                for path in sorted({settings.silver_root,settings.checkpoint_path}):
                    target=Path(unquote(urlsplit(path).path.lstrip('/') if os.name=='nt' else urlsplit(path).path))
                    local_locks.enter_context(lock(target.with_name(target.name+'.silver.lock')))
            except BaseException:
                local_locks.close();raise
            class LocalLease:
                def check(self,expected=None):
                    if expected is not None and expected.fingerprint!=settings.fingerprint:raise PipelineError('SILVER_TARGET_CONFLICT')
                def close(self,release=True):local_locks.close()
            lease=LocalLease()
        else:
            from ..bronze.lease import MongoLease
            # Reuse the tested common ownership protocol; Silver locks its whole root and checkpoint.
            proxy=SimpleNamespace(**{**asdict(settings),'bronze_path':settings.silver_root,
                'events_path':settings.silver_root,'quarantine_path':settings.silver_root,'fingerprint':settings.fingerprint})
            lease=MongoLease(proxy,lease_uri);lease.acquire()
        builder=SparkSession.builder.appName('product-silver-'+settings.environment_id)
        for key,value in spark_options.items():builder=builder.config(key,value)
        spark=(builder.config('spark.sql.session.timeZone','UTC')
            .config('spark.sql.extensions','io.delta.sql.DeltaSparkSessionExtension')
            .config('spark.sql.catalog.spark_catalog','org.apache.spark.sql.delta.catalog.DeltaCatalog').getOrCreate())
        if not args.rehearse:lease.start_heartbeat(lambda:spark.sparkContext.cancelAllJobs())
        from .source import BronzeSource,MongoStates,FileStates
        from .delta import DeltaSink
        from .checkpoint import Checkpoint
        from .runner import process
        source=BronzeSource(spark,settings,args.bronze_version)
        states=FileStates(read_json(args.image_states)) if args.rehearse else MongoStates(settings,uri)
        checkpoint=Checkpoint(spark,settings,lease)
        checkpoint.archive(inputs);checkpoint.archive(run)
        sink=DeltaSink(spark,settings,lease=lease)
        cursor=checkpoint.cursor(args.bronze_version)
        as_of=now()
        while True:
            lease.check(settings)
            page=source.page(cursor['after'])
            if not page:cursor['completed']=True;checkpoint.save(cursor);break
            for doc in page:
                out=process(doc,states,sink,inputs,processing_run_id=run_id,as_of=as_of,bronze_version=args.bronze_version,
                    bronze_path=settings.bronze_path,progress=emit,archive=checkpoint.archive)
                result['processed']+=1;result['reused']+=out['reused'];result['quality_wait']+=out['status']=='QUALITY_WAIT'
                emit(dict(stage='silver_observation',**out))
                cursor['after']=doc['observation_key'];checkpoint.save(cursor)
                if args.max_observations and result['processed']>=args.max_observations:break
            if args.max_observations and result['processed']>=args.max_observations:break
        lease.check(settings)
        result.update(status='COMPLETED',bronze_version=args.bronze_version,source_drained=cursor['completed'],
                      mode='LOCAL_REHEARSAL' if args.rehearse else 'OWNER_EXPORT_REGISTERED')
        code=0
    except KeyboardInterrupt:result['status']='INTERRUPTED';code=130
    except Exception as exc:result['error_code']=exc.code if isinstance(exc,PipelineError) else type(exc).__name__
    finally:
        stopped=True
        if hasattr(states,'close'):
            try:states.close()
            except Exception:stopped=False
        if spark is not None:
            try:spark.stop()
            except Exception:stopped=False
        if lease is not None:
            try:lease.close(release=stopped)
            except Exception:stopped=False
        if not stopped:result.update(status='FAILED',error_code='SILVER_SHUTDOWN_UNCONFIRMED');code=2
        if previous is not None:signal.signal(signal.SIGTERM,previous)
        result['duration_ms']=round((time.monotonic()-started)*1000)
        # Never write into source inputs just because a caller chose an overlapping log directory.
        state=args.state_dir.resolve()
        protected=[args.inputs.resolve(),args.registration.resolve(),*([args.image_states.resolve()] if args.image_states else [])]
        if not any(p.is_relative_to(state) for p in protected):
            try:atomic_json(state/'runs'/(execution+'.json'),result)
            except Exception:result.update(status='FAILED',error_code='SILVER_AUDIT_WRITE_FAILED');code=2
        emit(result)
    return code
