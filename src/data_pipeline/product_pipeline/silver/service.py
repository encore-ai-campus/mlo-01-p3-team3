"""Silver only: pinned Bronze + owner snapshots + confirmed image state -> Delta."""
from dataclasses import asdict, dataclass
from pathlib import Path
from types import SimpleNamespace
import os
import signal
import threading
import time
from uuid import uuid4

from ..contracts import now,digest
from ..errors import PipelineError
from ..files import atomic_json,parse,read_json
from ..bootstrap import required_env as _env
from ..execution import StageResult, ignore_progress
from .config import SilverSettings
from .inputs import registration,validate_inputs


@dataclass(frozen=True)
class Request:
    config: str
    bronze_version: int
    inputs: Path
    registration: Path
    state_dir: Path
    rehearse: bool = False
    image_states: Path | None = None
    max_observations: int = 0
    scan_id: str | None = None
    batch_writes: bool = False
    collector_normalized: bool = False
    selection: Path | None = None
    reuse_selection: Path | None = None


def run(args: Request, *, progress=ignore_progress):
    emit = progress
    spark=lease=states=None
    started,execution=time.monotonic(),str(uuid4())
    result=dict(status='FAILED',execution_id=execution,processed=0,reused=0,quality_wait=0)
    code=2;previous=None
    try:
        settings=SilverSettings.load(args.config)
        if args.scan_id is not None and (len(args.scan_id)!=64 or any(c not in '0123456789abcdef' for c in args.scan_id)):
            raise PipelineError('SILVER_SCAN_ID_INVALID')
        if args.bronze_version<0 or args.max_observations<0:raise PipelineError('SILVER_INVALID_LIMIT')
        if args.collector_normalized and args.rehearse:raise PipelineError('SILVER_NORMALIZED_INPUT_LIVE_ONLY')
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
            result['lease_owner_ref']=lease.record_owner(args.state_dir/'lease-owners'/(execution+'.json'),execution)
        from ..spark_storage import storage_options,bind_spark_credentials
        spark_options=storage_options(spark_options,writer=True,lease=lease)
        builder=SparkSession.builder.appName('product-silver-'+settings.environment_id)
        for key,value in spark_options.items():builder=builder.config(key,value)
        from ..spark_session import get_or_create,release_spark
        builder=(builder.config('spark.sql.session.timeZone','UTC')
            .config('spark.sql.extensions','io.delta.sql.DeltaSparkSessionExtension')
            .config('spark.sql.catalog.spark_catalog','org.apache.spark.sql.delta.catalog.DeltaCatalog'))
        spark=get_or_create(builder,spark_options)
        bind_spark_credentials(spark,writer=True,lease=lease)
        if not args.rehearse:lease.start_heartbeat(lambda:spark.sparkContext.cancelAllJobs())
        from .source import BronzeSource,MongoStates,FileStates
        from .delta import DeltaSink
        from .checkpoint import Checkpoint
        from .runner import process
        source=BronzeSource(spark,settings,args.bronze_version)
        window=None
        reuse_choices={}
        if args.selection:
            from ..windows import validate_window
            window=validate_window(read_json(args.selection));source.restrict_window(window)
        if args.reuse_selection:
            if window is None or not args.batch_writes:raise PipelineError('SILVER_REUSE_WINDOW_REQUIRED')
            from .selection import validate
            if args.reuse_selection.stat().st_size>1024*1024:raise PipelineError('SILVER_REUSE_SELECTION_TOO_LARGE')
            reuse_choices={c['raw_document_id']:c for c in validate(read_json(args.reuse_selection),window)}
        states=FileStates(read_json(args.image_states)) if args.rehearse else MongoStates(settings,uri)
        checkpoint=Checkpoint(spark,settings,lease)
        checkpoint.archive(inputs);checkpoint.archive(run)
        sink=DeltaSink(spark,settings,lease=lease)
        from .checkpoint import bind_scan
        from .normalize import VERSION
        scope=dict(inputs=inputs,normalizer_version=VERSION,scan_id=args.scan_id,
            image_states=read_json(args.image_states) if args.image_states else None)
        if args.collector_normalized:scope['normalized_input']='danawa_products/observation-binding-v1'
        if window:scope['window_hash']=window['content_hash'];checkpoint.archive(window)
        if args.reuse_selection:scope['reuse_selection_hash']=digest(read_json(args.reuse_selection));checkpoint.archive(read_json(args.reuse_selection))
        scan_scope=digest(scope)
        cursor=bind_scan(checkpoint.cursor(args.bronze_version),scan_scope)
        choices=[]
        if window:
            # The bounded window is replayed from its receipts after a restart;
            # a discovery hint cannot omit earlier receipt choices.
            cursor.update(after=None,completed=False)
        as_of=now()
        while True:
            lease.check(settings)
            page=source.page(cursor['after'])
            if not page:cursor['completed']=True;checkpoint.save(cursor);break
            if args.max_observations:page=page[:args.max_observations-result['processed']]
            if args.collector_normalized:
                proofs=[states.normalized(doc) for doc in page]
                references=checkpoint.archive_many(proofs) if args.batch_writes else [checkpoint.archive(p) for p in proofs]
                for doc,proof,reference in zip(page,proofs,references):
                    from ..image_waits import record_gallery_wait
                    waiting=record_gallery_wait(args.state_dir,doc,proof)
                    if waiting:emit(dict(stage='source_gallery_wait',raw_document_id=doc['_id'],evidence_ref=str(waiting)))
                    emit(dict(stage='collector_normalized_input',raw_document_id=doc['_id'],
                              normalized_hash=proof['normalized_hash'],archive_ref=reference,status='VERIFIED'))
                    result['normalized_verified']=result.get('normalized_verified',0)+1
            if args.batch_writes:
                from .batch import process_many
                outputs=process_many(page,states,sink,inputs,processing_run_id=run_id,as_of=as_of,bronze_version=args.bronze_version,
                    bronze_path=settings.bronze_path,archive=checkpoint.archive,reuse_choices=reuse_choices,archive_many=checkpoint.archive_many)
            else:
                outputs=(process(doc,states,sink,inputs,processing_run_id=run_id,as_of=as_of,bronze_version=args.bronze_version,
                    bronze_path=settings.bronze_path,progress=emit,archive=checkpoint.archive) for doc in page)
            for doc,out in zip(page,outputs):
                result['processed']+=1;result['reused']+=out['reused'];result['quality_wait']+=out['status']=='QUALITY_WAIT'
                result['normalization_skipped']=result.get('normalization_skipped',0)+bool(out.get('normalization_skipped'))
                emit(dict(stage='silver_observation',**out))
                cursor['after']=doc['observation_key']
                if window:choices.append(dict(product_key=doc['source_code']+':'+doc['source_product_id'],raw_document_id=doc['_id'],
                    observation_key=doc['observation_key'],evaluation_key=out['evaluation_key'],evidence_ref='silver-receipt://'+out['evaluation_key']))
                if args.max_observations and result['processed']>=args.max_observations:break
            # All page outputs and receipts are durable before advancing once.
            checkpoint.save(cursor)
            if args.max_observations and result['processed']>=args.max_observations:break
        lease.check(settings)
        if window:
            from .selection import seal
            selected=seal(window,choices)
            atomic_json(args.state_dir/'evaluation-selection.json',selected)
            result['evaluation_selection_hash']=selected['content_hash']
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
            try:release_spark(spark)
            except Exception:stopped=False
        if lease is not None:
            try:lease.close(release=stopped)
            except Exception:stopped=False
        if not stopped:result.update(status='FAILED',error_code='SILVER_SHUTDOWN_UNCONFIRMED');code=2
        if previous is not None:signal.signal(signal.SIGTERM,previous)
        result['duration_ms']=round((time.monotonic()-started)*1000)
        if lease is not None and getattr(lease,'loss_diagnostic',None):result['lease_failure']=lease.loss_diagnostic
        from ..spark_session import stamp_driver_exit
        if stamp_driver_exit(result):code=2
        # Never write into source inputs just because a caller chose an overlapping log directory.
        state=args.state_dir.resolve()
        protected=[args.inputs.resolve(),args.registration.resolve(),*([args.image_states.resolve()] if args.image_states else [])]
        if not any(p.is_relative_to(state) for p in protected):
            try:atomic_json(state/'runs'/(execution+'.json'),result)
            except Exception:result.update(status='FAILED',error_code='SILVER_AUDIT_WRITE_FAILED');code=2
    return StageResult(code, result)
