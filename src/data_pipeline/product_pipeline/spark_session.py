"""Driver-local session ownership and bounded reuse. Never persisted as evidence."""
from collections import OrderedDict
from contextvars import ContextVar
from contextlib import contextmanager
from copy import deepcopy
import time
import tempfile
import shutil
from pathlib import Path
from hashlib import sha256
from functools import wraps
from .contracts import canonical,digest
from .errors import PipelineError

_current=ContextVar('pipeline_spark_scope',default=None)
IDENTITY=('spark.master','spark.jars','spark.driver.host','spark.driver.memory',
          'spark.executor.memory','spark.executor.cores','spark.cores.max',
          'spark.driver.extraJavaOptions','spark.executor.extraJavaOptions',
          'spark.driver.extraClassPath','spark.executor.extraClassPath')


class SessionScope:
    def __init__(self,*,max_cache_bytes=32*1024*1024,max_disk_cache_bytes=256*1024*1024):
        self.spark=None;self.identity=None;self.cache=OrderedDict();self.bytes=0
        self.max_cache_bytes=max_cache_bytes;self.metrics={}
        self.max_disk_cache_bytes=max_disk_cache_bytes;self.disk_cache=OrderedDict();self.disk_bytes=0;self.disk_root=None

    def __enter__(self):
        if _current.get() is not None:raise PipelineError('SPARK_SCOPE_NESTED')
        self.token=_current.set(self);return self

    def __exit__(self,kind,value,tb):
        try:
            if self.spark is not None:
                if kind:self.spark.sparkContext.cancelAllJobs()
                self.spark.stop()
        finally:
            self.cache.clear();self.bytes=0;self.disk_cache.clear();self.disk_bytes=0
            try:
                if self.disk_root is not None:self.disk_root.cleanup()
            finally:_current.reset(self.token)


def bump(name,amount=1):
    scope=_current.get()
    if scope is not None:scope.metrics[name]=scope.metrics.get(name,0)+amount


def metrics():
    scope=_current.get()
    return dict(scope.metrics) if scope is not None else {}


@contextmanager
def timed(name):
    """Record inclusive wall time, including failed attempts, without changing I/O."""
    started=time.monotonic()
    try:yield
    finally:
        bump(name+'_ms',round((time.monotonic()-started)*1000));bump(name+'_calls')


def measured(name):
    def decorate(function):
        @wraps(function)
        def run(*args,**kwargs):
            with timed(name):return function(*args,**kwargs)
        return run
    return decorate


def get_or_create(builder,options):
    scope=_current.get()
    if scope is None:return builder.getOrCreate()
    identity=digest({k:options.get(k) for k in IDENTITY})
    if scope.spark is not None:
        if identity!=scope.identity:raise PipelineError('SPARK_SHARED_CONFIG_CHANGED')
        bump('spark_session_reuses');return scope.spark
    started=time.monotonic();scope.spark=builder.getOrCreate();scope.identity=identity
    bump('spark_sessions_started');bump('spark_startup_ms',round((time.monotonic()-started)*1000))
    return scope.spark


def release_spark(spark):
    scope=_current.get()
    if scope is None or scope.spark is not spark:spark.stop();return
    tracker=spark.sparkContext.statusTracker();deadline=time.monotonic()+5
    while spark.streams.active or tracker.getActiveJobsIds() or tracker.getActiveStageIds():
        # Allow the scheduler's completion notifications to settle, but never
        # release a writer lease while Spark jobs could still commit.
        if time.monotonic()>=deadline:raise PipelineError('SPARK_SHARED_WORK_NOT_QUIESCENT')
        time.sleep(0.05)


def cache_get(namespace,key):
    scope=_current.get()
    if scope is None:return None
    identity=(namespace,key)
    if identity not in scope.cache:
        if identity not in scope.disk_cache:return None
        path,size,checksum=scope.disk_cache.pop(identity);scope.disk_cache[identity]=(path,size,checksum)
        from .files import parse
        with path.open('rb') as stream:data=stream.read(size+1)
        if len(data)!=size or sha256(data).hexdigest()!=checksum:raise PipelineError('SPARK_DISK_CACHE_CHANGED')
        bump(namespace+'_disk_cache_hits');return parse(data.decode('utf-8'))
    value,size=scope.cache.pop(identity);scope.cache[identity]=(value,size)
    bump(namespace+'_cache_hits');return deepcopy(value)


def cache_put(namespace,key,value):
    scope=_current.get()
    if scope is None:return
    data=canonical(value).encode('utf-8');size=len(data)
    identity=(namespace,key)
    if size>scope.max_cache_bytes:
        # Only validated pinned Silver groups may spill; no credentials or
        # writer proofs enter this disposable cache. It never authorizes reuse.
        if namespace!='silver_groups' or size>scope.max_disk_cache_bytes:return
        if scope.disk_root is None:scope.disk_root=tempfile.TemporaryDirectory(prefix='pipeline-silver-cache-')
        if shutil.disk_usage(scope.disk_root.name).free<size+64*1024*1024:return
        if identity in scope.disk_cache:
            old,old_size,_=scope.disk_cache.pop(identity);old.unlink();scope.disk_bytes-=old_size
        while scope.disk_cache and scope.disk_bytes+size>scope.max_disk_cache_bytes:
            _,(old,old_size,_)=scope.disk_cache.popitem(last=False);old.unlink();scope.disk_bytes-=old_size
        checksum=sha256(data).hexdigest();path=Path(scope.disk_root.name)/(digest([namespace,key])+'.json')
        with path.open('wb') as stream:stream.write(data)
        path.chmod(0o600);scope.disk_cache[identity]=(path,size,checksum);scope.disk_bytes+=size
        bump(namespace+'_disk_cache_writes');return
    if identity in scope.cache:scope.bytes-=scope.cache.pop(identity)[1]
    while scope.cache and (scope.bytes+size>scope.max_cache_bytes or len(scope.cache)>=1000):
        _,(_,old)=scope.cache.popitem(last=False);scope.bytes-=old
    scope.cache[identity]=(deepcopy(value),size);scope.bytes+=size
