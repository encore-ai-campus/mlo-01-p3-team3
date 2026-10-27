"""Lazy Spark lifetime: capture needs no JVM; status never acquires a writer lease."""
import json
import os
from pathlib import Path
from types import SimpleNamespace
from ..domain.contracts import digest, Invalid
from .delta import DeltaLake


class ServerLake:
    def __init__(self, config, limits, writable,timing=None,lease_role=None):
        self.config,self.limits,self.writable=config,limits,writable
        # Delta writers fence the lake; a publish-only worker fences MySQL under a separate target.
        self.lease_role=lease_role or ('delta-writer' if writable else None)
        self.timing=timing
        self.identity=dict(kind='DELTA_V2',root=config['lake_root'].rstrip('/'))
        self.inner=self.spark=self.lease=None
        self.metrics={}

    def _open(self):
        if self.inner is not None:return self.inner
        from pyspark.sql import SparkSession
        from pipeline_v2.legacy.bronze.lease import MongoLease
        from pipeline_v2.legacy.spark_storage import storage_options, bind_spark_credentials
        config=self.config
        if self.lease_role:
            uri=os.environ.get(config['lease']['uri_env'])
            if not uri:raise Invalid('LEASE_ENV_MISSING')
            root=config['lake_root'] if self.lease_role=='delta-writer' else config['lake_root'].rstrip('/')+'/_'+self.lease_role
            settings=SimpleNamespace(mongo_replica_set=config['mongo']['replica_set'],
                lease_database=config['lease']['database'],lease_collection=config['lease']['collection'],
                lease_seconds=config['lease'].get('seconds',120),lease_heartbeat_seconds=config['lease'].get('heartbeat_seconds',20),
                bronze_path=root,events_path=root,quarantine_path=root,
                checkpoint_path=root+'/_writer',fingerprint=digest(dict(target=config['target'],lake=config['lake_root'])
                    if self.lease_role=='delta-writer' else dict(target=config['target'],lake=config['lake_root'],role=self.lease_role)))
            self.lease=MongoLease(settings,uri)
            if self.timing:
                with self.timing.measure('server.writer_lease_acquire',self.timing.active_scope or 'runtime'):self.lease.acquire()
            else:self.lease.acquire()
        try:
            value=os.environ.get(config['spark_options_env'])
            if not value:raise Invalid('SPARK_ENV_MISSING')
            options=json.loads(value)
            if not isinstance(options,dict) or any(not k.startswith('spark.') or not isinstance(v,str) for k,v in options.items()):
                raise Invalid('SPARK_OPTIONS_INVALID')
            metrics = config.get('metrics',{})
            if metrics.get('spark_event_log',True):
                # Persist once for offline analysis; never query the Spark UI during processing.
                event_directory=Path(config['state_root'])/'metrics'/'spark-events'
                event_directory.mkdir(parents=True,exist_ok=True)
                options.setdefault('spark.eventLog.enabled','true')
                options.setdefault('spark.eventLog.dir',event_directory.resolve().as_uri())
                options.setdefault('spark.eventLog.compress','false')
                options.setdefault('spark.eventLog.logStageExecutorMetrics','true')
                # Memory sizing evidence: cached block sizes (memory/disk) and Python worker RSS.
                options.setdefault('spark.eventLog.logBlockUpdates.enabled','true')
                options.setdefault('spark.executor.processTreeMetrics.enabled','true')
            # Delta writes and reads are bound by the ~10 MB/s link to Garage, and the tables are
            # mostly JSON strings: zstd stores them much smaller than the snappy default.
            options.setdefault('spark.sql.parquet.compression.codec','zstd')
            options.setdefault('spark.redaction.regex','(?i)secret|password|token|access[.]?key|uri|credential')
            options=storage_options(options,writer=self.writable,lease=self.lease if self.writable else None)
            builder=SparkSession.builder.appName('sketchon-direct-pipeline-v2')
            for key,value in options.items():builder=builder.config(key,value)
            def start_spark():
                self.spark=(builder.config('spark.sql.session.timeZone','UTC').config('spark.sql.extensions','io.delta.sql.DeltaSparkSessionExtension')
                    .config('spark.sql.catalog.spark_catalog','org.apache.spark.sql.delta.catalog.DeltaCatalog').getOrCreate())
                bind_spark_credentials(self.spark,writer=self.writable,lease=self.lease if self.writable else None)
            if self.timing:
                with self.timing.measure('server.spark_session_startup',self.timing.active_scope or 'runtime'):start_spark()
            else:start_spark()
            if self.lease:self.lease.start_heartbeat(lambda:self.spark.sparkContext.cancelAllJobs())
            guard=self.lease or SimpleNamespace(check=lambda:None)
            self.inner=DeltaLake(self.spark,config['lake_root'],guard,self.limits.history_bytes*2,read_only=not self.writable,timing=self.timing)
            self.metrics=self.inner.metrics
            return self.inner
        except BaseException:
            self.close();raise

    def read_keys(self,*args,**kwargs):return self._open().read_keys(*args,**kwargs)
    def check(self):
        self._open()
        if self.lease:self.lease.check()
    def verify(self,*args,**kwargs):return self._open().verify(*args,**kwargs)
    def commit(self,*args,**kwargs):
        if not self.writable:raise Invalid('READ_ONLY_LAKE')
        return self._open().commit(*args,**kwargs)

    def close(self):
        stopped=True
        if self.spark is not None:
            try:self.spark.stop()
            except Exception:stopped=False
        if self.lease is not None:self.lease.close(release=stopped)
        self.inner=self.spark=self.lease=None
        if not stopped:raise Invalid('SPARK_SHUTDOWN_UNCONFIRMED')
