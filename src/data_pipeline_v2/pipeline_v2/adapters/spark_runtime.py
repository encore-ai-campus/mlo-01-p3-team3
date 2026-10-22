"""Lazy Spark lifetime: capture needs no JVM; status never acquires a writer lease."""
import json
import os
from types import SimpleNamespace
from ..domain.contracts import digest, Invalid
from .delta import DeltaLake


class ServerLake:
    def __init__(self, config, limits, writable,timing=None):
        self.config,self.limits,self.writable=config,limits,writable
        self.timing=timing
        self.identity=dict(kind='DELTA_V2',root=config['lake_root'].rstrip('/'))
        self.inner=self.spark=self.lease=None
        self.metrics={}

    def _open(self):
        if self.inner is not None:return self.inner
        from pyspark.sql import SparkSession
        from product_pipeline.bronze.lease import MongoLease
        from product_pipeline.spark_storage import storage_options, bind_spark_credentials
        config=self.config
        if self.writable:
            uri=os.environ.get(config['lease']['uri_env'])
            if not uri:raise Invalid('LEASE_ENV_MISSING')
            settings=SimpleNamespace(mongo_replica_set=config['mongo']['replica_set'],
                lease_database=config['lease']['database'],lease_collection=config['lease']['collection'],
                lease_seconds=config['lease'].get('seconds',120),lease_heartbeat_seconds=config['lease'].get('heartbeat_seconds',20),
                bronze_path=config['lake_root'],events_path=config['lake_root'],quarantine_path=config['lake_root'],
                checkpoint_path=config['lake_root']+'/_writer',fingerprint=digest(dict(target=config['target'],lake=config['lake_root'])))
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
            options=storage_options(options,writer=self.writable,lease=self.lease)
            builder=SparkSession.builder.appName('sketchon-direct-pipeline-v2')
            for key,value in options.items():builder=builder.config(key,value)
            def start_spark():
                self.spark=(builder.config('spark.sql.session.timeZone','UTC').config('spark.sql.extensions','io.delta.sql.DeltaSparkSessionExtension')
                    .config('spark.sql.catalog.spark_catalog','org.apache.spark.sql.delta.catalog.DeltaCatalog').getOrCreate())
                bind_spark_credentials(self.spark,writer=self.writable,lease=self.lease)
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
