"""Environment identity is separate from immutable normalization input versions."""
from dataclasses import dataclass
import re

from ..bronze.config import location
from ..contracts import digest
from ..errors import PipelineError
from ..files import read_json


@dataclass(frozen=True)
class SilverSettings:
    environment_id:str
    mongo_database:str
    bronze_path:str
    silver_root:str
    checkpoint_path:str
    mongo_replica_set:str='rs-products'
    mongo_uri_env:str='PIPELINE_MONGO_URI'
    spark_options_env:str='SILVER_SPARK_OPTIONS'
    lease_database:str='pipeline_ops'
    lease_collection:str='pipeline_writer_leases'
    lease_uri_env:str='SILVER_LEASE_MONGO_URI'
    lease_seconds:int=120
    lease_heartbeat_seconds:int=10
    max_records:int=20
    max_raw_bytes:int=32*1024*1024

    def __post_init__(self):
        for value in (self.environment_id,self.mongo_database,self.mongo_replica_set,self.lease_database,self.lease_collection):
            if not isinstance(value,str) or not re.fullmatch(r'[A-Za-z0-9_.-]{1,100}',value):raise PipelineError('SILVER_INVALID_CONFIG')
        for value in (self.mongo_uri_env,self.spark_options_env,self.lease_uri_env):
            if not isinstance(value,str) or not re.fullmatch(r'[A-Z][A-Z0-9_]*',value):raise PipelineError('SILVER_INVALID_CONFIG')
        paths=[location(p) for p in (self.bronze_path,self.silver_root,self.checkpoint_path)]
        if any(a==b or a.startswith(b+'/') or b.startswith(a+'/') for i,a in enumerate(paths) for b in paths[i+1:]):
            raise PipelineError('SILVER_PATHS_OVERLAP')
        if (self.lease_database==self.mongo_database or self.lease_collection=='raw_products'
                or type(self.max_records) is not int or not 1<=self.max_records<=100
                or type(self.max_raw_bytes) is not int or not 1<=self.max_raw_bytes<=128*1024*1024
                or type(self.lease_seconds) is not int or not 60<=self.lease_seconds<=3600
                or type(self.lease_heartbeat_seconds) is not int or not 1<=self.lease_heartbeat_seconds<=self.lease_seconds//6):
            raise PipelineError('SILVER_INVALID_CONFIG')

    @property
    def fingerprint(self):
        return digest(dict(environment_id=self.environment_id,mongo_database=self.mongo_database,
            mongo_replica_set=self.mongo_replica_set,bronze_path=location(self.bronze_path),
            silver_root=location(self.silver_root),checkpoint_path=location(self.checkpoint_path),schema_version='silver-1'))

    @classmethod
    def load(cls,path):
        try:return cls(**read_json(path))
        except TypeError as exc:raise PipelineError('SILVER_INVALID_CONFIG') from exc
