"""External Gold locations and bounded product history reads."""
from dataclasses import dataclass
import re

from ..bronze.config import location
from ..contracts import digest
from ..errors import PipelineError
from ..files import read_json


@dataclass(frozen=True)
class GoldSettings:
    environment_id:str
    silver_root:str
    gold_root:str
    checkpoint_path:str
    spark_options_env:str='GOLD_SPARK_OPTIONS'
    lease_database:str='pipeline_ops'
    lease_collection:str='pipeline_writer_leases'
    lease_uri_env:str='GOLD_LEASE_MONGO_URI'
    mongo_replica_set:str='rs-products'
    lease_seconds:int=120
    lease_heartbeat_seconds:int=10
    max_records:int=20
    max_product_rows:int=20000
    max_product_bytes:int=32*1024*1024

    def __post_init__(self):
        for value in (self.environment_id,self.lease_database,self.lease_collection,self.mongo_replica_set):
            if not isinstance(value,str) or not re.fullmatch(r'[A-Za-z0-9_.-]{1,100}',value):raise PipelineError('GOLD_INVALID_CONFIG')
        for value in (self.spark_options_env,self.lease_uri_env):
            if not isinstance(value,str) or not re.fullmatch(r'[A-Z][A-Z0-9_]*',value):raise PipelineError('GOLD_INVALID_CONFIG')
        paths=[location(p) for p in (self.silver_root,self.gold_root,self.checkpoint_path)]
        if any(a==b or a.startswith(b+'/') or b.startswith(a+'/') for i,a in enumerate(paths) for b in paths[i+1:]):
            raise PipelineError('GOLD_PATHS_OVERLAP')
        if (type(self.max_records) is not int or not 1<=self.max_records<=100
                or type(self.max_product_rows) is not int or not 1<=self.max_product_rows<=100000
                or type(self.max_product_bytes) is not int or not 1<=self.max_product_bytes<=128*1024*1024
                or type(self.lease_seconds) is not int or not 60<=self.lease_seconds<=3600
                or type(self.lease_heartbeat_seconds) is not int or not 1<=self.lease_heartbeat_seconds<=self.lease_seconds//6
                or self.lease_collection=='raw_products'):
            raise PipelineError('GOLD_INVALID_CONFIG')

    @property
    def fingerprint(self):
        return digest(dict(environment_id=self.environment_id,silver_root=location(self.silver_root),
            gold_root=location(self.gold_root),checkpoint_path=location(self.checkpoint_path),schema_version='gold-1'))

    @classmethod
    def load(cls,path):
        try:return cls(**read_json(path))
        except TypeError as exc:raise PipelineError('GOLD_INVALID_CONFIG') from exc
