"""Environment-only composition. No command parsing or operational workflow imports."""
from contextlib import ExitStack, contextmanager
from pathlib import Path
import os

from .domain.contracts import Limits, Invalid
from .adapters.files import read
from .adapters.state import State
from .adapters.timing import Timing
from .adapters.sources import JsonSource, MongoSource
from .adapters.publication import FilePublisher, MySQLPublisher
from .application.coordinator import Coordinator


def configuration(path):
    path=Path(path).resolve();value=read(path)
    if value.get('contract_version')!=2 or value.get('mode') not in {'local','server'}:
        raise Invalid('V2_CONFIG_REQUIRED')
    if not value.get('target') or not value.get('state_root'):raise Invalid('TARGET_AND_STATE_REQUIRED')
    for name in ('state_root','source_file','lake_root','publication_root'):
        if name in value and value[name] and '://' not in value[name]:
            value[name]=str((path.parent/value[name]).resolve())
    if value['mode']=='local':
        roots=[Path(value[k]).resolve() for k in ('state_root','lake_root','publication_root')]
        if any(a==b or a.is_relative_to(b) or b.is_relative_to(a) for i,a in enumerate(roots) for b in roots[i+1:]):
            raise Invalid('LOCAL_OUTPUTS_OVERLAP')
        if any(Path(value['source_file']).is_relative_to(root) for root in roots):raise Invalid('SOURCE_OUTPUT_OVERLAP')
    else:
        if any('replace-with-' in str(value.get(k,'')) for k in ('target','mongo','lease','mysql')):
            raise Invalid('SERVER_PLACEHOLDERS_REQUIRED_TO_REPLACE')
        if not value['lake_root'].rstrip('/').endswith('/direct-v2'):
            raise Invalid('SEPARATE_V2_LAKE_NAMESPACE_REQUIRED')
        if value.get('single_coordinator') is not True:raise Invalid('SINGLE_COORDINATOR_REQUIRED')
        for key in ('mongo','lease','mysql','spark_options_env'):
            if not value.get(key):raise Invalid('SERVER_RUNTIME_CONFIG_REQUIRED')
        source_product_ids=value['mongo'].get('source_product_ids')
        if source_product_ids is not None and (not isinstance(source_product_ids,list) or not source_product_ids
                or len(source_product_ids)>100 or any(not isinstance(v,str) or not v or len(v)>64 for v in source_product_ids)
                or len(set(source_product_ids))!=len(source_product_ids)):
            raise Invalid('SOURCE_PRODUCT_SCOPE_INVALID')
    Limits(**value.get('limits',{}))
    policy=value.get('policy',{})
    if set(policy)-{'mappings','rules','gold'}:raise Invalid('DIRECT_POLICY_KEYS_INVALID')
    if set(policy.get('gold',{}))-{'require_image','require_comparison_link'}:raise Invalid('GOLD_POLICY_KEYS_INVALID')
    return value


def required(name):
    value=os.environ.get(name,'')
    if not value:raise Invalid('REQUIRED_ENV_MISSING')
    return value


@contextmanager
def runtime(config, writable=False,scope_id='runtime'):
    with ExitStack() as stack:
        limits=Limits(**config.get('limits',{}))
        timing=Timing(config['state_root'])
        if config['mode']=='local':
            from .adapters.files import FileLake
            with timing.measure('bootstrap.source_open',scope_id):source=JsonSource(config['source_file'],limits.page_bytes)
            lake=FileLake(config['lake_root']);publisher=FilePublisher(config['publication_root'])
        else:
            from .adapters.spark_runtime import ServerLake
            with timing.measure('bootstrap.mongo_connect',scope_id):
                source=MongoSource(required(config['mongo']['uri_env']),config['mongo']['database'],config['mongo']['replica_set'],
                    config['mongo']['collection_run_id'],limits.page_bytes,config['mongo'].get('source_product_ids'))
            stack.callback(source.close)
            lake=ServerLake(config,limits,writable,timing=timing)
            publisher=MySQLPublisher(config['mysql'],fence=lake.check,timing=timing)
        stack.callback(publisher.close);stack.callback(lake.close)
        if config['mode']=='local':stack.callback(source.close)
        target=dict(logical=config['target'],source={k:v for k,v in source.identity.items() if k not in {'content_hash','collection_run_id'}},
            lake=lake.identity,publisher=publisher.identity)
        state=State(config['state_root'],target)
        state.timing=timing
        yield Coordinator(state,source,lake,publisher,limits)
