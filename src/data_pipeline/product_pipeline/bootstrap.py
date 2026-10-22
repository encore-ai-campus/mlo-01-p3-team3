"""Environment configuration and live ingestion adapter composition."""
import os

from .errors import PipelineError
from .files import parse


def required_env(name):
    value = os.environ.get(name)
    if not value:
        raise PipelineError("REQUIRED_ENV_MISSING", name)
    return value



def open_live(settings, stack):
    from .mongo import MongoLedger
    from .objects import S3Objects
    from .publisher import KafkaPublisher
    if any(v.startswith("replace-with-") for v in (settings.environment_id, settings.mongo_database, settings.image_bucket)):
        raise PipelineError("EXAMPLE_CONFIG_NOT_DEPLOYABLE")
    # Read every required configuration before opening a network connection.
    uri = required_env(settings.mongo_uri_env)
    endpoint = required_env(settings.s3_endpoint_env)
    kafka = parse(required_env(settings.kafka_config_env))
    if endpoint != "aws" and not endpoint.startswith("https://"):
        raise PipelineError("S3_TLS_ENDPOINT_REQUIRED")
    ledger = MongoLedger(settings, uri)
    stack.callback(ledger.close)
    objects = S3Objects.from_settings(settings, endpoint)
    publisher = KafkaPublisher(settings.kafka_topic, kafka, max_attempts=settings.max_attempts)
    stack.callback(publisher.close)
    return ledger, objects, publisher


def open_incremental(config, stack):
    """Compose the existing trusted server coordinator and product adapters."""
    import importlib.util
    import sys
    from pathlib import Path
    from .files import sha_file
    from .errors import Conflict
    from .infrastructure.incremental_state import IncrementalState
    from .incremental.production import ProductionChanges,ProductionStages
    from .publication.mysql import MySQL
    from pymongo import MongoClient,ReadPreference
    from pymongo.read_concern import ReadConcern
    from pymongo.write_concern import WriteConcern
    settings=config['incremental'];root=Path(settings['production_root']).resolve()
    script=root/'production_work.py'
    if sha_file(script)!=settings['production_sha256']:raise Conflict('INCREMENTAL_PRODUCTION_CODE_CHANGED')
    if str(root) not in sys.path:sys.path.insert(0,str(root))
    work=sys.modules.get('production_work')
    if work is None or Path(work.__file__).resolve()!=script:
        spec=importlib.util.spec_from_file_location('production_work',script)
        work=importlib.util.module_from_spec(spec);sys.modules['production_work']=work;spec.loader.exec_module(work)
    prepared=work.setup()
    original_setup=work.setup
    work.setup=lambda:prepared
    stack.callback(setattr,work,'setup',original_setup)
    db=MySQL(prepared[0]);stack.callback(db.close)
    mongo=MongoClient(required_env('PIPELINE_MONGO_URI'),tz_aware=True,retryWrites=False,
        serverSelectionTimeoutMS=5000,socketTimeoutMS=30000)
    stack.callback(mongo.close)
    hello=mongo.admin.command('hello')
    if hello.get('setName')!=settings['mongo_replica_set'] or len(hello.get('hosts',[]))!=3 or hello.get('arbiters'):
        raise PipelineError('REPLICA_SET_CONTRACT_REQUIRED')
    database=mongo.get_database(settings['mongo_database'],read_preference=ReadPreference.PRIMARY,
        read_concern=ReadConcern('majority'),write_concern=WriteConcern(w='majority',j=True,wtimeout=30000))
    pending=database.ingestion_observations.index_information().get('pipeline_incremental_pending_v1')
    if not pending or pending.get('key')!=[('incremental_change',1)] or pending.get('sparse') is not True:
        raise PipelineError('INCREMENTAL_PENDING_INDEX_REQUIRED')
    state=IncrementalState(settings['state_root'],config['logical_target'])
    state.bind(dict(mongo_database=settings['mongo_database'],mysql_target=db.target,lake=work.LAKE))
    changes=ProductionChanges(database.ingestion_observations,db,state.root,work,prepared[1])
    return state,changes,ProductionStages(work,changes,*prepared)
