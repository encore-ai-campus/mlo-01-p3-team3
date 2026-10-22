"""Seal confirmed source events, then replay a bounded portion. No ledger resets."""
import argparse,json,os,sys
from pathlib import Path


def client_config(base):
    from product_pipeline.files import read_json
    from product_pipeline.errors import PipelineError
    config=read_json(Path(base)/'config/kafka-python.json')
    if config.get('security.protocol')!='SSL' or config.get('ssl.endpoint.identification.algorithm','https')!='https':
        raise PipelineError('KAFKA_RECOVERY_TLS_CONFIG_REQUIRED')
    return config


def topup(root,ceiling=1000,expected_directory=None):
    from product_pipeline.files import lock
    with lock(Path(root)/'checkpoint-generation.lock'):
        return _topup_locked(root,ceiling,expected_directory)


def _topup_locked(root,ceiling=1000,expected_directory=None):
    """Called between epochs; at most 1,000 uncommitted replay events in Kafka."""
    root=Path(root);sys.path[:0]=[str(root),str(root/'source')]
    from product_pipeline.files import read_json,lock,atomic_json
    from product_pipeline.event_replay import replay,admission_count
    from product_pipeline.errors import PipelineError
    path=root/'bronze-recovery.json'
    if not path.exists():return dict(pending=False,enabled=False)
    recovery=read_json(path);directory=Path(recovery['replay_manifest_file']).parent
    if expected_directory is not None and directory.resolve()!=Path(expected_directory).resolve():raise ValueError('REPLAY_SOURCE_CHANGED')
    if not directory.resolve().is_relative_to(root.resolve()):raise ValueError('REPLAY_DIRECTORY_OUTSIDE_DEPLOYMENT')
    from dataclasses import replace
    from product_pipeline.bronze.config import BronzeSettings
    from product_pipeline.bronze.recovery import validate_replay_plan
    settings=replace(BronzeSettings.load(directory/'original-bronze-config.json'),checkpoint_recovery=recovery)
    validate_replay_plan(settings);manifest=read_json(directory/'manifest.json')
    from production_work import BASE
    from product_pipeline.publisher import KafkaPublisher
    from confluent_kafka import Consumer,TopicPartition,TopicCollection
    from confluent_kafka.admin import AdminClient
    config=client_config(BASE);admin=AdminClient(config)
    metadata=admin.list_topics(recovery['kafka_topic'],timeout=8)
    description=admin.describe_topics(TopicCollection([recovery['kafka_topic']]),request_timeout=8)[recovery['kafka_topic']].result(10)
    actual_topic=str(description.topic_id).replace('+','-').replace('/','_').rstrip('=')
    if metadata.cluster_id!=recovery['kafka_cluster_id'] or actual_topic!=recovery['kafka_topic_id']:
        raise PipelineError('BRONZE_KAFKA_TOPIC_CHANGED')
    if set(metadata.topics[recovery['kafka_topic']].partitions)!={0}:raise PipelineError('BRONZE_KAFKA_PARTITIONS_CHANGED')
    reader=Consumer({**config,'group.id':'pj3-replay-frontier-readonly','enable.auto.commit':False,'enable.auto.offset.store':False,'allow.auto.create.topics':False})
    try:low,high=reader.get_watermark_offsets(TopicPartition(recovery['kafka_topic'],0),timeout=8,cached=False)
    finally:reader.close()
    import boto3
    from urllib.parse import urlsplit
    from botocore.config import Config
    from product_pipeline.s3_credentials import service_credentials
    target=urlsplit(recovery['checkpoint_path']);prefix=target.path.strip('/')+'/'
    endpoint=os.environ.get('PIPELINE_S3_ENDPOINT','https://192.168.0.141:8443')
    parsed=urlsplit(endpoint)
    if parsed.scheme!='https' or not parsed.hostname or parsed.username or parsed.password or parsed.query or parsed.fragment or parsed.path not in ('','/'):
        raise PipelineError('S3_TLS_ENDPOINT_REQUIRED')
    s3=boto3.client('s3',endpoint_url=endpoint,region_name='us-east-1',
        verify=os.environ.get('AWS_CA_BUNDLE','/etc/pj3-clients/s3/ca.pem'),
        **service_credentials(os.environ.get('PIPELINE_SPARK_S3_CREDENTIALS_FILE','/etc/pj3-clients/s3/spark.properties')),
        config=Config(signature_version='s3v4',s3={'addressing_style':'path'},connect_timeout=5,read_timeout=10,retries={'max_attempts':1}))
    commits=[]
    for page in s3.get_paginator('list_objects_v2').paginate(Bucket=target.netloc,Prefix=prefix+'commits/'):
        commits.extend(int(v['Key'].rsplit('/',1)[-1]) for v in page.get('Contents',[]) if v['Key'].rsplit('/',1)[-1].isdigit())
    committed=recovery['starting_offsets'][recovery['kafka_topic']]['0']
    if commits:
        proof=s3.get_object(Bucket=target.netloc,Key=prefix+'_pipeline_recovery')['Body'].read(1024)
        if proof!=len(recovery['content_hash']).to_bytes(2,'big')+recovery['content_hash'].encode():raise PipelineError('BRONZE_RECOVERY_CHECKPOINT_CHANGED')
        body=s3.get_object(Bucket=target.netloc,Key=prefix+'offsets/'+str(max(commits)))['Body'].read(1024*1024).decode()
        committed=json.loads(body.splitlines()[-1])[recovery['kafka_topic']]['0']
    admitted=admission_count(committed,low,high,ceiling=ceiling)
    with lock(directory/'replay.lock'):
        state=read_json(directory/'replay-progress.json') if (directory/'replay-progress.json').exists() else dict(events_sent=0)
        if admitted and state['events_sent']<manifest['events']:
            from product_pipeline.bronze.mongo import MongoSource
            source=MongoSource(settings,os.environ['PIPELINE_MONGO_URI']);publisher=KafkaPublisher(settings.kafka_topic,config)
            try:
                publisher.preflight()
                state=replay(directory,publisher.producer,source.get,max_events=admitted,
                    progress=lambda value:atomic_json(directory/'progress.json',value))
            finally:publisher.close();source.close()
    return dict(enabled=True,pending=state['events_sent']<manifest['events'],events_sent=state['events_sent'],
                planned_events=manifest['events'],committed_offset=committed,queued_before=high-committed,admission_limit=admitted)


def main(argv=None):
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('action',choices=['freeze','create','replay','status'])
    p.add_argument('--root',type=Path,required=True);p.add_argument('--directory',type=Path,required=True)
    p.add_argument('--run-id',required=True);p.add_argument('--expected-count',type=int,default=81882)
    p.add_argument('--max-events',type=int,default=1000)
    p.add_argument('--source-config',type=Path,help='Existing Bronze config; required for create')
    p.add_argument('--replace-generation',action='store_true',help='Preserve the previous manifest, then explicitly switch generation')
    args=p.parse_args(argv);root=args.root.resolve();sys.path[:0]=[str(root),str(root/'source')]
    from production_work import setup,BASE
    from product_pipeline.files import read_json,atomic_json,lock
    from product_pipeline.event_replay import freeze,replay
    from pymongo import MongoClient
    from pymongo.read_concern import ReadConcern
    from product_pipeline.bronze.config import BronzeSettings
    from product_pipeline.bronze.mongo import MongoSource
    from product_pipeline.publisher import KafkaPublisher
    setup();args.directory.mkdir(parents=True,exist_ok=True,mode=0o700)
    def progress(value):atomic_json(args.directory/'progress.json',value)
    if args.action=='status':
        print(json.dumps({p.name:read_json(p) for p in args.directory.glob('*.json')}));return
    if args.action=='replay':
        if read_json(args.directory/'manifest.json')['source_run_id']!=args.run_id:raise ValueError('REPLAY_SOURCE_CHANGED')
        print(json.dumps(topup(root,ceiling=args.max_events,expected_directory=args.directory)));return
    from contextlib import nullcontext
    configuration_lock=lock(root/'checkpoint-generation.lock') if args.action=='create' else nullcontext()
    with configuration_lock,lock(args.directory/'replay.lock'):
        if args.action=='create':
            if args.source_config is None:raise ValueError('BRONZE_SOURCE_CONFIG_REQUIRED')
            from dataclasses import replace,asdict
            from product_pipeline.bronze.recovery import make_recovery,validate_replay_plan
            from product_pipeline.recovery_probe import probe
            from product_pipeline.lease_recovery import assert_quiet
            quiet=probe(master_url='http://192.168.0.121:8080/json/',ca='/etc/pj3-secrets/spark/ca.pem',
                client_cert='/etc/pj3-secrets/spark/cert.pem',client_key='/etc/pj3-secrets/spark/key.pem',
                worker_hosts=['192.168.0.122','192.168.0.123','192.168.0.124'])
            assert_quiet(quiet)
            settings=replace(BronzeSettings.load(args.source_config),checkpoint_recovery=None)
            plan=read_json(args.directory/'manifest.json')
            if plan['source_run_id']!=args.run_id:raise ValueError('REPLAY_SOURCE_CHANGED')
            active=root/'bronze-recovery.json'
            if active.exists():
                existing=read_json(active)
                if (existing['replay_manifest_hash']==plan['content_hash']
                        and Path(existing['replay_manifest_file']).resolve()==(args.directory/'manifest.json').resolve()):
                    validate_replay_plan(replace(settings,checkpoint_recovery=existing))
                    print(json.dumps(dict(status='EXISTING_GENERATION',checkpoint_path=existing['checkpoint_path'])));return
                if not args.replace_generation:raise ValueError('RECOVERY_GENERATION_REPLACEMENT_REQUIRES_EXPLICIT_FLAG')
            from confluent_kafka import Consumer,TopicPartition,TopicCollection
            from confluent_kafka.admin import AdminClient
            config=client_config(BASE);admin=AdminClient(config)
            metadata=admin.list_topics(settings.kafka_topic,timeout=8)
            if set(metadata.topics[settings.kafka_topic].partitions)!={0}:raise ValueError('RECOVERY_PARTITION_CONTRACT_CHANGED')
            description=admin.describe_topics(TopicCollection([settings.kafka_topic]),request_timeout=8)[settings.kafka_topic].result(10)
            consumer=Consumer({**config,'group.id':'pj3-recovery-create-readonly','enable.auto.commit':False,'enable.auto.offset.store':False})
            try:frontier=consumer.get_watermark_offsets(TopicPartition(settings.kafka_topic,0),timeout=8,cached=False)
            finally:consumer.close()
            value=make_recovery(settings,plan,args.directory/'manifest.json',cluster_id=metadata.cluster_id,
                topic_id=str(description.topic_id).replace('+','-').replace('/','_').rstrip('='),frontiers={'0':frontier})
            if active.exists():
                old=Path(existing['replay_manifest_file']).parent/'checkpoint-recovery.json'
                if old.exists() and read_json(old)!=existing:raise ValueError('PREVIOUS_RECOVERY_ARCHIVE_CHANGED')
                if not old.exists():atomic_json(old,existing)
            original=args.directory/'original-bronze-config.json'
            if original.exists() and read_json(original)!=asdict(settings):raise ValueError('ORIGINAL_BRONZE_CONFIG_CHANGED')
            if not original.exists():atomic_json(original,asdict(settings))
            atomic_json(args.directory/'checkpoint-recovery.json',value);atomic_json(active,value)
            print(json.dumps(dict(status='CREATED_GENERATION',checkpoint_path=value['checkpoint_path'])));return
        if args.action=='freeze':
            client=MongoClient(os.environ['PIPELINE_MONGO_URI'],replicaSet='rs-products',tz_aware=True,
                               serverSelectionTimeoutMS=8000,socketTimeoutMS=30000)
            try:
                db=client.get_database('pj3_products_raw',read_concern=ReadConcern('majority'))
                query={'event.correlation_id':args.run_id,'raw_confirmed':True,'event_confirmed':True}
                projection={'event':1,'observation_key':1,'document._id':1,'document.document_hash':1,'raw_confirmed':1,'event_confirmed':1}
                records=db.ingestion_observations.find(query,projection).sort('_id',1).batch_size(100)
                result=freeze(records,args.directory,run_id=args.run_id,expected_count=args.expected_count,progress=progress)
                progress(dict(status='FROZEN',**result));print(json.dumps(result))
            finally:client.close()


if __name__=='__main__':main()
