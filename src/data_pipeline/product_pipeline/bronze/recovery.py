"""Explicit checkpoint generations keep the original Delta target identity.

A recovery generation is bound to a sealed replay plan and Kafka topic ID.
It never edits or deletes the original checkpoint or manufactures offset logs.
"""
import json
from pathlib import Path
import re
from ..contracts import canonical,digest
from ..errors import Conflict,PipelineError


def make_recovery(settings,plan,manifest_path,*,cluster_id,topic_id,frontiers):
    from uuid import uuid4
    from dataclasses import replace
    from ..contracts import now
    if cluster_id!=settings.kafka_cluster_id or set(frontiers)!={'0'}:
        raise Conflict('BRONZE_RECOVERY_SOURCE_CHANGED')
    assert_offsets({settings.kafka_topic:{k:v[0] for k,v in frontiers.items()}},frontiers,settings.kafka_topic)
    rid=uuid4().hex
    value=dict(contract_version=1,reason='KAFKA_RETENTION_GAP',recovery_id=rid,
        original_target_fingerprint=settings.fingerprint,original_checkpoint_path=settings.checkpoint_path,
        checkpoint_path=settings.checkpoint_path+'-recovery/'+rid,kafka_cluster_id=cluster_id,
        kafka_topic=settings.kafka_topic,kafka_topic_id=topic_id,
        starting_offsets={settings.kafka_topic:{k:v[0] for k,v in frontiers.items()}},
        replay_manifest_hash=plan['content_hash'],replay_manifest_file=str(Path(manifest_path).resolve()),created_at=now())
    value['content_hash']=digest(value)
    validate_replay_plan(replace(settings,checkpoint_recovery=value))
    return value


def recovery(settings):
    value=getattr(settings,'checkpoint_recovery',None)
    if value is None:return None
    keys={'contract_version','reason','recovery_id','original_target_fingerprint','original_checkpoint_path',
          'checkpoint_path','kafka_cluster_id','kafka_topic','kafka_topic_id','starting_offsets',
          'replay_manifest_hash','replay_manifest_file','created_at','content_hash'}
    try:
        if (set(value)!=keys or value['contract_version']!=1 or value['reason']!='KAFKA_RETENTION_GAP'
                or value['content_hash']!=digest({k:v for k,v in value.items() if k!='content_hash'})
                or value['original_target_fingerprint']!=settings.fingerprint
                or value['original_checkpoint_path']!=settings.checkpoint_path
                or value['kafka_cluster_id']!=settings.kafka_cluster_id or value['kafka_topic']!=settings.kafka_topic
                or not re.fullmatch('[a-f0-9]{32}',value['recovery_id'])
                or value['checkpoint_path']!=settings.checkpoint_path+'-recovery/'+value['recovery_id']
                or not re.fullmatch('[a-f0-9]{64}',value['replay_manifest_hash'])
                or not isinstance(value['kafka_topic_id'],str) or not value['kafka_topic_id']
                or not Path(value['replay_manifest_file']).is_absolute()):raise ValueError()
        positions=value['starting_offsets']
        if set(positions)!={settings.kafka_topic} or not positions[settings.kafka_topic]:raise ValueError()
        if any(not re.fullmatch('0|[1-9][0-9]*',p) or type(n) is not int or n<0
               for p,n in positions[settings.kafka_topic].items()):raise ValueError()
    except (ValueError,KeyError,TypeError,AttributeError):raise Conflict('BRONZE_RECOVERY_MANIFEST_INVALID') from None
    return value


def checkpoint_path(settings):
    value=recovery(settings)
    return value['checkpoint_path'] if value else settings.checkpoint_path


def validate_replay_plan(settings):
    value=recovery(settings)
    if value is None:return
    from ..files import read_json
    from ..event_replay import file_hash
    path=Path(value['replay_manifest_file']);plan=read_json(path)
    if (plan.get('content_hash')!=value['replay_manifest_hash']
            or plan['content_hash']!=digest({k:v for k,v in plan.items() if k!='content_hash'})
            or plan.get('events',0)<=0 or file_hash(path.with_name('events.jsonl'))!=plan['events_sha256']):
        raise Conflict('BRONZE_RECOVERY_REPLAY_PLAN_CHANGED')


def assert_offsets(expected,frontiers,topic):
    if set(expected)!={topic} or set(expected[topic])!=set(frontiers):
        raise PipelineError('BRONZE_KAFKA_PARTITIONS_CHANGED')
    for partition,offset in expected[topic].items():
        low,high=frontiers[partition]
        if type(offset) is not int or not low<=offset<=high:
            raise PipelineError('BRONZE_KAFKA_OFFSETS_OUT_OF_RANGE')


def checkpoint_offsets(spark,settings,frontiers):
    path=spark._jvm.org.apache.hadoop.fs.Path(checkpoint_path(settings))
    fs=path.getFileSystem(spark._jsc.hadoopConfiguration());PathClass=spark._jvm.org.apache.hadoop.fs.Path
    def read(p):
        stream=fs.open(p)
        try:
            data=spark._jvm.org.apache.commons.io.IOUtils.toString(stream,'UTF-8')
            if len(data)>1024*1024:raise ValueError()
            return json.loads(data.splitlines()[-1])
        finally:stream.close()
    try:
        commits=PathClass(path,'commits')
        ids=[int(s.getPath().getName()) for s in fs.listStatus(commits) if s.getPath().getName().isdigit()] if fs.exists(commits) else []
        if ids:return read(PathClass(path,'offsets/'+str(max(ids))))
        initial=PathClass(path,'sources/0/0')
        if fs.exists(initial):return read(initial)
        value=recovery(settings)
        return value['starting_offsets'] if value else {settings.kafka_topic:{k:v[0] for k,v in frontiers.items()}}
    except PipelineError:raise
    except Exception:raise PipelineError('BRONZE_CHECKPOINT_OFFSETS_UNCONFIRMED') from None


def check_frontiers(spark,admin,settings):
    jvm=spark._jvm;unit=jvm.java.util.concurrent.TimeUnit.MILLISECONDS
    names=jvm.java.util.ArrayList();names.add(settings.kafka_topic)
    description=admin.describeTopics(names).allTopicNames().get(10000,unit).get(settings.kafka_topic)
    value=recovery(settings)
    if value and str(description.topicId())!=value['kafka_topic_id']:raise Conflict('BRONZE_KAFKA_TOPIC_CHANGED')
    low=jvm.java.util.HashMap();high=jvm.java.util.HashMap()
    for part in description.partitions():
        key=jvm.org.apache.kafka.common.TopicPartition(settings.kafka_topic,part.partition())
        low.put(key,jvm.org.apache.kafka.clients.admin.OffsetSpec.earliest())
        high.put(key,jvm.org.apache.kafka.clients.admin.OffsetSpec.latest())
    earliest=admin.listOffsets(low).all().get(10000,unit);latest=admin.listOffsets(high).all().get(10000,unit)
    ranges={str(p.partition()):(int(earliest.get(p).offset()),int(latest.get(p).offset())) for p in low.keySet()}
    assert_offsets(checkpoint_offsets(spark,settings,ranges),ranges,settings.kafka_topic)
