"""Immutable, bounded Kafka recovery. Original ingestion receipts stay untouched."""
import hashlib
import os
from pathlib import Path
import time

from .contracts import canonical,digest,now
from .errors import Conflict,PipelineError
from .files import atomic_json,read_json
from .bronze.contracts import Message,event_from,validate_raw


def file_hash(path):
    h=hashlib.sha256()
    with Path(path).open('rb') as f:
        for data in iter(lambda:f.read(1024*1024),b''):h.update(data)
    return h.hexdigest()


def freeze(records,directory,*,run_id,expected_count,progress=None):
    directory=Path(directory);directory.mkdir(parents=True,exist_ok=True,mode=0o700)
    manifest=directory/'manifest.json';events=directory/'events.jsonl'
    if manifest.exists():
        value=read_json(manifest)
        if (value.get('content_hash')!=digest({k:v for k,v in value.items() if k!='content_hash'})
                or value['source_run_id']!=run_id or value['events']!=expected_count or file_hash(events)!=value['events_sha256']):
            raise Conflict('REPLAY_PLAN_CHANGED')
        return value
    if events.exists():raise PipelineError('REPLAY_MANIFEST_MISSING')
    partial=directory/'events.jsonl.partial';count=0;ids=set();previous=None
    with partial.open('xb') as stream:
        os.chmod(partial,0o600)
        for saved in records:
            event=saved['event'];p=event['payload'];doc=saved['document'];key=p['source_record_key']
            event_from(Message('product.raw.collected',0,count,key.encode(),canonical(event).encode()))
            if (not saved.get('raw_confirmed') or not saved.get('event_confirmed')
                    or event['correlation_id']!=run_id or p['raw_document_id']!=str(doc['_id'])
                    or p['document_hash']!=doc['document_hash'] or p['observation_key']!=saved['observation_key']
                    or event['event_id'] in ids or (previous is not None and saved['observation_key']<=previous)):
                raise Conflict('REPLAY_SOURCE_CONFLICT')
            previous=saved['observation_key'];ids.add(event['event_id'])
            stream.write((canonical(dict(event=event,key=key))+'\n').encode());count+=1
            if progress and count%1000==0:progress(dict(status='FREEZING',events=count,updated_at=now()))
        stream.flush();os.fsync(stream.fileno())
    if count!=expected_count:raise Conflict('REPLAY_SOURCE_COUNT_CHANGED')
    partial.replace(events)
    value=dict(contract_version=1,source_run_id=run_id,events=count,events_sha256=file_hash(events),
               bytes=events.stat().st_size,created_at=now())
    value['content_hash']=digest(value);atomic_json(manifest,value)
    return value


def send_batch(producer,topic,rows,*,clock=time.monotonic):
    if not 1<=len(rows)<=100:raise PipelineError('REPLAY_BATCH_LIMIT')
    delivered=[None]*len(rows);seen=set();errors=[]
    def callback(index,error,message):
        seen.add(index)
        if error:errors.append(index)
        else:delivered[index]=dict(partition=message.partition(),offset=message.offset())
    for index,row in enumerate(rows):
        producer.produce(topic,key=row['key'].encode(),value=canonical(row['event']).encode(),
                         on_delivery=lambda error,message,i=index:callback(i,error,message))
    deadline=clock()+35
    while len(seen)<len(rows) and clock()<deadline:producer.poll(0.1)
    if len(seen)!=len(rows) or errors or any(v is None for v in delivered):raise PipelineError('REPLAY_DELIVERY_UNCONFIRMED')
    return delivered


def replay(directory,producer,raw_get,*,max_events=1000,topic='product.raw.collected',progress=None):
    """Resume only an acknowledged byte cursor. Unknown delivery reuses exact IDs."""
    if type(max_events) is not int or not 1<=max_events<=1000:raise PipelineError('REPLAY_ADMISSION_LIMIT')
    directory=Path(directory);manifest=read_json(directory/'manifest.json');events=directory/'events.jsonl'
    if (manifest.get('content_hash')!=digest({k:v for k,v in manifest.items() if k!='content_hash'})
            or file_hash(events)!=manifest['events_sha256']):raise Conflict('REPLAY_PLAN_CHANGED')
    state_path=directory/'replay-progress.json'
    state=read_json(state_path) if state_path.exists() else dict(manifest_hash=manifest['content_hash'],events_sent=0,byte_cursor=0)
    if state['manifest_hash']!=manifest['content_hash']:raise Conflict('REPLAY_STATE_CHANGED')
    if (type(state['events_sent']) is not int or not 0<=state['events_sent']<=manifest['events']
            or type(state['byte_cursor']) is not int or not 0<=state['byte_cursor']<=manifest['bytes']):
        raise Conflict('REPLAY_STATE_CHANGED')
    with events.open('rb') as prefix:
        left=state['byte_cursor'];lines=0;last=b''
        while left:
            data=prefix.read(min(left,1024*1024));left-=len(data);lines+=data.count(b'\n');last=data[-1:]
        if lines!=state['events_sent'] or (state['byte_cursor'] and last!=b'\n'):raise Conflict('REPLAY_STATE_CHANGED')
    admitted=0
    import json
    with events.open('rb') as stream:
        stream.seek(state['byte_cursor'])
        while admitted<max_events:
            rows=[]
            for _ in range(min(100,max_events-admitted)):
                line=stream.readline(16385)
                if not line:break
                if len(line)>16384 or not line.endswith(b'\n'):raise Conflict('REPLAY_LINE_INVALID')
                rows.append(json.loads(line))
            if not rows:break
            receipt_path=directory/'delivery-receipts'/('%012d.json'%state['byte_cursor'])
            identity=dict(manifest_hash=manifest['content_hash'],start_byte=state['byte_cursor'],end_byte=stream.tell(),
                          rows_hash=digest(rows),event_ids=[r['event']['event_id'] for r in rows])
            if receipt_path.exists():
                receipt=read_json(receipt_path)
                if (receipt.get('content_hash')!=digest({k:v for k,v in receipt.items() if k!='content_hash'})
                        or any(receipt.get(k)!=v for k,v in identity.items()) or receipt.get('status')!='CONFIRMED'):
                    raise Conflict('REPLAY_RECEIPT_CHANGED')
                positions=receipt['positions']
                if len(positions)!=len(rows):raise Conflict('REPLAY_RECEIPT_CHANGED')
            else:
                for row in rows:
                    event=row['event']
                    event_from(Message(topic,0,0,row['key'].encode(),canonical(event).encode()))
                    validate_raw(raw_get(event['payload']['raw_document_id']),event)
                try:positions=send_batch(producer,topic,rows)
                except Exception as exc:
                    atomic_json(receipt_path.with_suffix('.unknown.json'),dict(identity,status='UNKNOWN',at=now(),
                                error_code=getattr(exc,'code',type(exc).__name__)))
                    raise
                receipt=dict(identity,status='CONFIRMED',positions=positions,at=now())
                receipt['content_hash']=digest(receipt);atomic_json(receipt_path,receipt)
            state.update(events_sent=state['events_sent']+len(rows),byte_cursor=stream.tell(),last_delivery=positions[-1],
                         updated_at=now(),status='COMPLETED' if state['events_sent']+len(rows)==manifest['events'] else
                         'PAUSED_ADMISSION_LIMIT' if admitted+len(rows)>=max_events else 'RUNNING')
            atomic_json(state_path,state);admitted+=len(rows)
            if progress:progress(state)
    return state


def admission_count(committed,earliest,latest,*,ceiling=1000):
    if any(type(v) is not int or v<0 for v in (committed,earliest,latest,ceiling)) or not 1<=ceiling<=1000:
        raise PipelineError('REPLAY_ADMISSION_LIMIT')
    if not earliest<=committed<=latest:raise PipelineError('BRONZE_KAFKA_OFFSETS_OUT_OF_RANGE')
    return max(0,ceiling-(latest-committed))
