"""Resume fixed events for already confirmed immutable MongoDB raw documents."""
from .errors import Conflict
from .contracts import now


def relay(ledger,publisher,*,collection_run_id,progress=None,snapshot_ids=None):
    sent=reused=0
    query={'document.collection_run_id':collection_run_id,'raw_confirmed':True,'event_confirmed':{'$ne':True}}
    if snapshot_ids:
        if len(snapshot_ids)>1000 or any(not isinstance(s,str) or len(s)!=64 for s in snapshot_ids):
            raise Conflict('RELAY_SNAPSHOT_SCOPE_INVALID')
        query['document.input_snapshot_id']={'$in':list(snapshot_ids)}
    with ledger.exclusive():
        keys=ledger.observations.find(query,{'observation_key':1}).sort('observation_key',1).batch_size(100)
        for record in keys:
            key=record['observation_key'];token=ledger.acquire(key)
            try:
                saved=ledger.get(key)
                if saved['target_fingerprint']!=ledger.settings.fingerprint:raise Conflict('TARGET_MISMATCH')
                if not saved.get('raw_confirmed'):raise Conflict('RELAY_RAW_NOT_CONFIRMED')
                # Majority readback compares the complete immutable body before emission.
                from bson import ObjectId
                from .mongo import wire_document
                stored=ledger.raw.find_one({'_id':ObjectId(saved['document']['_id'])})
                if stored is None or wire_document(stored)!=saved['document']:raise Conflict('RELAY_RAW_UNCONFIRMED')
                if saved.get('event_confirmed'):reused+=1;continue
                ledger.update(key,token,stage='EVENT_PENDING')
                try:publisher.send(saved['event'],saved['document']['source_record_key'])
                except BaseException:
                    ledger.update(key,token,stage='EVENT_UNKNOWN');raise
                ledger.update(key,token,stage='EVENT_CONFIRMED',event_confirmed=True,last_error=None);sent+=1
                if progress:progress(dict(events_sent=sent,events_reused=reused,last_event_id=saved['event']['event_id'],updated_at=now()))
            finally:ledger.release(key,token)
    return dict(status='COMPLETED',events_sent=sent,events_reused=reused,finished_at=now())
