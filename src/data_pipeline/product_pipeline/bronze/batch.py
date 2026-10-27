"""Bounded microbatch. Raising prevents Spark from checkpointing completion."""
from ..contracts import canonical
from ..errors import PipelineError
from .contracts import Poison, event_from, event_row, quarantine_row, raw_row, validate_raw


def process_batch(messages, source, sink, batch_id, max_records=20, max_raw_bytes=32 * 1024 * 1024,
                  max_input_bytes=8 * 1024 * 1024):
    inputs, total = [], 0
    for message in messages:
        total += len(message.value or b'') + len(message.key or b'')
        if len(inputs) >= max_records or total > max_input_bytes:
            raise PipelineError('BRONZE_BATCH_INPUT_LIMIT')
        inputs.append(message)
    # Preserve deterministic first evidence for duplicates within one batch.
    inputs.sort(key=lambda m: (m.topic, m.partition, m.offset))
    pending, quarantine, raw_cache = [], {}, {}
    raw_bytes = 0
    for message in inputs:
        try:
            event = event_from(message)
            key = event['payload']['raw_document_id']
            if key not in raw_cache:
                doc = source.get(key)  # Missing/majority-unconfirmed Raw is RETRY, never poison.
                validate_raw(doc, event)
                raw_bytes += len(canonical(doc).encode('utf-8'))
                if raw_bytes > max_raw_bytes:
                    raise PipelineError('BRONZE_BATCH_RAW_LIMIT')
                raw_cache[key] = doc
            doc = raw_cache[key]
            validate_raw(doc, event)
            pending.append((message, raw_row(doc), event_row(event, message)))
        except Poison as exc:
            row = quarantine_row(message, exc.code)
            quarantine[row['quarantine_id']] = row
    existing_raw = sink.existing('raw', {r['raw_document_id'] for _, r, _ in pending})
    existing_events = sink.existing('events', {e['event_id'] for _, _, e in pending})
    observations = sink.observations({r['observation_key'] for _, r, _ in pending})
    new_raw, new_events, accepted = {}, {}, 0
    for message, raw, event in pending:
        rid, eid = raw['raw_document_id'], event['event_id']
        prior_raw = existing_raw.get(rid) or new_raw.get(rid)
        prior_event = existing_events.get(eid) or new_events.get(eid)
        prior_observation = observations.get(raw['observation_key'])
        code = None
        if prior_event and (prior_event['event_hash'] != event['event_hash']
                            or prior_event['event_json'] != event['event_json']):
            code = 'BRONZE_EVENT_ID_CONFLICT'
        elif prior_raw and (prior_raw['document_hash'] != raw['document_hash']
                            or prior_raw['raw_envelope_json'] != raw['raw_envelope_json']):
            code = 'BRONZE_RAW_ID_CONFLICT'
        elif prior_observation and prior_observation['raw_document_id'] != rid:
            code = 'BRONZE_OBSERVATION_CONFLICT'
        if code:
            row = quarantine_row(message, code)
            quarantine[row['quarantine_id']] = row
            continue
        accepted += 1
        new_raw.setdefault(rid, prior_raw or raw)
        new_events.setdefault(eid, prior_event or event)
        observations[raw['observation_key']] = raw
    # Independent Delta commits; partial writes are replayable, never a distributed transaction.
    sink.insert('raw', list(new_raw.values()))
    sink.insert('events', list(new_events.values()))
    sink.insert('quarantine', list(quarantine.values()))
    return {'batch_id': batch_id, 'received': len(inputs), 'accepted': accepted,
            'quarantined': len(quarantine), 'raw_candidates': len(new_raw), 'event_candidates': len(new_events)}
