"""Crawl completion reconciliation and finite at-least-once Kafka delivery."""
from ..domain.batch import (source_contract, event_for_item, rejected_event, outside_allowlist, inventory_hash,
                            delivery_contract, kafka_key, wire_event, stable_item, BATCH_SCHEMA, REJECTED, Retention)
from ..domain.contracts import digest, canonical, seal, verified, stamp, Invalid


class Outbox:
    def __init__(self, store, producer, timing, limits, admission, retention=None, wire_payload=True,
                 early_relay=True):
        import threading
        self.store, self.producer, self.timing = store, producer, timing
        # True: each chunk goes to Kafka as soon as its source and outbox rows are durable, during
        # prepare; relay() then finds the acks and only writes the delivery manifest.
        self.early_relay = early_relay
        self._sending, self._inflight = threading.Lock(), []
        self.limits, self.admission = limits, admission
        self.retention = retention or Retention()
        # False: Kafka carries only the immutable source reference (consumer reads the ledger itself).
        self.wire_payload = wire_payload
        # Admitted payloads of the batch prepared last in this process, keyed by source_ref; their
        # hashes were computed from these objects, so a relay in the same process sends them as is.
        self.prepared = (None, {})

    def _admit(self, item, position, batch):
        """Row-level admission: one bad row never blocks the other rows of a complete crawl."""
        quarantine = self.admission['quarantine_allowed'] is True
        try:
            amount = len(canonical(item).encode())
        except (TypeError, ValueError):
            return rejected_event(batch, position, 'SOURCE_PAYLOAD_NOT_JSON', 'PAYLOAD_OMITTED'), None, 0
        if outside_allowlist(item, self.admission.get('field_allowlist')):
            # Not allowed in any persistent store: no payload, no payload hash, no field values.
            return rejected_event(batch, position, 'SOURCE_FIELD_NOT_ALLOWED', 'SOURCE_METADATA_ONLY'), None, amount
        if amount > self.limits.max_event_bytes:
            return rejected_event(batch, position, 'SOURCE_EVENT_BYTE_LIMIT', 'PAYLOAD_OMITTED'), None, amount
        try:
            return event_for_item(item, batch, **self.admission), item, amount
        except Invalid as exc:
            if str(exc) == 'SOURCE_STORAGE_NOT_ALLOWED':
                raise
            disposition = 'INPUT_QUARANTINE' if quarantine else 'PAYLOAD_OMITTED'
            return rejected_event(batch, position, str(exc), disposition,
                digest(item) if quarantine else None), item if quarantine else None, amount

    def _quarantine(self, crawl, event, item):
        # Restricted store, separate from normal source snapshots, with a deterministic expiry.
        record = dict(event_id=event['event_id'], crawl_batch_id=crawl['crawl_batch_id'], payload=item,
            payload_hash=event['payload_hash'], reason=event['reason'], received_position=event['received_position'],
            raw_expires_at=self.retention.expiry(crawl['completed_at'], 'raw'))
        self.store.put('quarantine_raw', event['event_id'], record)
        if self.store.get('quarantine_raw', event['event_id']) != record:
            raise Invalid('QUARANTINE_RECONCILIATION_FAILED')

    def _persist(self, crawl, pending):
        """Write one bounded chunk: source snapshots first, outbox second.

        Re-running reconciles a crash between the two. Every write is an immutable keyed insert
        acknowledged by a majority with journal, and an existing key must already hold the same
        value (`put_many`), so the chunk is not read back. SOURCE_COMPLETE is written last.
        """
        batch = crawl['crawl_batch_id']
        for event, payload in pending:
            if event['event_type'] == REJECTED and payload is not None:
                self._quarantine(crawl, event, payload)
        admitted = [(e, p) for e, p in pending if e['event_type'] != REJECTED]
        if admitted:
            self.store.put_many('source', [(e['source_ref'], p) for e, p in admitted])
            self.store.put_many('raw_identity', [(e['raw_document_id'], dict(document_hash=p['document']['document_hash']))
                for e, p in admitted])
            self.store.put_many('observation_identity', [([e['product_key'], e['source_version']],
                dict(raw_document_id=e['raw_document_id'], document_hash=p['document']['document_hash'])) for e, p in admitted])
            self.store.put_many('source_identity', [(e['event_id'], dict(payload_hash=e['payload_hash'])) for e, _ in admitted])
            self.prepared[1].update((e['source_ref'], p) for e, p in admitted)
        self.store.put_many('outbox', [([batch, e['event_id']], e) for e, _ in pending])

    def _send_early(self, batch, pending):
        """Hand a chunk whose source and outbox rows are durable to Kafka, without waiting.

        This is relay()'s own send, earlier: the same key and wire event, and an ack only for a
        broker offset. Chunks stay in flight together; `_confirm_early` waits once at the end of
        prepare. Nothing is final before the delivery manifest, so a failure only leaves acks
        missing and relay() sends those events as before; a crawl that later fails leaves messages
        outside any manifest range, which no batch reads.
        """
        try:
            # As in relay(): quarantined raw stays in its restricted store and is never sent.
            pairs = [(kafka_key(e), wire_event(e, None if e['event_type'] == REJECTED or not self.wire_payload else p))
                for e, p in pending]
            with self._sending:
                begin = getattr(self.producer, 'begin_many', None)
                if begin is None:
                    acks = self.producer.send_many(pairs)
                    handle = type('Sent', (), {'acks': lambda _: acks})()
                else:
                    handle = begin(pairs)
                self._inflight.append((handle, pending))
        except Exception:
            pass

    def _confirm_early(self, batch):
        inflight, self._inflight = self._inflight, []
        if not inflight:
            return
        with self.timing.measure('kafka.publish_ack', batch, counts={'messages': sum(len(p) for _, p in inflight)}):
            for handle, pending in inflight:
                try:
                    acks = handle.acks()
                    self.store.put_many('ack', [([batch, 0, e['event_id']], ack) for (e, _), ack in zip(pending, acks)
                        if ack is not None])
                except Exception:
                    pass

    def prepare(self, crawl, items):
        source_contract(crawl)
        batch = crawl['crawl_batch_id']
        self.prepared, self._inflight = (batch, {}), []
        inventory, total_bytes, records, duplicates = {}, 0, 0, 0
        dispositions = {}
        pending, pending_bytes = [], 0
        # Chunks have disjoint keys, so up to three are written at once (each waits for its own
        # majority acknowledgement); SOURCE_COMPLETE is written only after every chunk finished.
        import contextvars
        from concurrent.futures import ThreadPoolExecutor
        pool, writes = ThreadPoolExecutor(max_workers=3), []

        def persist(chunk):
            def write():
                self._persist(crawl, chunk)
                if self.early_relay:
                    self._send_early(batch, chunk)
            writes.append(pool.submit(contextvars.copy_context().run, write))
        with self.timing.measure('outbox.reconcile_source', batch) as counts, pool:
            for position, original in enumerate(items):
                item = stable_item(original)
                records += 1
                if records > self.limits.max_events:
                    raise Invalid('CRAWL_EVENT_LIMIT')
                event, payload, amount = self._admit(item, position, batch)
                if event['event_type'] == REJECTED:
                    # Held raw keeps its received form; only admitted events drop diagnostic fields.
                    event, payload, amount = self._admit(original, position, batch)
                    item = original
                total_bytes += amount
                if total_bytes > self.limits.max_input_bytes:
                    raise Invalid('CRAWL_INPUT_BYTE_LIMIT')
                key = event['event_id']
                if key in inventory:
                    if inventory[key]['payload_hash'] == event['payload_hash']:
                        # Identical retransmission from the crawler collapses into one event.
                        duplicates += 1
                        continue
                    event = rejected_event(batch, position, 'EVENT_ID_PAYLOAD_CONFLICT',
                        'INPUT_QUARANTINE' if self.admission['quarantine_allowed'] else 'PAYLOAD_OMITTED',
                        digest(item) if self.admission['quarantine_allowed'] else None)
                    payload = item if self.admission['quarantine_allowed'] else None
                    key = event['event_id']
                if event['event_type'] == REJECTED:
                    disposition = event['disposition']
                else:
                    payload = item
                    disposition = 'ACCEPTED_FOR_BRONZE'
                pending.append((event, payload))
                pending_bytes += amount
                dispositions[disposition] = dispositions.get(disposition, 0) + 1
                inventory[key] = dict(event_id=key, payload_hash=event['payload_hash'], product_key=event['product_key'])
                if len(pending) >= self.limits.outbox_chunk or pending_bytes >= self.limits.outbox_chunk_bytes:
                    persist(pending)
                    pending, pending_bytes = [], 0
            if pending:
                persist(pending)
            for write in writes:
                write.result()
            if self.early_relay:
                self._confirm_early(batch)
            if records != crawl['event_count']:
                raise Invalid('CRAWL_EVENT_COUNT_MISMATCH')
            source = seal(dict(schema_version=BATCH_SCHEMA, crawl=crawl, crawl_batch_id=batch,
                status='SOURCE_COMPLETE', inventory=sorted(inventory.values(), key=lambda v:v['event_id']),
                event_count=len(inventory), event_set_hash=inventory_hash(list(inventory.values())),
                input_records=records, duplicate_records=duplicates, intake=dispositions,
                source_bytes=total_bytes))
            self.store.put('crawl', batch, source)
            counts.update(events=len(inventory), records=records, duplicates=duplicates, source_bytes=total_bytes)
        return source

    def relay(self, batch, generation=0):
        if type(generation) is not int or generation < 0:
            raise Invalid('DELIVERY_GENERATION_INVALID')
        saved = self.store.get('delivery', [batch, generation])
        if saved:
            return delivery_contract(saved)
        source = self.store.get('crawl', batch)
        if not source:
            raise Invalid('SOURCE_COMPLETE_REQUIRED')
        verified(source)
        inventory = source['inventory']
        ranges = {}
        with self.timing.measure('kafka.relay_total', batch) as counts:
            sent = 0
            for begin in range(0, len(inventory), self.limits.outbox_chunk):
                group = inventory[begin:begin + self.limits.outbox_chunk]
                events = self.store.get_list('outbox', [[batch, v['event_id']] for v in group])
                for expected, event in zip(group, events):
                    if not event or event['payload_hash'] != expected['payload_hash']:
                        raise Invalid('OUTBOX_SOURCE_MISSING_OR_CHANGED')
                kept = self.prepared[1] if self.prepared[0] == batch else {}
                sources = dict(kept)
                wanted = [e['source_ref'] for e in events if e['event_type'] != REJECTED and e['source_ref'] not in kept]
                if wanted:
                    sources.update(self.store.get_many('source', wanted))
                held = self.store.get_many('quarantine_raw', [e['event_id'] for e in events
                    if e['event_type'] == REJECTED and e['source_ref'] is not None])
                payloads = []
                for expected, event in zip(group, events):
                    if event['event_type'] == REJECTED:
                        record = held.get(event['event_id'])
                        stored = record and record['payload']
                    else:
                        stored = sources[event['source_ref']]
                    fresh = event['event_type'] == REJECTED or event['source_ref'] not in kept
                    if fresh and expected['payload_hash'] is not None and digest(stored) != expected['payload_hash']:
                        raise Invalid('OUTBOX_SOURCE_MISSING_OR_CHANGED')
                    # Quarantined raw stays in its restricted store; only admitted payloads are sent.
                    payloads.append(None if event['event_type'] == REJECTED else stored)
                acks = self.store.get_list('ack', [[batch, generation, v['event_id']] for v in group])
                missing = [i for i, ack in enumerate(acks) if not ack]
                if missing:
                    with self.timing.measure('kafka.publish_ack', batch, counts={'messages': len(missing)}):
                        sent_acks = self.producer.send_many([(kafka_key(events[i]),
                            wire_event(events[i], payloads[i] if self.wire_payload else None))
                            for i in missing])
                    sent += len(missing)
                    # Ack loss before this write intentionally leads to identical event retransmission.
                    self.store.put_many('ack', [([batch, generation, group[i]['event_id']], ack)
                        for i, ack in zip(missing, sent_acks) if ack is not None])
                    if any(ack is None for ack in sent_acks):
                        raise Invalid(getattr(self.producer, 'failure_code', None) or 'KAFKA_DELIVERY_UNCONFIRMED')
                    for i, ack in zip(missing, sent_acks):
                        acks[i] = ack
                for ack in acks:
                    position = (ack['topic'], ack['partition'])
                    value = ranges.setdefault(position, dict(topic=position[0],partition=position[1],
                        start_offset=ack['offset'],end_offset=ack['offset'] + 1))
                    value['start_offset'] = min(value['start_offset'], ack['offset'])
                    value['end_offset'] = max(value['end_offset'], ack['offset'] + 1)
            for value in ranges.values():
                # Physical span may include retransmissions and other interleaved batches.
                value['physical_records'] = value['end_offset'] - value['start_offset']
            with self.timing.measure('kafka.delivery_manifest_commit', batch):
                delivery = seal(dict(schema_version=BATCH_SCHEMA, crawl_batch_id=batch, status='COMPLETE',
                    generation=generation, source_manifest_hash=source['content_hash'],
                    event_count=source['event_count'], event_set_hash=source['event_set_hash'],
                    inventory=inventory, ranges=sorted(ranges.values(),key=lambda v:(v['topic'],v['partition'])),
                    physical_records=sum(v['physical_records'] for v in ranges.values()),
                    source_bytes=source['source_bytes'], completed_at=stamp()))
                delivery_contract(delivery)
                self.store.put('delivery', [batch, generation], delivery)
            counts.update(events=len(inventory), sent=sent, offset_span=delivery['physical_records'])
        return delivery
