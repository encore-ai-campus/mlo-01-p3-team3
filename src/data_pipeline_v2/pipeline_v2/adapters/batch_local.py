"""Local reference backend for batch contracts and recovery, never a Spark substitute."""
from collections import defaultdict
from ..domain.batch import CODE_VERSION, REJECTED, Retention, kafka_key, outbox_event
from ..domain.contracts import canonical, digest, row, seal, stamp, Invalid
from ..domain.transform import normalize, gold, related_policy, VERSION, RUN
from .sources import reference

# Expiring de-identified intake metadata lives outside immutable stage snapshots.
METADATA_TABLES = {'INPUT_QUARANTINE':'bronze_quarantine','PAYLOAD_OMITTED':'bronze_metadata',
                   'SOURCE_METADATA_ONLY':'bronze_metadata'}


def classify(item, event):
    from product_pipeline.bronze.contracts import validate_raw, Poison
    from product_pipeline.contracts import event_for
    try:
        doc = item['document']
        validate_raw(doc, event_for(doc, RUN, doc['collected_at']))
    except Poison as exc:
        return 'INPUT_QUARANTINE' if event['quarantine_allowed'] else 'PAYLOAD_OMITTED', exc.code
    return 'ACCEPTED', None


def parse_observation(item, policy):
    from product_pipeline.errors import Conflict
    try:
        return dict(quality='VALID', normalized=normalize(item, policy), reason=None)
    except Conflict as exc:
        # Only explicit deterministic source conflicts become row-level holds.
        return dict(quality='QUALITY_HOLD', normalized=None, reason=exc.code)


def intake_metadata(event, disposition, reason, expires_at, raw_store=None):
    # De-identified: event identity, reason code and receive position only; never payload values.
    return dict(event_id=event['event_id'], disposition=disposition, reason=reason,
        received_position=event.get('received_position'), raw_store=raw_store,
        source_ref=event['source_ref'] if raw_store else None, metadata_expires_at=expires_at)


def withdrawal(product, retracted):
    source, identifier = product.split(':', 1)
    return dict(product_key=product, source_code=source, source_product_id=identifier, withdrawn=True,
        retracted_raw_document_ids=sorted(retracted), catalog_ready=False, recommendation_ready=False,
        outcome='SOURCE_RETRACTED', policy_version=VERSION)


def pinned_inputs(store, key):
    """Fix, before any output exists, the earlier-batch history and retractions a job reads.

    A retry after a lost stage manifest must see exactly what the first attempt saw, even if
    later batches have completed Bronze or new retractions were registered in between.
    """
    saved = store.get('input_pin', key)
    if saved:
        return saved
    jobs = sorted(v['job_id'] for v in store.values('stage')
                  if v.get('stage') == 'bronze' and v.get('status') == 'COMPLETE' and v['job_id'] != key)
    retractions = sorted({(v['product_key'], v['raw_document_id']) for v in store.values('retraction')})
    return store.put('input_pin', key, dict(job_id=key, bronze_jobs=jobs, retractions=[list(v) for v in retractions]))


def product_retractions(pin, products):
    found = defaultdict(set)
    for product, raw in pin['retractions']:
        if product in products:
            found[product].add(raw)
    return found


class LocalBatchProcessor:
    def __init__(self, store, broker, lake, timing, limits, policy, retention=None):
        self.store, self.broker, self.lake, self.timing = store, broker, lake, timing
        self.limits, self.policy = limits, policy
        self.retention = retention or Retention()
        self.metrics = dict(source_reads=0,normalized=0,reused=0)

    def _stage(self, job, name, produce, parent=None):
        saved = self.store.get('stage', [job['job_id'], name])
        if saved:
            return saved
        with self.timing.measure(name + '.total', job['crawl_batch_id']):
            started = stamp()
            result = produce()
            for proof in result.get('proofs', []):
                self.lake.verify(proof)
            result = seal(dict(result, stage=name, status='COMPLETE', job_id=job['job_id'], parent=parent,
                attempts=job['attempts'], started_at=started, completed_at=stamp(), code_version=CODE_VERSION))
            self.store.put('stage', [job['job_id'], name], result)
        return result

    def _bronze(self, job, manifest, confirmed_indices):
        import json
        from ..domain.batch import validate_event
        batch, key = job['crawl_batch_id'], job['job_id']
        expected = {v['event_id']:v for v in manifest['inventory']}
        received = {}
        with self.timing.measure('kafka.offset_read_and_validate', batch) as counts:
            physical = 0
            for record in self.broker.rows(manifest):
                physical += 1
                event = json.loads(record['value'])
                if event.get('crawl_batch_id') != batch:
                    continue
                validate_event(event)
                event = outbox_event(event)
                if record['key'] != kafka_key(event):
                    raise Invalid('KAFKA_KEY_MISMATCH')
                identity = event['event_id']
                if identity in received and received[identity] != event:
                    raise Invalid('EVENT_ID_CONFLICT')
                received[identity] = event
            actual = {k:dict(event_id=k,payload_hash=v['payload_hash'],product_key=v['product_key']) for k,v in received.items()}
            if actual != expected:
                raise Invalid('KAFKA_EVENT_SET_MISMATCH')
            counts.update(physical_records=physical,unique_events=len(received),duplicates=physical-len(received))
        crawl = self.store.get('crawl', batch)['crawl']
        expires = self.retention.expiry(crawl['completed_at'], 'metadata')
        rows, metadata, changed, watermark = [], defaultdict(list), set(), None
        tally = defaultdict(int)
        # One keyed bulk read for every new admitted event instead of a round trip per event.
        fresh = sorted({e['source_ref'] for e in received.values()
                        if e['event_type'] != REJECTED and e['event_id'] not in confirmed_indices})
        with self.timing.measure('bronze.source_read', batch, counts={'items':len(fresh)}):
            items = self.store.get_many('source', fresh) if fresh else {}
            self.metrics['source_reads'] += len(fresh)
        for event in received.values():
            eid = event['event_id']
            if event['event_type'] == REJECTED:
                disposition = event['disposition']
                raw_store = None
                if disposition == 'INPUT_QUARANTINE':
                    saved = self.store.get('quarantine_raw', eid)
                    if saved is None or digest(saved['payload']) != event['payload_hash']:
                        raise Invalid('QUARANTINE_RAW_UNAVAILABLE')
                    raw_store = 'quarantine_raw'
                metadata[METADATA_TABLES[disposition]].append(row(eid,'',intake_metadata(event,disposition,event['reason'],expires,raw_store)))
                tally[disposition] += 1
                continue
            history = confirmed_indices.get(eid)
            if history:
                if history['event']['payload_hash'] != event['payload_hash']:
                    raise Invalid('SOURCE_HISTORY_CONFLICT')
                changed.add(event['product_key'])
                tally['ACCEPTED_EXISTING'] += 1
                continue
            with self.timing.measure('bronze.classify', batch):
                item = items.get(event['source_ref'])
                if item is None or digest(item) != event['payload_hash']:
                    raise Invalid('IMMUTABLE_SOURCE_UNAVAILABLE')
                disposition, reason = classify(item, event)
            tally[disposition] += 1
            safe_event = {k:v for k,v in event.items() if k != 'crawl_batch_id'}
            if disposition == 'ACCEPTED':
                index = dict(event=safe_event, dependencies=reference(item), origin_job=key,
                    image_revision=item['image_state']['image_state_revision'])
                self.store.put('history_index', [key,eid], index)
                rows.append(row(eid,event['product_key'],index))
                changed.add(event['product_key'])
                watermark = max(filter(None,[watermark,event['collected_at']]))
            else:
                # Raw already lives in the normal immutable source snapshot; only a reference is kept.
                metadata[METADATA_TABLES[disposition]].append(row(eid,event['product_key'],
                    intake_metadata(event,disposition,reason,expires,'source' if disposition == 'INPUT_QUARANTINE' else None)))
        proofs = []
        if rows:
            with self.timing.measure('bronze.batch_bronze.commit', batch, counts={'rows':len(rows)}):
                proofs.append(self.lake.commit('batch_bronze', rows))
        outputs = {}
        for table, values in sorted(metadata.items()):
            # One write per non-empty destination; an empty quarantine is never committed.
            with self.timing.measure('bronze.' + table + '.commit', batch, counts={'rows':len(values)}):
                self.lake.commit(table, values)
            outputs[table] = dict(rows={r['row_key']:r['record_hash'] for r in values},expires_at=expires)
        return dict(proofs=proofs,metadata_outputs=outputs,changed_products=sorted(changed),
            input=dict(delivery_hash=manifest['content_hash'],event_set_hash=manifest['event_set_hash'],
                generation=manifest['generation']),source_version_watermark=watermark,
            counts=dict(physical_records=counts['physical_records'],unique_events=len(received),
                accepted=len(rows),**{k.lower():v for k,v in tally.items() if k != 'ACCEPTED'}))

    def process(self, job, manifest):
        key = job['job_id']
        pin = pinned_inputs(self.store, key)
        confirmed = set(pin['bronze_jobs']) | {key}
        confirmed_indices = {v['event']['event_id']:v for v in self.store.values('history_index') if v['origin_job'] in confirmed}
        if job['kind'] == 'REPAIR':
            b = dict(changed_products=job['products'],content_hash=None)
        else:
            b = self._stage(job, 'bronze', lambda: self._bronze(job, manifest, confirmed_indices))
        batch = job['crawl_batch_id']

        def silver():
            wanted = set(b['changed_products'])
            retracted = product_retractions(pin, wanted)
            histories, seen = defaultdict(list), set()
            for index in self.store.values('history_index'):
                if index['event']['product_key'] in wanted and index['origin_job'] in confirmed:
                    seen.add(index['event']['product_key'])
                    if index['event']['raw_document_id'] not in retracted[index['event']['product_key']]:
                        histories[index['event']['product_key']].append(index)
            for product, values in histories.items():
                histories[product] = list({v['event']['event_id']:v for v in values}.values())
            normalized, proofs, policies, holds, limited = {}, [], {}, [], {}
            # Reuse is decided from fixed identities first; only misses fetch a source body, in bulk.
            plan = []
            for product, indices in histories.items():
                policy = related_policy(self.policy, [v['dependencies'] for v in indices])
                policies[product] = policy
                normalized[product] = []
                normal_policy = {k:policy[k] for k in ('mappings','rules')}
                for index in sorted(indices,key=lambda v:(v['event']['source_version'],v['image_revision'])):
                    identity = digest(dict(event=index['event']['event_id'],policy=normal_policy,code=VERSION))
                    plan.append((product, index, identity, policy, normal_policy))
            cached = self.store.get_many('normalized', sorted({p[2] for p in plan})) if plan else {}
            missing = sorted({p[1]['event']['source_ref'] for p in plan if not cached.get(p[2])})
            with self.timing.measure('silver.source_read', batch, counts={'items':len(missing)}):
                items = self.store.get_many('source', missing) if missing else {}
                self.metrics['source_reads'] += len(missing)
            produced = []
            for product, index, identity, policy, normal_policy in plan:
                event = index['event']
                result = cached.get(identity)
                if result:
                    self.metrics['reused'] += 1
                else:
                    item = items.get(event['source_ref'])
                    if item is None or digest(item) != event['payload_hash']:
                        raise Invalid('IMMUTABLE_SOURCE_UNAVAILABLE')
                    with self.timing.measure('silver.parse_and_validate', batch):
                        result = parse_observation(item, policy)
                    cached[identity] = result
                    produced.append((identity, result))
                    self.metrics['normalized'] += 1
                if result['quality'] == 'VALID':
                    normalized[product].append((index['image_revision'],result['normalized']))
                else:
                    # Bronze reference only; the raw payload is not copied into Silver.
                    holds.append(row(digest(dict(job=key,event=event['event_id'])),product,
                        dict(event_id=event['event_id'],raw_document_id=event['raw_document_id'],
                            reason=result['reason'],policy_hash=digest(normal_policy))))
            if produced:
                with self.timing.measure('silver.normalized_store', batch, counts={'items':len(produced)}):
                    self.store.put_many('normalized', produced)
            for product in list(normalized):
                # An image-state update replaces that raw observation, not unrelated history.
                selected = {}
                for revision, value in normalized[product]:
                    raw_id = value['raw_ref']['_id']
                    if raw_id not in selected or revision > selected[raw_id][0]:
                        selected[raw_id] = (revision,value)
                normalized[product] = [v[1] for _,v in sorted(selected.items())]
                size = len(canonical(normalized[product]).encode())
                if len(normalized[product]) > self.limits.history_observations or size > self.limits.history_bytes:
                    # One oversized history holds that product only; the batch and other products go on.
                    limited[product] = dict(reason='PRODUCT_HISTORY_LIMIT',observations=len(normalized.pop(product)),bytes=size)
                    holds.append(row(digest(dict(job=key,product=product,hold='PRODUCT_HISTORY_LIMIT')),product,limited[product]))
            values = [row(digest(dict(job=key,product=p)),p,dict(history=h,policy=policies[p],
                retracted=sorted(retracted[p]),withdrawn=False)) for p,h in normalized.items() if h]
            # A product whose every retained observation was explicitly retracted is withdrawn.
            values += [row(digest(dict(job=key,product=p)),p,dict(history=[],policy={},retracted=sorted(retracted[p]),
                withdrawn=True)) for p in sorted(seen - set(histories)) if retracted[p]]
            if values:
                with self.timing.measure('silver.table_commit', batch, counts={'products':len(values)}):
                    proofs.append(self.lake.commit('batch_silver',values))
            if holds:
                with self.timing.measure('silver.quality_hold_commit', batch, counts={'rows':len(holds)}):
                    proofs.append(self.lake.commit('silver_quality_holds',holds))
            return dict(proofs=proofs,keys=[r['row_key'] for r in values],
                counts=dict(products=len(values),valid=sum(len(v['payload']['history']) for v in values),
                    quality_hold=len(holds),history_limit=len(limited),withdrawn=sum(v['payload']['withdrawn'] for v in values)))
        s = self._stage(job, 'silver', silver, parent=b['content_hash'])

        def gold_stage():
            groups = self.lake.read_keys('batch_silver',s['keys'])
            values = []
            with self.timing.measure('gold.compute_current',batch):
                for value in groups.values():
                    product, payload = value['product_key'], value['payload']
                    if payload['withdrawn']:
                        result = withdrawal(product, payload['retracted'])
                    else:
                        result = dict(gold(product,payload['history'],payload['policy']),withdrawn=False,
                            retracted_raw_document_ids=payload['retracted'])
                    values.append(row(digest(dict(job=key,product=product)),product,result))
            proofs = []
            if values:
                with self.timing.measure('gold.table_commit',batch,counts={'products':len(values)}):
                    proofs.append(self.lake.commit('batch_gold',values))
            outcomes = defaultdict(int)
            for value in values:
                outcomes[value['payload']['outcome']] += 1
            return dict(proofs=proofs,keys=[r['row_key'] for r in values],gold_version=digest(proofs),
                input_hash=job['event_set_hash'],rule_version=VERSION,
                counts=dict(products=len(values),**{k.lower():v for k,v in outcomes.items()}))
        return self._stage(job, 'gold', gold_stage, parent=s['content_hash'])

    def products(self, gold_stage):
        for proof in gold_stage['proofs']:
            # Required validation on the publication boundary; status never does a full audit.
            self.lake.verify(proof)
        rows = self.lake.read_keys('batch_gold',gold_stage['keys'])
        chunk, amount = [], 0
        for value in rows.values():
            product = value['payload']
            size = len(canonical(product).encode())
            if size > self.limits.publication_bytes:
                raise Invalid('PUBLICATION_PRODUCT_BYTE_LIMIT')
            if chunk and (len(chunk) >= self.limits.publication_products or amount+size > self.limits.publication_bytes):
                yield chunk
                chunk, amount = [], 0
            chunk.append(product)
            amount += size
        if chunk:
            yield chunk

    def audit(self, stage):
        for proof in stage['proofs']:
            self.lake.verify(proof)
        now = stamp()
        for table, output in (stage.get('metadata_outputs') or {}).items():
            # Expired metadata is legitimately purged; unexpired metadata must still match.
            if output['expires_at'] <= now:
                continue
            actual = self.lake.read_keys(table, list(output['rows']))
            if {k:v['record_hash'] for k,v in actual.items()} != output['rows']:
                raise Invalid('QUARANTINE_METADATA_CHANGED')

    def purge_quarantine_metadata(self, now):
        return sum(self.lake.purge(table, lambda payload: payload['metadata_expires_at'] <= now)
                   for table in sorted(set(METADATA_TABLES.values())))

    def close(self):
        self.lake.close()
