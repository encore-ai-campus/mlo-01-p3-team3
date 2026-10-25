"""Finite distributed source resolution, cached parsing, native Gold and bounded publication."""
from contextlib import contextmanager
from itertools import islice
import json

from ..domain.contracts import canonical, digest, seal, stamp, Invalid
from ..domain.batch import CODE_VERSION, REJECTED, Retention, validate_event
from ..domain.transform import related_policy, VERSION
from .batch_local import classify, parse_observation, intake_metadata, pinned_inputs, METADATA_TABLES
from .sources import reference


def open_store(config):
    from .batch_store import FileBatchStore, MongoBatchStore
    if config['mode'] == 'local':
        return FileBatchStore(config['state_root'])
    import os
    return MongoBatchStore(os.environ[config['uri_env']],config['database'],config['replica_set'])


def resolve_partition(rows, store_config, page_size, normalize=False, expires=None):
    # Called in an Executor. Admitted payloads arrive with the Kafka event or the Bronze index,
    # so the normal path needs no source DB; the bounded store lookup is only a fallback.
    store = None
    try:
        iterator = iter(rows)
        while True:
            page = list(islice(iterator,page_size))
            if not page:
                break
            events = [json.loads(row['event_json']) for row in page]
            carried = [json.loads(row['item_json']) if row['item_json'] else events[i].get('payload')
                       for i,row in enumerate(page)]
            missing = [e['source_ref'] for e,c in zip(events,carried) if e['event_type'] != REJECTED and c is None]
            items = {}
            if missing:
                if store is None:
                    store = open_store(store_config)
                items = store.get_many('source',list(set(missing)))
            for record, event, item in zip(page,events,carried):
                if not normalize:
                    # The Kafka wire body, validated here instead of in a separate UDF stage.
                    validate_event(event)
                event = {k:v for k,v in event.items() if k != 'payload'}
                if event['event_type'] == REJECTED:
                    # Rejected rows carry no trusted product identity and never reach Silver.
                    # Their restricted raw copy was verified by the relay; Executors never read it.
                    disposition = event['disposition']
                    raw_store = 'quarantine_raw' if disposition == 'INPUT_QUARANTINE' else None
                    body = dict(intake_metadata(event,disposition,event['reason'],expires,raw_store),origin_job=record['origin_job'])
                    yield (event['event_id'],'',canonical(body),disposition)
                    continue
                item = item if item is not None else items.get(event['source_ref'])
                if item is None or digest(item) != event['payload_hash']:
                    raise Invalid('IMMUTABLE_SOURCE_UNAVAILABLE')
                if normalize:
                    policy = json.loads(record['policy_json'])
                    result = parse_observation(item,policy)
                    body = dict(quality=result['quality'],reason=result['reason'],
                        normalized_json=canonical(result['normalized']) if result['normalized'] is not None else None,
                        raw_document_id=event['raw_document_id'],event_id=event['event_id'],
                        image_revision=item['image_state']['image_state_revision'])
                    yield (record['row_key'],event['product_key'],canonical(body))
                else:
                    disposition, reason = classify(item,event)
                    if disposition == 'ACCEPTED':
                        # Bronze keeps the admitted raw payload once, for later Silver reuse misses.
                        body = dict(event={k:v for k,v in event.items() if k != 'crawl_batch_id'},
                            dependencies=reference(item),image_revision=item['image_state']['image_state_revision'],
                            origin_job=record['origin_job'],item=item)
                        yield (record['row_key'],event['product_key'],canonical(body),disposition)
                    else:
                        # The raw stays in the normal immutable source snapshot; only a reference is kept.
                        body = dict(intake_metadata(event,disposition,reason,expires,
                            'source' if disposition == 'INPUT_QUARANTINE' else None),origin_job=record['origin_job'])
                        yield (event['event_id'],event['product_key'],canonical(body),disposition)
    finally:
        if store is not None:
            store.close()


class SparkBatchProcessor:
    def __init__(self, store, server_lake, store_config, kafka_config, timing, limits, policy, retention=None):
        self.store, self.server_lake, self.store_config = store, server_lake, store_config
        self.kafka_config, self.timing, self.limits, self.policy = kafka_config, timing, limits, policy
        self.retention = retention or Retention()
        self.spark = self.delta = None
        self.read_kafka = None

    def _open(self):
        if self.spark is None:
            from .batch_delta import BatchDelta
            self.server_lake._open()
            self.spark = self.server_lake.spark
            self.delta = BatchDelta(self.spark,self.server_lake.config['lake_root'],self.server_lake.check,self.timing)

    @contextmanager
    def action(self,name,batch):
        self._open()
        with self.timing.measure(name,batch):
            context = self.spark.sparkContext
            previous = {k:context.getLocalProperty(k) for k in ('spark.jobGroup.id','pipeline.v2.scope_hash','pipeline.v2.stage')}
            context.setLocalProperty('spark.jobGroup.id',self.timing.active_span_id)
            context.setLocalProperty('pipeline.v2.scope_hash',digest(batch))
            context.setLocalProperty('pipeline.v2.stage',name)
            try:
                yield
            finally:
                for name,value in previous.items():
                    context.setLocalProperty(name,value)

    @staticmethod
    def validate_range(decoded, batch, expected):
        """All Kafka range checks in one aggregate: each check was a separate job before.

        Rows of other batches only feed the unclassifiable flag. Per event: distinct wire bodies
        (conflict) and its hash/product, then a full join with the manifest inventory (unique ids)
        counts events missing on either side or carrying a different hash/product.
        """
        from pyspark.sql import functions as F
        mine = F.col('e.crawl_batch_id') == batch
        events = decoded.groupBy(F.when(mine,F.col('e.event_id')).alias('_event_id')).agg(
            F.max(F.col('e.crawl_batch_id').isNull().cast('int')).alias('unclassifiable'),
            F.max((mine & ~F.col('key').eqNullSafe(F.coalesce('e.product_key','e.event_id'))).cast('int')).alias('key_mismatch'),
            F.sum(mine.cast('int')).alias('rows'),
            F.countDistinct(F.when(mine,F.col('value'))).alias('bodies'),
            F.countDistinct(F.when(mine,F.struct('e.payload_hash','e.product_key'))).alias('identities'),
            F.first(F.when(mine,F.col('e.payload_hash')),ignorenulls=True).alias('_hash'),
            F.first(F.when(mine,F.col('e.product_key')),ignorenulls=True).alias('_product'))
        joined = events.join(expected,F.col('_event_id') == F.col('_expected_id'),'full_outer')
        present = F.coalesce(F.col('rows'),F.lit(0)) > 0
        mismatch = (present & F.col('_expected_id').isNull()) | (~present & F.col('_expected_id').isNotNull()) | (
            present & F.col('_expected_id').isNotNull() & ((F.col('identities') > 1) |
            ~F.col('_hash').eqNullSafe(F.col('_expected_hash')) | ~F.col('_product').eqNullSafe(F.col('_expected_product'))))
        return joined.agg(F.coalesce(F.max('unclassifiable'),F.lit(0)).alias('unclassifiable'),
            F.coalesce(F.max('key_mismatch'),F.lit(0)).alias('key_mismatch'),
            F.coalesce(F.max((F.col('bodies') > 1).cast('int')),F.lit(0)).alias('conflict'),
            F.coalesce(F.max(mismatch.cast('int')),F.lit(0)).alias('mismatch')).first()

    def _stage(self,job,name,produce,parent=None):
        saved = self.store.get('stage',[job['job_id'],name])
        if saved:
            return saved
        with self.action(name+'.total',job['crawl_batch_id']):
            started = stamp()
            result = seal(dict(produce(),stage=name,status='COMPLETE',job_id=job['job_id'],parent=parent,
                attempts=job['attempts'],started_at=started,completed_at=stamp(),code_version=CODE_VERSION,
                spark_application_id=self.spark.sparkContext.applicationId))
            self.store.put('stage',[job['job_id'],name],result)
            return result

    def process(self,job,manifest):
        from pyspark.sql import functions as F
        from .kafka_batch import read_kafka
        self._open()
        spark, delta, batch, key = self.spark,self.delta,job['crawl_batch_id'],job['job_id']
        pin = pinned_inputs(self.store, key)
        allowed = pin['bronze_jobs'] + [key]
        store_config, page_size = self.store_config, self.limits.source_page_size
        reader = self.read_kafka or read_kafka

        def bronze():
            crawl = self.store.get('crawl',batch)['crawl']
            expires = self.retention.expiry(crawl['completed_at'],'metadata')
            with self.action('kafka.offset_read_and_validate',batch):
                wire = reader(spark,self.kafka_config,manifest,self.limits.spark_partitions).persist()
                try:
                    schema = 'crawl_batch_id string, event_id string, payload_hash string, product_key string'
                    decoded = wire.withColumn('e',F.from_json('value',schema))
                    selected = decoded.filter(F.col('e.crawl_batch_id') == batch)
                    # Do not silently skip undecodable records in a selected physical range.
                    # Other batches may interleave; malformed records cannot be assigned safely.
                    inventory = [(v['event_id'],v['payload_hash'],v['product_key']) for v in manifest['inventory']]
                    if len({v[0] for v in inventory}) != len(inventory):
                        raise Invalid('KAFKA_EVENT_SET_MISMATCH')
                    flags = self.validate_range(decoded,batch,spark.createDataFrame(
                        inventory,'_expected_id string,_expected_hash string,_expected_product string'))
                    # Same precedence as the separate checks this one aggregate replaces.
                    if flags['unclassifiable']:
                        raise Invalid('KAFKA_RANGE_UNCLASSIFIABLE_EVENT')
                    if flags['key_mismatch']:
                        raise Invalid('KAFKA_KEY_MISMATCH')
                    if flags['conflict']:
                        raise Invalid('EVENT_ID_CONFLICT')
                    if flags['mismatch']:
                        raise Invalid('KAFKA_EVENT_SET_MISMATCH')
                    # dropDuplicates resolves top-level names only; lift the nested event id first.
                    # Events are validated inside resolve_partition, not by a separate Python UDF stage.
                    events = selected.withColumn('_event_id',F.col('e.event_id')).dropDuplicates(['_event_id']).select(
                        F.sha2(F.concat(F.lit(key+':'),F.col('e.event_id')),256).alias('row_key'),
                        F.col('value').alias('event_json'),F.lit(None).cast('string').alias('item_json'),F.lit(key).alias('origin_job'))
                    resolved = spark.createDataFrame(events.rdd.mapPartitions(
                        lambda rows:resolve_partition(rows,store_config,page_size,expires=expires)),
                        'row_key string,product_key string,payload_json string,disposition string').persist()
                    try:
                        # One small aggregate decides which destinations need a write at all, and
                        # names the accepted products (the Bronze index is their only stored copy).
                        found = resolved.groupBy('disposition').agg(F.count('*').alias('n'),
                            F.max(F.get_json_object('payload_json','$.event.collected_at')).alias('watermark'),
                            F.sort_array(F.collect_set('product_key')).alias('products'),
                            F.sort_array(F.collect_set(delta.bucket('product_key'))).alias('buckets')).collect()
                        tally = {r['disposition']:(int(r['n']),r['watermark']) for r in found}
                        products, buckets = next(((list(r['products']),list(r['buckets'])) for r in found if r['disposition'] == 'ACCEPTED'),([],[]))
                        accepted = resolved.filter(F.col('disposition') == 'ACCEPTED').drop('disposition')
                        if products:
                            with self.action('bronze.accepted.commit',batch):
                                # One row per event (deduplicated above), so the in-batch key check is not needed.
                                # Keys carry this job's id: on its first attempt none can be stored yet.
                                delta.append_history('input_index',accepted,unique=dict(keys=tally['ACCEPTED'][0],
                                    buckets=buckets,absent=job['attempts'] == 1))
                        outputs = {}
                        for table in sorted(set(METADATA_TABLES.values())):
                            names = [d for d,t in METADATA_TABLES.items() if t == table and tally.get(d,(0,))[0]]
                            if not names:
                                continue
                            frame = resolved.filter(F.col('disposition').isin(names)).drop('disposition')
                            # Expiring metadata table: idempotent keyed append, one write per destination.
                            with self.action('bronze.'+table+'.commit',batch):
                                delta.append_history(table,frame)
                            outputs[table] = dict(delta.digest_rows(frame),expires_at=expires)
                        counts = dict(accepted=tally.get('ACCEPTED',(0,))[0],**{d.lower():v[0] for d,v in tally.items() if d != 'ACCEPTED'})
                        return dict(format=2,proofs=[],products=products,buckets=buckets,metadata_outputs=outputs,counts=counts,
                            input=dict(delivery_hash=manifest['content_hash'],event_set_hash=manifest['event_set_hash'],
                                generation=manifest['generation']),
                            source_version_watermark=tally.get('ACCEPTED',(0,None))[1])
                    finally:
                        resolved.unpersist()
                finally:
                    wire.unpersist()
        if job['kind'] == 'REPAIR':
            b = dict(content_hash=None)
        else:
            b = self._stage(job,'bronze',bronze)

        if (b.get('outputs') or {}).get('accepted') or (self.store.get('stage',[key,'silver']) and not self.store.get('stage',[key,'gold'])):
            # Stage records written before the single product pass cannot be resumed by it.
            raise Invalid('BATCH_STAGE_FORMAT_CHANGED')

        def output():
            from .spark_product import product_pass
            products = job['products'] if job['kind'] == 'REPAIR' else b['products']
            wanted = set(products)
            retractions = {}
            for product, raw in pin['retractions']:
                if product in wanted:
                    retractions.setdefault(product,[]).append(raw)
            context = dict(job_id=key,policy=self.policy,store_config=store_config,retractions=retractions,
                limits=dict(history_observations=self.limits.history_observations,history_bytes=self.limits.history_bytes))
            frame = spark.createDataFrame([(p,) for p in products],'product_key string').persist()
            try:
                # One shuffle by product: its Bronze index entries and its cached normalized rows meet
                # in one Executor function that applies every Silver and Gold rule for that product.
                # Bronze already listed the products' buckets; a repair computes them once here.
                shared = {} if job['kind'] == 'REPAIR' else dict(buckets=b['buckets'])
                index = delta.history('input_index',frame,shared).filter(
                    F.get_json_object('payload_json','$.origin_job').isin(allowed)).select(
                    'product_key',F.lit(0).alias('kind'),'row_key','payload_json')
                cached = delta.history('normalized_history',frame,shared).select(
                    'product_key',F.lit(1).alias('kind'),'row_key','payload_json')
                rows = (index.unionByName(cached).repartition(self.limits.spark_partitions,'product_key')
                    .sortWithinPartitions('product_key','kind','row_key'))
                with self.action('product.silver_gold',batch):
                    out = spark.createDataFrame(rows.rdd.mapPartitions(lambda part:product_pass(part,context)),
                        'kind string,row_key string,product_key string,payload_json string').persist()
                    label = F.coalesce(F.get_json_object('payload_json','$.outcome'),F.get_json_object('payload_json','$.reason'))
                    tally = {(r['kind'],r['label']):int(r['n']) for r in out.groupBy('kind',label.alias('label')).agg(
                        F.count('*').alias('n')).collect()}
                self._release_cache()
                self._cached = (key,out)
                count = lambda kind:sum(n for (k,_),n in tally.items() if k == kind)
                kind = lambda name:out.filter(F.col('kind') == name).drop('kind')
                if count('normalized'):
                    with self.action('silver.normalize_and_commit',batch):
                        # One row per identity by construction; buckets are a subset of the products' buckets.
                        # The pass read every stored row of these products and emits only identities not
                        # among them, and the writer lease admits no other append in between.
                        delta.append_history('normalized_history',kind('normalized'),
                            unique=dict(keys=count('normalized'),buckets=shared.get('buckets') or [],absent=True))
                outputs = {}
                if count('hold'):
                    outputs['holds'] = delta.snapshot(key,'silver_holds',kind('hold'))
                with self.action('gold.commit',batch):
                    proof = delta.snapshot(key,'gold',kind('gold'))
            finally:
                frame.unpersist()
            silver = dict(proofs=list(outputs.values()),outputs=outputs,counts=dict(valid=count('silver'),
                quality_hold=count('hold'),history_limit=tally.get(('hold','PRODUCT_HISTORY_LIMIT'),0),
                products=count('policy'),withdrawn=count('withdrawn')))
            outcomes = {label.lower():n for (k,label),n in tally.items() if k == 'gold'}
            return dict(format=2,proofs=[proof],output=proof,gold_version=digest([proof]),input_hash=job['event_set_hash'],
                rule_version=VERSION,counts=dict(products=proof['count'],**outcomes),silver=silver)
        g = self._stage(job,'gold',output,parent=b['content_hash'])
        if not self.store.get('stage',[key,'silver']):
            # Derived from the Gold record alone, so writing it again after a crash is identical.
            self.store.put('stage',[key,'silver'],seal(dict(g['silver'],stage='silver',status='COMPLETE',job_id=key,
                parent=b['content_hash'],attempts=g['attempts'],started_at=g['started_at'],completed_at=g['completed_at'],
                code_version=g['code_version'],spark_application_id=g['spark_application_id'])))
        return g

    def _release_cache(self):
        cached, self._cached = getattr(self,'_cached',None), None
        if cached:
            cached[1].unpersist()

    def products(self, stage):
        self._open()
        # This is the bounded service boundary, not a Driver normalization/reduction path.
        # Tag the Gold read jobs (snapshot load included; rows run lazily between yields) for the
        # event-log report.
        context = self.spark.sparkContext
        previous = {k:context.getLocalProperty(k) for k in ('pipeline.v2.scope_hash','pipeline.v2.stage')}
        if self.timing.active_scope:
            context.setLocalProperty('pipeline.v2.scope_hash',digest(self.timing.active_scope))
        context.setLocalProperty('pipeline.v2.stage','publish.lake_read')
        cached = getattr(self,'_cached',None)
        try:
            if cached and cached[0] == stage['job_id']:
                # Processed in this session: the Gold rows just committed are still cached.
                from pyspark.sql import functions as F
                frame = cached[1].filter(F.col('kind') == 'gold').select('product_key','payload_json')
            else:
                frame = self.delta.load(stage['output']).select('product_key','payload_json')
            yield from self._chunks(frame)
            if cached:
                self._release_cache()
        finally:
            for name,value in previous.items():
                context.setLocalProperty(name,value)

    def _chunks(self, frame):
        chunk, amount = [], 0
        for value in frame.toLocalIterator(prefetchPartitions=False):
            size = len(value['payload_json'].encode())
            if size > self.limits.publication_bytes:
                raise Invalid('PUBLICATION_PRODUCT_BYTE_LIMIT')
            if chunk and (len(chunk) >= self.limits.publication_products or amount+size > self.limits.publication_bytes):
                yield chunk
                chunk, amount = [], 0
            chunk.append(json.loads(value['payload_json']))
            amount += size
        if chunk:
            yield chunk

    def audit(self,stage):
        self._open()
        for proof in stage['proofs']:
            self.delta.audit(proof)
        now = stamp()
        for table, output in (stage.get('metadata_outputs') or {}).items():
            # Expired quarantine metadata may be purged by retention; unexpired metadata must match.
            if output['expires_at'] > now and self.delta.digest_rows(self.delta.metadata(table,stage['job_id'])) != \
                    {k:output[k] for k in ('count','hash')}:
                raise Invalid('QUARANTINE_METADATA_CHANGED')

    def purge_quarantine_metadata(self, now):
        self._open()
        return sum(self.delta.purge(table,now) for table in sorted(set(METADATA_TABLES.values())))

    def compact(self):
        """Idle-time maintenance of the history tables every batch reads."""
        self._open()
        with self.timing.measure('serve.history_compaction','maintenance'):
            return {name:self.delta.compact(name) for name in ('input_index','normalized_history')}

    def close(self):
        self.server_lake.close()
