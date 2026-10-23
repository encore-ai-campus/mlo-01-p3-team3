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
        from pyspark.sql import functions as F, Window
        from .kafka_batch import read_kafka
        from .spark_gold import build_gold
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
                    flags = decoded.agg(
                        F.max(F.col('e.crawl_batch_id').isNull().cast('int')).alias('unclassifiable'),
                        F.max((F.col('e.crawl_batch_id') == batch).cast('int') *
                              (~F.col('key').eqNullSafe(F.coalesce('e.product_key','e.event_id'))).cast('int')).alias('key_mismatch')).first()
                    if flags['unclassifiable']:
                        raise Invalid('KAFKA_RANGE_UNCLASSIFIABLE_EVENT')
                    if flags['key_mismatch']:
                        raise Invalid('KAFKA_KEY_MISMATCH')
                    if selected.groupBy('e.event_id').agg(F.countDistinct('value').alias('n')).filter('n > 1').limit(1).count():
                        raise Invalid('EVENT_ID_CONFLICT')
                    actual = selected.select('e.event_id','e.payload_hash','e.product_key').dropDuplicates()
                    expected = spark.createDataFrame(
                        [(v['event_id'],v['payload_hash'],v['product_key']) for v in manifest['inventory']],
                        'event_id string,payload_hash string,product_key string')
                    if actual.exceptAll(expected).limit(1).count() or expected.exceptAll(actual).limit(1).count():
                        raise Invalid('KAFKA_EVENT_SET_MISMATCH')

                    @F.udf('boolean')
                    def valid(value):
                        validate_event(json.loads(value))
                        return True
                    # dropDuplicates resolves top-level names only; lift the nested event id first.
                    events = selected.withColumn('_event_id',F.col('e.event_id')).dropDuplicates(['_event_id']).filter(valid('value')).select(
                        F.sha2(F.concat(F.lit(key+':'),F.col('e.event_id')),256).alias('row_key'),
                        F.col('value').alias('event_json'),F.lit(None).cast('string').alias('item_json'),F.lit(key).alias('origin_job'))
                    resolved = spark.createDataFrame(events.rdd.mapPartitions(
                        lambda rows:resolve_partition(rows,store_config,page_size,expires=expires)),
                        'row_key string,product_key string,payload_json string,disposition string').persist()
                    try:
                        # One small aggregate decides which destinations need a write at all.
                        tally = {r['disposition']:(int(r['n']),r['watermark']) for r in resolved.groupBy('disposition').agg(
                            F.count('*').alias('n'),F.max(F.get_json_object('payload_json','$.event.collected_at')).alias('watermark')).collect()}
                        accepted = resolved.filter(F.col('disposition') == 'ACCEPTED').drop('disposition')
                        with self.action('bronze.accepted.commit',batch):
                            proof = delta.snapshot(key,'bronze_accepted',accepted)
                        delta.append_history('input_index',accepted)
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
                        counts = dict(accepted=proof['count'],**{d.lower():v[0] for d,v in tally.items() if d != 'ACCEPTED'})
                        return dict(proofs=[proof],outputs=dict(accepted=proof),metadata_outputs=outputs,counts=counts,
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

        def silver():
            if job['kind'] == 'REPAIR':
                products = spark.createDataFrame([(p,) for p in job['products']],'product_key string')
            else:
                products = delta.load(b['outputs']['accepted']).select('product_key').distinct()
            # Reused by the history lookups below; computed once instead of once per use.
            products = products.persist()
            # The retraction register is a small control set; it is joined, never looped per product.
            retractions = [tuple(v) for v in pin['retractions']]
            retracted = spark.createDataFrame(retractions,'product_key string,raw_document_id string').join(products,'product_key','left_semi')
            index = delta.history('input_index',products).filter(
                F.get_json_object('payload_json','$.origin_job').isin(allowed))
            # get_json_object retains nested dependency JSON without schema inference or driver data reads.
            index_base = index = index.select('product_key',
                F.get_json_object('payload_json','$.event').alias('event_json'),
                F.get_json_object('payload_json','$.event.raw_document_id').alias('raw_document_id'),
                F.get_json_object('payload_json','$.dependencies').alias('dependencies_json'),
                F.get_json_object('payload_json','$.image_revision').cast('long').alias('image_revision'),
                F.get_json_object('payload_json','$.item').alias('item_json')).dropDuplicates(['product_key','event_json']).persist()
            seen = index.select('product_key').distinct()
            if retractions:
                index = index.join(retracted,['product_key','raw_document_id'],'left_anti')
            counts = index.groupBy('product_key').agg(F.count('*').alias('n'),
                F.sum(F.length('dependencies_json')).alias('bytes'))
            # An oversized history holds that product only; the batch and other products go on.
            over = counts.filter((F.col('n') > self.limits.history_observations) |
                                 (F.col('bytes') > self.limits.history_bytes)).persist()
            index = index.join(over.select('product_key'),'product_key','left_anti')
            seen = seen.join(over.select('product_key'),'product_key','left_anti')

            def limit_holds(frame, count):
                return frame.select(F.sha2(F.concat(F.lit(key+':history:'),'product_key'),256).alias('row_key'),'product_key',
                    F.to_json(F.struct(F.lit('PRODUCT_HISTORY_LIMIT').alias('reason'),count.alias('observations'),
                        F.col('bytes').alias('bytes'))).alias('payload_json'))
            global_policy = self.policy

            @F.udf('string')
            def policy_for(dependencies):
                return canonical(related_policy(global_policy,[json.loads(v) for v in dependencies]))

            @F.udf('string')
            def policy_hash(policy_json):
                value = json.loads(policy_json)
                return digest(dict(version=VERSION,**{k:value.get(k,[]) for k in ('mappings','rules')}))

            @F.udf('string')
            def identity(event_json,policy_json):
                event, policy = json.loads(event_json),json.loads(policy_json)
                return digest(dict(event=event['event_id'],policy={k:policy[k] for k in ('mappings','rules')},code=VERSION))

            policies = (index.groupBy('product_key').agg(F.collect_list('dependencies_json').alias('_deps'))
                .withColumn('policy_json',policy_for('_deps')).drop('_deps')
                .withColumn('normalization_policy_hash',policy_hash('policy_json'))).persist()
            desired = index.join(policies,'product_key').withColumn('row_key',identity('event_json','policy_json'))
            cached = delta.history('normalized_history',products)
            # Reuse is decided from fixed identities before any source body is fetched.
            needed = desired.join(cached.select('row_key'),'row_key','left_anti')
            new = spark.createDataFrame(needed.select('row_key','event_json','item_json','policy_json').rdd.mapPartitions(
                lambda rows:resolve_partition(rows,store_config,page_size,normalize=True)),
                'row_key string,product_key string,payload_json string').persist()
            try:
                with self.action('silver.normalize_and_commit',batch):
                    delta.append_history('normalized_history',new)
                    reusable = cached.join(desired.select('row_key'),'row_key','left_semi').select('row_key','product_key','payload_json')
                    selected = reusable.unionByName(new).dropDuplicates(['row_key'])
                    selected = selected.withColumn('quality',F.get_json_object('payload_json','$.quality'))
                    holds = selected.filter("quality = 'QUALITY_HOLD'").drop('quality')
                    proofs, outputs = [], {}
                    selected = selected.filter("quality = 'VALID'").withColumn('raw_id',F.get_json_object('payload_json','$.raw_document_id'))
                    selected = selected.withColumn('image_revision',F.get_json_object('payload_json','$.image_revision').cast('long'))
                    latest = Window.partitionBy('product_key','raw_id').orderBy(F.col('image_revision').desc(),'row_key')
                    selected = selected.withColumn('_rank',F.row_number().over(latest)).filter('_rank = 1')
                    limits = selected.groupBy('product_key').agg(F.sum(F.length('payload_json')).alias('bytes'),
                        F.count('*').alias('n'))
                    late = limits.filter(F.col('bytes') > self.limits.history_bytes)
                    selected = selected.join(late.select('product_key'),'product_key','left_anti')
                    # Bronze event reference and reason only; the raw payload is never copied.
                    holds = holds.unionByName(limit_holds(over,F.col('n'))).unionByName(limit_holds(late,F.col('n')))
                    if not holds.isEmpty():
                        outputs['holds'] = delta.snapshot(key,'silver_holds',holds)
                    outputs['silver'] = delta.snapshot(key,'silver',selected.select('row_key','product_key','payload_json'))
                    policy_rows = policies.select(F.sha2('product_key',256).alias('row_key'),'product_key',
                        F.to_json(F.struct('policy_json','normalization_policy_hash')).alias('payload_json'))
                    outputs['policies'] = delta.snapshot(key,'policies',policy_rows)
                    if retractions:
                        lists = retracted.groupBy('product_key').agg(
                            F.sort_array(F.collect_set('raw_document_id')).alias('retracted_raw_document_ids'))
                        outputs['retractions'] = delta.snapshot(key,'retractions',lists.select(
                            F.sha2('product_key',256).alias('row_key'),'product_key',
                            F.to_json(F.struct('retracted_raw_document_ids')).alias('payload_json')))
                        # Every retained observation of these products was explicitly retracted.
                        withdrawn = (seen.join(index.select('product_key').distinct(),'product_key','left_anti')
                            .join(lists,'product_key'))
                        if not withdrawn.isEmpty():
                            outputs['withdrawals'] = delta.snapshot(key,'withdrawals',withdrawn.select(
                                F.sha2('product_key',256).alias('row_key'),'product_key',
                                F.to_json(F.struct('retracted_raw_document_ids')).alias('payload_json')))
                limited = 0
                if 'holds' in outputs:
                    limited = delta.load(outputs['holds']).filter(
                        F.get_json_object('payload_json','$.reason') == 'PRODUCT_HISTORY_LIMIT').count()
                counts = dict(valid=outputs['silver']['count'],quality_hold=outputs['holds']['count'] if 'holds' in outputs else 0,
                    history_limit=limited,
                    products=outputs['policies']['count'],withdrawn=outputs['withdrawals']['count'] if 'withdrawals' in outputs else 0)
                return dict(proofs=list(outputs.values()),outputs=outputs,counts=counts)
            finally:
                new.unpersist()
                policies.unpersist()
                over.unpersist()
                index_base.unpersist()
                products.unpersist()
        s = self._stage(job,'silver',silver,parent=b['content_hash'])

        def gold():
            outputs = s['outputs']
            history = delta.load(outputs['silver']).select('product_key',
                F.get_json_object('payload_json','$.normalized_json').alias('normalized_json'))
            policy = delta.load(outputs['policies']).select('product_key',
                F.get_json_object('payload_json','$.normalization_policy_hash').alias('normalization_policy_hash'))
            retracted = None
            if 'retractions' in outputs:
                retracted = delta.load(outputs['retractions']).select('product_key',F.from_json(
                    F.get_json_object('payload_json','$.retracted_raw_document_ids'),'array<string>').alias('retracted_raw_document_ids'))
            with self.action('gold.native_join_window_aggregate',batch):
                output = build_gold(history,policy,self.policy.get('gold',{}),retracted)
                cached_frames = output._pipeline_cached_frames
                try:
                    if 'withdrawals' in outputs:
                        source = F.split('product_key',':')
                        withdrawn = delta.load(outputs['withdrawals']).select('product_key',F.to_json(F.struct(
                            'product_key',source.getItem(0).alias('source_code'),source.getItem(1).alias('source_product_id'),
                            F.lit(True).alias('withdrawn'),F.from_json(F.get_json_object('payload_json','$.retracted_raw_document_ids'),
                                'array<string>').alias('retracted_raw_document_ids'),
                            F.lit(False).alias('catalog_ready'),F.lit(False).alias('recommendation_ready'),
                            F.lit('SOURCE_RETRACTED').alias('outcome'),F.lit(VERSION).alias('policy_version'))).alias('payload_json'))
                        output = output.unionByName(withdrawn)
                    rows = output.withColumn('row_key',F.sha2(F.concat(F.lit(key+':'),F.col('product_key')),256))
                    proof = delta.snapshot(key,'gold',rows)
                finally:
                    for cached in cached_frames:
                        cached.unpersist()
                outcomes = {r['outcome'].lower():int(r['n']) for r in delta.load(proof).groupBy(
                    F.get_json_object('payload_json','$.outcome').alias('outcome')).agg(F.count('*').alias('n')).collect()}
            return dict(proofs=[proof],output=proof,gold_version=digest([proof]),input_hash=job['event_set_hash'],
                rule_version=VERSION,counts=dict(products=proof['count'],**outcomes))
        return self._stage(job,'gold',gold,parent=s['content_hash'])

    def products(self, stage):
        self._open()
        frame = self.delta.load(stage['output']).select('product_key','payload_json')
        # This is the bounded service boundary, not a Driver normalization/reduction path.
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

    def close(self):
        self.server_lake.close()
