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


def resolve_partition(rows, store_config, page_size, expires=None):
    # Called in an Executor. Admitted payloads arrive with the Kafka event, so the normal path
    # needs no source DB; the bounded store lookup is only a fallback. Yields (row_key,
    # product_key, payload_json, disposition, raw_json): the admitted raw goes to its own table.
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
                # The Kafka wire body, validated here instead of in a separate UDF stage.
                validate_event(event)
                event = {k:v for k,v in event.items() if k != 'payload'}
                if event['event_type'] == REJECTED:
                    # Rejected rows carry no trusted product identity and never reach Silver.
                    # Their restricted raw copy was verified by the relay; Executors never read it.
                    disposition = event['disposition']
                    raw_store = 'quarantine_raw' if disposition == 'INPUT_QUARANTINE' else None
                    body = dict(intake_metadata(event,disposition,event['reason'],expires,raw_store),origin_job=record['origin_job'])
                    yield (event['event_id'],'',canonical(body),disposition,None)
                    continue
                item = item if item is not None else items.get(event['source_ref'])
                if item is None or digest(item) != event['payload_hash']:
                    raise Invalid('IMMUTABLE_SOURCE_UNAVAILABLE')
                disposition, reason = classify(item,event)
                if disposition == 'ACCEPTED':
                    # The index keeps what every batch reads (event, dependencies, revision); the
                    # raw is kept once in input_raw, read only when a normalization is not reusable.
                    body = dict(event={k:v for k,v in event.items() if k != 'crawl_batch_id'},
                        dependencies=reference(item),image_revision=item['image_state']['image_state_revision'],
                        origin_job=record['origin_job'])
                    yield (record['row_key'],event['product_key'],canonical(body),disposition,canonical(item))
                else:
                    # The raw stays in the normal immutable source snapshot; only a reference is kept.
                    body = dict(intake_metadata(event,disposition,reason,expires,
                        'source' if disposition == 'INPUT_QUARANTINE' else None),origin_job=record['origin_job'])
                    yield (event['event_id'],event['product_key'],canonical(body),disposition,None)
    finally:
        if store is not None:
            store.close()


def deferred(call):
    """A call run on the first `join()` (and only once); later joins return or raise its outcome."""
    state = {}

    class Handle:
        def join(self):
            if not state:
                try:
                    state['result'] = call()
                except BaseException as exc:
                    state['error'] = exc
            if 'error' in state:
                raise state['error']
            return state.get('result')
    return Handle()


def concurrently(calls, settle=False):
    """Run independent Spark calls at once. Each thread keeps the caller's timing context and
    Spark local properties; the first failure is raised after every call has finished, or with
    `settle=True` every outcome (result or exception) is returned by name."""
    import contextvars
    from pyspark import InheritableThread
    results, errors = {}, []

    def run(name, call, context):
        try:
            results[name] = context.run(call)
        except BaseException as exc:
            errors.append(exc)
            results[name] = exc
    threads = [InheritableThread(target=run,args=(name,call,contextvars.copy_context())) for name,call in calls.items()]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    if settle:
        return results
    if errors:
        raise errors[0]
    return results


class SparkBatchProcessor:
    def __init__(self, store, server_lake, store_config, kafka_config, timing, limits, policy, retention=None):
        self.store, self.server_lake, self.store_config = store, server_lake, store_config
        self.kafka_config, self.timing, self.limits, self.policy = kafka_config, timing, limits, policy
        self.retention = retention or Retention()
        self.spark = self.delta = None
        self.read_kafka = None
        self._handoff = self._cached = None

    def _open(self):
        if self.spark is None:
            from .batch_delta import BatchDelta
            self.server_lake._open()
            self.spark = self.server_lake.spark
            self.delta = BatchDelta(self.spark,self.server_lake.config['lake_root'],self.server_lake.check,self.timing)

    def warm(self):
        """Resident start: open Spark, start the Python workers on every executor (they are
        reused) and load the product registry, so a batch's first stages do not wait for them."""
        self._open()
        with self.timing.measure('serve.python_worker_warmup','runtime'):
            context = self.spark.sparkContext
            slots = max(context.defaultParallelism,1)*2
            context.parallelize(range(slots),slots).map(lambda value:value).count()
        # The product registry is loaded once per worker process; do it before any batch.
        self.delta.registered([])

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
        handoff = getattr(self,'_handoff',None)
        if handoff and handoff['job'] != key:
            # Another job's deferred writes finish before this pass reads any history: a retry or
            # repair reuses stored Bronze and would otherwise store normalized keys that job still
            # appends with its absent claim. Normally already done by that job's finalize.
            self._release_handoff()
        self.compact_if_needed(batch)
        pin = pinned_inputs(self.store, key)
        allowed = pin['bronze_jobs'] + [key]
        store_config, page_size = self.store_config, self.limits.source_page_size
        reader = self.read_kafka or read_kafka

        raw_of = self._raw_of
        read_range = lambda expires:self._read_range(job,manifest,expires)

        def bronze():
            crawl = self.store.get('crawl',batch)['crawl']
            expires = self.retention.expiry(crawl['completed_at'],'metadata')
            with self.action('kafka.offset_read_and_validate',batch):
                # One small aggregate (computed with the range checks) decides which destinations
                # need a write at all, and names the accepted products.
                wire, resolved, found = read_range(expires)
                handed = False
                try:
                    tally = {r['disposition']:(int(r['n']),r['watermark']) for r in found}
                    products, buckets = next(((list(r['products']),list(r['buckets'])) for r in found if r['disposition'] == 'ACCEPTED'),([],[]))
                    accepted = resolved.filter(F.col('disposition') == 'ACCEPTED')
                    prior = []
                    if products:
                        first = job['attempts'] == 1
                        # Read before this job's rows exist: which products already had history.
                        prior = sorted(delta.registered(products))
                        with self.action('bronze.accepted.commit',batch):
                            # One row per event (deduplicated above), so the in-batch key check is not needed.
                            # Keys carry this job's id: on its first attempt none can be stored yet.
                            delta.append_history('input_index',accepted.drop('disposition','raw_json'),
                                unique=dict(keys=tally['ACCEPTED'][0],buckets=buckets,absent=first))
                        # The product key set is the index itself; it grows only after this commit.
                        delta.register(products)
                        self._release_handoff()
                        # Silver/Gold take this raw from the cache. Its commit starts only after
                        # publication (finalize), so it never shares the executors with them.
                        self._handoff = dict(job=key,frames=[wire,resolved],raw=raw_of(resolved),
                            index=accepted.select('row_key','product_key','payload_json'),
                            commit=deferred(lambda:delta.append_raw(key,raw_of(resolved),first)))
                        handed = True
                    outputs = {}
                    for table in sorted(set(METADATA_TABLES.values())):
                        names = [d for d,t in METADATA_TABLES.items() if t == table and tally.get(d,(0,))[0]]
                        if not names:
                            continue
                        frame = resolved.filter(F.col('disposition').isin(names)).drop('disposition','raw_json')
                        # Expiring metadata table: idempotent keyed append, one write per destination.
                        with self.action('bronze.'+table+'.commit',batch):
                            delta.append_history(table,frame)
                        outputs[table] = dict(delta.digest_rows(frame),expires_at=expires)
                    counts = dict(accepted=tally.get('ACCEPTED',(0,))[0],**{d.lower():v[0] for d,v in tally.items() if d != 'ACCEPTED'})
                    return dict(format=2,proofs=[],products=products,prior_products=prior,buckets=buckets,
                        metadata_outputs=outputs,counts=counts,
                        input=dict(delivery_hash=manifest['content_hash'],event_set_hash=manifest['event_set_hash'],
                            generation=manifest['generation']),
                        source_version_watermark=tally.get('ACCEPTED',(0,None))[1])
                finally:
                    if not handed:
                        resolved.unpersist()
                        wire.unpersist()

        def own_raw():
            """This job's raw for the product pass: the cache when Bronze ran here, else its partition
            (completed from the fixed range if the Bronze-time commit did not finish)."""
            handoff = getattr(self,'_handoff',None)
            if handoff and handoff['job'] == key:
                return handoff['raw']
            if job['kind'] == 'REPAIR':
                return delta.raw([])
            return self._complete_raw(job,manifest,b['counts']['accepted'])

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
                handoff = getattr(self,'_handoff',None)
                if job['kind'] != 'REPAIR' and 'prior_products' in b and handoff and handoff['job'] == key:
                    # Products seen for the first time have no stored history: their index rows are
                    # this job's (still cached) and no normalization exists. Only the others are read.
                    prior = b['prior_products']
                    fresh = handoff['index'].join(spark.createDataFrame([(p,) for p in prior],'product_key string'),'product_key','left_anti')
                    stored = spark.createDataFrame([(p,) for p in prior],'product_key string')
                    read = dict(buckets=sorted({r[0] for r in stored.select(delta.bucket('product_key')).collect()})) if prior else None
                else:
                    fresh, stored, read = None, frame, shared
                if read is None:
                    index = delta.empty().select('product_key',F.lit(0).alias('kind'),'row_key','payload_json')
                    cached = delta.empty().select('product_key',F.lit(1).alias('kind'),'row_key','payload_json')
                else:
                    index = delta.history('input_index',stored,read).filter(
                        F.get_json_object('payload_json','$.origin_job').isin(allowed)).select(
                        'product_key',F.lit(0).alias('kind'),'row_key','payload_json')
                    cached = delta.history('normalized_history',stored,read).select(
                        'product_key',F.lit(1).alias('kind'),'row_key','payload_json')
                if fresh is not None:
                    index = index.unionByName(fresh.select('product_key',F.lit(0).alias('kind'),'row_key','payload_json'))
                # The raw of this job's own observations is one input_raw partition; older raw is
                # read only for the observations the pass reports missing (a policy change).
                own = own_raw().select('product_key',F.lit(2).alias('kind'),'row_key','payload_json')
                base = index.unionByName(cached)

                def run(raw):
                    rows = (base.unionByName(raw).repartition(self.limits.spark_partitions,'product_key')
                        .sortWithinPartitions('product_key','kind','row_key'))
                    out = spark.createDataFrame(rows.rdd.mapPartitions(lambda part:product_pass(part,context)),
                        'kind string,row_key string,product_key string,payload_json string').persist()
                    label = F.coalesce(F.get_json_object('payload_json','$.outcome'),F.get_json_object('payload_json','$.reason'))
                    tally = {(r['kind'],r['label']):int(r['n']) for r in out.groupBy('kind',label.alias('label')).agg(
                        F.count('*').alias('n')).collect()}
                    return out, tally
                with self.action('product.silver_gold',batch):
                    out, tally = run(own)
                if any(k == 'missing' for k,_ in tally):
                    with self.action('product.raw_misses',batch):
                        missing = out.filter(F.col('kind') == 'missing').select('row_key','product_key','payload_json').collect()
                        affected = sorted({r['product_key'] for r in missing})
                        extra = delta.raw({r['payload_json'] for r in missing},{r['row_key'] for r in missing}).persist()
                        if extra.select('row_key').distinct().count() != len({r['row_key'] for r in missing}):
                            raise Invalid('IMMUTABLE_SOURCE_UNAVAILABLE')
                        # Only the affected products are computed again, now with their older raw.
                        base = base.filter(F.col('product_key').isin(affected))
                        again, retally = run(own.filter(F.col('product_key').isin(affected)).unionByName(
                            extra.select('product_key',F.lit(2).alias('kind'),'row_key','payload_json')))
                        extra.unpersist()
                        if any(k == 'missing' for k,_ in retally):
                            raise Invalid('IMMUTABLE_SOURCE_UNAVAILABLE')
                        merged = out.filter(~F.col('product_key').isin(affected)).unionByName(again).persist()
                        # The merged output is cached by this count before its inputs are released.
                        tally = {(r['kind'],r['label']):int(r['n']) for r in merged.groupBy('kind',F.coalesce(
                            F.get_json_object('payload_json','$.outcome'),F.get_json_object('payload_json','$.reason')).alias('label')).agg(
                            F.count('*').alias('n')).collect()}
                        out.unpersist()
                        again.unpersist()
                        out = merged
                self._release_cache()
                self._cached = (key,out)
                count = lambda kind:sum(n for (k,_),n in tally.items() if k == kind)
                kind = lambda name:out.filter(F.col('kind') == name).drop('kind')
                def normalized():
                    with self.action('silver.normalize_and_commit',batch):
                        # One row per identity by construction; buckets are a subset of the products' buckets.
                        # The pass read every stored row of these products and emits only identities not
                        # among them, and the writer lease admits no other append in between.
                        delta.append_history('normalized_history',kind('normalized'),
                            unique=dict(keys=count('normalized'),buckets=shared.get('buckets') or [],absent=True))

                def gold_commit():
                    with self.action('gold.commit',batch):
                        return delta.snapshot(key,'gold',kind('gold'))
                # Three independent tables: their writes and commits overlap instead of queueing.
                writes = dict(gold=gold_commit)
                handoff = getattr(self,'_handoff',None)
                if count('normalized') and handoff and handoff['job'] == key:
                    # The normalization cache is not needed to publish: it is appended after
                    # publication (finalize), so the Gold commit has the executors to itself. The
                    # absent claim still holds then: this process's next pass joins it first
                    # (_release_handoff), and a lost writer lease stays lost, so no other writer
                    # can append in between. Losing it only means a later batch parses again.
                    handoff['normalized'] = deferred(lambda:delta.append_history('normalized_history',kind('normalized'),
                        unique=dict(keys=count('normalized'),buckets=shared.get('buckets') or [],absent=True)))
                elif count('normalized'):
                    writes['normalized'] = normalized
                if count('hold'):
                    writes['holds'] = lambda:delta.snapshot(key,'silver_holds',kind('hold'))
                done = concurrently(writes)
                proof, outputs = done['gold'], {k:v for k,v in done.items() if k == 'holds'}
            finally:
                frame.unpersist()
            silver = dict(proofs=list(outputs.values()),outputs=outputs,counts=dict(valid=count('silver'),
                quality_hold=count('hold'),history_limit=tally.get(('hold','PRODUCT_HISTORY_LIMIT'),0),
                products=count('policy'),withdrawn=count('withdrawn')))
            outcomes = {label.lower():n for (k,label),n in tally.items() if k == 'gold'}
            return dict(format=2,proofs=[proof],output=proof,gold_version=digest([proof]),input_hash=job['event_set_hash'],
                rule_version=VERSION,counts=dict(products=proof['count'],**outcomes),silver=silver)
        try:
            g = self._stage(job,'gold',output,parent=b['content_hash'])
        except BaseException:
            self._release_handoff()
            raise
        # The raw commit runs after publication; finalize() runs it.
        if not self.store.get('stage',[key,'silver']):
            # Derived from the Gold record alone, so writing it again after a crash is identical.
            self.store.put('stage',[key,'silver'],seal(dict(g['silver'],stage='silver',status='COMPLETE',job_id=key,
                parent=b['content_hash'],attempts=g['attempts'],started_at=g['started_at'],completed_at=g['completed_at'],
                code_version=g['code_version'],spark_application_id=g['spark_application_id'])))
        return g

    def _read_range(self, job, manifest, expires):
        """Fixed-offset Kafka read, every range check in one aggregate, then Executor resolve.
        Returns the cached wire and resolved frames (the caller releases them) and the resolve
        summary by disposition."""
        from pyspark.sql import functions as F
        from .kafka_batch import read_kafka
        spark, batch, key = self.spark, job['crawl_batch_id'], job['job_id']
        store_config, page_size = self.store_config, self.limits.source_page_size
        # One read task per executor core: more splits only add a second wave and more consumers.
        splits = max(1,min(self.limits.spark_partitions,spark.sparkContext.defaultParallelism))
        wire = (self.read_kafka or read_kafka)(spark,self.kafka_config,manifest,splits).persist()
        # Fill the cache with one read of the fixed range first; otherwise the stages that follow
        # start together and each reads the range from the broker again.
        wire.count()
        schema = 'crawl_batch_id string, event_id string, payload_hash string, product_key string'
        decoded = wire.withColumn('e',F.from_json('value',schema))
        selected = decoded.filter(F.col('e.crawl_batch_id') == batch)
        # Do not silently skip undecodable records in a selected physical range.
        # Other batches may interleave; malformed records cannot be assigned safely.
        inventory = [(v['event_id'],v['payload_hash'],v['product_key']) for v in manifest['inventory']]
        try:
            if len({v[0] for v in inventory}) != len(inventory):
                raise Invalid('KAFKA_EVENT_SET_MISMATCH')
            # The manifest inventory as one JSON literal expanded in the JVM: a Python-side
            # DataFrame would wait for Python workers to start just to hold these few columns.
            expected = spark.range(1).select(F.explode(F.from_json(F.lit(json.dumps(
                [dict(_expected_id=a,_expected_hash=h,_expected_product=p) for a,h,p in inventory])),
                'array<struct<_expected_id:string,_expected_hash:string,_expected_product:string>>')).alias('x')).select('x.*')
            # dropDuplicates resolves top-level names only; lift the nested event id first.
            # Events are validated inside resolve_partition, not by a separate Python UDF stage.
            events = selected.withColumn('_event_id',F.col('e.event_id')).dropDuplicates(['_event_id']).select(
                F.sha2(F.concat(F.lit(key+':'),F.col('e.event_id')),256).alias('row_key'),
                F.col('value').alias('event_json'),F.lit(None).cast('string').alias('item_json'),F.lit(key).alias('origin_job'))
            resolved = spark.createDataFrame(events.rdd.mapPartitions(
                lambda rows:resolve_partition(rows,store_config,page_size,expires=expires)),
                'row_key string,product_key string,payload_json string,disposition string,raw_json string').persist()
            # The range checks and the resolve summary read the same cached range and do not
            # depend on each other, so they run at once. A range violation still wins over
            # whatever resolve reported, keeping the checks' precedence.
            done = concurrently(dict(flags=lambda:self.validate_range(decoded,batch,expected),
                found=lambda:resolved.groupBy('disposition').agg(F.count('*').alias('n'),
                    F.max(F.get_json_object('payload_json','$.event.collected_at')).alias('watermark'),
                    F.sort_array(F.collect_set('product_key')).alias('products'),
                    F.sort_array(F.collect_set(self.delta.bucket('product_key'))).alias('buckets')).collect()),settle=True)
            flags = done['flags']
            if isinstance(flags,BaseException):
                raise flags
            # Same precedence as the separate checks this one aggregate replaces.
            if flags['unclassifiable']:
                raise Invalid('KAFKA_RANGE_UNCLASSIFIABLE_EVENT')
            if flags['key_mismatch']:
                raise Invalid('KAFKA_KEY_MISMATCH')
            if flags['conflict']:
                raise Invalid('EVENT_ID_CONFLICT')
            if flags['mismatch']:
                raise Invalid('KAFKA_EVENT_SET_MISMATCH')
            if isinstance(done['found'],BaseException):
                raise done['found']
        except BaseException:
            if 'resolved' in locals():
                resolved.unpersist()
            wire.unpersist()
            raise
        return wire, resolved, done['found']

    @staticmethod
    def _raw_of(resolved):
        from pyspark.sql import functions as F
        return resolved.filter(F.col('disposition') == 'ACCEPTED').select('row_key','product_key',F.col('raw_json').alias('payload_json'))

    def _complete_raw(self, job, manifest, accepted):
        """Make this job's raw partition whole. The source records the outbox stored at intake
        are the copy (checked against each event's payload hash), so this works after the Kafka
        offsets expired. Only the Delta writer may write; any other worker reports it pending."""
        from pyspark.sql import functions as F
        handoff = getattr(self,'_handoff',None)
        if handoff and handoff['job'] == job['job_id']:
            # This process still holds the job's own deferred raw commit: run it first, so the
            # source copy only fills what that commit did not store (never the same rows twice).
            try:
                handoff['commit'].join()
            except BaseException:
                pass
            finally:
                self._release_handoff()
        stored = self.delta.raw([job['job_id']])
        # Completeness by distinct keys, not row count.
        if stored.select('row_key').distinct().count() >= accepted:
            return stored
        if not getattr(self.server_lake,'writable',True):
            raise Invalid('RAW_COMMIT_PENDING')
        with self.action('bronze.raw_recovery',job['crawl_batch_id']):
            # Missing keys come from Spark one row at a time (never the whole index on the
            # driver). Source bodies are fetched in pages sized so even maximum-size events stay
            # within the byte budget, and rows are written whenever the budget is reached.
            budget = self.limits.outbox_chunk_bytes
            page_size = max(1,min(self.limits.source_page_size,budget // self.limits.max_event_bytes))
            missing = self.delta.job_index(job['job_id']).join(stored.select('row_key'),'row_key','left_anti')
            pending, rows, amount = [], [], 0

            def write():
                self.delta.append_raw(job['job_id'],self.spark.createDataFrame(rows,
                    'row_key string,product_key string,payload_json string'),False)
                rows.clear()

            def fetch():
                nonlocal amount
                events = [json.loads(r['payload_json'])['event'] for r in pending]
                items = self.store.get_many('source',sorted({e['source_ref'] for e in events}))
                for row, event in zip(pending,events):
                    item = items.get(event['source_ref'])
                    if item is None or digest(item) != event['payload_hash']:
                        raise Invalid('IMMUTABLE_SOURCE_UNAVAILABLE')
                    body = canonical(item)
                    rows.append((row['row_key'],row['product_key'],body))
                    amount += len(body.encode())
                    if amount >= budget:
                        write()
                        amount = 0
                pending.clear()
            for row in missing.toLocalIterator():
                pending.append(row)
                if len(pending) >= page_size:
                    fetch()
            if pending:
                fetch()
            if rows:
                write()
        stored = self.delta.raw([job['job_id']])
        if stored.select('row_key').distinct().count() < accepted:
            raise Invalid('IMMUTABLE_SOURCE_UNAVAILABLE')
        return stored

    def complete_raw(self, job):
        """Writer-side: make a job's raw whole (before abandoning it, or for a job whose
        publish-only worker reported RAW_COMMIT_PENDING)."""
        if job['kind'] == 'REPAIR':
            return
        bronze = self.store.get('stage',[job['job_id'],'bronze'])
        if bronze and bronze.get('counts',{}).get('accepted'):
            self._open()
            self._complete_raw(job,None,bronze['counts']['accepted'])

    def finalize(self, job):
        """Before a job is marked complete: its raw commit (deferred past publication, which does
        not need it) is durable. A worker that did not run Bronze checks and completes it."""
        if job['kind'] == 'REPAIR':
            return
        self._open()
        handoff = getattr(self,'_handoff',None)
        if handoff and handoff['job'] == job['job_id']:
            try:
                if handoff.get('normalized'):
                    # A failed cache append keeps the job open; a retry without this process's
                    # cache only completes the raw (the normalization is redone by a later batch).
                    handoff['normalized'].join()
                try:
                    handoff['commit'].join()
                    return
                except BaseException:
                    pass
            finally:
                self._release_handoff()
        bronze = self.store.get('stage',[job['job_id'],'bronze'])
        if not bronze or not bronze.get('counts',{}).get('accepted'):
            return
        self._complete_raw(job,None,bronze['counts']['accepted'])

    def _release_handoff(self):
        handoff, self._handoff = getattr(self,'_handoff',None), None
        if handoff:
            for name in ('normalized','commit'):
                try:
                    if handoff.get(name):
                        handoff[name].join()
                except BaseException:
                    pass
            for frame in handoff['frames']:
                frame.unpersist()
            cached = getattr(self,'_cached',None)
            if cached and cached[0] == handoff['job']:
                self._release_cache()

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
            handoff = getattr(self,'_handoff',None)
            if cached and not (handoff and handoff['job'] == cached[0]):
                # Otherwise kept until finalize: the deferred normalized append reads it.
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
        """Idle-time maintenance of the history tables every batch reads. A cancel request stops
        it before the next table starts (part of OPTIMIZE runs on the driver, outside any job)."""
        self._open()
        self._cancelled = False
        done = {}
        with self.timing.measure('serve.history_compaction','maintenance'):
            for name in ('input_index','normalized_history'):
                if self._cancelled:
                    raise Invalid('MAINTENANCE_CANCELLED')
                done[name] = self.delta.compact(name)
        return done

    def compact_if_needed(self, batch):
        """Safety net when scheduled compaction did not run: a history table over the file limit
        is compacted before the batch reads it (that batch waits; normally never needed)."""
        for name in ('input_index','normalized_history'):
            if self.delta.files(name) > self.limits.history_files:
                with self.action('history.forced_compaction',batch):
                    self.delta.compact(name)

    def cancel_maintenance(self):
        # Only called while maintenance is the sole Spark work in this process; an interrupted
        # OPTIMIZE never commits, so the tables stay as they were.
        self._cancelled = True
        if self.spark is not None:
            self.spark.sparkContext.cancelAllJobs()

    def close(self):
        # A process-only worker ends here: let a running raw commit finish before Spark stops
        # (a failure is completed later by finalize()).
        self._release_handoff()
        self.server_lake.close()
