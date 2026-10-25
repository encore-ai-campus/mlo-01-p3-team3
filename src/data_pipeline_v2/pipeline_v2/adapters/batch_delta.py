"""DataFrame-only Delta writes with immutable stage snapshots and a single fenced writer."""
from contextlib import contextmanager
import threading
from ..domain.contracts import digest, seal, verified, Invalid
from .files import read


class BatchDelta:
    def __init__(self, spark, root, fence, timing):
        self.spark, self.root, self.fence, self.timing = spark, root.rstrip('/')+'/'+self.NAMESPACE, fence, timing
        self.legacy = [root.rstrip('/')+'/'+name for name in self.LEGACY]
        # Tables known to exist in this process. Tables are never dropped (purge deletes rows), so a
        # positive answer stays true; a negative one is asked again because a later write creates it.
        self.known = set()
        self._carry_lock = threading.Lock()

    @contextmanager
    def _step(self, name):
        """Time one sub-step and tag its Spark jobs with the same name for the event-log report.

        Only existing work is wrapped; nothing is counted or re-read for the measurement. The first
        action on a lazy frame also runs its upstream computation, so a step's time includes it.
        """
        stage = 'delta.' + name
        context = self.spark.sparkContext
        previous = context.getLocalProperty('pipeline.v2.stage')
        with self.timing.measure(stage, self.timing.active_scope or 'delta') as counts:
            context.setLocalProperty('pipeline.v2.stage', stage)
            try:
                yield counts
            finally:
                context.setLocalProperty('pipeline.v2.stage', previous)

    def _path(self, name):
        if not name.replace('_','').isalnum():
            raise Invalid('BATCH_TABLE_NAME_INVALID')
        return self.root + '/' + name

    def _exists(self, path):
        from delta.tables import DeltaTable
        if path in self.known:
            return True
        if DeltaTable.isDeltaTable(self.spark,path):
            self.known.add(path)
            return True
        return self._carry(path)

    def _carry(self, path):
        """First use of a history table in this namespace: copy the newest older one, once.

        input_index rows are split on the way: the raw `item` goes to input_raw (partitioned by
        origin job) and the index keeps the small rest. The raw table is written first and the
        index commit marks the copy done, so a crash in between only repeats the copy (the raw
        copy overwrites). Stage snapshots stay where their proofs point.
        """
        from delta.tables import DeltaTable
        name = path[len(self.root)+1:]
        if '/' in name or name in {'input_raw','product_registry'}:
            return False
        with self._carry_lock:
            if path in self.known:
                return True
            old = next((r + '/' + name for r in self.legacy if DeltaTable.isDeltaTable(self.spark,r + '/' + name)),None)
            if old is None:
                return False
            self.fence()
            with self._step('history.' + name + '.carry'):
                frame = self.spark.read.format('delta').load(old).select('row_key','product_key','payload_json')
                if name == 'input_index':
                    frame = self._split_raw(frame)
                self.encode(frame).repartition('bucket').write.format('delta').mode('append').partitionBy('bucket').save(path)
            self.fence()
            self.known.add(path)
            return True

    def _split_raw(self, frame):
        import json
        from pyspark.sql import functions as F
        from ..domain.contracts import canonical

        def split(rows):
            for row in rows:
                value = json.loads(row['payload_json'])
                item = value.pop('item',None)
                yield (row['row_key'],row['product_key'],canonical(value),canonical(item) if item is not None else None,value.get('origin_job'))
        parts = self.spark.createDataFrame(frame.rdd.mapPartitions(split),
            'row_key string,product_key string,payload_json string,raw_json string,job string').persist()
        raw = parts.filter(F.col('raw_json').isNotNull()).select('row_key','product_key',F.col('raw_json').alias('payload_json'),'job')
        (raw.withColumn('record_hash',F.sha2('payload_json',256)).write.format('delta').mode('overwrite')
            .partitionBy('job').save(self._path('input_raw')))
        self.known.add(self._path('input_raw'))
        return parts.select('row_key','product_key','payload_json')

    def registered(self, products, shared=None):
        """Products that already have history: one small row per product in product_registry.

        A batch reads the history tables only for these; a product seen for the first time has
        nothing stored. The registry is built once from input_index when it does not exist yet.
        """
        from pyspark.sql import functions as F
        path = self._path('product_registry')
        if not self._exists(path):
            index = self._path('input_index')
            if not self._exists(index):
                return set()
            self.fence()
            with self._step('registry.build'):
                keys = self.spark.read.format('delta').load(index).select('product_key').distinct()
                self.encode(self._registry_rows(keys)).repartition('bucket').write.format('delta').mode('append').partitionBy('bucket').save(path)
            self.fence()
            self.known.add(path)
        with self._step('registry.read'):
            return {r[0] for r in self.history('product_registry',products,shared).select('product_key').distinct().collect()}

    @staticmethod
    def _registry_rows(keys):
        from pyspark.sql import functions as F
        # Presence only: the same row for a product whoever registers it, so repeats never conflict.
        return keys.select(F.sha2('product_key',256).alias('row_key'),'product_key',F.lit('{}').alias('payload_json'))

    def register(self, products, buckets):
        """Record products whose history now exists (called after their index rows commit)."""
        if not products:
            return
        frame = self.spark.createDataFrame([(p,) for p in sorted(products)],'product_key string')
        # A retry may register a product again: the stored-key join keeps the append idempotent.
        self.append_history('product_registry',self._registry_rows(frame),unique=dict(keys=len(products),buckets=buckets))

    def append_raw(self, job_id, frame, first):
        """Admitted raw of one job into input_raw, partitioned by job: a batch reads only its own
        partition. Keys carry the job id, so a first attempt appends without a lookup; a retry
        compares the job's stored rows and appends only the missing ones."""
        from pyspark.sql import functions as F
        path = self._path('input_raw')
        # A carry from an older namespace writes input_raw first; let it finish before appending.
        self._exists(self._path('input_index'))
        encoded = frame.select('row_key','product_key','payload_json').withColumn(
            'record_hash',F.sha2('payload_json',256)).withColumn('job',F.lit(job_id))
        self.fence()
        with self._step('raw.write'):
            if not first and self._exists(path):
                stored = self.spark.read.format('delta').load(path).filter(F.col('job') == job_id).select(
                    'row_key',F.col('record_hash').alias('_old'))
                joined = encoded.join(stored,'row_key','left').persist()
                try:
                    if joined.filter(F.col('_old').isNotNull() & (F.col('_old') != F.col('record_hash'))).limit(1).count():
                        raise Invalid('DELTA_IMMUTABLE_CONFLICT')
                    joined.filter(F.col('_old').isNull()).drop('_old').write.format('delta').mode('append').partitionBy('job').save(path)
                finally:
                    joined.unpersist()
            else:
                encoded.write.format('delta').mode('append').partitionBy('job').save(path)
        self.fence()
        self.known.add(path)

    def raw(self, jobs, keys=None):
        """Raw rows of these jobs (partition pruned), optionally only these row keys."""
        from pyspark.sql import functions as F
        path = self._path('input_raw')
        if not jobs or not self._exists(path):
            return self.spark.createDataFrame([],'row_key string,product_key string,payload_json string')
        frame = self.spark.read.format('delta').load(path).filter(F.col('job').isin(sorted(jobs)))
        if keys is not None:
            frame = frame.join(self.spark.createDataFrame([(k,) for k in sorted(keys)],'row_key string'),'row_key','left_semi')
        return frame.select('row_key','product_key','payload_json')

    def _version(self, path):
        from delta.tables import DeltaTable
        return int(DeltaTable.forPath(self.spark,path).history(1).select('version').first()[0])

    def empty(self):
        return self.spark.createDataFrame([], 'row_key string, product_key string, payload_json string, record_hash string, bucket int')

    encoded_columns = ('row_key','product_key','payload_json','record_hash','bucket')
    # finite-v3: the admitted raw lives in input_raw, not in the index every batch reads.
    # (v2: single product pass, 16 history buckets; v1: 128 buckets.) Older history tables are
    # copied in once on first use, newest first; older stage snapshots load where their proofs point.
    NAMESPACE, LEGACY, BUCKETS = 'finite-v3', ('finite-v2','finite-v1'), 16

    @classmethod
    def bucket(cls, column):
        from pyspark.sql import functions as F
        return F.pmod(F.xxhash64(column),F.lit(cls.BUCKETS)).cast('int')

    def encode(self, frame):
        from pyspark.sql import functions as F
        return (frame.select('row_key','product_key','payload_json')
            .withColumn('record_hash',F.sha2('payload_json',256))
            .withColumn('bucket',self.bucket('product_key')))

    def history(self, name, products, shared=None):
        """History rows of these products. Pass the same `shared` dict to reuse one bucket list."""
        from pyspark.sql import functions as F
        path = self._path(name)
        shared = {} if shared is None else shared
        with self._step('history.' + name + '.read_plan'):
            if not self._exists(path):
                return self.empty()
            # Literal buckets enable partition pruning, then a distributed key join narrows the rows.
            if 'buckets' not in shared:
                shared['buckets'] = [r[0] for r in products.select(self.bucket('product_key')).distinct().collect()]
            buckets = shared['buckets']
        if not buckets:
            return self.empty()
        return self.spark.read.format('delta').load(path).filter(F.col('bucket').isin(buckets)).join(products,'product_key','left_semi')

    @staticmethod
    def join_existing(encoded, old):
        """Left-join new encoded rows to the stored rows with the same (bucket, row_key)."""
        from pyspark.sql import functions as F
        return encoded.join(old.select('bucket','row_key',F.col('product_key').alias('_old_product'),
            F.col('record_hash').alias('_old_hash')),['bucket','row_key'],'left')

    @staticmethod
    def conflict_flag():
        """Same key already stored with different content or owner: never an overwrite."""
        from pyspark.sql import functions as F
        return F.col('_old_hash').isNotNull() & ((F.col('record_hash') != F.col('_old_hash')) |
            (F.col('product_key') != F.col('_old_product')))

    @classmethod
    def has_conflict(cls, joined):
        return bool(joined.filter(cls.conflict_flag()).limit(1).count())

    @classmethod
    def new_rows(cls, joined):
        from pyspark.sql import functions as F
        return joined.filter(F.col('_old_hash').isNull()).select(*cls.encoded_columns)

    def append_history(self, name, frame, unique=None):
        """Keyed immutable append. `unique` = dict(keys=, buckets=) when the caller built the frame
        with one row per row_key and already counted it: the in-batch key check is then skipped.
        `absent=True` in it also states no key is stored yet, so the stored-key join is skipped."""
        from pyspark.sql import functions as F
        path = self._path(name)
        encoded = self.encode(frame).persist()
        cached = [encoded]
        step = 'history.' + name
        try:
            if unique is None:
                # First action on the cached frame: includes computing the rows being stored. One
                # aggregate gives the in-batch key conflict, the touched buckets and the distinct key count.
                with self._step(step + '.key_check'):
                    keys = (encoded.groupBy('row_key').agg(F.countDistinct('record_hash').alias('n'),F.first('bucket').alias('bucket'))
                        .agg(F.max((F.col('n') > 1).cast('int')).alias('conflict'),F.count('*').alias('keys'),
                             F.collect_set('bucket').alias('buckets')).first())
                    if keys['conflict']:
                        raise Invalid('BATCH_OUTPUT_KEY_CONFLICT')
                encoded = encoded.dropDuplicates(['row_key'])
            else:
                keys = unique
            fresh = keys['keys']
            with self._step(step + '.exists'):
                exists = self._exists(path)
            if exists and fresh and not (unique or {}).get('absent'):
                # Only the buckets this write touches are read, so partition pruning applies and the
                # accumulated history is not scanned. One join and one aggregate serve the conflict
                # check and the new-row count; the new-row filter reuses the cached join.
                with self._step(step + '.conflict_join'):
                    old = self.spark.read.format('delta').load(path).filter(F.col('bucket').isin(sorted(keys['buckets'])))
                    joined = self.join_existing(encoded,old).persist()
                    cached.append(joined)
                    found = joined.agg(F.max(self.conflict_flag().cast('int')).alias('conflict'),
                        F.sum(F.col('_old_hash').isNull().cast('int')).alias('fresh')).first()
                    if found['conflict']:
                        raise Invalid('DELTA_IMMUTABLE_CONFLICT')
                encoded = self.new_rows(joined)
                fresh = found['fresh'] or 0
            self.fence()
            if fresh:
                with self._step(step + '.write'):
                    # One file per touched bucket: every later history read opens each file on S3.
                    encoded.repartition('bucket').write.format('delta').mode('append').partitionBy('bucket').save(path)
                self.known.add(path)
            self.fence()
        finally:
            for frame in cached:
                frame.unpersist()

    def compact(self, name):
        """Merge a history table's small files (Delta OPTIMIZE, bin-packing per bucket).

        Reads are latency-bound on S3 (one open per file), so many small appended files cost more
        than their bytes. Compaction rewrites data only; old files stay for time travel.
        """
        from delta.tables import DeltaTable
        path = self._path(name)
        if not self._exists(path):
            return None
        self.fence()
        with self._step('history.' + name + '.compact') as counts:
            result = DeltaTable.forPath(self.spark,path).optimize().executeCompaction().first()
            metrics = result['metrics'].asDict() if result is not None else {}
            counts.update(files_removed=int(metrics.get('numFilesRemoved') or 0),files_added=int(metrics.get('numFilesAdded') or 0))
        self.fence()
        return dict(files_removed=counts['files_removed'],files_added=counts['files_added'])

    def snapshot(self, job_id, stage, frame):
        from pyspark.sql import functions as F
        from delta.tables import DeltaTable
        path = self.root + '/runs/' + job_id + '/' + stage
        encoded = self.encode(frame).persist()
        step = 'snapshot.' + stage
        try:
            self.fence()
            with self._step(step + '.exists'):
                recovering = self._exists(path)
            if not recovering:
                # First action on the cached frame: includes computing the rows being stored.
                # A snapshot is always read whole at a pinned version, so it is not partitioned by
                # bucket: that wrote about one file per product. One file per write task instead;
                # bucket stays a column, and earlier bucket-partitioned snapshots still load.
                with self._step(step + '.write'):
                    (encoded.write.format('delta').mode('append')
                        .option('txnAppId',job_id + ':' + stage).option('txnVersion',0).save(path))
                self.known.add(path)
            self.fence()
            with self._step(step + '.history_read') as counts:
                history = DeltaTable.forPath(self.spark,path).history(1).first().asDict(recursive=True)
                # Already read for the proof; file and byte counts come from the same commit record.
                for metric, label in (('numFiles','output_files'),('numOutputBytes','output_bytes')):
                    value = (history.get('operationMetrics') or {}).get(metric)
                    if value is not None:
                        counts[label] = int(value)
            version = int(history['version'])
            if recovering:
                pinned = self.spark.read.format('delta').option('versionAsOf',version).load(path)
                # Only an uncertain earlier commit needs this recovery comparison.
                if (pinned.exceptAll(encoded).limit(1).count() or encoded.exceptAll(pinned).limit(1).count()):
                    raise Invalid('DELTA_STAGE_RETRY_CONFLICT')
            output_rows = history.get('operationMetrics',{}).get('numOutputRows')
            if output_rows is None:
                raise Invalid('DELTA_COMMIT_ROW_METRIC_UNAVAILABLE')
            return seal(dict(kind='DELTA_BATCH_SNAPSHOT',path=path,version=version,
                count=int(output_rows),target_hash=digest(self.root)))
        finally:
            encoded.unpersist()

    def load(self, proof):
        verified(proof)
        if not any(proof['target_hash'] == digest(root) and proof['path'].startswith(root+'/runs/') for root in (self.root,*self.legacy)):
            raise Invalid('DELTA_STAGE_TARGET_CONFLICT')
        frame = self.spark.read.format('delta').option('versionAsOf',proof['version']).load(proof['path'])
        return frame

    def audit(self, proof):
        from pyspark.sql import functions as F
        frame = self.load(proof)
        if frame.count() != proof['count'] or frame.filter(F.sha2('payload_json',256) != F.col('record_hash')).limit(1).count():
            raise Invalid('DELTA_STAGE_PROOF_CHANGED')

    def digest_rows(self, frame):
        """Order-independent hash of (row_key, record_hash) computed on Executors, one small result."""
        from pyspark.sql import functions as F
        value = (self.encode(frame).agg(F.count('*').alias('n'),F.sha2(F.concat_ws(',',F.sort_array(F.collect_list(
            F.concat_ws(':','row_key','record_hash')))),256).alias('h'))).first()
        return dict(count=int(value['n']),hash=value['h'])

    def metadata(self, name, job_id):
        from pyspark.sql import functions as F
        path = self._path(name)
        if not self._exists(path):
            return self.empty().select('row_key','product_key','payload_json')
        return (self.spark.read.format('delta').load(path)
            .filter(F.get_json_object('payload_json','$.origin_job') == job_id).select('row_key','product_key','payload_json'))

    def purge(self, name, expires_before):
        """Retention delete on an expiring metadata table; stage snapshots are never purged."""
        from pyspark.sql import functions as F
        from delta.tables import DeltaTable
        path = self._path(name)
        if not self._exists(path):
            return 0
        self.fence()
        table = DeltaTable.forPath(self.spark,path)
        before = self._version(path)
        table.delete(F.get_json_object('payload_json','$.metadata_expires_at') <= F.lit(expires_before))
        self.fence()
        latest = table.history(1).first().asDict(recursive=True)
        if int(latest['version']) == before:
            return 0
        return int((latest.get('operationMetrics') or {}).get('numDeletedRows') or 0)
