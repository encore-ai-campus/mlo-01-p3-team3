"""DataFrame-only Delta writes with immutable stage snapshots and a single fenced writer."""
from contextlib import contextmanager
from ..domain.contracts import digest, seal, verified, Invalid
from .files import read


class BatchDelta:
    def __init__(self, spark, root, fence, timing):
        self.spark, self.root, self.fence, self.timing = spark, root.rstrip('/')+'/'+self.NAMESPACE, fence, timing
        self.legacy = root.rstrip('/')+'/'+self.LEGACY
        # Tables known to exist in this process. Tables are never dropped (purge deletes rows), so a
        # positive answer stays true; a negative one is asked again because a later write creates it.
        self.known = set()

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
        """First use of a history table in this namespace: copy the v1 table, rebucketed, once.

        One Delta commit creates the table, so a crash leaves either no table (copied again next
        time) or the whole copy. Only the single fenced writer copies; stage snapshots stay where
        their proofs point.
        """
        from delta.tables import DeltaTable
        from pyspark.sql import functions as F
        name = path[len(self.root)+1:]
        old = self.legacy + '/' + name
        if '/' in name or not DeltaTable.isDeltaTable(self.spark,old):
            return False
        self.fence()
        with self._step('history.' + name + '.carry_v1'):
            (self.spark.read.format('delta').load(old).select('row_key','product_key','payload_json','record_hash')
                .withColumn('bucket',self.bucket('product_key')).repartition('bucket').write.format('delta').mode('append')
                .partitionBy('bucket').save(path))
        self.fence()
        self.known.add(path)
        return True

    def _version(self, path):
        from delta.tables import DeltaTable
        return int(DeltaTable.forPath(self.spark,path).history(1).select('version').first()[0])

    def empty(self):
        return self.spark.createDataFrame([], 'row_key string, product_key string, payload_json string, record_hash string, bucket int')

    encoded_columns = ('row_key','product_key','payload_json','record_hash','bucket')
    # finite-v2: single product pass outputs and 16 history buckets. A write touches at most 16
    # files instead of about one per product. v1 history tables (128 buckets) are copied in once on
    # first use; v1 stage snapshots are still loaded where their proofs point.
    NAMESPACE, LEGACY, BUCKETS = 'finite-v2', 'finite-v1', 16

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
        if not any(proof['target_hash'] == digest(root) and proof['path'].startswith(root+'/runs/') for root in (self.root,self.legacy)):
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
