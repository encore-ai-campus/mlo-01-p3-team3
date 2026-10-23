"""DataFrame-only Delta writes with immutable stage snapshots and a single fenced writer."""
from ..domain.contracts import digest, seal, verified, Invalid
from .files import read


class BatchDelta:
    def __init__(self, spark, root, fence, timing):
        self.spark, self.root, self.fence, self.timing = spark, root.rstrip('/')+'/finite-v1', fence, timing

    def _path(self, name):
        if not name.replace('_','').isalnum():
            raise Invalid('BATCH_TABLE_NAME_INVALID')
        return self.root + '/' + name

    def _exists(self, path):
        from delta.tables import DeltaTable
        return DeltaTable.isDeltaTable(self.spark,path)

    def _version(self, path):
        from delta.tables import DeltaTable
        return int(DeltaTable.forPath(self.spark,path).history(1).select('version').first()[0])

    def empty(self):
        return self.spark.createDataFrame([], 'row_key string, product_key string, payload_json string, record_hash string, bucket int')

    encoded_columns = ('row_key','product_key','payload_json','record_hash','bucket')

    def encode(self, frame):
        from pyspark.sql import functions as F
        return (frame.select('row_key','product_key','payload_json')
            .withColumn('record_hash',F.sha2('payload_json',256))
            .withColumn('bucket',F.pmod(F.xxhash64('product_key'),F.lit(128)).cast('int')))

    def history(self, name, products):
        from pyspark.sql import functions as F
        path = self._path(name)
        if not self._exists(path):
            return self.empty()
        # Literal buckets enable partition pruning, then a distributed key join narrows the rows.
        buckets = [r[0] for r in products.select(F.pmod(F.xxhash64('product_key'),F.lit(128)).cast('int')).distinct().collect()]
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
    def has_conflict(joined):
        """Same key already stored with different content or owner: never an overwrite."""
        from pyspark.sql import functions as F
        return bool(joined.filter(F.col('_old_hash').isNotNull() & ((F.col('record_hash') != F.col('_old_hash')) |
            (F.col('product_key') != F.col('_old_product')))).limit(1).count())

    @classmethod
    def new_rows(cls, joined):
        from pyspark.sql import functions as F
        return joined.filter(F.col('_old_hash').isNull()).select(*cls.encoded_columns)

    def append_history(self, name, frame):
        from pyspark.sql import functions as F
        path = self._path(name)
        encoded = self.encode(frame).persist()
        cached = [encoded]
        try:
            if encoded.groupBy('row_key').agg(F.countDistinct('record_hash').alias('n')).filter('n > 1').limit(1).count():
                raise Invalid('BATCH_OUTPUT_KEY_CONFLICT')
            encoded = encoded.dropDuplicates(['row_key'])
            if self._exists(path):
                # Only the buckets this write touches are read, so partition pruning applies and the
                # accumulated history is not scanned. One join serves both the conflict check and the
                # new-row filter.
                buckets = [r[0] for r in encoded.select('bucket').distinct().collect()]
                old = self.spark.read.format('delta').load(path).filter(F.col('bucket').isin(buckets))
                joined = self.join_existing(encoded,old).persist()
                cached.append(joined)
                if self.has_conflict(joined):
                    raise Invalid('DELTA_IMMUTABLE_CONFLICT')
                encoded = self.new_rows(joined)
            self.fence()
            if not encoded.isEmpty():
                encoded.write.format('delta').mode('append').partitionBy('bucket').save(path)
            self.fence()
        finally:
            for frame in cached:
                frame.unpersist()

    def snapshot(self, job_id, stage, frame):
        from pyspark.sql import functions as F
        from delta.tables import DeltaTable
        path = self.root + '/runs/' + job_id + '/' + stage
        encoded = self.encode(frame).persist()
        try:
            self.fence()
            recovering = self._exists(path)
            if not recovering:
                (encoded.write.format('delta').mode('append').partitionBy('bucket')
                    .option('txnAppId',job_id + ':' + stage).option('txnVersion',0).save(path))
            self.fence()
            history = DeltaTable.forPath(self.spark,path).history(1).first().asDict(recursive=True)
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
        if proof['target_hash'] != digest(self.root) or not proof['path'].startswith(self.root+'/runs/'):
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
