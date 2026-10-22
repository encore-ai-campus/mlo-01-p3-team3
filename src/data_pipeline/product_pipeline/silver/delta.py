"""Insert-only Silver Delta outputs and receipts, guarded by the pipeline lease."""
from ..contracts import canonical
from ..errors import Conflict,PipelineError
from .normalize import KEYS,semantic_hash
from .schemas import columns,EXTRA,ARRAYS,DECIMALS,LONGS,TIMES
from ..spark_session import measured


class DeltaSink:
    supports_history_bridge=True
    keys, schema_tables, schema_columns = KEYS, EXTRA, staticmethod(columns)
    arrays, decimals, longs, times, booleans = ARRAYS, DECIMALS, LONGS, TIMES, set()
    hash_row = staticmethod(semantic_hash)
    @measured('delta_target_open')
    def __init__(self,spark,settings,*,lease):
        from delta.tables import DeltaTable
        from ..bronze.metadata import read_metadata
        from pyspark.sql.types import StructType,StructField,StringType,TimestampType,LongType,DecimalType,ArrayType,BooleanType
        self.spark,self.settings,self.lease=spark,settings,lease
        self._versions={};self._proofs={};self._batch_reads=None;self._pending_appends={}
        spark.conf.set('spark.sql.session.timeZone','UTC')
        self.paths={t:settings.silver_root.rstrip('/')+'/'+t for t in self.schema_tables}
        self.paths={t:spark._jvm.org.apache.hadoop.fs.Path(p).toString() for t,p in self.paths.items()}
        self.schemas={}
        self.target_metadata={}
        for table,path in self.paths.items():
            def dtype(k):
                if k in self.arrays:return ArrayType(StringType())
                if k in self.decimals:return DecimalType(20,4)
                if k in self.longs:return LongType()
                if k in self.times:return TimestampType()
                if k in self.booleans:return BooleanType()
                return StringType()
            schema=StructType([StructField(k,dtype(k),True) for k in self.schema_columns(table)])
            self.schemas[table]=schema
            lease.check(settings)
            metadata=read_metadata(spark,path)
            if metadata is None:
                (DeltaTable.createIfNotExists(spark).location(path).addColumns(schema)
                    .property('delta.appendOnly','true').property('pipeline.targetFingerprint',settings.fingerprint).execute())
                metadata=read_metadata(spark,path)
            lease.check(settings)
            if (metadata is None or metadata['schema']!=schema
                    or metadata['properties'].get('pipeline.targetFingerprint')!=settings.fingerprint
                    or metadata['properties'].get('delta.appendOnly')!='true'):
                raise Conflict('SILVER_DELTA_TARGET_CONFLICT')
            self.target_metadata[table]=metadata
            self._versions[table]=metadata['version']
        lease.check(settings)
        from ..spark_session import bump
        bump('delta_metadata_read_batches');bump('delta_target_checks',len(self.paths))

    @measured('delta_output_read')
    def _read(self,table,keys,version=None):
        from pyspark.sql import functions as F
        if not keys:return {}
        reader=self.spark.read.format('delta')
        if version is not None:reader=reader.option('versionAsOf',version)
        frame=reader.load(self.paths[table]).filter(F.col(self.keys[table]).isin(sorted(keys))).limit(len(keys)+1)
        for name in self.schema_columns(table):
            if name in self.times:frame=frame.withColumn(name,F.date_format(F.col(name),"yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"))
        result={}
        for row in frame.collect():
            value=row.asDict(recursive=True)
            for name in self.decimals & set(value):
                if value[name] is not None:value[name]=format(value[name].normalize(),'f')
            key=value[self.keys[table]]
            if key in result:raise Conflict('SILVER_DUPLICATE_KEY')
            if value['record_hash']!=self.hash_row(value):raise Conflict('SILVER_STORED_HASH_CONFLICT')
            result[key]=value
        return result

    def batch_scope(self):
        from contextlib import contextmanager
        @contextmanager
        def scope():
            if getattr(self,'_batch_reads',None) is not None:raise Conflict('DELTA_BATCH_SCOPE_NESTED')
            self._batch_reads={}
            try:yield
            finally:self._batch_reads=None;self._pending_appends={};self._append_reads={}
        return scope()

    def existing(self,table,keys):
        reads=getattr(self,'_batch_reads',None)
        if reads is None:return self._read(table,keys)
        self.lease.check(self.settings);version=self.version(table)
        from ..delta_key_index import candidates
        keys=candidates(self,table,version,keys)
        entry=reads.setdefault((table,version),dict(checked=set(),rows={}))
        missing=set(keys)-entry['checked']
        if missing:
            rows=self._read(table,missing,version);self.lease.check(self.settings)
            entry['rows'].update(rows);entry['checked'].update(missing)
        from ..spark_session import bump
        bump('delta_batch_preflight_reused_keys',len(keys)-len(missing))
        return {k:entry['rows'][k] for k in keys if k in entry['rows']}

    def versions_many(self,tables):
        from ..delta_proofs import versions_many
        return versions_many(self,tables)

    def verify_many(self,proofs):
        from ..delta_proofs import verify_many
        return verify_many(self,proofs)

    def insert_many(self,tables,*,hook=None):
        from ..delta_batch import insert_many
        return insert_many(self,tables,hook=hook)

    @measured('delta_version_read')
    def version(self,table):
        return self.versions_many([table])[table]

    @measured('delta_output_verify')
    def verify(self,table,expected,version):
        self.verify_many([(table,version,expected)])

    def assert_revision(self,raw_id,revision,fingerprint):
        from pyspark.sql import functions as F
        values=(self.spark.read.format('delta').load(self.paths['product_images'])
            .filter((F.col('raw_document_id')==raw_id)&(F.col('image_state_revision')==revision))
            .select('image_state_fingerprint').distinct().limit(2).collect())
        if any(row[0]!=fingerprint for row in values):raise Conflict('SILVER_IMAGE_REVISION_CONFLICT')

    def assert_revisions(self,revisions):
        from pyspark.sql import functions as F
        if not revisions or len(revisions)>100:raise PipelineError('SILVER_BATCH_LIMIT')
        expected={(raw,rev):fp for raw,rev,fp in revisions};condition=None
        for raw,rev in expected:
            term=(F.col('raw_document_id')==raw)&(F.col('image_state_revision')==rev)
            condition=term if condition is None else condition|term
        values=(self.spark.read.format('delta').load(self.paths['product_images']).filter(condition)
            .select('raw_document_id','image_state_revision','image_state_fingerprint').distinct().limit(len(expected)+1).collect())
        if len(values)>len(expected) or any(expected[(r[0],r[1])]!=r[2] for r in values):
            raise Conflict('SILVER_IMAGE_REVISION_CONFLICT')

    @measured('delta_output_insert')
    def insert(self,table,rows):
        if not rows:return
        self.lease.check(self.settings)
        keys={r[self.keys[table]] for r in rows}
        if len(keys)!=len(rows):raise Conflict('SILVER_DUPLICATE_BATCH_OUTPUT')
        old=self.existing(table,keys)
        for row in rows:
            if row['record_hash']!=self.hash_row(row) or (row[self.keys[table]] in old and old[row[self.keys[table]]]['record_hash']!=row['record_hash']):
                raise Conflict('SILVER_OUTPUT_CONFLICT')
        if set(old)==keys:return
        # Every writer owns this complete target and its checkpoint exclusively.
        # Existing hashes were checked above; only genuinely missing immutable
        # rows are appended. An uncertain commit is recovered by the same read
        # before another append, not by blindly repeating a write.
        missing=[r for r in rows if r[self.keys[table]] not in old]
        self._append(table,missing)

    def _append(self,table,missing):
        if not missing:return
        from pyspark.sql import functions as F
        from pyspark.sql.types import ArrayType
        self.lease.check(self.settings)
        bridge=table in getattr(self,'target_metadata',{})
        pending=None
        if bridge:
            from uuid import uuid4
            pending=dict(base=self.version(table),token='pipeline-append:'+uuid4().hex,
                expected={r[self.keys[table]]:r['record_hash'] for r in missing})
        frame=(self.spark.range(1).select(F.explode(F.from_json(F.lit(canonical(missing)),ArrayType(self.schemas[table]),
            {'mode':'FAILFAST','timeZone':'UTC','timestampFormat':"yyyy-MM-dd'T'HH:mm:ss.SSSXXX"})).alias('row')).select('row.*'))
        self.lease.check(self.settings)
        self._versions.pop(table,None)
        if getattr(self,'_batch_reads',None) is not None:
            self._batch_reads={k:v for k,v in self._batch_reads.items() if k[0]!=table}
        from ..spark_session import bump
        from ..spark_session import timed
        with timed('delta_append_commit'):
            writer=frame.write.format('delta').mode('append').option('mergeSchema','false')
            if pending:
                self._pending_appends[table]=pending;writer=writer.option('userMetadata',pending['token'])
            try:writer.save(self.paths[table])
            except BaseException:
                if pending:self._pending_appends.pop(table,None)
                raise
        bump('delta_append_batches');bump('delta_appended_rows',len(missing))
        self.lease.check(self.settings)
        # The stage runner verifies fixed-version and current outputs before
        # writing its receipt. Repeating that same read here doubles Spark I/O.
