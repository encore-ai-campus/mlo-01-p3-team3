"""Insert-only Silver Delta outputs and receipts, guarded by the pipeline lease."""
from ..contracts import canonical
from ..errors import Conflict,PipelineError
from .normalize import KEYS,semantic_hash
from .schemas import columns,EXTRA,ARRAYS,DECIMALS,LONGS,TIMES


class DeltaSink:
    keys, schema_tables, schema_columns = KEYS, EXTRA, staticmethod(columns)
    arrays, decimals, longs, times, booleans = ARRAYS, DECIMALS, LONGS, TIMES, set()
    hash_row = staticmethod(semantic_hash)
    def __init__(self,spark,settings,*,lease):
        from delta.tables import DeltaTable
        from pyspark.sql.types import StructType,StructField,StringType,TimestampType,LongType,DecimalType,ArrayType,BooleanType
        self.spark,self.settings,self.lease=spark,settings,lease
        spark.conf.set('spark.sql.session.timeZone','UTC')
        self.paths={t:settings.silver_root.rstrip('/')+'/'+t for t in self.schema_tables}
        self.paths={t:spark._jvm.org.apache.hadoop.fs.Path(p).toString() for t,p in self.paths.items()}
        self.schemas={}
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
            (DeltaTable.createIfNotExists(spark).location(path).addColumns(schema)
                .property('delta.appendOnly','true').property('pipeline.targetFingerprint',settings.fingerprint).execute())
            lease.check(settings)
            stored=spark.read.format('delta').load(path).schema
            props=DeltaTable.forPath(spark,path).detail().select('properties').first()[0]
            if stored!=schema or props.get('pipeline.targetFingerprint')!=settings.fingerprint:
                raise Conflict('SILVER_DELTA_TARGET_CONFLICT')

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

    def existing(self,table,keys):return self._read(table,keys)

    def version(self,table):
        from delta.tables import DeltaTable
        return int(DeltaTable.forPath(self.spark,self.paths[table]).history(1).select('version').first()[0])

    def verify(self,table,expected,version):
        for values in (self._read(table,set(expected),version),self._read(table,set(expected))):
            if set(values)!=set(expected) or any(values[k]['record_hash']!=v for k,v in expected.items()):
                raise Conflict('SILVER_OUTPUT_MISSING_OR_CHANGED')

    def assert_revision(self,raw_id,revision,fingerprint):
        from pyspark.sql import functions as F
        values=(self.spark.read.format('delta').load(self.paths['product_images'])
            .filter((F.col('raw_document_id')==raw_id)&(F.col('image_state_revision')==revision))
            .select('image_state_fingerprint').distinct().limit(2).collect())
        if any(row[0]!=fingerprint for row in values):raise Conflict('SILVER_IMAGE_REVISION_CONFLICT')

    def insert(self,table,rows):
        if not rows:return
        from delta.tables import DeltaTable
        from pyspark.sql import functions as F
        from pyspark.sql.types import ArrayType
        self.lease.check(self.settings)
        keys={r[self.keys[table]] for r in rows}
        old=self.existing(table,keys)
        for row in rows:
            if row['record_hash']!=self.hash_row(row) or (row[self.keys[table]] in old and old[row[self.keys[table]]]['record_hash']!=row['record_hash']):
                raise Conflict('SILVER_OUTPUT_CONFLICT')
        if set(old)==keys:return
        frame=(self.spark.range(1).select(F.explode(F.from_json(F.lit(canonical(rows)),ArrayType(self.schemas[table]),
            {'mode':'FAILFAST','timeZone':'UTC','timestampFormat':"yyyy-MM-dd'T'HH:mm:ss.SSSXXX"})).alias('row')).select('row.*'))
        self.lease.check(self.settings)
        (DeltaTable.forPath(self.spark,self.paths[table]).alias('t').merge(frame.alias('s'),
            f't.{self.keys[table]} = s.{self.keys[table]}').whenNotMatchedInsertAll().execute())
        self.lease.check(self.settings)
        self.verify(table,{r[self.keys[table]]:r['record_hash'] for r in rows},self.version(table))
