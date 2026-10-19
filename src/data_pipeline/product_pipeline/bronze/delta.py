"""Single Driver, insert-only Delta tables. Existing rows are never updated."""
from ..contracts import canonical
from ..errors import Conflict, PipelineError


def write_chunks(rows,*,max_bytes=2*1024*1024,max_records=10):
    """Bound each Catalyst JSON literal independently of Kafka replay batch size.

    An indivisible source row may exceed the preferred chunk size; it is kept
    whole. Existing source/document limits still apply before this boundary.
    """
    chunk=[];size=2
    for row in rows:
        amount=len(canonical(row).encode('utf-8'))+1
        if chunk and (len(chunk)>=max_records or size+amount>max_bytes):
            yield chunk;chunk=[];size=2
        chunk.append(row);size+=amount
    if chunk:yield chunk

SPECS = {
    'raw': ('raw_document_id', [('raw_document_id', 's'), ('source_code', 's'), ('source_product_id', 's'),
        ('source_record_key', 's'), ('collection_run_id', 's'), ('observation_key', 's'), ('payload_hash', 's'),
        ('document_hash', 's'), ('schema_version', 's'), ('collected_at', 't'),
        ('payload_json', 's'), ('raw_envelope_json', 's'), ('ingested_at', 't')]),
    'events': ('event_id', [('event_id', 's'), ('event_hash', 's'), ('event_json', 's'),
        ('raw_document_id', 's'), ('document_hash', 's'), ('kafka_topic', 's'),
        ('kafka_partition', 'i'), ('kafka_offset', 'l'), ('recorded_at', 't')]),
    'quarantine': ('quarantine_id', [('quarantine_id', 's'), ('evidence_hash', 's'),
        ('record_json', 's'), ('error_code', 's'), ('recorded_at', 't')]),
}


def same_content(table, first, second):
    fields = {'raw': [k for k, _ in SPECS['raw'][1] if k != 'ingested_at'],
              'events': ['event_id', 'event_hash', 'event_json', 'raw_document_id', 'document_hash'],
              'quarantine': ['quarantine_id', 'evidence_hash', 'record_json']}[table]
    return all(first.get(k) == second.get(k) for k in fields)


class DeltaSink:
    def __init__(self, spark, settings, *, lease=None):
        if lease is None:
            raise PipelineError('BRONZE_WRITER_LEASE_REQUIRED')
        self.lease, self.settings = lease, settings
        self.lease.check(self.settings)
        from delta.tables import DeltaTable
        from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType, LongType
        self.spark = spark
        spark.conf.set('spark.sql.session.timeZone', 'UTC')
        self.paths = {'raw': settings.bronze_path, 'events': settings.events_path, 'quarantine': settings.quarantine_path}
        # Hadoop canonicalizes file:/// to file:/; use one representation in Delta CREATE.
        self.paths = {k: spark._jvm.org.apache.hadoop.fs.Path(v).toString() for k, v in self.paths.items()}
        types = {'s': StringType, 't': TimestampType, 'i': IntegerType, 'l': LongType}
        self.schemas = {}
        # Claim target identity in every table; a changed target cannot silently reuse a checkpoint.
        for table, (_, fields) in SPECS.items():
            schema = StructType([StructField(k, types[t](), True) for k, t in fields])
            self.schemas[table] = schema
            self.lease.check(self.settings)
            (DeltaTable.createIfNotExists(spark).location(self.paths[table]).addColumns(schema)
             .property('delta.appendOnly', 'true').property('pipeline.targetFingerprint', settings.fingerprint).execute())
            self.lease.check(self.settings)
            existing = spark.read.format('delta').load(self.paths[table]).schema
            if [(f.name, f.dataType) for f in existing] != [(f.name, f.dataType) for f in schema]:
                raise Conflict('BRONZE_DELTA_SCHEMA_CONFLICT')
            props = DeltaTable.forPath(spark, self.paths[table]).detail().select('properties').first()[0]
            if props.get('pipeline.targetFingerprint') != settings.fingerprint:
                raise Conflict('BRONZE_DELTA_TARGET_CONFLICT')

    def _lookup(self, table, field, keys):
        from pyspark.sql import functions as F
        if not keys:
            return {}
        frame = (self.spark.read.format('delta').load(self.paths[table])
                 .filter(F.col(field).isin(sorted(keys))).limit(len(keys) + 1))
        # Format inside Spark's UTC session; Python collect() otherwise uses Driver OS timezone.
        for k, t in SPECS[table][1]:
            if t == 't':
                frame = frame.withColumn(k, F.date_format(F.col(k), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"))
        values = frame.collect()
        result = {}
        for row in values:
            value = row.asDict()
            if value[field] in result:
                raise Conflict('BRONZE_DELTA_DUPLICATE_KEY')
            result[value[field]] = value
        return result

    def existing(self, table, keys):
        return self._lookup(table, SPECS[table][0], keys)

    def observations(self, keys):
        return self._lookup('raw', 'observation_key', keys)

    def insert(self, table, rows):
        for chunk in write_chunks(rows):self._insert_chunk(table,chunk)

    def _insert_chunk(self, table, rows):
        self.lease.check(self.settings)
        from delta.tables import DeltaTable
        from pyspark.sql import functions as F
        from pyspark.sql.types import ArrayType
        if not rows:
            return
        key = SPECS[table][0]
        old = self.existing(table, {row[key] for row in rows})
        for row in rows:
            if row[key] in old and not same_content(table, old[row[key]], row):
                raise Conflict('BRONZE_DELTA_CONTENT_CONFLICT')
        rows=[row for row in rows if row[key] not in old]
        if not rows:return
        # Data already resides on Driver as canonical JSON; parse with Spark SQL on JVM.
        # This avoids serializing documents through a second Python worker process.
        frame = (self.spark.range(1).select(F.explode(F.from_json(F.lit(canonical(rows)),
            ArrayType(self.schemas[table]), {'mode': 'FAILFAST', 'timeZone': 'UTC',
                'timestampFormat': "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"})).alias('row')).select('row.*'))
        self.lease.check(self.settings)
        (DeltaTable.forPath(self.spark, self.paths[table]).alias('target').merge(frame.alias('incoming'),
            f'target.{key} = incoming.{key}').whenNotMatchedInsertAll().execute())
        self.lease.check(self.settings)
        stored = self.existing(table, {row[key] for row in rows})
        if any(row[key] not in stored or not same_content(table, stored[row[key]], row) for row in rows):
            raise Conflict('BRONZE_DELTA_COMMIT_UNCONFIRMED')
