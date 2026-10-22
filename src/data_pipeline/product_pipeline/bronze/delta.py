"""Single Driver, insert-only Delta tables. Existing rows are never updated."""
from ..contracts import canonical
from ..errors import Conflict, PipelineError
from .metrics import timed,bump


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
        self._prepared={}
        spark.conf.set('spark.sql.session.timeZone', 'UTC')
        self.paths = {'raw': settings.bronze_path, 'events': settings.events_path, 'quarantine': settings.quarantine_path}
        # Hadoop canonicalizes file:/// to file:/; use one representation in Delta CREATE.
        self.paths = {k: spark._jvm.org.apache.hadoop.fs.Path(v).toString() for k, v in self.paths.items()}
        types = {'s': StringType, 't': TimestampType, 'i': IntegerType, 'l': LongType}
        self.schemas = {}
        # Claim target identity in every table; a changed target cannot silently reuse a checkpoint.
        from .metadata import read_metadata
        self.target_metadata={}
        for table, (_, fields) in SPECS.items():
            schema = StructType([StructField(k, types[t](), True) for k, t in fields])
            self.schemas[table] = schema
            metadata=read_metadata(spark,self.paths[table])
            if metadata is None:
                self.lease.check(self.settings)
                with timed('delta_target_create'):
                    (DeltaTable.createIfNotExists(spark).location(self.paths[table]).addColumns(schema)
                     .property('delta.appendOnly', 'true').property('pipeline.targetFingerprint', settings.fingerprint).execute())
                self.lease.check(self.settings)
                metadata=read_metadata(spark,self.paths[table])
                if metadata is None:raise Conflict('BRONZE_DELTA_TARGET_UNCONFIRMED')
            existing=metadata['schema']
            if [(f.name, f.dataType) for f in existing] != [(f.name, f.dataType) for f in schema]:
                raise Conflict('BRONZE_DELTA_SCHEMA_CONFLICT')
            if (metadata['properties'].get('pipeline.targetFingerprint')!=settings.fingerprint
                    or metadata['properties'].get('delta.appendOnly')!='true'):
                raise Conflict('BRONZE_DELTA_TARGET_CONFLICT')
            self.target_metadata[table]={k:metadata[k] for k in ('table_id','version')}
        self.lease.check(settings)

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
        with timed('delta_lookup'):values = frame.collect()
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

    def batch_lookups(self,raw_keys,event_keys,observation_keys):
        """One bounded collect, one raw table scan for both identity checks.

        Prepared absence is scoped to this batch and consumed before writing.
        A failed append/retry must query durable storage again.
        """
        from pyspark.sql import functions as F
        from ..files import parse
        self._prepared={}
        requests={'raw':{'raw_document_id':set(raw_keys),'observation_key':set(observation_keys)},
                  'events':{'event_id':set(event_keys)}}
        frames=[]
        for table,fields in requests.items():
            fields={k:v for k,v in fields.items() if v}
            if not fields:continue
            predicates=[F.col(k).isin(sorted(v)) for k,v in fields.items()]
            predicate=predicates[0]
            for value in predicates[1:]:predicate=predicate|value
            frame=self.spark.read.format('delta').load(self.paths[table]).filter(predicate).limit(sum(map(len,fields.values()))+1)
            for k,t in SPECS[table][1]:
                if t=='t':frame=frame.withColumn(k,F.date_format(F.col(k),"yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"))
            frames.append(frame.select(F.lit(table).alias('_table'),F.to_json(F.struct(*frame.columns),{'ignoreNullFields':'false'}).alias('_row')))
        result={table:{field:{} for field in fields} for table,fields in requests.items()}
        if frames:
            combined=frames[0]
            for frame in frames[1:]:combined=combined.unionByName(frame)
            with timed('delta_batch_lookup',track_max=True):values=combined.collect()
            for value in values:
                table=value['_table'];row=parse(value['_row'])
                for field,keys in requests[table].items():
                    key=row[field]
                    if key not in keys:continue
                    if key in result[table][field]:raise Conflict('BRONZE_DELTA_DUPLICATE_KEY')
                    result[table][field][key]=row
        for table,keys in [('raw',raw_keys),('events',event_keys)]:
            self._prepared[table]=(set(keys),result[table][SPECS[table][0]])
        return result['raw']['raw_document_id'],result['events']['event_id'],result['raw']['observation_key']

    def insert(self, table, rows):
        self.lease.check(self.settings)
        prepared=self._prepared.pop(table,None)
        if not rows:return
        key=SPECS[table][0];keys={r[key] for r in rows}
        if len(keys)!=len(rows):raise Conflict('BRONZE_DELTA_DUPLICATE_KEY')
        old=prepared[1] if prepared is not None and keys<=prepared[0] else self.existing(table,keys)
        for row in rows:
            if row[key] in old and not same_content(table, old[row[key]], row):
                raise Conflict('BRONZE_DELTA_CONTENT_CONFLICT')
        rows=[row for row in rows if row[key] not in old]
        if not rows:return
        for chunk in write_chunks(rows):self._append_chunk(table,chunk)
        self.lease.check(self.settings)
        with timed('delta_storage_verification',track_max=True),timed(f'delta_storage_verification_{table}',track_max=True):
            stored=DeltaSink._newly_stored(self,table,len(rows))
            if stored is None:stored=self.existing(table,{row[key] for row in rows})
        if any(row[key] not in stored or not same_content(table,stored[row[key]],row) for row in rows):
            raise Conflict('BRONZE_DELTA_COMMIT_UNCONFIRMED')

    def _newly_stored(self,table,count):
        metadata=getattr(self,'target_metadata',None)
        if not isinstance(metadata,dict) or table not in metadata:return None
        from .metadata import read_metadata
        from ..delta_range import append_files
        from pyspark.sql import functions as F
        previous=metadata[table];current=read_metadata(self.spark,self.paths[table])
        if (current is None or current['table_id']!=previous['table_id'] or current['schema']!=self.schemas[table]
                or current['properties'].get('pipeline.targetFingerprint')!=self.settings.fingerprint
                or current['properties'].get('delta.appendOnly')!='true'):raise Conflict('BRONZE_DELTA_TARGET_CONFLICT')
        paths=append_files(self.spark,self.paths[table],previous['version'],current['version'])
        metadata[table]={k:current[k] for k in ('table_id','version')}
        if paths is None:return None
        if not paths:return {}
        frame=self.spark.read.schema(self.schemas[table]).parquet(*paths).limit(count+1)
        for name,kind in SPECS[table][1]:
            if kind=='t':frame=frame.withColumn(name,F.date_format(F.col(name),"yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"))
        values=frame.collect();key=SPECS[table][0];result={}
        if len(values)!=count:raise Conflict('BRONZE_DELTA_COMMIT_UNCONFIRMED')
        for row in values:
            value=row.asDict()
            if value[key] in result:raise Conflict('BRONZE_DELTA_DUPLICATE_KEY')
            result[value[key]]=value
        bump('delta_new_file_verifications');return result

    def _append_chunk(self, table, rows):
        from contextlib import ExitStack
        prefix=f'delta_stage_{table}'
        bump(prefix+'_input_rows',len(rows))
        with timed('delta_stage_chunk',track_max=True),timed(prefix+'_chunk',track_max=True):
            with ExitStack() as cleanup:
                return DeltaSink._append_staged_chunk(self,table,rows,cleanup)

    def _append_staged_chunk(self, table, rows, cleanup):
        return self._append_staged_chunk_inner(table,rows,cleanup)

    def _append_staged_chunk_inner(self, table, rows, cleanup):
        import tempfile
        from pathlib import Path
        from uuid import uuid4
        from urllib.parse import urlsplit
        import os,hashlib
        # Transfer a small file path through Py4J, never a source-body literal.
        # Executors read the shared object using JVM JSON readers; no Python worker.
        jpath=self.spark._jvm.org.apache.hadoop.fs.Path
        staged=jpath(self.paths[table].rstrip('/')+'/_pipeline_inputs/'+uuid4().hex+'.json')
        fs=staged.getFileSystem(self.spark.sparkContext._jsc.hadoopConfiguration())
        objects=None;object_key=None
        self.lease.check(self.settings)
        with tempfile.TemporaryDirectory(prefix='bronze-input-') as directory:
            local=Path(directory)/'rows.json'
            with timed(f'delta_stage_{table}_local_serialize',track_max=True):
                with local.open('w',encoding='utf-8',newline='\n') as stream:
                    for row in rows:stream.write(canonical(row)+'\n')
            size=local.stat().st_size
            bump(f'delta_stage_{table}_input_bytes',size)
            parsed=urlsplit(self.paths[table])
            if parsed.scheme=='s3a' and os.environ.get('PIPELINE_SPARK_S3_PROFILE')=='garage_v241':
                # S3A copyFromLocalFile's upload protocol is incompatible with
                # this Garage deployment. Use the existing tested SDK profile.
                from ..objects import S3Objects
                with timed(f'delta_stage_{table}_client_open',track_max=True):
                    objects=S3Objects(parsed.netloc,os.environ['PIPELINE_S3_ENDPOINT'],profile='garage_v241',
                        credentials_file=os.environ['PIPELINE_SPARK_S3_CREDENTIALS_FILE'])
                def close_client():
                    with timed(f'delta_stage_{table}_client_close',track_max=True):objects.client.close()
                cleanup.callback(close_client)
                object_key=urlsplit(staged.toString()).path.lstrip('/')
                with timed(f'delta_stage_{table}_local_checksum',track_max=True):
                    checksum=hashlib.sha256(local.read_bytes()).hexdigest()
                with timed(f'delta_stage_{table}_put',track_max=True):
                    with local.open('rb') as body:
                        objects.client.put_object(Bucket=parsed.netloc,Key=object_key,Body=body,ContentLength=size,
                            Metadata={'sha256':checksum},ContentType='application/x-ndjson')
                with timed(f'delta_stage_{table}_get',track_max=True):
                    stored=objects.client.get_object(Bucket=parsed.netloc,Key=object_key)
                actual=hashlib.sha256();received=0
                body=stored['Body']
                with timed(f'delta_stage_{table}_readback_verify',track_max=True):
                    try:
                        while True:
                            block=body.read(65536)
                            if not block:break
                            received+=len(block)
                            if received>size:raise PipelineError('BRONZE_STAGED_INPUT_UNCONFIRMED')
                            actual.update(block)
                    finally:
                        try:body.close()
                        finally:bump(f'delta_stage_{table}_readback_bytes',received)
                    if received!=size or actual.hexdigest()!=checksum:
                        raise PipelineError('BRONZE_STAGED_INPUT_UNCONFIRMED')
            else:
                with timed(f'delta_stage_{table}_filesystem_copy',track_max=True):
                    fs.copyFromLocalFile(False,False,jpath(local.as_uri()),staged)
        # Spark DataFrame construction is lazy; source reads run inside the save action below.
        with timed(f'delta_stage_{table}_spark_json_plan',track_max=True):
            frame=(self.spark.read.schema(self.schemas[table]).option('mode','FAILFAST')
                .option('timeZone','UTC').option('timestampFormat',"yyyy-MM-dd'T'HH:mm:ss.SSSXXX")
                .json(staged.toString()))
        self.lease.check(self.settings)
        with timed('delta_append',track_max=True),timed(f'delta_append_{table}',track_max=True):
            frame.write.format('delta').mode('append').option('mergeSchema','false').save(self.paths[table])
        bump('delta_appended_rows',len(rows))
        self.lease.check(self.settings)
        # A failed/uncertain write leaves its unique input for diagnosis/replay.
        # Delete only this attempt's input after synchronous write and ownership check.
        with timed(f'delta_stage_{table}_delete',track_max=True):
            if objects is not None:
                objects.client.delete_object(Bucket=parsed.netloc,Key=object_key)
            elif not fs.delete(staged,False):raise PipelineError('BRONZE_STAGED_INPUT_CLEANUP_FAILED')
