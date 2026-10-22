"""Immutable v2 Delta tables. Scoped key reads and one append per table/batch."""
from ..domain.contracts import canonical, digest, check_rows, seal, verified, Invalid


class DeltaLake:
    def __init__(self, spark, root, lease, max_read_bytes=64*1024*1024, read_only=False,timing=None):
        self.spark, self.root, self.lease = spark, root.rstrip('/'), lease
        self.timing=timing
        self.identity = dict(kind='DELTA_V2', root=self.root)
        self.max_read_bytes=max_read_bytes
        self.read_only=read_only
        self._opened=set(); self._verified=set()
        self.metrics=dict(read_rows=0,written_rows=0,commits=0)

    def _path(self, table):
        if not table.replace('_','').isalnum(): raise Invalid('TABLE_NAME_INVALID')
        return self.root+'/'+table

    def _open(self, table):
        if table in self._opened:return
        scope=self.timing.active_scope or 'runtime' if self.timing else None
        if self.timing:
            with self.timing.measure('delta.open_or_validate_table',scope):return self._open_table(table)
        return self._open_table(table)

    def _open_table(self,table):
        from delta.tables import DeltaTable
        from pyspark.sql.types import StructType,StructField,StringType
        path=self._path(table)
        self.lease.check()
        schema=StructType([StructField(k,StringType(),False) for k in ('row_key','product_key','payload_json','record_hash')])
        if not self.read_only:
            (DeltaTable.createIfNotExists(self.spark).location(path).addColumns(schema)
                .property('delta.appendOnly','true').property('pipeline.v2.target',digest(self.identity)).execute())
        details=DeltaTable.forPath(self.spark,path).detail().first().asDict(recursive=True)
        if (details['properties'].get('pipeline.v2.target')!=digest(self.identity)
                or details['properties'].get('delta.appendOnly')!='true'
                or self.spark.read.format('delta').load(path).schema.simpleString()!=schema.simpleString()):
            raise Invalid('DELTA_TARGET_CONFLICT')
        self._opened.add(table)

    def _version(self, table):
        from delta.tables import DeltaTable
        return int(DeltaTable.forPath(self.spark,self._path(table)).history(1).select('version').first()[0])

    def read_keys(self, table, keys, version=None):
        if not keys:return {}
        if self.timing:
            scope=self.timing.active_scope or 'runtime'
            with self.timing.measure('delta.read_keys',scope,counters=self.metrics):return self._read_keys(table,keys,version)
        return self._read_keys(table,keys,version)

    def _read_keys(self, table, keys, version=None):
        import json
        from pyspark.sql import functions as F
        if not keys:return {}
        self._open(table);self.lease.check()
        reader=self.spark.read.format('delta')
        if version is not None:reader=reader.option('versionAsOf',version)
        frame=reader.load(self._path(table)).filter(F.col('row_key').isin(sorted(set(keys)))).limit(len(set(keys))+1)
        result={};amount=0
        for value in frame.toLocalIterator():
            amount+=len(value['payload_json'].encode())
            if amount>self.max_read_bytes:raise Invalid('DELTA_READ_BYTE_LIMIT')
            item=dict(row_key=value['row_key'],product_key=value['product_key'],
                payload=json.loads(value['payload_json']),record_hash=value['record_hash'])
            check_rows([item]);key=item['row_key']
            if key in result:raise Invalid('DELTA_DUPLICATE_KEY')
            result[key]=item
        self.metrics['read_rows']+=len(result)
        self.lease.check()
        return result

    def commit(self, table, rows):
        if self.read_only:raise Invalid('READ_ONLY_LAKE')
        if self.timing:
            scope=self.timing.active_scope or 'runtime'
            with self.timing.measure('delta.commit_and_verify',scope,counters=self.metrics):return self._commit(table,rows)
        return self._commit(table,rows)

    def _commit(self, table, rows):
        if self.read_only:raise Invalid('READ_ONLY_LAKE')
        from pyspark.sql.types import StructType,StructField,StringType
        rows=check_rows(rows);self._open(table)
        old=self.read_keys(table,[r['row_key'] for r in rows])
        pending=[]
        for item in rows:
            if item['row_key'] in old:
                if item!=old[item['row_key']]:raise Invalid('DELTA_IMMUTABLE_CONFLICT')
            else:pending.append(item)
        if pending:
            self.lease.check()
            schema=StructType([StructField(k,StringType(),False) for k in ('row_key','product_key','payload_json','record_hash')])
            frame=self.spark.createDataFrame([(r['row_key'],r['product_key'],canonical(r['payload']),r['record_hash']) for r in pending],schema)
            frame.write.format('delta').mode('append').option('mergeSchema','false').save(self._path(table))
            self.lease.check();self.metrics['commits']+=1;self.metrics['written_rows']+=len(pending)
        return seal(dict(table=table,version=self._version(table),records={r['row_key']:r['record_hash'] for r in rows},lake=self.identity))

    def verify(self, proof):
        if self.timing:
            scope=self.timing.active_scope or 'runtime'
            with self.timing.measure('delta.verify_proof',scope,counters=self.metrics):return self._verify(proof)
        return self._verify(proof)

    def _verify(self,proof):
        verified(proof)
        if proof['lake']!=self.identity:raise Invalid('DELTA_PROOF_TARGET_CHANGED')
        # A proof is reusable only inside this single latched writer session, at a fixed version.
        key=proof['content_hash']
        self.lease.check()
        if key not in self._verified:
            actual=self.read_keys(proof['table'],list(proof['records']),proof['version'])
            if {k:v['record_hash'] for k,v in actual.items()}!=proof['records']:raise Invalid('DELTA_PROOF_MISSING')
            self._verified.add(key)
            if len(self._verified)>4096:self._verified={key}

    def close(self): pass
