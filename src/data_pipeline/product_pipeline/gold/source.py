"""Pinned Silver receipts are the only Gold discovery and completeness authority."""
from ..contracts import utc
from ..errors import Conflict, PipelineError
from ..silver.schemas import TIMES, DECIMALS
from ..files import parse
from .contracts import validate_versions
from .build import validate_group
from ..spark_session import bump,measured,timed


class SilverSource:
    @measured('gold_silver_source_open')
    def __init__(self,spark,settings,versions):
        validate_versions(versions)
        self.spark,self.settings,self.versions=spark,settings,versions
        self.identities={}
        from ..bronze.metadata import read_metadata
        try:
            with timed('gold_silver_metadata_read'):
                for table,version in versions.items():
                    meta=read_metadata(spark,settings.silver_root.rstrip('/')+'/'+table)
                    if meta is None or meta['version']<version:break
                    self.identities[table]=meta['table_id']
        except PipelineError:self.identities={}
        if set(self.identities)!=set(versions):self.identities={}
        with timed('gold_silver_frame_open'):
            self.frames={t:spark.read.format('delta').option('versionAsOf',v).load(settings.silver_root.rstrip('/')+'/'+t)
                         for t,v in versions.items()}

    def restrict_products(self, keys):
        from pyspark.sql import functions as F
        if not keys or len(keys)>100 or len(set(keys))!=len(keys):raise PipelineError('GOLD_BATCH_LIMIT')
        self.frames={t:f.filter(F.col('product_key').isin(keys)) for t,f in self.frames.items()}
        self._product_keys=sorted(keys)

    def products(self,after=None):
        if hasattr(self,'_product_keys'):
            # The window already freezes discovery. groups_many still requires
            # complete, valid receipts for every key before any Gold write.
            bump('silver_discovery_reuses')
            return [key for key in self._product_keys if after is None or key>after][:self.settings.max_records]
        from pyspark.sql import functions as F
        frame=self.frames['pipeline_evaluations']
        if after:frame=frame.filter(F.col('product_key')>after)
        with timed('gold_silver_product_discovery'):
            values=frame.select('product_key').distinct().orderBy('product_key').limit(self.settings.max_records).collect()
        bump('gold_silver_discovered_products',len(values))
        if any(not r[0] for r in values):raise Conflict('GOLD_SILVER_PRODUCT_KEY_MISSING')
        return [r[0] for r in values]

    def _rows(self,frame,budget):
        from pyspark import StorageLevel
        cached=frame.persist(StorageLevel.DISK_ONLY)
        try:return self._cached_rows(cached,budget)
        finally:cached.unpersist()

    def _cached_rows(self,frame,budget):
        from ..spark_session import bump
        bump('silver_table_read_batches')
        from pyspark.sql import functions as F
        stats=frame.select(F.length(F.encode(F.to_json(F.struct('*')),'UTF-8')).alias('bytes')).agg(
            F.count('*').alias('count'),F.sum('bytes').alias('bytes')).first()
        budget[0]+=stats['count'];budget[1]+=stats['bytes'] or 0
        if budget[0]>self.settings.max_product_rows or budget[1]>self.settings.max_product_bytes:
            raise PipelineError('GOLD_PRODUCT_HISTORY_LIMIT')
        for name in TIMES & set(frame.columns):
            frame=frame.withColumn(name,F.date_format(F.col(name),"yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"))
        rows=[]
        for record in frame.collect():
            row=record.asDict(recursive=True)
            for name in DECIMALS & set(row):
                if row[name] is not None:row[name]=format(row[name].normalize(),'f')
            rows.append(row)
        return rows

    def groups(self,product_key):
        from pyspark.sql import functions as F
        from ..silver.normalize import KEYS
        budget=[0,0]
        receipts=self._rows(self.frames['pipeline_evaluations'].filter(F.col('product_key')==product_key),budget)
        if len({r['evaluation_key'] for r in receipts})!=len(receipts):raise Conflict('GOLD_DUPLICATE_SILVER_RECEIPT')
        groups=[]
        # Bound reads by product, then validate every receipt's exact output membership.
        children={t:self._rows(f.filter(F.col('product_key')==product_key),budget)
                  for t,f in self.frames.items() if t!='pipeline_evaluations'}
        for receipt in receipts:
            manifest=parse(receipt['output_manifest_json'])
            tables={}
            for table,proof in manifest.items():
                if table not in children or type(proof.get('version')) is not int or not 0<=proof['version']<=self.versions[table]:
                    raise Conflict('GOLD_SILVER_VERSION_INCOMPLETE')
                tables[table]=[r for r in children[table] if r['evaluation_key']==receipt['evaluation_key']]
            group=dict(receipt=receipt,tables=tables);validate_group(group);groups.append(group)
        return groups

    @measured('gold_silver_groups_many')
    def groups_many(self,product_keys):
        """Bound aggregate bytes/rows before collecting any requested table."""
        from pyspark.sql import functions as F
        if not product_keys or len(product_keys)>100 or len(set(product_keys))!=len(product_keys):
            raise PipelineError('GOLD_BATCH_LIMIT')
        from ..spark_session import cache_get,cache_put
        cache_key=(self.settings.silver_root,tuple(sorted(self.versions.items())),tuple(product_keys),
                   self.settings.max_product_rows,self.settings.max_product_bytes)
        with timed('gold_silver_group_cache_lookup'):
            cached=cache_get('silver_groups',cache_key)
        if cached is not None:
            bump('gold_silver_group_cache_hits')
            return cached
        bump('gold_silver_group_cache_misses')
        tables=self._tables_many(product_keys);complete=tables.copy()
        receipts=tables.pop('pipeline_evaluations')
        if len({r['evaluation_key'] for r in receipts})!=len(receipts):raise Conflict('GOLD_DUPLICATE_SILVER_RECEIPT')
        children={}
        for table,rows in tables.items():
            index={}
            for row in rows:index.setdefault(row['evaluation_key'],[]).append(row)
            children[table]=index
        output={key:[] for key in product_keys}
        with timed('gold_silver_receipt_validation'):
            for receipt in receipts:
                tables={}
                for table,proof in parse(receipt['output_manifest_json']).items():
                    if table not in children or type(proof.get('version')) is not int or not 0<=proof['version']<=self.versions[table]:
                        raise Conflict('GOLD_SILVER_VERSION_INCOMPLETE')
                    tables[table]=children[table].get(receipt['evaluation_key'],[])
                group=dict(receipt=receipt,tables=tables);validate_group(group);output[receipt['product_key']].append(group)
        bump('gold_silver_receipts_validated',len(receipts))
        if any(not output[key] for key in product_keys):raise Conflict('GOLD_SILVER_PRODUCT_KEY_MISSING')
        result=[output[key] for key in product_keys]
        from ..silver.append_bridge import remember
        with timed('gold_silver_append_bridge_store'):
            remember(self.settings.silver_root,product_keys,self.versions,getattr(self,'identities',{}),complete,
                max_rows=self.settings.max_product_rows,max_bytes=self.settings.max_product_bytes)
        with timed('gold_silver_group_cache_store'):
            cache_put('silver_groups',cache_key,result)
        bump('gold_silver_groups_built',len(product_keys))
        return result

    @measured('gold_silver_delta_read')
    def _tables_many(self,product_keys):
        """Stream a bounded pinned union; no temporary persisted DataFrame."""
        from decimal import Decimal
        import time
        from ..bounded_read import payload_rows
        from pyspark.sql import functions as F
        from ..silver.append_bridge import restore
        if getattr(self,'identities',{}):
            with timed('gold_silver_append_bridge_restore'):
                restored=restore(self.settings.silver_root,product_keys,self.versions,self.identities,
                    max_rows=self.settings.max_product_rows,max_bytes=self.settings.max_product_bytes)
            if restored is not None:
                bump('gold_silver_append_bridge_rows',sum(len(rows) for rows in restored.values()))
                return restored
        packed=None
        with timed('gold_silver_table_read_plan'):
            for table,frame in self.frames.items():
                frame=frame.filter(F.col('product_key').isin(product_keys))
                for name in TIMES & set(frame.columns):
                    frame=frame.withColumn(name,F.date_format(F.col(name),"yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"))
                for name in DECIMALS & set(frame.columns):frame=frame.withColumn(name,F.col(name).cast('string'))
                part=frame.select(F.lit(table).alias('table'),
                    F.to_json(F.struct('*'),{'ignoreNullFields':'false'}).alias('payload'))
                packed=part if packed is None else packed.unionByName(part)
        started=time.monotonic()
        try:
            bump('silver_union_read_batches')
            output={t:[] for t in self.frames}
            for record in payload_rows(packed,max_rows=self.settings.max_product_rows,
                    max_bytes=self.settings.max_product_bytes,error_code='GOLD_PRODUCT_HISTORY_LIMIT'):
                table=record['table'];payload=record['payload']
                bump('gold_silver_input_rows')
                bump('gold_silver_input_rows_'+table)
                bump('gold_silver_input_payload_chars_'+table,len(payload))
                row=parse(payload)
                for name in DECIMALS & set(row):
                    if row[name] is not None:row[name]=format(Decimal(row[name]).normalize(),'f')
                output[table].append(row)
            return output
        finally:
            elapsed=round((time.monotonic()-started)*1000)
            bump('silver_union_read_ms',elapsed);bump('silver_union_read_calls')


def iter_groups(source):
    """Reuse table reads across an entire bounded page, including owner export."""
    after=None
    while True:
        keys=source.products(after)
        if not keys:return
        while True:
            try:groups=source.groups_many(keys);break
            except PipelineError as exc:
                if exc.code!='GOLD_PRODUCT_HISTORY_LIMIT' or len(keys)==1:raise
                keys=keys[:max(1,len(keys)//2)]
        for group in groups:yield from group
        after=keys[-1]


def bounded_groups(source, max_bytes=32*1024*1024):
    """Materialize a window before DB review reads, with a total Driver budget."""
    from ..contracts import canonical
    result=[];size=0
    for group in iter_groups(source):
        size+=len(canonical(group).encode('utf-8'))
        if size>max_bytes:raise PipelineError('GOLD_PRODUCT_HISTORY_LIMIT')
        result.append(group)
    return result
