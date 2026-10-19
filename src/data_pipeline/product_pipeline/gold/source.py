"""Pinned Silver receipts are the only Gold discovery and completeness authority."""
from ..contracts import utc
from ..errors import Conflict, PipelineError
from ..silver.schemas import TIMES, DECIMALS
from ..files import parse
from .contracts import validate_versions
from .build import validate_group


class SilverSource:
    def __init__(self,spark,settings,versions):
        validate_versions(versions)
        self.spark,self.settings,self.versions=spark,settings,versions
        self.frames={t:spark.read.format('delta').option('versionAsOf',v).load(settings.silver_root.rstrip('/')+'/'+t)
                     for t,v in versions.items()}

    def products(self,after=None):
        from pyspark.sql import functions as F
        frame=self.frames['pipeline_evaluations']
        if after:frame=frame.filter(F.col('product_key')>after)
        values=frame.select('product_key').distinct().orderBy('product_key').limit(self.settings.max_records).collect()
        if any(not r[0] for r in values):raise Conflict('GOLD_SILVER_PRODUCT_KEY_MISSING')
        return [r[0] for r in values]

    def _rows(self,frame,budget):
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
