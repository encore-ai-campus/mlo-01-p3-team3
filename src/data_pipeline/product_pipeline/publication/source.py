"""Read a sealed Gold export at its exact Delta versions, never latest tables."""
from decimal import Decimal
from pathlib import Path
from urllib.parse import urlsplit

from ..contracts import digest
from ..errors import Conflict,PipelineError
from ..files import parse
from ..gold.contracts import TABLES
from ..gold.storage import DeltaSink
from ..bronze.config import location
from .plan import validate_group


class GoldSource:
    def __init__(self,spark,export_ref,gold_root,archive_root,*,max_group_rows=20000,max_group_bytes=32*1024*1024):
        self.spark,self.archive_root=spark,location(archive_root)
        self.max_group_rows,self.max_group_bytes=max_group_rows,max_group_bytes
        self.manifest=self.read(export_ref)
        m=self.manifest
        if (m.get('stage')!='GOLD_EXPORT' or not m.get('source_drained') or
            m.get('content_hash')!=digest({k:v for k,v in m.items() if k!='content_hash'}) or
            set(m.get('gold_versions',{}))!=set(TABLES)|{'pipeline_evaluations'} or
            any(type(v) is not int or v<0 for v in m['gold_versions'].values())):
            raise PipelineError('PUBLISH_COMPLETE_FIXED_EXPORT_REQUIRED')
        self.frames={t:spark.read.format('delta').option('versionAsOf',v).load(gold_root.rstrip('/')+'/'+t)
                     for t,v in m['gold_versions'].items()}
        policy=self.read(m['input_refs']['inputs'])['policy']
        self.price_history_mode=policy['price_history_mode'];self.price_max_age_seconds=policy['price_max_age_seconds']

    def read(self,ref):
        normalized=location(ref)
        if not normalized.startswith(self.archive_root+'/'):
            raise PipelineError('PUBLISH_ARCHIVE_REF_OUTSIDE_ROOT')
        path=self.spark._jvm.org.apache.hadoop.fs.Path(normalized)
        stream=path.getFileSystem(self.spark._jsc.hadoopConfiguration()).open(path)
        try:data=bytes(stream.readNBytes(32*1024*1024+1))
        finally:stream.close()
        if len(data)>32*1024*1024:raise PipelineError('PUBLISH_ARCHIVE_TOO_LARGE')
        value=parse(data.decode())
        if path.getName()!=digest(value)+'.json':raise Conflict('PUBLISH_ARCHIVE_HASH_CHANGED')
        return value

    def _rows(self,frame,budget):
        from pyspark.sql import functions as F
        for name in DeltaSink.times & set(frame.columns):
            frame=frame.withColumn(name,F.date_format(name,"yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"))
        values=[]
        for row in frame.toLocalIterator():
            r=row.asDict(recursive=True)
            for name in DeltaSink.decimals & set(r):
                if r[name] is not None:r[name]=format(r[name].normalize(),'f')
            budget[0]+=1;budget[1]+=len(str(r).encode())
            if budget[0]>self.max_group_rows or budget[1]>self.max_group_bytes:raise PipelineError('PUBLISH_PRODUCT_GROUP_LIMIT')
            values.append(r)
        return values

    def groups(self):
        from pyspark.sql import functions as F
        seen=set()
        for page in self.manifest['selection_pages']:
            for choice in self.read(page)['selections']:
                key=choice['product_key'];evaluation=choice['evaluation_key']
                if key in seen:raise Conflict('PUBLISH_EXPORT_DUPLICATE_PRODUCT')
                seen.add(key);budget=[0,0]
                data={t:self._rows(f.filter((F.col('product_key')==key)&(F.col('gold_evaluation_key')==evaluation)),budget)
                      for t,f in self.frames.items()}
                if len(data['pipeline_evaluations'])!=1:raise Conflict('PUBLISH_EXPORT_RECEIPT_MISSING')
                group=dict(receipt=data.pop('pipeline_evaluations')[0],tables=data)
                refs=parse(group['receipt']['input_refs_json'])
                for name in ('inputs','versions','silver_root'):
                    if refs.get(name)!=self.manifest['input_refs'].get(name):raise Conflict('PUBLISH_GOLD_INPUT_REF_CONFLICT')
                validate_group(group,self.manifest['gold_versions']);yield group
        if len(seen)!=self.manifest['product_count']:raise Conflict('PUBLISH_EXPORT_COUNT_CHANGED')

    def groups_batched(self,max_products=100):
        """Bounded pinned reads; no per-product Spark scan for every output table."""
        from pyspark.sql import functions as F
        if not 1<=max_products<=100:raise PipelineError('PUBLISH_BATCH_LIMIT')
        seen=set()
        for page in self.manifest['selection_pages']:
            choices=list(self.read(page)['selections'])
            while choices:
                size=min(max_products,len(choices))
                while True:
                    chosen=choices[:size];keys=[c['product_key'] for c in chosen];evaluations=[c['evaluation_key'] for c in chosen]
                    if len(set(keys))!=len(keys) or seen.intersection(keys):raise Conflict('PUBLISH_EXPORT_DUPLICATE_PRODUCT')
                    budget=[0,0]
                    try:
                        data={t:self._rows(frame.filter(F.col('product_key').isin(keys)&F.col('gold_evaluation_key').isin(evaluations)),budget)
                            for t,frame in self.frames.items()};break
                    except PipelineError as exc:
                        if exc.code!='PUBLISH_PRODUCT_GROUP_LIMIT' or size==1:raise
                        size=max(1,size//2)
                groups=[]
                for choice in chosen:
                    selected={t:[r for r in rows if r['product_key']==choice['product_key'] and r['gold_evaluation_key']==choice['evaluation_key']] for t,rows in data.items()}
                    if len(selected['pipeline_evaluations'])!=1:raise Conflict('PUBLISH_EXPORT_RECEIPT_MISSING')
                    group=dict(receipt=selected.pop('pipeline_evaluations')[0],tables=selected)
                    refs=parse(group['receipt']['input_refs_json'])
                    if any(refs.get(n)!=self.manifest['input_refs'].get(n) for n in ('inputs','versions','silver_root')):
                        raise Conflict('PUBLISH_GOLD_INPUT_REF_CONFLICT')
                    validate_group(group,self.manifest['gold_versions']);groups.append(group)
                seen.update(keys);choices=choices[size:]
                yield from groups
        if len(seen)!=self.manifest['product_count']:raise Conflict('PUBLISH_EXPORT_COUNT_CHANGED')
