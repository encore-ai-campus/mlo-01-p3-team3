"""Bounded fixed-version output proofs shared by the insert-only Delta writers."""
from decimal import Decimal
from .errors import Conflict,PipelineError
from .files import parse
from .spark_session import measured,bump


@measured('delta_versions_many')
def versions_many(sink,tables):
    from delta.tables import DeltaTable
    from pyspark.sql import functions as F
    tables=list(dict.fromkeys(tables));sink.lease.check(sink.settings)
    missing=[t for t in tables if t not in sink._versions]
    if missing:
        frames=[DeltaTable.forPath(sink.spark,sink.paths[t]).history(1).select(F.lit(t).alias('_table'),'version') for t in missing]
        combined=frames[0]
        for frame in frames[1:]:combined=combined.unionByName(frame)
        rows=combined.collect();values={r['_table']:int(r['version']) for r in rows}
        if len(rows)!=len(missing) or set(values)!=set(missing):raise Conflict('DELTA_VERSION_READ_INCOMPLETE')
        sink.lease.check(sink.settings);sink._versions.update(values)
        bump('delta_version_reads',len(missing));bump('delta_version_union_batches')
    return {t:sink._versions[t] for t in tables}


def _chunks(requests,max_keys=5000):
    batch=[];size=0
    for table,version,expected in requests:
        pairs=sorted(expected.items())
        for offset in range(0,len(pairs),max_keys):
            part=dict(pairs[offset:offset+max_keys])
            if batch and size+len(part)>max_keys:yield batch;batch=[];size=0
            batch.append((table,version,part));size+=len(part)
    if batch:yield batch


def _verify_rows(sink,requests,*,require_all=True,purpose='proof'):
    from pyspark import StorageLevel
    from pyspark.sql import functions as F
    wanted={(t,v):e for t,v,e in requests};packed=None
    for table,version,expected in requests:
        frame=(sink.spark.read.format('delta').option('versionAsOf',version).load(sink.paths[table])
            .filter(F.col(sink.keys[table]).isin(sorted(expected))).limit(len(expected)+1))
        for name in sink.times & set(frame.columns):frame=frame.withColumn(name,F.date_format(name,"yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"))
        for name in sink.decimals & set(frame.columns):frame=frame.withColumn(name,F.col(name).cast('string'))
        part=frame.select(F.lit(table).alias('_table'),F.lit(version).alias('_version'),
            F.to_json(F.struct('*'),{'ignoreNullFields':'false'}).alias('payload'))
        packed=part if packed is None else packed.unionByName(part)
    cached=packed.persist(StorageLevel.DISK_ONLY)
    try:
        stats=cached.agg(F.count('*').alias('count'),F.sum(F.length(F.encode('payload','UTF-8'))).alias('bytes')).first()
        if stats['count']>sum(len(e) for _,_,e in requests):raise Conflict('SILVER_OUTPUT_MISSING_OR_CHANGED')
        if (stats['bytes'] or 0)>32*1024*1024:
            # Split the request, never discard proof rows or weaken the hash check.
            cached.unpersist()
            if len(requests)>1:
                half=len(requests)//2;parts=[requests[:half],requests[half:]]
            else:
                table,version,expected=requests[0];pairs=sorted(expected.items())
                if len(pairs)==1:raise PipelineError('DELTA_PROOF_ROW_TOO_LARGE')
                half=len(pairs)//2;parts=[[(table,version,dict(pairs[:half]))],[(table,version,dict(pairs[half:]))]]
            merged={}
            for part in parts:
                for identity,values in _verify_rows(sink,part,require_all=require_all,purpose=purpose).items():
                    merged.setdefault(identity,{}).update(values)
            return merged
        seen={identity:{} for identity in wanted}
        for record in cached.collect():
            identity=(record['_table'],record['_version']);table=identity[0];row=parse(record['payload'])
            for name in sink.decimals & set(row):
                if row[name] is not None:row[name]=format(Decimal(row[name]).normalize(),'f')
            key=row[sink.keys[table]]
            if key in seen[identity]:raise Conflict('SILVER_DUPLICATE_KEY')
            if row['record_hash']!=sink.hash_row(row):raise Conflict('SILVER_STORED_HASH_CONFLICT')
            seen[identity][key]=row['record_hash']
        if require_all:
            if any(seen[k]!=expected for k,expected in wanted.items()):raise Conflict('SILVER_OUTPUT_MISSING_OR_CHANGED')
        elif any(key not in wanted[identity] or wanted[identity][key]!=h for identity,values in seen.items() for key,h in values.items()):
            raise Conflict('SILVER_OUTPUT_CONFLICT')
        bump('delta_'+purpose+'_union_batches');bump('delta_'+purpose+'_reads',len(requests))
        return seen
    finally:cached.unpersist()


@measured('delta_proof_union_read')
def verify_many(sink,proofs):
    """Verify historical and current snapshots before accepting any new proof."""
    proofs=list(proofs)
    if not proofs:return
    sink.lease.check(sink.settings)
    current=sink.versions_many([t for t,_,_ in proofs])
    if not hasattr(sink,'_proofs'):sink._proofs={}
    requests={}
    for table,version,expected in proofs:
        for v in dict.fromkeys([version,current[table]]):
            if all(sink._proofs.get((table,v),{}).get(k)==h for k,h in expected.items()):
                bump('delta_proof_reuses');continue
            target=requests.setdefault((table,v),{})
            if any(k in target and target[k]!=h for k,h in expected.items()):raise Conflict('DELTA_PROOF_EXPECTATION_CONFLICT')
            target.update(expected)
    for batch in _chunks([(t,v,e) for (t,v),e in requests.items()]):_verify_rows(sink,batch)
    sink.lease.check(sink.settings)
    for identity,expected in requests.items():sink._proofs.setdefault(identity,{}).update(expected)
    if sum(len(p) for p in sink._proofs.values())>10000:sink._proofs.clear()


@measured('delta_existing_union_read')
def existing_hashes_many(sink,expected):
    """One invocation-local prewrite lookup; missing rows are never cached."""
    current=sink.versions_many([t for t,e in expected.items() if e])
    result={t:{} for t in expected}
    for batch in _chunks([(t,current[t],e) for t,e in expected.items() if e]):
        found=_verify_rows(sink,batch,require_all=False,purpose='existing')
        for (table,_),values in found.items():result[table].update(values)
    sink.lease.check(sink.settings)
    return result
