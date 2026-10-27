"""Bounded fixed-version output proofs shared by the insert-only Delta writers."""
from decimal import Decimal
from .errors import Conflict,PipelineError
from .files import parse
from .spark_session import measured,bump,timed


@measured('delta_versions_many')
def versions_many(sink,tables):
    from delta.tables import DeltaTable
    from pyspark.sql import functions as F
    tables=list(dict.fromkeys(tables));sink.lease.check(sink.settings)
    missing=[t for t in tables if t not in sink._versions]
    if missing:
        ownership=bool(getattr(sink,'_pending_appends',{}))
        fields=['version','operation','operationParameters','userMetadata'] if ownership else ['version']
        frames=[DeltaTable.forPath(sink.spark,sink.paths[t]).history(1).select(F.lit(t).alias('_table'),*fields) for t in missing]
        combined=frames[0]
        for frame in frames[1:]:combined=combined.unionByName(frame)
        rows=combined.collect();values={r['_table']:int(r['version']) for r in rows}
        if len(rows)!=len(missing) or set(values)!=set(missing):raise Conflict('DELTA_VERSION_READ_INCOMPLETE')
        sink.lease.check(sink.settings);sink._versions.update(values)
        if ownership:
            for row in rows:
                table=row['_table'];pending=sink._pending_appends.get(table)
                if pending:
                    if (values[table]==pending['base']+1 and row['operation']=='WRITE'
                            and row['operationParameters'].get('mode')=='Append' and row['userMetadata']==pending['token']):
                        pending['version']=values[table]
                    else:sink._pending_appends.pop(table,None)
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
    from pyspark.sql import functions as F
    from .bounded_read import payload_rows
    wanted={(t,v):e for t,v,e in requests};packed=None
    bump('delta_'+purpose+'_version_requests',len(wanted))
    bump('delta_'+purpose+'_requested_rows',sum(len(expected) for _,_,expected in requests))
    for table,version,expected in requests:
        bump('delta_'+purpose+'_requested_rows_'+table,len(expected))
    with timed('delta_'+purpose+'_read_plan'):
        for table,version,expected in requests:
            frame=(sink.spark.read.format('delta').option('versionAsOf',version).load(sink.paths[table])
                .filter(F.col(sink.keys[table]).isin(sorted(expected))).limit(len(expected)+1))
            for name in sink.times & set(frame.columns):frame=frame.withColumn(name,F.date_format(name,"yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"))
            for name in sink.decimals & set(frame.columns):frame=frame.withColumn(name,F.col(name).cast('string'))
            part=frame.select(F.lit(table).alias('_table'),F.lit(version).alias('_version'),
                F.to_json(F.struct('*'),{'ignoreNullFields':'false'}).alias('payload'))
            packed=part if packed is None else packed.unionByName(part)
    try:
        seen={identity:{} for identity in wanted};bodies={identity:{} for identity in wanted}
        with timed('delta_'+purpose+'_read_validate'):
            for record in payload_rows(packed,max_rows=sum(len(e) for _,_,e in requests),
                    max_bytes=32*1024*1024,error_code='DELTA_PROOF_BUDGET'):
                identity=(record['_table'],record['_version']);table=identity[0];payload=record['payload']
                bump('delta_'+purpose+'_returned_rows_'+table)
                bump('delta_'+purpose+'_payload_chars_'+table,len(payload))
                row=parse(payload)
                for name in sink.decimals & set(row):
                    if row[name] is not None:row[name]=format(Decimal(row[name]).normalize(),'f')
                key=row[sink.keys[table]]
                if key in seen[identity]:raise Conflict('SILVER_DUPLICATE_KEY')
                if row['record_hash']!=sink.hash_row(row):raise Conflict('SILVER_STORED_HASH_CONFLICT')
                seen[identity][key]=row['record_hash']
                bodies[identity][key]=row
        if require_all:
            if any(seen[k]!=expected for k,expected in wanted.items()):raise Conflict('SILVER_OUTPUT_MISSING_OR_CHANGED')
        elif any(key not in wanted[identity] or wanted[identity][key]!=h for identity,values in seen.items() for key,h in values.items()):
            raise Conflict('SILVER_OUTPUT_CONFLICT')
        bump('delta_'+purpose+'_union_batches');bump('delta_'+purpose+'_reads',len(requests))
        if require_all:
            for identity,rows in bodies.items():
                table,version=identity;pending=getattr(sink,'_pending_appends',{}).get(table)
                if pending and pending.get('version')==version:
                    sink._append_reads.setdefault(identity,{}).update(rows)
        return seen
    except PipelineError as exc:
        if exc.code!='DELTA_PROOF_BUDGET':raise
        bump('delta_'+purpose+'_budget_retries')
        # No partial result is accepted. Retry bounded subrequests afresh.
        if len(requests)>1:
            half=len(requests)//2;parts=[requests[:half],requests[half:]]
        else:
            table,version,expected=requests[0];pairs=sorted(expected.items())
            if len(pairs)<=1:raise PipelineError('DELTA_PROOF_ROW_TOO_LARGE') from None
            half=len(pairs)//2;parts=[[(table,version,dict(pairs[:half]))],[(table,version,dict(pairs[half:]))]]
        merged={}
        for part in parts:
            for identity,values in _verify_rows(sink,part,require_all=require_all,purpose=purpose).items():
                merged.setdefault(identity,{}).update(values)
        return merged


def _shared_key(sink,table,version):
    meta=getattr(sink,'target_metadata',{}).get(table)
    if not meta:return None
    return (sink.paths[table],meta['table_id'],version)


def _known(sink,table,version):
    from .spark_session import cache_get
    result=dict(getattr(sink,'_proofs',{}).get((table,version),{}))
    key=_shared_key(sink,table,version)
    if key is not None:result.update(cache_get('delta_verified_rows',key) or {})
    return result


def _remember(sink,requests):
    from .spark_session import cache_put
    if not hasattr(sink,'_proofs'):sink._proofs={}
    for table,version,expected in requests:
        saved=_known(sink,table,version);saved.update(expected)
        if len(saved)>10000:saved=dict(expected)
        sink._proofs[(table,version)]=saved
        key=_shared_key(sink,table,version)
        if key is not None:cache_put('delta_verified_rows',key,saved)
    if sum(len(p) for p in sink._proofs.values())>10000:sink._proofs.clear()


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
            known=_known(sink,table,v)
            missing={k:h for k,h in expected.items() if known.get(k)!=h}
            bump('delta_proof_reused_rows',len(expected)-len(missing))
            if not missing:
                bump('delta_proof_reuses');continue
            target=requests.setdefault((table,v),{})
            if any(k in target and target[k]!=h for k,h in missing.items()):raise Conflict('DELTA_PROOF_EXPECTATION_CONFLICT')
            target.update(missing)
    sink._append_reads={}
    try:
        for batch in _chunks([(t,v,e) for (t,v),e in requests.items()]):_verify_rows(sink,batch)
        sink.lease.check(sink.settings)
        _remember(sink,[(t,v,e) for (t,v),e in requests.items()])
        promote_appends(sink)
    finally:sink._append_reads={}


def promote_appends(sink):
    from .silver.append_bridge import record_append
    for table,pending in list(getattr(sink,'_pending_appends',{}).items()):
        version=pending.get('version');rows=sink._append_reads.get((table,version),{})
        if not all(k in rows and rows[k]['record_hash']==h for k,h in pending['expected'].items()):continue
        meta=sink.target_metadata[table]
        from .delta_key_index import advance
        advance(sink,table,pending['base'],version,pending['expected'])
        earlier=_known(sink,table,pending['base'])
        if earlier:
            current=_known(sink,table,version)
            if any(k in current and current[k]!=h for k,h in earlier.items()):raise Conflict('DELTA_APPEND_PROOF_CONFLICT')
            _remember(sink,[(table,version,earlier)])
            bump('delta_proof_forwarded_rows',len(earlier))
        if getattr(sink,'supports_history_bridge',True):
            record_append(sink.paths[table],meta['table_id'],pending['base'],version,
                [rows[k] for k in pending['expected']])
        sink._pending_appends.pop(table,None)


@measured('delta_existing_union_read')
def existing_hashes_many(sink,expected):
    """One invocation-local prewrite lookup; missing rows are never cached."""
    current=sink.versions_many([t for t,e in expected.items() if e])
    from .delta_key_index import seed,candidates
    if getattr(sink,'target_metadata',{}) and hasattr(sink,'spark'):
        all_versions=sink.versions_many(sink.paths)
        seed(sink,all_versions)
    result={t:{} for t in expected}
    if not hasattr(sink,'_proofs'):sink._proofs={}
    remaining={}
    for t,values in expected.items():
        if not values:continue
        known=_known(sink,t,current[t]);result[t]={k:h for k,h in values.items() if known.get(k)==h}
        pending={k:h for k,h in values.items() if k not in result[t]}
        selected=candidates(sink,t,current[t],pending)
        remaining[t]={k:pending[k] for k in selected}
        bump('delta_existing_reused_rows',len(result[t]))
    verified=[]
    for batch in _chunks([(t,current[t],e) for t,e in remaining.items() if e]):
        found=_verify_rows(sink,batch,require_all=False,purpose='existing')
        for (table,version),values in found.items():
            result[table].update(values);verified.append((table,version,values))
    sink.lease.check(sink.settings)
    _remember(sink,verified)
    return result
