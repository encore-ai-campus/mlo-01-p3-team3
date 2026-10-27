"""Disposable full-query snapshots plus verified, owned append-only edges.

No edge is inferred from a version number alone. The writer must identify its
own successful Delta transaction AND read back all appended row hashes.
"""
from ..spark_session import cache_get,cache_put,bump,measured
from ..contracts import canonical
from ..errors import Conflict,PipelineError
from .normalize import KEYS,semantic_hash


def record_append(path,table_id,base,version,rows):
    if version!=base+1 or not rows:return
    cache_put('silver_append_rows',(path,table_id,version),dict(base=base,rows=rows))
    bump('silver_owned_append_edges')


def snapshot_key(root,keys,max_rows,max_bytes):
    return (root.rstrip('/'),tuple(sorted(keys)),max_rows,max_bytes)


@measured('silver_snapshot_remember')
def remember(root,keys,versions,identities,tables,*,max_rows,max_bytes):
    if set(versions)!=set(identities) or set(tables)!=set(versions):return
    cache_put('silver_tables',snapshot_key(root,keys,max_rows,max_bytes),
        dict(versions=versions,identities=identities,tables=tables))
    cache_put('silver_table_scope',(root.rstrip('/'),max_rows,max_bytes),sorted(keys))


@measured('silver_snapshot_bridge')
def restore(root,keys,versions,identities,*,max_rows,max_bytes):
    saved=cache_get('silver_tables',snapshot_key(root,keys,max_rows,max_bytes))
    if saved is None:
        prior=cache_get('silver_table_scope',(root.rstrip('/'),max_rows,max_bytes))
        if prior and set(keys)<=set(prior):saved=cache_get('silver_tables',snapshot_key(root,prior,max_rows,max_bytes))
    if saved is None or saved['identities']!=identities or set(saved['versions'])!=set(versions):return None
    additions={};steps=0
    for table,target in versions.items():
        base=saved['versions'][table];version=target;edges=[]
        if target<base:return None
        while version>base:
            edge=cache_get('silver_append_rows',(root.rstrip('/')+'/'+table,identities[table],version))
            if edge is None or edge['base']!=version-1 or steps>=64:return None
            edges.append(edge['rows']);version=edge['base'];steps+=1
        additions[table]=[row for batch in reversed(edges) for row in batch if row['product_key'] in keys]
    tables={t:[r for r in rows if r['product_key'] in keys] for t,rows in saved['tables'].items()}
    for table,rows in additions.items():
        seen={r[KEYS[table]] for r in tables[table]}
        for row in rows:
            key=row[KEYS[table]]
            if key in seen or row['record_hash']!=semantic_hash(row):raise Conflict('SILVER_APPEND_BRIDGE_CONFLICT')
            seen.add(key);tables[table].append(row)
    count=sum(len(rows) for rows in tables.values())
    size=sum(len(canonical(row).encode('utf-8')) for rows in tables.values() for row in rows)
    if count>max_rows or size>max_bytes:raise PipelineError('GOLD_PRODUCT_HISTORY_LIMIT')
    bump('silver_snapshot_bridge_reuses');bump('silver_snapshot_bridge_edges',steps)
    bump('silver_snapshot_bridge_rows',sum(map(len,additions.values())))
    return tables
