"""Disposable complete-key Bloom filter: negatives only, never a row proof.

Seed the entire bounded fixed snapshot, then advance only through an owned,
physically verified adjacent append. A missing/gapped entry uses normal reads.
"""
from hashlib import sha256
from .spark_session import cache_get,cache_put,bump,measured
from .errors import PipelineError
from pathlib import Path
import os,json
from .contracts import digest
from .files import atomic_json

BYTES=128*1024
MAX_KEYS=500000


def _directory(key):
    value=os.environ.get('PIPELINE_DELTA_KEY_INDEX_DIR')
    if not value:return None
    root=Path(value)
    if root.is_symlink():return None
    root.mkdir(parents=True,exist_ok=True,mode=0o700)
    if os.name!='nt' and (root.stat().st_uid!=os.getuid() or root.stat().st_mode&0o077):return None
    directory=root/digest(key[:2]);directory.mkdir(exist_ok=True,mode=0o700)
    if directory.is_symlink():return None
    return directory


def _load(key):
    try:
        directory=_directory(key)
        if directory is None:return None
        path=directory/(str(key[2])+'.json')
        if path.is_symlink() or path.stat().st_size>BYTES*2+4096:return None
        if os.name!='nt' and (path.stat().st_uid!=os.getuid() or path.stat().st_mode&0o077):return None
        value=json.loads(path.read_text(encoding='utf-8'))
        body={k:value[k] for k in ('contract_version','identity','bits')}
        if body['contract_version']!=1 or body['identity']!=list(key) or digest(body)!=value['checksum']:return None
        if len(bytes.fromhex(body['bits']))!=BYTES:return None
        bump('delta_key_index_persistent_hits');return {'bits':body['bits']}
    except (OSError,ValueError,KeyError,TypeError):return None


def _save(key,data):
    try:
        directory=_directory(key)
        if directory is None:return
        body=dict(contract_version=1,identity=list(key),bits=data['bits'])
        path=directory/(str(key[2])+'.json');atomic_json(path,dict(body,checksum=digest(body)));path.chmod(0o600)
        # Retain at most two complete versions per path/table identity.
        older=sorted((p for p in directory.glob('*.json') if p.stem.isdigit()),key=lambda p:int(p.stem),reverse=True)
        for old in older[2:]:old.unlink()
        bump('delta_key_index_persistent_writes')
    except (OSError,ValueError,TypeError):bump('delta_key_index_persistent_write_failures')


def positions(key):
    value=sha256(key.encode('utf-8')).digest()
    return [int.from_bytes(value[i:i+4],'big')%(BYTES*8) for i in (0,4,8,12)]


def add(bits,key):
    for n in positions(key):bits[n//8]|=1<<(n%8)


def possible(bits,key):return all(bits[n//8]&(1<<(n%8)) for n in positions(key))


def identity(sink,table,version):
    meta=getattr(sink,'target_metadata',{}).get(table)
    if not meta:return None
    return (sink.paths[table],meta['table_id'],version)


def get(sink,table,version):
    key=identity(sink,table,version)
    if key is None:return None
    data=cache_get('delta_key_index',key)
    if data is None:
        data=_load(key)
        if data is None:return None
        cache_put('delta_key_index',key,data)
    bits=bytes.fromhex(data['bits'])
    return bits if len(bits)==BYTES else None


def put(sink,table,version,bits):
    key=identity(sink,table,version)
    if key is not None:
        data=dict(bits=bytes(bits).hex());cache_put('delta_key_index',key,data);_save(key,data)


def candidates(sink,table,version,keys):
    bits=get(sink,table,version)
    if bits is None:return set(keys)
    selected={key for key in keys if possible(bits,key)}
    bump('delta_key_index_absent_keys',len(keys)-len(selected))
    return selected


@measured('delta_key_index_seed')
def seed(sink,versions):
    from .spark_session import scope_active
    if not scope_active():return
    missing=[t for t,v in versions.items() if get(sink,t,v) is None]
    if not missing:return
    from pyspark.sql import functions as F
    from .bounded_read import payload_rows
    packed=None
    for table in missing:
        part=(sink.spark.read.format('delta').option('versionAsOf',versions[table]).load(sink.paths[table])
            .select(F.lit(table).alias('_table'),F.col(sink.keys[table]).alias('payload')).limit(MAX_KEYS+1))
        packed=part if packed is None else packed.unionByName(part)
    bits={t:bytearray(BYTES) for t in missing}
    try:
        count=0
        for row in payload_rows(packed,max_rows=MAX_KEYS,max_bytes=64*1024*1024,error_code='DELTA_KEY_INDEX_BUDGET'):
            add(bits[row['_table']],row['payload']);count+=1
    except PipelineError as exc:
        if exc.code!='DELTA_KEY_INDEX_BUDGET':raise
        bump('delta_key_index_budget_fallbacks');return
    sink.lease.check(sink.settings)
    # Nothing is published if the complete snapshot read or lease check failed.
    for table in missing:put(sink,table,versions[table],bits[table])
    bump('delta_key_index_seeded_keys',count);bump('delta_key_index_seeded_tables',len(missing))


def advance(sink,table,base,version,keys):
    if version!=base+1:return
    prior=get(sink,table,base)
    if prior is None:return
    bits=bytearray(prior)
    for key in keys:add(bits,key)
    put(sink,table,version,bits);bump('delta_key_index_owned_advances')
