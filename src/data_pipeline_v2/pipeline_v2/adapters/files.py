"""Portable durable files: atomic replacement, checksum binding and exclusive locks."""
from contextlib import contextmanager
import json
import os
from pathlib import Path
import tempfile

from ..domain.contracts import canonical, digest, seal, verified, Invalid, check_rows


def confined(root, name):
    root=Path(root).resolve()
    target=(root/name).resolve()
    if not target.is_relative_to(root) or Path(name).is_absolute():raise Invalid('PATH_OUTSIDE_STATE')
    return target


def read(path):
    return json.loads(Path(path).read_text(encoding='utf-8'))


def write(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary = tempfile.mkstemp(prefix='.next-', dir=path.parent)
    try:
        with os.fdopen(descriptor, 'w', encoding='utf-8', newline='\n') as stream:
            stream.write(canonical(value))
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        if os.name != 'nt':
            parent = os.open(path.parent, os.O_RDONLY)
            try: os.fsync(parent)
            finally: os.close(parent)
    finally:
        if os.path.exists(temporary): os.unlink(temporary)


def immutable(path, value):
    if Path(path).exists():
        if read(path) != value:
            raise Invalid('IMMUTABLE_FILE_CONFLICT')
    else:
        write(path, value)


@contextmanager
def locked(path):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('a+b') as stream:
        if stream.tell() == 0:
            stream.write(b'0'); stream.flush()
        stream.seek(0)
        try:
            if os.name == 'nt':
                import msvcrt
                msvcrt.locking(stream.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            raise Invalid('WRITER_BUSY') from exc
        try:
            yield
        finally:
            stream.seek(0)
            if os.name == 'nt':
                msvcrt.locking(stream.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                fcntl.flock(stream, fcntl.LOCK_UN)


class FileLake:
    """Bounded local execution backend; direct key lookup, no historical table scans."""
    def __init__(self, root):
        self.root = Path(root).resolve()
        self.identity = dict(kind='FILE_LAKE', root=str(self.root))
        self.metrics = dict(read_rows=0, written_rows=0, commits=0)

    def _path(self, table, key):
        if not table.replace('_', '').isalnum(): raise Invalid('TABLE_NAME_INVALID')
        checksum = digest(key)
        return self.root/table/'rows'/(checksum[0]+'.json')

    def read_keys(self, table, keys, version=None):
        result = {}
        buckets={}
        for key in keys:
            buckets.setdefault(self._path(table,key),[]).append(key)
        for path,wanted in buckets.items():
            if path.exists():
                values=read(path)
                for key in wanted:
                    if key not in values:continue
                    item=values[key];check_rows([item])
                    if item['row_key'] != key: raise Invalid('LAKE_KEY_CHANGED')
                    result[key] = item
                    self.metrics['read_rows'] += 1
        return result

    def commit(self, table, rows):
        rows = check_rows(rows)
        keys = [r['row_key'] for r in rows]
        with locked(self.root/'writer.lock'):
            old = self.read_keys(table, keys)
            buckets={}
            for item in rows:
                if item['row_key'] in old and old[item['row_key']] != item:
                    raise Invalid('LAKE_IMMUTABLE_CONFLICT')
                if item['row_key'] not in old:
                    path=self._path(table,item['row_key'])
                    if path not in buckets:buckets[path]=read(path) if path.exists() else {}
                    buckets[path][item['row_key']]=item
            for path,values in buckets.items():write(path,values)
            proof = seal(dict(table=table, records={r['row_key']:r['record_hash'] for r in rows}, lake=self.identity))
            path = self.root/table/'commits'/(proof['content_hash']+'.json')
            if not path.exists():
                write(path, proof)
                self.metrics['commits'] += bool(len(rows)-len(old))
                self.metrics['written_rows'] += len(rows)-len(old)
        return proof

    def purge(self, table, expired):
        """Retention deletion for expiring tables only; immutable stage proofs never point here."""
        if not table.replace('_', '').isalnum(): raise Invalid('TABLE_NAME_INVALID')
        removed = 0
        with locked(self.root/'writer.lock'):
            for path in sorted((self.root/table/'rows').glob('*.json')):
                values = read(path)
                kept = {k:v for k,v in values.items() if not expired(v['payload'])}
                if len(kept) != len(values):
                    removed += len(values) - len(kept)
                    write(path, kept)
        return removed

    def verify(self, proof):
        verified(proof)
        if proof['lake'] != self.identity: raise Invalid('LAKE_TARGET_CHANGED')
        actual = self.read_keys(proof['table'], list(proof['records']))
        if {k:v['record_hash'] for k,v in actual.items()} != proof['records']:
            raise Invalid('LAKE_OUTPUT_MISSING')

    def close(self): pass
