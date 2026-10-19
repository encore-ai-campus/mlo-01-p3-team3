"""Bounded content-addressed transport. A ready manifest is published last."""
import hashlib
import os
from pathlib import Path
import tempfile

from .contracts import canonical, digest, now
from .errors import PipelineError
from .files import atomic_bytes, atomic_json, confined, parse, read_json, sha_file, sync_dir
from .snapshot import verify

CHUNK_BYTES = 8 * 1024 * 1024
MAX_READY_BYTES = 16 * 1024 * 1024


def send_snapshot(snapshot, objects):
    snapshot = Path(snapshot).resolve()
    manifest = verify(snapshot)
    files, written = {}, 0
    with tempfile.TemporaryDirectory(prefix='handoff-send-') as tmp:
        chunk_path = Path(tmp)/'chunk'
        for name in sorted([*manifest['files'], 'snapshot.json']):
            path = confined(snapshot, name)
            chunks = []
            with path.open('rb') as stream:
                while data := stream.read(CHUNK_BYTES):
                    checksum = hashlib.sha256(data).hexdigest()
                    key = 'handoff/chunks/sha256/'+checksum
                    atomic_bytes(chunk_path, data)
                    written += bool(objects.ensure(key, chunk_path, checksum, len(data)))
                    chunks.append(dict(key=key, sha256=checksum, bytes=len(data)))
            files[name] = dict(sha256=sha_file(path), bytes=path.stat().st_size, chunks=chunks)
        # Source tampering during transfer must not produce ready.
        verify(snapshot)
        ready = dict(transport_schema_version=1, snapshot_id=manifest['snapshot_id'], files=files)
        ready['transport_id'] = digest(ready)
        data = canonical(ready).encode()
        if len(data) > MAX_READY_BYTES:
            raise PipelineError('TRANSPORT_MANIFEST_TOO_LARGE')
        ready_key = f"handoff/ready/{ready['transport_id']}.ready.json"
        atomic_bytes(chunk_path, data)
        written += bool(objects.ensure(ready_key, chunk_path, hashlib.sha256(data).hexdigest(), len(data)))
    return dict(result='TRANSFER_CONFIRMED', ready_key=ready_key,
                snapshot_id=manifest['snapshot_id'], objects_written=written)


def receive_snapshot(ready_file, fetch, destination, *, max_total_bytes=10*1024**3):
    destination = Path(destination).resolve()
    ready_path = Path(ready_file)
    if ready_path.stat().st_size > MAX_READY_BYTES:
        raise PipelineError('TRANSPORT_MANIFEST_TOO_LARGE')
    ready = read_json(ready_path)
    stable = {k: v for k, v in ready.items() if k != 'transport_id'}
    if (ready.get('transport_schema_version') != 1 or digest(stable) != ready.get('transport_id')
            or 'snapshot.json' not in ready.get('files', {})):
        raise PipelineError('TRANSPORT_MANIFEST_INVALID')
    if destination.exists():
        if verify(destination)['snapshot_id'] != ready['snapshot_id']:
            raise PipelineError('RECEIVED_SNAPSHOT_CONFLICT')
        return dict(result='RECEIVED_REUSED', snapshot_id=ready['snapshot_id'])
    if type(max_total_bytes) is not int or max_total_bytes < 1:
        raise PipelineError('RECEIVE_QUOTA_REQUIRED')
    size = sum(info['bytes'] for info in ready['files'].values())
    if size > max_total_bytes:
        raise PipelineError('RECEIVE_QUOTA_EXCEEDED')
    destination.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix='.receive-', dir=destination.parent) as tmp:
        build = Path(tmp)/'sealed'
        build.mkdir()
        for name, info in ready['files'].items():
            path = confined(build, name)
            path.parent.mkdir(parents=True, exist_ok=True)
            checksum, count = hashlib.sha256(), 0
            with path.open('xb') as stream:
                for chunk in info['chunks']:
                    if (chunk['key'] != 'handoff/chunks/sha256/'+chunk['sha256']
                            or type(chunk['bytes']) is not int or not 0 < chunk['bytes'] <= CHUNK_BYTES):
                        raise PipelineError('TRANSPORT_CHUNK_INVALID')
                    data = fetch(chunk['key'])
                    if len(data) != chunk['bytes'] or hashlib.sha256(data).hexdigest() != chunk['sha256']:
                        raise PipelineError('TRANSPORT_CHUNK_CHANGED')
                    count += len(data)
                    if count > info['bytes']:
                        raise PipelineError('TRANSPORT_SIZE_MISMATCH')
                    checksum.update(data)
                    stream.write(data)
                stream.flush()
                os.fsync(stream.fileno())
            if count != info['bytes'] or checksum.hexdigest() != info['sha256']:
                raise PipelineError('TRANSPORT_FILE_CHANGED')
        if verify(build)['snapshot_id'] != ready['snapshot_id']:
            raise PipelineError('TRANSPORT_SNAPSHOT_MISMATCH')
        build.rename(destination)
        sync_dir(destination.parent)
    return dict(result='RECEIVED', snapshot_id=ready['snapshot_id'])
