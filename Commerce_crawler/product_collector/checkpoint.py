"""File-only live records and resumable, checksummed JSONL commit history."""
from contextlib import contextmanager
import copy
import hashlib
import json
import os
from pathlib import Path
import re
from uuid import uuid4

from .storage import atomic_json, write_jsonl


def encoded(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, allow_nan=False)


@contextmanager
def writer_lock(path):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('a+b') as stream:
        if path.stat().st_size == 0:
            stream.write(b'0')
            stream.flush()
        stream.seek(0)
        if os.name == 'nt':
            import msvcrt
            try:
                msvcrt.locking(stream.fileno(), msvcrt.LK_NBLCK, 1)
            except OSError as exc:
                raise ValueError('이 작업은 다른 프로세스에서 수집 중입니다.') from exc
            try:
                yield
            finally:
                stream.seek(0)
                msvcrt.locking(stream.fileno(), msvcrt.LK_UNLCK, 1)
        else:
            import fcntl
            fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
            try:
                yield
            finally:
                fcntl.flock(stream, fcntl.LOCK_UN)


class CollectionCheckpoint:
    def __init__(self, directory, adapter=None, config=None, readonly=False):
        self.directory, self.adapter = Path(directory), adapter
        self.manifest = self.directory / 'run_config.json'
        self.journal = self.directory / 'checkpoint_events.jsonl'
        if self.manifest.exists():
            saved = json.loads(self.manifest.read_text(encoding='utf-8'))
            if config is not None and encoded(saved['config']) != encoded(config):
                raise ValueError('기존 작업과 설정이 다릅니다. 같은 설정 또는 --new-run을 사용해주세요.')
            self.job_id = saved['job_id']
        elif readonly:
            raise ValueError(f'파일 체크포인트가 없습니다: {self.directory}')
        else:
            self.directory.mkdir(parents=True, exist_ok=True)
            self.job_id = self.directory.name
            atomic_json(self.manifest, {'job_id': self.job_id, 'storage': 'jsonl-v1', 'config': config})
        self.records, self.normalized, self.categories, self.requests = {}, {}, {}, {}
        self.report, self.sequence = {}, 0
        self.status = 'running'
        state = self.directory / 'checkpoint_state.json'
        if state.exists():
            self.status = json.loads(state.read_text(encoding='utf-8'))['status']
        if self.journal.exists():
            with self.journal.open('rb') as stream:
                for line in stream:
                    if not line.strip():
                        continue
                    try:
                        envelope = json.loads(line.decode('utf-8'))
                    except (ValueError, UnicodeDecodeError):
                        continue  # an interrupted tail is separated by the next append's newline
                    event = envelope['event']
                    if event['seq'] != self.sequence + 1 or hashlib.sha256(encoded(event).encode()).hexdigest() != envelope['sha256']:
                        raise ValueError('체크포인트 이력의 순서 또는 해시가 맞지 않습니다.')
                    self._apply(event)
        self.request_base = max(self.requests, default=0)
        self.attempt_versions = {}
        self.category_versions = {key: encoded(value) for key, value in self.categories.items()}
        if not readonly:
            self._materialize(self.records)

    @staticmethod
    def key(raw):
        source, code = raw['source'], str(raw['product_id'])
        if not all(re.fullmatch(r'[A-Za-z0-9_-]+', value) for value in (source, code)):
            raise ValueError('unsafe product path')
        return source, code

    def _apply(self, event):
        for row in event['records']:
            key = self.key(row['raw'])
            self.records[key], self.normalized[key] = row['raw'], row['normalized']
        for category in event['categories']:
            self.categories[category['category_id']] = category
        for request in event['requests']:
            self.requests[request['id']] = request['data']
        self.report = event['report']
        self.sequence = event['seq']

    def _materialize(self, keys):
        for key in keys:
            name = f'{key[0]}_{key[1]}.json'
            for folder, value, hash_key in (('product_raw', self.records[key], 'payload_hash'),
                                           ('products', self.normalized[key], 'raw_payload_hash')):
                path = self.directory / folder / name
                path.parent.mkdir(parents=True, exist_ok=True)
                try:
                    if json.loads(path.read_text(encoding='utf-8')).get(hash_key) == value.get(hash_key):
                        continue
                except (OSError, ValueError):
                    pass
                atomic_json(path, value)

    def current_report(self):
        report = copy.deepcopy(self.report)
        report.update(job_id=self.job_id, categories=list(copy.deepcopy(self.categories).values()),
                      requests=[copy.deepcopy(self.requests[k]) for k in sorted(self.requests)],
                      request_count=len(self.requests), product_count=len(self.records), checkpoint_status=self.status)
        return report

    def restore(self):
        return copy.deepcopy(self.records), self.current_report()

    def save(self, changed, report, attempts):
        from .core import payload_hash
        rows = []
        for raw in changed:
            self.key(raw)
            raw['payload_hash'] = payload_hash(raw['payload'])
            rows.append({'raw': raw, 'normalized': self.adapter.normalize(raw)})
        versions = {index: encoded(attempt) for index, attempt in enumerate(list(attempts))}
        category_versions = {c['category_id']: encoded(c) for c in report.get('categories', [])}
        event = {'seq': self.sequence + 1, 'records': rows,
                 'categories': [c for c in report.get('categories', []) if self.category_versions.get(c['category_id']) != category_versions[c['category_id']]],
                 'requests': [{'id': self.request_base + index + 1, 'data': json.loads(data)} for index, data in versions.items()
                              if self.attempt_versions.get(index) != data],
                 'report': {k: v for k, v in report.items() if k not in {'requests', 'categories', 'updated_at'}}}
        # Freeze mutable worker request entries and caller-owned product dictionaries.
        body = encoded(event)
        frozen = json.loads(body)
        envelope = encoded({'event': frozen, 'sha256': hashlib.sha256(body.encode()).hexdigest()})
        with self.journal.open('ab') as stream:
            stream.write(('\n' + envelope + '\n').encode('utf-8'))
            stream.flush()
            os.fsync(stream.fileno())
        self._apply(frozen)
        self.attempt_versions, self.category_versions = versions, category_versions
        self._materialize(self.key(row['raw']) for row in frozen['records'])
        self.state('running')

    def state(self, status, error=None):
        from .core import utc_now
        self.status = status
        state = {'job_id': self.job_id, 'status': status, 'updated_at': utc_now(), 'commit': self.sequence,
                 'product_count': len(self.records), 'request_count': len(self.requests), 'error': error,
                 'categories': [{'category_id': c['category_id'], 'name': c.get('category_name'),
                     'last_completed_page': max((p['page'] for p in c.get('pages', []) if p['status'] in {'ok', 'empty'}), default=None),
                     'reason': c.get('reason')} for c in self.categories.values()]}
        atomic_json(self.directory / 'checkpoint_state.json', state)

    def export_to(self, target):
        target = Path(target)
        target.mkdir(parents=True, exist_ok=True)
        write_jsonl(target / 'product_raw.jsonl', self.records.values())
        write_jsonl(target / 'products.jsonl', self.normalized.values())
        atomic_json(target / 'run_report.json', self.current_report())
        return target

    def export(self):
        return self.export_to(self.directory)

    def interrupt(self, attempts, status='interrupted', error=None):
        self.save([], self.current_report(), attempts)
        self.state(status, error)
        self.export()

    def close(self):
        pass  # no database or persistent file handles


@contextmanager
def open_collection(output, adapter, config, resume_from=None, new_run=False):
    output = Path(output)
    output.mkdir(parents=True, exist_ok=True)
    with writer_lock(output / '.collection.lock'):
        active = output / 'active_run.json'
        directory = Path(resume_from) if resume_from else None
        if directory is None and not new_run and active.exists():
            directory = Path(json.loads(active.read_text(encoding='utf-8'))['directory'])
        if directory is None:
            directory = output / str(uuid4())
        elif not (directory / 'run_config.json').is_file():
            raise ValueError(f'파일 체크포인트가 없습니다. 기존 자료는 보존되며 --new-run으로 새 작업을 시작할 수 있습니다: {directory}')
        with writer_lock(directory / 'writer.lock'):
            store = CollectionCheckpoint(directory, adapter, config)
            atomic_json(active, {'directory': str(directory.resolve())})
            try:
                yield store
            finally:
                if store.status == 'completed':
                    active.unlink(missing_ok=True)
                store.close()


def export_checkpoint(directory):
    directory = Path(directory).resolve()
    store = CollectionCheckpoint(directory, readonly=True)
    return store.export_to(directory / 'exports' / uuid4().hex[:12])
