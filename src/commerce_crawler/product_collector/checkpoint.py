"""File-only live records and resumable, checksummed JSONL commit history.

Only a position index of the products is kept in memory; a record is read back from its
journal line when it is needed, so memory does not grow with the size of the records."""
from contextlib import contextmanager
import copy
import hashlib
import json
import os
from pathlib import Path
import re
import sys
import time
from uuid import uuid4

from .storage import atomic_json, images_pruned, write_jsonl


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
            try:
                fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as exc:
                # Same error as on Windows: callers tell "held elsewhere" apart from I/O failures.
                raise ValueError('이 작업은 다른 프로세스에서 수집 중입니다.') from exc
            try:
                yield
            finally:
                fcntl.flock(stream, fcntl.LOCK_UN)


class CollectionCheckpoint:
    def __init__(self, directory, adapter=None, config=None, readonly=False, progress=None, repair_files=False):
        self.directory, self.adapter = Path(directory), adapter
        self.progress = progress
        started = time.monotonic()
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
        # (source, product_id) -> (line offset, position in the event's records, seq, detail_status, category ids),
        # in first-appearance order like the record dictionaries it replaces.
        self.index, self.categories, self.requests = {}, {}, {}
        self.report, self.sequence = {}, 0
        self._cached = (None, None)  # the last event read back: (line offset, event)
        self.status = 'running'
        files_commit = 0
        state = self.directory / 'checkpoint_state.json'
        if state.exists():
            saved_state = json.loads(state.read_text(encoding='utf-8'))
            self.status = saved_state['status']
            # Legacy writers also saved state only AFTER both product files.
            if saved_state.get('job_id') == self.job_id:
                files_commit = saved_state.get('commit', 0)
            if type(files_commit) is not int or files_commit < 0:
                raise ValueError('체크포인트 저장 완료 지점이 올바르지 않습니다.')
        pending_files = set()
        if self.journal.exists():
            total_bytes = self.journal.stat().st_size
            read_bytes, last_progress = 0, started
            self._notify('checkpoint_read', completed=0, total=total_bytes, elapsed=0)
            with self.journal.open('rb') as stream:
                for line in stream:
                    offset = read_bytes
                    read_bytes += len(line)
                    if not line.strip():
                        continue
                    try:
                        envelope = json.loads(line.decode('utf-8'))
                    except (ValueError, UnicodeDecodeError):
                        continue  # an interrupted tail is separated by the next append's newline
                    event = envelope['event']
                    if event['seq'] != self.sequence + 1 or hashlib.sha256(encoded(event).encode()).hexdigest() != envelope['sha256']:
                        raise ValueError('체크포인트 이력의 순서 또는 해시가 맞지 않습니다.')
                    self._apply(event, offset)
                    if not readonly and event['seq'] > files_commit:
                        pending_files.update(self.key(row['raw']) for row in event['records'])
                    now = time.monotonic()
                    if now - last_progress >= 1:
                        self._notify('checkpoint_read', completed=read_bytes, total=total_bytes,
                                     elapsed=now - started)
                        last_progress = now
            self._notify('checkpoint_read', completed=read_bytes, total=total_bytes,
                         elapsed=time.monotonic() - started)
        if files_commit > self.sequence:
            raise ValueError('저장 완료 지점보다 체크포인트 이력이 짧습니다. 기존 파일을 확인해주세요.')
        self.request_base = max(self.requests, default=0)
        # Encoded attempts of this process. The leading run that was finished when it was saved
        # never changes again and is not encoded on later saves (_changed_requests).
        self.attempt_versions, self._stable_attempts = {}, 0
        self.category_versions = {key: encoded(value) for key, value in self.categories.items()}
        self._pending_files = pending_files
        if not readonly:
            # The journal is authoritative. Completed mirrors need no startup reads.
            # --repair-files also checks externally removed/corrupted old mirrors.
            self._materialize(list(self.index) if repair_files else pending_files,
                              check_existing=True, show_progress=True)
            if pending_files or repair_files:
                self.state(self.status)
            self._pending_files.clear()
        self._notify('checkpoint_ready', elapsed=time.monotonic() - started)

    def _notify(self, stage, **values):
        if self.progress:
            self.progress({'stage': stage, 'products': len(self.index), 'commit': self.sequence, **values})

    @staticmethod
    def key(raw):
        source, code = raw['source'], str(raw['product_id'])
        if not all(re.fullmatch(r'[A-Za-z0-9_-]+', value) for value in (source, code)):
            raise ValueError('unsafe product path')
        return source, code

    def _apply(self, event, offset):
        for position, row in enumerate(event['records']):
            raw = row['raw']
            key = self.key(raw)
            self.index[key] = (offset, position, event['seq'], sys.intern(raw['detail_status']),
                               tuple(sys.intern(c['category_id']) for c in raw['categories']))
        for category in event['categories']:
            self.categories[category['category_id']] = category
        for request in event['requests']:
            self.requests[request['id']] = request['data']
        self.report = event['report']
        self.sequence = event['seq']

    def _materialize(self, keys, check_existing=False, show_progress=False):
        total = len(keys) if show_progress else 0
        started = last_progress = time.monotonic()
        if show_progress:
            self._notify('checkpoint_files', completed=0, total=total, elapsed=0)
            if total == 0:
                return
        for index, key in enumerate(keys, 1):
            name = f'{key[0]}_{key[1]}.json'
            row = self.row(key)
            for folder, value in (('product_raw', row['raw']), ('products', row['normalized'])):
                path = self.directory / folder / name
                path.parent.mkdir(parents=True, exist_ok=True)
                if check_existing:
                    try:
                        if json.loads(path.read_text(encoding='utf-8')) == value:
                            continue
                    except (OSError, ValueError):
                        pass
                atomic_json(path, value)
            now = time.monotonic()
            if show_progress and now - last_progress >= 1:
                self._notify('checkpoint_files', completed=index, total=total, elapsed=now - started)
                last_progress = now
        if show_progress:
            self._notify('checkpoint_files', completed=total, total=total, elapsed=time.monotonic() - started)

    def row(self, key):
        """The latest saved {'raw', 'normalized'} of a product. Shared with the read cache: do not mutate."""
        offset, position, seq = self.index[key][:3]
        if self._cached[0] != offset:
            with self.journal.open('rb') as stream:
                stream.seek(offset)
                event = json.loads(stream.readline().decode('utf-8'))['event']
            self._cached = (offset, event)
        event = self._cached[1]
        records = event['records']
        if event['seq'] != seq or position >= len(records) or self.key(records[position]['raw']) != key:
            raise ValueError('체크포인트 색인이 이력과 맞지 않습니다.')
        return records[position]

    def __len__(self):
        return len(self.index)

    def __bool__(self):
        return True  # an empty checkpoint is still a checkpoint (callers test "if checkpoint")

    def __contains__(self, key):
        return key in self.index

    def keys(self):
        return list(self.index)

    def detail_status(self, key):
        return self.index[key][3]

    def category_ids(self, key):
        return self.index[key][4]

    def load(self, key):
        """A private copy of the product's latest raw record."""
        return copy.deepcopy(self.row(key)['raw'])

    def load_normalized(self, key):
        return copy.deepcopy(self.row(key)['normalized'])

    def current_report(self):
        report = copy.deepcopy(self.report)
        # Request entries are frozen journal values that nobody mutates; they are not copied.
        report.update(job_id=self.job_id, categories=list(copy.deepcopy(self.categories).values()),
                      requests=[self.requests[k] for k in sorted(self.requests)],
                      request_count=len(self.requests), product_count=len(self.index), checkpoint_status=self.status)
        return report

    def _changed_requests(self, attempts):
        """Attempts whose encoding differs from the last save, in order.

        The leading run of attempts that were already finished when they were saved is skipped,
        since a finished attempt never changes. RequestClient fills an attempt and only then marks
        it finished; the mark is read before encoding, so a skipped attempt was saved in its final
        state. A plain list has no marks and is compared in full."""
        count = len(attempts)
        if count < self._stable_attempts:  # another attempt list: compare it from the start
            self._stable_attempts, self.attempt_versions = 0, {}
        finished = getattr(attempts, 'finished', None)
        versions, done = {}, {}
        for index in range(self._stable_attempts, count):
            done[index] = finished is not None and finished(index)
            versions[index] = encoded(attempts[index])
        changed = [{'id': self.request_base + index + 1, 'data': json.loads(data)} for index, data in versions.items()
                   if self.attempt_versions.get(index) != data]
        return changed, versions, done

    def save(self, changed, report, attempts):
        from .core import payload_hash
        rows = []
        for raw in changed:
            self.key(raw)
            raw['payload_hash'] = payload_hash(raw['payload'])
            rows.append({'raw': raw, 'normalized': self.adapter.normalize(raw)})
        requests, versions, done = self._changed_requests(attempts)
        category_versions = {c['category_id']: encoded(c) for c in report.get('categories', [])}
        event = {'seq': self.sequence + 1, 'records': rows,
                 'categories': [c for c in report.get('categories', []) if self.category_versions.get(c['category_id']) != category_versions[c['category_id']]],
                 'requests': requests,
                 'report': {k: v for k, v in report.items() if k not in {'requests', 'categories', 'updated_at'}}}
        # Freeze mutable worker request entries and caller-owned product dictionaries.
        body = encoded(event)
        frozen = json.loads(body)
        envelope = encoded({'event': frozen, 'sha256': hashlib.sha256(body.encode()).hexdigest()})
        with self.journal.open('ab') as stream:
            offset = os.fstat(stream.fileno()).st_size + 1  # after the separating newline
            stream.write(('\n' + envelope + '\n').encode('utf-8'))
            stream.flush()
            os.fsync(stream.fileno())
        self._apply(frozen, offset)
        self._cached = (offset, frozen)  # what reading the new line back would give
        self.attempt_versions, self.category_versions = versions, category_versions
        while done.get(self._stable_attempts):
            self.attempt_versions.pop(self._stable_attempts)
            self._stable_attempts += 1
        self._pending_files.update(self.key(row['raw']) for row in frozen['records'])
        self._materialize(self._pending_files)
        self._pending_files.clear()
        self.state('running')

    def state(self, status, error=None):
        from .core import utc_now
        self.status = status
        state = {'job_id': self.job_id, 'status': status, 'updated_at': utc_now(), 'commit': self.sequence,
                 'product_count': len(self.index), 'request_count': len(self.requests), 'error': error,
                 'categories': [{'category_id': c['category_id'], 'name': c.get('category_name'),
                     'last_completed_page': max((p['page'] for p in c.get('pages', []) if p['status'] in {'ok', 'empty'}), default=None),
                     'reason': c.get('reason')} for c in self.categories.values()]}
        atomic_json(self.directory / 'checkpoint_state.json', state)

    def export_to(self, target):
        target = Path(target)
        target.mkdir(parents=True, exist_ok=True)
        # One record at a time, read back from the journal exactly as the old dictionaries held them.
        keys = list(self.index)
        write_jsonl(target / 'product_raw.jsonl', (self.row(key)['raw'] for key in keys))
        write_jsonl(target / 'products.jsonl', (self.row(key)['normalized'] for key in keys))
        # Detail pages stay in the run's details/, also for a snapshot exported elsewhere.
        atomic_json(target / 'run_report.json', {**self.current_report(),
                                                 'detail_evidence_dirs': [str(self.directory.resolve())]})
        return target

    def export(self):
        return self.export_to(self.directory)

    def interrupt(self, attempts, status='interrupted', error=None):
        report = self.current_report()
        if 'result' in report:
            # A result of this or an earlier attempt (e.g. "completed" before the image step failed)
            # must not outlive the attempt that ended here.
            report['result'] = status
        self.save([], report, attempts)
        self.state(status, error)
        self.export()

    def close(self):
        pass  # no database or persistent file handles


@contextmanager
def open_collection(output, adapter, config, resume_from=None, new_run=False, progress=None, repair_files=False,
                    image_policy_version=None):
    output = Path(output)
    output.mkdir(parents=True, exist_ok=True)
    with writer_lock(output / '.collection.lock'):
        active = output / 'active_run.json'
        directory = Path(resume_from) if resume_from else None
        if directory is None and not new_run and active.exists():
            directory = Path(json.loads(active.read_text(encoding='utf-8'))['directory'])
        resuming = directory is not None
        if directory is None:
            directory = output / str(uuid4())
        elif not (directory / 'run_config.json').is_file():
            raise ValueError(f'파일 체크포인트가 없습니다. 기존 자료는 보존되며 --new-run으로 새 작업을 시작할 수 있습니다: {directory}')
        with writer_lock(directory / 'writer.lock'):
            # prune may move the run between the check above and this lock; never start it empty.
            if resuming and not (directory / 'run_config.json').is_file():
                raise ValueError(f'재개할 체크포인트가 정리되었습니다: {directory}')
            if resuming and images_pruned(directory / 'media'):
                raise ValueError(f'사진 파일을 정리한(prune) 실행은 재개하지 않습니다: {directory}')
            images = directory / 'media' / 'image_settings.json'
            if resuming and image_policy_version is not None and images.is_file():
                fixed = json.loads(images.read_text(encoding='utf-8')).get('retry_policy_version')
                if fixed != image_policy_version:
                    # Older retry rules cannot be resumed by this code: give the run up and start anew.
                    raise ValueError(f'이미지 재시도 규칙 버전 {fixed}로 시작한 실행은 이 버전({image_policy_version})으로 '
                                     f'재개하지 않습니다. python -m product_collector.abandon --output <출력 경로> '
                                     f'--run "{directory}" 로 포기한 뒤 --new-run으로 새 실행을 시작해주세요.')
            saved = directory / 'checkpoint_state.json'
            # Read before the checkpoint opens: opening may rewrite the state (file repair) and drop the mark.
            if resuming and saved.is_file() and json.loads(saved.read_text(encoding='utf-8')).get('status') == ABANDONED:
                raise ValueError(f'포기한 실행은 재개하지 않습니다. 새 실행은 --new-run으로 시작해주세요: {directory}')
            store = CollectionCheckpoint(directory, adapter, config, progress=progress, repair_files=repair_files)
            atomic_json(active, {'directory': str(directory.resolve())})
            try:
                yield store
            finally:
                if store.status == 'completed':
                    active.unlink(missing_ok=True)
                store.close()


ABANDONED = 'abandoned'


HANDOFF_DIR = 'handoff'
LOADED_MARKER = 'pipeline_loaded.json'


def handoff_pending(output, directory, job):
    """The launcher handed this run to the pipeline (handoff/<job_id>.json) and no load is marked yet."""
    if not isinstance(job, str) or not job:
        return False
    return (Path(output) / HANDOFF_DIR / f'{job}.json').is_file() and not (Path(directory) / LOADED_MARKER).is_file()


def abandon_run(output, directory=None, reason=None):
    """Mark a run that will not be resumed as abandoned and stop it being the active run.

    Taken under the root's .collection.lock and the run's writer.lock, so a collecting run is refused.
    A completed run is never abandoned. The journal and records stay; prune --prune-abandoned may delete them."""
    from .core import utc_now
    output = Path(output)
    with writer_lock(output / '.collection.lock'):
        active = output / 'active_run.json'
        active_dir = None
        if active.exists():
            active_dir = Path(json.loads(active.read_text(encoding='utf-8'))['directory']).resolve()
        target = Path(directory).resolve() if directory else active_dir
        if target is None:
            raise ValueError('포기할 실행이 없습니다. 활성 실행이 없으면 실행 경로를 지정해주세요.')
        if not (target / 'run_config.json').is_file():
            raise ValueError(f'파일 체크포인트가 없습니다: {target}')
        with writer_lock(target / 'writer.lock'):
            path = target / 'checkpoint_state.json'
            if not (target / 'run_config.json').is_file() or not path.is_file():
                raise ValueError(f'파일 체크포인트가 없습니다: {target}')
            state = json.loads(path.read_text(encoding='utf-8'))
            job = json.loads((target / 'run_config.json').read_text(encoding='utf-8')).get('job_id')
            if handoff_pending(output, target, job):
                # The pipeline may be reading this run: it removes the hand-off file first.
                raise ValueError(f'적재 인계 중인 실행은 포기할 수 없습니다: {target}')
            if state.get('status') == 'completed':
                raise ValueError(f'완료된 실행은 포기할 수 없습니다: {target}')
            previous = state.get('status')
            if previous != ABANDONED:
                state.update(status=ABANDONED, updated_at=utc_now(), abandoned_from=previous,
                             abandon_reason=reason)
                atomic_json(path, state)
            if active_dir == target:
                active.unlink()
    return {'directory': str(target), 'job_id': state.get('job_id'), 'status': ABANDONED,
            'previous_status': state.get('abandoned_from'), 'was_active': active_dir == target}


def export_checkpoint(directory):
    directory = Path(directory).resolve()
    store = CollectionCheckpoint(directory, readonly=True)
    return store.export_to(directory / 'exports' / uuid4().hex[:12])
