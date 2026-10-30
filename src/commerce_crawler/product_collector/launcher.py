"""Server launcher: NiFi wakes `tick` every hour; the launcher decides what the collector does.

A tick ends within seconds. It hands completed runs to the pipeline, prunes, and either waits or
starts a detached `supervise` process that runs the collector and judges its result. The rules are
crawler-optimization/operations.md (policy 4) and launcher.md. Every command prints one JSON line last;
tick, supervise, release and stop also keep it in <root>/launcher/last.json for monitoring.

<root> is the folder above the control file's config/ folder. Files of the launcher:
  <root>/launcher/state.json      attempts, per-run counters and back-off, operator wait, start/finish times
  <root>/launcher/state.lock      held while state.json is read and written (every command)
  <root>/launcher/tick.lock       one tick at a time
  <root>/launcher/supervisor.lock held by the supervise process while it lives
  <root>/HOLD                     operator stop: no new run, no resume (hand-off and prune still run)
  <root>/logs/<job_id|new>/<time>.log  collector output of one attempt
  <runs>/handoff/<job_id>.json    hand-off to the pipeline (written once, never replaced)
"""
from __future__ import annotations

import argparse
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import hashlib
import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
import sys
import time
from uuid import uuid4

from .checkpoint import HANDOFF_DIR, LOADED_MARKER, writer_lock

DEFAULT_CONTROL = Path('/var/lib/pj3/product-pipeline/nifi/collector/config/collector-control.json')
CONTRACT_VERSION = 2
HANDOFF_SCHEMA = 1
READY_SECONDS = 30          # tick waits this long for supervise to record its attempt
HANDOFF_ALERT_HOURS = 48
RELEASE_EXTENSION_DAYS = 3  # a run released after max_run_days gets this much more time
CHECK_TIMEOUT = 120
PRUNE_TIMEOUT = 1800
EXIT_OK, EXIT_ERROR, EXIT_CONFIG = 0, 1, 3

# Exit codes of the collector (product_collector/__main__.py) and of --check-pagination.
COLLECTOR_INTERRUPTED, COLLECTOR_NOT_STARTED = 130, 3
CHECK_OK, CHECK_NOT_STARTED, CHECK_BLOCKED, CHECK_REQUEST_FAILED, CHECK_PARSE_FAILED = 0, 3, 4, 5, 6
BLOCKED = {'http_403', 'http_429'}
BUDGET = {'request_limit', 'total_limit'}
LISTING_TEMPLATE_FAILURES = {'parse_failed', 'no_products'}

REQUIRED = {
    'contract_version': int, 'enabled': bool, 'source': str, 'source_commit': str, 'python': str,
    'categories': str, 'pagination_template': str, 'output': str, 'workers': int,
    'listing_interval': (int, float), 'interval': (int, float), 'image_overlap': str, 'detail_refresh': str,
    'refresh_sample': int, 'quality_gate': str, 'prune_keep': int, 'prune_abandoned': bool,
    'handoff_since': str, 'schedule': dict, 'disk': dict, 'backoff_hours': dict,
}
# Fixed for a run once it starts: a change applies from the next new run (operations.md 6).
FIXED = ('categories', 'output', 'detail_refresh', 'refresh_sample', 'image_store_config', 'quality_gate')


class ConfigError(ValueError):
    """The control file or the installation is wrong: exit 3."""


def utc(value: str | None) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace('Z', '+00:00'))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def iso(value: datetime | None) -> str | None:
    return value.astimezone(timezone.utc).isoformat(timespec='seconds') if value else None


def read_json(path: Path):
    try:
        return json.loads(Path(path).read_text(encoding='utf-8'))
    except (OSError, ValueError):
        return None


def write_json(path: Path, value) -> None:
    """Temporary file, fsync, replace: a reader sees the old or the new file, never half of one."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f'{path.name}.{uuid4().hex[:8]}.tmp')
    with temporary.open('w', encoding='utf-8') as stream:
        json.dump(value, stream, ensure_ascii=False, indent=2)
        stream.flush()
        os.fsync(stream.fileno())
    temporary.replace(path)


def fsync_dir(path: Path) -> None:
    if os.name != 'posix':
        return
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


# writer_lock reports a held lock as ValueError on every OS; BlockingIOError stays for an older checkout.
LOCK_HELD = (ValueError, BlockingIOError)


def lock_busy(path: Path) -> bool:
    """True when another process holds this non-blocking lock file."""
    try:
        with writer_lock(path):
            return False
    except LOCK_HELD:
        return True


@contextmanager
def try_lock(path: Path):
    """Yields True holding the lock, or False at once when another process holds it."""
    lock = writer_lock(path)
    try:
        lock.__enter__()
    except LOCK_HELD:
        yield False
        return
    try:
        yield True
    finally:
        lock.__exit__(None, None, None)


@contextmanager
def blocking_lock(path: Path, seconds: float = 30.0):
    """writer_lock retried until it is free: state.json is held only for short reads and writes."""
    deadline = time.monotonic() + seconds
    while True:
        try:
            with writer_lock(path):
                yield
                return
        except LOCK_HELD:
            if time.monotonic() >= deadline:
                raise
            time.sleep(0.05)


def load_control(path: Path) -> dict:
    control = read_json(path)
    if not isinstance(control, dict):
        raise ConfigError(f'control file is missing or not JSON: {path}')
    if control.get('contract_version') != CONTRACT_VERSION:
        raise ConfigError(f"control contract_version must be {CONTRACT_VERSION}, got {control.get('contract_version')!r}")
    for key, kind in REQUIRED.items():
        value = control.get(key)
        if not isinstance(value, kind) or (kind is int and isinstance(value, bool)):
            raise ConfigError(f'control key {key} is missing or has the wrong type')
    if 'image_store_config' not in control or not isinstance(control['image_store_config'], (str, type(None))):
        raise ConfigError('control key image_store_config must be a path or null (null keeps it off)')
    if control['image_overlap'] not in ('on', 'off') or control['detail_refresh'] not in ('all', 'changed'):
        raise ConfigError('image_overlap must be on|off and detail_refresh all|changed')
    if control['quality_gate'] not in ('warn', 'enforce') or control['prune_keep'] < 1 or control['workers'] < 1:
        raise ConfigError('quality_gate must be warn|enforce, prune_keep and workers positive')
    schedule, disk, backoff = control['schedule'], control['disk'], control['backoff_hours']
    numbers = [schedule.get('period_days'), schedule.get('rest_hours'), schedule.get('max_run_days'),
               disk.get('new_run_free_gb'), disk.get('resume_free_gb'), backoff.get('retry')]
    blocked = backoff.get('blocked')
    if (not all(isinstance(n, (int, float)) and not isinstance(n, bool) and n >= 0 for n in numbers)
            or not isinstance(blocked, list) or not blocked
            or not all(isinstance(n, (int, float)) and not isinstance(n, bool) and n >= 0 for n in blocked)):
        raise ConfigError('schedule, disk and backoff_hours need non-negative numbers (backoff_hours.blocked a list)')
    if utc(control['handoff_since']) is None:
        raise ConfigError('handoff_since must be an ISO time')
    return control


class Launcher:
    def __init__(self, control_path: Path = DEFAULT_CONTROL, now=None, spawner=None, run=None, disk_free=None):
        self.control_path = Path(control_path)
        self.root = self.control_path.resolve().parent.parent
        self.home = self.root / 'launcher'
        self._now = now or (lambda: datetime.now(timezone.utc))
        self.spawner = spawner or self._spawn_detached
        self.run = run or subprocess.run
        self.disk_free = disk_free or (lambda path: shutil.disk_usage(path).free)
        self.control: dict = {}
        self.alerts: list[dict] = []

    # ----- files -----------------------------------------------------------------------------------

    def now(self) -> datetime:
        return self._now()

    @property
    def runs(self) -> Path:
        return Path(self.control['output'])

    def load(self) -> dict:
        self.control = load_control(self.control_path)
        return self.control

    @contextmanager
    def state(self):
        """state.json under state.lock; the block's changes are written when it ends."""
        self.home.mkdir(parents=True, exist_ok=True)
        with blocking_lock(self.home / 'state.lock'):
            state = read_json(self.home / 'state.json')
            if not isinstance(state, dict):
                state = {}
            state.setdefault('runs', {})
            state.setdefault('pre_run', {})
            before = json.dumps(state, sort_keys=True)
            yield state
            if json.dumps(state, sort_keys=True) != before:
                write_json(self.home / 'state.json', state)

    def read_state(self) -> dict:
        with self.state() as state:
            return json.loads(json.dumps(state))

    def finish(self, command: str, status: str, code: int = EXIT_OK, record=True, **values) -> int:
        line = {'at': iso(self.now()), 'command': command, 'status': status, **values}
        if self.alerts:
            line['alerts'] = self.alerts
        if record:
            with blocking_lock(self.home / 'state.lock'):
                write_json(self.home / 'last.json', line)
        print(json.dumps(line, ensure_ascii=False), flush=True)
        return code

    def alert(self, code: str, **values):
        self.alerts.append({'code': code, **values})

    # ----- runs on disk ----------------------------------------------------------------------------

    def run_dirs(self):
        if not self.runs.is_dir():
            return []
        return [p for p in sorted(self.runs.iterdir())
                if p.is_dir() and not p.name.startswith(('.', '_')) and p.name != HANDOFF_DIR
                and (p / 'run_config.json').is_file()]

    def active_dir(self) -> Path | None:
        active = read_json(self.runs / 'active_run.json')
        if isinstance(active, dict) and isinstance(active.get('directory'), str):
            return Path(active['directory'])
        return None

    @staticmethod
    def job_of(directory: Path) -> str | None:
        config = read_json(directory / 'run_config.json')
        job = config.get('job_id') if isinstance(config, dict) else None
        return job if isinstance(job, str) and job else None

    def fixed_options(self) -> dict:
        value = {k: self.control.get(k) for k in FIXED}
        # The categories file moves with every release: compare what it says, not where it lies.
        try:
            value['categories'] = 'sha256:' + hashlib.sha256(Path(value['categories']).read_bytes()).hexdigest()
        except (OSError, TypeError):
            pass
        return value

    @staticmethod
    def status_of(directory: Path) -> str | None:
        state = read_json(directory / 'checkpoint_state.json')
        return state.get('status') if isinstance(state, dict) else None

    def completed(self, directory: Path) -> bool:
        """The pipeline's completed_run check: checkpoint and report completed, both for run_config's job."""
        job = self.job_of(directory)
        state = read_json(directory / 'checkpoint_state.json') or {}
        report = read_json(directory / 'run_report.json') or {}
        return bool(job and state.get('status') == 'completed' and report.get('result') == 'completed'
                    and state.get('job_id') == job and report.get('job_id') == job)

    def started_at(self, state: dict, directory: Path) -> datetime | None:
        job = self.job_of(directory)
        recorded = utc((state['runs'].get(job) or {}).get('started_at'))
        if recorded:
            return recorded
        report = read_json(directory / 'run_report.json') or {}
        return utc(report.get('started_at'))

    def finished_at(self, state: dict, directory: Path) -> datetime | None:
        job = self.job_of(directory)
        recorded = utc((state['runs'].get(job) or {}).get('finished_at'))
        if recorded:
            return recorded
        return utc((read_json(directory / 'checkpoint_state.json') or {}).get('updated_at'))

    # ----- hand-off --------------------------------------------------------------------------------

    def unrestricted(self, directory: Path) -> bool:
        """An operating run: no product, request, page or start limit, details collected."""
        config = (read_json(directory / 'run_config.json') or {}).get('config') or {}
        limits = (read_json(directory / 'run_report.json') or {}).get('limits') or {}
        categories = config.get('categories') or []
        return (config.get('total_limit') is None and config.get('collect_details') is True
                and limits.get('requests') is None and bool(categories)
                and all(c.get('max_pages') is None and c.get('start_page', 1) == 1 for c in categories))

    def handoff_problem(self, state: dict, directory: Path) -> str | None:
        """Why this run may not be handed over yet, None when all five conditions hold (launcher.md 6)."""
        job = self.job_of(directory)
        if not self.completed(directory):
            return 'not_completed'
        if lock_busy(directory / 'writer.lock'):
            return 'writer_locked'
        media = directory / 'media'
        report = read_json(media / 'download_report.json') or {}
        if not (media / 'products_with_local_images.jsonl').is_file() or not report.get('finished_at'):
            return 'media_not_finished'
        wait = state.get('operator_wait') or {}
        if wait.get('target') == job or self.status_of(directory) == 'abandoned':
            return 'operator_wait'
        if not self.unrestricted(directory):
            return 'restricted'
        return None

    def write_handoff(self, state: dict, directory: Path) -> str:
        """Create <runs>/handoff/<job_id>.json once: the final name appears only with complete content."""
        job = self.job_of(directory)
        problem = self.handoff_problem(state, directory)
        if problem:
            return problem
        folder = self.runs / HANDOFF_DIR
        folder.mkdir(parents=True, exist_ok=True)
        final = folder / f'{job}.json'
        if final.exists():
            existing = read_json(final) or {}
            if existing.get('job_id') != job or existing.get('run_dir') != str(directory.resolve()):
                self.alert('handoff_mismatch', job_id=job, handoff=str(final))
                return 'mismatch'
            return 'exists'
        fixed = (state['runs'].get(job) or {}).get('fixed') or self.fixed_options()
        value = {'schema_version': HANDOFF_SCHEMA, 'job_id': job, 'run_dir': str(directory.resolve()),
                 'media_manifest': 'media/products_with_local_images.jsonl',  # relative to run_dir
                 'created_at': iso(self.now()),
                 'checks': {'run_completed': True, 'writer_unlocked': True, 'media_finished': True,
                            'unrestricted': True},
                 'crawler_commit': self.control.get('source_commit'),
                 'options': {'detail_refresh': fixed.get('detail_refresh'),
                             'image_store': bool(fixed.get('image_store_config')),
                             'quality_gate': fixed.get('quality_gate')}}
        temporary = folder / f'.{job}.{uuid4().hex[:8]}.tmp'
        with temporary.open('w', encoding='utf-8') as stream:
            json.dump(value, stream, ensure_ascii=False, indent=2)
            stream.flush()
            os.fsync(stream.fileno())
        try:
            os.link(temporary, final)  # never replaces a hand-off the pipeline may already hold
        except FileExistsError:
            temporary.unlink(missing_ok=True)
            return self.write_handoff(state, directory)
        temporary.unlink(missing_ok=True)
        fsync_dir(folder)
        return 'created'

    def recover_handoffs(self, state: dict) -> list[str]:
        """Completed runs without a load marker or hand-off (the supervisor died in between) get one now."""
        since = utc(self.control['handoff_since'])
        created = []
        for directory in self.run_dirs():
            job = self.job_of(directory)
            if (not job or (directory / LOADED_MARKER).exists()
                    or (self.runs / HANDOFF_DIR / f'{job}.json').exists() or not self.completed(directory)):
                continue
            started = self.started_at(state, directory)
            if not started or started < since:
                continue  # runs before operation are not handed over
            result = self.write_handoff(state, directory)
            if result == 'created':
                created.append(job)
        return created

    def handoff_alerts(self) -> None:
        folder = self.runs / HANDOFF_DIR
        if not folder.is_dir():
            return
        limit = timedelta(hours=HANDOFF_ALERT_HOURS)
        for path in folder.glob('*.json'):
            job = path.stem
            failed = read_json(path.with_suffix('.failed'))
            if failed is not None:
                # The hand-off step counts its failures here; past its limit it stops (EXHAUSTED).
                self.alert('handoff_failed', job_id=job, failed=failed)
            claimed = path.with_suffix('.claimed')
            if claimed.exists():
                since = datetime.fromtimestamp(claimed.stat().st_mtime, timezone.utc)
                if self.now() - since > limit:
                    self.alert('handoff_claimed_not_loaded', job_id=job, since=iso(since))
            else:
                created = utc((read_json(path) or {}).get('created_at')) or datetime.fromtimestamp(
                    path.stat().st_mtime, timezone.utc)
                if self.now() - created > limit:
                    self.alert('handoff_not_claimed', job_id=job, since=iso(created))

    def clear_active_pointer(self, directory: Path) -> None:
        """A completed run the active pointer still names (the collector died before removing it)."""
        active = self.active_dir()
        if not active or active.resolve() != directory.resolve():
            return
        try:
            with writer_lock(self.runs / '.collection.lock'):
                active = self.active_dir()
                if active and active.resolve() == directory.resolve() and self.completed(directory):
                    (self.runs / 'active_run.json').unlink(missing_ok=True)
        except LOCK_HELD:
            pass  # a collector holds the root; the next tick tries again

    # ----- judging an attempt -----------------------------------------------------------------------

    @staticmethod
    def result_line(log: Path | None) -> dict | None:
        """The collector's last stdout JSON line (finish_line), if it got that far."""
        if not log or not Path(log).is_file():
            return None
        try:
            lines = Path(log).read_text(encoding='utf-8', errors='replace').splitlines()
        except OSError:
            return None
        for line in reversed(lines):
            line = line.strip()
            if line.startswith('{'):
                try:
                    value = json.loads(line)
                except ValueError:
                    continue
                if isinstance(value, dict) and 'exit_code' in value and 'result' in value:
                    return value
        return None

    @staticmethod
    def run_from_line(line: dict | None) -> Path | None:
        """The run folder the collector's result line names (not for a refused start)."""
        if line and line.get('output') and line.get('result') != 'not_started':
            candidate = Path(line['output'])
            if (candidate / 'run_config.json').is_file():
                return candidate
        return None

    def classify(self, exit_code, line: dict | None, directory: Path | None) -> tuple[str, str | None]:
        """(outcome, detail) of one finished attempt; launcher.md 5, from the top."""
        status = self.status_of(directory) if directory else None
        job = self.job_of(directory) if directory else None
        if status == 'abandoned':
            return 'abandoned', None
        if line and line.get('result') == 'not_started':
            return 'not_started', str(line.get('error') or '')
        if line and exit_code is None:
            exit_code = line.get('exit_code')
        if line and line.get('job_id') and job and line['job_id'] != job:
            return 'job_mismatch', line['job_id']
        if line is None:
            # Without a result line only a completed run is believed: a resume killed while restoring
            # leaves the previous attempt's partial/failed/interrupted state, which must not count again.
            if directory is not None and self.completed(directory):
                return 'completed', None
            return 'crashed', status
        if exit_code == COLLECTOR_INTERRUPTED:
            return 'interrupted', None
        if line.get('result') == 'failed':
            return 'failed', str(line.get('error') or '')
        if directory is not None and self.completed(directory):
            return 'completed', None
        report = (read_json(directory / 'run_report.json') or {}) if directory else {}
        if report.get('result') == 'quality_failed' or (line or {}).get('result') == 'quality_failed':
            return 'quality_failed', None
        if report.get('result') == 'partial' or (line or {}).get('result') == 'partial':
            return self.partial_cause(report)
        return 'unclassified', str((line or {}).get('result') or status)

    @staticmethod
    def partial_cause(report: dict) -> tuple[str, str | None]:
        """Blocked > listing > detail > image > budget > other (operations.md)."""
        images = report.get('image_download') or {}
        for reason in (report.get('stop_reason'), images.get('stop_reason')):
            if reason in BLOCKED:
                return 'blocked', reason
        listing = [e for e in report.get('errors') or [] if e.get('stage') != 'detail']
        if listing:
            failed = [c for c in report.get('categories') or [] if c.get('status') in LISTING_TEMPLATE_FAILURES]
            return ('listing_template', f'{len(failed)} categories') if len(failed) > 1 else ('listing', None)
        if not (report.get('detail_failures') or {}).get('passed', True):
            return 'detail', None
        if images and (images.get('result') != 'completed' or images.get('verification') != 'completed'):
            return 'image', images.get('result')
        if report.get('stop_reason') in BUDGET:
            return 'budget', report['stop_reason']
        return 'unclassified', report.get('stop_reason')

    def apply_outcome(self, state: dict, job: str | None, outcome: str, detail, ended: datetime,
                      directory: Path | None) -> dict:
        """Next action for this run from its outcome: counters, back-off, operator wait or hand-off."""
        backoff = self.control['backoff_hours']
        entry = state['runs'].setdefault(job, {}) if job else state['pre_run']
        cause = outcome if outcome != 'failed' else f'failed:{detail}'
        if outcome not in ('interrupted',):
            entry['streak'] = entry.get('streak', 0) + 1 if entry.get('last_cause') == cause else 1
            entry['last_cause'] = cause
        streak = entry.get('streak', 0)
        result = {'outcome': outcome, 'detail': detail}

        def wait(reason):
            state['operator_wait'] = {'reason': reason, 'at': iso(ended), 'target': job or 'pre_run',
                                      'detail': detail}
            entry.pop('next_at', None)
            result['action'] = 'operator_wait'

        def resume(hours):
            entry['next_at'] = iso(ended + timedelta(hours=hours))
            result['action'] = 'resume'
            result['next_at'] = entry['next_at']

        if outcome == 'completed':
            if job:
                entry.update(finished_at=entry.get('finished_at') or iso(ended))
                for key in ('streak', 'last_cause', 'next_at'):
                    entry.pop(key, None)
                state['last_completed'] = {'job_id': job, 'started_at': entry.get('started_at'),
                                           'finished_at': entry['finished_at']}
            result['action'] = 'handoff'
            if directory is not None:
                result['handoff'] = self.write_handoff(state, directory)
                self.clear_active_pointer(directory)
        elif outcome == 'abandoned':
            for key in ('streak', 'last_cause', 'next_at'):
                entry.pop(key, None)
            if (state.get('operator_wait') or {}).get('target') == job:
                state.pop('operator_wait', None)
            result['action'] = 'closed'
        elif outcome == 'interrupted':
            resume(0)
        elif outcome == 'crashed':
            wait('crashed_twice') if streak >= 2 else resume(backoff['retry'])
        elif outcome == 'failed':
            wait('failed_twice_same_error') if streak >= 2 else resume(backoff['retry'])
        elif outcome == 'blocked':
            limit = len(backoff['blocked'])
            wait(f'blocked_{streak}_times') if streak >= limit else resume(backoff['blocked'][streak - 1])
        elif outcome in ('listing', 'detail'):
            wait(f'{outcome}_after_retry') if streak >= 2 else resume(backoff['retry'])
        else:
            # not_started, job_mismatch, listing_template, image, budget, quality_failed, unclassified
            wait(outcome)
        return result

    # ----- tick ------------------------------------------------------------------------------------

    def tick(self) -> int:
        self.home.mkdir(parents=True, exist_ok=True)
        with try_lock(self.home / 'tick.lock') as held:
            if not held:
                return self.finish('tick', 'TICK_BUSY', record=False)
            return self._tick()

    def _tick(self) -> int:
        try:
            self.load()
        except ConfigError as exc:
            return self.finish('tick', 'CONFIG_INVALID', EXIT_CONFIG, reason=str(exc))
        if not self.control['enabled']:
            return self.finish('tick', 'DISABLED')
        self.runs.mkdir(parents=True, exist_ok=True)
        with self.state() as state:
            handed = self.recover_handoffs(state)
        self.handoff_alerts()
        extra = {'handoffs_created': handed} if handed else {}
        if lock_busy(self.home / 'supervisor.lock'):
            attempt = self.read_state().get('attempt') or {}
            return self.finish('tick', 'RUNNING', job_id=attempt.get('job_id'), started_at=attempt.get('started_at'),
                               **extra)
        if lock_busy(self.runs / '.collection.lock'):
            self.alert('orphan_collector', detail='no supervisor but the collection lock is held')
            return self.finish('tick', 'ORPHAN_RUNNING', **extra)
        prune = self.prune()
        extra['prune'] = prune
        if (self.root / 'HOLD').exists():
            return self.finish('tick', 'HOLD', **extra)
        with self.state() as state:
            self.close_lost_attempt(state)
            decision = self.decide(state)
        if decision['action'] in ('wait', 'operator_wait'):
            status = 'OPERATOR_WAIT' if decision['action'] == 'operator_wait' else 'WAIT'
            return self.finish('tick', status, job_id=decision.get('job_id'), reason=decision.get('reason'),
                               next_action_at=decision.get('next_at'), **extra)
        problem = self.preflight(decision)
        if problem:
            return self.finish('tick', problem['status'], job_id=decision.get('job_id'), reason=problem['reason'],
                               next_action_at=problem.get('next_at'), **extra)
        attempt = uuid4().hex[:12]
        started = self.start(decision['action'], attempt)
        if not started:
            self.alert('start_failed', attempt=attempt)
            return self.finish('tick', 'START_FAILED', job_id=decision.get('job_id'), **extra)
        return self.finish('tick', 'STARTED', kind=decision['action'], job_id=decision.get('job_id'),
                           attempt=attempt, **extra)

    def prune(self) -> dict:
        argv = [self.control['python'], '-m', 'product_collector.prune', '--output', str(self.runs),
                '--keep', str(self.control['prune_keep']), '--apply']
        if self.control['prune_abandoned']:
            argv.append('--prune-abandoned')
        try:
            done = self.run(argv, cwd=self.control['source'], env=self.env(), capture_output=True, text=True,
                            timeout=PRUNE_TIMEOUT)
        except (OSError, subprocess.TimeoutExpired) as exc:
            self.alert('prune_failed', detail=f'{type(exc).__name__}: {exc}')
            return {'ok': False}
        report = None
        for line in reversed((done.stdout or '').splitlines()):
            try:
                report = json.loads(line)
                break
            except ValueError:
                continue
        if done.returncode != 0 or not isinstance(report, dict):
            self.alert('prune_failed', exit_code=done.returncode, detail=(done.stderr or '')[-300:])
            return {'ok': False, 'exit_code': done.returncode}
        summary = {'ok': True, 'deleted': report.get('deleted', [])}
        if report.get('images_pruned'):
            summary['images_pruned'] = [p.get('run') for p in report['images_pruned']]
        changed = sorted({run for key in ('kept', 'images_kept') for run, reason in (report.get(key) or {}).items()
                          if reason == 'loaded_inputs_changed'})
        if changed:
            # Loaded, then changed by hand (resume, image retry): what is on disk now was never loaded.
            self.alert('loaded_inputs_changed', runs=changed)
        return summary

    def close_lost_attempt(self, state: dict) -> None:
        """An attempt whose supervisor died before judging it is judged from its log and files."""
        attempt = state.get('attempt')
        if not attempt or attempt.get('done'):
            return
        line = self.result_line(Path(attempt['log']) if attempt.get('log') else None)
        # Recorded folder, else the one the result line names (a completed run drops the active pointer),
        # else the active pointer.
        directory = Path(attempt['run_dir']) if attempt.get('run_dir') else (self.run_from_line(line)
                                                                             or self.active_dir())
        if directory is not None and not (directory / 'run_config.json').is_file():
            directory = None
        job = self.job_of(directory) if directory else None
        ended = self.now()
        if directory is None and line is None:
            outcome, detail = 'crashed', 'no_run'
        else:
            outcome, detail = self.classify(None, line, directory)
        if outcome == 'completed' and job and not (state['runs'].get(job) or {}).get('finished_at'):
            # No supervisor time: the checkpoint's last write is the closest record of the end.
            finished = utc((read_json(directory / 'checkpoint_state.json') or {}).get('updated_at')) or ended
            state['runs'].setdefault(job, {})['finished_at'] = iso(finished)
        attempt.update(done=True, ended_at=iso(ended), outcome=outcome, recovered=True,
                       action=self.apply_outcome(state, job, outcome, detail, ended, directory).get('action'))

    def last_completed(self, state: dict) -> tuple[datetime | None, datetime | None]:
        """Start and finish of the newest completed run, from the ledger and the run folders."""
        best = (None, None)
        recorded = state.get('last_completed') or {}
        if utc(recorded.get('started_at')):
            best = (utc(recorded['started_at']), utc(recorded.get('finished_at')))
        for directory in self.run_dirs():
            if not self.completed(directory):
                continue
            started = self.started_at(state, directory)
            if started and (best[0] is None or started > best[0]):
                best = (started, self.finished_at(state, directory))
        return best

    def decide(self, state: dict) -> dict:
        now = self.now()
        wait = state.get('operator_wait')
        if wait:
            return {'action': 'operator_wait', 'reason': wait.get('reason'),
                    'job_id': wait.get('target') if wait.get('target') != 'pre_run' else None}
        pre = state['pre_run']
        directory = self.active_dir()
        if directory is not None and not (directory / 'run_config.json').is_file():
            state['operator_wait'] = {'reason': 'active_run_missing', 'at': iso(now), 'target': 'pre_run',
                                      'detail': str(directory)}
            return {'action': 'operator_wait', 'reason': 'active_run_missing'}
        if directory is None:
            # The output root is fixed for an unfinished run: a changed root must not hide it.
            last = Path((state.get('attempt') or {}).get('run_dir') or '.')
            if ((last / 'run_config.json').is_file() and last.parent.resolve() != self.runs.resolve()
                    and self.status_of(last) not in ('completed', 'abandoned')):
                state['operator_wait'] = {'reason': 'fixed_options_changed', 'at': iso(now),
                                          'target': self.job_of(last), 'detail': 'output'}
                return {'action': 'operator_wait', 'reason': 'fixed_options_changed', 'job_id': self.job_of(last)}
        if directory is not None:
            job = self.job_of(directory)
            entry = state['runs'].setdefault(job, {})
            started = self.started_at(state, directory)
            deadline = utc(entry.get('deadline')) or (
                started + timedelta(days=self.control['schedule']['max_run_days']) if started else None)
            if deadline and now > deadline:
                state['operator_wait'] = {'reason': 'max_run_days', 'at': iso(now), 'target': job}
                return {'action': 'operator_wait', 'reason': 'max_run_days', 'job_id': job}
            fixed = entry.get('fixed')
            if fixed and fixed != self.fixed_options():
                state['operator_wait'] = {'reason': 'fixed_options_changed', 'at': iso(now), 'target': job}
                return {'action': 'operator_wait', 'reason': 'fixed_options_changed', 'job_id': job}
            for hold in (entry.get('next_at'), pre.get('next_at')):
                if utc(hold) and now < utc(hold):
                    return {'action': 'wait', 'reason': 'backoff', 'job_id': job, 'next_at': hold}
            return {'action': 'resume', 'job_id': job}
        if utc(pre.get('next_at')) and now < utc(pre['next_at']):
            return {'action': 'wait', 'reason': 'backoff', 'next_at': pre['next_at']}
        started, finished = self.last_completed(state)
        if started and not state.get('start_now'):
            schedule = self.control['schedule']
            due = max(started + timedelta(days=schedule['period_days']),
                      (finished or started) + timedelta(hours=schedule['rest_hours']))
            if now < due:
                return {'action': 'wait', 'reason': 'schedule', 'next_at': iso(due)}
        return {'action': 'new'}

    def preflight(self, decision: dict) -> dict | None:
        """Disk, template, image store settings and one listing request before starting the collector."""
        now = self.now()
        job = decision.get('job_id')
        if not Path(self.control['pagination_template']).is_file():
            return self.hold_operator(job, 'template_missing')
        store = self.control.get('image_store_config')
        if store:
            path = Path(store)
            if not path.is_file() or (os.name == 'posix' and path.stat().st_mode & 0o077):
                return self.hold_operator(job, 'image_store_config_unreadable_or_open')
        need = self.control['disk']['resume_free_gb' if decision['action'] == 'resume' else 'new_run_free_gb']
        free = self.disk_free(self.runs)
        if free < need * 1024 ** 3:
            self.alert('disk_low', free_gb=round(free / 1024 ** 3, 1), need_gb=need, runs=self.run_sizes())
            return self.hold_operator(job, 'disk_low')
        if self.active_unloaded_completed():
            self.alert('previous_run_not_loaded', jobs=self.active_unloaded_completed())
        argv = [self.control['python'], '-m', 'product_collector', '--check-pagination',
                '--pagination-template', self.control['pagination_template']]
        try:
            done = self.run(argv, cwd=self.control['source'], env=self.env(), capture_output=True, text=True,
                            timeout=CHECK_TIMEOUT)
            code = done.returncode
        except (OSError, subprocess.TimeoutExpired):
            code = CHECK_REQUEST_FAILED
        with self.state() as state:
            pre = state['pre_run']
            if code == CHECK_OK:
                for key in ('check_failures', 'check_blocked'):
                    pre.pop(key, None)
                return None
            if code == CHECK_BLOCKED:
                pre['check_blocked'] = pre.get('check_blocked', 0) + 1
                waits = self.control['backoff_hours']['blocked']
                if pre['check_blocked'] >= len(waits):
                    state['operator_wait'] = {'reason': f"check_blocked_{pre['check_blocked']}_times",
                                              'at': iso(now), 'target': 'pre_run'}
                    return {'status': 'OPERATOR_WAIT', 'reason': state['operator_wait']['reason']}
                pre['next_at'] = iso(now + timedelta(hours=waits[pre['check_blocked'] - 1]))
                return {'status': 'WAIT', 'reason': 'check_blocked', 'next_at': pre['next_at']}
            if code == CHECK_REQUEST_FAILED:
                pre['check_failures'] = pre.get('check_failures', 0) + 1
                if pre['check_failures'] >= 3:
                    state['operator_wait'] = {'reason': 'check_request_failed_3_times', 'at': iso(now),
                                              'target': 'pre_run'}
                    return {'status': 'OPERATOR_WAIT', 'reason': 'check_request_failed_3_times'}
                return {'status': 'WAIT', 'reason': 'check_request_failed'}
            reason = 'template_expired' if code == CHECK_PARSE_FAILED else f'check_exit_{code}'
            state['operator_wait'] = {'reason': reason, 'at': iso(now), 'target': 'pre_run'}
            return {'status': 'OPERATOR_WAIT', 'reason': reason}

    def hold_operator(self, job, reason) -> dict:
        with self.state() as state:
            state['operator_wait'] = {'reason': reason, 'at': iso(self.now()), 'target': job or 'pre_run'}
        return {'status': 'OPERATOR_WAIT', 'reason': reason}

    def run_sizes(self) -> dict:
        sizes = {}
        for directory in self.run_dirs():
            total = sum(p.stat().st_size for p in directory.rglob('*') if p.is_file())
            sizes[directory.name] = {'gb': round(total / 1024 ** 3, 2), 'status': self.status_of(directory),
                                     'loaded': (directory / LOADED_MARKER).exists()}
        return sizes

    def active_unloaded_completed(self) -> list[str]:
        return [self.job_of(d) for d in self.run_dirs()
                if self.completed(d) and not (d / LOADED_MARKER).exists()]

    def env(self) -> dict:
        env = dict(os.environ)
        env['PYTHONPATH'] = self.control['source']
        return env

    def start(self, kind: str, attempt: str) -> bool:
        """Start supervise and wait until it has recorded the attempt."""
        if kind == 'new':
            with self.state() as state:
                state.pop('start_now', None)
        self.spawner(kind, attempt)
        deadline = time.monotonic() + READY_SECONDS
        while time.monotonic() < deadline:
            if (self.read_state().get('attempt') or {}).get('id') == attempt:
                return True
            time.sleep(0.2)
        return False

    def _spawn_detached(self, kind: str, attempt: str) -> None:
        logs = self.root / 'logs' / 'launcher'
        logs.mkdir(parents=True, exist_ok=True)
        stamp = self.now().strftime('%Y%m%dT%H%M%SZ')
        with (logs / f'supervise-{stamp}-{attempt}.log').open('ab') as log:
            options = dict(start_new_session=True) if os.name == 'posix' else {}
            subprocess.Popen([self.control['python'], '-m', 'product_collector.launcher',
                              '--control', str(self.control_path), 'supervise', '--kind', kind, '--attempt', attempt],
                             cwd=self.control['source'], env=self.env(), stdin=subprocess.DEVNULL, stdout=log,
                             stderr=log, close_fds=True, **options)

    # ----- supervise -------------------------------------------------------------------------------

    def collector_argv(self, kind: str) -> list[str]:
        c = self.control
        argv = [c['python'], '-m', 'product_collector', '--categories', c['categories'],
                '--pagination-template', c['pagination_template'], '--output', c['output'],
                '--workers', str(c['workers']), '--listing-interval', str(c['listing_interval']),
                '--interval', str(c['interval']), '--download-images', '--image-overlap', c['image_overlap'],
                '--detail-refresh', c['detail_refresh'], '--refresh-sample', str(c['refresh_sample']),
                '--quality-gate', c['quality_gate']]
        if c.get('image_store_config'):
            argv += ['--image-store-config', c['image_store_config']]
        if kind == 'new':
            argv.append('--new-run')
        return argv

    def supervise(self, kind: str, attempt: str, popen=None, poll: float = 2.0) -> int:
        try:
            self.load()
        except ConfigError as exc:
            return self.finish('supervise', 'CONFIG_INVALID', EXIT_CONFIG, reason=str(exc))
        self.home.mkdir(parents=True, exist_ok=True)
        with try_lock(self.home / 'supervisor.lock') as held:
            if not held:
                return self.finish('supervise', 'SUPERVISOR_BUSY', record=False)
            return self._supervise(kind, attempt, popen or subprocess.Popen, poll)

    def _supervise(self, kind: str, attempt: str, popen, poll: float) -> int:
        started = self.now()
        previous = self.active_dir()
        job = self.job_of(previous) if kind == 'resume' and previous else None
        log = self.root / 'logs' / (job or 'new') / f"{started.strftime('%Y%m%dT%H%M%SZ')}-{attempt}.log"
        log.parent.mkdir(parents=True, exist_ok=True)
        with self.state() as state:
            state['attempt'] = {'id': attempt, 'kind': kind, 'started_at': iso(started), 'pid': os.getpid(),
                                'log': str(log), 'job_id': job, 'run_dir': str(previous) if job else None,
                                'options': {k: self.control[k] for k in
                                            ('workers', 'listing_interval', 'interval', 'image_overlap')}}
        stop = {'requested': False}
        with log.open('ab') as stream:
            child = popen(self.collector_argv(kind), cwd=self.control['source'], env=self.env(),
                          stdin=subprocess.DEVNULL, stdout=stream, stderr=subprocess.STDOUT, close_fds=True)

            def forward(signum, frame):
                # NiFi stop, `launcher stop`: the collector saves its run as interrupted.
                stop['requested'] = True
                try:
                    if os.name == 'posix':
                        os.kill(child.pid, signal.SIGINT)
                    else:
                        child.terminate()
                except OSError:
                    pass

            previous_handler = None
            if hasattr(signal, 'SIGTERM'):
                try:
                    previous_handler = signal.signal(signal.SIGTERM, forward)
                except ValueError:
                    previous_handler = None  # not the main thread (tests)
            try:
                while True:
                    try:
                        code = child.wait(timeout=poll)
                        break
                    except subprocess.TimeoutExpired:
                        self.note_run(kind, attempt, previous)
            finally:
                if previous_handler is not None:
                    signal.signal(signal.SIGTERM, previous_handler)
        ended = self.now()
        self.note_run(kind, attempt, previous)
        line = self.result_line(log)
        with self.state() as state:
            record = state.get('attempt') or {}
            directory = Path(record['run_dir']) if record.get('run_dir') else self.run_from_line(line)
            job = self.job_of(directory) if directory else None
            if job:
                # A collector that ended between two polls was never noted while it ran.
                entry = state['runs'].setdefault(job, {})
                entry.setdefault('fixed', self.fixed_options())
                if kind == 'new' and not entry.get('started_at'):
                    entry['started_at'] = record.get('started_at')
            outcome, detail = self.classify(code, line, directory)
            if outcome == 'completed' and job:
                state['runs'][job]['finished_at'] = iso(ended)
            action = self.apply_outcome(state, job, outcome, detail, ended, directory)
            record.update(done=True, ended_at=iso(ended), exit_code=code, outcome=outcome, job_id=job,
                          run_dir=str(directory) if directory else None, stop_requested=stop['requested'],
                          action=action.get('action'))
            state['attempt'] = record
        return self.finish('supervise', outcome.upper(), job_id=job, exit_code=code, **{
            k: v for k, v in action.items() if k not in ('outcome',)})

    def note_run(self, kind: str, attempt: str, previous: Path | None) -> None:
        """Link the attempt to its run folder once the collector made it the active run."""
        directory = self.active_dir()
        if directory is None or not (directory / 'run_config.json').is_file():
            return
        if kind == 'new' and previous is not None and directory.resolve() == previous.resolve():
            return  # still the pointer from before
        job = self.job_of(directory)
        with self.state() as state:
            record = state.get('attempt') or {}
            if record.get('id') != attempt or (record.get('run_dir') and record.get('noted')):
                return
            record.update(job_id=job, run_dir=str(directory), noted=True)
            entry = state['runs'].setdefault(job, {})
            if kind == 'new' and not entry.get('started_at'):
                entry['started_at'] = record['started_at']
            entry.setdefault('fixed', self.fixed_options())
            if not entry.get('started_at'):
                started = utc((read_json(directory / 'run_report.json') or {}).get('started_at'))
                if started:
                    entry['started_at'] = iso(started)

    # ----- operator commands -----------------------------------------------------------------------

    def status(self) -> int:
        try:
            self.load()
        except ConfigError as exc:
            return self.finish('status', 'CONFIG_INVALID', EXIT_CONFIG, record=False, reason=str(exc))
        state = self.read_state()
        active = self.active_dir()
        return self.finish('status', 'OK', record=False, enabled=self.control['enabled'],
                           hold=(self.root / 'HOLD').exists(),
                           supervisor_alive=lock_busy(self.home / 'supervisor.lock'),
                           collector_alive=self.runs.is_dir() and lock_busy(self.runs / '.collection.lock'),
                           active_run=str(active) if active else None,
                           operator_wait=state.get('operator_wait'), attempt=state.get('attempt'),
                           pre_run=state.get('pre_run'), last=read_json(self.home / 'last.json'))

    def release(self, start_now: bool = False) -> int:
        try:
            self.load()
        except ConfigError as exc:
            return self.finish('release', 'CONFIG_INVALID', EXIT_CONFIG, reason=str(exc))
        with self.state() as state:
            wait = state.pop('operator_wait', None)
            target = (wait or {}).get('target')
            entry = state['pre_run'] if target in (None, 'pre_run') else state['runs'].setdefault(target, {})
            for key in ('streak', 'last_cause', 'next_at', 'check_failures', 'check_blocked'):
                entry.pop(key, None)
            if wait and wait.get('reason') == 'max_run_days':
                entry['deadline'] = iso(self.now() + timedelta(days=RELEASE_EXTENSION_DAYS))
            if start_now:
                state['start_now'] = True
        return self.finish('release', 'RELEASED' if wait else 'NOTHING_TO_RELEASE', released=wait,
                           start_now=start_now)

    def stop(self) -> int:
        """SIGTERM to the supervisor; it passes SIGINT on, and the collector saves the run as interrupted."""
        state = self.read_state()
        attempt = state.get('attempt') or {}
        if not lock_busy(self.home / 'supervisor.lock') or not attempt.get('pid') or attempt.get('done'):
            return self.finish('stop', 'NOT_RUNNING')
        try:
            os.kill(attempt['pid'], signal.SIGTERM)
        except OSError as exc:
            return self.finish('stop', 'STOP_FAILED', EXIT_ERROR, reason=str(exc))
        return self.finish('stop', 'STOP_SENT', job_id=attempt.get('job_id'), pid=attempt['pid'])

    def check(self) -> int:
        """Installation check without network: control file, release, template, categories."""
        try:
            self.load()
        except ConfigError as exc:
            return self.finish('check', 'CONFIG_INVALID', EXIT_CONFIG, record=False, reason=str(exc))
        source = Path(self.control['source'])
        missing = [name for name, path in (('python', Path(self.control['python'])),
                                           ('collector', source / 'product_collector' / '__main__.py'),
                                           ('categories', Path(self.control['categories'])),
                                           ('pagination_template', Path(self.control['pagination_template'])))
                   if not path.is_file()]
        if missing:
            return self.finish('check', 'DEPLOYMENT_INCOMPLETE', EXIT_CONFIG, record=False, missing=missing)
        return self.finish('check', 'CONFIGURED', record=False, enabled=self.control['enabled'],
                           output=self.control['output'], source_commit=self.control['source_commit'],
                           network_requests=0)


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description='다나와 수집 실행기(정책 4). NiFi가 tick을 주기적으로 부른다')
    parser.add_argument('--control', type=Path, default=DEFAULT_CONTROL, help='제어 파일(contract_version 2)')
    parser.add_argument('--check', action='store_true', help='설치 확인(네트워크 요청 없음)')
    commands = parser.add_subparsers(dest='command')
    commands.add_parser('tick', help='할 일 하나를 정하고 몇 초 안에 끝남')
    supervise = commands.add_parser('supervise', help='tick이 분리 실행. 직접 부르지 않는다')
    supervise.add_argument('--kind', choices=('new', 'resume'), required=True)
    supervise.add_argument('--attempt', required=True)
    commands.add_parser('status', help='현재 상태(네트워크 요청 없음)')
    release = commands.add_parser('release', help='운영자 대기 해제')
    release.add_argument('--start-now', action='store_true', help='7일·휴지 조건을 한 번 건너뛰고 새 실행')
    commands.add_parser('stop', help='감독 프로세스를 멈춤(수집은 interrupted로 저장). HOLD와 함께 쓴다')
    args = parser.parse_args(argv)
    launcher = Launcher(args.control)
    try:
        if args.check:
            return launcher.check()
        if args.command == 'tick':
            return launcher.tick()
        if args.command == 'supervise':
            return launcher.supervise(args.kind, args.attempt)
        if args.command == 'status':
            return launcher.status()
        if args.command == 'release':
            return launcher.release(args.start_now)
        if args.command == 'stop':
            return launcher.stop()
    except Exception as exc:  # the last line still tells the monitor what happened
        return launcher.finish(args.command or 'check', 'LAUNCHER_ERROR', EXIT_ERROR,
                               record=launcher.home.is_dir(), reason=f'{type(exc).__name__}: {exc}')
    parser.print_usage(sys.stderr)
    return EXIT_CONFIG


if __name__ == '__main__':
    raise SystemExit(main())
