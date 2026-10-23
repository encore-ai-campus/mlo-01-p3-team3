"""Durable spans; nested work and reported cross-process boundaries stay distinct."""
from collections import Counter, defaultdict
from contextlib import contextmanager
from contextvars import ContextVar
from datetime import datetime
from math import ceil
from pathlib import Path
from threading import RLock, get_ident
import time
from uuid import uuid4

from ..domain.contracts import digest, seal, verified, stamp, utc, Invalid
from .files import immutable, read, write


def percentile(histogram, fraction):
    target = max(1, ceil(sum(histogram.values()) * fraction))
    count = 0
    for value, frequency in sorted(histogram.items()):
        count += frequency
        if count >= target:
            return value
    return None


class Timing:
    BACKLOG = 4096

    def __init__(self, state_root):
        self.failures = 0
        self.root = Path(state_root).resolve() / 'metrics' / 'stage-times'
        self.run_id = str(uuid4())
        self._stack = ContextVar('pipeline_spans_' + self.run_id, default=())
        self._lock = RLock()
        self._pending = defaultdict(list)
        self._last_flush = time.monotonic()
        self._scope_hashes = {}

    @property
    def active_scope(self):
        spans = self._stack.get()
        return spans[-1]['scope'] if spans else None

    @property
    def active_span_id(self):
        spans = self._stack.get()
        return spans[-1]['id'] if spans else None

    def _directory(self, scope_id):
        return self.root / self._scope_hash(scope_id)

    def _scope_hash(self, scope_id):
        if scope_id not in self._scope_hashes:
            if len(self._scope_hashes) >= 128:
                self._scope_hashes.clear()
            self._scope_hashes[scope_id] = digest(scope_id)
        return self._scope_hashes[scope_id]

    @contextmanager
    def measure(self, stage, scope_id, *, job_id=None, counters=None, counts=None, partition=None):
        stack = self._stack.get()
        parent = stack[-1] if stack and stack[-1]['scope'] == scope_id and stack[-1]['thread'] == get_ident() else None
        span = dict(id=uuid4().hex, scope=scope_id, thread=get_ident(), child_ns=0)
        before = dict(counters or {})
        token = self._stack.set((*stack, span))
        started = stamp()
        start = time.perf_counter_ns()
        outcome, error = 'OK', None
        values = dict(counts or {})
        try:
            yield values
        except BaseException as exc:
            outcome, error = 'ERROR', type(exc).__name__
            raise
        finally:
            elapsed = max(0, time.perf_counter_ns() - start)
            if parent is not None:
                parent['child_ns'] += elapsed
            for name, value in (counters or {}).items():
                if type(value) is int:
                    values[name] = value - before.get(name, 0)
            event = seal(dict(schema_version=2, run_id=self.run_id, span_id=span['id'],
                parent_span_id=parent['id'] if parent else None, stage=stage,
                started_at=started, finished_at=stamp(), elapsed_ms=elapsed / 1_000_000,
                exclusive_ms=max(0, elapsed - span['child_ns']) / 1_000_000,
                clock='monotonic', outcome=outcome, error_type=error, scope_hash=self._scope_hash(scope_id),
                job_hash=digest(job_id) if job_id else None, partition=partition,
                counts={k:v for k,v in values.items() if type(v) is int}))
            self._stack.reset(token)
            self._record(scope_id, event, durable=outcome == 'ERROR' or stage in {
                'command.total','run.total','tick.total','capture.total','batch.total'})

    def iterate(self, stage, scope_id, values):
        # next() excludes work performed by the consumer between yields.
        # Spans opened inside next() (for example a lazy session start) already credit the parent;
        # their time is kept out of this metric's exclusive time and out of the parent credit.
        iterator = iter(values)
        started, elapsed, nested, count, error = stamp(), 0, 0, 0, None

        def parent():
            stack = self._stack.get()
            return stack[-1] if stack and stack[-1]['scope'] == scope_id and stack[-1]['thread'] == get_ident() else None
        try:
            while True:
                owner = parent()
                credited = owner['child_ns'] if owner else 0
                begin = time.perf_counter_ns()
                try:
                    value = next(iterator)
                except StopIteration:
                    return
                finally:
                    elapsed += time.perf_counter_ns()-begin
                    if owner:
                        nested += owner['child_ns']-credited
                count += 1
                yield value
        except BaseException as exc:
            error = type(exc).__name__
            raise
        finally:
            if hasattr(iterator, 'close'):
                iterator.close()
            self._record(scope_id,seal(dict(schema_version=2,run_id=self.run_id,span_id=uuid4().hex,
                parent_span_id=self.active_span_id,stage=stage,started_at=started,finished_at=stamp(),
                elapsed_ms=max(0,elapsed)/1_000_000,exclusive_ms=max(0,elapsed-nested)/1_000_000,
                clock='monotonic_accumulated',outcome='ERROR' if error else 'OK',error_type=error,
                scope_hash=self._scope_hash(scope_id),job_hash=None,partition=None,counts={'items':count})),durable=bool(error))
            owner = parent()
            if owner:
                owner['child_ns'] += max(0,elapsed-nested)

    def boundary(self, stage, scope_id, started_at, finished_at, *, identity, counts=None):
        start, end = utc(started_at), utc(finished_at)
        elapsed = (datetime.fromisoformat(end.replace('Z', '+00:00')) -
                   datetime.fromisoformat(start.replace('Z', '+00:00'))).total_seconds() * 1000
        if elapsed < 0:
            raise Invalid('METRIC_BOUNDARY_ORDER_INVALID')
        event = seal(dict(schema_version=2, run_id=None, span_id=digest(dict(stage=stage, identity=identity)),
            parent_span_id=None, stage=stage, started_at=start, finished_at=end, elapsed_ms=elapsed,
            exclusive_ms=None, clock='reported_utc', outcome='OK', error_type=None,
            scope_hash=digest(scope_id), job_hash=None, partition=None, counts=counts or {}))
        immutable(self._directory(scope_id) / ('boundary-' + event['span_id'] + '.json'), event)

    def _record(self, scope_id, event, durable=False):
        with self._lock:
            self._pending[scope_id].append(event)
            if durable or len(self._pending[scope_id]) >= 512 or time.monotonic()-self._last_flush >= 5:
                self._flush(scope_id)

    def _flush(self, scope_id):
        values = self._pending.get(scope_id, [])
        if values:
            try:
                immutable(self._directory(scope_id) / (uuid4().hex + '.json'), seal(dict(events=values)))
            except Exception:
                # Measurement is evidence about the work, never a condition of it. Keep a bounded
                # backlog for the next flush and let the caller see the count.
                self.failures += 1
                del values[:-self.BACKLOG]
                self._last_flush = time.monotonic()
                return
            self._pending.pop(scope_id, None)
            self._last_flush = time.monotonic()

    def flush(self):
        with self._lock:
            for scope in list(self._pending):
                self._flush(scope)

    def summary(self, scope_id, *, persist=True):
        with self._lock:
            self._flush(scope_id)
        directory = self._directory(scope_id)
        totals = {}
        event_count, run_wall = 0, 0
        for path in sorted(directory.glob('*.json')):
            if path.name == 'summary.json':
                continue
            batch = verified(read(path))
            for event in batch.get('events', [batch]):
                verified(event)
                event_count += 1
                key = event['stage']
                value = totals.setdefault(key, dict(events=0, total_ms=0, exclusive_ms=0,
                    max_ms=0, errors=0, counts=Counter(), histogram=Counter(), successful_histogram=Counter()))
                duration = event['elapsed_ms']
                value['events'] += 1
                value['total_ms'] += duration
                value['exclusive_ms'] += event.get('exclusive_ms') or 0
                value['max_ms'] = max(value['max_ms'], duration)
                value['errors'] += event['outcome'] == 'ERROR'
                value['histogram'][round(duration, 3)] += 1
                if event['outcome'] == 'OK':
                    value['successful_histogram'][round(duration, 3)] += 1
                value['counts'].update(event['counts'])
                if key == 'run.total' and event['outcome'] == 'OK':
                    run_wall += duration
        stages = {}
        for name, value in sorted(totals.items()):
            histogram = value.pop('histogram')
            successful = value.pop('successful_histogram')
            stages[name] = dict(value, counts=dict(value['counts']),
                p50_ms=percentile(histogram, .5), p95_ms=percentile(histogram, .95),
                successful_p50_ms=percentile(successful, .5), successful_p95_ms=percentile(successful, .95))
        report = dict(scope_hash=digest(scope_id), event_count=event_count, stages=stages,
            run_wall_ms=run_wall, percentile_method='nearest-rank, rounded to microseconds',
            timing_model='Inclusive spans overlap. Exclusive time excludes synchronous children. '
                'Reported UTC boundaries include waits and require synchronized clocks.')
        if persist:
            write(directory / 'summary.json', seal(report))
        return report
