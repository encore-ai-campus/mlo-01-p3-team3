"""Durable monotonic stage timings with counts and redacted error metadata."""
from collections import defaultdict
from contextlib import contextmanager
from contextvars import ContextVar
from datetime import datetime, timezone
from pathlib import Path
import time
from threading import RLock
from uuid import uuid4

from ..domain.contracts import digest, seal, verified
from .files import immutable, read, write


class Timing:
    def __init__(self, state_root):
        self.root=Path(state_root).resolve()/'metrics'/'stage-times'
        self.run_id=str(uuid4())
        self._active_scope=ContextVar('pipeline_v2_active_scope_'+self.run_id,default=None)
        self._lock=RLock();self._pending=defaultdict(list)

    @property
    def active_scope(self):return self._active_scope.get()

    def _directory(self, scope_id):
        return self.root/digest(scope_id)

    @contextmanager
    def measure(self, stage, scope_id, *, job_id=None, counters=None):
        start=time.perf_counter_ns()
        started=datetime.now(timezone.utc).isoformat(timespec='milliseconds').replace('+00:00','Z')
        before={k:int(v) for k,v in (counters or {}).items() if type(v) is int}
        token=self._active_scope.set(scope_id)
        outcome='OK';error=None
        try:
            yield
        except BaseException as exc:
            outcome='ERROR';error=type(exc).__name__
            raise
        finally:
            elapsed=max(0,(time.perf_counter_ns()-start)//1_000_000)
            after={k:int(v) for k,v in (counters or {}).items() if type(v) is int}
            event=seal(dict(run_id=self.run_id,stage=stage,started_at=started,
                finished_at=datetime.now(timezone.utc).isoformat(timespec='milliseconds').replace('+00:00','Z'),
                elapsed_ms=elapsed,outcome=outcome,error_type=error,
                scope_hash=digest(scope_id),job_hash=digest(job_id) if job_id else None,
                counts={k:v-before.get(k,0) for k,v in after.items()}))
            try:self._record(scope_id,event,durable=outcome=='ERROR' or stage in {'capture.total','tick.total','status.total','run.total','capture.command_total'})
            finally:self._active_scope.reset(token)

    def iterate(self, stage, scope_id, values):
        start=time.perf_counter_ns();started=datetime.now(timezone.utc).isoformat(timespec='milliseconds').replace('+00:00','Z')
        count=0;outcome='OK';error=None
        token=self._active_scope.set(scope_id)
        try:
            for value in values:
                count+=1
                yield value
        except BaseException as exc:
            outcome='ERROR';error=type(exc).__name__
            raise
        finally:
            event=seal(dict(run_id=self.run_id,stage=stage,started_at=started,
                finished_at=datetime.now(timezone.utc).isoformat(timespec='milliseconds').replace('+00:00','Z'),
                elapsed_ms=max(0,(time.perf_counter_ns()-start)//1_000_000),outcome=outcome,error_type=error,
                scope_hash=digest(scope_id),job_hash=None,counts={'items':count}))
            try:self._record(scope_id,event,durable=outcome=='ERROR')
            finally:self._active_scope.reset(token)

    def _record(self,scope_id,event,durable=False):
        with self._lock:
            self._pending[scope_id].append(event)
            if durable or len(self._pending[scope_id])>=32:self._flush(scope_id)

    def _flush(self,scope_id):
        values=self._pending.pop(scope_id,[])
        if values:
            directory=self._directory(scope_id)
            immutable(directory/(uuid4().hex+'.json'),seal(dict(events=values)))

    def summary(self, scope_id):
        with self._lock:self._flush(scope_id)
        directory=self._directory(scope_id)
        events=[]
        if directory.exists():
            for path in sorted(directory.glob('*.json')):
                if path.name=='summary.json':continue
                batch=read(path);verified(batch)
                if 'events' in batch:events.extend(batch['events'])
                else:events.append(batch) # Read timing files written before event batching was introduced.
        for event in events:verified(event)
        totals=defaultdict(lambda:dict(events=0,total_ms=0,max_ms=0,errors=0,counts=defaultdict(int)))
        for event in events:
            value=totals[event['stage']];value['events']+=1
            value['total_ms']+=event['elapsed_ms'];value['max_ms']=max(value['max_ms'],event['elapsed_ms'])
            value['errors']+=event['outcome']=='ERROR'
            for name,count in event['counts'].items():value['counts'][name]+=count
        stages={name:{**value,'counts':dict(value['counts'])} for name,value in sorted(totals.items())}
        run_totals=[e['elapsed_ms'] for e in events if e['stage']=='run.total' and e['outcome']=='OK']
        report=dict(scope_hash=digest(scope_id),event_count=len(events),stages=stages,
            run_wall_ms=sum(run_totals),timing_model='Monotonic stage spans are inclusive; nested stage durations must not be summed.')
        write(directory/'summary.json',seal(report))
        return report
