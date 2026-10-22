"""Single-owner file queue. OS locks serialize writers; task receipts survive restart.

RUNNING tasks are NEVER automatically stolen after a timeout. Recover only after
the operator has fenced/stopped the prior worker. Distributed scheduling uses one
shared queue on the NiFi primary node, not independent queues on every host.
"""
from datetime import datetime, timezone, timedelta
from pathlib import Path
import time
from uuid import uuid4
import re

from ..contracts import digest, now, utc
from ..errors import Conflict, PipelineError
from ..files import atomic_json, lock, read_json

KINDS = {'INGEST', 'PUBLISH', 'RECEIVE', 'RECONCILE', 'FLOW'}
TERMINAL = {'SUCCEEDED', 'RETRY_EXHAUSTED', 'QUARANTINED'}


class RetryLater(PipelineError):
    def __init__(self, code='RETRY_REQUIRED', retry_after_seconds=None):
        super().__init__(code)
        self.retry_after_seconds = retry_after_seconds


class Queue:
    def __init__(self, root, target, *, clock=now):
        self.root, self.target, self.clock = Path(root).resolve(), target, clock
        if not isinstance(target,str) or not re.fullmatch(r'[A-Za-z0-9_.-]{1,100}',target): raise PipelineError('QUEUE_TARGET_REQUIRED')

    def _read(self, key):
        if not isinstance(key,str) or len(key)!=64 or any(c not in '0123456789abcdef' for c in key):
            raise PipelineError('QUEUE_KEY_INVALID')
        row=read_json(self.root/'tasks'/(key+'.json'))
        if (row.get('task_id')!=key or row.get('target')!=self.target or row.get('spec_hash')!=digest(row['spec']) or
            key!=digest(dict(kind=row['kind'],spec=row['spec'],target=self.target))):
            raise Conflict('QUEUE_TASK_CHANGED')
        return row

    def _save(self, row): atomic_json(self.root/'tasks'/(row['task_id']+'.json'),row)

    def _binding(self):
        path=self.root/'target.json'; expected=dict(contract_version=1,target=self.target)
        if path.exists() and read_json(path)!=expected: raise Conflict('QUEUE_TARGET_MISMATCH')
        if not path.exists(): atomic_json(path,expected)

    def enqueue(self, kind, spec, *, max_attempts=5, not_before=None):
        if kind not in KINDS or not isinstance(spec,dict) or type(max_attempts) is not int or not 1<=max_attempts<=100:
            raise PipelineError('QUEUE_TASK_INVALID')
        # Specs are immutable references and hashes, never secrets or a shell command.
        if any(k.lower() in {'password','token','secret','command','shell','uri'} for k in spec):
            raise PipelineError('QUEUE_SECRET_OR_COMMAND_FORBIDDEN')
        key=digest(dict(kind=kind,spec=spec,target=self.target))
        with lock(self.root/'queue.lock'):
            self._binding()
            if (self.root/'tasks'/(key+'.json')).exists():
                row=self._read(key)
                if row['max_attempts']!=max_attempts:raise Conflict('QUEUE_POLICY_CHANGED')
                return row
            row=dict(task_id=key,kind=kind,spec=spec,spec_hash=digest(spec),target=self.target,
                     status='READY',attempts=0,max_attempts=max_attempts,retry_limit=max_attempts,next_retry_at=utc(not_before or self.clock()),
                     created_at=self.clock(),attempt_token=None)
            self._save(row);return row

    def claim(self):
        with lock(self.root/'queue.lock'):
            self._binding()
            candidates=[self._read(p.stem) for p in sorted((self.root/'tasks').glob('*.json'))]
            eligible=[r for r in candidates if r['status'] in {'READY','RETRY_WAIT'} and
                      utc(r['next_retry_at'])<=utc(self.clock())]
            if not eligible:return None
            row=min(eligible,key=lambda r:(r['next_retry_at'],r['task_id']))
            row.update(status='RUNNING',attempts=row['attempts']+1,attempt_token=str(uuid4()),started_at=self.clock())
            self._save(row);return row

    def summary(self):
        with lock(self.root/'queue.lock'):
            self._binding();counts={}
            for path in (self.root/'tasks').glob('*.json'):
                state=self._read(path.stem)['status'];counts[state]=counts.get(state,0)+1
            return counts

    def finish(self, key, token, *, result=None, error=None, retryable=False, retry_after=None, duration_ms=0):
        with lock(self.root/'queue.lock'):
            row=self._read(key)
            if row['status']!='RUNNING' or row['attempt_token']!=token:raise Conflict('QUEUE_FENCE_LOST')
            if error is None:
                row.update(status='SUCCEEDED',result=result,next_retry_at=None)
            elif not retryable:
                row.update(status='QUARANTINED',next_retry_at=None,last_error=error)
            elif row['attempts']>=row.get('retry_limit',row['max_attempts']):
                row.update(status='RETRY_EXHAUSTED',next_retry_at=None,last_error=error)
            else:
                # Retry-After is a minimum; never shorten the provider's requested wait.
                if retry_after is not None and (type(retry_after) not in (int,float) or not 0<=retry_after<=86400*30):
                    raise PipelineError('RETRY_AFTER_INVALID')
                delay=max(min(3600,2**min(row['attempts'],12)),retry_after or 0)
                due=datetime.fromisoformat(utc(self.clock()).replace('Z','+00:00'))+timedelta(seconds=delay)
                row.update(status='RETRY_WAIT',next_retry_at=due.isoformat(),last_error=error)
            row.update(finished_at=self.clock(),duration_ms=duration_ms)
            self._save(row)
            atomic_json(self.root/'attempts'/key/(token+'.json'),dict(task_id=key,attempt_token=token,
                attempt=row['attempts'],status=row['status'],error_code=error,duration_ms=duration_ms,
                finished_at=row['finished_at']))
            return row

    def recover(self, key, expected_token, stop_evidence):
        if not isinstance(stop_evidence,str) or not stop_evidence.strip():raise PipelineError('WORKER_STOP_EVIDENCE_REQUIRED')
        with lock(self.root/'worker.lock'),lock(self.root/'queue.lock'):
            row=self._read(key)
            if row['status']!='RUNNING' or row['attempt_token']!=expected_token:raise Conflict('QUEUE_RECOVERY_CONFLICT')
            row.update(status='RETRY_EXHAUSTED' if row['attempts']>=row.get('retry_limit',row['max_attempts']) else 'RETRY_WAIT',
                next_retry_at=self.clock(),recovery_evidence=stop_evidence,attempt_token=None)
            self._save(row);return row

    def retry_exhausted(self,key,evidence,additional_attempts=1):
        if not evidence or type(additional_attempts) is not int or not 1<=additional_attempts<=100:
            raise PipelineError('MANUAL_RETRY_EVIDENCE_REQUIRED')
        with lock(self.root/'worker.lock'),lock(self.root/'queue.lock'):
            row=self._read(key)
            if row['status']!='RETRY_EXHAUSTED':raise Conflict('MANUAL_RETRY_ONLY_EXHAUSTED')
            row.update(status='RETRY_WAIT',retry_limit=row['attempts']+additional_attempts,next_retry_at=self.clock(),
                       retry_evidence=evidence,attempt_token=None)
            self._save(row);return row

    def tick(self, handlers, *, limit=10):
        if type(limit) is not int or not 1<=limit<=1000:raise PipelineError('QUEUE_LIMIT_INVALID')
        report=dict(processed=0,succeeded=0,retry_wait=0,quarantined=0,retry_exhausted=0,recovered_succeeded=0)
        started=time.monotonic()
        # Covers the WHOLE child operation, not just the state transition.
        with lock(self.root/'worker.lock'):
            for _ in range(limit):
                row=self.claim()
                if row is None:break
                began=time.monotonic()
                try:
                    handler=handlers.get(row['kind'])
                    if handler is None:raise PipelineError('QUEUE_HANDLER_UNSUPPORTED')
                    result=handler(row['spec'])
                except Exception as exc:
                    from ..retry import retryable as transient
                    code=exc.code if isinstance(exc,PipelineError) else type(exc).__name__
                    sql_retry=False
                    provider_retry=False;retry_after=getattr(exc,'retry_after_seconds',None)
                    response=getattr(exc,'response',None)
                    if isinstance(response,dict):
                        status=response.get('ResponseMetadata',{}).get('HTTPStatusCode')
                        provider_retry=status in {408,429,500,502,503,504} or response.get('Error',{}).get('Code') in {
                            'SlowDown','Throttling','RequestTimeout','ServiceUnavailable','InternalError'}
                        header=response.get('ResponseMetadata',{}).get('HTTPHeaders',{}).get('retry-after')
                        if header:
                            try:retry_after=float(header)
                            except ValueError:
                                from email.utils import parsedate_to_datetime
                                retry_after=max(0,(parsedate_to_datetime(header)-datetime.fromisoformat(utc(self.clock()).replace('Z','+00:00'))).total_seconds())
                    try:
                        from pymysql.err import OperationalError
                        sql_retry=isinstance(exc,OperationalError) and exc.args[0] in {1040,1203,1205,1213,2006,2013}
                    except ImportError:pass
                    finished=self.finish(row['task_id'],row['attempt_token'],error=code,
                        retryable=isinstance(exc,RetryLater) or transient(exc) or sql_retry or provider_retry,
                        retry_after=retry_after,duration_ms=round((time.monotonic()-began)*1000))
                else:
                    finished=self.finish(row['task_id'],row['attempt_token'],result=result,
                                         duration_ms=round((time.monotonic()-began)*1000))
                # BaseException/crash intentionally leaves RUNNING for explicit recovery.
                report['processed']+=1;report[finished['status'].lower()]+=1
                report['recovered_succeeded']+=finished['status']=='SUCCEEDED' and row['attempts']>1
        report['duration_ms']=round((time.monotonic()-started)*1000);return report
