"""Run on the existing Master coordinator's safe boundaries, under its owner."""
from datetime import datetime
from pathlib import Path
from ..contracts import now,utc
from ..files import read_json,atomic_json
from ..errors import PipelineError


def boundary(root,*,at=None):
    root=Path(root);at=utc(at or now());config=root/'incremental-operations.json'
    if not config.exists():return dict(status='NOT_CONFIGURED')
    if (root/'pause-after-window.json').exists():
        return dict(status='PAUSED')
    stopped=root/'manual-stop.json';attempt=root/'flow-attempt.json'
    if stopped.exists() and (not attempt.exists() or
        datetime.fromisoformat(utc(read_json(attempt)['started_at']).replace('Z','+00:00')).timestamp()<=stopped.stat().st_mtime):
        return dict(status='PAUSED')
    previous=root/'incremental-last-tick.json'
    if previous.exists():
        last=read_json(previous)
        if last.get('status')=='TICK_COMPLETED' and (datetime.fromisoformat(at.replace('Z','+00:00'))-
                datetime.fromisoformat(last['at'].replace('Z','+00:00'))).total_seconds()<300:
            return dict(status='NOT_DUE')
    from ..operations.service import tick
    try:result=tick(config)
    except PipelineError as exc:
        if exc.code!='WRITER_ACTIVE':raise
        result=dict(status='BUSY')
    atomic_json(previous,dict(**result,at=at))
    if result.get('status')=='ATTENTION_REQUIRED':raise PipelineError('INCREMENTAL_ATTENTION_REQUIRED')
    return result
