"""Read-only storage/backlog alert evidence. Never prune recovery evidence or sources."""
from pathlib import Path
import shutil
import time

from ..errors import PipelineError


def report(root, max_bytes, min_free_bytes, log_retention_days=30):
    root=Path(root).resolve()
    if (not root.is_dir() or type(max_bytes) is not int or max_bytes<1
            or type(min_free_bytes) is not int or min_free_bytes<0
            or type(log_retention_days) is not int or log_retention_days<1):
        raise PipelineError('STORAGE_POLICY_REQUIRED')
    total=files=old_logs=0;cutoff=time.time()-log_retention_days*86400
    for path in root.rglob('*'):
        if path.is_symlink():raise PipelineError('STORAGE_SYMLINK_FORBIDDEN')
        if not path.is_file():continue
        info=path.stat();files+=1;total+=info.st_size
        if path.name=='progress.jsonl' and info.st_mtime<cutoff:old_logs+=1
    free=shutil.disk_usage(root).free
    alerts=[]
    if total>max_bytes:alerts.append('STATE_SIZE_LIMIT')
    if free<min_free_bytes:alerts.append('FREE_SPACE_LOW')
    if old_logs:alerts.append('LOG_RETENTION_REVIEW_REQUIRED')
    return dict(status='ATTENTION_REQUIRED' if alerts else 'STORAGE_OK',bytes=total,files=files,
        free_bytes=free,log_retention_days=log_retention_days,old_progress_logs=old_logs,
        alerts=alerts,automatic_deletion=False)
