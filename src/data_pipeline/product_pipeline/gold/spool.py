"""Bounded temporary Silver evidence, fully read before a review DB transaction."""
from contextlib import contextmanager
from hashlib import sha256
from pathlib import Path
import shutil
import tempfile

from ..contracts import canonical
from ..errors import PipelineError,Conflict
from ..files import parse


class EvidenceSpool:
    def __init__(self,root,max_group_bytes):
        self.root=Path(root);self.max_group_bytes=max_group_bytes;self.entries=[]

    def __iter__(self):
        for name,size,checksum in self.entries:
            with (self.root/name).open('rb') as stream:data=stream.read(self.max_group_bytes+1)
            if len(data)!=size or sha256(data).hexdigest()!=checksum:
                raise Conflict('GOLD_REVIEW_SPOOL_CHANGED')
            yield parse(data.decode('utf-8'))


@contextmanager
def review_spool(groups,*,directory=None,max_bytes=256*1024*1024,max_group_bytes=32*1024*1024):
    """Never hold the complete window on Driver or perform Spark I/O in MySQL.

    The spool is disposable evidence, not a completion receipt or checkpoint.
    Single-group and aggregate disk limits are independent and fail closed.
    """
    if min(max_bytes,max_group_bytes)<=0:raise PipelineError('GOLD_REVIEW_SPOOL_LIMIT')
    with tempfile.TemporaryDirectory(prefix='gold-review-',dir=directory) as root:
        spool=EvidenceSpool(root,max_group_bytes);total=0
        for group in groups:
            data=canonical(group).encode('utf-8');total+=len(data)
            if len(data)>max_group_bytes:raise PipelineError('GOLD_PRODUCT_HISTORY_LIMIT')
            if total>max_bytes:raise PipelineError('GOLD_REVIEW_SPOOL_LIMIT')
            if shutil.disk_usage(root).free<len(data)+64*1024*1024:
                raise PipelineError('GOLD_REVIEW_SPOOL_DISK_FULL')
            name='%06d.json'%len(spool.entries);path=Path(root)/name
            with path.open('xb') as stream:stream.write(data)
            path.chmod(0o600);spool.entries.append((name,len(data),sha256(data).hexdigest()))
        yield spool
