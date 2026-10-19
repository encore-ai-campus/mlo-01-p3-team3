"""A nonblocking OS lock: process death releases ownership, not a file's age."""
import os
from contextlib import contextmanager


class RunBusy(Exception):
    pass


@contextmanager
def execution_lock(directory):
    path = directory / ".worker-lock"
    # The file is intentionally never unlinked: unlinking a locked inode can
    # let a different process acquire an unrelated lock at the same pathname.
    stream = path.open("a+b")
    locked = False
    try:
        stream.seek(0, 2)
        if stream.tell() == 0:
            stream.write(b"0")
            stream.flush()
        stream.seek(0)
        try:
            if os.name == "nt":
                import msvcrt
                msvcrt.locking(stream.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            raise RunBusy("Another process still owns the run") from None
        locked = True
        yield
    finally:
        if locked:
            stream.seek(0)
            if os.name == "nt":
                import msvcrt
                msvcrt.locking(stream.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl
                fcntl.flock(stream.fileno(), fcntl.LOCK_UN)
        stream.close()
