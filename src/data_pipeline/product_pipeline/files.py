"""Bounded reads, durable replace and OS locks; never remove collector locks."""
from contextlib import contextmanager
import hashlib
import json
import os
from pathlib import Path
from uuid import uuid4

from .contracts import canonical
from .errors import PipelineError


def confined(root, name):
    root = Path(root).resolve()
    if not isinstance(name, str) or not name or Path(name).is_absolute() or "\\" in name:
        raise PipelineError("UNSAFE_RELATIVE_PATH")
    path = (root / name).resolve()
    if not path.is_relative_to(root) or path == root:
        raise PipelineError("UNSAFE_RELATIVE_PATH")
    return path


def sha_file(path):
    h = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def sync_dir(path):
    if os.name != "nt":
        fd = os.open(path, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)


def atomic_bytes(path, data):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(path.name + ".tmp-" + uuid4().hex)
    try:
        with temp.open("xb") as stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temp, path)
        sync_dir(path.parent)
    finally:
        temp.unlink(missing_ok=True)


def atomic_json(path, value):
    atomic_bytes(path, canonical(value).encode())


def _pairs(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise PipelineError("DUPLICATE_JSON_KEY")
        result[key] = value
    return result


def parse(text):
    def invalid(_):
        raise PipelineError("NONFINITE_JSON")
    try:
        return json.loads(text, object_pairs_hook=_pairs, parse_constant=invalid)
    except (ValueError, UnicodeError) as exc:
        raise PipelineError("INVALID_JSON") from exc


def read_json(path):
    return parse(Path(path).read_text(encoding="utf-8-sig"))


def rows(path, max_line=32 * 1024 * 1024):
    with Path(path).open("rb") as stream:
        first = True
        while line := stream.readline(max_line + 1):
            if len(line) > max_line:
                raise PipelineError("INPUT_LINE_TOO_LARGE")
            if first:
                line = line.removeprefix(b"\xef\xbb\xbf")
                first = False
            if line.strip():
                try:
                    value = parse(line.decode("utf-8"))
                except UnicodeError as exc:
                    raise PipelineError("INVALID_UTF8") from exc
                if not isinstance(value, dict):
                    raise PipelineError("EXPECTED_JSON_OBJECT")
                yield value


@contextmanager
def lock(path, existing=False):
    path = Path(path)
    if not existing:
        path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("r+b" if existing else "a+b") as stream:
        if not existing and path.stat().st_size == 0:
            stream.write(b"0")
            stream.flush()
        stream.seek(0)
        try:
            if os.name == "nt":
                import msvcrt
                msvcrt.locking(stream.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            raise PipelineError("WRITER_ACTIVE") from exc
        try:
            yield
        finally:
            if os.name == "nt":
                stream.seek(0)
                msvcrt.locking(stream.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                fcntl.flock(stream, fcntl.LOCK_UN)
