"""Disposable file-validation memo; the download journal remains authoritative."""
import hashlib
import json
import os
import re
from pathlib import Path


def canonical(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(',', ':'))


def records(path):
    if not path.exists():
        return
    with path.open(encoding='utf-8-sig') as stream:
        for line in stream:
            if not line.strip():
                continue
            try:
                value = json.loads(line)
            except ValueError:
                continue  # Compatibility with the existing interrupted journal.
            if isinstance(value, dict):
                yield value


def download_history(path):
    return {row['url']: row for row in records(path) if row.get('url')}


def read_products(path):
    rows, digest = [], hashlib.sha256()
    with path.open('rb') as stream:
        for line in stream:
            digest.update(line)
            if line.strip():
                rows.append(json.loads(line.decode('utf-8-sig')))
    return rows, digest.hexdigest()


def signature(path):
    value = path.stat()
    result = [value.st_dev, value.st_ino, value.st_size,
              value.st_mtime_ns, value.st_ctime_ns]
    if os.name == 'nt':
        # Windows st_ctime is creation time, not the NTFS change timestamp.
        result.append(windows_change_time(path))
    return result


def windows_change_time(path):
    import ctypes
    from ctypes import wintypes
    class BasicInfo(ctypes.Structure):
        _fields_ = [('created', ctypes.c_longlong), ('accessed', ctypes.c_longlong),
                    ('written', ctypes.c_longlong), ('changed', ctypes.c_longlong),
                    ('attributes', wintypes.DWORD)]
    kernel = ctypes.WinDLL('kernel32', use_last_error=True)
    create = kernel.CreateFileW
    create.argtypes = [wintypes.LPCWSTR, wintypes.DWORD, wintypes.DWORD,
                       ctypes.c_void_p, wintypes.DWORD, wintypes.DWORD, wintypes.HANDLE]
    create.restype = wintypes.HANDLE
    get = kernel.GetFileInformationByHandleEx
    get.argtypes = [wintypes.HANDLE, ctypes.c_int, ctypes.c_void_p, wintypes.DWORD]
    get.restype = wintypes.BOOL
    close = kernel.CloseHandle
    close.argtypes = [wintypes.HANDLE]
    handle = create(str(path), 0x80, 7, None, 3, 0, None)
    if handle == ctypes.c_void_p(-1).value:
        return None
    try:
        info = BasicInfo()
        return info.changed if get(handle, 0, ctypes.byref(info), ctypes.sizeof(info)) else None
    finally:
        close(handle)


class ValidationMemo:
    """Reuse a SHA check only for the same path, expected SHA and file signature.

    A missing/corrupt memo, changed file, moved dataset or --verify-cache falls
    back to an actual content hash. It is a performance cache, not an audit.
    """
    def __init__(self, root, verify_all=False):
        self.root = Path(root).resolve()
        self.path = self.root / 'image_validation.jsonl'
        self.verify_all = verify_all
        self.rows, self.checked = {}, {}
        self.full_hashes = self.stat_reuses = 0
        self.stream = None
        for row in records(self.path):
            body = {k: v for k, v in row.items() if k != 'record_hash'}
            if (row.get('contract_version') == 1 and isinstance(row.get('local_path'), str)
                    and row.get('record_hash') == hashlib.sha256(canonical(body).encode()).hexdigest()):
                self.rows[row['local_path']] = body

    def __enter__(self):
        return self

    def __exit__(self, *args):
        if self.stream is not None:
            self.stream.close()

    def flush(self):
        if self.stream is not None:
            self.stream.flush()

    def valid(self, entry):
        if (entry.get('status') != 'downloaded' or not entry.get('local_path')
                or not re.fullmatch('[a-f0-9]{64}', str(entry.get('sha256', '')))):
            return False
        path = (self.root / entry['local_path']).resolve()
        if not path.is_relative_to(self.root) or not path.is_file():
            return False
        try:
            before = signature(path)
            expected = entry['sha256']
            if entry.get('bytes') is not None and before[2] != entry['bytes']:
                return False
            local = path.relative_to(self.root).as_posix()
            row = self.checked.get(local) or (None if self.verify_all else self.rows.get(local))
            reusable = before[1] != 0 and (os.name != 'nt' or bool(before[-1]))
            if (reusable and row and row.get('root') == str(self.root) and row.get('sha256') == expected
                    and row.get('signature') == before):
                self.stat_reuses += 1
                return True
            digest = hashlib.sha256()
            with path.open('rb') as stream:
                for block in iter(lambda: stream.read(1024 * 1024), b''):
                    digest.update(block)
            self.full_hashes += 1
            if digest.hexdigest() != expected or signature(path) != before:
                return False
            body = dict(contract_version=1, root=str(self.root), local_path=local,
                        sha256=expected, signature=before)
            if self.stream is None:
                self.stream = self.path.open('a', encoding='utf-8')
            row = dict(body, record_hash=hashlib.sha256(canonical(body).encode()).hexdigest())
            self.stream.write('\n' + canonical(row) + '\n')
            self.rows[local] = self.checked[local] = body
            return True
        except OSError:
            return False
