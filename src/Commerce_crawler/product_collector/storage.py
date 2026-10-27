"""Atomic snapshots, append-only resume records and shared file links."""
import hashlib
import json
import os
import shutil
from pathlib import Path


def read_jsonl(path: Path, tolerate_interruption=False) -> list[dict]:
    rows = []
    for line in path.read_text(encoding="utf-8-sig").splitlines():
        if not line.strip():
            continue
        try:
            rows.append(json.loads(line))
        except ValueError:
            if not tolerate_interruption:
                raise
    return rows


def atomic_json(path: Path, value) -> None:
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, ensure_ascii=False, indent=2), encoding="utf-8")
    temporary.replace(path)


def write_jsonl(path: Path, rows) -> None:
    temporary = path.with_suffix(path.suffix + ".tmp")
    with temporary.open("w", encoding="utf-8", newline="\n") as stream:
        for row in rows:
            stream.write(json.dumps(row, ensure_ascii=False, allow_nan=False) + "\n")
    temporary.replace(path)


def append_record(path: Path, entry) -> None:
    # Leading newline isolates a truncated last record after forced termination.
    with path.open("a", encoding="utf-8") as stream:
        stream.write("\n" + json.dumps(entry, ensure_ascii=False) + "\n")
        stream.flush()


def history(path: Path) -> dict:
    return {row["url"]: row for row in read_jsonl(path, True) if isinstance(row, dict) and row.get("url")} if path.exists() else {}


def file_matches(path: Path, sha256: str) -> bool:
    return path.is_file() and hashlib.sha256(path.read_bytes()).hexdigest() == sha256


def link_file(source: Path, target: Path, sha256: str) -> None:
    """Reuse physical bytes where hard links are supported; copy otherwise."""
    if file_matches(target, sha256):
        return
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_suffix(target.suffix + ".tmp")
    temporary.unlink(missing_ok=True)
    try:
        os.link(source, temporary)
    except OSError:
        shutil.copyfile(source, temporary)
    temporary.replace(target)
