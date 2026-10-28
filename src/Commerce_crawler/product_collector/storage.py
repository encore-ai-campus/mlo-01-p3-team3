"""Atomic snapshots, append-only resume records and shared file links."""
from __future__ import annotations

import gzip
import hashlib
import json
import os
import re
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


def detail_html_path(directory: Path, product_id: str, collected_at: str) -> Path:
    # One file per response: a retried detail never replaces the page an earlier snapshot used.
    stamp = re.sub(r"[^0-9A-Za-z]", "", collected_at)
    return Path(directory) / "details" / str(product_id) / f"{stamp}.json.gz"


def save_detail_html(directory: Path, product_id: str, url: str, html: str, collected_at: str) -> None:
    """Full detail HTML stays beside the run as gzip local evidence, never in the payload."""
    path = detail_html_path(directory, product_id, collected_at)
    path.parent.mkdir(parents=True, exist_ok=True)
    body = json.dumps({"product_id": str(product_id), "url": url, "html": html, "collected_at": collected_at},
                      ensure_ascii=False).encode("utf-8")
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_bytes(gzip.compress(body, mtime=0))
    temporary.replace(path)


def load_detail_html(directories, product_id: str, url: str, collected_at: str | None = None):
    """Saved detail evidence of this response from the first directory that has it, else None.

    With collected_at only that response matches. The older uncompressed details/<id>.json
    (gallery_assets output) has one page per product and is used when no gzip file matches."""
    for directory in directories:
        base = Path(directory) / "details"
        if collected_at is not None:
            candidates = [detail_html_path(directory, product_id, collected_at)]
        else:
            candidates = sorted((base / str(product_id)).glob("*.json.gz"), reverse=True)[:1]
        for path in candidates + [base / f"{product_id}.json"]:
            if not path.is_file():
                continue
            text = gzip.decompress(path.read_bytes()).decode("utf-8") if path.suffix == ".gz" else path.read_text(encoding="utf-8")
            saved = json.loads(text)
            if saved.get("product_id") != str(product_id) or saved.get("url") != url:
                raise ValueError(f"saved detail evidence identity mismatch: {product_id}")
            return saved
    return None


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
