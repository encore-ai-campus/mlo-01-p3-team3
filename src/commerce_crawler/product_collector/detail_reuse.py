"""Detail refresh "changed": reuse unchanged details of earlier completed runs without a request.

The base is the previous completed run. A product the base does not list at all may come from an older
completed run still on disk (newest first); a product the base turned down (sample, changed fingerprint,
damaged page) is requested. Only product indexes are kept in memory; a reused record is read from its
line on demand. The reused page is hard-linked (or copied) into this run's details/, so every run keeps
complete reprocess evidence of its own and never depends on an older run that prune may remove.

In "all" mode nothing is reused, but the same index tells how many details "changed" would have reused,
and both modes keep the observation index the stage-2 decision is measured with
(crawler-optimization/detail-refresh.md, 후속)."""
from __future__ import annotations

import copy
from datetime import datetime
import hashlib
import json
from pathlib import Path

from .storage import atomic_json, detail_html_path, link_file, load_detail_html, write_jsonl

REUSE_FILE = "detail_reuse.json"
MODES = ("all", "changed")
# Older runs searched for a product the base does not list; prune keeps only a few runs anyway.
MAX_OTHER_RUNS = 4
# Output root: last listing observation, last ok detail and its fingerprint per product. No page bytes.
SEEN_FILE = Path("_detail_index") / "seen.jsonl"
WEEK_BUCKETS = ((1, "under_1"), (2, "1_2"), (4, "2_4"), (8, "4_8"), (12, "8_12"))
KEEP_BEFORE = 3


def _read(path: Path):
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None


def completed_runs(output_root: Path, current: Path, source: str) -> list[Path]:
    """Completed runs in the same output root that collected details of the same source, newest first."""
    current, found = Path(current).resolve(), []
    for directory in Path(output_root).iterdir():
        if not directory.is_dir() or directory.name.startswith((".", "_")) or directory.resolve() == current:
            continue
        state, config = _read(directory / "checkpoint_state.json"), _read(directory / "run_config.json")
        settings = config.get("config") if isinstance(config, dict) else None
        if (isinstance(state, dict) and state.get("status") == "completed" and isinstance(settings, dict)
                and settings.get("source") == source and settings.get("collect_details") is True
                and (directory / "product_raw.jsonl").is_file()):
            # updated_at has second precision; the state file's write time breaks a tie between quick runs.
            found.append((str(state.get("updated_at") or ""), (directory / "checkpoint_state.json").stat().st_mtime_ns,
                          directory))
    return [directory for *_, directory in sorted(found, reverse=True)]


def find_base(output_root: Path, current: Path, source: str) -> Path | None:
    """Newest completed run in the same output root that collected details of the same source."""
    runs = completed_runs(output_root, current, source)
    return runs[0] if runs else None


def _run(directory: Path) -> dict:
    report = _read(directory / "run_report.json") or {}
    evidence = [directory] + [Path(d) for d in report.get("detail_evidence_dirs", []) if Path(d) != directory]
    return {"dir": directory, "job_id": report.get("job_id") or directory.name, "evidence": evidence}


def _index(directory: Path, run: int, into: dict, skip: dict | None = None) -> None:
    offset = 0
    with (directory / "product_raw.jsonl").open("rb") as stream:
        for line in stream:
            if line.strip():
                record = json.loads(line)
                code = str(record["product_id"])
                if code not in into and (skip is None or code not in skip):
                    detail = record["payload"].get("detail") or {}
                    into[code] = {
                        "run": run, "offset": offset, "status": record.get("detail_status"),
                        "fingerprint": record.get("listing_fingerprint"),
                        "extractor_version": record.get("extractor_version"),
                        "collected_at": detail.get("collected_at"), "gallery": len(detail.get("gallery_images") or [])}
            offset += len(line)


def _parse(value):
    try:
        return datetime.fromisoformat(value) if value else None
    except ValueError:
        return None


class DetailReuse:
    def __init__(self, base: Path | None, job_id: str, sample_size: int, others=(), candidates: int | None = None,
                 mode: str = "changed", source: str | None = None):
        if type(sample_size) is not int or sample_size < 0:
            raise ValueError("refresh sample must be zero or a positive integer")
        self.base, self.job_id, self.sample_size, self.mode = (Path(base) if base else None), job_id, sample_size, mode
        self.source = source
        self.index, self.fallback, self.runs, self.base_job_id, self.base_missing = {}, {}, [], None, None
        self.others_missing = []
        if self.base is not None:
            self.runs.append(_run(self.base))
            self.base_job_id = self.runs[0]["job_id"]
            _index(self.base, 0, self.index)
            for directory in others:
                directory = Path(directory)
                if not (directory / "product_raw.jsonl").is_file():
                    self.others_missing.append(str(directory))  # pruned while this run was interrupted
                    continue
                self.runs.append(_run(directory))
                _index(directory, len(self.runs) - 1, self.fallback, skip=self.index)
        self.evidence = self.runs[0]["evidence"] if self.runs else []
        # Fixed on the first start: more reuse candidates must not shrink the sample, and a resume draws the same.
        self.candidates = len(self.index) + len(self.fallback) if candidates is None else candidates

    @classmethod
    def open(cls, run_dir: Path, output_root: Path, source: str, job_id: str, sample_size: int,
             mode: str = "changed") -> "DetailReuse":
        """The runs searched are chosen once, on the first start, and read back on resume."""
        path = Path(run_dir) / REUSE_FILE
        saved = _read(path)
        if isinstance(saved, dict):
            base = Path(saved["base_dir"]) if saved.get("base_dir") else None
            if base is not None and not (base / "product_raw.jsonl").is_file():
                # Pruned while this run was interrupted. Details reused so far have their own evidence
                # here; the remaining products are simply requested.
                reuse = cls(None, job_id, sample_size, mode=mode, source=source)
                reuse.base_job_id, reuse.base_missing = saved.get("base_job_id"), str(base)
                return reuse
            return cls(base, job_id, sample_size, saved.get("other_dirs") or (), saved.get("candidates"), mode, source)
        runs = completed_runs(output_root, run_dir, source)
        reuse = cls(runs[0] if runs else None, job_id, sample_size, runs[1:1 + MAX_OTHER_RUNS], mode=mode, source=source)
        atomic_json(path, {"base_dir": str(reuse.base.resolve()) if reuse.base else None,
                           "base_job_id": reuse.base_job_id,
                           "other_dirs": [str(run["dir"].resolve()) for run in reuse.runs[1:]],
                           "candidates": reuse.candidates})
        return reuse

    def sampled(self, product_id: str) -> bool:
        """Deterministic per run: a resumed run draws the same sample."""
        if not self.candidates:
            return False
        point = int(hashlib.sha256(f"{self.job_id}:{product_id}".encode()).hexdigest()[:12], 16) / 16 ** 12
        return point < self.sample_size / self.candidates

    def entry(self, product_id: str) -> dict | None:
        """The base's entry; another run's only when the base does not list the product at all."""
        return self.index.get(product_id) or self.fallback.get(product_id)

    @staticmethod
    def acceptable(entry: dict | None, fingerprint: str | None, extractor_version) -> bool:
        return (entry is not None and fingerprint is not None and entry["fingerprint"] == fingerprint
                and entry["status"] == "ok" and entry["gallery"] >= 1 and bool(entry["collected_at"])
                and entry["extractor_version"] == extractor_version)

    def reuse_from(self, record: dict, fingerprint: str | None, target: Path | None) -> tuple[dict, str] | None:
        """(detail, providing job_id) when this product is unchanged and the saved page checks out, else None."""
        code = str(record["product_id"])
        entry = self.entry(code)
        if not self.acceptable(entry, fingerprint, record.get("extractor_version")) or self.sampled(code):
            return None
        run = self.runs[entry["run"]]
        with (run["dir"] / "product_raw.jsonl").open("rb") as stream:
            stream.seek(entry["offset"])
            detail = json.loads(stream.readline())["payload"]["detail"]
        url, at = record["payload"]["listings"][0]["product_url"], entry["collected_at"]
        if detail.get("url") != url:
            return None
        # Only this response's gzip page counts; an older per-product details/<id>.json is no evidence of it.
        source = next((path for path in (detail_html_path(d, code, at) for d in run["evidence"]) if path.is_file()), None)
        if source is None:
            return None
        try:
            load_detail_html([source.parents[2]], code, url, at)
        except ValueError:
            return None  # damaged or foreign page: request this product again
        if target is not None:
            link_file(source, detail_html_path(target, code, at), hashlib.sha256(source.read_bytes()).hexdigest())
        return copy.deepcopy(detail), run["job_id"]

    def reuse(self, record: dict, fingerprint: str | None, target: Path | None) -> dict | None:
        found = self.reuse_from(record, fingerprint, target)
        return found[0] if found else None

    def summary(self, raw: list[dict], output_root: Path | None = None, now: str | None = None) -> dict:
        # Counted from the saved records, so an interrupted and resumed run reports the whole run.
        funnel = dict.fromkeys(("needed", "base_listed", "other_listed", "unchanged", "sampled", "reused",
                                "reused_from_other"), 0)
        new = set()
        for r in raw:
            if r["detail_status"] in ("not_needed", "disabled"):
                continue
            code = str(r["product_id"])
            funnel["needed"] += 1
            entry = self.entry(code)
            if code in self.index:
                funnel["base_listed"] += 1
            else:
                new.add(code)
                funnel["other_listed"] += entry is not None
            if self.acceptable(entry, r.get("listing_fingerprint"), r.get("extractor_version")):
                funnel["unchanged"] += 1
                funnel["sampled"] += self.sampled(code)
            reused = r.get("detail_reused_from")
            if reused:
                funnel["reused"] += 1
                funnel["reused_from_other"] += reused.get("job_id") != self.base_job_id
        value = {"mode": self.mode, "base_job_id": self.base_job_id,
                 "base_dir": str(self.base) if self.base else None, "base_missing": self.base_missing,
                 "other_job_ids": [run["job_id"] for run in self.runs[1:]], "others_missing": self.others_missing,
                 "candidates": self.candidates, "sample_size": self.sample_size, "reused": funnel["reused"],
                 "sampled": sum((str(r["product_id"]) in self.index or str(r["product_id"]) in self.fallback)
                                and self.sampled(str(r["product_id"])) for r in raw),
                 "funnel": funnel}
        if output_root is not None and now is not None and self.source:
            value["new_products"] = observe(output_root, self.source, raw, self.job_id, now, new)
        return value


def newer(saved: str | None, incoming: str | None) -> bool:
    """The saved time is later than the incoming one."""
    a, b = _parse(saved), _parse(incoming)
    return bool(a and b and a > b)


def observe(output_root: Path, source: str, raw: list[dict], job_id: str, now: str, new: set) -> dict:
    """Update the observation index with this run and tell what it knew of the products new to the base.

    Rewritten whole (atomic replace) under the collection lock. Each entry keeps its state from before
    each recent job, so repeating it for a job (a resume) counts the same, even after another run."""
    path = Path(output_root) / SEEN_FILE
    path.parent.mkdir(parents=True, exist_ok=True)
    current = {}
    for r in raw:
        ok = r["detail_status"] == "ok"
        current[str(r["product_id"])] = (r.get("listed_at") or now, r.get("detail_at") if ok else None,
                                         r.get("listing_fingerprint") if ok else None)
    counts = {"new_to_base": len(new), "first_seen": 0, "seen_before": 0, "weeks_since_listed": {},
              "with_ok_detail": 0, "same_fingerprint": 0}
    fingerprints = {str(r["product_id"]): r.get("listing_fingerprint") for r in raw if str(r["product_id"]) in new}

    def count(code, prior):
        if code not in new:
            return
        if not prior or not prior.get("listed_at"):
            counts["first_seen"] += 1
            return
        counts["seen_before"] += 1
        # Aged from the job's first observation, so a resume on a later day lands in the same bucket.
        then, at = _parse(prior["listed_at"]), _parse(prior.get("at") or now)
        weeks = (at - then).days / 7 if then and at else None
        bucket = next((name for limit, name in WEEK_BUCKETS if weeks is not None and weeks < limit), "12_plus")
        counts["weeks_since_listed"][bucket] = counts["weeks_since_listed"].get(bucket, 0) + 1
        if prior.get("detail_at"):
            counts["with_ok_detail"] += 1
            counts["same_fingerprint"] += prior.get("fingerprint") is not None and prior["fingerprint"] == fingerprints.get(code)

    def updated(entry):
        code = entry["product_id"]
        # What the index held before each job's first observation, per job: a partial run resumed after a
        # newer run still counts against its own starting point. Only the last few jobs are kept.
        before = entry.setdefault("before", {})
        if job_id not in before:
            before[job_id] = {**{key: entry.get(key) for key in ("listed_at", "detail_at", "fingerprint")}, "at": now}
            for old in list(before)[:-KEEP_BEFORE]:
                del before[old]
        count(code, before[job_id])
        listed_at, detail_at, fingerprint = current.pop(code)
        # A resumed older run never moves the index back past what a newer run saw.
        if not newer(entry.get("listed_at"), listed_at):
            entry["listed_at"] = listed_at
        if detail_at and not newer(entry.get("detail_at"), detail_at):
            entry["detail_at"], entry["fingerprint"] = detail_at, fingerprint
        return entry

    def rows():
        if path.is_file():
            with path.open(encoding="utf-8") as stream:
                for line in stream:
                    if not line.strip():
                        continue
                    entry = json.loads(line)
                    yield updated(entry) if entry.get("source") == source and entry["product_id"] in current else entry
        for code in list(current):
            yield updated({"source": source, "product_id": code, "listed_at": None})

    write_jsonl(path, rows())
    counts["weeks_since_listed"] = dict(sorted(counts["weeks_since_listed"].items()))
    return counts
