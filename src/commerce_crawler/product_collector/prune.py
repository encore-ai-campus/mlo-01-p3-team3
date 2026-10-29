"""Keep the newest completed and pipeline-loaded collection runs; delete older ones.

A run is deletable only when its checkpoint is completed, the loader confirmed it with
pipeline_loaded.json naming the same job_id as run_config and checkpoint, it is not the
active run, no collector or image step holds its locks, and no remaining run points to it through
run_report.detail_evidence_dirs. Everything else is kept.
"""
import argparse
from contextlib import ExitStack, contextmanager
import json
import os
from pathlib import Path
import shutil

from .checkpoint import writer_lock

LOADED_MARKER = 'pipeline_loaded.json'
DELETING = '.deleting-'


def _read(path):
    try:
        return json.loads(path.read_text(encoding='utf-8'))
    except (OSError, ValueError):
        return None


def _evidence(directory):
    report = _read(directory / 'run_report.json')
    dirs = report.get('detail_evidence_dirs', []) if isinstance(report, dict) else []
    return {Path(d).resolve() for d in dirs if isinstance(d, str)}


def _ineligible(directory, original, active_dir):
    """Reason to keep this run, or None with its completion time when it may be deleted."""
    config = _read(directory / 'run_config.json')
    state = _read(directory / 'checkpoint_state.json')
    marker = _read(directory / LOADED_MARKER)
    if not isinstance(config, dict) or not isinstance(state, dict):
        return 'not_a_collection_run', None
    if original == active_dir:
        return 'active', None
    if state.get('status') != 'completed':
        return f"status_{state.get('status')}", None
    job = config.get('job_id')
    # The loader confirms one specific job; a marker without that identity confirms nothing.
    if not (isinstance(job, str) and job and state.get('job_id') == job
            and isinstance(marker, dict) and marker.get('job_id') == job):
        return 'not_loaded', None
    return None, str(state.get('updated_at') or '')


@contextmanager
def _run_locks(directory):
    """The collector's writer.lock and, when the run has images, the image step's lock:
    an image command working on a finished run must not see it disappear."""
    with ExitStack() as stack:
        stack.enter_context(writer_lock(directory / 'writer.lock'))
        if (directory / 'media').is_dir():
            stack.enter_context(writer_lock(directory / 'media' / '.image-download.lock'))
        yield


def _unlocked(directory):
    try:
        with _run_locks(directory):
            return True
    except (ValueError, OSError):
        return False


def _delete(directory, active_dir):
    """Delete one run unless a collector holds it, else the reason it stays.

    A run resumed from another output root holds only its own writer.lock, never this
    root's .collection.lock, so the lock is taken before the directory moves."""
    target = directory.with_name(DELETING + directory.name)
    try:
        with _run_locks(directory):
            reason, _ = _ineligible(directory, directory, active_dir)
            if not reason and os.name == 'posix':
                directory.rename(target)  # a collector opening this path now finds no checkpoint
    except (ValueError, OSError):
        return 'in_use'
    if reason:
        return reason
    if os.name != 'posix':
        try:
            # Windows cannot move a directory with an open lock file, so this follows the release;
            # a collector that took the lock in between keeps files open and the move is refused.
            directory.rename(target)
        except OSError:
            return 'in_use'
    # An interrupted removal leaves only a .deleting- directory, never a half run that looks resumable.
    shutil.rmtree(target)
    return None


def _delete_in_order(runs, delete, active_dir, kept):
    """Delete a run only after every run that reads its pages is gone, so a referencing run
    that has to stay (in use, changed meanwhile) keeps its evidence too."""
    deleted, pending = [], list(delete)
    progress = True
    while pending and progress:
        progress = False
        for directory in list(pending):
            readers = [p for p in runs if p not in deleted and p != directory and directory in _evidence(p)]
            if readers:
                continue
            pending.remove(directory)
            progress = True
            reason = _delete(directory, active_dir)
            if reason:
                kept[directory.name] = reason
            else:
                deleted.append(directory)
    for directory in pending:
        kept[directory.name] = 'referenced_evidence'
    return [p.name for p in deleted]


def prune_runs(output, keep=2, apply=False):
    if type(keep) is not int or keep < 1:
        raise ValueError('keep must be positive')
    output = Path(output).resolve()
    # The collector holds this lock while collecting, so pruning never races a crawl.
    with writer_lock(output / '.collection.lock'):
        active = _read(output / 'active_run.json') or {}
        active_dir = Path(active['directory']).resolve() if isinstance(active.get('directory'), str) else None
        leftovers = [p for p in output.iterdir() if p.is_dir() and p.name.startswith(DELETING)]
        runs = [p for p in sorted(output.iterdir())
                if p.is_dir() and not p.is_symlink() and not p.name.startswith(DELETING)]
        loaded, kept = [], {}
        for directory in runs:
            reason, finished = _ineligible(directory, directory, active_dir)
            if reason:
                kept[directory.name] = reason
            else:
                loaded.append((finished, directory))
        loaded.sort(reverse=True)
        for _, directory in loaded[:keep]:
            kept[directory.name] = 'newest_loaded'
        delete = []
        for _, directory in loaded[keep:]:
            if _unlocked(directory):
                delete.append(directory)
            else:
                kept[directory.name] = 'in_use'
        # A remaining run (export target, reprocess result) may read its pages from an older run.
        while True:
            remaining = [p for p in runs if p not in delete]
            referenced = set().union(*(_evidence(p) for p in remaining))
            protected = [p for p in delete if p in referenced]
            if not protected:
                break
            for directory in protected:
                kept[directory.name] = 'referenced_evidence'
                delete.remove(directory)
        report = {'output': str(output), 'keep': keep, 'applied': apply,
                  'deleted': [p.name for p in delete], 'kept': kept,
                  'interrupted_deletions': [p.name for p in leftovers]}
        if apply:
            for directory in leftovers:
                if _unlocked(directory):
                    shutil.rmtree(directory)
            report['deleted'] = _delete_in_order(runs, delete, active_dir, kept)
        return report


def main():
    parser = argparse.ArgumentParser(description='완료·적재 확인된 수집 실행 중 최근 N회만 남기고 정리')
    parser.add_argument('--output', type=Path, required=True, help='수집기 --output과 같은 실행 루트')
    parser.add_argument('--keep', type=int, default=2)
    parser.add_argument('--apply', action='store_true', help='실제 삭제. 없으면 삭제 대상만 출력')
    args = parser.parse_args()
    try:
        report = prune_runs(args.output, args.keep, args.apply)
    except (ValueError, OSError) as exc:
        parser.error(str(exc))
    print(json.dumps(report, ensure_ascii=False))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
