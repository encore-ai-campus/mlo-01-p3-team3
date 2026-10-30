"""Keep the newest completed and pipeline-loaded collection runs; delete older ones.

A run is deletable only when its checkpoint is completed, the loader confirmed it with
pipeline_loaded.json naming the same job_id as run_config and checkpoint, it is not the
active run, no collector or image step holds its locks, and no remaining run points to it through
run_report.detail_evidence_dirs. Everything else is kept.

The marker is the loader's confirmation of a successful load into the operating database
(schema_version 1, every field present, target LOADED_TARGET, no failed product). A deleted run goes
whole, its details/ (the gzip detail pages) included: they are kept for the newest runs only.

A loaded run that is kept (one of the newest) loses only its image bytes: media/images/ and the
media/categories/ hard links. The loader put the images in object storage; the manifest, reports and
history stay, and media/images_pruned.json tells verification and resume that the files are gone on purpose.
The same lock, active-run and reference checks apply.

With --prune-abandoned (off by default) a run marked abandoned, or a partial run, is deleted too
when it was last updated before the newest completed and loaded run, with the same lock and
reference checks. Without a loaded run nothing of the kind is deleted.
"""
import argparse
from contextlib import ExitStack, contextmanager
import json
import os
from pathlib import Path
import shutil

from .checkpoint import HANDOFF_DIR, LOADED_MARKER, writer_lock
from .core import utc_now
from .storage import IMAGE_BYTE_DIRS, IMAGES_PRUNED, atomic_json

LOADED_SCHEMA = 1
# The marker's target for the operating database (fixed by the loader). environment_id is for audit only.
LOADED_TARGET = 'production'
LOADED_FIELDS = ('schema_version', 'job_id', 'snapshot_id', 'ingestion_run_id', 'target', 'loaded_at', 'result',
                 'products', 'raw_confirmed', 'failed', 'image_relations_unavailable', 'gallery_missing')
DELETING = '.deleting-'
# Never resumed again: abandoned by an operator, or a partial run a newer loaded run has replaced.
STALE = {'abandoned', 'partial'}


def _read(path):
    try:
        return json.loads(path.read_text(encoding='utf-8'))
    except (OSError, ValueError):
        return None


def _confirmed(marker, job):
    """The loader wrote this marker for this job after a complete load into the operating database."""
    return (isinstance(marker, dict) and all(k in marker for k in LOADED_FIELDS)
            and type(marker['schema_version']) is int and marker['schema_version'] == LOADED_SCHEMA
            and marker['job_id'] == job and marker['target'] == LOADED_TARGET
            and marker['failed'] == 0 and marker['raw_confirmed'] == marker['products'])


def _evidence(directory):
    report = _read(directory / 'run_report.json')
    dirs = report.get('detail_evidence_dirs', []) if isinstance(report, dict) else []
    return {Path(d).resolve() for d in dirs if isinstance(d, str)}


def _ineligible(directory, original, active_dir, stale_before=None):
    """Reason to keep this run, or None with its completion time when it may be deleted.

    With stale_before (the newest loaded run's time), an abandoned or partial run updated before it
    may be deleted too; its time is returned as None so it never counts as one of the kept runs."""
    config = _read(directory / 'run_config.json')
    state = _read(directory / 'checkpoint_state.json')
    marker = _read(directory / LOADED_MARKER)
    if not isinstance(config, dict) or not isinstance(state, dict):
        return 'not_a_collection_run', None
    if original == active_dir:
        return 'active', None
    job = config.get('job_id')
    if (isinstance(job, str) and job and (directory.parent / HANDOFF_DIR / f'{job}.json').is_file()
            and not _confirmed(marker, job)):
        # Handed to the pipeline and not loaded yet: never deleted, --prune-abandoned included.
        return 'handoff_pending', None
    if state.get('status') != 'completed':
        if (stale_before and state.get('status') in STALE and isinstance(job, str) and job
                and state.get('job_id') == job and str(state.get('updated_at') or '') < stale_before):
            return None, None
        return f"status_{state.get('status')}", None
    # The loader confirms one specific job; a marker without that identity confirms nothing.
    if not (isinstance(job, str) and job and state.get('job_id') == job and _confirmed(marker, job)):
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


def _delete(directory, active_dir, stale_before=None):
    """Delete one run unless a collector holds it, else the reason it stays.

    A run resumed from another output root holds only its own writer.lock, never this
    root's .collection.lock, so the lock is taken before the directory moves."""
    target = directory.with_name(DELETING + directory.name)
    try:
        with _run_locks(directory):
            reason, _ = _ineligible(directory, directory, active_dir, stale_before)
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


def _image_bytes(media):
    """Folders, distinct files and bytes of a run's image byte folders (hard links counted once)."""
    folders, seen, size = [], set(), 0
    for name in IMAGE_BYTE_DIRS:
        folder = media / name
        if not folder.is_dir():
            continue
        folders.append(f'media/{name}')
        for path in folder.rglob('*'):
            if path.is_file():
                info = path.stat()
                identity = (info.st_dev, info.st_ino) if info.st_ino else path
                if identity not in seen:
                    seen.add(identity)
                    size += info.st_size
    return folders, len(seen), size


def _prune_images(directory, active_dir):
    """Remove the image bytes of a kept loaded run, else the reason they stay (None when done)."""
    media = directory / 'media'
    try:
        with _run_locks(directory):
            reason, _ = _ineligible(directory, directory, active_dir)
            if reason:
                return reason, None
            folders, files, size = _image_bytes(media)
            if not folders:
                return None, None
            marker = media / IMAGES_PRUNED
            if not marker.is_file():
                # Written before removing: an interrupted removal is finished by the next prune,
                # never mistaken for lost files.
                job = (_read(directory / 'run_config.json') or {}).get('job_id')
                atomic_json(marker, {'schema_version': 1, 'job_id': job, 'pruned_at': utc_now(),
                                     'folders': folders, 'files': files, 'bytes': size})
            for folder in folders:
                shutil.rmtree(directory / folder)
    except (ValueError, OSError):
        return 'in_use', None
    return None, {'run': directory.name, 'folders': folders, 'files': files, 'bytes': size}


def _delete_in_order(runs, delete, active_dir, kept, stale_before=None):
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
            reason = _delete(directory, active_dir, stale_before)
            if reason:
                kept[directory.name] = reason
            else:
                deleted.append(directory)
    for directory in pending:
        kept[directory.name] = 'referenced_evidence'
    return [p.name for p in deleted]


def prune_runs(output, keep=2, apply=False, prune_abandoned=False):
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
        stale_before = loaded[0][0] if prune_abandoned and loaded else None
        candidates = [directory for _, directory in loaded[keep:]]
        if stale_before:
            for directory in runs:
                if kept.get(directory.name, '').startswith('status_'):
                    reason, _ = _ineligible(directory, directory, active_dir, stale_before)
                    if not reason:
                        kept.pop(directory.name)
                        candidates.append(directory)
        delete = []
        for directory in candidates:
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
        if prune_abandoned:
            report['prune_abandoned'] = {'before': stale_before}
        if apply:
            for directory in leftovers:
                if _unlocked(directory):
                    shutil.rmtree(directory)
            report['deleted'] = _delete_in_order(runs, delete, active_dir, kept, stale_before)
        # Kept loaded runs: only their image bytes go (the newest runs keep everything else).
        gone = set(report['deleted']) if apply else {p.name for p in delete}
        prunable = []
        for _, directory in loaded[:keep]:
            if kept.get(directory.name) != 'newest_loaded' or not _image_bytes(directory / 'media')[0]:
                continue
            # Same reference rule as deleting a run; a run's report always names its own folder.
            readers = [p for p in runs if p.name not in gone and p != directory and directory.resolve() in _evidence(p)]
            if readers:
                report.setdefault('images_kept', {})[directory.name] = 'referenced_evidence'
            else:
                prunable.append(directory)
        if prunable and not apply:
            report['images_prunable'] = [d.name for d in prunable]
        elif prunable:
            pruned = []
            for directory in prunable:
                reason, done = _prune_images(directory, active_dir)
                if done:
                    pruned.append(done)
                elif reason:
                    report.setdefault('images_kept', {})[directory.name] = reason
            if pruned:
                report['images_pruned'] = pruned
        return report


def main():
    parser = argparse.ArgumentParser(description='완료·적재 확인된 수집 실행 중 최근 N회만 남기고 정리')
    parser.add_argument('--output', type=Path, required=True, help='수집기 --output과 같은 실행 루트')
    parser.add_argument('--keep', type=int, default=2)
    parser.add_argument('--apply', action='store_true', help='실제 삭제. 없으면 삭제 대상만 출력')
    parser.add_argument('--prune-abandoned', action='store_true',
                        help='포기(abandoned)·partial 실행 중 가장 최근 완료·적재 실행보다 오래된 것도 삭제. '
                             '기본은 끔(적재 인계 계약 확정 전)')
    args = parser.parse_args()
    try:
        report = prune_runs(args.output, args.keep, args.apply, args.prune_abandoned)
    except (ValueError, OSError) as exc:
        parser.error(str(exc))
    print(json.dumps(report, ensure_ascii=False))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
