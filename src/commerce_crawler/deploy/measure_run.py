"""Measurements of a finished collection run, read from its folder (crawler-optimization/measurement-plan.md).

    python deploy/measure_run.py <run_dir> [<other_run_dir>] [--rss-log rss.log] [--json]

Reads run_report.json, media/download_report.json, product_raw.jsonl and file sizes; writes nothing.
With two runs the key values are shown side by side. Standard library only, so it runs with any
python3 on the collector host. --rss-log is the sampler output of the plan: "<epoch> <pid> <VmRSS kB> <VmHWM kB>".
"""
import argparse
from collections import Counter
from datetime import datetime
import json
import os
from pathlib import Path
import shutil
import sys

# Same values as product_collector (quality.py, media.py); copied so this file needs no release on the path.
IMAGE_FAILURE_LIMIT = 0.01
IMAGE_EXTERNAL_ALERT_LIMIT = 0.05
REQUESTED = {'ok', 'unrecognized', 'identity_mismatch', 'request_failed'}
BLOCKED = {403, 429}


def read_json(path: Path):
    try:
        return json.loads(path.read_text(encoding='utf-8'))
    except (OSError, ValueError):
        return None


def when(value):
    try:
        return datetime.fromisoformat(value) if isinstance(value, str) else None
    except ValueError:
        return None


def hours(start, end):
    start, end = when(start), when(end)
    return round((end - start).total_seconds() / 3600, 3) if start and end else None


def phase_of(attempt) -> str:
    url = str(attempt.get('url') or '')
    return 'detail' if '/info/' in url else 'listing' if '/list/' in url else 'other'


def requests_summary(report) -> dict:
    """The collector's own requests (listing and detail); the image step counts its own."""
    phases = {}
    for attempt in report.get('requests') or []:
        phase = phases.setdefault(phase_of(attempt), {'count': 0, 'status': Counter(), 'errors': 0,
                                                       'first': None, 'last': None})
        phase['count'] += 1
        phase['status'][str(attempt.get('status_code'))] += 1
        phase['errors'] += bool(attempt.get('error'))
        at = attempt.get('requested_at')
        if at and (phase['first'] is None or at < phase['first']):
            phase['first'] = at
        if at and (phase['last'] is None or at > phase['last']):
            phase['last'] = at
    for phase in phases.values():
        phase['blocked'] = sum(n for code, n in phase['status'].items() if code.isdigit() and int(code) in BLOCKED)
        # Wall time between the first and last request: a resume inside it adds the time stopped.
        phase['span_hours'] = hours(phase['first'], phase['last'])
        phase['status'] = dict(phase['status'])
    return phases


def quality_summary(report) -> dict:
    gate = report.get('quality') or {}
    failures = report.get('detail_failures') or {}
    requested = failures.get('requested') or 0
    metrics = {name: {'rate': m.get('rate'), 'limit': m.get('limit'), 'count': m.get('count'), 'total': m.get('total')}
               for name, m in (gate.get('metrics') or {}).items()}
    for status, count in (failures.get('counts') or {}).items():
        metrics[f'detail_{status}'] = {'rate': round(count / requested, 4) if requested else None,
                                       'limit': (failures.get('limits') or {}).get(status), 'count': count,
                                       'total': requested}
    categories = report.get('categories') or []
    return {'mode': gate.get('mode'), 'passed': gate.get('passed'), 'judged': gate.get('judged'),
            'received_details': gate.get('received_details'), 'gallery_count_median': gate.get('gallery_count_median'),
            'detail_failures_passed': failures.get('passed'), 'metrics': metrics,
            'violations': [v.get('signal') for v in gate.get('violations') or []],
            'alerts': dict(Counter(a.get('signal') for a in gate.get('alerts') or [])),
            'categories': dict(Counter(c.get('status') for c in categories)),
            'category_reasons': dict(Counter(c.get('reason') for c in categories if c.get('reason')))}


def image_summary(directory: Path, report, phases) -> dict:
    step = read_json(directory / 'media' / 'download_report.json') or {}
    delivered = report.get('image_download') or {}
    if not step and not delivered:
        return {}
    # The step report is current; run_report keeps the copy made when the run ended, older after a
    # standalone image retry (python -m product_collector.media). Its result stays as delivered_result.
    current = {**delivered, **step}
    failures = current.get('failures') or {}
    summary = {'result': current.get('result'), 'delivered_result': delivered.get('result'),
               'verification': delivered.get('verification'),
               'requests': step.get('requests'), 'unique_urls': failures.get('unique_urls', step.get('unique_urls')),
               **{k: current.get(k) for k in ('downloaded', 'stored', 'cached', 'failed', 'gave_up',
                                              'store_checks', 'store_errors', 'storage_unverified')},
               'failure_rate': failures.get('rate'), 'failure_limit': failures.get('limit', IMAGE_FAILURE_LIMIT),
               'external_rate': failures.get('external_rate'),
               'external_limit': failures.get('external_alert_limit', IMAGE_EXTERNAL_ALERT_LIMIT),
               'by_kind': failures.get('by_kind'), 'blocked_by': failures.get('blocked_by'),
               'retry_passes': len((current.get('retry') or {}).get('passes') or []),
               # A block belongs to one attempt: a retry that ended well has none, even if the run's copy had one.
               'stop_reason': (step if step else delivered).get('stop_reason'),
               'started_at': step.get('started_at'), 'finished_at': step.get('finished_at'),
               'span_hours': hours(step.get('started_at'), step.get('finished_at'))}
    detail = phases.get('detail') or {}
    start, end = when(step.get('started_at')), when(step.get('finished_at'))
    first, last = when(detail.get('first')), when(detail.get('last'))
    if start and end and first and last:
        # Overlap: the image step ran while details were still requested (--image-overlap on).
        shared = (min(end, last) - max(start, first)).total_seconds()
        summary['overlap_hours'] = round(max(shared, 0) / 3600, 3)
        summary['sequential_hours'] = round(((last - first) + (end - start)).total_seconds() / 3600, 3)
        summary['combined_hours'] = round((max(end, last) - min(start, first)).total_seconds() / 3600, 3)
    return summary


def raw_rows(path: Path):
    """product_id, status, reuse and fingerprint of each raw record, one line at a time."""
    try:
        with path.open(encoding='utf-8') as stream:
            for line in stream:
                if line.strip():
                    row = json.loads(line)
                    yield (str(row.get('product_id')), row.get('detail_status'), bool(row.get('detail_reused_from')),
                           row.get('listing_fingerprint'))
    except OSError:
        return


def reuse_summary(directory: Path, report) -> dict:
    statuses, requested = Counter(), {}
    reused = 0
    for product_id, status, was_reused, fingerprint in raw_rows(directory / 'product_raw.jsonl'):
        statuses[status] += 1
        reused += was_reused
        if status in REQUESTED and not was_reused:
            requested[product_id] = fingerprint
    summary = {'detail_status': dict(statuses), 'reused': reused, 'detail_requested': len(requested),
               **{k: v for k, v in (report.get('detail_reuse') or {}).items() if k in ('mode', 'base_job_id', 'sampled',
                                                                                      'sample_size', 'base_missing')}}
    base = (report.get('detail_reuse') or {}).get('base_dir')
    if base and (Path(base) / 'product_raw.jsonl').is_file():
        # Why each requested product was asked for: absent from the base run, changed, or neither (sample,
        # or a reuse condition other than the fingerprint failed).
        previous = {pid: fp for pid, _, _, fp in raw_rows(Path(base) / 'product_raw.jsonl')}
        reasons = Counter('new' if pid not in previous else 'changed' if previous[pid] != fp else 'same_fingerprint'
                          for pid, fp in requested.items())
        summary['requested_because'] = dict(reasons)
    return summary


def size(path: Path) -> int:
    total = 0
    for root, _, files in os.walk(path):
        for name in files:
            try:
                total += os.lstat(os.path.join(root, name)).st_size
            except OSError:
                pass
    return total


def disk_summary(directory: Path) -> dict:
    # A hard-linked image counts once per name here: media/categories/ links show what du would not.
    parts = {'details': directory / 'details', 'media_images': directory / 'media' / 'images',
             'media_categories': directory / 'media' / 'categories', 'product_raw': directory / 'product_raw',
             'products': directory / 'products'}
    summary = {f'{name}_gb': round(size(path) / 1024 ** 3, 3) for name, path in parts.items() if path.exists()}
    summary['run_gb'] = round(size(directory) / 1024 ** 3, 3)
    summary['images_pruned'] = (directory / 'media' / 'images_pruned.json').is_file()
    summary['pipeline_loaded'] = (directory / 'pipeline_loaded.json').is_file()
    try:
        summary['free_gb'] = round(shutil.disk_usage(directory).free / 1024 ** 3, 1)
    except OSError:
        summary['free_gb'] = None
    return summary


def rss_summary(path: Path, start=None, end=None) -> dict:
    """Samples between the run's start and end only: the sampler log may hold other runs."""
    start, end = when(start), when(end)
    peak = samples = 0
    pids = set()
    for line in path.read_text(encoding='utf-8').splitlines():
        fields = line.split()
        if len(fields) >= 4 and all(f.isdigit() for f in fields[:4]):
            at = int(fields[0])
            if (start and at < start.timestamp()) or (end and at > end.timestamp()):
                continue
            samples += 1
            pids.add(fields[1])
            peak = max(peak, int(fields[2]), int(fields[3]))
    return {'samples': samples, 'pids': len(pids), 'peak_rss_mb': round(peak / 1024, 1) if samples else None}


def measure(directory: Path, rss_log: Path = None) -> dict:
    directory = Path(directory)
    report = read_json(directory / 'run_report.json')
    if not isinstance(report, dict):
        raise ValueError(f'run_report.json missing or unreadable: {directory}')
    phases = requests_summary(report)
    images = image_summary(directory, report, phases)
    # run_report.finished_at is set when collecting ends; the image step may run on after it.
    ended = max(filter(None, (report.get('finished_at'), images.get('finished_at'))), key=when, default=None)
    result = {'run': {'job_id': report.get('job_id'), 'result': report.get('result'),
                      'started_at': report.get('started_at'), 'resumed_at': report.get('resumed_at'),
                      'finished_at': report.get('finished_at'), 'ended_at': ended,
                      'collect_hours': hours(report.get('started_at'), report.get('finished_at')),
                      'hours': hours(report.get('started_at'), ended),
                      'products': report.get('product_count'), 'requests': report.get('request_count'),
                      'stop_reason': report.get('stop_reason')},
              'requests': phases, 'quality': quality_summary(report), 'images': images,
              'reuse': reuse_summary(directory, report), 'disk': disk_summary(directory)}
    if rss_log:
        result['rss'] = rss_summary(Path(rss_log), report.get('started_at'), ended)
    return result


# The values compared between two runs; the full detail stays in --json.
KEY_VALUES = (('run', 'hours'), ('run', 'collect_hours'), ('run', 'products'), ('run', 'requests'), ('run', 'result'),
              ('requests', 'detail', 'count'), ('requests', 'detail', 'span_hours'), ('requests', 'detail', 'blocked'),
              ('requests', 'listing', 'count'), ('requests', 'listing', 'blocked'),
              ('quality', 'passed'), ('quality', 'violations'), ('quality', 'alerts'),
              ('images', 'requests'), ('images', 'downloaded'), ('images', 'stored'), ('images', 'failure_rate'),
              ('images', 'external_rate'), ('images', 'stop_reason'), ('images', 'span_hours'),
              ('images', 'overlap_hours'), ('images', 'sequential_hours'), ('images', 'combined_hours'),
              ('reuse', 'detail_requested'), ('reuse', 'reused'), ('reuse', 'sampled'), ('reuse', 'requested_because'),
              ('disk', 'run_gb'), ('disk', 'details_gb'), ('disk', 'media_images_gb'), ('disk', 'free_gb'),
              ('rss', 'peak_rss_mb'))


def pick(value, path):
    for key in path:
        value = value.get(key) if isinstance(value, dict) else None
    return value


def table(results) -> str:
    lines = []
    for path in KEY_VALUES:
        values = [pick(r, path) for r in results]
        if all(v is None for v in values):
            continue
        lines.append('\t'.join(['.'.join(path)] + [json.dumps(v, ensure_ascii=False) for v in values]))
    names = dict.fromkeys(name for r in results for name in r['quality'].get('metrics') or {})
    for name in names:
        # Headroom of each gate signal: the calibration reads rate against limit. A run with the gate off has none.
        limit = next(m['limit'] for m in (pick(r, ('quality', 'metrics', name)) for r in results) if m)
        values = [pick(r, ('quality', 'metrics', name, 'rate')) for r in results]
        lines.append('\t'.join([f'quality.{name} (limit {limit})'] + [json.dumps(v) for v in values]))
    return '\n'.join(lines)


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description='Read-only measurements of collection runs')
    parser.add_argument('runs', nargs='+', type=Path, help='one run folder, or two to compare')
    parser.add_argument('--rss-log', type=Path, help='sampler output for the first run')
    parser.add_argument('--json', action='store_true', help='all values as JSON')
    args = parser.parse_args(argv)
    if len(args.runs) > 2:
        parser.error('one or two run folders')
    try:
        results = [measure(run, args.rss_log if i == 0 else None) for i, run in enumerate(args.runs)]
    except ValueError as exc:
        print(str(exc), file=sys.stderr)
        return 2
    print(json.dumps(results if len(results) > 1 else results[0], ensure_ascii=False, indent=1)
          if args.json else table(results))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
