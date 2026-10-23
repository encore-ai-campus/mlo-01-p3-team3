"""Offline Spark event-log aggregation. No polling/actions on the processing path."""
from collections import Counter, defaultdict
import gzip
import json
from pathlib import Path
from ..domain.contracts import digest, Invalid
from .timing import percentile


def report(path, scope_id):
    path = Path(path)
    paths = sorted(p for p in path.rglob('*') if p.is_file() and not p.name.startswith('.') and p.name.startswith('events_')) if path.is_dir() else [path]
    wanted = digest(scope_id)
    jobs, stages, tasks = {}, {}, {}
    application = {}
    ignored, partial = 0, False
    for file in paths:
        if file.suffix in {'.zstd','.lz4','.snappy','.lzf'}:
            raise Invalid('SPARK_EVENT_LOG_CODEC_UNSUPPORTED')
        opener = gzip.open if file.suffix == '.gz' else open
        with opener(file,'rt',encoding='utf-8') as stream:
            for line in stream:
                try:
                    event = json.loads(line)
                except json.JSONDecodeError:
                    partial = True
                    continue
                kind = event.get('Event','')
                if kind == 'SparkListenerApplicationStart':
                    application['started_at_ms'] = event.get('Timestamp')
                elif kind == 'SparkListenerApplicationEnd':
                    application['finished_at_ms'] = event.get('Timestamp')
                elif kind == 'SparkListenerJobStart':
                    properties = event.get('Properties') or {}
                    if properties.get('pipeline.v2.scope_hash') == wanted:
                        job = event['Job ID']
                        jobs[job] = dict(start=event['Submission Time'],end=None,stage=properties.get('pipeline.v2.stage','unknown'))
                        for stage in event.get('Stage IDs',[]):
                            stages.setdefault(stage,set()).add(properties.get('pipeline.v2.stage','unknown'))
                elif kind == 'SparkListenerJobEnd' and event['Job ID'] in jobs:
                    jobs[event['Job ID']]['end'] = event.get('Completion Time')
                elif kind == 'SparkListenerTaskEnd':
                    stage = event['Stage ID']
                    info = event.get('Task Info') or {}
                    # Retries remain distinct; duplicate copies of the same task event do not inflate counters.
                    tasks[(stage,event.get('Stage Attempt ID',0),info.get('Task ID'))] = event
    summaries = {}
    histogram, success_histogram = Counter(), Counter()
    counters = Counter()
    for (stage,attempt,task),event in tasks.items():
        if stage not in stages:
            continue
        info, metrics = event.get('Task Info') or {}, event.get('Task Metrics') or {}
        duration = max(0,info.get('Finish Time',0)-info.get('Launch Time',0))
        histogram[duration] += 1
        success = not info.get('Failed',False) and not info.get('Killed',False)
        if success:
            success_histogram[duration] += 1
        values = dict(task_attempts=1,failed_task_attempts=int(not success),
            task_wall_ms=duration,executor_run_ms=metrics.get('Executor Run Time',0),
            executor_cpu_ns=metrics.get('Executor CPU Time',0),gc_ms=metrics.get('JVM GC Time',0),
            deserialize_ms=metrics.get('Executor Deserialize Time',0),serialize_ms=metrics.get('Result Serialization Time',0),
            memory_spill_bytes=metrics.get('Memory Bytes Spilled',0),disk_spill_bytes=metrics.get('Disk Bytes Spilled',0),
            input_bytes=(metrics.get('Input Metrics') or {}).get('Bytes Read',0),
            input_records=(metrics.get('Input Metrics') or {}).get('Records Read',0),
            shuffle_read_bytes=sum((metrics.get('Shuffle Read Metrics') or {}).get(k,0) for k in ('Remote Bytes Read','Local Bytes Read')),
            shuffle_write_bytes=(metrics.get('Shuffle Write Metrics') or {}).get('Shuffle Bytes Written',0),
            output_bytes=(metrics.get('Output Metrics') or {}).get('Bytes Written',0),
            output_records=(metrics.get('Output Metrics') or {}).get('Records Written',0))
        counters.update(values)
        for name in stages[stage]:
            summaries.setdefault(name,Counter()).update(values)
    # Per pipeline stage: job count and the wall time covered by its jobs (overlaps merged once).
    # A span's time minus this is Driver-side work outside Spark jobs; it is not split further here.
    intervals = defaultdict(list)
    for job in jobs.values():
        if job['end'] is not None:
            intervals[job['stage']].append((job['start'],job['end']))
    by_job_stage = {}
    for name in sorted({job['stage'] for job in jobs.values()}):
        covered, last = 0, None
        for start,end in sorted(intervals[name]):
            if last is None or start > last:
                covered += end-start
                last = end
            elif end > last:
                covered += end-last
                last = end
        by_job_stage[name] = dict(jobs=sum(1 for j in jobs.values() if j['stage'] == name),
            unfinished_jobs=sum(1 for j in jobs.values() if j['stage'] == name and j['end'] is None),job_wall_ms=covered)
    start,end = application.get('started_at_ms'),application.get('finished_at_ms')
    return dict(scope_hash=wanted,job_count=len(jobs),stage_count=len(stages),
        totals=dict(counters),by_stage={k:dict(v) for k,v in sorted(summaries.items())},
        jobs_by_stage=by_job_stage,
        task_p50_ms=percentile(histogram,.5),task_p95_ms=percentile(histogram,.95),
        successful_task_p95_ms=percentile(success_histogram,.95),
        application_wall_ms=end-start if start is not None and end is not None else None,
        partial_log=partial or end is None,
        limitations=['Task time sums include parallel work and retries; they are not application wall time.',
            'Spark input bytes do not represent all object-store GET/PUT/LIST or physical network traffic.'])
