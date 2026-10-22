"""Fixed Python workers, no task-provided command/shell. Spark stays in child Drivers."""
from pathlib import Path
import os
import subprocess
import sys
from uuid import uuid4

from ..contracts import digest, now
from ..errors import PipelineError
from ..files import atomic_json, read_json, parse, sha_file
from .flow import seal, verified, WorkerUnconfirmed


def execute(context):
    root = Path(context['root']); stage = context['stage']; token = str(uuid4())
    directory = root / 'workers' / stage / token; directory.mkdir(parents=True)
    request = directory / 'context.json'; response = directory / 'response.json'
    atomic_json(request, context)
    env = dict(os.environ, PYTHONIOENCODING='utf-8')
    package = str(Path(__file__).resolve().parents[2])
    env['PYTHONPATH'] = package + os.pathsep + env.get('PYTHONPATH', '')
    child = subprocess.Popen([sys.executable, '-m', 'product_pipeline.operations.flow_worker',
        '--context', str(request), '--output', str(response)], env=env, stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
        creationflags=subprocess.CREATE_NO_WINDOW if os.name == 'nt' else 0)
    atomic_json(directory / 'child.json', dict(pid=child.pid, stage=stage, started_at=now()))
    # Drain continuously, storing only allowlisted JSON progress, at most 1 MiB.
    # Raw SDK/JVM output may contain secrets and is intentionally not copied.
    size = 0
    with (directory / 'progress.jsonl').open('wb') as log:
        try:
            while True:
                line = child.stdout.readline(16384)
                if not line: break
                try: value = parse(line.decode('utf-8'))
                except Exception: continue
                if not isinstance(value, dict): continue
                clean = {k: v for k, v in value.items() if k in
                    {'stage', 'status', 'result', 'processed', 'reused', 'duration_ms'}
                    and isinstance(v, (str, int, bool, type(None)))}
                data = (str(__import__('json').dumps(clean))+'\n').encode()
                if clean and size+len(data) <= 1024*1024: log.write(data); size += len(data)
            code = child.wait()
        except BaseException:
            # A Python child may own a JVM. Never pretend that terminate() fences
            # every descendant; retain RUNNING and require explicit stop evidence.
            raise
        finally: child.stdout.close()
    if not response.exists(): raise WorkerUnconfirmed('FLOW_WORKER_RESULT_UNCONFIRMED')
    result = verified(read_json(response))
    if result.get('context_hash') != context['content_hash']: raise PipelineError('FLOW_WORKER_CONTEXT_CHANGED')
    if code or result.get('status') != 'SUCCEEDED':
        if result.get('error_code','').endswith('SHUTDOWN_UNCONFIRMED'):
            raise WorkerUnconfirmed('FLOW_STAGE_SHUTDOWN_UNCONFIRMED')
        if result.get('retryable'):
            from .queue import RetryLater
            raise RetryLater(result.get('error_code','FLOW_WORKER_RETRY_REQUIRED'))
        raise PipelineError(result.get('error_code', 'FLOW_WORKER_FAILED'))
    output = result['output']; output['artifacts'] = [*output.get('artifacts', []),
        dict(path=str(response), sha256=sha_file(response))]
    return output


def spark_session(environment='GOLD_SPARK_OPTIONS'):
    from pyspark.sql import SparkSession
    options = parse(os.environ.get(environment, '{}'))
    if not isinstance(options, dict) or any(not k.startswith('spark.') or not isinstance(v, str) for k,v in options.items()):
        raise PipelineError('FLOW_SPARK_OPTIONS_INVALID')
    from ..spark_storage import storage_options,bind_spark_credentials
    options=storage_options(options)
    builder = SparkSession.builder.appName('pipeline-flow-proof')
    for key, value in options.items(): builder = builder.config(key, value)
    spark = (builder.config('spark.sql.session.timeZone', 'UTC')
        .config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension')
        .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog').getOrCreate())
    bind_spark_credentials(spark)
    return spark


def versions(spark, paths):
    from delta.tables import DeltaTable
    return {name: int(DeltaTable.forPath(spark, path).history(1).select('version').first()[0])
            for name, path in paths.items()}


def settings(plan):
    from ..config import Settings
    from ..bronze.config import BronzeSettings, location
    from ..silver.config import SilverSettings
    from ..gold.config import GoldSettings
    p = {k: v['path'] for k,v in plan['files'].items()}
    source = Settings.load(p['pipeline_config']); bronze = BronzeSettings.load(p['bronze_config'])
    silver = SilverSettings.load(p['silver_config']); gold = GoldSettings.load(p['gold_config'])
    if (len({source.environment_id, bronze.environment_id, silver.environment_id, gold.environment_id}) != 1
            or len({source.mongo_database, bronze.mongo_database, silver.mongo_database}) != 1
            or len({source.mongo_replica_set, bronze.mongo_replica_set, silver.mongo_replica_set}) != 1
            or source.kafka_topic != bronze.kafka_topic
            or location(bronze.bronze_path) != location(silver.bronze_path)
            or location(silver.silver_root) != location(gold.silver_root)):
        raise PipelineError('FLOW_STAGE_TARGET_MISMATCH')
    writes=[location(v) for v in (bronze.bronze_path,bronze.events_path,bronze.quarantine_path,
        bronze.checkpoint_path,silver.silver_root,silver.checkpoint_path,gold.gold_root,gold.checkpoint_path)]
    if any(a==b or a.startswith(b+'/') or b.startswith(a+'/') for i,a in enumerate(writes) for b in writes[i+1:]):
        raise PipelineError('FLOW_STAGE_OUTPUTS_OVERLAP')
    # Require actual owner exports before any ingest, not after data is written.
    from ..silver.inputs import registration as silver_registration, validate_inputs as silver_inputs
    from ..gold.contracts import registration as gold_registration, validate_inputs as gold_inputs
    silver_registration(read_json(p['silver_registration'])); silver_inputs(read_json(p['silver_inputs']))
    gold_registration(read_json(p['gold_registration'])); gold_inputs(read_json(p['gold_inputs']))
    guards = read_json(p['guard_hashes']); mysql = read_json(p['mysql_config'])
    if (not isinstance(guards, dict) or any(not isinstance(v,str) or len(v)!=64 or any(c not in '0123456789abcdef' for c in v) for v in guards.values())
            or mysql.get('contract_version')!=1 or not mysql.get('target_id') or not mysql.get('database')
            or mysql['target_id'].startswith('replace-with-') or not mysql.get('host') or not mysql.get('user')
            or mysql.get('local_test_only')):
        raise PipelineError('FLOW_PUBLICATION_INPUT_REQUIRED')
    return p, source, bronze, silver, gold


def perform(context):
    """Live adapters. Fixture executors are injected only by tests, never by config."""
    from .flow import load
    plan = load(context['root'])
    if plan != context['flow']: raise PipelineError('FLOW_WORKER_PLAN_CHANGED')
    p, source, bronze, silver, gold = settings(plan)
    root = Path(context['root']); stage = context['stage']; outputs = context['outputs']
    if stage == 'INGEST':
        from .dispatch import handlers
        config = dict(mode='live')
        spec = dict(snapshot=plan['snapshot'], snapshot_id=plan['snapshot_id'], state=str(root/'ingest'),
            config=p['pipeline_config'], config_sha256=plan['files']['pipeline_config']['sha256'], target_fingerprint=source.fingerprint)
        result = handlers(config, None)['INGEST'](spec)
        return dict(status='VERIFIED', ingestion=result)
    if stage in {'BRONZE', 'SILVER', 'GOLD'}:
        if stage == 'BRONZE':
            from ..bronze.service import Request, run
            result = run(Request(config=p['bronze_config'], available_now=True)).verified()
            paths = dict(raw=bronze.bronze_path, events=bronze.events_path, quarantine=bronze.quarantine_path)
            spark = spark_session(bronze.spark_options_env)
        elif stage == 'SILVER':
            from ..silver.service import Request, run
            result = run(Request(config=p['silver_config'], bronze_version=outputs['BRONZE']['versions']['raw'],
                inputs=Path(p['silver_inputs']), registration=Path(p['silver_registration']),
                state_dir=root/'silver', scan_id=plan['content_hash'])).verified()
            if result.get('source_drained') is not True: raise PipelineError('FLOW_SILVER_PARTIAL')
            from ..gold.contracts import INPUT_TABLES
            paths = {t: silver.silver_root.rstrip('/')+'/'+t for t in INPUT_TABLES}
            spark = spark_session(silver.spark_options_env)
        else:
            from ..gold.service import Request, run
            fixed = root/'silver-versions.json'
            expected = outputs['SILVER']['versions']
            if fixed.exists() and read_json(fixed) != expected: raise PipelineError('FLOW_FIXED_VERSIONS_CHANGED')
            if not fixed.exists(): atomic_json(fixed, expected)
            result = run(Request(config=Path(p['gold_config']), versions=fixed, inputs=Path(p['gold_inputs']),
                registration=Path(p['gold_registration']), state_dir=root/'gold', as_of=plan['as_of'])).verified()
            if result.get('source_drained') is not True or not result.get('export_manifest_ref'):
                raise PipelineError('FLOW_GOLD_INCOMPLETE_OR_EMPTY')
            return dict(status='VERIFIED', export_manifest_ref=result['export_manifest_ref'],
                        product_count=result['product_count'], artifacts=[dict(path=str(fixed), sha256=sha_file(fixed))])
        try: pinned = versions(spark, paths)
        finally: spark.stop()
        return dict(status='VERIFIED', versions=pinned, stage_result=result)
    if stage == 'PREPARE':
        from ..publication.source import GoldSource
        from ..publication.preparation import prepare_from_source
        target = digest({k: read_json(p['mysql_config'])[k] for k in ('target_id', 'database')})
        directory = root/'publication-plans'; spark = spark_session('PUBLICATION_SPARK_OPTIONS')
        try:
            reader = GoldSource(spark, outputs['GOLD']['export_manifest_ref'], gold.gold_root, gold.checkpoint_path)
            if directory.exists():
                # Deterministic ready-last recovery; inspect every plan, don't overwrite.
                from .dispatch import discover_local_plans
                from .flow_audit import prepared
                result = prepared(directory, reader.manifest['content_hash'], target)
            else:
                result = prepare_from_source(reader, read_json(p['guard_hashes']), target, directory)
        finally: spark.stop()
        ready = directory/'ready.json'
        return dict(status='VERIFIED', directory=str(directory), blocked=result['blocked'], plans=result['plans'],
                    artifacts=[dict(path=str(ready), sha256=sha_file(ready))])
    if stage == 'PUBLISH':
        from ..publication.mysql import MySQL
        from ..publication.plan import load_plan
        from ..publication.runner import publish
        from .flow_audit import prepared
        directory = Path(outputs['PREPARE']['directory']); ready = read_json(directory/'ready.json')
        prepared(directory, ready['export_hash'], ready['target'])
        if not ready['plans']: return dict(status='VERIFIED', plans=0, products=0)
        db = MySQL(read_json(p['mysql_config']))
        try:
            db.preflight(); count = 0
            for name in sorted(ready['plans']):
                from ..files import confined
                item = load_plan(confined(directory, name)); publish(item, db); count += len(item['products'])
        finally: db.close()
        return dict(status='VERIFIED', plans=len(ready['plans']), products=count)
    if stage == 'RECONCILE':
        from .flow_audit import live_audit
        return live_audit(context, p, source, bronze, silver, gold)
    raise PipelineError('FLOW_STAGE_UNSUPPORTED')
