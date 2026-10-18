"""Garage images -> one Gemini key -> durable receipts -> latched Delta batches.

The first HTTP 429 stops the run. No key rotation, quota bypass or auto retry.
Spark lives only in short child processes, never across provider waits.
"""
import argparse
import base64
from datetime import datetime, timezone
import hashlib
import io
import json
import math
import os
from pathlib import Path
import re
import signal
import stat
import subprocess
import sys
import tempfile
import time
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from .contracts import canonical, digest, now
from .embedding_vectors import row as vector_row
from .errors import PipelineError
from .files import atomic_json, lock, read_json

MODEL = 'gemini-embedding-2'
PREPROCESSING = 'exif-rgb-fit1024-png-l2-v1'
OBJECT = re.compile(r'^images/sha256/([0-9a-f]{2})/([0-9a-f]{64})\.(jpg|jpeg|png|webp)$')


def protected(path):
    path = Path(path)
    info = path.lstat()
    if (not stat.S_ISREG(info.st_mode) or path.is_symlink() or info.st_size > 1024*1024
            or (os.name == 'posix' and (info.st_uid != os.geteuid() or stat.S_IMODE(info.st_mode) != 0o600))):
        raise PipelineError('EMBEDDING_PROTECTED_CONFIG_INVALID')
    return path.read_text(encoding='utf-8-sig')


def load_config(path):
    cfg = json.loads(protected(path))
    if (cfg.get('contract') != 'garage-embedding-worker-v1' or cfg.get('model') != MODEL
            or cfg.get('preprocessing') != PREPROCESSING or cfg.get('dimension') != 768
            or cfg.get('bucket') != 'pj3-data' or cfg.get('endpoint') not in
            {'https://192.168.0.141:8443', 'https://192.168.0.142:8443'}):
        raise PipelineError('EMBEDDING_WORKER_CONFIG_INVALID')
    return cfg


def storage(cfg):
    import boto3
    from botocore.config import Config
    from .s3_credentials import service_credentials
    values = service_credentials(cfg['s3_credentials_file'])
    return boto3.client('s3', endpoint_url=cfg['endpoint'], region_name='us-east-1',
        aws_access_key_id=values['aws_access_key_id'], aws_secret_access_key=values['aws_secret_access_key'],
        verify=cfg['s3_ca'], config=Config(signature_version='s3v4', connect_timeout=5, read_timeout=30,
            s3={'addressing_style': 'path'}, retries={'max_attempts': 2},
            request_checksum_calculation='when_required', response_checksum_validation='when_required'))


def images(client, bucket):
    seen = set()
    for page in client.get_paginator('list_objects_v2').paginate(Bucket=bucket, Prefix='images/sha256/'):
        for item in page.get('Contents', []):
            match = OBJECT.fullmatch(item['Key'])
            if not match or match[1] != match[2][:2] or match[2] in seen:
                continue
            seen.add(match[2])
            if not 0 < item['Size'] <= 15*1024*1024:
                continue
            yield dict(key=item['Key'], checksum=match[2], bytes=item['Size'])


def prepare(client, bucket, item):
    from PIL import Image, ImageOps
    response = client.get_object(Bucket=bucket, Key=item['key'])
    try:
        raw = response['Body'].read(15*1024*1024+1)
    finally:
        response['Body'].close()
    if len(raw) != item['bytes'] or hashlib.sha256(raw).hexdigest() != item['checksum']:
        raise PipelineError('EMBEDDING_ORIGINAL_CHECKSUM_MISMATCH')
    with Image.open(io.BytesIO(raw)) as image:
        if image.format not in {'JPEG', 'PNG', 'WEBP'}:
            raise PipelineError('EMBEDDING_IMAGE_FORMAT_UNSUPPORTED')
        image.load()
        image = ImageOps.exif_transpose(image).convert('RGB')
        image.thumbnail((1024, 1024), Image.Resampling.LANCZOS)
        output = io.BytesIO()
        image.save(output, format='PNG')
    return output.getvalue()


class RateLimited(Exception):
    def __init__(self, seconds):
        self.seconds = seconds
        super().__init__('HTTP_429')


def embed(png, cfg, key, *, opener=urlopen):
    body = dict(model='models/'+cfg['model'], content=dict(parts=[dict(inlineData=dict(
        mimeType='image/png', data=base64.b64encode(png).decode('ascii')))]),
        outputDimensionality=cfg['dimension'])
    request = Request('https://generativelanguage.googleapis.com/v1beta/models/'+cfg['model']+':embedContent',
        data=canonical(body).encode(), headers={'Content-Type': 'application/json', 'x-goog-api-key': key}, method='POST')
    try:
        with opener(request, timeout=120) as response:
            return json.loads(response.read(512*1024))
    except HTTPError as error:
        if error.code == 429:
            try:
                seconds = max(0.0, float(error.headers.get('Retry-After', '60')))
            except (ValueError, TypeError):
                seconds = 60.0
            if not math.isfinite(seconds):
                seconds = 60.0
            error.close()
            raise RateLimited(seconds) from None
        code = error.code
        error.close()
        raise PipelineError('EMBEDDING_HTTP_'+str(code)) from None


def result(payload, checksum, preprocessed_checksum, cfg):
    try:
        values = payload['embedding']['values']
        if (not isinstance(values, list) or len(values) != cfg['dimension']
                or any(isinstance(x, bool) or not isinstance(x, (int, float)) or not math.isfinite(x) for x in values)):
            raise ValueError()
        norm = math.sqrt(sum(x*x for x in values))
        if not math.isfinite(norm) or norm <= 0:
            raise ValueError()
    except (KeyError, TypeError, ValueError, OverflowError):
        raise PipelineError('EMBEDDING_RESPONSE_INVALID') from None
    return vector_row(dict(image_checksum=checksum, preprocessed_checksum=preprocessed_checksum,
        model_version=cfg['model'], preprocessing_version=cfg['preprocessing'], dimension=cfg['dimension'],
        vector=[x/norm for x in values], created_at=now()))


def serializable(value):
    return {**value, 'created_at': value['created_at'].isoformat()}


def delta_child(cfg, work, action, input_path=None):
    """A separate Spark lifetime makes releasing the writer claim unambiguous."""
    command = [sys.executable, '-B', '-m', 'product_pipeline.garage_embeddings', '--config', str(cfg['_config_path']),
               '--work-dir', str(work), '--delta-task', action]
    if input_path:
        command += ['--input', str(input_path)]
    # No secrets in argv. The child loads protected service files itself.
    child = subprocess.run(command, capture_output=True, text=True)
    reports = []
    for line in child.stdout.splitlines():
        if line.startswith('{'):
            try:
                reports.append(json.loads(line))
            except ValueError:
                pass
    if child.returncode:
        code = next((r.get('error_code') for r in reversed(reports) if r.get('error_code')), 'DELTA_CHILD_FAILED')
        raise PipelineError(code)
    if not reports:
        raise PipelineError('DELTA_CHILD_REPORT_MISSING')
    return reports[-1]


def spark_environment(cfg, temporary):
    from urllib.parse import quote
    deps = read_json(cfg['dependencies_manifest'])
    if any(hashlib.sha256(Path(j).read_bytes()).hexdigest() != deps['sha256'][Path(j).name] for j in deps['jars']):
        raise PipelineError('EMBEDDING_SPARK_DEPENDENCY_CHANGED')
    accounts = json.loads(protected(cfg['mongo_accounts_file']))
    account = next(a for a in accounts['accounts'] if a['username'] == 'pipeline_spark')
    uri = ('mongodb://pipeline_spark:'+quote(account['password'], safe='')+
        '@mongo01.pj3.internal:27017,mongo02.pj3.internal:27017,mongo03.pj3.internal:27017/'
        '?replicaSet=rs-products&authSource=admin&tls=true&tlsCAFile='+quote(accounts['ca'], safe=''))
    os.environ.update(dict(HOME=temporary, TMPDIR=temporary, SPARK_LOCAL_DIRS=temporary,
        JAVA_HOME='/usr/lib/jvm/java-21-openjdk-amd64', SPARK_CONF_DIR='/etc/pj3/spark', SPARK_LOCAL_IP='127.0.0.1',
        PYSPARK_PYTHON=sys.executable, PYSPARK_DRIVER_PYTHON=sys.executable,
        PIPELINE_SPARK_S3_PROFILE='garage_v241', PIPELINE_S3_ENDPOINT=cfg['endpoint'],
        PIPELINE_SPARK_S3_BUCKET=cfg['bucket'], PIPELINE_SPARK_S3_CREDENTIALS_FILE=cfg['s3_credentials_file'],
        PIPELINE_SPARK_S3_TRUSTSTORE='/etc/pj3-clients/s3/ca-truststore.p12',
        EMBEDDING_LEASE_MONGO_URI=uri, AWS_DEFAULT_REGION='us-east-1',
        SPARK_SUBMIT_OPTS='-Djavax.net.ssl.trustStore=/etc/pj3-clients/s3/ca-truststore.p12 '
            '-Djavax.net.ssl.trustStoreType=PKCS12 -Djavax.net.ssl.trustStorePassword=changeit',
        EMBEDDING_SPARK_OPTIONS=canonical({'spark.master':'local[1]', 'spark.driver.memory':'768m',
            'spark.ui.enabled':'false','spark.sql.shuffle.partitions':'1','spark.default.parallelism':'1',
            'spark.databricks.delta.snapshotPartitions':'1','spark.jars':','.join(deps['jars']),
            'spark.sql.warehouse.dir':temporary+'/warehouse'})))


def delta_task(cfg, work, action, input_path):
    from .embedding_table import Settings, main as table_main
    args = ['--environment-id','pj3-danawa','--silver-root','s3a://pj3-data/silver',
            '--checkpoint-path','s3a://pj3-data/checkpoints/product-image-embeddings']
    with tempfile.TemporaryDirectory(prefix='.spark-embedding-', dir=work) as temporary:
        spark_environment(cfg, temporary)
        if action == 'commit':
            return table_main(args+['--import-jsonl', str(input_path)])
        from pyspark.sql import SparkSession, functions as F
        from .spark_storage import storage_options, bind_spark_credentials
        from .embedding_table import inspect
        options = storage_options(json.loads(os.environ['EMBEDDING_SPARK_OPTIONS']))
        builder = SparkSession.builder.appName('embedding-inventory')
        for key, value in options.items():
            builder = builder.config(key, value)
        spark = None
        try:
            spark = (builder.config('spark.sql.extensions','io.delta.sql.DeltaSparkSessionExtension')
                .config('spark.sql.catalog.spark_catalog','org.apache.spark.sql.delta.catalog.DeltaCatalog')
                .config('spark.sql.session.timeZone','UTC').getOrCreate())
            bind_spark_credentials(spark)
            spark.sparkContext.setLogLevel('ERROR')
            settings = Settings('pj3-danawa','s3a://pj3-data/silver','s3a://pj3-data/checkpoints/product-image-embeddings')
            state = inspect(spark, settings)
            if state['status'] != 'READY':
                raise PipelineError('EMBEDDING_TABLE_NOT_READY')
            rows = (spark.read.format('delta').option('versionAsOf',state['version']).load(settings.table_path)
                .filter((F.col('model_version')==cfg['model']) & (F.col('preprocessing_version')==cfg['preprocessing'])
                        & (F.col('dimension')==cfg['dimension'])))
            inventory = {}
            for raw in rows.toLocalIterator():
                value = vector_row(raw.asDict(recursive=True))
                if value['image_checksum'] in inventory:
                    raise PipelineError('DUPLICATE_DELTA_EMBEDDING_IDENTITY')
                inventory[value['image_checksum']] = value['record_hash']
            atomic_json(work/'delta-inventory.json', dict(version=state['version'], records=inventory))
            print(canonical(dict(status='INVENTORY_READY', delta_version=state['version'], vectors=len(inventory))))
        finally:
            if spark is not None:
                spark.stop()
    return 0


def run(cfg, work, *, max_api_calls=0, rpm=10, batch_size=25, reuse_file=None, plan=False):
    if not 1 <= rpm <= 60 or not 1 <= batch_size <= 100 or max_api_calls < 0:
        raise PipelineError('EMBEDDING_RUN_LIMIT_INVALID')
    work = Path(work).resolve()
    work.mkdir(parents=True, exist_ok=True, mode=0o700)
    with lock(work/'worker.lock'):
        delta_child(cfg, work, 'inventory')
        inventory = read_json(work/'delta-inventory.json')['records']
        reuse = {}
        if reuse_file:
            with Path(reuse_file).open(encoding='utf-8') as stream:
                for line in stream:
                    value = vector_row(json.loads(line))
                    if (value['model_version'],value['preprocessing_version'],value['dimension']) != (
                            cfg['model'],cfg['preprocessing'],cfg['dimension']):
                        raise PipelineError('REUSE_EMBEDDING_SETTINGS_MISMATCH')
                    if value['image_checksum'] in reuse and reuse[value['image_checksum']]['record_hash'] != value['record_hash']:
                        raise PipelineError('REUSE_EMBEDDING_CONFLICT')
                    reuse[value['image_checksum']] = value
        client = storage(cfg)
        inputs = list(images(client, cfg['bucket']))
        summary = dict(status='PLANNED', source_images=len(inputs), completed_before=len(inventory),
            matching_reuse=sum(x['checksum'] in reuse and x['checksum'] not in inventory for x in inputs),
            new_images=sum(x['checksum'] not in reuse and x['checksum'] not in inventory for x in inputs),
            api_attempts=0, api_completed=0, reused=0, delta_inserted=0)
        if plan:
            return summary
        key = protected(cfg['api_key_file']).strip()
        if not key:
            raise PipelineError('EMBEDDING_API_KEY_MISSING')
        receipts = work/'receipts'
        receipts.mkdir(exist_ok=True, mode=0o700)
        pending = []
        previous = read_json(work/'state.json') if (work/'state.json').exists() else {}
        if previous.get('next_retry_at',0) > time.time():
            raise PipelineError('PROVIDER_RETRY_AFTER_NOT_ELAPSED')
        last_call = previous.get('last_api_started',0)
        def save():
            atomic_json(work/'state.json', dict(summary, updated_at=now(), last_api_started=last_call))
        def flush():
            if not pending:
                return
            with tempfile.TemporaryDirectory(prefix='.delta-input-',dir=work) as temporary:
                target = Path(temporary)/'vectors.jsonl'
                target.write_text(''.join(canonical(serializable(v))+'\n' for v in pending),encoding='utf-8')
                report = delta_child(cfg, work, 'commit', target)
            summary['delta_inserted'] += report['inserted']
            for value in pending:
                inventory[value['image_checksum']] = value['record_hash']
            pending.clear()
            save()
            print(canonical(dict(event='delta_committed', **report)), flush=True)
        summary['status'] = 'RUNNING'
        save()
        try:
            for item in inputs:
                checksum = item['checksum']
                if checksum in inventory:
                    continue
                identity = digest(dict(checksum=checksum,model=cfg['model'],preprocessing=cfg['preprocessing'],dimension=cfg['dimension']))
                receipt = receipts/(identity+'.json')
                png = prepare(client, cfg['bucket'], item)
                prepared = hashlib.sha256(png).hexdigest()
                if receipt.exists():
                    cached = read_json(receipt)
                    if cached['image_checksum'] != checksum or cached['preprocessed_checksum'] != prepared:
                        raise PipelineError('RECEIPT_IMAGE_IDENTITY_CHANGED')
                    if (cached.get('model'), cached.get('preprocessing'), cached.get('dimension')) != (
                            cfg['model'], cfg['preprocessing'], cfg['dimension']):
                        raise PipelineError('RECEIPT_SETTINGS_CHANGED')
                    value = result(cached['payload'],checksum,prepared,cfg)
                    summary['reused'] += 1
                elif checksum in reuse:
                    value = reuse[checksum]
                    if value['preprocessed_checksum'] != prepared:
                        raise PipelineError('REUSE_PREPROCESSING_CHECKSUM_MISMATCH')
                    summary['reused'] += 1
                else:
                    if max_api_calls and summary['api_attempts'] >= max_api_calls:
                        summary['status'] = 'LIMIT_REACHED'
                        break
                    time.sleep(max(0,60/rpm-(time.time()-last_call)))
                    last_call = time.time()
                    summary['api_attempts'] += 1
                    save()
                    payload = embed(png,cfg,key)
                    # Save a successful provider response before validation or Delta work.
                    atomic_json(receipt,dict(image_checksum=checksum,preprocessed_checksum=prepared,
                        model=cfg['model'],preprocessing=cfg['preprocessing'],dimension=cfg['dimension'],payload=payload))
                    value = result(payload,checksum,prepared,cfg)
                    summary['api_completed'] += 1
                pending.append(value)
                save()
                print(canonical(dict(event='embedding_ready',checksum=checksum,api_completed=summary['api_completed'],reused=summary['reused'])),flush=True)
                if len(pending) >= batch_size:
                    flush()
                if max_api_calls and summary['api_attempts'] >= max_api_calls:
                    summary['status'] = 'LIMIT_REACHED'
                    break
            else:
                summary['status'] = 'COMPLETED'
        except RateLimited as error:
            summary.update(status='RATE_LIMIT_429',next_retry_at=time.time()+error.seconds)
        except KeyboardInterrupt:
            summary['status'] = 'INTERRUPTED'
        except Exception:
            summary['status'] = 'FAILED'
            save()
            raise
        finally:
            # Includes successes before HTTP 429. Failed Delta commits remain recoverable from receipts/reuse input.
            flush()
            save()
        return summary


def main(argv=None):
    if os.name == 'posix':
        os.umask(0o077)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config',type=Path,required=True)
    parser.add_argument('--work-dir',type=Path,required=True)
    parser.add_argument('--max-api-calls',type=int,default=0,help='0=until all images or first 429')
    parser.add_argument('--requests-per-minute',type=int,default=10)
    parser.add_argument('--commit-every',type=int,default=25)
    parser.add_argument('--reuse-jsonl',type=Path)
    parser.add_argument('--plan',action='store_true')
    parser.add_argument('--delta-task',choices=['inventory','commit'],help=argparse.SUPPRESS)
    parser.add_argument('--input',type=Path,help=argparse.SUPPRESS)
    args = parser.parse_args(argv)
    try:
        cfg = load_config(args.config)
        cfg['_config_path'] = args.config.resolve()
        args.work_dir.mkdir(parents=True,exist_ok=True,mode=0o700)
        if args.delta_task:
            return delta_task(cfg,args.work_dir,args.delta_task,args.input)
        previous = signal.signal(signal.SIGTERM,lambda *_: (_ for _ in ()).throw(KeyboardInterrupt()))
        try:
            report = run(cfg,args.work_dir,max_api_calls=args.max_api_calls,rpm=args.requests_per_minute,
                batch_size=args.commit_every,reuse_file=args.reuse_jsonl,plan=args.plan)
        finally:
            signal.signal(signal.SIGTERM,previous)
        print(canonical(report),flush=True)
        return 75 if report['status']=='RATE_LIMIT_429' else 130 if report['status']=='INTERRUPTED' else 0
    except KeyboardInterrupt:
        print(canonical(dict(status='INTERRUPTED')),flush=True)
        return 130
    except Exception as error:
        print(canonical(dict(status='FAILED',error_code=error.code if isinstance(error,PipelineError) else type(error).__name__)),flush=True)
        return 2


if __name__ == '__main__':
    raise SystemExit(main())
