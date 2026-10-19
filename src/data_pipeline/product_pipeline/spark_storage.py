"""Explicit Garage S3A profile, using existing service files and latched writers."""
import os
from pathlib import Path
from urllib.parse import urlsplit

from .errors import PipelineError
from .s3_credentials import service_credentials

PROFILE_ENV = 'PIPELINE_SPARK_S3_PROFILE'
LOG_STORE = 'io.delta.storage.S3SingleDriverLogStore'


def enabled():
    profile = os.environ.get(PROFILE_ENV, '')
    if profile not in {'', 'garage_v241'}:
        raise PipelineError('SPARK_S3_PROFILE_INVALID')
    return profile == 'garage_v241'


def writer_check(lease):
    from .bronze.lease import MongoLease
    if not isinstance(lease, MongoLease):
        raise PipelineError('GARAGE_SPARK_LATCHED_WRITER_REQUIRED')
    lease.check_s3_bucket(os.environ.get('PIPELINE_SPARK_S3_BUCKET', 'pj3-data'))


def storage_options(options, *, writer=False, lease=None):
    result = dict(options)
    if not enabled():
        return result
    if writer:
        writer_check(lease)
    endpoint = os.environ.get('PIPELINE_S3_ENDPOINT', '')
    parsed = urlsplit(endpoint)
    if (parsed.scheme != 'https' or not parsed.hostname or parsed.username or parsed.password
            or parsed.path not in {'', '/'} or parsed.query or parsed.fragment):
        raise PipelineError('S3_TLS_ENDPOINT_REQUIRED')
    if not os.environ.get('PIPELINE_SPARK_S3_CREDENTIALS_FILE'):
        raise PipelineError('SPARK_S3_CREDENTIALS_FILE_REQUIRED')
    truststore = os.environ.get('PIPELINE_SPARK_S3_TRUSTSTORE', '/etc/pj3-clients/s3/ca-truststore.p12')
    jars = ['/opt/pj3/s3-clients/hadoop-3.4.2/hadoop-aws-3.4.2.jar',
            '/opt/pj3/s3-clients/hadoop-3.4.2/bundle-2.29.52.jar']
    if not Path(truststore).is_file() or any(not Path(p).is_file() for p in jars):
        raise PipelineError('GARAGE_SPARK_CLIENT_FILES_MISSING')
    if any(c.isspace() or c in "'\"\\" for c in truststore):
        raise PipelineError('GARAGE_SPARK_TRUSTSTORE_PATH_INVALID')
    if any('fs.s3a.access.key' in k or 'fs.s3a.secret.key' in k for k in result):
        raise PipelineError('SPARK_S3_SECRET_IN_OPTIONS_FORBIDDEN')
    if result.get('spark.delta.logStore.s3a.impl', LOG_STORE) != LOG_STORE:
        raise PipelineError('GARAGE_SPARK_LOGSTORE_CONFLICT')
    # Keep the infrastructure's fixed Hadoop/SDK jars; never fetch another AWS SDK.
    extra = [j for j in result.get('spark.jars', '').split(',') if j]
    if any(('hadoop-aws' in j or 'bundle-' in j) and j not in {'local:'+p for p in jars} for j in extra):
        raise PipelineError('GARAGE_SPARK_JAR_CONFLICT')
    result['spark.jars'] = ','.join(dict.fromkeys([*extra, *('local:'+p for p in jars)]))
    trust = ('-Djavax.net.ssl.trustStore='+truststore+' -Djavax.net.ssl.trustStoreType=PKCS12'
             ' -Djavax.net.ssl.trustStorePassword=changeit')  # Public CA store, no private key.
    for name in ['spark.driver.extraJavaOptions', 'spark.executor.extraJavaOptions']:
        existing = result.get(name, '')
        if 'javax.net.ssl.trustStore' in existing and trust not in existing:
            raise PipelineError('GARAGE_SPARK_TRUSTSTORE_CONFLICT')
        result[name] = existing if trust in existing else (existing+' '+trust).strip()
    result.update({
        'spark.delta.logStore.s3a.impl': LOG_STORE,
        'spark.hadoop.fs.s3a.impl': 'org.apache.hadoop.fs.s3a.S3AFileSystem',
        'spark.hadoop.fs.s3a.endpoint': endpoint,
        'spark.hadoop.fs.s3a.endpoint.region': os.environ.get('AWS_DEFAULT_REGION', 'us-east-1'),
        'spark.hadoop.fs.s3a.path.style.access': 'true',
        'spark.hadoop.fs.s3a.connection.ssl.enabled': 'true',
        'spark.hadoop.fs.s3a.aws.credentials.provider': 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider',
        'spark.hadoop.fs.s3a.change.detection.source': 'etag',
        'spark.hadoop.fs.s3a.change.detection.mode': 'server',
        'spark.hadoop.fs.s3a.change.detection.version.required': 'false',
        'spark.hadoop.fs.s3a.bucket.probe': '0',
        'spark.redaction.regex': '(?i)secret|password|token|access[.]?key|connection|sasl[.]jaas',
    })
    return result


def bind_spark_credentials(spark, *, writer=False, lease=None):
    if not enabled():
        return
    if writer:
        writer_check(lease)
    expected = os.environ.get('PIPELINE_SPARK_S3_TRUSTSTORE', '/etc/pj3-clients/s3/ca-truststore.p12')
    actual = spark.sparkContext._jvm.java.lang.System.getProperty('javax.net.ssl.trustStore')
    if actual != expected:
        raise PipelineError('SPARK_S3_DRIVER_TRUSTSTORE_NOT_APPLIED')
    # Must be called before constructing sources/sinks or touching a lake path.
    values = service_credentials(os.environ.get('PIPELINE_SPARK_S3_CREDENTIALS_FILE', ''))
    try:
        hadoop = spark.sparkContext._jsc.hadoopConfiguration()
        hadoop.set('fs.s3a.access.key', values['aws_access_key_id'])
        hadoop.set('fs.s3a.secret.key', values['aws_secret_access_key'])
    except Exception:
        raise PipelineError('SPARK_S3_CREDENTIAL_BIND_FAILED') from None
