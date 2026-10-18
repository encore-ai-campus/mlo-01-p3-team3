"""Ansible-only bounded verification; no service/config/credential changes."""
import hashlib
import importlib.util
import os
from pathlib import Path
import pwd
import sys
import types
from ansible.module_utils.basic import AnsibleModule


def verify(params):
    role = params['role']
    account = pwd.getpwnam('pj3-' + role)
    os.setgroups([])
    os.setgid(account.pw_gid)
    os.setuid(account.pw_uid)
    os.environ['HOME'] = account.pw_dir
    result = dict(role=role, uid=os.getuid(), status='PREFLIGHT',
                  production_objects_modified=False, service_config_modified=False)
    props = Path('/etc/pj3-clients/s3') / (role + '.properties')
    ca = Path('/etc/pj3-clients/s3/ca.pem')
    result['protected_file_readable'] = os.access(props, os.R_OK)
    result['ca_readable'] = os.access(ca, os.R_OK)
    result['boto3_available'] = importlib.util.find_spec('boto3') is not None
    if role == 'spark':
        jars = Path('/opt/pj3/spark/4.1.3/jars')
        result['delta_jars'] = [str(p) for p in jars.glob('*delta*.jar')]
        for root in [Path('/var/lib/pj3/spark/home/.ivy2'),
                     Path('/var/lib/pj3/spark/home/.ivy2.5'),
                     Path('/var/lib/pj3/spark/s3-verification')]:
            if root.exists():
                result['delta_jars'].extend(str(p) for p in root.rglob('*delta*.jar'))
        result['spark_submit_executable'] = os.access('/opt/pj3/spark/4.1.3/bin/spark-submit', os.X_OK)
    if not (result['protected_file_readable'] and result['ca_readable'] and result['boto3_available']):
        result['status'] = 'BLOCKED_PREREQUISITES'
        return result
    if not params['write_test']:
        return result
    import boto3
    from botocore.config import Config
    values = {}
    for line in props.read_text().splitlines():
        if line.strip() and not line.lstrip().startswith(('#', '!')):
            key, sep, value = line.partition('=')
            if sep:
                values[key.strip()] = value.strip()
    if not values.get('accessKey') or not values.get('secretKey'):
        result['status'] = 'BLOCKED_CREDENTIAL_FORMAT'
        return result
    cfg = Config(signature_version='s3v4', s3={'addressing_style': 'path'},
                 connect_timeout=5, read_timeout=10,
                 retries={'total_max_attempts': 1})
    clients = [boto3.client('s3', endpoint_url='https://192.168.0.'+ip+':8443',
                           region_name='us-east-1', verify=str(ca),
                           aws_access_key_id=values['accessKey'],
                           aws_secret_access_key=values['secretKey'], config=cfg)
               for ip in ['141', '142']]
    def observe_conditional_header(request, **kwargs):
        # Record only this public condition, never Authorization or other headers.
        value = request.headers.get('If-None-Match')
        result['sdk_sent_if_none_match_star'] = value in ('*', b'*')
    clients[1].meta.events.register('before-send.s3.PutObject', observe_conditional_header)
    package = types.ModuleType('product_pipeline'); package.__path__ = []
    sys.modules['product_pipeline'] = package
    for name, source in [('errors', params['errors_source']), ('objects', params['objects_source'])]:
        module = types.ModuleType('product_pipeline.' + name)
        module.__package__ = 'product_pipeline'
        sys.modules[module.__name__] = module
        exec(compile(source, '<verified-project-source-'+name+'>', 'exec'), module.__dict__)
    from product_pipeline.objects import S3Objects
    import tempfile
    key = 'verification/' + params['run_id'] + '/' + role + '-conditional.bin'
    bucket = 'pj3-data'; data = b'pipeline-conditional-verification-v1\n'
    checksum = hashlib.sha256(data).hexdigest()
    result.update(key=key, boto3=boto3.__version__,
                  objects_source_sha256=hashlib.sha256(params['objects_source'].encode()).hexdigest())
    attempted = False
    try:
        objects = S3Objects(bucket, client=clients[0])
        objects.preflight()
        result['head_bucket'] = True
        with tempfile.TemporaryDirectory(prefix='pipeline-garage-check-') as folder:
            source = Path(folder) / 'test.bin'; source.write_bytes(data)
            attempted = True
            if objects.ensure(key, source, checksum, len(data)) is not True:
                raise ValueError('FIRST_WRITE_NOT_CONFIRMED')
            result['first_ensure'] = True
            if objects.ensure(key, source, checksum, len(data)) is not False:
                raise ValueError('REUSE_NOT_CONFIRMED')
            result['duplicate_reused'] = True
            try:
                reply = clients[1].put_object(Bucket=bucket, Key=key, Body=b'different bytes', IfNoneMatch='*')
                result['conditional_put_http'] = reply.get('ResponseMetadata', {}).get('HTTPStatusCode')
            except Exception as exc:
                response = getattr(exc, 'response', {})
                if response.get('ResponseMetadata', {}).get('HTTPStatusCode') != 412:
                    raise ValueError('CONDITIONAL_REJECTION_NOT_412') from exc
                result['conditional_conflict_http'] = 412
            overwritten = False
            for client in clients:
                reply = client.get_object(Bucket=bucket, Key=key)
                try:
                    received = reply['Body'].read(len(data)+1)
                finally:
                    reply['Body'].close()
                if received == b'different bytes':
                    overwritten = True
                elif hashlib.sha256(received).hexdigest() != checksum:
                    raise ValueError('CROSS_ENDPOINT_CONTENT_CHANGED')
            result['conflicting_body_observed'] = overwritten
            if overwritten:
                raise ValueError('CONDITIONAL_OVERWRITE_ALLOWED')
            if result.get('conditional_conflict_http') != 412:
                raise ValueError('CONDITIONAL_REJECTION_NOT_412')
            result['cross_endpoint_sha256_verified'] = True
        result['status'] = 'S3_CONDITIONAL_VERIFIED'
    except Exception as exc:
        result['status'] = 'FAILED'
        result['error_type'] = type(exc).__name__
        if isinstance(exc, ValueError):
            result['error_code'] = str(exc)
        response = getattr(exc, 'response', {})
        result['http_status'] = response.get('ResponseMetadata', {}).get('HTTPStatusCode')
        # Never include SDK exception text: it may contain request headers/credentials.
    finally:
        if attempted:
            try:
                clients[0].delete_object(Bucket=bucket, Key=key)
                try:
                    clients[1].head_object(Bucket=bucket, Key=key)
                except Exception as exc:
                    result['test_object_deleted'] = getattr(exc, 'response', {}).get('ResponseMetadata', {}).get('HTTPStatusCode') == 404
                else:
                    result['test_object_deleted'] = False
            except Exception:
                result['test_object_deleted'] = False
            if not result.get('test_object_deleted'):
                result['status'] = 'CLEANUP_UNCONFIRMED'
        for client in clients:
            client.close()
    return result


def main():
    module = AnsibleModule(argument_spec=dict(
        role=dict(required=True, choices=['nifi', 'spark']),
        run_id=dict(required=True), write_test=dict(type='bool', default=False),
        objects_source=dict(required=True), errors_source=dict(required=True)))
    import re
    if not re.fullmatch(r'pipeline-[a-zA-Z0-9-]+', module.params['run_id']):
        module.fail_json(msg='INVALID_TEST_RUN_ID')
    # Ansible's root-owned payload must remain accessible to its serializer.
    # Drop identity only in the child performing the actual verification.
    import json
    read_fd, write_fd = os.pipe()
    child = os.fork()
    if child == 0:
        os.close(read_fd)
        try:
            result = verify(module.params)
        except Exception as exc:
            result = dict(status='VERIFICATION_TOOL_FAILED', error_type=type(exc).__name__)
        with os.fdopen(write_fd, 'w') as pipe:
            json.dump(result, pipe)
        os._exit(0)
    os.close(write_fd)
    with os.fdopen(read_fd) as pipe:
        result = json.load(pipe)
    os.waitpid(child, 0)
    module.exit_json(changed=module.params['write_test'], result=result)


if __name__ == '__main__':
    main()
