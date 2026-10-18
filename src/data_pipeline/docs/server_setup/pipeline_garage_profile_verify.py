"""Ansible-only verification of the actual Garage application profile."""
import hashlib
import json
import os
from pathlib import Path
import pwd
import sys
import tempfile
import types
from ansible.module_utils.basic import AnsibleModule


def run(params):
    role = params['role']; identity = pwd.getpwnam('pj3-'+role)
    os.setgroups([]); os.setgid(identity.pw_gid); os.setuid(identity.pw_uid)
    os.environ['HOME'] = identity.pw_dir
    os.environ['AWS_CA_BUNDLE'] = '/etc/pj3-clients/s3/ca.pem'
    os.environ['AWS_DEFAULT_REGION'] = 'us-east-1'
    package = types.ModuleType('product_pipeline'); package.__path__ = []
    sys.modules['product_pipeline'] = package
    for name in ['errors', 'contracts', 's3_credentials', 'objects']:
        item = types.ModuleType('product_pipeline.'+name); item.__package__='product_pipeline'
        sys.modules[item.__name__] = item
        exec(compile(params['sources'][name], '<project-'+name+'>', 'exec'), item.__dict__)
    from product_pipeline.objects import S3Objects
    from product_pipeline.errors import Conflict
    import boto3
    stores = [S3Objects('pj3-data','https://192.168.0.'+ip+':8443',profile='garage_v241',
              credentials_file='/etc/pj3-clients/s3/'+role+'.properties') for ip in ['141','142']]
    first, other = stores
    if params.get('audit_prefixes'):
        import re
        counts={}
        try:
            for prefix in params['audit_prefixes']:
                if not re.fullmatch(r'verification/pipeline-garage-delta-[a-f0-9]{32}/',prefix):
                    raise ValueError('INVALID_OWNED_AUDIT_PREFIX')
                response=first.client.list_objects_v2(Bucket='pj3-data',Prefix=prefix,MaxKeys=1)
                counts[prefix]=len(response.get('Contents',[]))
            return dict(status='TEST_PREFIXES_EMPTY' if not any(counts.values()) else 'TEST_OBJECTS_REMAIN',
                        prefixes=counts,objects_modified=False)
        finally:
            for store in stores:store.client.close()
    result = dict(status='STARTED',role=role,boto3=boto3.__version__,
        production_records_modified=False,service_config_modified=False,
        source_hashes={name:hashlib.sha256(source.encode()).hexdigest() for name,source in params['sources'].items()})
    counts={'requests':0,'puts':0,'conditional_puts':0}
    def request(request, **kwargs):
        counts['requests']+=1
        if request.method=='PUT':
            counts['puts']+=1
            if request.headers.get('If-None-Match') is not None:counts['conditional_puts']+=1
    first.client.meta.events.register('before-send.s3',request)
    owned=[]
    data=b'PJ3-GARAGE-PROFILE-TEST\0'+os.urandom(32)
    sha=hashlib.sha256(data).hexdigest()
    keys=[f'images/sha256/{sha[:2]}/{sha}.bin','handoff/chunks/sha256/'+sha]
    try:
        first.preflight()
        with tempfile.TemporaryDirectory(prefix='pj3-garage-profile-') as folder:
            source=Path(folder)/'test.bin';source.write_bytes(data)
            for key in keys:
                try:
                    first.client.head_object(Bucket='pj3-data',Key=key)
                except Exception as exc:
                    if getattr(exc,'response',{}).get('ResponseMetadata',{}).get('HTTPStatusCode')!=404:raise
                else:
                    raise ValueError('TEST_KEY_PREEXISTED')
                owned.append(key)
                if key==keys[1]:
                    # Real server write, deliberately lost acknowledgement in this client.
                    raw=first.client
                    class LostAck:
                        def __getattr__(self,name):return getattr(raw,name)
                        def put_object(self,**kwargs):
                            raw.put_object(**kwargs)
                            raise TimeoutError()
                    first.client=LostAck()
                try:
                    if first.ensure(key,source,sha,len(data)) is not True:raise ValueError('FIRST_WRITE_NOT_CONFIRMED')
                finally:
                    if key==keys[1]:first.client=raw
                if first.ensure(key,source,sha,len(data)) is not False:raise ValueError('REUSE_NOT_CONFIRMED')
                if not other._exists(key,sha,len(data)):raise ValueError('CROSS_ENDPOINT_READ_FAILED')
            result.update(image_created_and_reused=True,handoff_chunk_created_and_reused=True,
                          cross_endpoint_sha256=True,lost_ack_recovered=True)
            before=counts['requests'];source.write_bytes(b'wrong content')
            try:
                first.ensure(keys[0],source,hashlib.sha256(b'wrong content').hexdigest(),13)
            except Conflict as exc:
                if exc.code!='OBJECT_KEY_CONTENT_MISMATCH':raise
            else:raise ValueError('WRONG_CONTENT_NOT_REJECTED')
            if counts['requests']!=before:raise ValueError('WRONG_CONTENT_REACHED_HTTP')
            result['wrong_content_rejected_before_http']=True
            if not other._exists(keys[0],sha,len(data)):raise ValueError('ORIGINAL_CONTENT_CHANGED')
            result['original_content_preserved']=True
        if counts['conditional_puts']:raise ValueError('CONDITIONAL_HEADER_STILL_USED')
        result.update(status='GARAGE_PROFILE_VERIFIED',http_counts=dict(counts),test_keys=keys)
    except Exception as exc:
        result.update(status='FAILED',error_type=type(exc).__name__)
        if isinstance(exc,ValueError):result['error_code']=str(exc)
        elif hasattr(exc,'code'):result['error_code']=exc.code
    finally:
        deleted=[]
        for key in owned:
            try:
                first.client.delete_object(Bucket='pj3-data',Key=key)
                try:other.client.head_object(Bucket='pj3-data',Key=key)
                except Exception as exc:
                    if getattr(exc,'response',{}).get('ResponseMetadata',{}).get('HTTPStatusCode')==404:
                        deleted.append(key)
            except Exception:pass
        result['test_objects_deleted']=len(deleted)==len(owned)
        if len(deleted)!=len(owned):
            result.update(status='CLEANUP_UNCONFIRMED',remaining_test_keys=sorted(set(owned)-set(deleted)))
        for store in stores:store.client.close()
    return result


def main():
    module=AnsibleModule(argument_spec=dict(role=dict(required=True,choices=['nifi','spark']),
        sources=dict(type='dict',required=True),audit_prefixes=dict(type='list',elements='str',default=[])))
    read_fd,write_fd=os.pipe();child=os.fork()
    if child==0:
        os.close(read_fd)
        try:result=run(module.params)
        except Exception as exc:result=dict(status='TOOL_FAILED',error_type=type(exc).__name__)
        with os.fdopen(write_fd,'w') as stream:json.dump(result,stream)
        os._exit(0)
    os.close(write_fd)
    with os.fdopen(read_fd) as stream:result=json.load(stream)
    os.waitpid(child,0)
    module.exit_json(changed=True,result=result)


if __name__=='__main__':main()
