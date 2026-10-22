"""Run on NiFi VM: provision the production timer using existing mutual TLS.

No token/password is printed. Existing isolated verification processors remain
untouched. Configuration mismatch fails rather than editing a different flow.
"""
import importlib.util,json,ssl,time,urllib.request
from pathlib import Path

BASE='https://192.168.0.125:8443/nifi-api'
GROUP='product-pipeline-production'
PERIOD='5 min'
VERSION='2.12.0'


def main():
    root=Path('/var/lib/pj3/product-pipeline/nifi')
    spec=importlib.util.spec_from_file_location('product_pipeline.operations.nifi_verified',root/'nifi_client_verified.py')
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    context=ssl.create_default_context(cafile='/etc/pj3-secrets/nifi/ca.pem')
    context.load_cert_chain('/etc/pj3-secrets/nifi/cert.pem','/etc/pj3-secrets/nifi/key.pem')
    opener=urllib.request.build_opener(urllib.request.ProxyHandler({}),urllib.request.HTTPSHandler(context=context))
    def request(method,path,body=None):
        data=None if body is None else json.dumps(body).encode()
        req=urllib.request.Request(BASE+path,data=data,method=method,headers={'Accept':'application/json','Content-Type':'application/json'})
        with opener.open(req,timeout=20) as response:return json.load(response)
    client=module.Client(BASE,ca_file='/etc/pj3-secrets/nifi/ca.pem',request=request)
    assert request('GET','/flow/about')['about']['version']==VERSION,'NIFI_VERSION_MISMATCH'
    rid=request('GET','/flow/process-groups/root')['processGroupFlow']['id']
    groups=request('GET','/process-groups/'+rid+'/process-groups').get('processGroups',[])
    matches=[g for g in groups if g['component']['name']==GROUP]
    assert len(matches)<=1,'DUPLICATE_PRODUCTION_GROUP'
    if matches:gid=matches[0]['component']['id']
    else:gid=request('POST','/process-groups/'+rid+'/process-groups',dict(revision=dict(version=0),component=dict(name=GROUP,position=dict(x=600.0,y=100.0))))['component']['id']
    plan=module.remote_processor_plan('/etc/pj3-clients/pipeline-trigger/ssh.conf',str(root),period=PERIOD)
    plan['name']='product-pipeline-production-control'
    plan['config']['properties']['Command Arguments']='-F;/etc/pj3-clients/pipeline-trigger/ssh.conf;pj3-pipeline-tick;production-tick'
    prepared=client.prepare(gid,plan,expected_version=VERSION,apply=True)
    pid=prepared['processor_id']
    for _ in range(30):
        entity=request('GET','/processors/'+pid)
        if entity['component'].get('validationStatus')=='VALID':break
        time.sleep(.5)
    else:raise RuntimeError('NIFI_PROCESSOR_NOT_VALID')
    if entity['component']['state']!='RUNNING':
        client.set_running(pid,plan,expected_version=VERSION,running=True)
    for _ in range(20):
        entity=request('GET','/processors/'+pid)
        if entity['component']['state']=='RUNNING':break
        time.sleep(.5)
    else:raise RuntimeError('NIFI_START_UNCONFIRMED')
    result=dict(status='RUNNING',version=VERSION,group_id=gid,processor_id=pid,period=PERIOD,
        concurrency=1,execution_node='PRIMARY',command='production-tick',configuration=prepared['status'],
        existing_verification_flow_preserved=True,credentials_in_repository=False,
        validation_status=entity['component']['validationStatus'],
        active_threads=entity.get('status',{}).get('aggregateSnapshot',{}).get('activeThreadCount'))
    (root/'production-nifi-control.json').write_text(json.dumps(result));(root/'production-nifi-control.json').chmod(0o600)
    print(json.dumps(result))


if __name__=='__main__':main()
