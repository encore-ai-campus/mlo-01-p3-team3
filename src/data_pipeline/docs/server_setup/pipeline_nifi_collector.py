"""Connect the deployed collector to NiFi without starting the processor."""
import importlib.util,json,ssl,time,urllib.request
from pathlib import Path


def main():
    root=Path('/var/lib/pj3/product-pipeline/nifi')
    control=json.loads((root/'collector/config/collector-control.json').read_text())
    assert control['enabled'] is False,'COLLECTOR_MUST_BE_DISABLED'
    spec=importlib.util.spec_from_file_location('product_pipeline.operations.nifi_verified',root/'nifi_client_verified.py')
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    context=ssl.create_default_context(cafile='/etc/pj3-secrets/nifi/ca.pem')
    context.load_cert_chain('/etc/pj3-secrets/nifi/cert.pem','/etc/pj3-secrets/nifi/key.pem')
    opener=urllib.request.build_opener(urllib.request.ProxyHandler({}),urllib.request.HTTPSHandler(context=context))
    base='https://192.168.0.125:8443/nifi-api'
    def request(method,path,body=None):
        req=urllib.request.Request(base+path,data=None if body is None else json.dumps(body).encode(),method=method,headers={'Accept':'application/json','Content-Type':'application/json'})
        with opener.open(req,timeout=20) as response:return json.load(response)
    client=module.Client(base,ca_file='/etc/pj3-secrets/nifi/ca.pem',request=request)
    assert request('GET','/flow/about')['about']['version']=='2.12.0','NIFI_VERSION_MISMATCH'
    rid=request('GET','/flow/process-groups/root')['processGroupFlow']['id']
    groups=request('GET','/process-groups/'+rid+'/process-groups').get('processGroups',[])
    matches=[g for g in groups if g['component']['name']=='danawa-collector']
    assert len(matches)<=1,'DUPLICATE_COLLECTOR_GROUP'
    gid=matches[0]['component']['id'] if matches else request('POST','/process-groups/'+rid+'/process-groups',dict(revision=dict(version=0),component=dict(name='danawa-collector',position=dict(x=950.0,y=100.0))))['component']['id']
    plan=module.processor_plan(control['python'],str(root/'collector/config/collector-control.json'),str(root/'collector'),period='1440 min')
    plan['name']='danawa-collector-manual-enable-required'
    plan['config']['properties']['Command Arguments']='/opt/pj3/danawa-collector/collector_launcher.py'
    prepared=client.prepare(gid,plan,expected_version='2.12.0',apply=True)
    pid=prepared['processor_id']
    for _ in range(30):
        entity=request('GET','/processors/'+pid)
        if entity['component'].get('validationStatus')=='VALID':break
        time.sleep(.5)
    else:raise RuntimeError('COLLECTOR_PROCESSOR_NOT_VALID')
    assert entity['component']['state']=='STOPPED','COLLECTOR_PROCESSOR_NOT_STOPPED'
    threads=entity.get('status',{}).get('aggregateSnapshot',{}).get('activeThreadCount',0)
    assert threads==0,'COLLECTOR_ACTIVE_THREADS'
    result=dict(status='CONNECTED_STOPPED',validation='VALID',version='2.12.0',group_id=gid,processor_id=pid,
        launcher_enabled=False,active_threads=threads,configuration=prepared['status'],actual_collection_started=False,
        schedule='1440 min (inactive placeholder; confirm before enabling)',source_commit=control['source_commit'],output=control['output'],
        downstream_handoff='existing capture/drain adapter; a new run has not been registered or ingested')
    p=root/'collector/connection.json';p.write_text(json.dumps(result));p.chmod(0o600)
    print(json.dumps(result))


if __name__=='__main__':main()
