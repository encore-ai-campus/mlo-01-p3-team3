from ansible.module_utils.basic import AnsibleModule
import pwd,pathlib,os,json,subprocess
CODE=r"""
import sys,json,urllib.request,urllib.parse,ssl,time,importlib.util
from pathlib import Path
spec=importlib.util.spec_from_file_location('product_pipeline.operations.nifi_verified','/var/lib/pj3/product-pipeline/nifi/nifi_client_verified.py');m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
c=json.load(sys.stdin);base='https://192.168.0.125:8443/nifi-api';ctx=ssl.create_default_context(cafile='/etc/pj3-secrets/nifi/ca.pem');opener=urllib.request.build_opener(urllib.request.ProxyHandler({}),urllib.request.HTTPSHandler(context=ctx));result={'status':'FAIL','scope':'isolated NiFi -> restricted SSH -> Master durable queue tick'};client=None;processor=None;group=None;stopped=False
try:
 with opener.open(urllib.request.Request(base+'/access/token',data=urllib.parse.urlencode({'username':c['username'],'password':c['password']}).encode(),method='POST'),timeout=10)as r:token=r.read().decode()
 client=m.Client(base,ca_file='/etc/pj3-secrets/nifi/ca.pem',token=token);version=client.request('GET','/flow/about')['about']['version'];root=client.request('GET','/flow/process-groups/root')['processGroupFlow']['id']
 group=client.request('POST','/process-groups/'+root+'/process-groups',dict(revision=dict(version=0),component=dict(name='pipeline-verify-0594f7b7',position=dict(x=100.0,y=100.0))))['component']['id']
 plan=m.remote_processor_plan('/etc/pj3-clients/pipeline-trigger/ssh.conf','/var/lib/pj3/product-pipeline/nifi',period='1 sec')
 processor=client.prepare(group,plan,expected_version=version,apply=True)['processor_id']
 for i in range(20):
  e=client.request('GET','/processors/'+processor)
  if e['component'].get('validationStatus')=='VALID':break
  time.sleep(.5)
 else:raise ValueError('PROCESSOR_NOT_VALID')
 client.set_running(processor,plan,expected_version=version,running=True)
 task_count=0
 for i in range(30):
  e=client.request('GET','/processors/'+processor);stats=e.get('status',{}).get('aggregateSnapshot',{});task_count=stats.get('taskCount',0)
  if task_count and stats.get('flowFilesOut',0)>0:break
  time.sleep(.5)
 else:raise ValueError('NIFI_TICK_NOT_OBSERVED')
 result.update(status='NIFI_REMOTE_TICK_EXECUTED',version=version,task_count=task_count,flowfiles_out=stats.get('flowFilesOut',0))
except Exception as e:result.update(error_type=type(e).__name__,error_code=str(e)if isinstance(e,ValueError)else 'REMOTE_VERIFICATION_FAILED')
finally:
 if processor and client:
  try:
   e=client.request('GET','/processors/'+processor);client.request('PUT','/processors/'+processor+'/run-status',dict(revision=e['revision'],state='STOPPED',disconnectedNodeAcknowledged=False))
   for i in range(30):
    e=client.request('GET','/processors/'+processor)
    if e['component'].get('state')=='STOPPED'and e.get('status',{}).get('aggregateSnapshot',{}).get('activeThreadCount',0)==0:stopped=True;break
    time.sleep(.5)
  except Exception:pass
 result.update(process_group_id=group,processor_id=processor,processor_stopped=stopped,production_flow_modified=False)
 if not stopped:result['status']='STOP_UNCONFIRMED'
 Path('/var/lib/pj3/product-pipeline/nifi/remote-nifi-verification.json').write_text(json.dumps(result));print(json.dumps(result))
"""
def main():
 m=AnsibleModule(argument_spec={'username':{'no_log':True,'required':True},'password':{'no_log':True,'required':True}});u=pwd.getpwnam('pj3-nifi');py='/var/lib/pj3/product-pipeline/nifi/venvs/ce456c2fd3972154-base/bin/python'
 p=subprocess.run([py,'-c',CODE],input=json.dumps(m.params),user=u.pw_uid,group=u.pw_gid,extra_groups=[],env={'PATH':'/usr/bin:/bin','HOME':u.pw_dir},cwd='/tmp',capture_output=True,text=True,timeout=100)
 result=json.loads(p.stdout)if p.returncode==0 else{'status':'NIFI_VERIFY_PROCESS_FAILED'};m.exit_json(changed=True,result=result)
if __name__=='__main__':main()
