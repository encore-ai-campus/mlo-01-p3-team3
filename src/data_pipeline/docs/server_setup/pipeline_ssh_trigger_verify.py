from ansible.module_utils.basic import AnsibleModule
import pathlib,json,os,pwd,subprocess,stat,shutil
ROOT='/var/lib/pj3/product-pipeline/spark/pipeline-acceptance-0594f7b79ed347c5bf9e449cdb5912af'
PY='/var/lib/pj3/product-pipeline/spark/venvs/ce456c2fd3972154-spark/bin/python'
ENV={'PATH':'/usr/bin:/bin','HOME':ROOT,'PYTHONPATH':ROOT+'/fixed-source','AWS_CA_BUNDLE':'/etc/pj3-clients/s3/ca.pem','AWS_DEFAULT_REGION':'us-east-1','PIPELINE_S3_ENDPOINT':'https://192.168.0.141:8443','PIPELINE_S3_CREDENTIALS_FILE':'/etc/pj3-clients/s3/spark.properties'}
LAUNCHER=r"""
import os,pathlib,json,sys
root=pathlib.Path('/var/lib/pj3/product-pipeline/spark/pipeline-acceptance-0594f7b79ed347c5bf9e449cdb5912af')
os.chdir(root)
exec(compile((root/'live_acceptance_resumed.py').read_text().split("sys.argv=['tests.smoke_live_services'")[0],'protected-service-runtime','exec'))
from product_pipeline.operations.cli import tick
from product_pipeline.files import read_json,sha_file
config=root/'ssh-operations-v2.json';receipt=read_json(root/'ssh-audit-v2/manual-smoke.json')
if receipt.get('status')!='MANUAL_TICK_PASSED' or not receipt.get('recovery_verified') or receipt.get('config_sha256')!=sha_file(config):raise SystemExit('MANUAL_RECOVERY_REQUIRED')
result=tick(config);print(json.dumps(result));sys.exit(2 if result.get('status')=='ATTENTION_REQUIRED'else 0)
"""
MANUAL=r"""
import pathlib,json,sys,os
root=pathlib.Path.cwd()
exec(compile((root/'live_acceptance_resumed.py').read_text().split("sys.argv=['tests.smoke_live_services'")[0],'protected-service-runtime','exec'))
from product_pipeline.operations.queue import Queue
from product_pipeline.operations.cli import tick
from product_pipeline.config import Settings
from product_pipeline.files import read_json,sha_file,atomic_json
manifest=read_json(root/'snapshot/snapshot.json');settings=Settings.load(root/'environment.json');conf=root/'ssh-operations-v2.json';q=Queue(root/'ssh-queue-v2','pj3-verify-0594f7b7')
if sys.argv[1]=='interrupt':
 atomic_json(conf,dict(contract_version=1,logical_target='pj3-verify-0594f7b7',mode='live',single_owner_confirmed=True,queue_root=str(root/'ssh-queue-v2'),audit_root=str(root/'ssh-audit-v2'),discover_remote=False))
 q.enqueue('INGEST',dict(snapshot=str(root/'snapshot'),snapshot_id=manifest['snapshot_id'],state=str(root/'ssh-ingest-state'),config=str(root/'environment.json'),config_sha256=sha_file(root/'environment.json'),target_fingerprint=settings.fingerprint))
 def interrupt(_):raise KeyboardInterrupt()
 try:q.tick({'INGEST':interrupt})
 except KeyboardInterrupt:sys.exit(130)
 raise SystemExit('INTERRUPTION_NOT_REPRODUCED')
row=next(q._read(p.stem) for p in (root/'ssh-queue-v2/tasks').glob('*.json'))
assert row['status']=='RUNNING'
q.recover(row['task_id'],row['attempt_token'],'operator://verified-child-exit-130-no-external-handler-started')
result=tick(conf);assert result['recovered_succeeded']==1 and result['succeeded']==1
assert read_json(root/'ssh-audit-v2/manual-smoke.json')['recovery_verified']
print(json.dumps({'status':'ACTUAL_PROCESS_QUEUE_RECOVERY_VERIFIED','interrupted_process_exit':130,'task_id':row['task_id'],'tick':result}))
"""
def directory(path,uid=0,gid=0,mode=0o755):
 p=pathlib.Path(path)
 if p.is_symlink():raise ValueError('SYMLINK_FORBIDDEN')
 if not p.exists():p.mkdir(mode=mode);os.chown(p,uid,gid)
 if p.stat().st_uid!=uid or stat.S_IMODE(p.stat().st_mode)!=mode:raise ValueError('DIRECTORY_DIFFERS')
 return p
def fixed(path,data,mode=0o644):
 p=pathlib.Path(path)
 if p.exists():
  if p.is_symlink()or p.stat().st_uid!=0 or p.read_text()!=data or stat.S_IMODE(p.stat().st_mode)!=mode:raise ValueError('EXISTING_FIXED_FILE_DIFFERS')
 else:
  fd=os.open(p,os.O_WRONLY|os.O_CREAT|os.O_EXCL,mode)
  with os.fdopen(fd,'w')as f:f.write(data)
 return p
def main():
 m=AnsibleModule(argument_spec={'kind':{'required':True},'public_key':{'default':''},'host_key':{'default':''}});kind=m.params['kind']
 try:
  if kind=='key':
   u=pwd.getpwnam('pj3-nifi');directory('/etc/pj3-secrets/pipeline-trigger',u.pw_uid,u.pw_gid,0o700);p=pathlib.Path('/etc/pj3-secrets/pipeline-trigger/id_ed25519')
   if not p.exists():
    r=subprocess.run(['ssh-keygen','-q','-t','ed25519','-N','','-C','pj3-pipeline-nifi-trigger','-f',str(p)],user=u.pw_uid,group=u.pw_gid,extra_groups=[],capture_output=True,timeout=15)
    if r.returncode:raise ValueError('KEY_GENERATION_FAILED')
   if p.is_symlink()or p.stat().st_uid!=u.pw_uid or stat.S_IMODE(p.stat().st_mode)!=0o600:raise ValueError('KEY_PERMISSION_INVALID')
   result={'status':'DEDICATED_KEY_READY','public_key':p.with_suffix('.pub').read_text().strip()}
  elif kind=='master':
   u=pwd.getpwnam('pj3-spark');home=directory('/var/lib/pj3/pipeline-trigger');directory(home/'.ssh');key=m.params['public_key']
   if not key.startswith('ssh-ed25519 ')or '\n'in key:raise ValueError('PUBLIC_KEY_INVALID')
   fixed(home/'.ssh/authorized_keys','restrict,from="192.168.0.125",command="/usr/local/libexec/pj3-pipeline-ssh-gate" '+key+'\n')
   directory('/usr/local/libexec')
   fixed('/usr/local/libexec/pj3-pipeline-ssh-gate','#!/bin/sh\n[ "$SSH_ORIGINAL_COMMAND" = "tick" ] || { echo REMOTE_COMMAND_DENIED; exit 64; }\nexec /usr/bin/sudo.ws -n -u pj3-spark -- /usr/local/libexec/pj3-pipeline-verify-tick\n',0o755)
   directory('/opt/pj3/product-pipeline/launchers');fixed('/opt/pj3/product-pipeline/launchers/verify_tick.py',LAUNCHER)
   envargs=' '.join(k+'='+v for k,v in ENV.items())
   fixed('/usr/local/libexec/pj3-pipeline-verify-tick','#!/bin/sh\nexec /usr/bin/env -i '+envargs+' '+PY+' /opt/pj3/product-pipeline/launchers/verify_tick.py\n',0o755)
   sudoers=fixed('/etc/sudoers.d/pj3-pipeline-trigger-verify','pj3-pipeline-trigger ALL=(pj3-spark) NOPASSWD: /usr/local/libexec/pj3-pipeline-verify-tick\n',0o440)
   check=subprocess.run([shutil.which('visudo.ws')or shutil.which('visudo'),'-cf',str(sudoers)],capture_output=True,timeout=10)
   if check.returncode:raise ValueError('SUDOERS_INVALID')
   p=pathlib.Path(ROOT)/'manual_queue_recovery.py';p.write_text(MANUAL);p.chmod(0o600);os.chown(p,u.pw_uid,u.pw_gid)
   result={'status':'SSH_GATE_CONFIGURED'}
   env=dict(ENV,LANG='C.UTF-8')
   for phase in ['interrupt','recover']:
    r=subprocess.run([PY,str(p),phase],cwd=ROOT,env=env,user=u.pw_uid,group=u.pw_gid,extra_groups=[],capture_output=True,text=True,timeout=100)
    if (phase=='interrupt'and r.returncode!=130)or(phase=='recover'and r.returncode!=0):
     log=pathlib.Path(ROOT)/('ssh-'+phase+'-private.log');log.write_text(r.stdout+'\n'+r.stderr);log.chmod(0o600);os.chown(log,u.pw_uid,u.pw_gid);result={'status':'MANUAL_RECOVERY_TEST_FAILED','phase':phase};break
    if phase=='recover':result['manual_recovery']=json.loads(r.stdout)
   result['host_key']=pathlib.Path('/etc/ssh/ssh_host_ed25519_key.pub').read_text().strip()
  else:
   u=pwd.getpwnam('pj3-nifi');d=directory('/etc/pj3-clients/pipeline-trigger');key=m.params['host_key']
   if not key.startswith('ssh-ed25519 ')or '\n'in key:raise ValueError('HOST_KEY_INVALID')
   fixed(d/'known_hosts','192.168.0.121 '+key+'\n')
   config='Host pj3-pipeline-tick\n  HostName 192.168.0.121\n  User pj3-pipeline-trigger\n  IdentityFile /etc/pj3-secrets/pipeline-trigger/id_ed25519\n  IdentitiesOnly yes\n  BatchMode yes\n  RequestTTY no\n  ForwardAgent no\n  ClearAllForwardings yes\n  StrictHostKeyChecking yes\n  UserKnownHostsFile /etc/pj3-clients/pipeline-trigger/known_hosts\n  ConnectTimeout 10\n  LogLevel ERROR\n'
   fixed(d/'ssh.conf',config);result={'status':'FAIL'};outputs=[]
   for cmd in ['id','tick;id','tick']:
    r=subprocess.run(['ssh','-F',str(d/'ssh.conf'),'pj3-pipeline-tick',cmd],cwd='/tmp',env={'PATH':'/usr/bin:/bin','HOME':u.pw_dir},user=u.pw_uid,group=u.pw_gid,extra_groups=[],capture_output=True,text=True,timeout=60)
    if cmd!='tick':outputs.append({'command':cmd,'blocked':r.returncode==64 and'REMOTE_COMMAND_DENIED'in r.stdout})
    elif r.returncode==0:result={'status':'DEDICATED_REMOTE_TICK_VERIFIED','tick':json.loads(r.stdout),'negative_command_checks':outputs,'host_key_pinned':True,'port_forwarding_disabled':True}
    else:result['ssh_exit_code']=r.returncode
  m.exit_json(changed=True,result=result)
 except Exception as e:m.fail_json(msg=str(e)if isinstance(e,ValueError)else type(e).__name__)
if __name__=='__main__':main()
