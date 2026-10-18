from ansible.module_utils.basic import AnsibleModule
import pathlib,pwd,subprocess,os,json
CODE=r"""
import pathlib,json,sys,os
from product_pipeline.objects import S3Objects
from product_pipeline.transfer import send_snapshot,receive_snapshot
from product_pipeline.snapshot import verify
from product_pipeline.files import atomic_bytes
c=json.load(sys.stdin);root=pathlib.Path(c['root']);role=c['role'];obj=S3Objects('pj3-data','https://192.168.0.141:8443',profile='garage_v241',credentials_file='/etc/pj3-clients/s3/'+role+'.properties');obj.preflight()
if role=='spark':
 first=send_snapshot(root/'snapshot',obj);again=send_snapshot(root/'snapshot',obj);assert first['ready_key']==again['ready_key']and again['objects_written']==0
 result={'status':'REAL_SEND_REUSE_VERIFIED','ready_key':first['ready_key'],'snapshot_id':first['snapshot_id'],'first_objects_written':first['objects_written'],'repeat_objects_written':0}
else:
 root.mkdir(mode=0o700,exist_ok=True);assert root.stat().st_uid==os.geteuid() and (root.stat().st_mode & 0o777)==0o700;ready=root/'ready.json'
 def fetch(key):
  response=obj.client.get_object(Bucket='pj3-data',Key=key)
  try:data=response['Body'].read(8*1024*1024+1)
  finally:response['Body'].close()
  assert len(data)<=8*1024*1024;return data
 atomic_bytes(ready,fetch(c['ready_key']));receive_snapshot(ready,fetch,root/'received',max_total_bytes=10*1024*1024);m=verify(root/'received');assert m['snapshot_id']==c['snapshot_id']and m['product_count']==3 and m['verified_image_relations']==9
 result={'status':'REAL_CROSS_HOST_RECEIVE_VERIFIED','snapshot_id':m['snapshot_id'],'products':m['product_count'],'verified_image_relations':m['verified_image_relations'],'hashes_verified':True,'destination':str(root/'received')}
print(json.dumps(result))
"""
def main():
 m=AnsibleModule(argument_spec={'role':{'required':True},'ready_key':{'default':''},'snapshot_id':{'default':''}});role=m.params['role'];u=pwd.getpwnam('pj3-'+role);suffix='spark'if role=='spark'else'base';py=f'/var/lib/pj3/product-pipeline/{role}/venvs/ce456c2fd3972154-{suffix}/bin/python';root=f'/var/lib/pj3/product-pipeline/{role}/pipeline-acceptance-0594f7b79ed347c5bf9e449cdb5912af';env={'PATH':'/usr/bin:/bin','HOME':u.pw_dir,'AWS_CA_BUNDLE':'/etc/pj3-clients/s3/ca.pem','AWS_DEFAULT_REGION':'us-east-1'};args=dict(m.params,root=root)
 p=subprocess.run([py,'-c',CODE],input=json.dumps(args),user=u.pw_uid,group=u.pw_gid,extra_groups=[],cwd='/tmp',env=env,capture_output=True,text=True,timeout=100)
 result=json.loads(p.stdout)if p.returncode==0 else{'status':'TRANSFER_TEST_FAILED'};m.exit_json(changed=True,result=result)
if __name__=='__main__':main()
