"""Read-only Delta commit metadata and business MySQL counts; no Spark Driver."""
from ansible.module_utils.basic import AnsibleModule
import json,os,pwd,subprocess
from pathlib import Path

SCRIPT=r'''
import sys,json,os
from pathlib import Path
root=Path('/var/lib/pj3/product-pipeline/spark/production-pending-20261019')
sys.path.insert(0,str(root));sys.path.insert(0,str(root/'source'))
from production_work import setup
from product_pipeline.s3_credentials import service_credentials
from product_pipeline.publication.mysql import MySQL
from botocore.config import Config
import boto3
result={}
try:
    config,_=setup()
    client=boto3.client('s3',endpoint_url='https://192.168.0.141:8443',region_name='us-east-1',verify='/etc/pj3-clients/s3/ca.pem',
        config=Config(signature_version='s3v4',s3={'addressing_style':'path'}),**service_credentials('/etc/pj3-clients/s3/spark.properties'))
    targets=[] if os.environ.get('PIPELINE_READBACK_MYSQL_ONLY')=='1' else [('bronze','products'),('bronze','events'),('silver','product_observations'),('silver','pipeline_evaluations'),('gold','product_catalog'),('gold','pipeline_evaluations')]
    for stage,table in targets:
        prefix='lake/pj3-danawa/'+stage+'/'+table+'/_delta_log/'
        files=[]
        for page in client.get_paginator('list_objects_v2').paginate(Bucket='pj3-data',Prefix=prefix):
            files.extend(o for o in page.get('Contents',[]) if o['Key'].rsplit('/',1)[-1].removesuffix('.json').isdigit() and o['Key'].endswith('.json'))
        files.sort(key=lambda o:o['Key']);commits=[];total_rows=0;has_remove=False
        selected=files if stage=='bronze' and table=='products' else files[-3:]
        for obj in selected:
            data=client.get_object(Bucket='pj3-data',Key=obj['Key'])['Body'].read()
            added=0;metrics={}
            for line in data.splitlines():
                row=json.loads(line)
                if 'add' in row:added+=json.loads(row['add'].get('stats','{}')).get('numRecords',0)
                if 'remove' in row:has_remove=True
                if 'commitInfo' in row:metrics=row['commitInfo'].get('operationMetrics',{})
            commits.append(dict(version=int(obj['Key'].rsplit('/',1)[-1][:-5]),added_rows=added,modified_at=obj['LastModified'].isoformat(),metrics=metrics))
            total_rows+=added
        result[stage+'/'+table]=dict(commit_files=len(files),latest=commits[-3:])
        if stage=='bronze' and table=='products':result[stage+'/'+table].update(stored_rows=total_rows if not has_remove else None)
    db=MySQL(config)
    try:
        with db.read_snapshot():
            counts=db.one("SELECT COUNT(*) AS products,SUM(status='PENDING_REVIEW') AS pending,SUM(status='ACTIVE') AS active FROM products WHERE source_code='danawa'")
            result['mysql']={k:int(v or 0) for k,v in counts.items()}
            result['mysql']['images']=db.one("SELECT COUNT(*) AS n FROM product_images WHERE source_code='danawa'")['n']
            result['mysql']['latest_publication']=db.one("SELECT MAX(completed_at) AS at FROM pipeline_publications WHERE status='COMPLETED'")['at']
            if result['mysql']['latest_publication']:result['mysql']['latest_publication']=result['mysql']['latest_publication'].isoformat()
    finally:db.close()
    print(json.dumps(dict(status='READBACK_VERIFIED',**result)))
except Exception as exc:
    print(json.dumps(dict(status='FAILED',error_code=getattr(exc,'code',type(exc).__name__))));sys.exit(2)
'''


def main():
    m=AnsibleModule(argument_spec=dict(mysql_only=dict(type='bool',default=False)));user=pwd.getpwnam('pj3-spark')
    root=Path('/var/lib/pj3/product-pipeline/spark/production-pending-20261019')
    p=subprocess.run(['/var/lib/pj3/product-pipeline/spark/venvs/128dbd37d879b397-spark/bin/python','-c',SCRIPT],
        cwd=root,user=user.pw_uid,group=user.pw_gid,extra_groups=[],capture_output=True,text=True,timeout=60,
        env=dict(PATH='/usr/bin:/bin',HOME=str(root),LANG='C.UTF-8',PIPELINE_READBACK_MYSQL_ONLY='1' if m.params['mysql_only'] else '0'))
    values=[json.loads(line) for line in p.stdout.splitlines() if line.startswith('{')]
    if p.returncode or not values:m.fail_json(msg='READBACK_FAILED',result=values[-1] if values else {})
    m.exit_json(changed=False,result=values[-1])


if __name__=='__main__':main()
