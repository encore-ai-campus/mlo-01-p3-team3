"""Install a separately scoped review identity and verify it without owner writes."""
from ansible.module_utils.basic import AnsibleModule
from pathlib import Path
import json,os,pwd,subprocess


def main():
    m=AnsibleModule(argument_spec=dict(action=dict(choices=['install','verify'],default='verify')))
    base=Path('/var/lib/pj3/product-pipeline/spark');root=base/'production-pending-20261019'
    user=pwd.getpwnam('pj3-spark');directory=base/'mysql-review-owner';directory.mkdir(mode=0o700,exist_ok=True)
    os.chown(directory,user.pw_uid,user.pw_gid);os.chmod(directory,0o700)
    if m.params['action']=='install':
        source=Path('/home/team3ops/.config/pj3-review/owner-transfer.json')
        secret=json.loads(source.read_text());settings=json.loads((base/'mysql-publication-v2-20261018/mysql-publication.json').read_text())
        settings.update(user=secret['user'],password_env='PIPELINE_REVIEW_MYSQL_PASSWORD')
        for name,value in [('owner.private.json',secret),('mysql-review.json',settings)]:
            path=directory/name
            if path.exists() and json.loads(path.read_text())!=value:m.fail_json(msg='REVIEW_PROTECTED_IDENTITY_CONFLICT')
            fd=os.open(path,os.O_WRONLY|os.O_CREAT|os.O_TRUNC,0o600)
            with os.fdopen(fd,'w') as f:json.dump(value,f)
            os.chown(path,user.pw_uid,user.pw_gid);os.chmod(path,0o600)
        source.unlink()
    settings=json.loads((directory/'mysql-review.json').read_text());secret=json.loads((directory/'owner.private.json').read_text())
    env=dict(PATH='/usr/bin:/bin',HOME=str(root),PYTHONPATH=str(root/'source'),LANG='C.UTF-8',
        PIPELINE_REVIEW_MYSQL_PASSWORD=secret['password'],PIPELINE_MYSQL_CA=str(base/'mysql-publication-v2-20261018/ca.pem'))
    command=[str(base/'venvs/128dbd37d879b397-spark/bin/python'),str(root/'source/run_review.py'),'preflight','--config',str(directory/'mysql-review.json')]
    p=subprocess.run(command,cwd=root,env=env,user=user.pw_uid,group=user.pw_gid,extra_groups=[],capture_output=True,text=True,timeout=40)
    values=[json.loads(line) for line in p.stdout.splitlines() if line.startswith('{')]
    if p.returncode or not values:m.fail_json(msg='REVIEW_SERVER_PREFLIGHT_FAILED',error_code=values[-1].get('error_code') if values else 'PROCESS_FAILURE')
    from uuid import uuid4
    output=directory/('inventory-'+uuid4().hex+'.json')
    inventory=command[:-3]+['inventory','--config',str(directory/'mysql-review.json'),'--limit','100','--output',str(output)]
    read=subprocess.run(inventory,cwd=root,env=env,user=user.pw_uid,group=user.pw_gid,extra_groups=[],capture_output=True,text=True,timeout=40)
    rows=[json.loads(line) for line in read.stdout.splitlines() if line.startswith('{')]
    if read.returncode or not rows:m.fail_json(msg='REVIEW_INVENTORY_FAILED',error_code=rows[-1].get('error_code') if rows else 'PROCESS_FAILURE')
    os.chmod(output,0o600)
    m.exit_json(changed=m.params['action']=='install',status='REVIEW_SERVER_PREFLIGHT_VERIFIED',result=values[-1],inventory=rows[-1],approvals_created=0)


if __name__=='__main__':main()
