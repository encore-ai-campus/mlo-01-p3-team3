"""Explicit additive owner workflow setup on the MySQL writable primary only."""
from ansible.module_utils.basic import AnsibleModule
import json,os,pwd,secrets,subprocess
from pathlib import Path


def main():
    m=AnsibleModule(argument_spec=dict(action=dict(choices=['check','apply'],default='check'),ddl_path=dict(type='path',required=True)))
    def sql(value):
        p=subprocess.run(['mysql','--protocol=SOCKET','--batch','--skip-column-names'],input=value,text=True,capture_output=True,timeout=30)
        if p.returncode:m.fail_json(msg='REVIEW_MYSQL_ADMIN_SQL_FAILED',sql_error_codes=__import__('re').findall(r'ERROR (\d+)',p.stderr))
        return p.stdout.strip()
    if sql('SELECT @@global.read_only,@@global.super_read_only,@@require_secure_transport')!='0\t0\t1':m.fail_json(msg='WRITABLE_TLS_PRIMARY_REQUIRED')
    owner="'pj3_pipeline_reviewer'@'192.168.0.121'"
    exists=sql("SELECT COUNT(*) FROM mysql.user WHERE User='pj3_pipeline_reviewer' AND Host='192.168.0.121'")=='1'
    table_names=['category_mappings','pipeline_review_events']
    missing=[name for name in table_names if sql("SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA='pj3_products' AND TABLE_NAME='"+name+"'")=='0']
    if m.params['action']=='check':m.exit_json(changed=False,missing_tables=missing,owner_exists=exists)
    ddl='\n'.join(line for line in Path(m.params['ddl_path']).read_text().splitlines() if not line.lstrip().startswith('--'))
    statements=[s.strip() for s in ddl.split(';')]
    # The checked source DDL is an explicit migration, never arbitrary ALTER/DROP.
    for name in missing:
        statement=next((s for s in statements if s.startswith('CREATE TABLE '+name+' (')),None)
        if not statement:m.fail_json(msg='REVIEW_MIGRATION_STATEMENT_MISSING')
        sql('USE pj3_products;'+statement)
    account=pwd.getpwnam('team3ops');parent=Path('/home/team3ops/.config')
    if not parent.exists():parent.mkdir(mode=0o700);os.chown(parent,account.pw_uid,account.pw_gid)
    staging=parent/'pj3-review';staging.mkdir(mode=0o700,exist_ok=True)
    os.chown(staging,account.pw_uid,account.pw_gid);os.chmod(staging,0o700)
    secret=staging/'owner-transfer.json'
    if exists and not secret.exists():m.fail_json(msg='EXISTING_REVIEW_OWNER_SECRET_REQUIRED_NO_PASSWORD_RESET')
    if secret.exists():password=json.loads(secret.read_text())['password']
    else:
        password=secrets.token_urlsafe(48)
        fd=os.open(secret,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600)
        with os.fdopen(fd,'w') as f:json.dump(dict(user='pj3_pipeline_reviewer',password=password),f)
        os.chown(secret,account.pw_uid,account.pw_gid)
    if not exists:sql('CREATE USER '+owner+" IDENTIFIED BY '"+password+"' REQUIRE SSL")
    sql('GRANT SELECT ON pj3_products.* TO '+owner)
    writable=['categories','category_mappings','assets','product_images','pipeline_publication_guards','products','offers','offer_snapshots',
        'pipeline_product_versions','pipeline_publications','pipeline_publication_staging','product_link_observations']
    for table in writable:sql('GRANT INSERT,UPDATE ON pj3_products.'+table+' TO '+owner)
    # The owner review audit is append-only for this account.
    sql('GRANT INSERT ON pj3_products.pipeline_review_events TO '+owner)
    for table in table_names:sql("GRANT SELECT ON pj3_products."+table+" TO 'pj3_pipeline_publisher'@'192.168.0.121'")
    m.exit_json(changed=True,status='REVIEW_SCHEMA_AND_SCOPED_OWNER_PREPARED',created_tables=missing,owner_created=not exists,
        protected_transfer_file=str(secret),approvals_created=0,guards_created=0)


if __name__=='__main__':main()
