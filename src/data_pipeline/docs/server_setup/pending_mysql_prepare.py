"""Scoped additive preparation; never creates approval/guard/mapping rows."""
from ansible.module_utils.basic import AnsibleModule
import subprocess
DDL='''CREATE TABLE pipeline_runs (id CHAR(32) NOT NULL PRIMARY KEY,type VARCHAR(32) NOT NULL,input_range_json JSON NOT NULL,code_version VARCHAR(191) NOT NULL,status VARCHAR(32) NOT NULL,started_at DATETIME(3) NULL,finished_at DATETIME(3) NULL,created_at DATETIME(3) NOT NULL,updated_at DATETIME(3) NOT NULL) ENGINE=InnoDB'''
def main():
    m=AnsibleModule(argument_spec={})
    def sql(value):
        p=subprocess.run(['mysql','--protocol=SOCKET','--batch','--skip-column-names'],input=value,text=True,capture_output=True,timeout=30)
        if p.returncode:m.fail_json(msg='PENDING_MYSQL_PREPARATION_FAILED',error_code='MYSQL_ADMIN_QUERY_FAILED')
        return p.stdout.strip()
    assert sql('SELECT @@global.read_only,@@global.super_read_only,@@require_secure_transport')=='0\t0\t1'
    assert sql("SELECT COUNT(*) FROM mysql.user WHERE User='pj3_pipeline_publisher' AND Host='192.168.0.121'")=='1'
    created=False
    if sql("SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA='pj3_products' AND TABLE_NAME='pipeline_runs'")=='0':
        sql('USE pj3_products;'+DDL);created=True
    value=sql("SELECT id,active,recognition_enabled FROM pj3_products.categories WHERE code='PIPELINE_UNMAPPED'")
    if not value:
        sql("INSERT INTO pj3_products.categories (id,code,name,active,recognition_enabled) VALUES ('25701b7522f95fea9148d7a4c006a440','PIPELINE_UNMAPPED','미매핑 · 검수 대기',0,0)")
    elif value!='25701b7522f95fea9148d7a4c006a440\t0\t0':m.fail_json(msg='PENDING_CATEGORY_IDENTITY_CONFLICT')
    sql("GRANT SELECT,INSERT,UPDATE ON pj3_products.pipeline_runs TO 'pj3_pipeline_publisher'@'192.168.0.121'")
    m.exit_json(changed=True,status='PENDING_INTAKE_PREPARED',registration_table_created=created,
        pending_category_id='25701b75-22f9-5fea-9148-d7a4c006a440',category_active=False,approvals_created=0,guards_created=0,mappings_created=0)
if __name__=='__main__':main()
