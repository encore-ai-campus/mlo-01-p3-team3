"""Check and optionally repair the precise publisher table privileges."""
from ansible.module_utils.basic import AnsibleModule
import subprocess

WRITABLE={'products','offers','offer_snapshots','assets','product_images','pipeline_product_versions',
          'pipeline_publications','pipeline_publication_staging','product_link_observations','pipeline_runs'}
READONLY={'categories','pipeline_publication_guards','product_embeddings','search_index_versions'}


def main():
    module=AnsibleModule(argument_spec=dict(apply=dict(type='bool',default=False)))
    def sql(statement):
        p=subprocess.run(['mysql','--protocol=SOCKET','--batch','--skip-column-names'],input=statement,
            text=True,capture_output=True,timeout=30)
        if p.returncode:module.fail_json(msg='PRIVILEGE_AUDIT_QUERY_FAILED')
        return p.stdout.strip()
    account="'pj3_pipeline_publisher'@'192.168.0.121'"
    rows=sql("SELECT TABLE_NAME,PRIVILEGE_TYPE FROM information_schema.TABLE_PRIVILEGES "
        "WHERE TABLE_SCHEMA='pj3_products' AND GRANTEE=\""+account+"\"")
    actual={}
    for line in rows.splitlines():
        table,privilege=line.split('\t');actual.setdefault(table,set()).add(privilege)
    missing={t:sorted(({'SELECT','INSERT','UPDATE'} if t in WRITABLE else {'SELECT'})-actual.get(t,set()))
        for t in sorted(WRITABLE|READONLY)}
    missing={t:p for t,p in missing.items() if p}
    readonly=sql('SELECT @@global.read_only,@@global.super_read_only')!='0\t0'
    repaired={}
    if module.params['apply']:
        if readonly:module.fail_json(msg='PRIVILEGE_REPAIR_REQUIRES_PRIMARY')
        for table,privileges in missing.items():
            sql('GRANT '+','.join(privileges)+' ON pj3_products.`'+table+'` TO '+account)
            repaired[table]=privileges
        missing={}
    tls=sql("SELECT ssl_type FROM mysql.user WHERE User='pj3_pipeline_publisher' AND Host='192.168.0.121'")
    if tls not in {'ANY','X509','SPECIFIED'}:module.fail_json(msg='PUBLISHER_TLS_REQUIREMENT_MISSING')
    module.exit_json(changed=bool(repaired),status='VERIFIED' if not missing else 'MISSING_PRIVILEGES',
        hostname=sql('SELECT @@hostname'),readonly=readonly,missing=missing,repaired=repaired,tls_required=True,
        owner_writes_granted=False,grant_option_granted=False,
        business_database='pj3_products',
        product_counts=sql("SELECT source_code,status,COUNT(*),SUM(recommendation_ready) FROM pj3_products.products GROUP BY source_code,status"),
        image_counts=sql('SELECT COUNT(*),COALESCE(SUM(active),0) FROM pj3_products.product_images'),
        registered_runs=sql("SELECT type,status,COUNT(*) FROM pj3_products.pipeline_runs WHERE type IN ('NORMALIZATION','GOLD_BUILD') GROUP BY type,status"),
        sample=sql("SELECT source_product_id,name,status FROM pj3_products.products WHERE source_code IN ('danawa','DANAWA') ORDER BY source_product_id LIMIT 3"))


if __name__=='__main__':main()
