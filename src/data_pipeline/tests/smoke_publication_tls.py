"""Explicit VM TLS verification; synthetic approvals stay in *_validation only.

Run prepare first, have the schema owner apply approval-seed.sql to the separate
validation DB, then run publish. Credentials are read from a 0600 private file.
"""
import argparse
import json
import os
from pathlib import Path
import sys
from uuid import UUID, uuid4

from product_pipeline.contracts import canonical
from product_pipeline.files import atomic_json
from product_pipeline.publication.mysql import MySQL
from product_pipeline.publication.runner import publish, audit_publication
sys.path.insert(0, str(Path(__file__).resolve().parent))
from publication_fixture import fixture


def main():
    import pymysql
    parser=argparse.ArgumentParser()
    parser.add_argument('phase', choices=['prepare','publish'])
    parser.add_argument('--root', type=Path, required=True)
    args=parser.parse_args(); root=args.root.resolve()
    private=root/'bootstrap.private.json'
    if private.stat().st_mode & 0o077:raise RuntimeError('PRIVATE_CREDENTIAL_PERMISSIONS_REQUIRED')
    creds=json.loads(private.read_text())
    identity=creds['identity']; primary=identity['database']; validation=identity['validation_database']
    if validation!=primary+'_validation':raise RuntimeError('SEPARATE_VALIDATION_DATABASE_REQUIRED')
    os.environ['PIPELINE_MYSQL_PASSWORD']=creds['password']
    os.environ['PIPELINE_MYSQL_CA']=str(root/'ca.pem')
    common=dict(contract_version=1,host='192.168.0.131',port=3306,user=identity['user'],
                password_env='PIPELINE_MYSQL_PASSWORD',ca_env='PIPELINE_MYSQL_CA',local_test_only=False)
    settings=dict(common,database=primary,target_id='pj3-products-service-v2')
    test_settings=dict(common,database=validation,target_id='pj3-products-validation-v2')
    for name,value in [('mysql-publication.json',settings),('mysql-validation.json',test_settings)]:
        atomic_json(root/name,value);os.chmod(root/name,0o600)
    live=MySQL(settings);db=MySQL(test_settings)
    try:
        report=dict(scope='document contract with synthetic approval, actual VM TLS',
                    production_approvals_created=False,production_products_published=0,
                    primary=live.preflight(),validation=db.preflight(),checks=[])
        tls=db.one("SHOW SESSION STATUS LIKE 'Ssl_cipher'")
        if not tls['Value']:raise RuntimeError('TLS_CIPHER_REQUIRED')
        report['tls_cipher']=tls['Value'];report['certificate_and_hostname_verified']=True
        assert live.one('SELECT COUNT(*) AS n FROM products')['n']==0
        live.connection.rollback()
        if args.phase=='prepare':
            try:
                plain=pymysql.connect(host=common['host'],port=3306,user=common['user'],password=creds['password'],
                                      database=validation,ssl_disabled=True,connect_timeout=5)
            except pymysql.err.OperationalError as exc:
                if exc.args[0] not in {3159,1045}:raise
                report['plaintext_rejected']=True
            else:
                plain.close();raise AssertionError('plaintext connection accepted')
            for statement in ["CREATE TABLE unauthorized_probe (id INT)",
                              "UPDATE pipeline_publication_guards SET revision=revision WHERE 1=0"]:
                try:db.execute(statement)
                except pymysql.err.OperationalError as exc:
                    if exc.args[0]!=1142:raise
                else:raise AssertionError('publisher owner/DDL privilege accepted')
                finally:db.connection.rollback()
            report['ddl_and_approval_write_rejected']=True
            plan,guard=fixture(root/'synthetic-fixture',db.target)
            atomic_json(root/'validation-plan.json',plan)
            atomic_json(root/'validation-guard.json',guard)
            catalog=plan['products'][0]['group']['tables']['product_catalog'][0]
            def quote(value):return "'"+pymysql.converters.escape_string(str(value))+"'"
            sql='-- SYNTHETIC VALIDATION ONLY; not operating approval\n'
            sql+='USE `'+validation+'`;\nSTART TRANSACTION;\n'
            sql+='INSERT INTO categories (id,code,name,active,recognition_enabled) VALUES ('+','.join([
                quote(UUID(catalog['service_category_id']).hex),quote(catalog['service_category_code']),
                quote('SYNTHETIC VALIDATION ONLY'), '1','1'])+');\n'
            sql+='INSERT INTO pipeline_publication_guards (id,product_key,revision,guard_json) VALUES ('+','.join([
                quote(uuid4().hex),quote(catalog['product_key']),'1',quote(canonical(guard))])+');\nCOMMIT;\n'
            (root/'approval-seed.sql').write_text(sql,encoding='utf-8')
            os.chmod(root/'approval-seed.sql',0o600)
            report['status']='TLS_SCHEMA_VERIFIED_VALIDATION_INPUT_PREPARED'
        else:
            plan=json.loads((root/'validation-plan.json').read_text())
            first=publish(plan,db);again=publish(plan,db)
            assert again['status']=='REUSED' and first['publication_id']==again['publication_id']
            with db.read_snapshot():audit=audit_publication(plan,db)
            assert audit['current_products']==1
            report.update(status='TLS_PUBLICATION_AND_AUDIT_VERIFIED',publication=first,
                          repeat=again,audit=audit,
                          counts={table:db.one('SELECT COUNT(*) AS n FROM `'+table+'`')['n']
                                  for table in ('products','product_images','assets','product_link_observations')})
            db.connection.rollback()
        atomic_json(root/(args.phase+'-result.json'),report)
        print(json.dumps(report,ensure_ascii=False))
    finally:db.close();live.close()


if __name__=='__main__':main()
