"""Actual isolated MySQL integration. Never uses the existing Windows service.

Pass a mysqld executable; initializes a fresh datadir, binds loopback on a free
non-default port, runs document reference DDL and tears down only its own child.
"""
import argparse
import copy
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import time

from product_pipeline.contracts import canonical,digest
from product_pipeline.errors import Conflict,PipelineError
from product_pipeline.files import atomic_json
from product_pipeline.publication.mysql import MySQL
from product_pipeline.publication.schema import reference_ddl
from product_pipeline.publication.runner import publish
sys.path.insert(0, str(Path(__file__).resolve().parent))
from publication_fixture import fixture


def main():
    p=argparse.ArgumentParser();p.add_argument('--mysqld',required=True);p.add_argument('--output',type=Path,required=True);args=p.parse_args()
    import pymysql
    args.output.mkdir(parents=True,exist_ok=False);data=args.output/'data';data.mkdir()
    report=dict(actual_mysql=True,existing_service_touched=False,vm_connections=0,checks=[])
    initialized=subprocess.run([args.mysqld,'--no-defaults','--initialize-insecure','--datadir='+str(data.resolve())],
        capture_output=True,timeout=90,creationflags=subprocess.CREATE_NO_WINDOW if os.name=='nt' else 0)
    if initialized.returncode:raise RuntimeError(initialized.stderr.decode(errors='replace'))
    with socket.socket() as sock:sock.bind(('127.0.0.1',0));port=sock.getsockname()[1]
    log=(args.output/'mysqld.log').open('wb')
    child=subprocess.Popen([args.mysqld,'--no-defaults','--datadir='+str(data.resolve()),'--bind-address=127.0.0.1',
        '--port='+str(port),'--mysqlx=0','--skip-log-bin','--innodb-buffer-pool-size=64M','--console'],stdout=log,stderr=log,
        creationflags=subprocess.CREATE_NO_WINDOW if os.name=='nt' else 0)
    db=admin=None
    try:
        for _ in range(100):
            if child.poll() is not None:raise RuntimeError('isolated mysqld stopped; see its log')
            try:admin=pymysql.connect(host='127.0.0.1',port=port,user='root',password='',autocommit=True);break
            except pymysql.err.OperationalError:time.sleep(.2)
        if admin is None:raise RuntimeError('isolated mysql startup timed out')
        with admin.cursor() as cursor:
            cursor.execute('CREATE DATABASE pipeline_publication_test CHARACTER SET utf8mb4 COLLATE utf8mb4_bin')
            cursor.execute('USE pipeline_publication_test')
            for statement in reference_ddl().split(';'):
                if statement.strip():cursor.execute(statement)
        config=dict(contract_version=1,host='127.0.0.1',port=port,user='root',database='pipeline_publication_test',
                    target_id='isolated-publication',local_test_only=True)
        db=MySQL(config,local_test=True);report['preflight']=db.preflight()
        from datetime import datetime,timezone
        db.execute('SET timestamp = %s',(int(datetime(2026,10,18,tzinfo=timezone.utc).timestamp()),));db.connection.commit()
        plan,guard=fixture(args.output/'fixture',db.target)
        catalog=plan['products'][0]['group']['tables']['product_catalog'][0];key=catalog['product_key']
        from uuid import UUID,uuid4
        with db.transaction():
            db.insert('categories',dict(id=UUID(catalog['service_category_id']).hex,code=catalog['service_category_code'],name='fixture category',active=True,recognition_enabled=True))
            db.insert('pipeline_publication_guards',dict(id=uuid4().hex,product_key=key,revision=1,guard_json=canonical(guard)))
        def broken(stage):
            if stage=='PRODUCT_WRITTEN':raise RuntimeError('injected partial publication failure')
        try:publish(plan,db,hook=broken);raise AssertionError('failure not injected')
        except RuntimeError:pass
        assert db.one('SELECT COUNT(*) AS n FROM products')['n']==0
        db.connection.rollback();report['checks'].append('partial_write_rolls_back_all_service_rows_staging_survives')
        result=publish(plan,db);again=publish(plan,db)
        assert result['publication_id']==again['publication_id'] and again['status']=='REUSED'
        product=db.get('products',dict(product_key=key),locking=False);original_id=product['id'];db.connection.rollback()
        assert len(original_id)==32
        image=db.one('SELECT * FROM product_images LIMIT 1');db.connection.rollback()
        assert image and not image['active']
        from product_pipeline.publication.runner import audit_publication
        from product_pipeline.publication.mysql import schema_difference
        from product_pipeline.files import parse
        with db.read_snapshot():
            projection=audit_publication(plan,db)
        assert projection['current_products']==1 and projection['link_observations']>0
        assert parse(product['attributes_json'])==parse(catalog['attributes_json'])
        assert parse(product['field_status_json'])==parse(catalog['field_status_json'])
        assert product['representative_image_id'] in {r['id'] for r in db.all('SELECT id FROM product_images')}
        difference=schema_difference(db)
        assert not difference['missing_tables'] and not difference['column_differences'] and not difference['constraint_differences']
        assert not difference['ddl_applied'] and not difference['service_rows_changed']
        report['checks'].append('v2_projection_and_read_only_snapshot_audit_match_pinned_gold')
        try:
            with db.transaction():
                db.update('products',dict(id=product['id']),dict(attributes_json='[]'))
                audit_publication(plan,db)
            raise AssertionError('missing projected attributes accepted')
        except Conflict as exc:assert exc.code=='PUBLICATION_SERVICE_ATTRIBUTE_CHANGED'
        report['checks'].append('missing_service_attributes_detected_without_committing_tamper')
        report['checks'].append('same_plan_keeps_product_image_asset_publication_uuids')
        report['checks'].append('new_image_links_inactive_until_owner_activation')
        from product_pipeline.operations.flow_audit import sql_plan_audit
        with db.transaction():
            audited=sql_plan_audit(plan,db)
        assert audited['products']==1 and audited['images']==2
        try:
            with db.transaction():
                db.update('product_images',dict(id=image['id']),dict(source_url='https://example.invalid/wrong-product'))
                sql_plan_audit(plan,db)
            raise AssertionError('same row counts with wrong image link accepted')
        except Conflict as exc:
            assert exc.code=='FLOW_SQL_IMAGE_LINK_CHANGED'
        with db.transaction():sql_plan_audit(plan,db)
        report['checks'].append('read_only_reconciliation_detects_wrong_product_image_link_and_rollback_preserves_evidence')
        # Owner state changes after staging must prevent a new plan commit.
        changed=copy.deepcopy(plan);changed['export_hash']='b'*64;changed['plan_id']=digest({k:v for k,v in changed.items() if k!='plan_id'})
        wrong={**guard,'product_active':False}
        with db.transaction():db.update('pipeline_publication_guards',dict(product_key=key),dict(revision=2,guard_json=canonical(wrong)))
        try:publish(changed,db);raise AssertionError('owner conflict not blocked')
        except Conflict:pass
        report['checks'].append('owner_guard_changed_since_plan_blocks_commit')
        with db.transaction():db.update('pipeline_publication_guards',dict(product_key=key),dict(revision=3,guard_json=canonical(guard)))
        # A commit acknowledgement lost AFTER server commit reuses the same receipt on reconnect.
        real_commit=db.connection.commit;armed=[False]
        def lost_commit():
            real_commit()
            if armed[0]:armed[0]=False;raise pymysql.err.OperationalError(2013,'fixture commit response lost')
        db.connection.commit=lost_commit
        def arm(stage):
            if stage=='BEFORE_COMMIT':armed[0]=True
        try:publish(changed,db,hook=arm);raise AssertionError('commit response loss not injected')
        except pymysql.err.OperationalError:pass
        db.connection.commit=real_commit
        recovered=publish(changed,db);assert recovered['status']=='REUSED'
        assert db.get('products',dict(product_key=key),locking=False)['id']==original_id;db.connection.rollback()
        report['checks'].append('unknown_commit_reconciles_existing_receipt_no_new_ids')
        # The next confirmed price/name retains service UUIDs; an older snapshot
        # remains historical and cannot overwrite the current value.
        newer,new_guard=fixture(args.output/'newer',db.target,name='new verified name',stamp='2026-10-17T00:00:00.000Z',
                                as_of='2026-10-18T00:00:00.000Z',price_amount='2000')
        with db.transaction():db.update('pipeline_publication_guards',dict(product_key=key),dict(revision=4,guard_json=canonical(new_guard)))
        publish(newer,db)
        latest=db.get('products',dict(product_key=key),locking=False);offer=db.one('SELECT * FROM offers');db.connection.rollback()
        assert latest['name']=='new verified name' and latest['id']==original_id and offer['price_amount']==2000
        assert latest['price_krw']==2000
        offer_id=offer['id']
        older,old_guard=fixture(args.output/'older',db.target,name='older verified name',stamp='2026-10-16T00:00:00.000Z',
                                as_of='2026-10-17T00:00:00.000Z',price_amount='1000')
        with db.transaction():db.update('pipeline_publication_guards',dict(product_key=key),dict(revision=5,guard_json=canonical(old_guard)))
        publish(older,db)
        assert db.get('products',dict(product_key=key),locking=False)['name']=='new verified name'
        offer=db.one('SELECT * FROM offers');assert offer['price_amount']==2000 and offer['id']==offer_id
        assert db.one('SELECT COUNT(*) AS n FROM offer_snapshots')['n']==2;db.connection.rollback()
        report['checks'].append('older_price_name_cannot_overwrite_newer_confirmed_values')
        report['checks'].append('offer_and_history_conditions_are_preserved_with_existing_parent_uuid')
        with db.transaction():
            assert sql_plan_audit(newer,db)['offers']==1
            assert sql_plan_audit(older,db)['history']==1
        snapshot=db.one('SELECT * FROM offer_snapshots WHERE price_amount = 2000');db.connection.rollback()
        try:
            with db.transaction():
                db.update('offer_snapshots',dict(id=snapshot['id']),dict(pricing_terms_json=canonical({'wrong':'terms'})))
                sql_plan_audit(newer,db)
            raise AssertionError('changed immutable pricing terms accepted')
        except Conflict as exc:assert exc.code=='FLOW_SQL_PRICE_HISTORY_CHANGED'
        report['checks'].append('reconciliation_checks_offer_parent_and_immutable_history_terms_without_overwriting_newer_values')
        with db.read_snapshot():
            assert audit_publication(newer,db)['current_products']==1
            assert audit_publication(older,db)['superseded_products']==1
        report['checks'].append('v2_price_and_lineage_audit_support_current_and_superseded_publications')
        counts={t:db.one('SELECT COUNT(*) AS n FROM `'+t+'`')['n'] for t in
                ('products','assets','product_images','offers','offer_snapshots','pipeline_publications','pipeline_publication_staging')}
        db.connection.rollback();report.update(passed=True,counts=counts,product_id=original_id,port=port)
        atomic_json(args.output/'report.json',report);print(json.dumps(report,ensure_ascii=False))
    finally:
        if db:db.close()
        if admin:
            try:
                with admin.cursor() as cursor:cursor.execute('SHUTDOWN')
            except Exception:pass
            admin.close()
        try:child.wait(timeout=20)
        except subprocess.TimeoutExpired:child.terminate();child.wait(timeout=10)
        log.close()


if __name__=='__main__':main()
