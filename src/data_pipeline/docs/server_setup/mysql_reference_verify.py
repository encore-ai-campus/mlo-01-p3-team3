"""Document-contract publication verification on a dedicated VM schema only.
Credentials arrive on stdin; never run against a service schema.
"""
import json,sys,copy
from pathlib import Path
from types import SimpleNamespace
import pymysql
from product_pipeline.contracts import canonical,digest
from product_pipeline.errors import Conflict
from product_pipeline.publication.mysql import MySQL
from product_pipeline.publication.runner import publish
from publication_fixture import fixture
c=json.load(sys.stdin)
assert c['database'].startswith('pipeline_verify_') and c['user'].startswith('pipeline_verify_')
connection=pymysql.connect(unix_socket='/var/run/mysqld/mysqld.sock',user=c['user'],password=c['password'],database=c['database'],charset='utf8mb4',autocommit=False,cursorclass=pymysql.cursors.DictCursor)
config={'contract_version':1,'target_id':'isolated-vm-reference','database':c['database']}
db=MySQL(config,connection=connection)
db.execute("SET time_zone = '+00:00'");db.execute('SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED');db.connection.commit()
args=SimpleNamespace(output=Path.cwd()/'publication-fixtures');args.output.mkdir()
port=3306
report={'scope':'VM MySQL reference DDL and synthetic owner fixtures over local Unix socket','actual_service_ddl':False,'service_data_modified':False,'checks':[],'preflight':db.preflight()}

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
counts={t:db.one('SELECT COUNT(*) AS n FROM `'+t+'`')['n'] for t in
        ('products','assets','product_images','offers','offer_snapshots','pipeline_publications','pipeline_publication_staging')}
db.connection.rollback();report.update(passed=True,counts=counts,product_id=original_id,port=port)

from product_pipeline.files import atomic_json
atomic_json(Path.cwd()/'mysql-reference-result.json',report)
db.close()
print(json.dumps(report))
