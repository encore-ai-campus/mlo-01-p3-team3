"""Real SQL workflow with artificial owner decisions in an isolated database only."""
from pathlib import Path
from uuid import uuid4

from product_pipeline import review
from product_pipeline.contracts import canonical,digest
from product_pipeline.files import parse
from product_pipeline.publication.mysql import MySQL
from product_pipeline.publication.schema import reference_ddl
from product_pipeline.publication.plan import make_plan
from product_pipeline.publication.runner import publish,audit_publication
from product_pipeline.errors import Conflict


def exercise(admin,config,root):
    from publication_fixture import fixture
    from test_publication_pending import CATEGORY
    from test_silver import RUN,sample
    from test_silver_runtime import Sink,States
    from product_pipeline.silver.runner import process as normalize
    from product_pipeline.gold.runner import process as build
    from test_gold import MemoryGoldSink,VERSIONS
    root=Path(root);root.mkdir()
    schema='pipeline_review_isolated'
    with admin.cursor() as cursor:
        cursor.execute('CREATE DATABASE '+schema+' CHARACTER SET utf8mb4 COLLATE utf8mb4_bin')
        cursor.execute('USE '+schema)
        ddl=reference_ddl()+(Path(__file__).resolve().parents[1]/'sql/review_workflow.sql').read_text(encoding='utf-8')
        ddl='\n'.join(line for line in ddl.splitlines() if not line.lstrip().startswith('--'))
        for statement in ddl.split(';'):
            if statement.strip():cursor.execute(statement)
    db=MySQL(dict(config,database=schema,target_id='isolated-review'),local_test=True)
    try:
        at='2026-10-19T00:00:00.000Z'
        base,_=fixture(root/'initial',db.target);group=base['products'][0]['group'];key=base['products'][0]['product_key']
        with db.transaction():db.insert('categories',dict(id=CATEGORY.replace('-',''),code='PIPELINE_UNMAPPED',name='fixture pending',active=False,recognition_enabled=False))
        initial=make_plan([group],base['gold_versions'],export_hash=base['export_hash'],as_of=base['as_of'],guard_hashes={},
            target=db.target,price_history_mode='UNRESOLVED',price_max_age_seconds=None,pending_category_id=CATEGORY)
        publish(initial,db)
        product=db.get('products',dict(product_key=key),locking=False)
        images=db.all('SELECT * FROM product_images ORDER BY id');db.connection.rollback()
        original_id=product['id'];original_images={i['id'] for i in images}
        category=str(uuid4())
        requests=[dict(kind='CATEGORY',payload=dict(category_id=category,code='FIXTURE_CHAIR',name='fixture chair',active=True,recognition_enabled=True)),
            dict(kind='MAPPING',payload=dict(source_code='danawa',source_category_codes=['1','2'],service_category_id=category,mapping_version='fixture-v1',active=True,excluded=False)),
            dict(kind='PRODUCT',payload=dict(product_key=key,active=True,supported=True,service_category_id=category,
                applicable_attributes=['display_name'],raw_document_ids=[r['raw_document_id'] for r in parse(product['source_refs_json'])]))]
        for image in images:
            asset=db.get('assets',dict(id=image['asset_id']),locking=False)
            requests.append(dict(kind='IMAGE',payload=dict(image_id=image['id'],source_version=image['source_version'],checksum=asset['checksum'],
                scopes=['DISPLAY','RECOMMENDATION'],permission_evidence_ref='fixture://not-real-permission',
                asset_policy=dict(decision='APPROVED',allowed_scopes=['DISPLAY','RECOMMENDATION'],denied_scopes=[],evidence_ref='fixture://not-real-permission'))))
        db.connection.rollback()
        requests.append(dict(kind='POLICY',payload=dict(price_history_mode='SNAPSHOT_V1',price_max_age_seconds=86400,representative_price_kinds=[])))
        bundle=review.draft(db,requests)
        bundle.update(status='REVIEWED',actor_ref='fixture://owner',criteria_ref='fixture://criteria',evidence_ref='fixture://decision',
            reviewed_at='2026-10-18T00:00:00.000Z',expires_at='2026-11-01T00:00:00.000Z')
        for item in bundle['items']:item['decision']='APPROVED'
        bundle=review.seal(bundle)
        assert review.apply(db,bundle,at=at)['created']==len(requests)
        assert review.apply(db,bundle,at=at)['reused']==len(requests)
        with db.read_snapshot():first=review.export_inputs(db,[],as_of=at)
        folder=root/'normalized';folder.mkdir();doc,state=sample(folder);sink=Sink()
        normalize(doc,States(state),sink,first['silver'],processing_run_id=RUN,as_of=at,bronze_version=1,bronze_path='file:///fixture-bronze')
        silver=dict(receipt=next(iter(sink.data['pipeline_evaluations'].values())),tables={t:list(rows.values()) for t,rows in sink.data.items() if t!='pipeline_evaluations'})
        with db.read_snapshot():exports=review.export_inputs(db,[silver],as_of=at)
        assert exports['gold']['product_states']['rows'] and not exports['blocked']
        gs=MemoryGoldSink();build([silver],exports['gold'],VERSIONS,gs,processing_run_id=RUN,as_of=at)
        gold=dict(receipt=next(iter(gs.data['pipeline_evaluations'].values())),tables={t:list(rows.values()) for t,rows in gs.data.items() if t!='pipeline_evaluations'})
        versions={t:gs.version(t) for t in gs.data}
        assert gold['tables']['product_catalog'][0]['catalog_ready']
        guard=review.approve_guard(db,gold,versions,price_history_mode='SNAPSHOT_V1',as_of=at,owner_inputs=exports['gold'])
        plan=make_plan([gold],versions,export_hash='f'*64,as_of=at,guard_hashes={key:guard['guard_hash']},target=db.target,
            price_history_mode='SNAPSHOT_V1',price_max_age_seconds=86400,activate_pending=True)
        def fail(stage):
            if stage=='PRODUCT_WRITTEN':raise RuntimeError('fixture rollback')
        try:publish(plan,db,hook=fail);raise AssertionError('rollback not exercised')
        except RuntimeError:pass
        assert db.get('products',dict(product_key=key),locking=False)['status']=='PENDING_REVIEW';db.connection.rollback()
        publish(plan,db);assert publish(plan,db)['status']=='REUSED'
        with db.read_snapshot():audit=audit_publication(plan,db)
        current=db.get('products',dict(product_key=key),locking=False)
        assert current['id']==original_id and current['status']=='ACTIVE' and not current['recommendation_ready']
        links=db.all('SELECT id,active FROM product_images');db.connection.rollback()
        assert {i['id'] for i in links}==original_images and all(i['active'] for i in links)
        # A later owner revision invalidates the old guard; it never silently reuses approval.
        decision=review.draft(db,[requests[-1]])
        decision.update({k:bundle[k] for k in ('status','actor_ref','criteria_ref','evidence_ref','reviewed_at','expires_at')})
        decision['items'][0]['decision']='APPROVED';review.apply(db,review.seal(decision),at=at)
        try:review.approve_guard(db,gold,versions,price_history_mode='SNAPSHOT_V1',as_of=at,owner_inputs=exports['gold']);raise AssertionError('stale owner input accepted')
        except Conflict as exc:assert exc.code=='REVIEW_GOLD_OWNER_INPUTS_CHANGED'
        return dict(actual_mysql=True,isolated=True,business_approvals_created=0,review_events=len(requests)+2,
            product_uuid_preserved=True,image_uuids_preserved=True,activation_rollback=True,repeated_activation='REUSED',
            stale_approval_blocked=True,embedding_not_fabricated=True,audit=audit)
    finally:db.close()
