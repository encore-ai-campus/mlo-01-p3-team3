"""Explicit owner-approved fixture. Never represents production approval."""
from product_pipeline.contracts import canonical,digest
from product_pipeline.gold.runner import process
from product_pipeline.publication.plan import make_plan
from product_pipeline.files import parse
from test_gold import group,side,ready,seal,reseal,VERSIONS,MemoryGoldSink
from test_silver import RUN,AS_OF


def fixture(root,target,*,name=None,stamp=None,as_of=AS_OF,price_amount=None,evaluation_identity=None):
    from pathlib import Path
    Path(root).mkdir(parents=True,exist_ok=True)
    silver=group(root);inputs=side();inputs['policy']['price_history_mode']='SNAPSHOT_V1';seal(inputs)
    if evaluation_identity:inputs['policy']['evaluation_identity']=evaluation_identity;seal(inputs)
    image_proofs={r['image_relation_key']:parse(r['evidence_json']) for r in silver['tables']['product_images']}
    silver,inputs=ready(silver,inputs)
    for image in silver['tables']['product_images']:
        image['evidence_json']=canonical({**image_proofs[image['image_relation_key']],**parse(image['evidence_json'])})
    if name:silver['tables']['product_observations'][0]['display_name']=name
    if stamp:silver['tables']['product_observations'][0]['observed_at']=stamp
    if price_amount is not None:
        from product_pipeline.silver.schemas import COMMON,complete
        from test_gold import provide
        product=silver['tables']['product_observations'][0]
        price=next(r for r in silver['tables']['price_observations'] if r['price_kind']=='SELLER_REPORTED')
        price.update(offer_key='offer-v1:fixture-stable-identity',option_key='UNSPECIFIED',numeric_status='CONFIRMED',
                     amount=str(price_amount),currency='KRW',observed_at=stamp or price['observed_at'])
        common={k:v for k,v in price.items() if k in COMMON}
        silver['tables']['normalized_offers']=[complete('normalized_offers',dict(common,
            normalized_offer_key='silver-offer-fixture',offer_key=price['offer_key'],source_seller_id='S1',source_offer_id='O1',
            option_key='UNSPECIFIED',identity_rule_version='fixture-stable-1',identity_evidence_json='{}',
            price_observation_keys=[price['price_observation_key']],link_observation_keys=[]))]
        condition=dict(condition_schema_version=1,price_scope='SELLER_ITEM',unresolved_evidence_key=None,
            **{k:dict(state='NOT_APPLICABLE',value=None) for k in ('discount','membership','coupon','delivery','installation','tax')},
            option=dict(state='CONFIRMED',value=dict(option_key='UNSPECIFIED',identity_status='NO_OPTION_CONFIRMED')))
        provide(inputs,'price_confirmations',[dict(product_key=product['product_key'],raw_document_id=price['raw_document_id'],
            source_evidence_key=price['source_evidence_key'],option_key='UNSPECIFIED',price_kind='SELLER_REPORTED',price_basis='REGULAR',
            condition=condition,checked_at='2026-10-16T00:00:00.000Z',expires_at='2026-11-01T00:00:00.000Z',evidence_ref='fixture://conditions')])
    reseal(silver)
    sink=MemoryGoldSink();process([silver],inputs,VERSIONS,sink,processing_run_id=RUN,as_of=as_of)
    group_=dict(receipt=next(iter(sink.data['pipeline_evaluations'].values())),
                tables={t:list(v.values()) for t,v in sink.data.items() if t!='pipeline_evaluations'})
    catalog=group_['tables']['product_catalog'][0]
    reviews={}
    for image in group_['tables']['product_images']:
        review=dict(decision='APPROVED',scope='RECOMMENDATION',evidence_ref='fixture://owner')
        reviews[image['image_relation_key']]=dict(snapshot_hash=image['review_snapshot_hash'],usage_review=review,
                                                current_review_hash=digest(review))
    guard=dict(contract_version=1,evidence_ref='fixture://publication-owner',gold_evaluation_key=catalog['gold_evaluation_key'],
        input_fingerprint=catalog['input_fingerprint'],category_id=catalog['service_category_id'],product_active=True,
        expires_at='2099-01-01T00:00:00Z',price_history_mode='SNAPSHOT_V1',image_reviews=reviews)
    versions={t:sink.version(t) for t in sink.data}
    plan=make_plan([group_],versions,export_hash=digest({'fixture':catalog['gold_evaluation_key']}),as_of=as_of,
                   guard_hashes={catalog['product_key']:digest(guard)},target=target,price_history_mode='SNAPSHOT_V1',price_max_age_seconds=86400*3)
    return plan,guard
