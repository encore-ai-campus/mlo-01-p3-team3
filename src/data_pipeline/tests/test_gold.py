import copy
import tempfile
import unittest

from test_silver import sample,inputs as silver_inputs,RUN,AS_OF
from test_silver_runtime import States,Sink as SilverSink
from product_pipeline.contracts import canonical,digest
from product_pipeline.errors import PipelineError,Conflict
from product_pipeline.files import parse
from product_pipeline.silver.normalize import KEYS as SILVER_KEYS,semantic_hash as silver_hash
from product_pipeline.silver.runner import process as silver_process
from product_pipeline.gold.build import build,validate_group,choose
from product_pipeline.gold.contracts import INPUT_TABLES,validate_inputs,registration,semantic_hash
from product_pipeline.gold.storage import EXTRA
from product_pipeline.gold.runner import process


def side():
    value=dict(contract_version=1,policy=dict(version='fixture-policy-1',evidence_ref='fixture://policy',
        price_history_mode='UNRESOLVED',price_max_age_seconds=86400*3,
        representative_price_kinds=['SELLER_REPORTED'],bootstrap_single_gallery=True))
    for kind in ('product_states','embeddings','evaluation_selection','gallery_selection','price_confirmations'):
        value[kind]=dict(status='NOT_PROVIDED',rows=[],evidence_ref=None)
    return seal(value)


def seal(value):
    value['content_hash']=digest({k:v for k,v in value.items() if k!='content_hash'});return value


def provide(value,kind,rows):
    value[kind]=dict(status='PROVIDED',rows=rows,evidence_ref='fixture://'+kind)
    return seal(value)


def group(root):
    doc,state=sample(root);sink=SilverSink()
    silver_process(doc,States(state),sink,silver_inputs(),processing_run_id=RUN,as_of=AS_OF,
                   bronze_version=1,bronze_path='file:///fixture-bronze')
    return dict(receipt=next(iter(sink.data['pipeline_evaluations'].values())),
                tables={t:list(rows.values()) for t,rows in sink.data.items() if t!='pipeline_evaluations'})


def reseal(value):
    manifest={}
    for table,rows in value['tables'].items():
        for row in rows:row['record_hash']=silver_hash(row)
        manifest[table]=dict(version=1,records={r[SILVER_KEYS[table]]:r['record_hash'] for r in rows})
    value['receipt']['output_manifest_json']=canonical(manifest)
    value['receipt']['record_hash']=silver_hash(value['receipt'])
    return value


VERSIONS={t:10 for t in INPUT_TABLES}


def ready(g,s):
    product=g['tables']['product_observations'][0];key=product['product_key']
    product.update(service_category_id=RUN,service_category_code='CHAIR',mapping_status='MAPPED')
    for r in g['tables']['product_images']:
        r.update(review_status='APPROVED',availability='AVAILABLE',validation_status='LOCAL_BYTES_VERIFIED',
                 evidence_json=canonical(dict(review_rows=[dict(decision='APPROVED',effective_status='ACTIVE',
                    scopes=['RECOMMENDATION'],evidence_ref='fixture://rights',expires_at='2026-11-01T00:00:00.000Z')])))
    link=g['tables']['link_observations'][0]
    link.update(structure_status='VALID',link_status='VALID',checked_at='2026-10-16T00:00:00.000Z',
                expires_at='2026-11-01T00:00:00.000Z',link_kind='COMPARISON')
    provide(s,'product_states',[dict(product_key=key,active=True,supported=True,service_category_id=RUN,
        mapping_version=product['mapping_version'],applicable_attributes=['display_name','width_mm','depth_mm','height_mm'],
        checked_at='2026-10-16T00:00:00.000Z',expires_at='2026-11-01T00:00:00.000Z',evidence_ref='fixture://state')])
    img=min(g['tables']['product_images'],key=lambda r:r['display_order'])
    provide(s,'embeddings',[dict(product_key=key,status='READY',image_relation_key=img['image_relation_key'],
        checksum=img['image_checksum'],source_version=img['source_version'],embedding_id=RUN,
        vector_ref='fixture://vector',model_version='model-1',preprocessing_version='prep-1',index_version='index-1',
        dimensions=32,silver_confirmed_ref='fixture://embedding-receipt',embedding_usage_approved=True,index_active=True,
        checked_at='2026-10-16T00:00:00.000Z',expires_at='2026-11-01T00:00:00.000Z',evidence_ref='fixture://embedding')])
    return reseal(g),s


class MemoryGoldSink:
    def __init__(self):self.data={t:{} for t in EXTRA};self.history={t:[{}] for t in EXTRA}
    def existing(self,t,keys):return {k:v for k,v in self.data[t].items() if k in keys}
    def insert(self,t,rows):
        for r in rows:
            k=r['row_key']
            if r['record_hash']!=semantic_hash(r) or (k in self.data[t] and self.data[t][k]['record_hash']!=r['record_hash']):
                raise Conflict('TEST_CONFLICT')
            self.data[t].setdefault(k,copy.deepcopy(r))
        self.history[t].append(copy.deepcopy(self.data[t]))
    def version(self,t):return len(self.history[t])-1
    def verify(self,t,expected,version):
        for data in (self.history[t][version],self.data[t]):
            for k,v in expected.items():
                if k not in data or data[k]['record_hash']!=v or semantic_hash(data[k])!=v:raise Conflict('TEST_MISSING_OUTPUT')


class GoldTests(unittest.TestCase):
    def build(self,g,s=None,**kw):
        return build([g],s or side(),VERSIONS,processing_run_id=kw.get('run',RUN),as_of=kw.get('as_of',AS_OF))

    def test_unprovided_inputs_preserve_relations_and_block_ready(self):
        with tempfile.TemporaryDirectory() as d:
            g=group(d);out=self.build(g);p=out['tables']['product_catalog'][0]
            self.assertFalse(p['catalog_ready']);self.assertFalse(p['recommendation_ready']);self.assertIsNone(p['price_krw'])
            self.assertEqual(len(out['tables']['product_images']),2)
            self.assertEqual(out['tables']['current_offers'],[])
            self.assertEqual(p['publication_status'],'BLOCKED')

    def test_owner_confirmed_catalog_image_link_embedding_enable_recommendation_without_price(self):
        with tempfile.TemporaryDirectory() as d:
            g,s=ready(group(d),side());p=self.build(g,s)['tables']['product_catalog'][0]
            self.assertTrue(p['catalog_ready']);self.assertTrue(p['comparison_link_ready'])
            self.assertTrue(p['recommendation_ready']);self.assertFalse(p['purchase_link_ready'])
            self.assertIsNone(p['price_krw']);self.assertEqual(p['publication_status'],'BLOCKED')

    def test_expired_review_and_embedding_or_wrong_checksum_cannot_enable_ready(self):
        with tempfile.TemporaryDirectory() as d:
            g,s=ready(group(d),side())
            self.assertFalse(self.build(g,s,as_of='2026-12-01T00:00:00.000Z')['tables']['product_catalog'][0]['recommendation_ready'])
            s['embeddings']['rows'][0]['checksum']='bad';seal(s)
            self.assertFalse(self.build(g,s)['tables']['product_catalog'][0]['recommendation_ready'])

    def test_silver_missing_output_or_content_tamper_rejected(self):
        with tempfile.TemporaryDirectory() as d:
            original=group(d);g=copy.deepcopy(original);g['tables']['product_images'].pop()
            with self.assertRaises(PipelineError):self.build(g)
            g=copy.deepcopy(original);g['tables']['product_observations'][0]['display_name']='forged'
            with self.assertRaises(PipelineError):self.build(g)

    def test_equal_time_conflict_retains_older_confirmed_value(self):
        rows=[dict(observed_at='2026-10-16T00:00:00.000Z',time_status='OBSERVED',value='B',record_hash='b'),
              dict(observed_at='2026-10-16T00:00:00.000Z',time_status='OBSERVED',value='C',record_hash='c'),
              dict(observed_at='2026-10-15T00:00:00.000Z',time_status='OBSERVED',value='A',record_hash='a')]
        value,conflicts=choose(rows,['value']);self.assertEqual(value['value'],'A');self.assertEqual(len(conflicts),1)
        self.assertIsNone(choose(rows[:2],['value'])[0])

    def test_evaluation_hash_is_not_latest_order(self):
        with tempfile.TemporaryDirectory() as d:
            g=group(d);other=copy.deepcopy(g)
            for rows in other['tables'].values():
                for r in rows:r['evaluation_key']='other-evaluation'
            other['receipt']['evaluation_key']='other-evaluation';reseal(other)
            with self.assertRaisesRegex(PipelineError,'GOLD_EVALUATION_SELECTION_REQUIRED'):
                build([g,other],side(),VERSIONS,processing_run_id=RUN,as_of=AS_OF)
            s=side();provide(s,'evaluation_selection',[dict(product_key=g['receipt']['product_key'],
                raw_document_id=g['receipt']['raw_document_id'],evaluation_key=g['receipt']['evaluation_key'],evidence_ref='fixture://selection')])
            out=build([g,other],s,VERSIONS,processing_run_id=RUN,as_of=AS_OF)
            self.assertEqual(out['tables']['quality_daily'][0]['evaluation_count'],1)

    def test_same_input_is_stable_across_execution_ids(self):
        with tempfile.TemporaryDirectory() as d:
            g=group(d);a=self.build(g);b=self.build(g,run='a1da6176-3e44-40f3-88e5-2b0b70630035')
            self.assertEqual(a['evaluation_key'],b['evaluation_key'])
            self.assertEqual(a['tables']['product_catalog'][0]['record_hash'],b['tables']['product_catalog'][0]['record_hash'])

    def test_partial_output_recovery_receipt_last_and_no_missing_output_skip(self):
        with tempfile.TemporaryDirectory() as d:
            g=group(d);sink=MemoryGoldSink();s=side()
            def crash(stage):
                if stage=='BEFORE_RECEIPT':raise RuntimeError('interruption')
            kwargs=dict(processing_run_id=RUN,as_of=AS_OF)
            with self.assertRaises(RuntimeError):process([g],s,VERSIONS,sink,hook=crash,**kwargs)
            self.assertFalse(sink.data['pipeline_evaluations']);old=copy.deepcopy(sink.data['product_catalog'])
            self.assertFalse(process([g],s,VERSIONS,sink,**kwargs)['reused'])
            self.assertEqual(old,sink.data['product_catalog'])
            self.assertTrue(process([g],s,VERSIONS,sink,**kwargs)['reused'])
            sink.data['product_images'].clear()
            with self.assertRaises(PipelineError):process([g],s,VERSIONS,sink,**kwargs)

    def test_input_hash_fixed_versions_and_registration_gates(self):
        from product_pipeline.gold.contracts import validate_versions
        s=side();s['policy']['price_history_mode']='PRICE_EFFECT_V2'
        with self.assertRaises(PipelineError):validate_inputs(s)
        with self.assertRaises(PipelineError):validate_versions({'product_observations':1})
        with self.assertRaises(PipelineError):registration(dict(processing_run_id=RUN,stage='GOLD_BUILD',status='FIXTURE',evidence_ref='fixture'))

    def test_snapshot_file_counts_are_distinct_from_image_relationships(self):
        with tempfile.TemporaryDirectory() as d:
            g=group(d)
            g['tables']['product_images'][1]['image_checksum']=g['tables']['product_images'][0]['image_checksum'];reseal(g)
            q=self.build(g)['tables']['quality_daily'][0]
            self.assertEqual(q['image_relation_count'],2);self.assertEqual(q['unique_file_count'],1)

    def test_excluded_mapping_cannot_be_published(self):
        with tempfile.TemporaryDirectory() as d:
            g,s=ready(group(d),side());g['tables']['product_observations'][0]['mapping_status']='EXCLUDED';reseal(g)
            self.assertFalse(self.build(g,s)['tables']['product_catalog'][0]['catalog_ready'])

    def test_future_observation_is_not_current_value(self):
        rows=[dict(observed_at='2027-01-01T00:00:00.000Z',time_status='OBSERVED',value='future',record_hash='a')]
        self.assertIsNone(choose(rows,['value'],AS_OF)[0])

    def test_dimensions_use_attribute_observation_time(self):
        with tempfile.TemporaryDirectory() as d:
            g=group(d)
            attr=g['tables']['product_attributes'][0]
            attr.update(attribute_name='DIMENSION',attribute_status='CONFIRMED',measurement_target='PRODUCT',
                option_status='NO_OPTION_CONFIRMED',value_text='50x60x100cm',observed_at='2026-10-16T01:00:00.000Z',time_status='OBSERVED')
            reseal(g);p=self.build(g)['tables']['product_catalog'][0]
            self.assertEqual((p['width_mm'],p['depth_mm'],p['height_mm']),('500','600','1000'))
            self.assertEqual(parse(p['field_lineage_json'])['width_mm']['observed_at'],'2026-10-16T01:00:00.000Z')

    def test_verified_offer_with_owner_conditions_can_have_current_price(self):
        from product_pipeline.silver.schemas import complete
        with tempfile.TemporaryDirectory() as d:
            g,s=ready(group(d),side());product=g['tables']['product_observations'][0]
            price=next(r for r in g['tables']['price_observations'] if r['price_kind']=='SELLER_REPORTED')
            price.update(offer_key='offer-v1:fixture-stable-identity',option_key='UNSPECIFIED',
                numeric_status='CONFIRMED',amount='1000',currency='KRW')
            common={k:v for k,v in price.items() if k in __import__('product_pipeline.silver.schemas',fromlist=['COMMON']).COMMON}
            g['tables']['normalized_offers']=[complete('normalized_offers',dict(common,
                normalized_offer_key='silver-offer-fixture',offer_key=price['offer_key'],source_seller_id='S1',source_offer_id='O1',
                option_key='UNSPECIFIED',identity_rule_version='fixture-stable-1',identity_evidence_json='{}',
                price_observation_keys=[price['price_observation_key']],link_observation_keys=[]))]
            condition=dict(condition_schema_version=1,price_scope='SELLER_ITEM',unresolved_evidence_key=None,
                **{k:dict(state='NOT_APPLICABLE',value=None) for k in ('discount','membership','coupon','delivery','installation','tax')},
                option=dict(state='CONFIRMED',value=dict(option_key='UNSPECIFIED',identity_status='NO_OPTION_CONFIRMED')))
            confirmation=dict(product_key=product['product_key'],raw_document_id=price['raw_document_id'],source_evidence_key=price['source_evidence_key'],
                option_key='UNSPECIFIED',price_kind='SELLER_REPORTED',price_basis='REGULAR',condition=condition,
                checked_at='2026-10-16T00:00:00.000Z',expires_at='2026-11-01T00:00:00.000Z',evidence_ref='fixture://price-conditions')
            provide(s,'price_confirmations',[confirmation]);reseal(g)
            out=self.build(g,s)
            self.assertEqual(len(out['tables']['current_offers']),1)
            self.assertEqual(out['tables']['product_catalog'][0]['price_krw'],'1000')
            self.assertEqual(len(out['tables']['offer_history']),1)
            self.assertFalse(out['tables']['product_catalog'][0]['purchase_link_ready'])
            confirmation['condition']['membership']=dict(state='UNKNOWN',value=None);seal(s)
            unverified=self.build(g,s)
            self.assertEqual(len(unverified['tables']['current_offers']),1)
            self.assertEqual(parse(unverified['tables']['current_offers'][0]['evidence_json'])['offer_publication_status'],'OBSERVED_UNVERIFIED')
            self.assertIsNone(unverified['tables']['product_catalog'][0]['price_krw'])
            self.assertFalse(unverified['tables']['product_catalog'][0]['purchase_link_ready'])
