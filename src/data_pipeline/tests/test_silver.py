import copy
from pathlib import Path
import tempfile
import unittest

from helpers import fixture
from product_pipeline.contracts import canonical, collector_payload_hash, digest, envelope, event_for, observation_key
from product_pipeline.files import rows
from product_pipeline.snapshot import prepare
from product_pipeline.errors import PipelineError

RUN = '5dd50e5a-18b9-4c80-ad7e-05145774a650'
AS_OF = '2026-10-16T12:00:00.000Z'


def sample(root):
    source, media = fixture(root, codes=('11',))
    sealed = Path(root)/'sealed'
    prepare(source, sealed, media)
    row = next(rows(sealed/'observations.jsonl'))
    raw = row['raw']
    listing = raw['payload']['listings'][0]
    listing.update(format='action_product', price_kind='comparison_lowest', price_text='1,000원', product_name='의자 A',
                   api_product=dict(productCode=11, name=dict(full='의자 A', maker='제조사', brand='브랜드', model='M1'),
                     category=dict(codes=[1,2], names=['가구','의자']),
                     price=dict(min=1000, minFormatted='1,000', member=1000, memberFormatted='1,000', total=1000, initial=0),
                     dates=dict(register='18.02.', make='2018.02.01')))
    raw['payload']['detail'].update(status='ok', matched_by='pcode', specifications=[
        dict(name='제품 전체(가로x깊이x높이)', value='50x60x100cm'),
        dict(name='좌판높이', value='40~50cm'), dict(name='색상', value='블랙')],
        selected_product=dict(sku='11', name='의자 A', offers=dict(**{'@type':'AggregateOffer'}, priceCurrency='KRW', lowPrice='1000')),
        seller_offers=[dict(seller_id='S1', price_attribute='1000', price_text='1,000원',
                           url='https://prod.danawa.com/bridge/loadingBridge.html?link_pcode=123')])
    raw['payload_hash'] = collector_payload_hash(raw['payload'])
    row['observation_key']=observation_key(raw,row['images'])
    doc = envelope(row, '0123456789abcdef01234567', 'snapshot', 'images')
    vector = sorted([dict(image_slot=i['image_slot'], checksum=i.get('sha256'), bucket=i.get('bucket'),
        object_key=i.get('object_key'), availability='AVAILABLE', verification='sha256-bytes-v1') for i in doc['image_refs']], key=lambda i:i['image_slot'])
    state = dict(raw_document_id=doc['_id'], observation_key=doc['observation_key'], image_state_revision=1,
                 image_state_vector=vector, image_state_fingerprint=digest(vector), image_state_checked_at=AS_OF)
    return doc, state


def snapshot(kind, values=None):
    from product_pipeline.silver.inputs import input_snapshot
    return input_snapshot(kind, values, provider_version='fixture-1', evidence_ref='fixture://owner-export')


def inputs(**kwargs):
    return {kind: snapshot(kind, kwargs.get(kind)) for kind in ('mappings','reviews','links','rules')}


def reseal(doc,state):
    doc['payload_hash']=collector_payload_hash(doc['payload'])
    doc['collector_metadata']['payload_hash']=doc['payload_hash']
    raw=dict(doc['collector_metadata'],payload=doc['payload'])
    doc['source_observation_hash']=digest(raw)
    doc['observation_key']=observation_key(raw,doc['image_refs'])
    state['observation_key']=doc['observation_key']
    doc['document_hash']=digest({k:v for k,v in doc.items() if k!='document_hash'})


class SilverTests(unittest.TestCase):
    def normalize(self, doc, state, side=None, **kwargs):
        from product_pipeline.silver.normalize import normalize
        return normalize(doc, state, side or inputs(), processing_run_id=RUN, as_of=kwargs.get('as_of', AS_OF))

    def test_price_types_times_and_gallery_relationships_are_separate(self):
        with tempfile.TemporaryDirectory() as d:
            doc, state = sample(d)
            result = self.normalize(doc,state)
            prices = result['tables']['price_observations']
            kinds = {r['price_kind']:r for r in prices}
            self.assertEqual(kinds['LISTING_LOWEST']['amount'], '1000')
            self.assertEqual(kinds['MEMBER_REPORTED']['amount'], '1000')
            self.assertEqual(kinds['INITIAL_UNCLASSIFIED']['numeric_status'], 'INVALID_ZERO')
            self.assertNotEqual(kinds['LISTING_LOWEST']['condition_fingerprint'], kinds['MEMBER_REPORTED']['condition_fingerprint'])
            self.assertEqual(kinds['LISTING_LOWEST']['observed_at'], '2026-10-15T03:00:00.000Z')
            self.assertEqual(len(result['tables']['product_images']), 2)
            self.assertEqual(result['tables']['normalized_offers'], [])
            self.assertEqual(result['status'], 'QUALITY_WAIT')

    def test_unknown_dimensions_dates_and_options_are_never_invented(self):
        with tempfile.TemporaryDirectory() as d:
            doc,state=sample(d)
            result=self.normalize(doc,state)
            product=result['tables']['product_observations'][0]
            self.assertIsNone(product['width_mm'])
            self.assertIsNone(product['registered_year'])
            self.assertEqual(product['registered_status'], 'AMBIGUOUS')
            attrs=result['tables']['product_attributes']
            self.assertTrue(any(a['value_min']=='400' and a['value_max']=='500' for a in attrs))
            self.assertTrue(all(a['option_status']=='UNKNOWN' for a in attrs))

    def test_owner_rules_and_mapping_enable_only_confirmed_projection(self):
        with tempfile.TemporaryDirectory() as d:
            doc,state=sample(d)
            side=inputs(mappings=[dict(source_code='danawa',source_category_codes=['1','2'],
                service_category_id=RUN,service_category_code='CHAIR',active=True,excluded=False)],
                rules=[dict(type='AXIS_RULE',source_name='제품 전체(가로x깊이x높이)',
                    axes=['WIDTH','DEPTH','HEIGHT'],measurement_target='PRODUCT',option_status='NO_OPTION_CONFIRMED'),
                    dict(type='ATTRIBUTE_ALIAS',attribute_name='COLOR',raw_value='블랙',codes=['BLACK'])])
            result=self.normalize(doc,state,side)
            product=result['tables']['product_observations'][0]
            self.assertEqual((product['width_mm'],product['depth_mm'],product['height_mm']), ('500','600','1000'))
            self.assertEqual(product['mapping_status'],'MAPPED')
            self.assertTrue(any(r['codes']==['BLACK'] for r in result['tables']['product_attributes']))

    def test_state_tampering_and_raw_conflict_fail_before_quality_receipt(self):
        with tempfile.TemporaryDirectory() as d:
            doc,state=sample(d)
            broken=copy.deepcopy(state); broken['image_state_vector'][0]['availability']='MISSING'
            with self.assertRaises(PipelineError): self.normalize(doc,broken)
            broken=copy.deepcopy(doc); broken['payload']['listings'][0]['api_product']['productCode']=12
            broken['document_hash']=digest({k:v for k,v in broken.items() if k!='document_hash'})
            with self.assertRaises(PipelineError): self.normalize(broken,state)

    def test_same_inputs_reuse_and_related_changes_create_new_evaluations(self):
        with tempfile.TemporaryDirectory() as d:
            doc,state=sample(d)
            first=self.normalize(doc,state)
            second=self.normalize(doc,state,as_of='2026-10-17T12:00:00.000Z')
            self.assertEqual(first['evaluation_key'],second['evaluation_key'])
            irrelevant=inputs(mappings=[dict(source_code='danawa',source_category_codes=['999'],
                service_category_id=RUN,service_category_code='OTHER',active=True,excluded=False)])
            # Explicit PROVIDED versus NOT_PROVIDED changes the missing-input evidence.
            provided_empty=inputs(mappings=[])
            self.assertEqual(self.normalize(doc,state,irrelevant)['evaluation_key'],
                             self.normalize(doc,state,provided_empty)['evaluation_key'])
            newer=copy.deepcopy(state);newer['image_state_revision']=2
            newer['image_state_vector'][0]['availability']='MISSING'
            newer['image_state_fingerprint']=digest(newer['image_state_vector'])
            self.assertNotEqual(first['evaluation_key'],self.normalize(doc,newer)['evaluation_key'])

    def test_review_and_link_expiry_change_effective_state_not_processing_time(self):
        with tempfile.TemporaryDirectory() as d:
            doc,state=sample(d); image=doc['image_refs'][0]
            side=inputs(reviews=[dict(source_code='danawa',source_product_id='11',source_version=image['source_version'],
                checksum=image['sha256'],scopes=['RECOMMENDATION'],decision='APPROVED',
                evidence_ref='fixture://rights',expires_at='2026-10-17T00:00:00.000Z')])
            first=self.normalize(doc,state,side)
            expired=self.normalize(doc,state,side,as_of='2026-10-18T00:00:00.000Z')
            self.assertEqual(next(r for r in first['tables']['product_images'] if r['source_version']==image['source_version'])['review_status'],'APPROVED')
            self.assertEqual(next(r for r in expired['tables']['product_images'] if r['source_version']==image['source_version'])['review_status'],'EXPIRED')
            self.assertNotEqual(first['evaluation_key'],expired['evaluation_key'])

    def test_price_zero_negative_fraction_currency_and_display_conflict(self):
        for value,text,expected in [(0,'0원','INVALID_ZERO'),(-100,'-100원','INVALID_NEGATIVE'),
             ('1.5','1.5원','INVALID_FRACTION'),(1000,'2,000원','VALUE_CONFLICT')]:
            with self.subTest(value=value),tempfile.TemporaryDirectory() as d:
                doc,state=sample(d)
                listing=doc['payload']['listings'][0]
                listing['api_product']['price']['min']=value;listing['price_text']=text
                listing['api_product']['price']['minFormatted']=str(value)
                reseal(doc,state)
                row=next(p for p in self.normalize(doc,state)['tables']['price_observations'] if p['price_kind']=='LISTING_LOWEST')
                self.assertEqual(row['numeric_status'],expected)

    def test_stable_offer_needs_owner_rule_and_real_sale_identity_not_bridge_token(self):
        with tempfile.TemporaryDirectory() as d:
            doc,state=sample(d)
            rule=dict(type='OFFER_IDENTITY',verified_stable=True,seller_id_field='seller_id',
                offer_id_field='source_offer_id',option_status='NO_OPTION_CONFIRMED',option_key='BASE',
                version='owner-offer-1',evidence_ref='fixture://verified-identity')
            side=inputs(rules=[rule])
            self.assertFalse(self.normalize(doc,state,side)['tables']['normalized_offers'])
            doc['payload']['detail']['seller_offers'][0]['source_offer_id']='SALE-001'
            reseal(doc,state)
            result=self.normalize(doc,state,side)
            offer=result['tables']['normalized_offers'][0]
            self.assertEqual(offer['source_offer_id'],'SALE-001')
            self.assertEqual(len(offer['price_observation_keys']),1)
            self.assertEqual(len(offer['link_observation_keys']),1)

    def test_conflicting_api_formatted_price_is_not_hidden_by_matching_listing_text(self):
        with tempfile.TemporaryDirectory() as d:
            doc,state=sample(d)
            doc['payload']['listings'][0]['api_product']['price']['minFormatted']='9,999'
            reseal(doc,state)
            price=next(r for r in self.normalize(doc,state)['tables']['price_observations'] if r['price_kind']=='LISTING_LOWEST')
            self.assertEqual(price['numeric_status'],'VALUE_CONFLICT')

    def test_fallback_listing_attribute_retains_listing_time_and_exact_source_pointer(self):
        with tempfile.TemporaryDirectory() as d:
            doc,state=sample(d);doc['payload']['detail']['specifications']=[]
            doc['payload']['listings'][0]['spec_text']='50cm'
            reseal(doc,state)
            attr=self.normalize(doc,state)['tables']['product_attributes'][0]
            from product_pipeline.files import parse
            self.assertEqual(attr['observed_at'],'2026-10-15T03:00:00.000Z')
            self.assertEqual(parse(attr['evidence_json'])['source_path'],'/payload/listings/0/spec_text')
            self.assertEqual(attr['raw_value_json'],canonical('50cm'))

    def test_unrelated_review_and_link_rows_do_not_change_the_evaluation(self):
        with tempfile.TemporaryDirectory() as d:
            doc,state=sample(d)
            base=inputs(reviews=[],links=[])
            other=inputs(reviews=[dict(source_code='danawa',source_product_id='11',source_version='another-version',
                checksum='another-checksum',scopes=['RECOMMENDATION'],decision='REVOKED')],
                links=[dict(source_code='danawa',source_product_id='11',url='https://other.example/unobserved',kind='DIRECT_PURCHASE',status='INVALID')])
            self.assertEqual(self.normalize(doc,state,base)['evaluation_key'],self.normalize(doc,state,other)['evaluation_key'])

    def test_missing_detail_is_extraction_failure_and_future_review_is_not_approved(self):
        with tempfile.TemporaryDirectory() as d:
            doc,state=sample(d);doc['payload']['detail']['specifications']=[]
            doc['payload']['detail']['status']='failed';doc['collector_metadata']['detail_status']='failed'
            reseal(doc,state)
            from product_pipeline.files import parse
            product=self.normalize(doc,state)['tables']['product_observations'][0]
            self.assertEqual(parse(product['field_status_json'])['attributes'],'EXTRACTION_FAILED')
            self.assertEqual(product['dimension_status'],'EXTRACTION_FAILED')
            img=doc['image_refs'][0]
            side=inputs(reviews=[dict(source_code='danawa',source_product_id='11',source_version=img['source_version'],checksum=img['sha256'],
                scopes=['RECOMMENDATION'],decision='APPROVED',evidence_ref='fixture://rights',valid_from='2026-12-01T00:00:00Z')])
            self.assertEqual(next(r for r in self.normalize(doc,state,side)['tables']['product_images'] if r['source_version']==img['source_version'])['review_status'],'NOT_YET_VALID')

    def test_link_expired_and_option_dimensions_cannot_enable_current_fields(self):
        with tempfile.TemporaryDirectory() as d:
            doc,state=sample(d)
            side=inputs(links=[dict(source_code='danawa',source_product_id='11',url=doc['payload']['detail']['url'],kind='COMPARISON',
                status='VALID',checked_at='2026-10-14T00:00:00Z',expires_at='2026-10-15T00:00:00Z',evidence_ref='fixture://link')],
                rules=[dict(type='AXIS_RULE',source_name='제품 전체(가로x깊이x높이)',axes=['WIDTH','DEPTH','HEIGHT'],measurement_target='PRODUCT',option_status='UNKNOWN')])
            result=self.normalize(doc,state,side)
            self.assertIsNone(result['tables']['product_observations'][0]['width_mm'])
            self.assertTrue(any(r['link_status']=='EXPIRED' for r in result['tables']['link_observations']))


if __name__=='__main__': unittest.main()
