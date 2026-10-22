import copy
import tempfile
import unittest
from datetime import datetime

from test_silver import sample, inputs, reseal, RUN, AS_OF
from test_silver_runtime import States, Sink
from test_gold import side, VERSIONS
from test_publication_pending import Database
from product_pipeline.contracts import canonical
from product_pipeline.files import parse
from product_pipeline.silver.offer_identity import bridge_identity
from product_pipeline.silver.runner import process
from product_pipeline.gold.build import build
from product_pipeline.publication.repository import _observed_offers


URL = 'https://prod.danawa.com/bridge/loadingBridge.html?pcode=11&cmpnyc=S1&link_pcode=001234&r=changing'


class OfferRecoveryTests(unittest.TestCase):
    def identity(self, url=URL, **changes):
        return bridge_identity(dict(source_code='danawa', source_product_id='11'),
                               dict(seller_id='S1', url=url, **changes))

    def test_bridge_recovers_item_without_click_token_or_invented_option(self):
        item = self.identity()
        self.assertEqual(item['source_offer_id'], '001234')
        self.assertEqual(item['option_status'], 'UNKNOWN')
        self.assertEqual(item['option_key'], 'UNKNOWN')
        self.assertEqual(item['offer_key'], self.identity(URL.replace('changing', 'another'))['offer_key'])
        self.assertNotEqual(item['offer_key'], self.identity(URL.replace('001234', '001235'))['offer_key'])

    def test_invalid_ambiguous_or_mismatched_urls_are_not_identities(self):
        for url in (URL.replace('pcode=11', 'pcode=12'), URL.replace('cmpnyc=S1', 'cmpnyc=S2'),
                    URL + '&link_pcode=another', URL.replace('prod.danawa.com', 'evil.example'),
                    URL.replace('https:', 'http:'), URL.replace('001234', ''),
                    URL.replace('link_pcode=001234&', ''), URL.replace('001234', '0')):
            with self.subTest(url=url): self.assertIsNone(self.identity(url))
        self.assertIsNone(self.identity(source_offer_id='conflicting'))

    def recovered(self, root):
        doc, state = sample(root)
        doc['payload']['detail']['seller_offers'][0]['url'] = URL
        reseal(doc, state)
        original = copy.deepcopy(doc)
        sink = Sink()
        process(doc, States(state), sink, inputs(), processing_run_id=RUN, as_of=AS_OF,
                bronze_version=1, bronze_path='file:///fixture-bronze')
        self.assertEqual(doc, original)
        group = dict(receipt=next(iter(sink.data['pipeline_evaluations'].values())),
                     tables={t:list(rows.values()) for t,rows in sink.data.items() if t!='pipeline_evaluations'})
        return build([group], side(), VERSIONS, processing_run_id=RUN, as_of=AS_OF)

    def test_recovery_reaches_gold_without_making_price_or_purchase_ready(self):
        with tempfile.TemporaryDirectory() as root:
            result = self.recovered(root)
            tables = result['tables']
            self.assertEqual(len(tables['current_offers']), 1)
            offer = tables['current_offers'][0]
            self.assertFalse(offer['purchase_ready'])
            self.assertIsNone(offer['purchase_url'])
            self.assertEqual(parse(offer['evidence_json'])['offer_publication_status'], 'OBSERVED_UNVERIFIED')
            self.assertFalse(tables['offer_history'][0]['eligible'])
            self.assertIsNone(tables['product_catalog'][0]['price_krw'])
            self.assertFalse(tables['product_catalog'][0]['purchase_link_ready'])

    def test_publication_reuses_ids_history_and_keeps_unknown_conditions(self):
        with tempfile.TemporaryDirectory() as root:
            result = self.recovered(root)
            tables = result['tables']
            catalog = tables['product_catalog'][0]
            item = dict(product_key=catalog['product_key'], group=dict(tables=tables))
            plan = dict(plan_id='p', export_hash='h', gold_versions={}, price_history_mode='SNAPSHOT_V1')
            db = Database(); product = dict(id='product')
            def run(): _observed_offers(db, product, catalog, tables, datetime(2026,10,19), item, plan)
            run(); saved = copy.deepcopy(db.rows['offers']); history = copy.deepcopy(db.rows['offer_snapshots'])
            self.assertEqual(len(saved), 1); self.assertEqual(len(history), 1)
            self.assertEqual(saved[0]['source_offer_id'], '001234')
            self.assertFalse(saved[0]['eligible'])
            self.assertEqual(saved[0]['price_basis'], 'UNKNOWN')
            self.assertEqual(parse(saved[0]['option_attributes_json'])['status'], 'UNKNOWN')
            run()
            self.assertEqual(saved, db.rows['offers']); self.assertEqual(history, db.rows['offer_snapshots'])
            db.rows['offers'][0]['collected_at'] = datetime(2026,10,18)
            db.rows['offers'][0]['price_amount'] = 5000
            run()
            self.assertEqual(db.rows['offers'][0]['price_amount'], 5000)
