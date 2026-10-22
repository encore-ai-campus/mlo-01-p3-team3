import copy
import tempfile
import unittest
from helpers import fixture
from product_pipeline.files import rows
from product_pipeline.contracts import digest, envelope, observation_key, GALLERY_FIELDS
from product_pipeline.errors import PipelineError
from product_pipeline.catalog_input import bind_normalized, CatalogInputs


class CatalogTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        export, _ = fixture(self.tmp.name)
        raw = next(rows(export / 'product_raw.jsonl'))
        self.normal = next(rows(export / 'products.jsonl'))
        images = [dict(i, acquisition_state='UNAVAILABLE') for i in self.normal['images']]
        row = dict(raw=raw, images=images, lineage={}, observation_key=observation_key(raw, images))
        self.doc = envelope(row, '0123456789abcdef01234567', 'snapshot', 'images')
        self.normal['_id'] = 'normalized-11'

    def tearDown(self):
        self.tmp.cleanup()

    def test_binding_preserves_normalized_document_without_changing_raw_or_approving(self):
        before = copy.deepcopy(self.doc)
        proof = bind_normalized(self.doc, self.normal)
        self.assertEqual(self.doc, before)
        self.assertEqual(proof['status'], 'VERIFIED')
        self.assertEqual(proof['normalized_document'], self.normal)
        self.assertEqual(proof['normalized_hash'], digest(self.normal))
        self.assertEqual(proof['raw_document_id'], self.doc['_id'])
        self.assertNotIn('APPROVED', str(proof))

    def test_changed_identity_run_hash_date_or_gallery_is_rejected(self):
        for field, value in [('product_id', '12'), ('job_id', 'other'),
                             ('raw_payload_hash', 'a' * 64), ('collected_at', '2020-01-01T00:00:00Z'),
                             ('categories', []), ('images', [])]:
            with self.subTest(field=field):
                bad = dict(self.normal, **{field: value})
                with self.assertRaises(PipelineError):
                    bind_normalized(self.doc, bad)

    def test_source_query_is_bound_to_observation_not_latest_product(self):
        normal = self.normal
        class Cursor(list):
            def limit(self, n): return self[:n]
        class Collection:
            def find(self, query):
                self.query = query
                return Cursor([normal])
        collection = Collection()
        proof = CatalogInputs(collection).get(self.doc)
        self.assertEqual(collection.query['job_id'], normal['job_id'])
        self.assertEqual(collection.query['raw_payload_hash'], normal['raw_payload_hash'])
        self.assertEqual(proof['normalized_hash'], digest(normal))

    def test_missing_or_duplicate_observation_is_not_silently_selected(self):
        for values in ([], [self.normal, self.normal]):
            class Cursor(list):
                def limit(self, n): return self[:n]
            class Collection:
                def find(self, query): return Cursor(values)
            with self.assertRaises(PipelineError):
                CatalogInputs(Collection()).get(self.doc)

    def test_null_gallery_is_allowed_only_for_an_empty_raw_gallery_and_preserves_original(self):
        normal=dict(self.normal,images=None);doc=dict(self.doc,image_refs=[])
        proof=bind_normalized(doc,normal)
        self.assertIsNone(proof['normalized_document']['images'])
        self.assertEqual(proof['normalized_hash'],digest(normal))
        with self.assertRaises(PipelineError):bind_normalized(self.doc,normal)

    def test_malformed_gallery_is_an_explicit_contract_failure(self):
        for images in ('invalid',{},[None]):
            with self.assertRaisesRegex(PipelineError,'CATALOG_GALLERY_INVALID'):
                bind_normalized(self.doc,dict(self.normal,images=images))
