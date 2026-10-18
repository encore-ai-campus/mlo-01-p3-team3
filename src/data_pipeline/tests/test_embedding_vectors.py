from datetime import datetime, timezone
import math
import unittest

from product_pipeline.embedding_vectors import row
from product_pipeline.errors import PipelineError


class EmbeddingVectorTests(unittest.TestCase):
    def value(self):
        return dict(image_checksum='a'*64, preprocessed_checksum='b'*64, model_version='gemini-embedding-2',
                    preprocessing_version='exif-rgb-fit1024-png-l2-v1', dimension=768,
                    vector=[1.0] + [0.0]*767, created_at=datetime.now(timezone.utc))

    def test_identity_is_image_and_contract_not_product_or_ingestion_time(self):
        a = row(self.value())
        b = row({**self.value(), 'asset_id': 'c'*32})
        self.assertEqual(a['embedding_id'], b['embedding_id'])
        self.assertEqual(a['record_hash'], b['record_hash'])

    def test_model_or_preprocessing_change_uses_different_identity(self):
        value = self.value()
        self.assertNotEqual(row(value)['embedding_id'], row({**value, 'model_version': 'other-model'})['embedding_id'])
        self.assertNotEqual(row(value)['embedding_id'], row({**value, 'preprocessing_version': 'other-preproc'})['embedding_id'])

    def test_float32_delta_round_trip_preserves_record_hash(self):
        value = self.value()
        value['vector'] = [1/math.sqrt(768)]*768
        converted = row(value)
        self.assertEqual(row(converted)['record_hash'], converted['record_hash'])

    def test_corrupt_dimension_nan_zero_or_nonnormalized_vectors_fail(self):
        for vector in ([0.0]*768, [float('nan')]*768, [1.0]*767, [2.0] + [0.0]*767):
            with self.subTest(vector=vector[:2]), self.assertRaises(PipelineError):
                row({**self.value(), 'vector': vector})

    def test_existing_id_or_hash_cannot_be_forged(self):
        for extra in ({'embedding_id': '0'*32}, {'record_hash': '0'*64}):
            with self.assertRaisesRegex(PipelineError, 'ID_OR_HASH_CONFLICT'):
                row({**self.value(), **extra})
