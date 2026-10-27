"""Gold image slot selection without Spark: equal-time observations of one slot."""
import sys
import unittest
from pathlib import Path

HERE = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(HERE),str(HERE.parent/'data_pipeline')]
from pipeline_v2.adapters.spark_product import _images

AT = '2026-10-15T01:00:00.000Z'


def stored(**changes):
    value = dict(image_slot='main', order=0, source_url='https://img.example/a.jpg', raw_collected_at=AT,
                 raw_document_id='a', available=True, sha256='s1', bucket='b', object_key='k1',
                 bytes=10, width=1, height=1, format='jpeg')
    value.update(changes)
    return value


def unacquired(**changes):
    # An observation that listed the image but never downloaded it.
    value = dict(raw_document_id='b', available=False, sha256=None, bucket=None, object_key=None,
                 bytes=None, width=None, height=None, format=None)
    value.update(changes)
    return stored(**value)


def slots(*values):
    return [v['image_slot'] for v in _images([dict(direct_images=list(values))])]


class EqualTimeImageTests(unittest.TestCase):
    def test_a_stored_image_is_kept_next_to_its_never_acquired_twin(self):
        self.assertEqual(slots(stored(), unacquired()), ['main'])
        self.assertEqual(slots(unacquired(), stored()), ['main'])

    def test_a_never_acquired_ref_of_another_url_still_conflicts(self):
        self.assertEqual(slots(stored(), unacquired(source_url='https://img.example/other.jpg')), [])

    def test_two_different_stored_images_still_conflict(self):
        self.assertEqual(slots(stored(), stored(raw_document_id='b', sha256='s2', object_key='k2')), [])

    def test_a_slot_only_never_acquired_has_no_image(self):
        self.assertEqual(slots(unacquired(), unacquired(raw_document_id='c')), [])

    def test_a_newer_never_acquired_observation_still_decides_the_slot(self):
        # Only equal-time twins are merged; a later observation is not overridden by older evidence.
        self.assertEqual(slots(stored(), unacquired(raw_collected_at='2026-10-16T01:00:00.000Z')), [])


class EqualTimePriceTests(unittest.TestCase):
    """Two observations of one price differ only in the condition's provenance keys."""

    def offers(self, *amounts_and_keys):
        import json
        from pipeline_v2.adapters.spark_product import _offers
        from pipeline_v2.domain.contracts import canonical
        condition = dict(condition_schema_version=1, price_scope='SELLER', unresolved_evidence_key=None,
                         option=dict(state='UNKNOWN', value=dict(option_evidence_key=None, option_key=None)))
        parsed = []
        for index, (amount, key) in enumerate(amounts_and_keys):
            raw = 'r%d' % index
            value = dict(condition, unresolved_evidence_key=key,
                         option=dict(state='UNKNOWN', value=dict(option_evidence_key=key, option_key=None)))
            parsed.append(dict(tables=dict(
                normalized_offers=[dict(offer_key='o', observed_at=AT, raw_document_id=raw)],
                price_observations=[dict(offer_key='o', observed_at=AT, raw_document_id=raw, amount=amount,
                                         currency='KRW', numeric_status='CONFIRMED', condition_json=canonical(value))])))
        return [(o['price']['amount'], o['price']['raw_document_id'], json.loads(o['price']['condition_json'])['unresolved_evidence_key'])
                for o in _offers(parsed, 'danawa:1')]

    def test_the_same_price_of_two_observations_is_one_offer_from_the_smaller_raw_id(self):
        self.assertEqual(self.offers(('100', 'k1'), ('100', 'k2')), [('100', 'r0', 'k1')])

    def test_different_amounts_at_the_same_time_still_conflict(self):
        self.assertEqual(self.offers(('100', 'k1'), ('200', 'k2')), [])

    def test_one_document_listing_a_price_twice_keeps_the_smallest_price_key(self):
        from pipeline_v2.adapters.spark_product import _offers
        from pipeline_v2.domain.contracts import canonical
        rows = [dict(offer_key='o', observed_at=AT, raw_document_id='r0', amount='100', currency='KRW',
                     numeric_status='CONFIRMED', price_observation_key=key,
                     condition_json=canonical(dict(unresolved_evidence_key=key))) for key in ('p2', 'p1')]
        rows.append(dict(rows[0], numeric_status='INVALID_ZERO', amount='0', price_observation_key='p0'))
        parsed = [dict(tables=dict(normalized_offers=[dict(offer_key='o', observed_at=AT, raw_document_id='r0')],
                                   price_observations=rows))]
        self.assertEqual([o['price']['price_observation_key'] for o in _offers(parsed, 'danawa:1')], ['p1'])

    def test_only_the_provenance_keys_are_blanked(self):
        from pipeline_v2.domain.transform import condition_identity
        a = '{"option":{"state":"UNKNOWN","value":{"option_evidence_key":"aa","option_key":null}},"unresolved_evidence_key":"bb"}'
        b = '{"option":{"state":"UNKNOWN","value":{"option_evidence_key":"cc","option_key":null}},"unresolved_evidence_key":null}'
        self.assertEqual(condition_identity(a), condition_identity(b))
        self.assertNotEqual(condition_identity(a), condition_identity(a.replace('UNKNOWN', 'CONFIRMED')))


if __name__ == '__main__':
    unittest.main()
