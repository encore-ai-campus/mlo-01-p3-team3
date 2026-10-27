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


if __name__ == '__main__':
    unittest.main()
