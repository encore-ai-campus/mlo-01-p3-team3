import unittest
from product_pipeline.publication.runner import attribute_projection
from product_pipeline.errors import Conflict


class AttributeReplayTests(unittest.TestCase):
    def row(self, value, **changes):
        return dict(attribute_name='DIMENSION',measurement_target=None,component=None,option_key=None,
            attribute_status='CONFIRMED',observed_at='2026-10-15T12:39:06.000Z',value_type='MEASUREMENT',
            value_text=value+'cm',value_decimal=str(int(value)*10),codes=[],unit='mm',**changes)

    def test_same_multivalue_measurements_replay_without_cross_comparison(self):
        previous=[self.row('71'),self.row('52')]
        self.assertEqual(attribute_projection(previous,list(reversed(previous))),list(reversed(previous)))

    def test_changed_multivalue_set_at_equal_time_remains_conflict(self):
        with self.assertRaises(Conflict):
            attribute_projection([self.row('71'),self.row('52')],[self.row('71'),self.row('53')])

    def test_single_value_change_still_conflicts(self):
        with self.assertRaises(Conflict):attribute_projection([self.row('71')],[self.row('52')])
