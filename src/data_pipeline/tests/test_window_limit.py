import unittest
from product_pipeline.windows import record_limit
from product_pipeline.errors import PipelineError


class WindowLimitTests(unittest.TestCase):
    def test_bounded_runtime_window(self):
        self.assertEqual(record_limit(),100)
        self.assertEqual(record_limit('10'),10)
        for bad in ('0','101','-1','1.5','bad',True,None):
            with self.subTest(value=bad),self.assertRaises(PipelineError):record_limit(bad)
