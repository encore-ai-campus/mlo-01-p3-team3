import tempfile
from pathlib import Path
import unittest
from unittest.mock import patch
import test_engine
from pipeline_v2.adapters.timing import Timing
from pipeline_v2.adapters.files import read
from pipeline_v2.domain.contracts import Invalid


class TimingTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.timing = Timing(self.temp.name)

    def tearDown(self):
        self.temp.cleanup()

    def test_inclusive_and_exclusive_times_are_not_double_counted(self):
        with patch('pipeline_v2.adapters.timing.time.perf_counter_ns',side_effect=[0,10_000_000,20_000_000,50_000_000]):
            with self.timing.measure('parent','batch'):
                with self.timing.measure('child','batch'):
                    pass
        report = self.timing.summary('batch')
        self.assertEqual(report['stages']['parent']['total_ms'],50)
        self.assertEqual(report['stages']['parent']['exclusive_ms'],40)
        self.assertEqual(report['stages']['child']['total_ms'],10)

    def test_many_fast_spans_are_batched_without_per_event_fsync(self):
        for _ in range(100):
            with self.timing.measure('sql','batch'):
                pass
        self.assertEqual(len(list(Path(self.temp.name).rglob('*.json'))),0)
        self.timing.flush()
        self.assertEqual(len(list(Path(self.temp.name).rglob('*.json'))),1)
        report = self.timing.summary('batch',persist=False)
        self.assertEqual(report['event_count'],100)
        self.assertIsNotNone(report['stages']['sql']['p95_ms'])
        self.assertFalse(any(p.name == 'summary.json' for p in Path(self.temp.name).rglob('*')))

    def test_source_iteration_records_one_metric_and_excludes_consumer_time(self):
        with patch('pipeline_v2.adapters.timing.time.perf_counter_ns',side_effect=[0,1_000_000,100_000_000,101_000_000,200_000_000,201_000_000]):
            self.assertEqual(list(self.timing.iterate('source','batch',[1,2])),[1,2])
        report = self.timing.summary('batch')
        self.assertEqual(report['stages']['source']['events'],1)
        self.assertEqual(report['stages']['source']['total_ms'],3)
        self.assertEqual(report['stages']['source']['counts']['items'],2)

    def test_failure_flushes_without_exception_message(self):
        with self.assertRaises(ValueError):
            with self.timing.measure('failed','batch'):
                raise ValueError('secret source URI')
        files = list(Path(self.temp.name).rglob('*.json'))
        self.assertEqual(len(files),1)
        self.assertNotIn('secret',files[0].read_text())
        report = self.timing.summary('batch')
        self.assertEqual(report['stages']['failed']['errors'],1)
        self.assertIsNone(report['stages']['failed']['successful_p95_ms'])

    def test_boundary_is_idempotent_and_rejects_negative_time(self):
        self.timing.boundary('e2e','batch','2026-10-16T00:00:00Z','2026-10-16T00:01:00Z',identity='one')
        self.timing.boundary('e2e','batch','2026-10-16T00:00:00Z','2026-10-16T00:01:00Z',identity='one')
        report = self.timing.summary('batch')
        self.assertEqual(report['stages']['e2e']['events'],1)
        self.assertEqual(report['stages']['e2e']['p95_ms'],60000)
        with self.assertRaisesRegex(Invalid,'BOUNDARY_ORDER'):
            self.timing.boundary('e2e','batch','2026-10-16T00:01:00Z','2026-10-16T00:00:00Z',identity='bad')
