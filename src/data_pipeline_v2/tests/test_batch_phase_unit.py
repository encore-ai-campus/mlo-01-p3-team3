"""BatchRunner.work phase routing against a stub processor: no Spark, Delta, store or files."""
import sys
import unittest
from contextlib import nullcontext
from pathlib import Path
from types import SimpleNamespace

HERE = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(HERE),str(HERE.parent/'data_pipeline')]
from pipeline_v2.application.batch import BatchRunner, prefetched


class Processor:
    def __init__(self):
        self.calls = []

    def complete_raw(self, job):
        self.calls.append(('complete_raw',job['job_id']))


def runner(status):
    processor = Processor()
    timing = SimpleNamespace(measure=lambda *a,**k:nullcontext(),flush=lambda:None)
    run = BatchRunner(None,processor,None,timing,None,None,None)
    job = dict(job_id='j',crawl_batch_id='b',status=status)
    run._job = lambda key:dict(job)
    run._process = lambda key:dict(job,status='GOLD_COMMITTED')
    run.status = lambda key:None
    return run, processor


class WorkPhaseTests(unittest.TestCase):
    def test_a_job_processed_by_this_call_leaves_its_raw_to_publication(self):
        run, processor = runner('QUEUED')
        run.work('j','process')
        self.assertEqual(processor.calls,[])

    def test_an_already_processed_job_completes_its_raw_on_a_process_run(self):
        run, processor = runner('GOLD_COMMITTED')
        run.work('j','process')
        self.assertEqual(processor.calls,[('complete_raw','j')])


class ValidateEventPayloadTests(unittest.TestCase):
    def setUp(self):
        import copy
        import tempfile
        import test_engine as fixtures
        from pipeline_v2.domain.batch import event_for_item, wire_event
        self.temp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        item = fixtures.inputs(Path(self.temp.name), products=1, observations=1)[0]
        event = event_for_item(item, 'crawl-unit', storage_allowed=True, quarantine_allowed=True)
        self.wire, self.copy = wire_event(event, item), copy.deepcopy

    def tearDown(self):
        self.temp.cleanup()

    def test_a_carried_payload_is_checked_against_its_hash(self):
        from pipeline_v2.domain.batch import validate_event
        from pipeline_v2.domain.contracts import canonical, Invalid
        validate_event(self.wire)
        # The caller's encoding of the same payload object gives the same verdict.
        validate_event(self.wire, payload_text=canonical(self.wire['payload']))
        changed = self.copy(self.wire)
        changed['payload']['document']['source_record_key'] = 'changed'
        with self.assertRaisesRegex(Invalid, 'BATCH_EVENT_PAYLOAD_CHANGED'):
            validate_event(changed)
        # A text that is not this payload's encoding is caught as well.
        with self.assertRaisesRegex(Invalid, 'BATCH_EVENT_PAYLOAD_CHANGED'):
            validate_event(self.wire, payload_text=canonical(changed['payload']))


class PrefetchedTests(unittest.TestCase):
    def test_the_next_item_is_produced_while_the_current_one_is_consumed(self):
        import threading
        produced = []
        ahead = threading.Event()
        def values():
            for n in range(3):
                produced.append(n)
                if n == 1:
                    ahead.set()
                yield n
        seen = []
        for value in prefetched(values()):
            if value == 0:
                # Item 1 is produced while item 0 is still being consumed.
                self.assertTrue(ahead.wait(5))
            seen.append(value)
        self.assertEqual(seen, [0, 1, 2])

    def test_a_producer_failure_is_raised_at_its_place(self):
        def values():
            yield 1
            raise ValueError('lake read failed')
        seen = []
        with self.assertRaisesRegex(ValueError, 'lake read failed'):
            for value in prefetched(values()):
                seen.append(value)
        self.assertEqual(seen, [1])

    def test_a_consumer_that_stops_early_closes_the_producer(self):
        import threading
        closed = threading.Event()
        def values():
            try:
                for n in range(100):
                    yield n
            finally:
                closed.set()
        for value in prefetched(values()):
            if value == 1:
                break
        self.assertTrue(closed.wait(5))


if __name__ == '__main__':
    unittest.main()
