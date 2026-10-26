"""SparkBatchProcessor.finalize against stub handles: no Spark session, Delta or store."""
import sys
import threading
import unittest
from pathlib import Path

HERE = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(HERE),str(HERE.parent/'data_pipeline')]
from pipeline_v2.adapters.spark_batch import SparkBatchProcessor, deferred
from pipeline_v2.domain.contracts import Invalid


def processor(**calls):
    p = SparkBatchProcessor.__new__(SparkBatchProcessor)
    p._open = lambda:None
    p._cached = None
    p.completed = []
    p._complete_raw = lambda job,manifest,accepted:p.completed.append(accepted)
    p.store = type('Store',(),{'get':lambda self,kind,key:dict(counts=dict(accepted=3))})()
    p._handoff = dict(job='j',frames=[],**{name:deferred(call) for name,call in calls.items()})
    return p


class FinalizeTests(unittest.TestCase):
    job = dict(job_id='j',kind='CRAWL')

    def test_the_cache_append_and_raw_commit_overlap(self):
        started = {name:threading.Event() for name in ('normalized','commit')}
        def call(name, other):
            def run():
                started[name].set()
                # Each finishes only once the other has started: sequential calls would time out.
                if not started[other].wait(5):
                    raise AssertionError('not concurrent')
            return run
        p = processor(normalized=call('normalized','commit'),commit=call('commit','normalized'))
        p.finalize(self.job)
        self.assertEqual(p.completed,[])
        self.assertIsNone(p._handoff)

    def test_a_failed_cache_append_keeps_the_job_open_after_the_raw_commit_ran(self):
        ran = []
        def broken():
            raise Invalid('NORMALIZED_APPEND_FAILED')
        p = processor(normalized=broken,commit=lambda:ran.append('raw'))
        with self.assertRaisesRegex(Invalid,'NORMALIZED_APPEND_FAILED'):
            p.finalize(self.job)
        self.assertEqual(ran,['raw'])

    def test_a_failed_raw_commit_is_completed_from_the_source_copy(self):
        def broken():
            raise Invalid('RAW_COMMIT_FAILED')
        p = processor(normalized=lambda:None,commit=broken)
        p.finalize(self.job)
        self.assertEqual(p.completed,[3])


class CompactTests(unittest.TestCase):
    def processor(self, files):
        from contextlib import nullcontext
        p = SparkBatchProcessor.__new__(SparkBatchProcessor)
        p._open = lambda: None
        p.timing = type('Timing', (), {'measure': lambda self, *a, **k: nullcontext()})()
        compacted = []
        p.delta = type('Delta', (), {'files': lambda self, name: files[name],
            'compact': lambda self, name: compacted.append(name) or dict(files_removed=files[name])})()
        return p, compacted

    def test_only_tables_over_the_file_limit_are_compacted(self):
        p, compacted = self.processor(dict(input_index=30, normalized_history=90))
        done = p.compact(over=64)
        self.assertEqual(compacted, ['normalized_history'])
        self.assertEqual(done['input_index'], dict(skipped=True, files=30))

    def test_without_a_limit_every_table_is_compacted(self):
        p, compacted = self.processor(dict(input_index=1, normalized_history=1))
        p.compact()
        self.assertEqual(compacted, ['input_index', 'normalized_history'])


if __name__ == '__main__':
    unittest.main()
