"""BatchRunner.work phase routing against a stub processor: no Spark, Delta, store or files."""
import sys
import unittest
from contextlib import nullcontext
from pathlib import Path
from types import SimpleNamespace

HERE = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(HERE),str(HERE.parent/'data_pipeline')]
from pipeline_v2.application.batch import BatchRunner


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


if __name__ == '__main__':
    unittest.main()
