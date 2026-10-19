import unittest


class Lease:
    def check(self):pass


class Query:
    def __init__(self,progress):self.steps=list(progress);self.lastProgress=None;self.isActive=True;self.stops=0;self.waits=0
    def awaitTermination(self,timeout=None):
        if timeout is None:
            self.isActive=False;return True
        self.waits+=1
        if not self.steps:self.isActive=False;return True
        self.lastProgress=self.steps.pop(0)
        return False
    def stop(self):self.stops+=1;self.isActive=False


class WindowTests(unittest.TestCase):
    def test_result_cannot_overwrite_the_input_configuration(self):
        import tempfile
        from pathlib import Path
        from product_pipeline.bronze.cli import main
        with tempfile.TemporaryDirectory() as directory:
            config=Path(directory)/'config.json';config.write_text('{"preserve":true}')
            self.assertEqual(main(['--config',str(config),'--result-file',str(config)]),2)
            self.assertEqual(config.read_text(),'{"preserve":true}')
    def test_waits_for_engine_commit_instead_of_callback_output_then_stops(self):
        from product_pipeline.bronze.cli import await_window
        q=Query([None,dict(batchId=8,numInputRows=100)])
        result=await_window(q,Lease(),1)
        self.assertEqual(q.waits,2);self.assertEqual(q.stops,1)
        self.assertFalse(result['source_drained']);self.assertEqual(result['last_committed_batch_id'],8)

    def test_repeated_progress_is_not_counted_twice_and_empty_finish_is_drained(self):
        from product_pipeline.bronze.cli import await_window
        q=Query([dict(batchId=8,numInputRows=100),dict(batchId=8,numInputRows=100),dict(batchId=9,numInputRows=100)])
        self.assertEqual(await_window(q,Lease(),2)['committed_batches'],2);self.assertEqual(q.waits,3)
        q=Query([]);self.assertTrue(await_window(q,Lease(),1)['source_drained']);self.assertEqual(q.stops,0)

    def test_query_failure_is_propagated_and_never_returns_a_successful_window(self):
        from product_pipeline.bronze.cli import await_window
        class Broken(Query):
            def awaitTermination(self,timeout=None):raise OSError('injected checkpoint commit failure')
        with self.assertRaises(OSError):await_window(Broken([]),Lease(),1)


if __name__=='__main__':unittest.main()
