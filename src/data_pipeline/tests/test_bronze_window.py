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
    def test_next_callback_does_not_read_or_write_and_cannot_acknowledge(self):
        from unittest.mock import MagicMock,patch
        from test_bronze_lease import settings
        from product_pipeline.bronze.stream import start
        from product_pipeline.errors import PipelineError
        spark=MagicMock();frame=spark.readStream.format.return_value.options.return_value.load.return_value
        control=dict(limit=1,calls=1,boundary_batch_id=None);data=MagicMock()
        with patch('product_pipeline.bronze.stream.bind_checkpoint'),patch('product_pipeline.bronze.stream.check_kafka_cluster'),patch('product_pipeline.bronze.stream.DeltaSink'),patch('product_pipeline.bronze.stream.process_batch') as process:
            start(spark,settings(),object(),{'kafka.bootstrap.servers':'local:9092'},lease=MagicMock(),window_control=control)
            callback=frame.writeStream.foreachBatch.call_args.args[0]
            with self.assertRaises(PipelineError):callback(data,9)
            process.assert_not_called();data.select.assert_not_called()
        self.assertEqual(control['boundary_batch_id'],9)
    def test_boundary_failure_requires_engine_commit_and_no_further_io(self):
        from product_pipeline.bronze.cli import await_window
        class Boundary(Query):
            def awaitTermination(self,timeout=None):
                self.lastProgress=dict(batchId=8,numInputRows=100);self.isActive=False
                raise RuntimeError('intentional next callback boundary')
        result=await_window(Boundary([]),Lease(),1,dict(boundary_batch_id=9))
        self.assertFalse(result['source_drained']);self.assertEqual(result['last_committed_batch_id'],8)
        for boundary in (None,8,10):
            with self.assertRaises(RuntimeError):await_window(Boundary([]),Lease(),1,dict(boundary_batch_id=boundary))

    def test_boundary_without_durable_engine_progress_cannot_succeed(self):
        from product_pipeline.bronze.cli import await_window
        class Broken(Query):
            def awaitTermination(self,timeout=None):self.isActive=False;raise RuntimeError('no commit')
        with self.assertRaises(RuntimeError):await_window(Broken([]),Lease(),1,dict(boundary_batch_id=9))

    def test_late_shutdown_failure_without_boundary_is_not_hidden(self):
        from product_pipeline.bronze.cli import await_window
        class Broken(Query):
            def stop(self):self.isActive=False;raise RuntimeError('actual failure')
        with self.assertRaises(RuntimeError):await_window(Broken([dict(batchId=8,numInputRows=100)]),Lease(),1)
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
