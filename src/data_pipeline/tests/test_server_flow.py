import tempfile,unittest
from pathlib import Path
from product_pipeline.operations.server_flow import decision,worker,launch
from product_pipeline.files import atomic_json
from product_pipeline.errors import PipelineError


class ServerFlowTests(unittest.TestCase):
    def test_timer_never_restarts_failed_or_unconfirmed_run(self):
        with tempfile.TemporaryDirectory() as d:
            r=Path(d)
            self.assertEqual(decision(r),'START')
            for status in ('RUNNING','INTERRUPTED','RECOVERY_REQUIRED','PAUSED_AFTER_BATCH','UNKNOWN'):
                atomic_json(r/'flow-progress.json',dict(status=status))
                self.assertEqual(decision(r),'RECOVERY_REQUIRED')
            atomic_json(r/'flow-progress.json',dict(status='COMPLETED'))
            self.assertEqual(decision(r),'COMPLETED')
            atomic_json(r/'flow-progress.json',dict(status='READY_TO_START'))
            self.assertEqual(decision(r),'START')

    def test_active_worker_is_not_spawned_again(self):
        with tempfile.TemporaryDirectory() as d:
            r=Path(d)
            # Current non-Linux tests do not depend on a real /proc worker.
            from unittest.mock import patch
            with patch('product_pipeline.operations.server_flow.worker',return_value=42):
                result=launch(r,r,'/python','flow',automatic=True,popen=lambda *a,**k:self.fail('duplicate spawn'))
            self.assertEqual(result['status'],'ALREADY_RUNNING')
            self.assertFalse(result['started'])

    def test_pause_and_manual_stop_are_preserved(self):
        with tempfile.TemporaryDirectory() as d:
            r=Path(d)
            for name in ('pause-after-window.json','manual-stop.json'):
                atomic_json(r/name,{})
                self.assertEqual(decision(r),'PAUSED')
                self.assertEqual(decision(r,pid=42),'PAUSE_REQUESTED' if name=='pause-after-window.json' else 'ALREADY_RUNNING')
                (r/name).unlink()

    def test_pid_reuse_and_exact_process_identity(self):
        with tempfile.TemporaryDirectory() as d:
            r=Path(d)/'root';r.mkdir();proc=Path(d)/'proc';p=proc/'42';p.mkdir(parents=True)
            (r/'worker.pid').write_text('42')
            (p/'cmdline').write_bytes(b'/python\0'+str(r/'continuous_work.py').encode()+b'\0')
            self.assertEqual(worker(r,proc=proc),42)
            (p/'cmdline').write_bytes(b'/python\0/unrelated/continuous_work.py\0')
            with self.assertRaises(PipelineError):worker(r,proc=proc)

    def test_manual_launch_uses_same_lock_and_writes_actual_pid(self):
        from unittest.mock import patch,Mock
        from product_pipeline.files import lock
        with tempfile.TemporaryDirectory() as d:
            r=Path(d);child=Mock(pid=424242);spawn=Mock(return_value=child)
            runtime={'driver_host':'192.168.0.121','executor_python':'/python'}
            with patch('product_pipeline.runtime_overrides.load',return_value=(100,'/runtime.json',runtime)):
                with lock(r/'controller.lock'):
                    with self.assertRaises(PipelineError):launch(r,r,'/python','flow',popen=spawn)
                spawn.assert_not_called()
                result=launch(r,r,'/python','flow',popen=spawn)
            self.assertEqual(result['pid'],424242)
            self.assertEqual((r/'worker.pid').read_text(),'424242')
            self.assertEqual(spawn.call_args.args[0],['/python',str(r/'continuous_work.py')])
            self.assertEqual(spawn.call_args.kwargs['env']['PIPELINE_WINDOW_RECORDS'],'100')
            self.assertEqual(spawn.call_args.kwargs['env']['PIPELINE_SHARED_BRONZE_SESSION'],'1')


if __name__=='__main__':unittest.main()
