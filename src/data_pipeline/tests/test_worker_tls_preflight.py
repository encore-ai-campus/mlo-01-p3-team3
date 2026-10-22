import unittest
from types import SimpleNamespace
from unittest.mock import patch
from run_server_pipeline import worker_tls_preflight


class WorkerTLSPreflightTests(unittest.TestCase):
    def test_all_four_nodes_are_required_without_reading_private_contents(self):
        with patch('run_server_pipeline.subprocess.run',return_value=SimpleNamespace(returncode=0,stdout='KAFKA_TLS_FILES_READABLE\n'*4)) as run:
            worker_tls_preflight()
        self.assertIn('os.access',run.call_args.args[0][-1])
        self.assertNotIn('read_bytes',run.call_args.args[0][-1])
    def test_unreachable_or_missing_worker_blocks_launch(self):
        for result in [SimpleNamespace(returncode=0,stdout='KAFKA_TLS_FILES_READABLE\n'*3),SimpleNamespace(returncode=2,stdout='KAFKA_TLS_FILES_READABLE\n'*4)]:
            with patch('run_server_pipeline.subprocess.run',return_value=result),self.assertRaises(RuntimeError):worker_tls_preflight()
