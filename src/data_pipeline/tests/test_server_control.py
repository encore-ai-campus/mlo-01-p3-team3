import unittest
from unittest.mock import patch
import run_server_pipeline as control


class ServerControlTests(unittest.TestCase):
    @patch.object(control.time,'sleep')
    @patch.object(control,'call')
    def test_failure_does_not_start_the_next_stage(self,call,sleep):
        call.side_effect=[{},dict(worker_alive=False,**{'bronze-failure.json':{'at':'2026-10-19T01:00:00Z','error_code':'FAILED'}})]
        with patch('builtins.print'):self.assertEqual(control.main(['all']),2)
        self.assertEqual(call.call_count,2)

    @patch.object(control.time,'sleep')
    @patch.object(control,'call')
    def test_old_success_cannot_hide_current_worker_failure(self,call,sleep):
        call.side_effect=[{},dict(worker_alive=False,attempt_started_at='2026-10-19T02:00:00Z',
            **{'publish-result.json':{'finished_at':'2026-10-19T01:00:00Z','status':'COMPLETED'}})]
        with patch('builtins.print'):self.assertEqual(control.main(['publish']),2)

    @patch.object(control.time,'sleep')
    @patch.object(control,'call')
    def test_same_attempt_success_is_reported(self,call,sleep):
        call.side_effect=[{},dict(worker_alive=False,attempt_started_at='2026-10-19T01:00:00Z',
            **{'publish-result.json':{'finished_at':'2026-10-19T02:00:00Z','status':'COMPLETED'}})]
        with patch('builtins.print'):self.assertEqual(control.main(['publish']),0)
