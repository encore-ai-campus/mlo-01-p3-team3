import importlib.util
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from types import SimpleNamespace
from product_pipeline.files import read_json,atomic_json


def module():
    path=Path(__file__).resolve().parents[1]/'docs/server_setup/production_downstream_work.py'
    spec=importlib.util.spec_from_file_location('saved_bronze_downstream',path)
    result=importlib.util.module_from_spec(spec);spec.loader.exec_module(result);return result


class DownstreamTests(unittest.TestCase):
    def test_flow_resumes_only_after_successful_saved_data_publication(self):
        job=module()
        with patch.dict(job.os.environ,PIPELINE_RESUME_FLOW_AFTER_DOWNSTREAM='1'):
            with patch.object(job,'main',return_value=2),patch.object(job.os,'execv') as execute:
                self.assertEqual(job.run(),2);execute.assert_not_called()
            with patch.object(job,'main',return_value=0),patch.object(job.os,'execv') as execute:
                self.assertEqual(job.run(),0)
                self.assertEqual(execute.call_args.args[1][-1],str(job.ROOT/'continuous_work.py'))
    def test_starts_from_silver_and_reuses_completed_stages_on_resume(self):
        job=module();calls=[]
        with tempfile.TemporaryDirectory() as directory:
            job.ROOT=Path(directory)
            def run(command,**kwargs):
                stage=command[-1];calls.append(stage);cut=Path(kwargs['env']['PIPELINE_EXECUTION_ROOT'])
                atomic_json(cut/(stage+'-result.json'),dict(status='COMPLETED'))
                if stage=='silver':atomic_json(cut/'normalization-request.json',dict(input_range=dict(bronze_version=11)))
                return SimpleNamespace(returncode=0)
            with patch.object(job.subprocess,'run',side_effect=run):
                self.assertEqual(job.main(),0);self.assertEqual(job.main(),0)
            self.assertEqual(calls,['silver','gold','publish'])
            state=read_json(job.ROOT/'downstream-progress.json')
            self.assertEqual(state['bronze_version'],11);self.assertEqual(state['status'],'COMPLETED')

    def test_silver_failure_does_not_publish_or_invent_a_success_receipt(self):
        job=module();calls=[]
        with tempfile.TemporaryDirectory() as directory:
            job.ROOT=Path(directory)
            def run(command,**kwargs):calls.append(command[-1]);return SimpleNamespace(returncode=2)
            with patch.object(job.subprocess,'run',side_effect=run):self.assertEqual(job.main(),2)
            self.assertEqual(calls,['silver'])
            self.assertEqual(read_json(job.ROOT/'downstream-progress.json')['status'],'FAILED')


if __name__=='__main__':unittest.main()
