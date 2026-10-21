from contextlib import redirect_stdout
import io
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from product_pipeline.files import atomic_json
from product_pipeline.silver.inputs import registration,validate_inputs
from product_pipeline.silver.cli import main
from product_pipeline.silver.config import SilverSettings
from product_pipeline.errors import PipelineError
from test_silver import RUN,inputs


class SilverCliTests(unittest.TestCase):
    def test_run_registration_is_explicit_and_fixture_cannot_be_used_in_live(self):
        fixture=dict(processing_run_id=RUN,stage='NORMALIZATION',status='FIXTURE',evidence_ref='fixture://test')
        self.assertEqual(registration(fixture,True),RUN)
        with self.assertRaises(PipelineError):registration(fixture)
        with self.assertRaises(PipelineError):registration(dict(fixture,status='REGISTERED'))
        self.assertEqual(registration(dict(fixture,status='REGISTERED',provider='MYSQL_PIPELINE_RUNS_EXPORT')),RUN)

    def test_malformed_input_hash_and_owner_rows_fail_closed(self):
        value=inputs();value['mappings']['rows'].append({})
        with self.assertRaises(PipelineError):validate_inputs(value)
        with self.assertRaises(PipelineError):validate_inputs(inputs(mappings=[{}]))
        with self.assertRaises(PipelineError):validate_inputs({})

    def test_path_overlap_and_invalid_limits_are_rejected(self):
        args=dict(environment_id='test',mongo_database='raw',bronze_path='file:///data/bronze',silver_root='file:///data/silver',checkpoint_path='file:///checkpoints/silver')
        for changed in (dict(silver_root='file:///data/bronze'),dict(max_records=0),dict(lease_database='raw')):
            with self.assertRaises(PipelineError):SilverSettings(**dict(args,**changed))

    def test_unregistered_run_prevents_any_network_or_spark_start(self):
        with tempfile.TemporaryDirectory() as d:
            root=Path(d)
            atomic_json(root/'config.json',dict(environment_id='test',mongo_database='raw',bronze_path='file:///data/bronze',silver_root='file:///data/silver',checkpoint_path='file:///checkpoints/silver'))
            atomic_json(root/'inputs.json',inputs())
            atomic_json(root/'registration.json',dict(processing_run_id=RUN,stage='NORMALIZATION',status='NOT_REGISTERED',evidence_ref='owner://pending'))
            output=io.StringIO()
            with patch('socket.socket',side_effect=AssertionError('must not connect')),redirect_stdout(output):
                code=main(['--config',str(root/'config.json'),'--bronze-version','0','--inputs',str(root/'inputs.json'),
                    '--registration',str(root/'registration.json'),'--state-dir',str(root/'logs')])
            self.assertEqual(code,2)
            self.assertIn('SILVER_RUN_REGISTRATION_REQUIRED',output.getvalue())


if __name__=='__main__':unittest.main()
