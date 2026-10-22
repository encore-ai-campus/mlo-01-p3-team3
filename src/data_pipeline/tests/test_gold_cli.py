import contextlib
import io
from pathlib import Path
import tempfile
import unittest
from dataclasses import asdict
from unittest.mock import patch

from test_gold import side,VERSIONS,RUN
from product_pipeline.files import atomic_json,read_json
from product_pipeline.errors import PipelineError
from product_pipeline.gold.config import GoldSettings
from product_pipeline.gold.cli import main


class GoldCliTests(unittest.TestCase):
    def test_paths_limits_and_overlaps_rejected(self):
        with self.assertRaises(PipelineError):GoldSettings('test','file:///lake','file:///lake/gold','file:///checkpoint')
        with self.assertRaises(PipelineError):GoldSettings('test','file:///silver','file:///gold','file:///checkpoint',max_product_rows=0)
        with self.assertRaises(PipelineError):GoldSettings('test','relative','file:///gold','file:///checkpoint')

    def execute(self,root,registration,versions=None,rehearse=False):
        root=Path(root)
        settings=GoldSettings('test','file:///silver','file:///gold','file:///checkpoint')
        atomic_json(root/'config.json',asdict(settings));atomic_json(root/'inputs.json',side())
        atomic_json(root/'versions.json',VERSIONS if versions is None else versions)
        atomic_json(root/'registration.json',registration)
        argv=['--config',str(root/'config.json'),'--inputs',str(root/'inputs.json'),'--versions',str(root/'versions.json'),
              '--registration',str(root/'registration.json'),'--state-dir',str(root/'logs'),'--as-of','2026-10-17T00:00:00Z']
        if rehearse:argv+=['--rehearse']
        with contextlib.redirect_stdout(io.StringIO()):code=main(argv)
        return code,read_json(next((root/'logs'/'runs').glob('*.json')))

    def test_unregistered_or_fixture_operational_run_blocks_before_spark(self):
        with tempfile.TemporaryDirectory() as d,patch('product_pipeline.gold.service._env',side_effect=AssertionError('must not load runtime')):
            code,report=self.execute(d,dict(processing_run_id=RUN,stage='GOLD_BUILD',status='FIXTURE',evidence_ref='fixture'))
            self.assertEqual(code,2);self.assertEqual(report['error_code'],'GOLD_RUN_REGISTRATION_REQUIRED')

    def test_incomplete_version_vector_is_not_implicitly_latest(self):
        with tempfile.TemporaryDirectory() as d:
            code,report=self.execute(d,{},versions={'product_observations':1})
            self.assertEqual(code,2);self.assertEqual(report['error_code'],'GOLD_ALL_FIXED_SILVER_VERSIONS_REQUIRED')
