"""Protect dependency direction and structured execution across CLI/FLOW callers."""
import ast
from contextlib import redirect_stdout
import io
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from product_pipeline.errors import PipelineError
from product_pipeline.execution import StageResult


PACKAGE = Path(__file__).resolve().parents[1] / 'product_pipeline'


class LayerBoundaryTests(unittest.TestCase):
    def test_internal_modules_do_not_import_commands(self):
        for path in PACKAGE.rglob('*.py'):
            if path.name == '__main__.py':
                continue
            tree = ast.parse(path.read_text(encoding='utf-8'))
            for node in ast.walk(tree):
                if isinstance(node, ast.ImportFrom):
                    with self.subTest(file=str(path.relative_to(PACKAGE)), line=node.lineno):
                        self.assertNotIn('cli', (node.module or '').split('.'))

    def test_shared_storage_does_not_depend_on_stage_packages(self):
        for path in (PACKAGE / 'infrastructure').glob('*.py'):
            for node in ast.walk(ast.parse(path.read_text(encoding='utf-8'))):
                if isinstance(node, ast.ImportFrom):
                    self.assertFalse({'silver', 'gold'} & set((node.module or '').split('.')))

    def test_pure_publication_policy_has_no_database_or_workflow_dependency(self):
        tree = ast.parse((PACKAGE / 'publication/policy.py').read_text(encoding='utf-8'))
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom):
                self.assertFalse({'mysql', 'repository', 'runner', 'operations'} & set((node.module or '').split('.')))

    def test_stage_services_have_no_argument_parser_or_console_output(self):
        for stage in ('bronze', 'silver', 'gold'):
            tree = ast.parse((PACKAGE / stage / 'service.py').read_text(encoding='utf-8'))
            for node in ast.walk(tree):
                if isinstance(node, ast.Import):
                    self.assertNotIn('argparse', {alias.name for alias in node.names})
                if isinstance(node, ast.ImportFrom):
                    self.assertNotIn(node.module, ('console', 'cli'))
                if isinstance(node, ast.Call) and isinstance(node.func, ast.Name):
                    self.assertNotEqual(node.func.id, 'print')


class ExecutionBoundaryTests(unittest.TestCase):
    def test_failed_stage_preserves_redacted_error_and_worker_classification(self):
        with self.assertRaises(PipelineError) as raised:
            StageResult(2, {'error_code': 'GOLD_SHUTDOWN_UNCONFIRMED'}).verified()
        self.assertEqual(raised.exception.code, 'GOLD_SHUTDOWN_UNCONFIRMED')
        self.assertTrue(raised.exception.stage_error)
        with self.assertRaisesRegex(PipelineError, 'FLOW_COMMAND_RESULT_UNCONFIRMED'):
            StageResult(0, {'status': 'RUNNING'}).verified()

    def test_direct_bronze_call_returns_failure_without_printing(self):
        from product_pipeline.bronze.service import Request, run
        output = io.StringIO()
        with tempfile.TemporaryDirectory() as root, redirect_stdout(output):
            result = run(Request(config=str(Path(root) / 'missing.json')))
        self.assertEqual(result.code, 2)
        self.assertEqual(result.report['result'], 'failed')
        self.assertEqual(output.getvalue(), '')

    def test_cli_and_flow_share_the_bronze_service(self):
        from product_pipeline.bronze.cli import main
        from product_pipeline.operations.flow_runtime import perform
        from types import SimpleNamespace
        result = StageResult(0, {'result': 'completed', 'source_drained': True})
        with patch('product_pipeline.bronze.cli.run', return_value=result) as run, redirect_stdout(io.StringIO()):
            self.assertEqual(main(['--config', 'bronze.json', '--available-now']), 0)
        self.assertTrue(run.call_args.args[0].available_now)
        plan = {'files': {}}
        bronze = SimpleNamespace(bronze_path='raw', events_path='events', quarantine_path='quarantine', spark_options_env='OPTIONS')
        spark = SimpleNamespace(stop=lambda: None)
        with tempfile.TemporaryDirectory() as root, \
                patch('product_pipeline.operations.flow.load', return_value=plan), \
                patch('product_pipeline.operations.flow_runtime.settings', return_value=({'bronze_config': 'bronze.json'}, None, bronze, None, None)), \
                patch('product_pipeline.bronze.service.run', return_value=result) as run, \
                patch('product_pipeline.operations.flow_runtime.spark_session', return_value=spark), \
                patch('product_pipeline.operations.flow_runtime.versions', return_value={'raw': 4}):
            output = perform({'root': root, 'stage': 'BRONZE', 'flow': plan, 'outputs': {}})
        self.assertTrue(run.call_args.args[0].available_now)
        self.assertEqual(output['versions'], {'raw': 4})

    def test_gold_storage_uses_real_settings_and_lease(self):
        from product_pipeline.gold.storage import DeltaSink
        from types import SimpleNamespace
        settings = SimpleNamespace(gold_root='s3a://lake/gold')
        lease, spark = object(), object()
        with patch('product_pipeline.infrastructure.delta.AppendOnlyDeltaSink.__init__', return_value=None) as init:
            DeltaSink(spark, settings, lease=lease)
        init.assert_called_once_with(spark, settings, lease=lease, root=settings.gold_root)
