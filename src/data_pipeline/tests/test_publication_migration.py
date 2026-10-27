from contextlib import redirect_stdout
import hashlib
import io
import json
from pathlib import Path
import shutil
import tempfile
import unittest
from unittest.mock import patch

from product_pipeline.errors import PipelineError
from product_pipeline.publication.cli import main
from product_pipeline.publication.migration import verify_bundle


class MigrationBundleTests(unittest.TestCase):
    source = Path(__file__).resolve().parents[1] / 'sql/migrations'

    def test_bundle_and_cli_are_offline_and_read_only(self):
        with patch('socket.socket', side_effect=AssertionError('network forbidden')), \
                patch('product_pipeline.publication.cli.MySQL', side_effect=AssertionError('DB forbidden')), \
                redirect_stdout(io.StringIO()) as output:
            self.assertEqual(main(['migration-check', '--directory', str(self.source)]), 0)
        report = json.loads(output.getvalue())
        self.assertEqual((report['columns'], report['foreign_keys']), (160, 9))
        self.assertFalse(report['ddl_applied'])

    def copy_bundle(self, directory):
        target = Path(directory) / 'migrations'
        shutil.copytree(self.source, target)
        return target

    def test_owner_inputs_use_shared_locks_without_publisher_update_privileges(self):
        from product_pipeline.publication.mysql import MySQL
        db = object.__new__(MySQL)
        db.one = lambda sql, args: (sql, args)
        for table in ('categories', 'pipeline_publication_guards'):
            key = {'id': 'a' * 32}
            self.assertTrue(db.get(table, key)[0].endswith(' FOR SHARE'))
            self.assertNotIn(' FOR ', db.get(table, key, locking=False)[0])
        self.assertTrue(db.get('products', {'id': 'a' * 32})[0].endswith(' FOR UPDATE'))

    def test_changed_sql_and_wrong_contract_block(self):
        with tempfile.TemporaryDirectory() as d:
            target = self.copy_bundle(d)
            path = target / 'feature25_apply.sql'
            original = path.read_text(encoding='utf-8-sig')
            path.write_text(original + '\n-- changed\n', encoding='utf-8')
            with self.assertRaisesRegex(PipelineError, 'MYSQL_MIGRATION_FILE_CHANGED'):
                verify_bundle(target)
            path.write_text(original, encoding='utf-8')
            manifest = target / 'manifest.json'
            value = json.loads(manifest.read_text())
            value['contract_hash'] = '0' * 64
            manifest.write_text(json.dumps(value), encoding='utf-8')
            with self.assertRaisesRegex(PipelineError, 'MYSQL_MIGRATION_CONTRACT_MISMATCH'):
                verify_bundle(target)

    def test_v1_publication_plan_is_rejected_before_connecting_to_mysql(self):
        from product_pipeline.contracts import digest
        from product_pipeline.files import atomic_json
        from publication_fixture import fixture
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            plan, _ = fixture(root / 'fixture', 'a' * 64)
            plan['contract_version'] = 1
            plan['plan_id'] = digest({key: value for key, value in plan.items() if key != 'plan_id'})
            atomic_json(root / 'v1-plan.json', plan)
            with patch('product_pipeline.publication.cli.MySQL', side_effect=AssertionError('DB forbidden')) as db, \
                    redirect_stdout(io.StringIO()) as output:
                code = main(['publish', '--config', str(root / 'unused.json'),
                             '--plan', str(root / 'v1-plan.json'), '--apply'])
            self.assertEqual(code, 2)
            self.assertEqual(json.loads(output.getvalue())['error_code'], 'PUBLISH_PLAN_NONCANONICAL')
            db.assert_not_called()

    def test_line_endings_are_portable_but_schema_changes_are_rejected(self):
        with tempfile.TemporaryDirectory() as d:
            target = self.copy_bundle(d)
            path = target / 'feature25_apply.sql'
            original = path.read_text(encoding='utf-8-sig')
            path.write_bytes(original.replace('\n', '\r\n').encode('utf-8-sig'))
            self.assertEqual(verify_bundle(target)['columns'], 160)
            changed = original.replace("'manufacturer','varchar(255)'", "'manufacturer','varchar(254)'")
            self.assertNotEqual(changed, original)
            path.write_text(changed, encoding='utf-8', newline='\n')
            manifest = target / 'manifest.json'
            value = json.loads(manifest.read_text())
            value['scripts'][path.name]['sha256'] = hashlib.sha256(changed.encode()).hexdigest()
            manifest.write_text(json.dumps(value), encoding='utf-8')
            with self.assertRaisesRegex(PipelineError, 'MYSQL_MIGRATION_SCHEMA_MISMATCH'):
                verify_bundle(target)
