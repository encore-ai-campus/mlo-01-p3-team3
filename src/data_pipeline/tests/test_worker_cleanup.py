import importlib.util
from pathlib import Path
import unittest

path=Path(__file__).resolve().parents[1]/'docs/server_setup/configure_spark_worker_cleanup.py'
spec=importlib.util.spec_from_file_location('worker_cleanup',path)
module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)


class CleanupPolicyTests(unittest.TestCase):
    def test_preserves_existing_options_and_is_idempotent(self):
        original='export SPARK_WORKER_OPTS="-Dexisting=true"\nexport OTHER="keep"\n'
        updated=module.configured(original)
        self.assertTrue(updated.startswith(original))
        self.assertIn('${SPARK_WORKER_OPTS:-}',updated)
        self.assertIn('cleanup.enabled=true',updated)
        self.assertIn('appDataTtl=3600',updated)
        self.assertEqual(module.configured(updated),updated)

    def test_policy_changes_replace_the_one_managed_block(self):
        updated=module.configured(module.configured(''),retention=7200)
        self.assertEqual(updated.count(module.BEGIN),1)
        self.assertNotIn('appDataTtl=3600',updated)
        self.assertIn('appDataTtl=7200',updated)

    def test_conflicting_or_malformed_policy_is_not_overwritten(self):
        for text in ('export SPARK_WORKER_OPTS="-Dspark.worker.cleanup.enabled=false"',module.BEGIN,
                     module.END+'\n'+module.BEGIN):
            with self.assertRaises(ValueError):module.configured(text)
        with self.assertRaises(ValueError):module.configured('',retention=30)
