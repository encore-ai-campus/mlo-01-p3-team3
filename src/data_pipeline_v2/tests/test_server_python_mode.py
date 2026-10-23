"""server-python mode: configuration contract and reference-only Kafka events."""
import json
import tempfile
import unittest
from pathlib import Path

import test_engine as fixtures
import test_batch
from pipeline_v2.batch_bootstrap import configuration, runtime
from pipeline_v2.domain.contracts import Invalid
from pipeline_v2.adapters.files import write, read


class ServerPythonConfigurationTests(unittest.TestCase):
    def config(self, **overrides):
        value = dict(batch_config_version=1, mode='server-python', target='t', state_root='state', lake_root='lake',
            single_coordinator=True, store=dict(uri_env='U', database='d', replica_set='r'), mongo=dict(replica_set='r'),
            mysql=dict(contract_version=1, target_id='x', host='h', database='d', user='u', password_env='P', ca_env='C'),
            kafka=dict(topic='t', producer_options_env='K', wire_payload=False),
            admission=dict(storage_allowed=True, quarantine_allowed=True))
        value.update(overrides)
        return value

    def load(self, value):
        with tempfile.TemporaryDirectory() as root:
            path = Path(root) / 'batch.json'
            write(path, value)
            return configuration(path)

    def test_valid_configuration_resolves_local_lake(self):
        loaded = self.load(self.config())
        self.assertTrue(Path(loaded['lake_root']).is_absolute())
        self.assertFalse(loaded['kafka']['wire_payload'])

    def test_rejects_remote_lake_missing_mysql_and_overlap(self):
        with self.assertRaisesRegex(Invalid, 'LOCAL_LAKE'):
            self.load(self.config(lake_root='s3a://bucket/direct-v2'))
        with self.assertRaisesRegex(Invalid, 'SERVER_CONFIG_REQUIRED'):
            self.load({k: v for k, v in self.config().items() if k != 'mysql'})
        with self.assertRaisesRegex(Invalid, 'OUTPUTS_OVERLAP'):
            self.load(self.config(lake_root='state'))
        with self.assertRaisesRegex(Invalid, 'PLACEHOLDERS'):
            self.load(self.config(target='replace-with-target'))


class ReferenceOnlyWireTests(unittest.TestCase):
    setUp = test_batch.BatchTests.setUp
    tearDown = test_batch.BatchTests.tearDown
    ready = test_batch.BatchTests.ready

    def test_kafka_events_carry_references_only_and_batch_still_completes(self):
        config = dict(self.config, kafka=dict(wire_payload=False))
        with runtime(config) as (runner, outbox):
            job = self.ready(runner, outbox)['job_id']
            stored = [json.loads(r['value']) for p in (self.root / 'broker').rglob('*.json') for r in read(p)]
            self.assertEqual(len(stored), 5)
            self.assertFalse(any('payload' in e for e in stored))
            self.assertLess(max(len(json.dumps(e)) for e in stored), 2000)
            report = runner.work(job)
            self.assertEqual(report['stages']['publication']['resolutions'], {'PUBLISHED': 5})
            # The processor read every source body once from the ledger, in bulk.
            self.assertEqual(runner.processor.metrics['source_reads'], 10)


if __name__ == '__main__':
    unittest.main()
