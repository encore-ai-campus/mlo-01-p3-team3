"""No Spark needed: protect existing tables and exact-URI writer coordination."""
from types import ModuleType
import io
import unittest
from unittest.mock import MagicMock, patch

from product_pipeline.embedding_table import Settings, create, inspect, main, PROPERTIES
from product_pipeline.errors import PipelineError


class EmbeddingTableTests(unittest.TestCase):
    def setUp(self):
        self.settings = Settings('pj3-danawa', 's3a://pj3-data/silver',
                                 's3a://pj3-data/checkpoints/product-image-embeddings')
        self.delta = MagicMock()
        self.delta.isDeltaTable.return_value = True
        self.tables = ModuleType('delta.tables')
        self.tables.DeltaTable = self.delta
        self.modules = patch.dict('sys.modules', {'delta': ModuleType('delta'), 'delta.tables': self.tables})
        self.modules.start()
        self.addCleanup(self.modules.stop)

    def test_lease_claims_same_parent_root_as_existing_silver_writer(self):
        target = self.settings.lease_settings()
        self.assertEqual(target.bronze_path, 's3a://pj3-data/silver')
        self.assertEqual(target.bronze_path, target.events_path)
        self.assertEqual(target.fingerprint, self.settings.fingerprint)

    def test_overlap_and_nonshared_lease_namespace_are_rejected(self):
        for kwargs in ({'checkpoint_path': 's3a://pj3-data/silver/checkpoint'},
                       {'lease_collection': 'private_lock'}):
            values = {**self.settings.__dict__, **kwargs}
            with self.subTest(kwargs=kwargs), self.assertRaises(PipelineError):
                Settings(**values)

    def test_compatible_existing_table_does_not_create_or_overwrite(self):
        before = dict(status='READY', version=9)
        with patch('product_pipeline.embedding_table.inspect', return_value=before):
            result = create(MagicMock(), self.settings, MagicMock())
        self.assertFalse(result['created'])
        self.delta.createIfNotExists.assert_not_called()

    def test_incompatible_existing_table_blocks_creation(self):
        with patch('product_pipeline.embedding_table.inspect', side_effect=PipelineError('CONFLICT')):
            with self.assertRaises(PipelineError):
                create(MagicMock(), self.settings, MagicMock())
        self.delta.createIfNotExists.assert_not_called()

    def test_lost_lease_blocks_creation(self):
        lease = MagicMock()
        lease.check.side_effect = PipelineError('LEASE_LOST')
        with self.assertRaises(PipelineError):
            create(MagicMock(), self.settings, lease)
        self.delta.createIfNotExists.assert_not_called()

    def test_create_sets_constraints_and_rechecks_after_commit(self):
        builder = self.delta.createIfNotExists.return_value
        builder.location.return_value = builder
        builder.addColumns.return_value = builder
        builder.property.return_value = builder
        lease = MagicMock()
        with patch('product_pipeline.embedding_table.inspect', side_effect=[{'status': 'ABSENT'}, {'status': 'READY', 'version': 0}]), patch('product_pipeline.embedding_table.schema', return_value='schema'):
            result = create(MagicMock(), self.settings, lease)
        self.assertTrue(result['created'])
        builder.execute.assert_called_once()
        self.assertEqual(lease.check.call_count, 3)
        self.assertEqual(dict(call.args for call in builder.property.call_args_list), PROPERTIES)

    def test_non_delta_existing_directory_is_not_initialized(self):
        spark = MagicMock()
        self.delta.isDeltaTable.return_value = False
        path = spark._jvm.org.apache.hadoop.fs.Path.return_value
        path.getFileSystem.return_value.exists.return_value = True
        with self.assertRaisesRegex(PipelineError, 'NON_DELTA_TARGET_EXISTS'):
            inspect(spark, self.settings)
        self.delta.createIfNotExists.assert_not_called()

    def test_absent_target_is_read_only(self):
        spark = MagicMock()
        self.delta.isDeltaTable.return_value = False
        path = spark._jvm.org.apache.hadoop.fs.Path.return_value
        path.getFileSystem.return_value.exists.return_value = False
        self.assertEqual(inspect(spark, self.settings)['status'], 'ABSENT')
        self.delta.createIfNotExists.assert_not_called()

    def test_stored_property_drift_is_rejected(self):
        spark = MagicMock()
        expected = object()
        spark.read.format.return_value.load.return_value.schema = expected
        props = {**PROPERTIES, 'delta.appendOnly': 'false'}
        self.delta.forPath.return_value.detail.return_value.select.return_value.first.return_value = [props]
        with patch('product_pipeline.embedding_table.schema', return_value=expected):
            with self.assertRaisesRegex(PipelineError, 'TABLE_CONTRACT_CONFLICT'):
                inspect(spark, self.settings)

    def test_secret_spark_options_are_blocked_before_network_connections(self):
        args = ['--environment-id', self.settings.environment_id, '--silver-root', self.settings.silver_root,
                '--checkpoint-path', self.settings.checkpoint_path, '--create']
        env = {'PIPELINE_SPARK_S3_PROFILE': 'garage_v241',
               'EMBEDDING_SPARK_OPTIONS': '{"spark.hadoop.fs.s3a.secret.key":"must-not-leak"}'}
        output = io.StringIO()
        with patch.dict('os.environ', env, clear=True), patch('sys.stdout', output):
            self.assertEqual(main(args), 2)
        self.assertIn('SECRET_OPTION_FORBIDDEN', output.getvalue())
        self.assertNotIn('must-not-leak', output.getvalue())


if __name__ == '__main__':
    unittest.main()
