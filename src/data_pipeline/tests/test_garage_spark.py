"""S3A profile cannot silently bypass latched writer or credential boundaries."""
import io
import stat
from types import SimpleNamespace
import unittest
from unittest.mock import MagicMock, patch

from product_pipeline.errors import PipelineError
from product_pipeline.spark_storage import storage_options, bind_spark_credentials
from product_pipeline.bronze.lease import MongoLease
from product_pipeline.s3_credentials import service_credentials


class SparkGarageTests(unittest.TestCase):
    def env(self):
        return patch.dict('os.environ',{
            'PIPELINE_SPARK_S3_PROFILE':'garage_v241',
            'PIPELINE_S3_ENDPOINT':'https://192.168.0.141:8443',
            'PIPELINE_SPARK_S3_CREDENTIALS_FILE':'/private/spark.properties'}, clear=True)

    def test_default_profile_is_unchanged(self):
        with patch.dict('os.environ',{},clear=True):
            self.assertEqual(storage_options({'spark.master':'local[1]'}),{'spark.master':'local[1]'})
            bind_spark_credentials(None)

    def test_garage_writer_requires_actual_latched_mongo_protocol(self):
        with self.env(), self.assertRaisesRegex(PipelineError,'LATCHED_WRITER_REQUIRED'):
            storage_options({},writer=True,lease=SimpleNamespace(check=lambda:None))

    def test_writer_options_verify_lease_before_enabling_logstore(self):
        lease=MagicMock(spec=MongoLease)
        with self.env(), patch('product_pipeline.spark_storage.Path.is_file',return_value=True):
            result=storage_options({'spark.master':'spark://master:7077'},writer=True,lease=lease)
        lease.check_s3_bucket.assert_called_once_with('pj3-data')
        self.assertEqual(result['spark.delta.logStore.s3a.impl'],'io.delta.storage.S3SingleDriverLogStore')
        self.assertEqual(result['spark.hadoop.fs.s3a.path.style.access'],'true')
        self.assertEqual(result['spark.master'],'spark://master:7077')
        self.assertNotIn('spark.hadoop.fs.s3a.secret.key',result)

    def test_expired_lease_cannot_prepare_spark_options(self):
        lease=MagicMock(spec=MongoLease);lease.check_s3_bucket.side_effect=PipelineError('LEASE_LOST')
        with self.env(), self.assertRaisesRegex(PipelineError,'LEASE_LOST'):
            storage_options({},writer=True,lease=lease)

    def test_logstore_sdk_jar_and_secret_option_conflicts_are_blocked(self):
        for options,code in [
            ({'spark.delta.logStore.s3a.impl':'other'},'LOGSTORE_CONFLICT'),
            ({'spark.jars':'file:/other/hadoop-aws.jar'},'JAR_CONFLICT'),
            ({'spark.hadoop.fs.s3a.secret.key':'secret'},'SECRET_IN_OPTIONS_FORBIDDEN')]:
            with self.subTest(code=code),self.env(),patch('product_pipeline.spark_storage.Path.is_file',return_value=True),self.assertRaisesRegex(PipelineError,code):
                storage_options(options)

    def test_missing_jars_do_not_trigger_installation_or_unsafe_fallback(self):
        with self.env(),patch('product_pipeline.spark_storage.Path.is_file',return_value=False),self.assertRaisesRegex(PipelineError,'CLIENT_FILES_MISSING'):
            storage_options({})

    def test_secret_is_bound_only_to_hadoop_configuration(self):
        spark=MagicMock()
        spark.sparkContext._jvm.java.lang.System.getProperty.return_value='/etc/pj3-clients/s3/ca-truststore.p12'
        with self.env(),patch('product_pipeline.spark_storage.service_credentials',return_value={'aws_access_key_id':'access','aws_secret_access_key':'secret'}):
            bind_spark_credentials(spark)
        hadoop=spark.sparkContext._jsc.hadoopConfiguration.return_value
        self.assertEqual(hadoop.set.call_args_list[0].args,('fs.s3a.access.key','access'))
        self.assertEqual(hadoop.set.call_args_list[1].args,('fs.s3a.secret.key','secret'))
        spark.conf.set.assert_not_called()

    def test_existing_jvm_wrong_truststore_is_not_silently_reused(self):
        spark=MagicMock();spark.sparkContext._jvm.java.lang.System.getProperty.return_value=None
        with self.env(),self.assertRaisesRegex(PipelineError,'TRUSTSTORE_NOT_APPLIED'):
            bind_spark_credentials(spark)

    def test_garage_lease_rejects_alias_or_non_s3_target_before_renewal(self):
        lease=object.__new__(MongoLease);lease._targets=['s3a://other/lake']
        lease.check=MagicMock()
        with self.assertRaisesRegex(PipelineError,'WRITER_TARGET_MISMATCH'):
            lease.check_s3_bucket('pj3-data')
        lease.check.assert_not_called()
        lease._targets=['s3a://pj3-data/lake','s3a://pj3-data/checkpoint']
        lease.check_s3_bucket('pj3-data');lease.check.assert_called_once()


class ProtectedCredentialsTests(unittest.TestCase):
    def read(self, content, *, mode=0o600, uid=999):
        info=SimpleNamespace(st_mode=stat.S_IFREG|mode,st_uid=uid,st_size=len(content))
        with patch('os.name','posix'),patch('os.O_NOFOLLOW',131072,create=True),patch('os.O_NONBLOCK',2048,create=True),patch('os.geteuid',return_value=999,create=True),patch('os.open',return_value=50),patch('os.fstat',return_value=info),patch('os.fdopen',return_value=io.StringIO(content)),patch('os.close'):
            return service_credentials('/protected/test.properties')

    def test_private_file_is_parsed_without_environment_export(self):
        self.assertEqual(self.read('# comment\naccessKey=key\nsecretKey=value=tail\n'),
                         dict(aws_access_key_id='key',aws_secret_access_key='value=tail'))

    def test_other_owner_and_group_readable_files_are_rejected(self):
        for args in [dict(mode=0o640),dict(uid=0)]:
            with self.assertRaisesRegex(PipelineError,'PERMISSIONS'):
                self.read('accessKey=k\nsecretKey=s',**args)

    def test_duplicate_missing_or_escaped_secrets_are_rejected(self):
        for content in ['accessKey=k','accessKey=a\naccessKey=b\nsecretKey=s','accessKey=k\nsecretKey=escape\\x']:
            with self.assertRaisesRegex(PipelineError,'CREDENTIALS_FORMAT'):
                self.read(content)


if __name__=='__main__':unittest.main()
