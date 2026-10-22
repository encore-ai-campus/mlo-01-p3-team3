import json,sys,unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock,patch
from product_pipeline.bronze.delta import DeltaSink
from product_pipeline.spark_session import stamp_driver_exit,retire_gateway
from product_pipeline.operations.automatic_recovery import eligible


class MemoryBoundaryTests(unittest.TestCase):
    def test_garage_upload_uses_sdk_profile_and_closes_client(self):
        spark=MagicMock();jpath=spark._jvm.org.apache.hadoop.fs.Path
        jpath.return_value.toString.return_value='s3a://bucket/raw/_pipeline_inputs/owned.json'
        client=MagicMock();captured={}
        def put(**kwargs):
            import io
            captured.update(kwargs);captured['data']=kwargs['Body'].read()
            client.get_object.return_value={'Body':io.BytesIO(captured['data'])}
        client.put_object.side_effect=put
        sink=SimpleNamespace(spark=spark,lease=MagicMock(),settings=object(),paths={'raw':'s3a://bucket/raw'},schemas={'raw':'schema'})
        import os
        with patch.dict(os.environ,{'PIPELINE_SPARK_S3_PROFILE':'garage_v241','PIPELINE_S3_ENDPOINT':'https://test','PIPELINE_SPARK_S3_CREDENTIALS_FILE':'protected'}),patch('product_pipeline.objects.S3Objects',return_value=SimpleNamespace(client=client)):
            DeltaSink._append_chunk(sink,'raw',[{'id':'one'}])
        self.assertEqual(json.loads(captured['data']),{'id':'one'})
        jpath.return_value.getFileSystem.return_value.copyFromLocalFile.assert_not_called()
        client.delete_object.assert_called_once_with(Bucket='bucket',Key='raw/_pipeline_inputs/owned.json')
        client.close.assert_called_once()

    def test_raw_body_is_uploaded_as_file_not_gateway_literal(self):
        spark=MagicMock();fs=MagicMock();uploaded=[]
        path=spark._jvm.org.apache.hadoop.fs.Path
        path.return_value.getFileSystem.return_value=fs
        def copy(delete,overwrite,local,remote):
            uri=path.call_args_list[-1].args[0]
            from urllib.parse import unquote,urlsplit
            value=unquote(urlsplit(uri).path)
            if sys.platform=='win32':value=value.lstrip('/')
            uploaded.extend(json.loads(line) for line in Path(value).read_text(encoding='utf-8').splitlines())
        fs.copyFromLocalFile.side_effect=copy
        sink=SimpleNamespace(spark=spark,lease=MagicMock(),settings=object(),paths={'raw':'s3a://bucket/raw'},schemas={'raw':'schema'})
        rows=[dict(raw_document_id='one',payload='한글"\\\n'*50)]
        DeltaSink._append_chunk(sink,'raw',rows)
        self.assertEqual(uploaded,rows)
        spark.range.assert_not_called()
        self.assertTrue(all(len(c.args[0])<200 for c in path.call_args_list))
        fs.delete.assert_called_once()

    def test_uncertain_append_keeps_its_staged_input(self):
        spark=MagicMock();fs=MagicMock()
        spark._jvm.org.apache.hadoop.fs.Path.return_value.getFileSystem.return_value=fs
        reader=spark.read.schema.return_value
        reader.option.return_value=reader
        reader.json.return_value.write.format.return_value.mode.return_value.option.return_value.save.side_effect=OSError('uncertain')
        sink=SimpleNamespace(spark=spark,lease=MagicMock(),settings=object(),paths={'raw':'s3a://bucket/raw'},schemas={'raw':'schema'})
        with self.assertRaises(OSError):DeltaSink._append_chunk(sink,'raw',[{'id':'one'}])
        fs.delete.assert_not_called()

    def test_only_observed_nonzero_jvm_exit_is_recoverable(self):
        process=MagicMock()
        with patch.dict(sys.modules,{'pyspark':SimpleNamespace(SparkContext=SimpleNamespace(_gateway=SimpleNamespace(proc=process)))}):
            for code in (None,0,3):
                process.poll.return_value=code;result={'status':'FAILED','error_code':'Py4JError'}
                self.assertEqual(stamp_driver_exit(result),code==3)
                if code==3:
                    self.assertTrue(eligible(dict(status='RECOVERY_REQUIRED',error_code=result['error_code'],last_failure=result)))
                else:self.assertEqual(result['error_code'],'Py4JError')

    def test_retirement_waits_for_owned_gateway_exit(self):
        gateway=MagicMock()
        with patch.dict(sys.modules,{'pyspark':SimpleNamespace(SparkContext=SimpleNamespace(_gateway=gateway))}):retire_gateway()
        gateway.shutdown.assert_called_once();gateway.proc.stdin.close.assert_called_once()
        gateway.proc.wait.assert_called_once_with(timeout=20)
