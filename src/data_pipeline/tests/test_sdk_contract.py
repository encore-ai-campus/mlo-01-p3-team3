"""Validate boto3's actual request model without making HTTP requests."""
import hashlib
import importlib.util
import io
from pathlib import Path
import tempfile
import unittest

from helpers import png
from product_pipeline.objects import S3Objects


@unittest.skipUnless(importlib.util.find_spec("boto3"), "Install pipeline requirements to validate the real SDK model")
class SdkContractTests(unittest.TestCase):
    def test_garage_sdk_request_omits_condition_but_keeps_checksum_metadata(self):
        import boto3
        from botocore.response import StreamingBody
        from botocore.stub import Stubber
        client=boto3.client('s3',endpoint_url='https://s3.example.invalid',region_name='us-east-1',
                            aws_access_key_id='testing',aws_secret_access_key='testing')
        data=png();checksum=hashlib.sha256(data).hexdigest()
        key=f'images/sha256/{checksum[:2]}/{checksum}.png'
        with Stubber(client) as stub,tempfile.TemporaryDirectory() as root:
            stub.add_client_error('get_object',service_error_code='NoSuchKey',http_status_code=404,
                                  expected_params={'Bucket':'images','Key':key})
            stub.add_response('put_object',{},dict(Bucket='images',Key=key,Body=data,Metadata={'sha256':checksum}))
            stub.add_response('get_object',{'Body':StreamingBody(io.BytesIO(data),len(data)),'ContentLength':len(data)},
                              {'Bucket':'images','Key':key})
            source=Path(root)/'source.png';source.write_bytes(data)
            self.assertTrue(S3Objects('images',client=client,profile='garage_v241').ensure(key,source,checksum,len(data)))
            stub.assert_no_pending_responses()
        client.close()

    def test_real_boto3_model_accepts_conditional_immutable_upload(self):
        import boto3
        from botocore.response import StreamingBody
        from botocore.stub import Stubber
        client = boto3.client("s3", endpoint_url="https://s3.example.invalid", region_name="us-east-1",
                              aws_access_key_id="testing", aws_secret_access_key="testing")
        data = png()
        checksum = hashlib.sha256(data).hexdigest()
        key = f"images/sha256/{checksum[:2]}/{checksum}.png"
        with Stubber(client) as stub, tempfile.TemporaryDirectory() as root:
            stub.add_client_error("get_object", service_error_code="NoSuchKey", http_status_code=404,
                                  expected_params={"Bucket": "images", "Key": key})
            stub.add_response("put_object", {}, {"Bucket": "images", "Key": key, "Body": data,
                               "IfNoneMatch": "*", "Metadata": {"sha256": checksum}})
            stub.add_response("get_object", {"Body": StreamingBody(io.BytesIO(data), len(data)), "ContentLength": len(data)},
                              {"Bucket": "images", "Key": key})
            source = Path(root) / "source.png"
            source.write_bytes(data)
            self.assertTrue(S3Objects("images", client=client).ensure(key, source, checksum, len(data)))
            stub.assert_no_pending_responses()
        client.close()


if __name__ == "__main__":
    unittest.main()
