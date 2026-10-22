import json,tempfile,unittest
from pathlib import Path
from unittest.mock import MagicMock,patch
from product_pipeline.recovery_probe import probe
from product_pipeline.errors import PipelineError


class ProbeTests(unittest.TestCase):
    def run_probe(self,*,cmd=b'',url='https://master/json/',workers=None):
        with tempfile.TemporaryDirectory() as root:
            p=Path(root)/'123';p.mkdir();(p/'cmdline').write_bytes(cmd)
            cluster=dict(activeapps=[],workers=workers if workers is not None else [dict(host='worker',state='ALIVE',coresused=0,memoryused=0)])
            response=MagicMock();response.geturl.return_value=url;response.read.return_value=json.dumps(cluster).encode()
            opener=MagicMock();opener.open.return_value.__enter__.return_value=response
            with patch('product_pipeline.recovery_probe.ssl.create_default_context'),patch('product_pipeline.recovery_probe.urllib.request.build_opener',return_value=opener):
                return probe(master_url=url,ca='ca',client_cert='cert',client_key='key',worker_hosts=['worker'],proc_root=root)
    def test_authenticated_idle_cluster(self):self.assertEqual(self.run_probe()['status'],'QUIESCENT')
    def test_spark_submit_and_operational_driver_block(self):
        for cmd in (b'java\0org.apache.spark.deploy.SparkSubmit\0',b'python\0/server/continuous_work.py\0'):
            self.assertEqual(self.run_probe(cmd=cmd)['status'],'BUSY')
    def test_worker_missing_blocks(self):
        with self.assertRaises(PipelineError):self.run_probe(workers=[])
    def test_plaintext_final_endpoint_blocks(self):
        with self.assertRaises(PipelineError):self.run_probe(url='http://master/json/')
