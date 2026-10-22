import copy,tempfile,unittest
from pathlib import Path
from product_pipeline.image_waits import record_gallery_wait
from product_pipeline.files import read_json,atomic_json
from product_pipeline.errors import Conflict


class GalleryWaitTests(unittest.TestCase):
    def data(self):
        doc=dict(_id='a'*24,observation_key='b'*64,document_hash='c'*64,source_code='danawa',source_product_id='1',
            source_url='https://prod.danawa.com/info/?pcode=1',collected_at='2026-10-21T00:00:00Z',
            collector_metadata={'detail_status':'request_failed'},image_refs=[])
        proof=dict(raw_document_id=doc['_id'],observation_key=doc['observation_key'],status='VERIFIED',normalized_id='normalized-1',normalized_hash='d'*64)
        return doc,proof
    def test_wait_is_separate_immutable_evidence_not_an_approval_or_raw_change(self):
        with tempfile.TemporaryDirectory() as root:
            doc,proof=self.data();before=copy.deepcopy(doc)
            path=record_gallery_wait(root,doc,proof);value=read_json(path)
            self.assertEqual(value['reason'],'SOURCE_DETAIL_REQUEST_FAILED')
            self.assertFalse(value['changes_service_approval']);self.assertEqual(doc,before)
            self.assertEqual(record_gallery_wait(root,doc,proof),path)
            atomic_json(path,dict(value,reason='changed'))
            with self.assertRaises(Conflict):record_gallery_wait(root,doc,proof)
    def test_gallery_urls_waiting_for_bytes_do_not_enter_source_url_wait_list(self):
        with tempfile.TemporaryDirectory() as root:
            doc,proof=self.data();doc['image_refs']=[{'url':'https://example.com/p.jpg'}]
            self.assertIsNone(record_gallery_wait(root,doc,proof));self.assertEqual(list(Path(root).iterdir()),[])
            proof['observation_key']='wrong'
            with self.assertRaises(Conflict):record_gallery_wait(root,doc,proof)
