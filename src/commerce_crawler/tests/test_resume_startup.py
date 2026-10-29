"""Legacy JSONL compatibility and bounded startup I/O; no HTTP requests."""
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from product_collector.checkpoint import CollectionCheckpoint, encoded
from product_collector.core import Category, RequestClient, collect, payload_hash
from product_collector.danawa import DanawaAdapter
from test_product_collector import FakeSession, listing
from test_gallery_assets import gallery


def legacy_checkpoint(directory):
    """Write the previous jsonl-v1 layout, without using the new checkpoint writer."""
    adapter = DanawaAdapter()
    raw, _, report = collect(adapter, RequestClient(None, interval=0,
        session=FakeSession({adapter.category_url('1'): listing('11', '12')})),
        [Category('1', limit=None)], total_limit=None, collect_details=False)
    raw[0]['payload']['detail'] = adapter.parse_detail(gallery('11', 2)[0], raw[0]['payload']['listings'][0])
    for row in raw:
        row['detail_status'] = 'ok' if row['product_id'] == '11' else 'not_requested'
        row['payload_hash'] = payload_hash(row['payload'])
    event = {'seq': 1, 'records': [{'raw': row, 'normalized': adapter.normalize(row)} for row in raw],
             'categories': report['categories'],
             'requests': [{'id': i + 1, 'data': r} for i, r in enumerate(report['requests'])],
             'report': {k:v for k,v in report.items() if k not in {'requests', 'categories', 'updated_at'}}}
    directory.mkdir(parents=True, exist_ok=True)
    (directory/'run_config.json').write_text(json.dumps({'job_id':directory.name,'storage':'jsonl-v1','config':{}}), encoding='utf-8')
    body=encoded(event)
    (directory/'checkpoint_events.jsonl').write_text('\n'+encoded({'event':event,'sha256':hashlib.sha256(body.encode()).hexdigest()})+'\n',encoding='utf-8')
    (directory/'checkpoint_state.json').write_text(json.dumps({'job_id':directory.name,'status':'running','commit':1}),encoding='utf-8')
    for item in event['records']:
        for folder, value in [('product_raw',item['raw']),('products',item['normalized'])]:
            (directory/folder).mkdir(exist_ok=True)
            (directory/folder/f"danawa_{item['raw']['product_id']}.json").write_text(json.dumps(value),encoding='utf-8')
    return adapter


class ResumeStartupTests(unittest.TestCase):
    def test_legacy_checkpoint_skips_completed_mirror_reads_and_successful_detail(self):
        with tempfile.TemporaryDirectory() as root:
            directory=Path(root)/'legacy'
            adapter=legacy_checkpoint(directory)
            original_files={name:(directory/name).read_bytes() for name in
                            ('run_config.json','checkpoint_events.jsonl','checkpoint_state.json')}
            original=Path.read_text
            def reading(path,*args,**kwargs):
                if path.parent.name in {'products','product_raw'}:
                    self.fail('startup read an already completed product file')
                return original(path,*args,**kwargs)
            events=[]
            with patch.object(Path,'read_text',reading):
                store=CollectionCheckpoint(directory,adapter,{},progress=events.append)
            self.assertEqual([e['stage'] for e in events], ['checkpoint_read','checkpoint_read','checkpoint_files','checkpoint_ready'])
            for name,value in original_files.items():
                self.assertEqual((directory/name).read_bytes(),value)
            self.assertEqual(events[-2]['total'],0)
            session=FakeSession({'https://prod.danawa.com/info/?pcode=12':gallery('12',1)[0]})
            raw,normal,_=collect(adapter,RequestClient(None,interval=0,session=session),
                [Category('1',limit=None)],total_limit=None,checkpoint=store)
            self.assertEqual([url for url,_ in session.calls],['https://prod.danawa.com/info/?pcode=12'])
            self.assertTrue(all(r['detail_status']=='ok' for r in raw))
            self.assertTrue(all(n['raw_payload_hash']==r['payload_hash'] for n,r in zip(normal,raw)))

    def test_only_unfinished_mirrors_recover_even_when_payload_hash_is_unchanged(self):
        with tempfile.TemporaryDirectory() as root:
            directory=Path(root)/'legacy'
            adapter=legacy_checkpoint(directory)
            store=CollectionCheckpoint(directory,adapter,{})
            row=store.load(('danawa','12'))
            row.update(detail_status='request_failed',detail_error='synthetic failure')
            with patch('product_collector.checkpoint.atomic_json',side_effect=OSError('interrupted before product write')):
                with self.assertRaises(OSError):
                    store.save([row],store.current_report(),[])
            original=Path.read_text
            touched=[]
            def reading(path,*args,**kwargs):
                if path.parent.name in {'products','product_raw'}: touched.append(path.name)
                return original(path,*args,**kwargs)
            with patch.object(Path,'read_text',reading):
                restored=CollectionCheckpoint(directory,adapter,{})
            self.assertEqual(set(touched),{'danawa_12.json'})
            saved=json.loads((directory/'product_raw/danawa_12.json').read_text(encoding='utf-8'))
            self.assertEqual(saved['detail_status'],'request_failed')
            self.assertEqual(saved['detail_error'],'synthetic failure')
            self.assertEqual(restored.sequence,2)

    def test_explicit_repair_recovers_externally_removed_and_corrupted_files(self):
        with tempfile.TemporaryDirectory() as root:
            directory=Path(root)/'legacy'
            adapter=legacy_checkpoint(directory)
            (directory/'products/danawa_11.json').unlink()
            (directory/'product_raw/danawa_12.json').write_text('broken',encoding='utf-8')
            events=[]
            CollectionCheckpoint(directory,adapter,{},repair_files=True,progress=events.append)
            self.assertEqual(json.loads((directory/'products/danawa_11.json').read_text(encoding='utf-8'))['product_id'],'11')
            self.assertEqual(json.loads((directory/'product_raw/danawa_12.json').read_text(encoding='utf-8'))['product_id'],'12')
            self.assertEqual([e for e in events if e['stage']=='checkpoint_files'][-1]['completed'],2)

    def test_error_handler_retries_pending_files_before_advancing_saved_cursor(self):
        with tempfile.TemporaryDirectory() as root:
            directory=Path(root)/'legacy'
            adapter=legacy_checkpoint(directory)
            store=CollectionCheckpoint(directory,adapter,{})
            row=store.load(('danawa','12'))
            row.update(detail_status='request_failed',detail_error='pending write')
            with patch('product_collector.checkpoint.atomic_json',side_effect=OSError('interrupted file write')):
                with self.assertRaises(OSError):
                    store.save([row],store.current_report(),[])
            store.interrupt([],status='failed')
            saved=json.loads((directory/'product_raw/danawa_12.json').read_text(encoding='utf-8'))
            self.assertEqual(saved['detail_error'],'pending write')
            self.assertEqual(store._pending_files,set())

    def test_copy_on_write_does_not_change_durable_records_on_failed_page_save(self):
        with tempfile.TemporaryDirectory() as root:
            directory=Path(root)/'legacy'
            adapter=legacy_checkpoint(directory)
            store=CollectionCheckpoint(directory,adapter,{})
            before={key:store.load(key) for key in store.keys()}
            original=adapter.normalize
            def normalize(raw):
                if raw['product_id']=='12': raise KeyboardInterrupt()
                return original(raw)
            with patch.object(adapter,'normalize',side_effect=normalize):
                with self.assertRaises(KeyboardInterrupt):
                    collect(adapter,RequestClient(None,interval=0,session=FakeSession({adapter.category_url('2'):listing('11','12')})),
                            [Category('1',limit=None),Category('2',limit=None)],total_limit=None,
                            collect_details=False,checkpoint=store)
            self.assertEqual({key:store.load(key) for key in store.keys()},before)

    def test_loaded_records_are_private_copies_of_the_journal(self):
        with tempfile.TemporaryDirectory() as root:
            directory=Path(root)/'legacy'
            adapter=legacy_checkpoint(directory)
            store=CollectionCheckpoint(directory,adapter,{})
            key=('danawa','11')
            first=store.load(key)
            first['detail_status']='changed in memory only'
            self.assertEqual(store.load(key)['detail_status'],'ok')
            self.assertEqual(store.detail_status(key),'ok')
            self.assertEqual(store.category_ids(key),('1',))
            # A save moves the product's position to the new event; the old line stays as it was.
            first['detail_status']='request_failed'
            store.save([first],store.current_report(),[])
            self.assertEqual((store.load(key)['detail_status'],store.detail_status(key)),('request_failed','request_failed'))
            self.assertEqual(CollectionCheckpoint(directory,adapter,{}).load(key)['detail_status'],'request_failed')

    def test_tampered_journal_still_fails_checksum_validation(self):
        with tempfile.TemporaryDirectory() as root:
            directory=Path(root)/'legacy'
            adapter=legacy_checkpoint(directory)
            journal=directory/'checkpoint_events.jsonl'
            envelope=json.loads(journal.read_text(encoding='utf-8'))
            envelope['event']['records'][0]['raw']['product_id']='99'
            journal.write_text(json.dumps(envelope),encoding='utf-8')
            with self.assertRaisesRegex(ValueError,'해시'):
                CollectionCheckpoint(directory,adapter,{})

    def test_missing_state_repairs_all_products_without_advancing_past_journal(self):
        with tempfile.TemporaryDirectory() as root:
            directory=Path(root)/'legacy'
            adapter=legacy_checkpoint(directory)
            (directory/'checkpoint_state.json').unlink()
            (directory/'products/danawa_11.json').unlink()
            CollectionCheckpoint(directory,adapter,{})
            self.assertTrue((directory/'products/danawa_11.json').is_file())
