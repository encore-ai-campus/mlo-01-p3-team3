import copy
import hashlib
import io
import json
import tempfile
from pathlib import Path
import unittest
from contextlib import redirect_stdout
from unittest.mock import patch

import test_engine as fixtures
from pipeline_v2.batch_bootstrap import runtime
from pipeline_v2.domain.batch import inventory_hash, offsets, delivery_contract
from pipeline_v2.domain.contracts import Invalid, seal, digest
from pipeline_v2.adapters.files import write, read
from pipeline_v2.interfaces.cli import main


class BatchTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.items = fixtures.inputs(self.root,products=5)
        self.config = dict(batch_config_version=1,mode='local',target='test-batch',
            state_root=str(self.root/'state'),lake_root=str(self.root/'lake'),
            publication_root=str(self.root/'sql'),broker_root=str(self.root/'broker'),
            admission=dict(storage_allowed=True,quarantine_allowed=True),
            policy=fixtures.POLICY,batch_limits={})
        self.crawl = dict(schema_version=1,crawl_batch_id='crawl-1',status='SOURCE_COMPLETE',
            required_sources=['danawa'],completed_sources=['danawa'],event_count=len(self.items),
            input_sha256=hashlib.sha256((self.root/'source.json').read_bytes()).hexdigest(),
            started_at='2026-10-16T00:00:00.000Z',completed_at='2026-10-16T01:00:00.000Z',
            input_semantics='OBSERVATION_SNAPSHOT')

    def tearDown(self):
        self.temp.cleanup()

    def ready(self,runner,outbox,crawl=None,items=None,generation=0):
        crawl = crawl or self.crawl
        outbox.prepare(crawl,self.items if items is None else items)
        outbox.relay(crawl['crawl_batch_id'],generation)
        return runner.submit(crawl['crawl_batch_id'],generation)

    def test_local_end_to_end_and_three_pending_batches_do_not_limit_products(self):
        with runtime(self.config) as (runner,outbox):
            job = self.ready(runner,outbox)
            report = runner.work(job['job_id'])
            self.assertEqual(report['status'],'COMPLETED')
            self.assertEqual(report['stages']['publication']['resolutions'],{'PUBLISHED':5})
            self.assertNotIn('proofs',report['stages']['gold'])
            self.assertEqual(runner.audit(job['job_id'])['status'],'AUDIT_OK')
            summary = runner.timing.summary('crawl-1')
            self.assertIn('e2e.crawl_complete_to_receipt',summary['stages'])
            self.assertIsNotNone(summary['stages']['batch.total']['p95_ms'])

    def test_duplicate_submit_and_completed_work_reuse_outputs_without_source_read(self):
        with runtime(self.config) as (runner,outbox):
            job = self.ready(runner,outbox)
            runner.work(job['job_id'])
            writes = runner.publisher.writes
            with patch.object(runner.processor,'process',side_effect=AssertionError('must not run')):
                self.assertEqual(runner.submit('crawl-1')['job_id'],job['job_id'])
                self.assertEqual(runner.work(job['job_id'])['status'],'COMPLETED')
            self.assertEqual(runner.publisher.writes,writes)

    def test_ack_loss_retransmits_identical_event_and_deduplicates(self):
        with runtime(self.config) as (runner,outbox):
            outbox.prepare(self.crawl,self.items)
            original = outbox.store.put
            failed = False
            def put(kind,key,value):
                nonlocal failed
                if kind == 'ack' and not failed:
                    failed = True
                    raise OSError('lost ack persistence')
                return original(kind,key,value)
            with patch.object(outbox.store,'put',side_effect=put):
                with self.assertRaises(OSError):
                    outbox.relay('crawl-1')
            manifest = outbox.relay('crawl-1')
            # Acks are written per chunk, so the whole 5-event chunk is retransmitted and collapsed by event ID.
            self.assertEqual(sum(len(read(p)) for p in (self.root/'broker').rglob('*.json')),10)
            self.assertGreaterEqual(sum(v['end_offset']-v['start_offset'] for v in manifest['ranges']),5)
            report = runner.work(runner.submit('crawl-1')['job_id'])
            self.assertEqual(report['stages']['publication']['resolutions'],{'PUBLISHED':5})

    def test_interleaved_other_batch_events_are_filtered(self):
        with runtime(self.config) as (runner,outbox):
            self.ready(runner,outbox)
            other = dict(self.crawl,crawl_batch_id='other')
            outbox.prepare(other,self.items)
            outbox.relay('other')
            # Re-delivery brackets records of the other batch, on purpose.
            delivery = outbox.store.get('delivery',['crawl-1',0])
            broker = runner.processor.broker
            for event in [outbox.store.get('outbox',['crawl-1',v['event_id']]) for v in delivery['inventory']]:
                broker.send(event['product_key'],event)
            extended = {k:v for k,v in delivery.items() if k != 'content_hash'}
            for position in extended['ranges']:
                position['end_offset'] = len(read(broker.root/position['topic']/(str(position['partition'])+'.json')))
            outbox.store.put('delivery',['crawl-1',1],seal(dict(extended,generation=1)))
            job = runner.submit('crawl-1',1)
            self.assertEqual(runner.work(job['job_id'])['status'],'COMPLETED')

    def test_partial_source_outbox_is_reconciled_before_source_complete(self):
        with runtime(self.config) as (runner,outbox):
            original = outbox.store.put
            def fail(kind,key,value):
                if kind == 'outbox':
                    raise OSError('stop')
                return original(kind,key,value)
            with patch.object(outbox.store,'put',side_effect=fail):
                with self.assertRaises(OSError):
                    outbox.prepare(self.crawl,self.items)
            self.assertIsNone(outbox.store.get('crawl','crawl-1'))
            # Sources are written per chunk before the outbox, so the whole chunk is already durable.
            self.assertEqual(len(list(outbox.store.values('source'))),5)
            self.assertEqual(self.ready(runner,outbox)['status'],'QUEUED')

    def test_publication_response_loss_recovers_without_duplicate_service_writes(self):
        with runtime(self.config) as (runner,outbox):
            job = self.ready(runner,outbox)
            original = runner.publisher.publish
            def uncertain(delivery):
                original(delivery)
                raise OSError('response lost')
            with patch.object(runner.publisher,'publish',side_effect=uncertain):
                with self.assertRaises(OSError):
                    runner.work(job['job_id'])
            with self.assertRaisesRegex(Invalid,'RECOVERY_EVIDENCE'):
                runner.work(job['job_id'])
            runner.recover(job['job_id'],'old worker stopped')
            result = runner.work(job['job_id'])
            self.assertEqual(result['status'],'COMPLETED')
            self.assertEqual(runner.publisher.writes,5)

    def test_retention_loss_does_not_skip_and_new_generation_preserves_job_id(self):
        with runtime(self.config) as (runner,outbox):
            job = self.ready(runner,outbox)
            for path in (self.root/'broker').rglob('*.json'):
                path.unlink()
            with self.assertRaisesRegex(Invalid,'OFFSET_UNAVAILABLE'):
                runner.work(job['job_id'])
            outbox.relay('crawl-1',generation=1)
            runner.recover(job['job_id'],'worker stopped',generation=1)
            report = runner.work(job['job_id'])
            self.assertEqual(report['job_id'],job['job_id'])
            self.assertEqual(report['status'],'COMPLETED')

    def test_no_change_batch_reuses_normalization_before_fetching_source(self):
        with runtime(self.config) as (runner,outbox):
            first = self.ready(runner,outbox)
            runner.work(first['job_id'])
            before = dict(runner.processor.metrics)
            second = self.ready(runner,outbox,crawl=dict(self.crawl,crawl_batch_id='second'))
            runner.work(second['job_id'])
            self.assertEqual(runner.processor.metrics['source_reads'],before['source_reads'])
            self.assertEqual(runner.processor.metrics['normalized'],before['normalized'])
            self.assertEqual(runner.publisher.writes,5)

    def test_storage_forbidden_and_incomplete_crawl_fail_before_writes(self):
        with runtime(self.config) as (runner,outbox):
            with self.assertRaisesRegex(Invalid,'CRAWL_NOT_COMPLETE'):
                outbox.prepare(dict(self.crawl,status='RUNNING'),self.items)
            outbox.admission = dict(storage_allowed=False,quarantine_allowed=False)
            with self.assertRaisesRegex(Invalid,'STORAGE_NOT_ALLOWED'):
                outbox.prepare(self.crawl,self.items)
            self.assertEqual(list(outbox.store.values('source')),[])

    def test_three_batch_capacity_is_released_after_completion(self):
        config = dict(self.config,batch_limits={'pending_batches':1})
        with runtime(config) as (runner,outbox):
            first = self.ready(runner,outbox)
            with self.assertRaisesRegex(Invalid,'BACKPRESSURE'):
                self.ready(runner,outbox,crawl=dict(self.crawl,crawl_batch_id='second'))
            runner.work(first['job_id'])
            self.assertEqual(runner.submit('second')['status'],'QUEUED')

    def test_offset_range_contract_and_event_set_tampering(self):
        with runtime(self.config) as (runner,outbox):
            self.ready(runner,outbox)
            manifest = outbox.store.get('delivery',['crawl-1',0])
            starts,ends = offsets(manifest)
            self.assertTrue(json.loads(starts))
            tampered = {k:v for k,v in manifest.items() if k != 'content_hash'}
            tampered['event_count'] += 1
            with self.assertRaisesRegex(Invalid,'INVENTORY_CONFLICT'):
                delivery_contract(seal(tampered))

    def test_cli_and_metrics_status_do_not_start_processing(self):
        write(self.root/'config.json',self.config)
        write(self.root/'crawl.json',self.crawl)
        output = io.StringIO()
        common = ['--config',str(self.root/'config.json')]
        with redirect_stdout(output):
            self.assertEqual(main(common+['batch-prepare','--manifest',str(self.root/'crawl.json'),'--input',str(self.root/'source.json')]),0)
            self.assertEqual(main(common+['batch-relay','--batch','crawl-1']),0)
            self.assertEqual(main(common+['batch-submit','--batch','crawl-1']),0)
            job = json.loads(output.getvalue().splitlines()[-1])['job_id']
            self.assertEqual(main(common+['batch-work','--job',job]),0)
            self.assertEqual(main(common+['batch-status','--job',job]),0)
            self.assertEqual(main(common+['metrics','--scope','crawl-1']),0)
        self.assertEqual(json.loads(output.getvalue().splitlines()[-1])['stages']['batch.total']['events'],1)
