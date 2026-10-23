"""Redesign contracts: row-level admission, retention, phase overlap, retraction repair, capacity."""
import copy
import json
import unittest
from unittest.mock import patch

import test_engine as fixtures
import test_batch
from pipeline_v2.batch_bootstrap import runtime
from pipeline_v2.domain.contracts import Invalid
from pipeline_v2.adapters.files import read


class RedesignContractTests(unittest.TestCase):
    setUp = test_batch.BatchTests.setUp
    tearDown = test_batch.BatchTests.tearDown
    ready = test_batch.BatchTests.ready

    def crawl_for(self, name, items):
        return dict(self.crawl,crawl_batch_id=name,event_count=len(items))

    def history(self, observations=2):
        directory = self.root/('history-'+str(observations))
        directory.mkdir()
        return fixtures.inputs(directory,products=1,observations=observations)

    def tampered(self, item):
        bad = copy.deepcopy(item)
        bad['document']['source_record_key'] = 'tampered-without-reseal'
        return bad

    def test_bad_rows_are_isolated_and_disallowed_fields_are_never_stored(self):
        secret = copy.deepcopy(self.items[1])
        secret['document']['private_note'] = 'SENTINEL-NOT-ALLOWED'
        items = self.items + [self.tampered(self.items[0]), secret, copy.deepcopy(self.items[2])]
        config = dict(self.config,admission=dict(storage_allowed=True,quarantine_allowed=True,
            field_allowlist=sorted(self.items[0]['document'])))
        with runtime(config) as (runner,outbox):
            source = outbox.prepare(self.crawl_for('mixed',items),items)
            # Identical crawler retransmission collapses; one bad row does not block the crawl.
            self.assertEqual((source['input_records'],source['duplicate_records'],source['event_count']),(8,1,7))
            self.assertEqual(source['intake'],{'ACCEPTED_FOR_BRONZE':5,'INPUT_QUARANTINE':1,'SOURCE_METADATA_ONLY':1})
            outbox.relay('mixed')
            report = runner.work(runner.submit('mixed')['job_id'])
            self.assertEqual(report['status'],'COMPLETED')
            self.assertEqual(report['stages']['publication']['resolutions'],{'PUBLISHED':5})
            counts = report['stages']['bronze']['counts']
            self.assertEqual((counts['accepted'],counts['input_quarantine'],counts['source_metadata_only']),(5,1,1))
            held = list(outbox.store.values('quarantine_raw'))
            self.assertEqual([v['reason'] for v in held],['SOURCE_DOCUMENT_HASH_CONFLICT'])
            self.assertEqual(runner.audit(report['job_id'])['status'],'AUDIT_OK')
        for path in self.root.rglob('*.json'):
            if path.name != 'source.json':
                self.assertNotIn('SENTINEL-NOT-ALLOWED',path.read_text(encoding='utf-8'),path)

    def test_quarantine_disallowed_keeps_metadata_without_payload(self):
        items = [self.tampered(self.items[0])]+self.items
        config = dict(self.config,admission=dict(storage_allowed=True,quarantine_allowed=False))
        with runtime(config) as (runner,outbox):
            outbox.prepare(self.crawl_for('omit',items),items)
            outbox.relay('omit')
            report = runner.work(runner.submit('omit')['job_id'])
            self.assertEqual(report['stages']['bronze']['counts']['payload_omitted'],1)
            self.assertEqual(list(outbox.store.values('quarantine_raw')),[])

    def test_retention_purges_quarantine_raw_then_metadata_and_logs_reads(self):
        bad = self.tampered(self.items[0])
        items = [bad]+self.items
        with runtime(self.config) as (runner,outbox):
            job = self.ready(runner,outbox,crawl=self.crawl_for('held',items),items=items)['job_id']
            runner.work(job)
            event = next(iter(outbox.store.values('quarantine_raw')))['event_id']
            self.assertEqual(runner.quarantine_read(event,'reprocessing analysis','operator-a')['payload'],bad)
            self.assertEqual(len(list(outbox.store.values('quarantine_access'))),1)
            early = runner.retention_purge('2026-11-01T00:00:00.000Z')
            self.assertEqual((early['quarantine_raw_deleted'],early['metadata_deleted']),(0,0))
            raw = runner.retention_purge('2026-11-16T01:00:00.000Z')
            self.assertEqual((raw['quarantine_raw_deleted'],raw['metadata_deleted']),(1,0))
            with self.assertRaisesRegex(Invalid,'NOT_FOUND'):
                runner.quarantine_read(event,'late read','operator-a')
            # Raw expiry does not break the audit; metadata stays until its own deadline.
            self.assertEqual(runner.audit(job)['status'],'AUDIT_OK')
            self.assertEqual(runner.retention_purge('2027-01-15T01:00:00.000Z')['metadata_deleted'],1)

    def test_publication_of_batch_n_overlaps_processing_of_batch_n_plus_1(self):
        old, new = self.history()
        with runtime(self.config) as (runner,outbox):
            first = self.ready(runner,outbox,crawl=self.crawl_for('older',[old]),items=[old])['job_id']
            second = self.ready(runner,outbox,crawl=self.crawl_for('newer',[new]),items=[new])['job_id']
            self.assertEqual(runner.work(first,'process')['status'],'GOLD_COMMITTED')
            original = runner.lock
            # Different writer locks: processing n+1 does not wait for publication n.
            with original('publish'):
                self.assertEqual(runner.work(second,'process')['status'],'GOLD_COMMITTED')
                with self.assertRaisesRegex(Invalid,'WRITER_BUSY'):
                    runner.work(first,'publish')
            self.assertEqual(runner.store.get('job',first)['status'],'GOLD_COMMITTED')
            # Publishing the newer batch first: the older batch cannot overwrite it.
            self.assertEqual(runner.work(second,'publish')['stages']['publication']['resolutions'],{'PUBLISHED':1})
            self.assertEqual(runner.work(first,'publish')['stages']['publication']['resolutions'],{'LAST_GOOD_RETAINED':1})
            third = self.ready(runner,outbox,crawl=self.crawl_for('third',[old]),items=[old])['job_id']
            with self.assertRaisesRegex(Invalid,'GOLD_NOT_COMMITTED'):
                runner.work(third,'publish')

    def test_retraction_repair_recomputes_then_withdraws_without_recrawl(self):
        old, new = self.history()
        product = 'danawa:11'
        with runtime(self.config) as (runner,outbox):
            job = self.ready(runner,outbox,crawl=self.crawl_for('both',[old,new]),items=[old,new])['job_id']
            runner.work(job)
            reads = runner.processor.metrics['source_reads']
            runner.retract(dict(product_key=product,raw_document_id=new['document']['_id'],reason='SOURCE_WITHDRAWN'))
            repaired = runner.repair([product])
            self.assertEqual(repaired['kind'],'REPAIR')
            report = runner.work(repaired['job_id'])
            self.assertEqual(report['stages']['publication']['resolutions'],{'PUBLISHED':1})
            self.assertIsNone(report['stages']['bronze'])
            current = read(next((self.root/'sql'/'current').glob('*.json')))['job_id']
            version = read(self.root/'sql'/'versions'/(current+'.json'))
            self.assertEqual([r['_id'] for r in version['source_refs']],[old['document']['_id']])
            self.assertEqual(version['retracted_raw_document_ids'],[new['document']['_id']])
            # Normalized results are reused; no source bodies are fetched again.
            self.assertEqual(runner.processor.metrics['source_reads'],reads)
            self.assertEqual(runner.repair([product])['job_id'],repaired['job_id'])
            runner.retract(dict(product_key=product,raw_document_id=old['document']['_id'],reason='SOURCE_WITHDRAWN'))
            final = runner.work(runner.repair([product])['job_id'])
            self.assertEqual(final['stages']['publication']['resolutions'],{'WITHDRAWN':1})
            self.assertEqual(final['stages']['gold']['counts']['source_retracted'],1)
            self.assertFalse(list((self.root/'sql'/'current').glob('*.json')))
            self.assertEqual(runner.audit(final['job_id'])['status'],'AUDIT_OK')

    def test_backlog_age_backpressure_and_index_rebuild(self):
        from datetime import datetime, timedelta, timezone
        config = dict(self.config,batch_limits={'pending_age_seconds':3600})
        with runtime(config) as (runner,outbox):
            job = self.ready(runner,outbox)['job_id']
            later = datetime.now(timezone.utc)+timedelta(hours=2)
            backlog = runner.backlog(now=later)
            self.assertEqual((backlog['pending_batches'],backlog['statuses']),(1,{'QUEUED':1}))
            self.assertTrue(backlog['saturated'])
            with self.assertRaisesRegex(Invalid,'BACKPRESSURE'):
                runner._reserve('another',1,now=later)
            runner.store.mutate('capacity','global',lambda old:dict(reservations={}))
            self.assertEqual(runner.backlog(rebuild=True)['jobs'][0]['job_id'],job)

    def test_submit_conflict_releases_capacity_and_changed_delivery_stays_queued(self):
        with runtime(self.config) as (runner,outbox):
            job = self.ready(runner,outbox)
            original = runner.store.mutate
            def conflict(kind,key,change):
                if kind == 'job':
                    raise Invalid('BATCH_REQUEST_CONFLICT')
                return original(kind,key,change)
            other = dict(self.crawl,crawl_batch_id='other')
            outbox.prepare(other,self.items)
            outbox.relay('other')
            with patch.object(runner.store,'mutate',side_effect=conflict):
                with self.assertRaisesRegex(Invalid,'CONFLICT'):
                    runner.submit('other')
            self.assertEqual(list(runner.store.get('capacity','global')['reservations']),[job['job_id']])
            runner.store.mutate('job',job['job_id'],lambda old:dict(old,delivery_hash='0'*64))
            with self.assertRaisesRegex(Invalid,'DELIVERY_CHANGED'):
                runner.work(job['job_id'])
            self.assertEqual(runner.store.get('job',job['job_id'])['status'],'QUEUED')

    def mysql_publisher(self, runner):
        from uuid import UUID
        from test_publication import MemoryDB
        from pipeline_v2.adapters.publication import MySQLPublisher

        class BatchDB(MemoryDB):
            def prefetch(self,table,keys):
                return None
            def update(self,table,key,values):
                matched = [r for r in self.rows[table] if all(r[k]==v for k,v in key.items())]
                if not matched and table != 'product_images':
                    raise AssertionError('missing row')
                for row in matched:
                    row.update(copy.deepcopy(values))
                return len(matched)

        publisher = MySQLPublisher(dict(target_id='test',database='test'))
        publisher.db = db = BatchDB()
        db.rows['categories'] = [dict(id=UUID(fixtures.CATEGORY).hex,code='CHAIR',active=True)]
        runner.publisher = publisher
        return db

    def stale_withdrawal(self, runner, outbox):
        old, new = self.history()
        first = self.ready(runner,outbox,crawl=self.crawl_for('older',[old]),items=[old])['job_id']
        runner.work(first)
        runner.retract(dict(product_key='danawa:11',raw_document_id=old['document']['_id'],reason='SOURCE_WITHDRAWN'))
        repair = runner.repair(['danawa:11'])['job_id']
        # Every retained observation is retracted at this moment, so this output is a withdrawal.
        self.assertEqual(runner.work(repair,'process')['stages']['gold']['counts']['source_retracted'],1)
        second = self.ready(runner,outbox,crawl=self.crawl_for('newer',[new]),items=[new])['job_id']
        self.assertEqual(runner.work(second)['stages']['publication']['resolutions'],{'PUBLISHED':1})
        return repair

    def test_stale_withdrawal_keeps_a_product_republished_with_newer_evidence(self):
        with runtime(self.config) as (runner,outbox):
            repair = self.stale_withdrawal(runner,outbox)
            report = runner.work(repair,'publish')
            self.assertEqual(report['stages']['publication']['resolutions'],{'LAST_GOOD_RETAINED':1})
            self.assertEqual(len(list((self.root/'sql'/'current').glob('*.json'))),1)
            self.assertEqual(runner.audit(repair)['status'],'AUDIT_OK')

    def test_mysql_stale_withdrawal_keeps_a_product_republished_with_newer_evidence(self):
        with runtime(self.config) as (runner,outbox):
            db = self.mysql_publisher(runner)
            repair = self.stale_withdrawal(runner,outbox)
            report = runner.work(repair,'publish')
            self.assertEqual(report['stages']['publication']['resolutions'],{'LAST_GOOD_RETAINED':1})
            product = db.rows['products'][0]
            self.assertEqual((product['status'],product['catalog_ready']),('ACTIVE',True))
            self.assertTrue(any(r['active'] for r in db.rows['product_images']))
            self.assertEqual(runner.audit(repair)['status'],'AUDIT_OK')

    def test_retry_after_lost_stage_manifest_reads_the_same_history(self):
        old, new = self.history()
        with runtime(self.config) as (runner,outbox):
            first = self.ready(runner,outbox,crawl=self.crawl_for('older',[old]),items=[old])['job_id']
            second = self.ready(runner,outbox,crawl=self.crawl_for('newer',[new]),items=[new])['job_id']
            original = runner.store.put
            def lose_silver_manifest(kind,key,value):
                if kind == 'stage' and key == [first,'silver']:
                    raise OSError('manifest write lost')
                return original(kind,key,value)
            with patch.object(runner.store,'put',side_effect=lose_silver_manifest):
                with self.assertRaises(OSError):
                    runner.work(first,'process')
            # The later batch completes Bronze while the earlier one waits for recovery.
            self.assertEqual(runner.work(second,'process')['status'],'GOLD_COMMITTED')
            runner.recover(first,'worker stopped')
            retried = runner.work(first,'process')
            self.assertEqual(retried['status'],'GOLD_COMMITTED')
            self.assertEqual(retried['stages']['silver']['counts']['valid'],1)

    def test_oversized_product_history_holds_only_that_product(self):
        old, new = self.history()
        others = self.items[1:]
        config = dict(self.config,batch_limits={'history_observations':1})
        with runtime(config) as (runner,outbox):
            first = self.ready(runner,outbox,crawl=self.crawl_for('older',[old]+others),items=[old]+others)['job_id']
            self.assertEqual(runner.work(first)['stages']['publication']['resolutions'],{'PUBLISHED':5})
            current = {p.name:read(p)['job_id'] for p in (self.root/'sql'/'current').glob('*.json')}
            # The second observation of danawa:11 pushes its history over the limit.
            second = self.ready(runner,outbox,crawl=self.crawl_for('newer',[new]),items=[new])['job_id']
            report = runner.work(second)
            self.assertEqual(report['status'],'COMPLETED')
            self.assertEqual(report['stages']['silver']['counts']['history_limit'],1)
            self.assertEqual(report['stages']['silver']['counts']['quality_hold'],1)
            self.assertEqual(report['stages']['publication']['resolutions'],{})
            # The previously published version stays; nothing was published or hidden for it.
            self.assertEqual({p.name:read(p)['job_id'] for p in (self.root/'sql'/'current').glob('*.json')},current)
            # The same batch size with a product that fits is unaffected.
            third = self.ready(runner,outbox,crawl=self.crawl_for('single',[others[0]]),items=[others[0]])['job_id']
            self.assertEqual(runner.work(third)['stages']['silver']['counts']['history_limit'],0)
            self.assertEqual(runner.audit(second)['status'],'AUDIT_OK')

    def test_relay_publishes_in_bounded_groups_and_keeps_confirmed_acks(self):
        config = dict(self.config,batch_limits={'outbox_chunk':2})
        with runtime(config) as (runner,outbox):
            outbox.prepare(self.crawl,self.items)
            original = outbox.producer.send_many
            sizes = []
            def counted(pairs):
                pairs = list(pairs)
                sizes.append(len(pairs))
                return original(pairs)
            with patch.object(outbox.producer,'send_many',side_effect=counted):
                outbox.relay('crawl-1')
            # 5 events, groups of 2: one wait per group, not one per message.
            self.assertEqual(sizes,[2,2,1])
            sizes.clear()
            outbox.relay('crawl-1',generation=1)
            self.assertEqual(sizes,[])

    def test_unconfirmed_message_keeps_other_acks_and_only_it_is_resent(self):
        config = dict(self.config,batch_limits={'outbox_chunk':2})
        with runtime(config) as (runner,outbox):
            outbox.prepare(self.crawl,self.items)
            original = outbox.producer.send_many
            sizes, lose = [], [True]
            def flaky(pairs):
                pairs = list(pairs)
                sizes.append(len(pairs))
                acks = original(pairs)
                if lose[0]:
                    lose[0] = False
                    acks[0] = None
                return acks
            with patch.object(outbox.producer,'send_many',side_effect=flaky):
                with self.assertRaisesRegex(Invalid,'DELIVERY_UNCONFIRMED'):
                    outbox.relay('crawl-1')
                manifest = outbox.relay('crawl-1')
            self.assertEqual(sizes,[2,1,2,1])
            self.assertEqual(manifest['event_count'],5)
            self.assertEqual(runner.work(runner.submit('crawl-1')['job_id'])['status'],'COMPLETED')

    def test_partial_retraction_recompute_never_replaces_newer_published_evidence(self):
        o1, o2, o3 = self.history(3)
        product = 'danawa:11'
        with runtime(self.config) as (runner,outbox):
            db = self.mysql_publisher(runner)
            first = self.ready(runner,outbox,crawl=self.crawl_for('first',[o1,o2]),items=[o1,o2])['job_id']
            runner.work(first)
            runner.retract(dict(product_key=product,raw_document_id=o1['document']['_id'],reason='SOURCE_WITHDRAWN'))
            repair = runner.repair([product])['job_id']
            # Recomputed from the evidence that remained at that moment: without the third observation.
            self.assertEqual(runner.work(repair,'process')['stages']['gold']['counts']['output_committed'],1)
            newer = self.ready(runner,outbox,crawl=self.crawl_for('newer',[o3]),items=[o3])['job_id']
            self.assertEqual(runner.work(newer)['stages']['publication']['resolutions'],{'PUBLISHED':1})
            current = dict(db.rows['pipeline_product_versions'][0])
            self.assertEqual(runner.work(repair,'publish')['stages']['publication']['resolutions'],{'LAST_GOOD_RETAINED':1})
            self.assertEqual(db.rows['pipeline_product_versions'][0]['latest_as_of'],current['latest_as_of'])
            self.assertEqual(db.rows['pipeline_product_versions'][0]['latest_evaluation'],current['latest_evaluation'])
            self.assertEqual(runner.audit(repair)['status'],'AUDIT_OK')

    def test_image_recheck_time_alone_is_the_same_event(self):
        with runtime(self.config) as (runner,outbox):
            first = self.ready(runner,outbox)['job_id']
            runner.work(first)
            writes = runner.publisher.writes
            rechecked = copy.deepcopy(self.items)
            for item in rechecked:
                item['image_state']['image_state_checked_at'] = '2026-10-17T00:00:00.000Z'
            source = outbox.prepare(dict(self.crawl,crawl_batch_id='recheck'),rechecked)
            # Diagnostics changed, business content did not: same events, nothing to reprocess.
            self.assertEqual(source['event_set_hash'],outbox.store.get('crawl','crawl-1')['event_set_hash'])
            outbox.relay('recheck')
            self.assertEqual(runner.work(runner.submit('recheck')['job_id'])['status'],'COMPLETED')
            self.assertEqual(runner.publisher.writes,writes)

    def test_metric_storage_failure_does_not_undo_completed_publication(self):
        with runtime(self.config) as (runner,outbox):
            job = self.ready(runner,outbox)
            with patch('pipeline_v2.adapters.timing.immutable',side_effect=OSError('metrics volume full')):
                report = runner.work(job['job_id'])
            self.assertEqual(report['status'],'COMPLETED')
            self.assertGreater(report['metric_failures'],0)
            self.assertEqual(runner.store.get('job',job['job_id'])['status'],'COMPLETED')
            self.assertEqual(runner.audit(job['job_id'])['status'],'AUDIT_OK')

    def test_mysql_publication_keeps_newer_version_and_hides_fully_retracted_product(self):
        old, new = self.history()
        with runtime(self.config) as (runner,outbox):
            db = self.mysql_publisher(runner)
            first = self.ready(runner,outbox,crawl=self.crawl_for('older',[old]),items=[old])['job_id']
            second = self.ready(runner,outbox,crawl=self.crawl_for('newer',[new]),items=[new])['job_id']
            runner.work(first,'process')
            runner.work(second,'process')
            self.assertEqual(runner.work(second,'publish')['stages']['publication']['resolutions'],{'PUBLISHED':1})
            self.assertEqual(runner.work(first,'publish')['stages']['publication']['resolutions'],{'LAST_GOOD_RETAINED':1})
            # Batch n+1 already combined both observations; batch n is retained, not replayed over it.
            refs = json.loads(db.rows['products'][0]['source_refs_json'])
            self.assertEqual({r['_id'] for r in refs},{old['document']['_id'],new['document']['_id']})
            self.assertEqual(db.rows['pipeline_product_versions'][0]['latest_evaluation'],
                runner.store.get('stage',[second,'publication'])['receipts'][0])
            for raw in (old,new):
                runner.retract(dict(product_key='danawa:11',raw_document_id=raw['document']['_id'],reason='SOURCE_WITHDRAWN'))
            final = runner.work(runner.repair(['danawa:11'])['job_id'])
            self.assertEqual(final['stages']['publication']['resolutions'],{'WITHDRAWN':1})
            product = db.rows['products'][0]
            self.assertEqual((product['status'],product['catalog_ready'],product['publication_status']),
                ('INACTIVE',False,'SOURCE_RETRACTED'))
            self.assertFalse(any(r['active'] for r in db.rows['product_images']))
            self.assertEqual(runner.audit(final['job_id'])['status'],'AUDIT_OK')

    def test_cli_phase_backlog_retraction_retention_and_restricted_read(self):
        import io
        import hashlib
        from contextlib import redirect_stdout
        from pipeline_v2.adapters.files import write
        from pipeline_v2.interfaces.cli import main
        bad = self.tampered(self.items[0])
        items = [bad]+self.items
        write(self.root/'input.json',items)
        crawl = dict(self.crawl_for('cli',items),input_sha256=hashlib.sha256((self.root/'input.json').read_bytes()).hexdigest())
        write(self.root/'config.json',self.config)
        write(self.root/'crawl.json',crawl)
        common = ['--config',str(self.root/'config.json')]
        output = io.StringIO()
        def run(*args):
            with redirect_stdout(output):
                code = main(common+list(args))
            return code, json.loads(output.getvalue().splitlines()[-1])
        self.assertEqual(run('batch-prepare','--manifest',str(self.root/'crawl.json'),'--input',str(self.root/'input.json'))[0],0)
        run('batch-relay','--batch','cli')
        job = run('batch-submit','--batch','cli')[1]['job_id']
        self.assertEqual(run('batch-backlog')[1]['pending_batches'],1)
        self.assertEqual(run('batch-work','--job',job,'--phase','process')[1]['status'],'GOLD_COMMITTED')
        self.assertEqual(run('batch-work','--job',job,'--phase','publish')[1]['status'],'COMPLETED')
        event = next(iter(json.loads(p.read_text(encoding='utf-8'))['value']['event_id']
            for p in (self.root/'state'/'batches-v1'/'quarantine_raw').glob('*.json')))
        code, result = run('batch-quarantine-read','--event-id',event,'--purpose','analysis','--operator','op',
            '--output',str(self.root/'held.json'))
        self.assertEqual((code,result['status']),(0,'QUARANTINE_RECORD_WRITTEN'))
        self.assertNotIn('tampered-without-reseal',output.getvalue())
        self.assertEqual(read(self.root/'held.json')['payload'],bad)
        product, raw = 'danawa:12', self.items[1]['document']['_id']
        self.assertEqual(run('batch-retract','--product-key',product,'--raw-document-id',raw,'--reason','SOURCE_WITHDRAWN')[0],0)
        repair = run('batch-repair','--products',product)[1]['job_id']
        self.assertEqual(run('batch-work','--job',repair)[1]['stages']['publication']['resolutions'],{'WITHDRAWN':1})
        self.assertEqual(run('batch-retention','--as-of','2026-11-16T01:00:00.000Z')[1]['quarantine_raw_deleted'],1)
        with self.assertRaises(SystemExit), redirect_stdout(io.StringIO()), patch('sys.stderr',io.StringIO()):
            main(common+['batch-work','--job',job,'--phase','bogus'])


if __name__ == '__main__':
    unittest.main()
