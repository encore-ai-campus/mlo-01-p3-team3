import copy
import json
import unittest
from unittest.mock import patch
from uuid import UUID

import test_engine as fixtures
from test_publication import MemoryDB
from pipeline_v2.adapters.publication import MySQLPublisher
from pipeline_v2.adapters.files import read
from pipeline_v2.adapters.sources import JsonSource
from pipeline_v2.application.capture import capture
from pipeline_v2.domain.contracts import Invalid, Limits


class DirectSQLTests(unittest.TestCase):
    setUp=fixtures.EngineTests.setUp
    tearDown=fixtures.EngineTests.tearDown
    engine=fixtures.EngineTests.engine
    finish=fixtures.EngineTests.finish
    def sql_engine(self):
        engine=self.engine()
        publisher=MySQLPublisher(dict(target_id='test',database='test'))
        publisher.db=MemoryDB()
        publisher.db.rows['categories']=[dict(id=UUID(fixtures.CATEGORY).hex,code='CHAIR',active=True)]
        engine.publisher=publisher
        return engine,publisher.db

    def test_batched_readback_records_equal_the_per_product_readback(self):
        from pipeline_v2.adapters.sql_audit import service_rows, service_rows_many
        engine,db=self.sql_engine();self.assertEqual(self.finish(engine)['status'],'COMPLETED')
        key=engine._index(engine.state.scope('initial'),0)[0]
        delivery=read(engine.state.root/'jobs'/key/'delivery.json')
        product=engine.processor.materialize(delivery['gold'])
        items=[(product,key)]
        self.assertEqual(service_rows_many(db,items),[service_rows(db,product,key)])

    def test_sql_direct_write_strict_schema_and_tamper_audit(self):
        engine,db=self.sql_engine();result=self.finish(engine)
        self.assertEqual(result['status'],'COMPLETED')
        product=db.rows['products'][0]
        self.assertEqual(product['manufacturer'],'제조사')
        self.assertEqual(product['status'],'ACTIVE')
        self.assertTrue(product['representative_image_id'])
        self.assertEqual(json.loads(product['gold_snapshot_ref_json'])['schema_version'],3)
        self.assertFalse(db.rows['pipeline_publication_guards'])
        self.assertTrue(all(r['active'] for r in db.rows['product_images']))
        key=engine._index(engine.state.scope('initial'),0)[0]
        delivery=read(engine.state.root/'jobs'/key/'delivery.json')
        delivery['product']=engine.processor.materialize(delivery['gold'])
        receipt=engine.state.job(key)['publication']
        db.rows['assets'][0]['checksum']='changed'
        with self.assertRaisesRegex(Invalid,'READBACK_CHANGED'):engine.publisher.audit(delivery,receipt)

    def test_sql_response_loss_reuses_transaction_and_schema_check(self):
        engine,db=self.sql_engine();engine.tick('initial')
        publish=engine.publisher.publish
        def lost(delivery):publish(delivery);raise OSError('lost')
        with patch.object(db,'preflight',wraps=db.preflight) as preflight:
            with patch.object(engine.publisher,'publish',side_effect=lost):engine.tick('initial')
            key=engine.state.pending()[0]['job_id'];engine.state.update(key,next_retry_at='')
            self.assertEqual(self.finish(engine)['status'],'COMPLETED')
            self.assertEqual(preflight.call_count,1)
        self.assertEqual(len(db.rows['products']),1)
        self.assertEqual(len(db.rows['pipeline_publications']),1)

    def test_unchanged_scope_has_zero_new_data_writes(self):
        engine=self.engine();self.finish(engine)
        before=copy.deepcopy(engine.lake.metrics);writes=engine.publisher.writes
        normalized=engine.processor.metrics['normalized'];reads=engine.processor.metrics['source_rows']
        capture(engine.state,engine.source,'unchanged',fixtures.POLICY,engine.limits)
        self.finish(engine,'unchanged')
        self.assertEqual(engine.lake.metrics['commits'],before['commits'])
        self.assertEqual(engine.lake.metrics['written_rows'],before['written_rows'])
        self.assertEqual(engine.publisher.writes,writes)
        self.assertEqual(engine.processor.metrics['normalized'],normalized)
        self.assertEqual(engine.processor.metrics['source_rows'],reads)

    def test_changed_product_only_gets_new_normalization(self):
        engine=self.engine(products=2);self.finish(engine)
        original=engine.processor.metrics['normalized']
        changed=read(self.root/'source.json');changed[0]['image_state']['image_state_revision']+=1
        from pipeline_v2.adapters.files import write
        write(self.root/'source.json',changed)
        engine.source=engine.processor.source=JsonSource(self.root/'source.json')
        capture(engine.state,engine.source,'changed-one',fixtures.POLICY,engine.limits)
        self.assertEqual(self.finish(engine,'changed-one')['status'],'COMPLETED')
        self.assertEqual(engine.processor.metrics['normalized']-original,1)

    def test_recovery_rewinds_cursor_after_history_block(self):
        engine=self.engine(history_rows=1);self.finish(engine)
        key=engine._index(engine.state.scope('initial'),0)[0]
        engine.limits=engine.processor.limits=Limits(min_free_bytes=1)
        engine.recover(key,'operator://stopped-and-increased-history-cap')
        self.assertEqual(self.finish(engine)['status'],'COMPLETED')

    def test_completed_scope_status_detects_service_tamper(self):
        engine,db=self.sql_engine();self.finish(engine)
        db.rows['products'][0]['manufacturer']='changed'
        with self.assertRaisesRegex(Invalid,'READBACK_CHANGED'):engine.status('initial')

    def test_missing_new_fields_preserve_values_lineage_and_image(self):
        engine,db=self.sql_engine();self.finish(engine)
        key=engine._index(engine.state.scope('initial'),0)[0]
        group=engine.processor.materialize(engine.state.receipt(key,'gold'))
        before=copy.deepcopy(db.rows['products'][0])
        group.update(manufacturer=None,comparison_url=None,images=[])
        group['lineage'].update(manufacturer=None,comparison_url=None)
        with db.transaction():engine.publisher._product(db,group,'b'*64,before['updated_at'],before)
        saved=db.rows['products'][0]
        self.assertEqual(saved['id'],before['id'])
        for field in ('manufacturer','comparison_url','representative_image_id'):
            self.assertEqual(saved[field],before[field])
        self.assertEqual(json.loads(saved['field_lineage_json'])['comparison_url'],json.loads(before['field_lineage_json'])['comparison_url'])
        self.assertEqual(json.loads(saved['field_status_json'])['manufacturer'],'CONFIRMED')



class DirectMySQLIdentityTests(unittest.TestCase):
    def transport(self):
        from pipeline_v2.adapters.mysql import DirectMySQL
        db = DirectMySQL.__new__(DirectMySQL)
        db._read_cache, db._pending, db.executed = {}, {}, []
        db.execute = lambda sql, args=(): db.executed.append((sql, args)) or 1
        return db

    def test_absent_rows_wait_in_a_buffer_and_go_in_one_insert_per_table(self):
        db = self.transport()
        links = [dict(product_id='p1', link_observation_key='l%d' % i, gold_evaluation_key='j1') for i in range(3)]
        saved = [db.identity_absent('product_link_observations', key, dict(source_url='u')) for key in links]
        db.identity_absent('product_images', dict(product_id='p1', source_code='danawa', source_product_id='1',
            image_variant='representative', raw_document_id='r', source_version='v'), dict(asset_id='a'))
        self.assertTrue(all(created for _, created in saved))
        self.assertEqual(db.executed, [])
        # Served from the transaction cache before the write.
        self.assertEqual(db.get('product_link_observations', links[0]), saved[0][0])
        db.flush()
        self.assertEqual([sql.split('(')[0].strip() for sql, _ in db.executed],
            ['INSERT INTO `product_link_observations`', 'INSERT INTO `product_images`'])
        self.assertEqual(db.executed[0][0].count('),('), 2)
        self.assertEqual(db._pending, {})

    def absent(self, db, table, key):
        # What prefetch() records for a key it found no row for.
        from pipeline_v2.domain.contracts import canonical
        db._read_cache[(table, canonical(key), True)] = None

    def test_prefetched_absent_rows_of_many_products_go_in_one_parent_first_insert_per_table(self):
        db = self.transport()
        db._deferred = []
        images = []
        for n in ('1', '2'):
            product = dict(product_key='danawa:' + n)
            self.absent(db, 'products', product)
            saved, created = db.identity('products', product, dict(name='p' + n, representative_image_id=None))
            self.assertTrue(created)
            asset = dict(bucket='b', object_key='o' + n)
            self.absent(db, 'assets', asset)
            asset_row, _ = db.identity('assets', asset, dict(kind='PRODUCT_IMAGE'))
            image = dict(product_id=saved['id'], source_code='danawa', source_product_id=n, image_variant='representative',
                raw_document_id='r', source_version='v')
            self.absent(db, 'product_images', image)
            linked, _ = db.identity('product_images', image, dict(asset_id=asset_row['id']))
            images.append(linked['id'])
            # The new product's representative image is not stored yet: the update waits for it.
            db.update('products', dict(id=saved['id']), dict(representative_image_id=linked['id']))
        self.assertEqual(db.executed, [])
        db.flush()
        statements = [sql.split(' SET')[0].split('(')[0].strip() for sql, _ in db.executed]
        self.assertEqual(statements, ['INSERT INTO `products`', 'INSERT INTO `assets`', 'INSERT INTO `product_images`',
            'UPDATE `products`', 'UPDATE `products`'])
        self.assertEqual(db.executed[0][0].count('),('), 1)
        self.assertEqual([args[0] for _, args in db.executed[3:]], images)

    def test_an_update_of_a_buffered_row_goes_into_its_insert(self):
        db = self.transport()
        db._deferred = []
        offer = dict(offer_key='k')
        self.absent(db, 'offers', offer)
        saved, _ = db.identity('offers', offer, dict(price_amount=1))
        db.update('offers', dict(id=saved['id']), dict(price_amount=2))
        db.flush()
        self.assertEqual(len(db.executed), 1)
        sql, args = db.executed[0]
        self.assertIn(2, args)
        self.assertNotIn(1, args)

    def test_a_duplicate_in_a_buffered_insert_keeps_the_held_id_for_absent_rows(self):
        import pymysql
        db = self.transport()
        db._deferred = []
        calls = []
        def execute(sql, args=()):
            calls.append((sql, args))
            if '),(' in sql and sql.startswith('INSERT'):
                raise pymysql.err.IntegrityError(1062, 'Duplicate entry')
            return 1
        db.execute = execute
        db.one = lambda sql, args=(): None
        held = [db.identity_absent('offers', dict(offer_key=k), dict(price_amount=1))[0]['id'] for k in ('a', 'b')]
        db.flush()
        # Both rows were absent after all (the duplicate was elsewhere): each stored with its held id.
        self.assertEqual(len(calls), 3)
        self.assertEqual([args[list(args).index(h)] for (_, args), h in zip(calls[1:], held)], held)

    def test_a_duplicate_in_a_buffered_insert_is_checked_row_by_row(self):
        import pymysql
        from pipeline_v2.domain.contracts import Invalid
        db = self.transport()
        def duplicate(sql, args=()):
            raise pymysql.err.IntegrityError(1062, 'Duplicate entry')
        db.execute = duplicate
        key = dict(product_id='p1', link_observation_key='l1', gold_evaluation_key='j1')
        db.identity_absent('product_link_observations', key, dict(source_url='u'))
        db.one = lambda sql, args=(): dict(key, id='existing')
        # The caller already holds the buffered id, so another stored row is a conflict.
        with self.assertRaisesRegex(Invalid, 'MYSQL_DEFERRED_INSERT_CONFLICT'):
            db.flush()


if __name__=='__main__':unittest.main()
