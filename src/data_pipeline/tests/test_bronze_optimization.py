"""Batch I/O must preserve replay, poison isolation and lease fencing."""
import copy,json,sys,unittest
from threading import Thread
from types import SimpleNamespace
from unittest.mock import MagicMock,patch
from product_pipeline.bronze.delta import DeltaSink
from product_pipeline.bronze.mongo import MongoSource
from product_pipeline.bronze.metrics import Profile,bind,timed,current
from product_pipeline.bronze.batch import process_batch
from product_pipeline.bronze.contracts import Poison
from product_pipeline.errors import Conflict,PipelineError


class AppendTests(unittest.TestCase):
    def sink(self):
        sink=DeltaSink.__new__(DeltaSink);sink.settings=object();sink.lease=MagicMock();sink._prepared={}
        self.durable={};self.reads=[];self.writes=[]
        def read(table,keys):
            self.reads.append(set(keys));return copy.deepcopy({k:v for k,v in self.durable.items() if k in keys})
        def append(table,rows):
            self.writes.append(copy.deepcopy(rows));self.durable.update({r['event_id']:copy.deepcopy(r) for r in rows})
        sink.existing=read;sink._append_chunk=append
        return sink
    def rows(self,n=23):
        return [dict(event_id=str(i),event_hash=str(i),event_json='{}',raw_document_id=str(i),document_hash='h') for i in range(n)]
    def test_one_preflight_and_one_confirmation_for_many_chunks(self):
        sink=self.sink();rows=self.rows();sink.insert('events',rows)
        self.assertEqual(len(self.reads),2);self.assertEqual(list(map(len,self.writes)),[10,10,3])
        sink.insert('events',rows);self.assertEqual(len(self.reads),3);self.assertEqual(len(self.writes),3)
    def test_batch_preflight_reused_once_and_uncertain_append_is_reread(self):
        sink=self.sink();rows=self.rows(1);sink._prepared={'events':({'0'}, {})}
        def lost(table,values):
            self.durable['0']=copy.deepcopy(values[0]);raise OSError('lost response')
        sink._append_chunk=lost
        with self.assertRaises(OSError):sink.insert('events',rows)
        self.assertNotIn('events',sink._prepared)
        sink.insert('events',rows)
        self.assertEqual(len(self.reads),1);self.assertEqual(len(self.durable),1)
    def test_conflict_anywhere_prevents_first_append_and_duplicate_rejected(self):
        sink=self.sink();rows=self.rows();self.durable['22']=dict(rows[-1],event_hash='changed')
        with self.assertRaises(Conflict):sink.insert('events',rows)
        self.assertFalse(self.writes)
        with self.assertRaises(Conflict):sink.insert('events',[rows[0],rows[0]])
    def test_storage_confirmation_failure_never_succeeds(self):
        sink=self.sink();sink._append_chunk=lambda *args:None
        with self.assertRaisesRegex(Conflict,'BRONZE_DELTA_COMMIT_UNCONFIRMED'):sink.insert('events',self.rows(1))
    def test_production_write_is_append_without_merge(self):
        sink=self.sink();sink.spark=MagicMock();sink.paths={'events':'s3a://isolated/events'};sink.schemas={'events':[]}
        reader=sink.spark.read.schema.return_value;reader.option.return_value=reader
        functions=MagicMock()
        modules={'pyspark.sql':SimpleNamespace(functions=functions),'pyspark.sql.types':SimpleNamespace(ArrayType=lambda x:x)}
        with patch.dict(sys.modules,modules):DeltaSink._append_chunk(sink,'events',self.rows(1))
        writer=reader.json.return_value.write
        writer.format.assert_called_once_with('delta');writer.format.return_value.mode.assert_called_once_with('append')
        writer.format.return_value.mode.return_value.option.return_value.save.assert_called_once_with('s3a://isolated/events')


class MongoBatchTests(unittest.TestCase):
    def source(self,docs):
        source=MongoSource.__new__(MongoSource);source.raw=MagicMock();cursor=MagicMock()
        source.raw.find.return_value.limit.return_value.batch_size.return_value=cursor
        cursor.__iter__.side_effect=lambda:iter(docs)
        self.cursor=cursor;return source
    def test_bounded_in_lookup_closes_cursor_and_checks_missing(self):
        keys=['0'*23+'1','0'*23+'2'];docs=[{'_id':key} for key in keys];source=self.source(docs)
        with patch('product_pipeline.bronze.mongo.wire_document',side_effect=lambda x:x):
            values=source.get_many(keys,max_bytes=1024)
        self.assertEqual(set(values),set(keys));source.raw.find.assert_called_once();self.cursor.close.assert_called_once()
        self.assertEqual(len(source.raw.find.call_args.args[0]['_id']['$in']),2)
        source=self.source(docs[:1])
        with patch('product_pipeline.bronze.mongo.wire_document',side_effect=lambda x:x),self.assertRaisesRegex(PipelineError,'BRONZE_RAW_UNCONFIRMED'):
            source.get_many(keys,max_bytes=1024)
        self.cursor.close.assert_called_once()
    def test_bad_row_is_poison_but_budget_and_database_failure_are_retry(self):
        key='0'*24;source=self.source([{'_id':key}])
        with patch('product_pipeline.bronze.mongo.wire_document',side_effect=ValueError('malformed')):
            self.assertIsInstance(source.get_many([key],max_bytes=1024)[key],Poison)
        source=self.source([{'_id':key}])
        with patch('product_pipeline.bronze.mongo.wire_document',side_effect=lambda x:x),self.assertRaisesRegex(PipelineError,'RAW_LIMIT'):
            source.get_many([key],max_bytes=1)
        source=self.source([]);source.raw.find.side_effect=OSError('unavailable')
        with self.assertRaises(OSError):source.get_many([key],max_bytes=1024)
    def test_batch_pipeline_uses_one_bulk_read_and_still_validates_each_event(self):
        from tests.test_bronze import BronzeTests
        fixture=BronzeTests();fixture.setUp();self.addCleanup(fixture.tearDown)
        bulk=MagicMock(return_value={fixture.doc['_id']:fixture.doc});fixture.source.get_many=bulk
        result=fixture.run_batch([fixture.message(),fixture.message(offset=1)])
        self.assertEqual(result['accepted'],2);bulk.assert_called_once()
        changed=copy.deepcopy(fixture.event);changed['payload']['document_hash']='0'*64
        result=fixture.run_batch([fixture.message(changed,offset=2)])
        self.assertEqual(result['quarantined'],1);self.assertEqual(len(fixture.sink.raw),1)


class ProfileTests(unittest.TestCase):
    def test_callback_thread_bound_to_invocation_and_failure_time_recorded(self):
        with Profile() as profile:
            def callback():
                self.assertIsNone(current())
                with bind(profile),timed('callback'):pass
            thread=Thread(target=callback);thread.start();thread.join()
            with self.assertRaises(OSError):
                with timed('failed'):raise OSError('injected')
        self.assertIsNone(current());self.assertEqual(profile.snapshot()['callback_calls'],1)
        self.assertEqual(profile.snapshot()['failed_calls'],1)


class DeltaLookupTests(unittest.TestCase):
    def test_one_raw_scan_for_document_and_observation_keys(self):
        sink=DeltaSink.__new__(DeltaSink);sink.paths={'raw':'raw-path','events':'event-path'};sink._prepared={}
        frame=MagicMock();frame.filter.return_value=frame;frame.limit.return_value=frame
        frame.withColumn.return_value=frame;frame.select.return_value=frame;frame.unionByName.return_value=frame
        frame.columns=['raw_document_id','observation_key']
        raw=dict(raw_document_id='r1',observation_key='o1',collected_at='2026-10-21T00:00:00.000Z')
        event=dict(event_id='e1')
        frame.collect.return_value=[{'_table':'raw','_row':json.dumps(raw)},{'_table':'events','_row':json.dumps(event)}]
        sink.spark=MagicMock();sink.spark.read.format.return_value.load.return_value=frame
        with patch('pyspark.sql.functions.col'),patch('pyspark.sql.functions.lit'),patch('pyspark.sql.functions.date_format'),patch('pyspark.sql.functions.to_json'),patch('pyspark.sql.functions.struct'):
            a,b,c=sink.batch_lookups({'r1'},{'e1'},{'o1'})
        self.assertEqual(a,{'r1':raw});self.assertEqual(c,{'o1':raw});self.assertEqual(b,{'e1':event})
        self.assertEqual(sink.spark.read.format.return_value.load.call_count,2);frame.collect.assert_called_once()
    def test_existing_targets_skip_create_and_metadata_collected_once(self):
        from pyspark.sql.types import StructType,StructField,StringType,TimestampType,IntegerType,LongType
        from product_pipeline.bronze.delta import SPECS
        schemas={t:StructType([StructField(k,{'s':StringType,'t':TimestampType,'i':IntegerType,'l':LongType}[kind](),True) for k,kind in fields]) for t,(_,fields) in SPECS.items()}
        spark=MagicMock();spark._jvm.org.apache.hadoop.fs.Path.side_effect=lambda p:SimpleNamespace(toString=lambda:p)
        paths={t:t+'-path' for t in SPECS}
        spark.read.format.return_value.load.side_effect=lambda p:SimpleNamespace(schema=schemas[next(t for t in SPECS if paths[t]==p)])
        meta=MagicMock();meta.unionByName.return_value=meta
        meta.collect.return_value=[{'_table':t,'properties':{'pipeline.targetFingerprint':'hash','delta.appendOnly':'true'}} for t in SPECS]
        settings=SimpleNamespace(bronze_path=paths['raw'],events_path=paths['events'],quarantine_path=paths['quarantine'],fingerprint='hash')
        def metadata(spark,path):
            table=next(t for t in SPECS if paths[t]==path)
            return dict(schema=schemas[table],properties=meta.collect.return_value[list(SPECS).index(table)]['properties'],version=1,table_id=table)
        with patch('delta.tables.DeltaTable') as delta,patch('product_pipeline.bronze.metadata.read_metadata',side_effect=metadata) as read:
            DeltaSink(spark,settings,lease=MagicMock())
            delta.createIfNotExists.assert_not_called();delta.forPath.assert_not_called()
            spark.read.format.assert_not_called();self.assertEqual(read.call_count,3)
            meta.collect.return_value[-1]['properties']['delta.appendOnly']='false'
            with self.assertRaises(Conflict):DeltaSink(spark,settings,lease=MagicMock())


class SnapshotMetadataTests(unittest.TestCase):
    def test_fresh_snapshot_supplies_schema_and_properties_without_data_jobs(self):
        from product_pipeline.bronze.metadata import read_metadata
        spark=MagicMock();log=spark._jvm.org.apache.spark.sql.delta.DeltaLog.forTable.return_value
        snap=log.update.return_value;snap.version.return_value=7
        snap.schema.return_value.json.return_value=json.dumps({'type':'struct','fields':[]})
        snap.metadata.return_value.id.return_value='table-id'
        config=snap.metadata.return_value.configuration.return_value
        values={'delta.appendOnly':'true','pipeline.targetFingerprint':'expected'}
        config.get.side_effect=lambda k:SimpleNamespace(isDefined=lambda:k in values,get=lambda:values[k])
        value=read_metadata(spark,'s3a://test/raw')
        self.assertEqual(value['properties'],values);self.assertEqual(value['version'],7)
        self.assertFalse(log.update.call_args.args[0]);spark.read.format.assert_not_called()
        self.assertEqual(value['table_id'],'table-id')
    def test_only_negative_version_is_missing_metadata_errors_never_create(self):
        from product_pipeline.bronze.metadata import read_metadata
        spark=MagicMock();log=spark._jvm.org.apache.spark.sql.delta.DeltaLog.forTable.return_value
        log.update.return_value.version.return_value=-1
        self.assertIsNone(read_metadata(spark,'s3a://new/raw'))
        log.update.side_effect=OSError('unavailable')
        with self.assertRaisesRegex(PipelineError,'BRONZE_DELTA_METADATA_UNCONFIRMED'):read_metadata(spark,'s3a://existing/raw')


class SharedDriverTests(unittest.TestCase):
    def load(self,name,work):
        import importlib.util
        from pathlib import Path
        path=Path(__file__).resolve().parents[1]/'docs/server_setup'/name
        with patch.dict(sys.modules,production_work=work):
            spec=importlib.util.spec_from_file_location('bronze_shared_'+name[:-3],path)
            module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
        return module
    def test_window_stage_borrows_scope_and_restores_root_on_failure(self):
        from pathlib import Path
        from tempfile import TemporaryDirectory
        from product_pipeline.spark_session import SessionScope
        with TemporaryDirectory() as d:
            root=Path(d);work=SimpleNamespace(ROOT=root,setup=MagicMock(),pinned=MagicMock(),LAKE='s3a://test',BASE=root)
            job=self.load('production_window_work.py',work)
            with patch.object(job,'run',side_effect=OSError('stage failed')),SessionScope():
                with self.assertRaises(OSError):job.run_at(root/'epoch')
            self.assertEqual(job.ROOT,root);self.assertEqual(work.ROOT,root)
    def test_bronze_and_windows_share_session_and_error_restores_environment(self):
        from pathlib import Path
        import os
        from product_pipeline.spark_session import SessionScope,get_or_create,release_spark
        spark=MagicMock();spark.streams.active=[]
        spark.sparkContext.statusTracker.return_value.getActiveJobsIds.return_value=[]
        spark.sparkContext.statusTracker.return_value.getActiveStageIds.return_value=[]
        builder=MagicMock();builder.getOrCreate.return_value=spark
        def use(*args):get_or_create(builder,{'spark.master':'spark://test'});release_spark(spark)
        work=SimpleNamespace(setup=lambda:None,BASE=Path('/tmp'),run_at=MagicMock(side_effect=use))
        job=self.load('production_continuous_work.py',work)
        windows=SimpleNamespace(run_at=MagicMock(side_effect=lambda *args:use() or 0))
        with SessionScope(),patch.dict(sys.modules,production_work=work,production_window_work=windows),patch.dict(os.environ,PIPELINE_EXECUTION_ROOT='previous'):
            self.assertEqual(job.shared_stage('bronze',Path('/tmp/epoch'),{'PIPELINE_EXECUTION_ROOT':'next'}),0)
            self.assertEqual(job.shared_stage('windows',Path('/tmp/epoch'),{'PIPELINE_EXECUTION_ROOT':'next'}),0)
            self.assertEqual(os.environ['PIPELINE_EXECUTION_ROOT'],'previous')
            builder.getOrCreate.assert_called_once();spark.stop.assert_not_called()
            windows.run_at.side_effect=OSError('failure')
            self.assertEqual(job.shared_stage('windows',Path('/tmp/epoch'),{'PIPELINE_EXECUTION_ROOT':'next'}),2)
            self.assertEqual(os.environ['PIPELINE_EXECUTION_ROOT'],'previous')
        spark.stop.assert_called_once()


if __name__=='__main__':unittest.main()
