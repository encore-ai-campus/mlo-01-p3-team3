"""A paused receipt must not skip an unfinished downstream epoch."""
import importlib.util,tempfile,sys,unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock,patch
from product_pipeline.files import atomic_json,read_json


class ContinuousResumeTests(unittest.TestCase):
    def test_replay_gap_stops_without_starting_spark_or_retrying(self):
        from product_pipeline.errors import PipelineError
        path=Path(__file__).resolve().parents[1]/'docs/server_setup/production_continuous_work.py'
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);atomic_json(root/'flow-progress.json',dict(epoch=2,last_events=100))
            atomic_json(root/'bronze-recovery.json',{})
            with patch.dict(sys.modules,production_work=SimpleNamespace(setup=lambda:None,BASE=root.parent)):
                spec=importlib.util.spec_from_file_location('continuous_replay_gap_fixture',path)
                job=importlib.util.module_from_spec(spec);spec.loader.exec_module(job)
            job.ROOT=root;done=SimpleNamespace(pid=0,poll=lambda:0);client=MagicMock()
            refill=MagicMock(side_effect=PipelineError('BRONZE_KAFKA_OFFSETS_OUT_OF_RANGE'))
            with patch.dict(sys.modules,recover_kafka_retention=SimpleNamespace(topup=refill)),patch.dict(job.os.environ,PIPELINE_MONGO_URI='fixture://'),patch.object(job,'MongoClient',return_value=client),patch.object(job,'producer',return_value=done),patch.object(job,'child') as child:
                with self.assertRaises(SystemExit):job.main()
                child.assert_not_called();refill.assert_called_once()
            self.assertEqual(read_json(root/'flow-progress.json')['status'],'RECOVERY_REQUIRED')

    def test_replay_remaining_prevents_false_completion_on_unchanged_ingestion_count(self):
        path=Path(__file__).resolve().parents[1]/'docs/server_setup/production_continuous_work.py'
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);atomic_json(root/'flow-progress.json',dict(epoch=2,last_events=100,mysql_counts={'products':209}))
            atomic_json(root/'bronze-recovery.json',{})
            with patch.dict(sys.modules,production_work=SimpleNamespace(setup=lambda:None,BASE=root.parent)):
                spec=importlib.util.spec_from_file_location('continuous_replay_pending_fixture',path)
                job=importlib.util.module_from_spec(spec);spec.loader.exec_module(job)
            job.ROOT=root;done=SimpleNamespace(pid=0,poll=lambda:0);client=MagicMock()
            client.get_database.return_value.ingestion_observations.count_documents.return_value=100
            with patch.dict(sys.modules,recover_kafka_retention=SimpleNamespace(topup=lambda root:dict(pending=True))),patch.dict(job.os.environ,PIPELINE_MONGO_URI='fixture://'),patch.object(job,'MongoClient',return_value=client),patch.object(job,'producer',return_value=done),patch.object(job,'child',return_value=SimpleNamespace(wait=lambda:75)) as child:
                job.main();child.assert_called_once()
            self.assertEqual(read_json(root/'flow-progress.json')['status'],'PAUSED_AFTER_BATCH')
    def test_lease_failure_stops_once_with_recovery_required(self):
        path=Path(__file__).resolve().parents[1]/'docs/server_setup/production_continuous_work.py'
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);epoch=root/'epochs/000002'
            atomic_json(root/'flow-progress.json',dict(epoch=2,last_events=0))
            with patch.dict(sys.modules,production_work=SimpleNamespace(setup=lambda:None,BASE=root.parent)):
                spec=importlib.util.spec_from_file_location('continuous_lease_fixture',path)
                job=importlib.util.module_from_spec(spec);spec.loader.exec_module(job)
            job.ROOT=root
            def child(*a,**k):
                atomic_json(epoch/'bronze-failure.json',dict(error_code='BRONZE_WRITER_LEASE_LOST',lease_failure={'exception_type':'NetworkTimeout'}))
                return SimpleNamespace(wait=lambda:2)
            client=MagicMock();client.get_database.return_value.ingestion_observations.count_documents.return_value=100
            done=SimpleNamespace(pid=0,poll=lambda:0)
            with patch.dict(job.os.environ,PIPELINE_MONGO_URI='fixture://not-connected'),patch.object(job,'MongoClient',return_value=client),patch.object(job,'producer',return_value=done),patch.object(job,'child',side_effect=child) as execute:
                with self.assertRaises(SystemExit) as result:job.main()
                self.assertEqual(result.exception.code,2);self.assertEqual(execute.call_count,1)
            state=read_json(root/'flow-progress.json');self.assertEqual(state['status'],'RECOVERY_REQUIRED')
            self.assertEqual(state['error_code'],'BRONZE_WRITER_LEASE_LOST')
    def test_permanent_window_error_is_not_retried_three_times(self):
        path=Path(__file__).resolve().parents[1]/'docs/server_setup/production_continuous_work.py'
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);epoch=root/'epochs/000001'
            atomic_json(root/'flow-progress.json',dict(epoch=1,last_events=0))
            atomic_json(epoch/'bronze-result.json',dict(status='COMPLETED'))
            with patch.dict(sys.modules,production_work=SimpleNamespace(setup=lambda:None,BASE=root.parent)):
                spec=importlib.util.spec_from_file_location('continuous_error_fixture',path)
                job=importlib.util.module_from_spec(spec);spec.loader.exec_module(job)
            job.ROOT=root
            def child(*args,**kwargs):
                atomic_json(epoch/'windows-progress.json',dict(status='INTERRUPTED',stage='gold',error_code='GOLD_PRODUCT_HISTORY_LIMIT'))
                return SimpleNamespace(wait=lambda:2)
            client=MagicMock();client.get_database.return_value.ingestion_observations.count_documents.return_value=100
            done=SimpleNamespace(pid=0,poll=lambda:0)
            with patch.dict(job.os.environ,PIPELINE_MONGO_URI='fixture://not-connected'),patch.object(job,'MongoClient',return_value=client),patch.object(job,'producer',return_value=done),patch.object(job,'child',side_effect=child) as execute:
                with self.assertRaises(RuntimeError):job.main()
                self.assertEqual(execute.call_count,1)
            self.assertEqual(read_json(root/'flow-progress.json')['last_failure']['error_code'],'GOLD_PRODUCT_HISTORY_LIMIT')

    def test_resume_paused_epoch_executes_windows_and_preserves_completed_bronze(self):
        path=Path(__file__).resolve().parents[1]/'docs/server_setup/production_continuous_work.py'
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);epoch=root/'epochs/000001'
            atomic_json(root/'flow-progress.json',dict(epoch=1,last_events=0))
            atomic_json(epoch/'bronze-result.json',dict(status='COMPLETED',source_drained=True))
            atomic_json(epoch/'windows-result.json',dict(status='PAUSED_AFTER_BATCH'))
            fake=SimpleNamespace(setup=lambda:None,BASE=root.parent)
            with patch.dict(sys.modules,production_work=fake):
                spec=importlib.util.spec_from_file_location('continuous_fixture',path)
                job=importlib.util.module_from_spec(spec);spec.loader.exec_module(job)
            job.ROOT=root;calls=[]
            def child(script,args,logpath,env):
                calls.append(script.name)
                atomic_json(epoch/'windows-result.json',dict(status='COMPLETED',counts={'products':65498},
                    finished_at='2026-10-20T00:00:00.000Z',bronze_version=42))
                return SimpleNamespace(wait=lambda:0)
            client=MagicMock();client.get_database.return_value.ingestion_observations.count_documents.return_value=100
            done=SimpleNamespace(pid=0,poll=lambda:0)
            with patch.dict(job.os.environ,PIPELINE_MONGO_URI='fixture://not-connected'),patch.object(job,'MongoClient',return_value=client),patch.object(job,'producer',return_value=done),patch.object(job,'child',side_effect=child):
                job.main()
            self.assertEqual(calls,['production_window_work.py'])
            result=read_json(root/'flow-progress.json')
            self.assertEqual(result['status'],'COMPLETED');self.assertEqual(result['processed_bronze_version'],42)
            self.assertEqual(read_json(epoch/'bronze-result.json'),dict(status='COMPLETED',source_drained=True))


if __name__=='__main__':unittest.main()
