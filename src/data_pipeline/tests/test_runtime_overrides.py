import tempfile,unittest
from pathlib import Path
from product_pipeline.runtime_overrides import load
from product_pipeline.files import atomic_json
from product_pipeline.errors import PipelineError


class RuntimeOverrideTests(unittest.TestCase):
    def test_server_profile_and_window_limit_survive_different_operator_environment(self):
        profile=dict(master='spark://192.168.0.121:7077',driver_host='192.168.0.121',executor_cores=1,
            total_cores=2,executor_memory='768m',driver_memory='768m',shuffle_partitions=3,executor_python='/usr/bin/python3')
        with tempfile.TemporaryDirectory() as directory:
            base=Path(directory);root=base/'run';config=base/'config/spark-runtime.json'
            atomic_json(config,profile)
            self.assertEqual(load(root,base)[0],100)
            selected=root/'two-workers.json';atomic_json(selected,profile)
            atomic_json(root/'runtime-overrides.json',dict(window_records=25,spark_runtime_file=str(selected)))
            records,path,actual=load(root,base)
            self.assertEqual((records,path,actual),(25,str(selected.resolve()),profile))
            for bad in [dict(window_records=0),dict(window_records=101),dict(window_records=True),
                        dict(spark_runtime_file='../outside.json'),dict(PATH='untrusted')]:
                atomic_json(root/'runtime-overrides.json',bad)
                with self.assertRaises(PipelineError):load(root,base)


if __name__=='__main__':unittest.main()
