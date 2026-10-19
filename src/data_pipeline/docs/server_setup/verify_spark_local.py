"""One-row local Spark/Delta compatibility test. No production lake or services."""
import argparse
import importlib.metadata
import json
from pathlib import Path
import tempfile


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root',type=Path,default=Path.home()/'product-pipeline')
    args=parser.parse_args();root=args.root.resolve();audit=root/'audit';audit.mkdir(parents=True,exist_ok=True)
    if importlib.metadata.version('pyspark')!='4.1.3' or importlib.metadata.version('delta-spark')!='4.1.0':
        raise RuntimeError('expected separate server profile: Spark 4.1.3 / Delta 4.1.0')
    from pyspark.sql import SparkSession
    from delta.tables import DeltaTable
    cache=root/'cache/ivy';cache.mkdir(parents=True,exist_ok=True)
    with tempfile.TemporaryDirectory(prefix='local-delta-check-',dir=audit) as temporary:
        folder=Path(temporary);spark=None
        try:
            spark=(SparkSession.builder.master('local[1]').appName('pipeline-local-compatibility-check')
                .config('spark.ui.enabled','false').config('spark.sql.shuffle.partitions','1')
                .config('spark.sql.warehouse.dir',(folder/'warehouse').as_uri())
                .config('spark.jars.ivy',str(cache))
                .config('spark.jars.packages','io.delta:delta-spark_4.1_2.13:4.1.0')
                .config('spark.sql.extensions','io.delta.sql.DeltaSparkSessionExtension')
                .config('spark.sql.catalog.spark_catalog','org.apache.spark.sql.delta.catalog.DeltaCatalog')
                .getOrCreate())
            path=(folder/'delta').as_uri()
            spark.range(1).write.format('delta').mode('error').save(path)
            (DeltaTable.forPath(spark,path).alias('target').merge(spark.range(1,2).alias('incoming'),
                'target.id = incoming.id').whenNotMatchedInsertAll().execute())
            assert spark.read.format('delta').load(path).count()==2
            assert spark.read.format('delta').option('versionAsOf',0).load(path).count()==1
            result=dict(status='LOCAL_SPARK_DELTA_PASS',spark=spark.version,delta='4.1.0',rows=2,
                master='local[1]',cluster_job_tested=False,production_data_written=False)
            (audit/'spark413-delta410-local.json').write_text(json.dumps(result,indent=2)+'\n')
            print(json.dumps(result));return 0
        finally:
            if spark:spark.stop()


if __name__=='__main__':raise SystemExit(main())
