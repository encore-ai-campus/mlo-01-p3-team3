"""Actual Spark scheduler boundary; generated rate data, no Kafka/Mongo/Delta writes."""
import argparse,sys,json,threading,tempfile,time
from pathlib import Path
from unittest.mock import MagicMock,patch

parser=argparse.ArgumentParser(description=__doc__)
parser.add_argument('--deployment-root',type=Path,required=True)
args=parser.parse_args();root=args.deployment_root.resolve()
sys.path[:0]=[str(root),str(root/'source'),str(root/'source/tests')]
from production_work import setup,session
from product_pipeline.bronze.stream import start
from product_pipeline.bronze.cli import await_window
from product_pipeline.files import atomic_json
from test_bronze_lease import settings
from pyspark.sql import functions as F
_,options=setup();spark=session(options);query=None;reports=[];control=dict(limit=1,calls=0,boundary_batch_id=None)
result=dict(status='FAILED')
try:
    with tempfile.TemporaryDirectory(prefix='bronze-boundary-') as checkpoint:
        fake=MagicMock();lease=MagicMock();frame=fake.readStream.format.return_value.options.return_value.load.return_value
        def consume(messages,*unused,**kwargs):
            values=list(messages);return dict(batch_id=unused[-1],received=len(values),accepted=len(values))
        with patch('product_pipeline.bronze.stream.bind_checkpoint'),patch('product_pipeline.bronze.stream.check_kafka_cluster'),patch('product_pipeline.bronze.stream.DeltaSink'),patch('product_pipeline.bronze.stream.process_batch',side_effect=consume):
            start(fake,settings(),object(),{'kafka.bootstrap.servers':'fixture:9092'},lease=lease,window_control=control,progress=reports.append)
            callback=frame.writeStream.foreachBatch.call_args.args[0]
            rate=spark.readStream.format('rate').option('rowsPerSecond',1).option('numPartitions',1).load()
            rate=rate.select(F.lit('fixture').alias('topic'),F.lit(0).alias('partition'),F.col('value').alias('offset'),F.encode(F.lit('k'),'UTF-8').alias('key'),F.encode(F.lit('v'),'UTF-8').alias('value'))
            query=rate.writeStream.foreachBatch(callback).option('checkpointLocation',Path(checkpoint).as_uri()).trigger(processingTime='1 second').start()
            timer=threading.Timer(60,query.stop);timer.start()
            try:
                deadline=time.monotonic()+40
                while query.isActive and control['boundary_batch_id'] is None and time.monotonic()<deadline:time.sleep(0.1)
                window=await_window(query,lease,1,control)
            finally:timer.cancel()
            commits=sorted(int(p.name) for p in (Path(checkpoint)/'commits').iterdir() if p.name.isdigit())
            assert window['committed_batches']==1 and not window['source_drained']
            assert window['last_committed_batch_id'] in commits
            assert control['boundary_batch_id'] is not None and control['boundary_batch_id'] not in commits
            result=dict(status='VERIFIED',window=window,committed_ids=commits,boundary_batch_id=control['boundary_batch_id'],reports=reports)
finally:
    if query is not None and query.isActive:query.stop()
    spark.stop();atomic_json(root/'perf-20261020/boundary-spark-verification.json',result)
print(json.dumps(result))
