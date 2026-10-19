"""Real local Silver Delta smoke from saved immutable source. No external services."""
import argparse
from dataclasses import asdict
from pathlib import Path
from uuid import uuid4

from product_pipeline.bronze.contracts import raw_row
from product_pipeline.contracts import canonical,now
from product_pipeline.files import atomic_json,read_json
from product_pipeline.silver.config import SilverSettings
from product_pipeline.silver.checkpoint import Checkpoint
from product_pipeline.silver.delta import DeltaSink
from product_pipeline.silver.inputs import input_snapshot
from product_pipeline.silver.runner import process
from product_pipeline.silver.source import FileStates


class LocalTestLease:
    def check(self,*args):pass


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source-rehearsal',type=Path,required=True)
    parser.add_argument('--output',type=Path,required=True)
    args=parser.parse_args()
    output=args.output.resolve()
    if output.exists() or output.is_relative_to(args.source_rehearsal.resolve()):parser.error('new separate output required')
    records=[]
    for path in (args.source_rehearsal/'ledger').rglob('*.json'):
        value=read_json(path)
        if 'document' in value:records.append(value)
    if not 1<=len(records)<=3:parser.error('use 1–3 saved observations')
    settings=SilverSettings('silver-local','rehearsal',(output/'bronze').as_uri(),(output/'silver').as_uri(),(output/'checkpoint').as_uri())
    output.mkdir(parents=True)
    from pyspark.sql import SparkSession,functions as F
    from pyspark.sql.types import ArrayType,StructType,StructField,StringType
    from delta import configure_spark_with_delta_pip
    builder=(SparkSession.builder.master('local[2]').appName('silver-local-smoke').config('spark.ui.enabled','false')
        .config('spark.sql.shuffle.partitions','2').config('spark.databricks.delta.snapshotPartitions','2')
        .config('spark.driver.memory','1g').config('spark.sql.extensions','io.delta.sql.DeltaSparkSessionExtension')
        .config('spark.sql.catalog.spark_catalog','org.apache.spark.sql.delta.catalog.DeltaCatalog'))
    spark=configure_spark_with_delta_pip(builder).getOrCreate();spark.sparkContext.setLogLevel('ERROR')
    try:
        from product_pipeline.silver.source import BronzeSource
        bronze=[raw_row(r['document']) for r in records]
        schema=ArrayType(StructType([StructField(k,StringType(),True) for k in bronze[0]]))
        frame=spark.range(1).select(F.explode(F.from_json(F.lit(canonical(bronze)),schema)).alias('r')).select('r.*')
        frame.write.format('delta').save(settings.bronze_path)
        source=BronzeSource(spark,settings,0)
        docs=source.page(None)
        states=FileStates({r['document']['_id']:dict(raw_document_id=r['document']['_id'],
            **{k:r[k] for k in ('observation_key','image_state_vector','image_state_revision','image_state_fingerprint','image_state_checked_at')}) for r in records})
        side={k:input_snapshot(k) for k in ('mappings','reviews','links','rules')}
        lease=LocalTestLease();checkpoint=Checkpoint(spark,settings,lease);checkpoint.archive(side)
        sink=DeltaSink(spark,settings,lease=lease)
        run=str(uuid4());as_of=now();results=[]
        for doc in docs:results.append(process(doc,states,sink,side,processing_run_id=run,as_of=as_of,
            bronze_version=0,bronze_path=settings.bronze_path,archive=checkpoint.archive))
        repeated=[process(doc,states,sink,side,processing_run_id=str(uuid4()),as_of=as_of,
            bronze_version=0,bronze_path=settings.bronze_path,archive=checkpoint.archive) for doc in docs]
        counts={t:spark.read.format('delta').load(path).count() for t,path in sink.paths.items()}
        expected_images=sum(len(d['image_refs']) for d in docs)
        passed=counts['product_observations']==counts['pipeline_evaluations']==len(docs) and counts['product_images']==expected_images and all(r['reused'] for r in repeated)
        report=dict(passed=passed,counts=counts,products=len(docs),image_relations=expected_images,
                    statuses=[r['status'] for r in results],reused=len(repeated),external_service_connections=0,
                    lease_backend='localtest-double',spark_version=spark.version)
        fixtures=output/'cli-fixtures'
        fixtures.mkdir()
        atomic_json(fixtures/'config.json',asdict(settings))
        atomic_json(fixtures/'states.json',states.values)
        atomic_json(fixtures/'inputs.json',side)
        atomic_json(fixtures/'registration.json',dict(processing_run_id=run,stage='NORMALIZATION',status='FIXTURE',evidence_ref='fixture://saved-rehearsal'))
        atomic_json(fixtures/'spark.json',{'spark.master':'local[2]','spark.ui.enabled':'false',
            'spark.sql.shuffle.partitions':'2','spark.databricks.delta.snapshotPartitions':'2','spark.driver.memory':'1g',
            'spark.jars':spark.sparkContext.getConf().get('spark.jars','')})
        atomic_json(output/'report.json',report);print(canonical(report))
        return 0 if passed else 2
    finally:spark.stop()


if __name__=='__main__':raise SystemExit(main())
