"""Real local Gold Delta verification from existing Silver; no external services."""
import argparse
from dataclasses import asdict
from pathlib import Path
from uuid import uuid4

from product_pipeline.contracts import canonical,digest,now
from product_pipeline.files import atomic_json,read_json
from product_pipeline.gold.config import GoldSettings
from product_pipeline.gold.contracts import INPUT_TABLES
from product_pipeline.gold.source import SilverSource
from product_pipeline.gold.storage import DeltaSink,Checkpoint
from product_pipeline.gold.runner import process


def empty_inputs():
    value=dict(contract_version=1,policy=dict(version='local-conservative-1',evidence_ref='fixture://policy',
        price_history_mode='UNRESOLVED',price_max_age_seconds=86400,
        representative_price_kinds=['SELLER_REPORTED'],bootstrap_single_gallery=True))
    for k in ('product_states','embeddings','evaluation_selection','gallery_selection','price_confirmations'):
        value[k]=dict(status='NOT_PROVIDED',rows=[],evidence_ref=None)
    value['content_hash']=digest(value)
    return value


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--silver-root',type=Path,required=True)
    parser.add_argument('--spark-options',type=Path,required=True)
    parser.add_argument('--output',type=Path,required=True)
    args=parser.parse_args();out=args.output.resolve();source=args.silver_root.resolve()
    if out.exists() or out.is_relative_to(source) or source.is_relative_to(out):parser.error('new independent output required')
    from pyspark.sql import SparkSession
    from delta.tables import DeltaTable
    builder=SparkSession.builder.appName('gold-local-smoke')
    options=read_json(args.spark_options)
    for k,v in options.items():builder=builder.config(k,v)
    spark=(builder.config('spark.sql.session.timeZone','UTC')
        .config('spark.sql.extensions','io.delta.sql.DeltaSparkSessionExtension')
        .config('spark.sql.catalog.spark_catalog','org.apache.spark.sql.delta.catalog.DeltaCatalog').getOrCreate())
    spark.sparkContext.setLogLevel('ERROR');out.mkdir(parents=True)
    try:
        versions={t:int(DeltaTable.forPath(spark,(source/t).as_uri()).history(1).first()['version']) for t in INPUT_TABLES}
        settings=GoldSettings('gold-local',source.as_uri(),(out/'gold').as_uri(),(out/'checkpoint').as_uri())
        class LocalTestLease:
            def check(self,*args):pass
        lease=LocalTestLease();reader=SilverSource(spark,settings,versions)
        checkpoint=Checkpoint(spark,settings,lease);side=empty_inputs();checkpoint.archive(side)
        sink=DeltaSink(spark,settings,lease=lease);run=str(uuid4());as_of=now()
        products=reader.products()
        if not 1<=len(products)<=3:raise ValueError('use 1–3 sample products')
        results=[];repeated=[]
        for product in products:
            groups=reader.groups(product)
            results.append(process(groups,side,versions,sink,processing_run_id=run,as_of=as_of))
            repeated.append(process(groups,side,versions,sink,processing_run_id=str(uuid4()),as_of=as_of))
        counts={t:spark.read.format('delta').load(p).count() for t,p in sink.paths.items()}
        catalog=spark.read.format('delta').load(sink.paths['product_catalog'])
        passed=(counts['product_catalog']==counts['pipeline_evaluations']==len(products)
            and all(r['reused'] for r in repeated) and catalog.filter('recommendation_ready = true').count()==0)
        report=dict(passed=passed,counts=counts,reused=sum(r['reused'] for r in repeated),
            source_products=len(products),statuses=[r['status'] for r in results],external_service_connections=0,
            lease_backend='localtest-double',spark_version=spark.version)
        fixtures=out/'cli-fixtures';fixtures.mkdir()
        atomic_json(fixtures/'config.json',asdict(settings));atomic_json(fixtures/'versions.json',versions)
        atomic_json(fixtures/'inputs.json',side);atomic_json(fixtures/'spark.json',options)
        atomic_json(fixtures/'registration.json',dict(processing_run_id=run,stage='GOLD_BUILD',status='FIXTURE',evidence_ref='fixture://local-smoke'))
        atomic_json(fixtures/'as-of.json',dict(as_of=as_of))
        atomic_json(out/'report.json',report);print(canonical(report))
        return 0 if passed else 2
    finally:spark.stop()


if __name__=='__main__':raise SystemExit(main())
