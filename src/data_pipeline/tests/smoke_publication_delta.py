"""Real local Delta receipt→pinned Gold export→publication plans, no service DB."""
import argparse
from pathlib import Path

from product_pipeline.contracts import canonical,digest
from product_pipeline.files import atomic_json
from product_pipeline.gold.config import GoldSettings
from product_pipeline.gold.contracts import semantic_hash
from product_pipeline.gold.storage import DeltaSink,Checkpoint
from product_pipeline.gold.export import finalize
from product_pipeline.publication.source import GoldSource
from product_pipeline.publication.cli import prepare_from_source
from product_pipeline.publication.plan import load_plan
from publication_fixture import fixture


def main():
    from pyspark.sql import SparkSession
    import json
    p=argparse.ArgumentParser();p.add_argument('--output',required=True,type=Path);p.add_argument('--spark-options',required=True,type=Path)
    p.add_argument('--existing-root',type=Path,help='Read previously sealed local Gold without recreating its Delta tables')
    a=p.parse_args();out=a.output.resolve();out.mkdir(parents=True,exist_ok=False)
    options=json.loads(a.spark_options.read_text())
    builder=SparkSession.builder.appName('publication-pinned-gold-local-test')
    for k,v in options.items():builder=builder.config(k,v)
    spark=(builder.config('spark.sql.session.timeZone','UTC')
        .config('spark.sql.extensions','io.delta.sql.DeltaSparkSessionExtension')
        .config('spark.sql.catalog.spark_catalog','org.apache.spark.sql.delta.catalog.DeltaCatalog').getOrCreate())
    spark.sparkContext.setLogLevel('ERROR')
    try:
        if a.existing_root:
            from product_pipeline.files import read_json
            source_root=a.existing_root.resolve()
            exports=[p for p in (source_root/'checkpoint/inputs').glob('*.json') if read_json(p).get('stage')=='GOLD_EXPORT']
            assert len(exports)==1
            prior=[read_json(p) for p in (source_root/'plans').glob('*.json') if p.name!='ready.json']
            guards={item['product_key']:item['guard_hash'] for plan in prior for item in plan['products']}
            reader=GoldSource(spark,exports[0].as_uri(),(source_root/'gold').as_uri(),(source_root/'checkpoint').as_uri())
            prepared=prepare_from_source(reader,guards,'a'*64,out/'plans')
            plans=[load_plan(p) for p in (out/'plans').glob('*.json') if p.name!='ready.json']
            assert prepared['plans']==len(plans)==1
            assert reader.frames['product_catalog'].filter("product_key = 'danawa:later'").count()==0
            report=dict(passed=True,actual_delta=True,spark_version=spark.version,plans=1,products=1,
                ignored_later_delta_rows=True,external_service_connections=0,lease_backend='read-only-no-lease',
                schema_contract_hash=plans[0]['schema_contract_hash'])
            atomic_json(out/'report.json',report);print(json.dumps(report));return
        plan,guard=fixture(out/'fixture','a'*64);group=plan['products'][0]['group'];catalog=group['tables']['product_catalog'][0]
        settings=GoldSettings('publication-local',(out/'silver-unused').as_uri(),(out/'gold').as_uri(),(out/'checkpoint').as_uri())
        class LocalTestLease:
            def check(self,*a):pass
        sink=DeltaSink(spark,settings,lease=LocalTestLease());cp=Checkpoint(spark,settings,LocalTestLease())
        refs=dict(inputs=cp.archive(dict(policy=dict(price_history_mode='SNAPSHOT_V1',price_max_age_seconds=259200))),
                  versions=cp.archive(dict(silver_versions='fixture')),silver_root=settings.silver_root,as_of=plan['as_of'])
        manifest={}
        for table,rows in group['tables'].items():
            sink.insert(table,rows);manifest[table]=dict(version=sink.version(table),records={r['row_key']:r['record_hash'] for r in rows})
        receipt={**group['receipt'],'output_manifest_json':canonical(manifest),'input_refs_json':canonical(refs)}
        receipt['record_hash']=semantic_hash(receipt);sink.insert('pipeline_evaluations',[receipt])
        pages=[cp.archive(dict(selections=[dict(product_key=catalog['product_key'],evaluation_key=catalog['gold_evaluation_key'])]))]
        sealed=finalize(spark,sink,cp,pages,as_of=plan['as_of'],source_drained=True,input_refs=refs)
        reader=GoldSource(spark,sealed['export_manifest_ref'],settings.gold_root,settings.checkpoint_path)
        prepared=prepare_from_source(reader,{catalog['product_key']:digest(guard)},'a'*64,out/'plans')
        paths=list((out/'plans').glob('*.json'));plans=[load_plan(p) for p in paths if p.name!='ready.json']
        assert prepared['plans']==1 and len(plans)==1 and plans[0]['gold_versions']==reader.manifest['gold_versions']
        # Add later rows: the fixed export must still select only the original versions.
        newer={**catalog,'row_key':'fixture-later-row','product_key':'danawa:later'};newer['record_hash']=semantic_hash(newer)
        sink.insert('product_catalog',[newer])
        again=list(reader.groups());assert len(again)==1 and again[0]['tables']['product_catalog'][0]['product_key']==catalog['product_key']
        report=dict(passed=True,actual_delta=True,spark_version=spark.version,plans=1,products=1,
                    ignored_later_delta_rows=True,external_service_connections=0,lease_backend='localtest-double')
        atomic_json(out/'report.json',report);print(json.dumps(report))
    finally:spark.stop()


if __name__=='__main__':main()
