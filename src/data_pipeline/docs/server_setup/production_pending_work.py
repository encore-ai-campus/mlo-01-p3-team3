"""Actual pj3-danawa data, pinned stages, no synthetic owner approvals."""
import hashlib,json,os,sys,time
from pathlib import Path
from uuid import uuid4
CODE_ROOT=Path(__file__).resolve().parent
ROOT=Path(os.environ.get('PIPELINE_EXECUTION_ROOT',str(CODE_ROOT)))
ROOT.mkdir(parents=True,exist_ok=True,mode=0o700)
sys.path.insert(0,str(CODE_ROOT/'source'))
from product_pipeline.files import atomic_json,read_json
from product_pipeline.contracts import now,digest
BASE=CODE_ROOT.parent
LAKE='s3a://pj3-data/lake/pj3-danawa'
PY=sys.executable

def setup():
    from urllib.parse import quote
    config=BASE/'config';spec=read_json(config/'mongo-accounts.json')
    def uri(user):
        a=next(a for a in spec['accounts'] if a['username']==user)
        return 'mongodb://'+quote(user,safe='')+':'+quote(a['password'],safe='')+'@mongo01.pj3.internal:27017,mongo02.pj3.internal:27017,mongo03.pj3.internal:27017/?replicaSet=rs-products&authSource=admin&tls=true&tlsCAFile='+quote(spec['ca'],safe='')
    os.environ['PIPELINE_MONGO_URI']=uri('pipeline_ingest')
    for name in ('BRONZE','SILVER','GOLD'):os.environ[name+'_LEASE_MONGO_URI']=uri('pipeline_spark')
    producer=read_json(config/'kafka-python.json');tls='/etc/pj3-secrets/spark/'
    kafka={'kafka.bootstrap.servers':producer['bootstrap.servers'],'kafka.security.protocol':'SSL',
        'kafka.ssl.truststore.type':'PKCS12','kafka.ssl.truststore.location':tls+'truststore.p12',
        'kafka.ssl.truststore.password':producer['ssl.keystore.password'],'kafka.ssl.keystore.type':'PKCS12',
        'kafka.ssl.keystore.location':tls+'keystore.p12','kafka.ssl.keystore.password':producer['ssl.keystore.password'],
        'kafka.ssl.key.password':producer['ssl.keystore.password'],'kafka.ssl.endpoint.identification.algorithm':'https'}
    os.environ['BRONZE_KAFKA_OPTIONS']=json.dumps(kafka)
    dependency=read_json(BASE/'pipeline-acceptance-0594f7b79ed347c5bf9e449cdb5912af/dependencies.json')
    jars=dependency['jars']
    assert all(hashlib.sha256(Path(j).read_bytes()).hexdigest()==dependency['sha256'][Path(j).name] for j in jars)
    options={'spark.master':'local[1]','spark.driver.memory':'768m','spark.ui.enabled':'false','spark.sql.shuffle.partitions':'1',
        'spark.sql.maxPlanStringLength':'8192',
        'spark.default.parallelism':'1','spark.databricks.delta.snapshotPartitions':'1','spark.jars':','.join(jars),
        'spark.sql.warehouse.dir':str(ROOT/'warehouse')}
    for name in ('BRONZE','SILVER','GOLD','PUBLICATION'):os.environ[name+'_SPARK_OPTIONS']=json.dumps(options)
    secret=BASE/'mysql-publication-v2-20261018'
    os.environ['PIPELINE_MYSQL_PASSWORD']=read_json(secret/'bootstrap.private.json')['password']
    os.environ['PIPELINE_MYSQL_CA']=str(secret/'ca.pem')
    return read_json(secret/'mysql-publication.json'),options

def session(options):
    from product_pipeline.spark_storage import storage_options,bind_spark_credentials
    from pyspark.sql import SparkSession
    builder=SparkSession.builder.appName('pj3-danawa-fixed-version-inspection')
    for k,v in storage_options(options).items():builder=builder.config(k,v)
    spark=builder.config('spark.sql.session.timeZone','UTC').config('spark.sql.extensions','io.delta.sql.DeltaSparkSessionExtension').config('spark.sql.catalog.spark_catalog','org.apache.spark.sql.delta.catalog.DeltaCatalog').getOrCreate()
    bind_spark_credentials(spark);return spark

def pinned(options,root,tables):
    from delta.tables import DeltaTable
    spark=session(options)
    try:return {t:int(DeltaTable.forPath(spark,root+'/'+t).history(1).first()['version']) for t in tables}
    finally:spark.stop()

def registered(db,stage,scope):
    from product_pipeline.registration import register
    path=ROOT/(stage.lower()+'-request.json')
    request=read_json(path) if path.exists() else dict(processing_run_id=str(uuid4()),stage=stage,input_range=scope,code_version=read_json(CODE_ROOT/'deployment.json')['source_sha256'])
    assert request['input_range']==scope
    if not path.exists():atomic_json(path,request)
    export=register(db,request);db.connection.rollback()
    output=ROOT/(stage.lower()+'-registration.json');atomic_json(output,export);return output

def gallery_selection(options,versions):
    """Select only a single byte-verified acquisition; never approve its use."""
    from pyspark.sql import functions as F
    spark=session(options)
    try:
        frame=spark.read.format('delta').option('versionAsOf',versions['product_images']).load(LAKE+'/silver/product_images')
        candidates=(frame.filter((F.col('availability')=='AVAILABLE')&(F.col('validation_status')=='LOCAL_BYTES_VERIFIED'))
            .select('product_key','evaluation_key').distinct())
        unique=candidates.groupBy('product_key').count().filter('count = 1').select('product_key')
        rows=[dict(product_key=r['product_key'],evaluation_key=r['evaluation_key'],
            evidence_ref=LAKE+'/silver/product_images@'+str(versions['product_images'])+'#'+r['evaluation_key'])
            for r in candidates.join(unique,'product_key').toLocalIterator()]
        if len(rows)>100000:raise RuntimeError('GALLERY_SELECTION_LIMIT')
        return dict(status='PROVIDED' if rows else 'NOT_PROVIDED',rows=rows,
            evidence_ref='pipeline://unique-byte-verified-acquisition-v1' if rows else None)
    finally:spark.stop()

def main(stage):
    config,options=setup()
    from product_pipeline.publication.mysql import MySQL
    if stage=='preflight':
        db=MySQL(config)
        try:
            from product_pipeline.registration import preflight,mapping_snapshot
            result=dict(mysql=db.preflight(),registration=preflight(db),
                mappings=mapping_snapshot(db,evidence_ref='mysql://pj3_products/category_mappings',version='current'))
            result['tls_cipher']=db.one("SHOW SESSION STATUS LIKE 'Ssl_cipher'")['Value']
        finally:db.close()
        return result
    if stage=='bronze':
        from product_pipeline.bronze.cli import main as run
        value=dict(environment_id='pj3-danawa',mongo_database='pj3_products_raw',kafka_cluster_id='AGceilLlygifiV1hss1Rbw',single_writer_confirmed=True,
            bronze_path=LAKE+'/bronze/products',events_path=LAKE+'/bronze/events',quarantine_path=LAKE+'/bronze/quarantine',checkpoint_path=LAKE+'/checkpoints/bronze',max_records=100)
        atomic_json(ROOT/'bronze.json',value)
        args=['--config',str(ROOT/'bronze.json'),'--available-now','--result-file',str(ROOT/'bronze-window.json')]
        if os.environ.get('PIPELINE_CONTINUOUS')=='1':args+=['--max-batches','1']
        if run(args):raise RuntimeError('BRONZE_FAILED')
        window=read_json(ROOT/'bronze-window.json')
        return dict(status='COMPLETED',stage=stage,source_drained=window['source_drained'],committed_batches=window.get('committed_batches'))
    if stage=='silver':
        from product_pipeline.silver.inputs import KINDS,input_snapshot
        from product_pipeline.registration import mapping_snapshot
        from product_pipeline.silver.cli import main as run
        versions=pinned(options,LAKE+'/bronze', ['products'])
        db=MySQL(config)
        try:
            path=ROOT/'silver-inputs.json'
            if path.exists():inputs=read_json(path)
            elif db.one('SELECT COUNT(*) AS n FROM information_schema.TABLES WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s',(config['database'],'pipeline_review_events'))['n']:
                from product_pipeline.review import export_inputs
                with db.read_snapshot():inputs=export_inputs(db,[],as_of=now())['silver']
                atomic_json(path,inputs)
            else:
                inputs={k:input_snapshot(k) for k in KINDS}
                inputs['mappings']=mapping_snapshot(db,evidence_ref='mysql://pj3_products/category_mappings',version='current')['snapshot']
                atomic_json(path,inputs)
            reg=registered(db,'NORMALIZATION',dict(bronze_path=LAKE+'/bronze/products',bronze_version=versions['products'],inputs_hash=digest(inputs)))
        finally:db.close()
        value=dict(environment_id='pj3-danawa',mongo_database='pj3_products_raw',bronze_path=LAKE+'/bronze/products',silver_root=LAKE+'/silver',checkpoint_path=LAKE+'/checkpoints/silver',max_records=100)
        atomic_json(ROOT/'silver.json',value)
        if run(['--config',str(ROOT/'silver.json'),'--bronze-version',str(versions['products']),'--inputs',str(path),'--registration',str(reg),'--state-dir',str(ROOT/'silver-state'),'--batch-writes']):raise RuntimeError('SILVER_FAILED')
        return dict(status='COMPLETED',stage=stage)
    if stage=='gold':
        from product_pipeline.gold.contracts import INPUT_TABLES
        from product_pipeline.gold.cli import main as run
        versions=pinned(options,LAKE+'/silver',INPUT_TABLES)
        atomic_json(ROOT/'silver-versions.json',versions)
        inputs={k:dict(status='NOT_PROVIDED',rows=[],evidence_ref=None) for k in ('product_states','embeddings','evaluation_selection','gallery_selection','price_confirmations')}
        inputs.update(contract_version=1,policy=dict(version='pending-intake-v1',evidence_ref='operator://20261019/pending-review-inactive',price_history_mode='UNRESOLVED',price_max_age_seconds=86400,representative_price_kinds=[],bootstrap_single_gallery=True))
        db=MySQL(config)
        try:
            input_path=ROOT/'gold-inputs.json'
            if input_path.exists():inputs=read_json(input_path)
            elif db.one('SELECT COUNT(*) AS n FROM information_schema.TABLES WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s',(config['database'],'pipeline_review_events'))['n'] and db.one('SELECT COUNT(*) AS n FROM pipeline_review_events')['n']:
                from product_pipeline.review import export_inputs
                from product_pipeline.gold.source import SilverSource
                from product_pipeline.gold.config import GoldSettings
                spark=session(options)
                try:
                    source=SilverSource(spark,GoldSettings(environment_id='pj3-danawa',silver_root=LAKE+'/silver',gold_root=LAKE+'/gold',checkpoint_path=LAKE+'/checkpoints/gold',max_records=100),versions)
                    def groups():
                        after=None
                        while True:
                            keys=source.products(after)
                            if not keys:return
                            for key in keys:yield from source.groups(key)
                            after=keys[-1]
                    with db.read_snapshot():inputs=export_inputs(db,groups(),as_of=now())['gold']
                finally:spark.stop()
            else:
                if os.environ.get('PIPELINE_CONTINUOUS')=='1':inputs['gallery_selection']=gallery_selection(options,versions)
                inputs['content_hash']=digest(inputs)
            if not input_path.exists():atomic_json(input_path,inputs)
            reg=registered(db,'GOLD_BUILD',dict(silver_root=LAKE+'/silver',silver_versions=versions,inputs_hash=digest(inputs)))
        finally:db.close()
        value=dict(environment_id='pj3-danawa',silver_root=LAKE+'/silver',gold_root=LAKE+'/gold',checkpoint_path=LAKE+'/checkpoints/gold',max_records=100)
        atomic_json(ROOT/'gold.json',value)
        atpath=ROOT/'gold-as-of.json'
        if not atpath.exists():atomic_json(atpath,dict(as_of=now()))
        if run(['--config',str(ROOT/'gold.json'),'--versions',str(ROOT/'silver-versions.json'),'--inputs',str(ROOT/'gold-inputs.json'),'--registration',str(reg),'--state-dir',str(ROOT/'gold-state'),'--as-of',read_json(atpath)['as_of'],'--batch-writes']):raise RuntimeError('GOLD_FAILED')
        return dict(status='COMPLETED',stage=stage)
    if stage=='publish':
        if os.environ.get('PIPELINE_CONTINUOUS')=='1':
            from product_pipeline.publication.source import GoldSource
            from product_pipeline.publication.streaming import publish_pending
            runs=sorted((ROOT/'gold-state/runs').glob('*.json'),key=lambda p:p.stat().st_mtime)
            gold=read_json(runs[-1]);assert gold['status']=='COMPLETED' and gold['source_drained']
            spark=session(options);db=MySQL(config)
            try:
                source=GoldSource(spark,gold['export_manifest_ref'],LAKE+'/gold',LAKE+'/checkpoints/gold')
                def committed(value):atomic_json(ROOT/'publish-progress.json',dict(**value,updated_at=now()))
                result=publish_pending(source,db,ROOT/'pending-plans','25701b75-22f9-5fea-9148-d7a4c006a440',progress=committed)
                counts=db.one("SELECT COUNT(*) AS products,SUM(status='PENDING_REVIEW') AS pending,SUM(recommendation_ready) AS recommendation_ready FROM products WHERE source_code='danawa'")
                return dict(**result,counts={k:int(v or 0) for k,v in counts.items()})
            finally:db.close();spark.stop()
        from product_pipeline.publication.cli import prepare_from_source
        from product_pipeline.publication.source import GoldSource
        from product_pipeline.publication.plan import load_plan
        from product_pipeline.publication.runner import publish,audit_publication
        runs=sorted((ROOT/'gold-state/runs').glob('*.json'),key=lambda p:p.stat().st_mtime)
        gold=read_json(runs[-1]);assert gold['status']=='COMPLETED' and gold['source_drained']
        output=ROOT/'pending-plans'
        db=MySQL(config)
        try:
            if not output.exists():
                spark=session(options)
                try:prepare_from_source(GoldSource(spark,gold['export_manifest_ref'],LAKE+'/gold',LAKE+'/checkpoints/gold'),{},db.target,output,pending_category_id='25701b75-22f9-5fea-9148-d7a4c006a440')
                finally:spark.stop()
            ready=read_json(output/'ready.json');results=[]
            for name,proof in ready['plans'].items():
                plan=load_plan(output/name);assert digest(plan)==proof
                result=publish(plan,db);again=publish(plan,db);assert again['status']=='REUSED'
                with db.read_snapshot():audit=audit_publication(plan,db)
                results.append(dict(published=result,audit=audit,repeated=again))
            counts=db.one('SELECT COUNT(*) AS products,SUM(status=\'PENDING_REVIEW\') AS pending,SUM(recommendation_ready) AS recommendation_ready FROM products WHERE source_code=\'danawa\'')
            return dict(status='COMPLETED',stage=stage,results=results,counts={k:int(v or 0) for k,v in counts.items()})
        finally:db.close()
    raise ValueError('UNKNOWN_STAGE')

if __name__=='__main__':
    stage=sys.argv[1];began=time.monotonic()
    try:
        result=main(stage);result.update(duration_ms=round((time.monotonic()-began)*1000),finished_at=now())
        atomic_json(ROOT/(stage+'-result.json'),result)
    except Exception as exc:
        atomic_json(ROOT/(stage+'-failure.json'),dict(error_code=getattr(exc,'code',type(exc).__name__),driver_code=exc.args[0] if exc.args and isinstance(exc.args[0],int) else None,at=now()))
        raise
