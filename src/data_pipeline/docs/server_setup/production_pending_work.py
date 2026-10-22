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
from product_pipeline.spark_session import get_or_create,release_spark,metrics,timed
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
    from product_pipeline.kafka_tls import spark_options
    kafka=spark_options(read_json(config/'kafka-spark.json'))
    os.environ['BRONZE_KAFKA_OPTIONS']=json.dumps(kafka)
    dependency=read_json(BASE/'pipeline-acceptance-0594f7b79ed347c5bf9e449cdb5912af/dependencies.json')
    jars=dependency['jars']
    assert all(hashlib.sha256(Path(j).read_bytes()).hexdigest()==dependency['sha256'][Path(j).name] for j in jars)
    from product_pipeline.spark_runtime import cluster_options
    profile=read_json(Path(os.environ.get('PIPELINE_SPARK_RUNTIME_FILE',str(config/'spark-runtime.json'))))
    options={**cluster_options(profile),'spark.ui.enabled':'false',
        'spark.driver.extraJavaOptions':'-XX:+ExitOnOutOfMemoryError',
        'spark.sql.ui.retainedExecutions':'32','spark.ui.retainedJobs':'32',
        'spark.ui.retainedStages':'64','spark.sql.streaming.ui.retainedQueries':'4',
        'spark.sql.streaming.numRecentProgressUpdates':'10',
        'spark.sql.maxPlanStringLength':'8192',
        'spark.jars':','.join(jars),
        'spark.sql.warehouse.dir':str(CODE_ROOT/'warehouse')}
    for name in ('BRONZE','SILVER','GOLD','PUBLICATION'):os.environ[name+'_SPARK_OPTIONS']=json.dumps(options)
    secret=BASE/'mysql-publication-v2-20261018'
    os.environ['PIPELINE_MYSQL_PASSWORD']=read_json(secret/'bootstrap.private.json')['password']
    os.environ['PIPELINE_MYSQL_CA']=str(secret/'ca.pem')
    return read_json(secret/'mysql-publication.json'),options

def session(options):
    from product_pipeline.spark_storage import storage_options,bind_spark_credentials
    from pyspark.sql import SparkSession
    builder=SparkSession.builder.appName('pj3-danawa-fixed-version-inspection')
    options=storage_options(options)
    for k,v in options.items():builder=builder.config(k,v)
    builder=builder.config('spark.sql.session.timeZone','UTC').config('spark.sql.extensions','io.delta.sql.DeltaSparkSessionExtension').config('spark.sql.catalog.spark_catalog','org.apache.spark.sql.delta.catalog.DeltaCatalog')
    spark=get_or_create(builder,options)
    bind_spark_credentials(spark);return spark

def pinned(options,root,tables):
    from delta.tables import DeltaTable
    from pyspark.sql import functions as F
    spark=session(options)
    try:
        from product_pipeline.spark_session import bump
        frames=[]
        for t in tables:
            bump('pinned_version_reads')
            frames.append(DeltaTable.forPath(spark,root+'/'+t).history(1).select(F.lit(t).alias('_table'),'version'))
        combined=frames[0]
        for frame in frames[1:]:combined=combined.unionByName(frame)
        rows=combined.collect();bump('pinned_metadata_read_batches')
        result={r['_table']:int(r['version']) for r in rows}
        if len(rows)!=len(tables) or set(result)!=set(tables):raise RuntimeError('FIXED_VERSION_READ_INCOMPLETE')
        return result
    finally:release_spark(spark)

def registered(db,stage,scope):
    from product_pipeline.registration import register
    path=ROOT/(stage.lower()+'-request.json')
    deployed=read_json(CODE_ROOT/'deployment.json')
    request=read_json(path) if path.exists() else dict(processing_run_id=str(uuid4()),stage=stage,input_range=scope,
        code_version=deployed.get('performance_patch_sha256',deployed['source_sha256']))
    assert request['input_range']==scope
    if not path.exists():atomic_json(path,request)
    export=register(db,request);db.connection.rollback()
    output=ROOT/(stage.lower()+'-registration.json');atomic_json(output,export);return output

def catalog_review(*,product_keys=None):
    from pymongo import MongoClient,ReadPreference
    from pymongo.read_concern import ReadConcern
    from product_pipeline.catalog_policy import apply_catalog_policy
    from product_pipeline.publication.mysql import MySQL
    owner_root=BASE/'mysql-review-owner'
    settings=read_json(owner_root/'mysql-review.json')
    os.environ['PIPELINE_REVIEW_MYSQL_PASSWORD']=read_json(owner_root/'owner.private.json')['password']
    owner=MySQL(settings)
    mongo=MongoClient(os.environ['PIPELINE_MONGO_URI'],tz_aware=True,serverSelectionTimeoutMS=5000)
    try:
        db=mongo.get_database('pj3_products_raw',read_preference=ReadPreference.PRIMARY,read_concern=ReadConcern('majority'))
        selection=read_json(ROOT/'selection.json') if (ROOT/'selection.json').exists() else None
        result=apply_catalog_policy(owner,db,CODE_ROOT/'catalog-review',limit=500,product_keys=product_keys if product_keys is not None else selection['product_keys'] if selection else None)
        atomic_json(ROOT/'catalog-review-result.json',dict(**result,finished_at=now()))
        return result
    finally:owner.close();mongo.close()

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
    finally:release_spark(spark)

def main(stage):
    config,options=setup()
    from product_pipeline.publication.mysql import MySQL
    if stage=='activation':
        import production_activation
        import production_work as work
        return production_activation.run(work,config,options)
    if stage=='plan':
        from product_pipeline.silver.source import BronzeSource
        from product_pipeline.silver.config import SilverSettings
        from product_pipeline.windows import seal_window,record_limit
        scope=read_json(ROOT/'scope.json');spark=session(options)
        try:
            source=BronzeSource(spark,SilverSettings(environment_id='pj3-danawa',mongo_database='pj3_products_raw',
                bronze_path=LAKE+'/bronze/products',silver_root=LAKE+'/silver',checkpoint_path=LAKE+'/checkpoints/silver',max_records=record_limit(os.environ.get('PIPELINE_WINDOW_RECORDS','100'))),scope['bronze_version'])
            source.exclude_version(scope['base_bronze_version'])
            docs=source.page(scope['after'])
            if not docs:return dict(status='COMPLETED',stage=stage,drained=True)
            value=seal_window(scope['bronze_version'],scope['after'],docs,base_version=scope['base_bronze_version'])
            path=ROOT/'selection.json'
            if path.exists() and read_json(path)!=value:raise RuntimeError('PIPELINE_WINDOW_SOURCE_CHANGED')
            if not path.exists():atomic_json(path,value)
            return dict(status='COMPLETED',stage=stage,window_hash=value['content_hash'],observations=len(docs),products=len(value['product_keys']))
        finally:release_spark(spark)
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
        recovery_config=CODE_ROOT/'bronze-recovery.json'
        if recovery_config.exists():
            value['checkpoint_recovery']=read_json(recovery_config)
        atomic_json(ROOT/'bronze.json',value)
        args=['--config',str(ROOT/'bronze.json'),'--available-now','--result-file',str(ROOT/'bronze-window.json')]
        if os.environ.get('PIPELINE_CONTINUOUS')=='1':args+=['--max-batches','1']
        if run(args):
            from product_pipeline.errors import PipelineError
            failure=read_json(ROOT/'bronze-window.json') if (ROOT/'bronze-window.json').exists() else {}
            error=PipelineError(failure.get('error_code','BRONZE_FAILED'))
            error.lease_failure=failure.get('lease_failure');error.lease_owner_ref=failure.get('lease_owner_ref');raise error
        window=read_json(ROOT/'bronze-window.json')
        return dict(status='COMPLETED',stage=stage,source_drained=window['source_drained'],committed_batches=window.get('committed_batches'),
                    bronze_runtime_metrics=window.get('runtime_metrics',{}))
    if stage=='silver':
        # Existing epochs keep their pinned owner-input snapshot on resume.
        # New epochs register catalogue decisions before freezing Silver inputs.
        if not (ROOT/'silver-inputs.json').exists():catalog_review()
        from product_pipeline.silver.inputs import KINDS,input_snapshot
        from product_pipeline.registration import mapping_snapshot
        from product_pipeline.silver.cli import main as run
        selection=read_json(ROOT/'selection.json') if (ROOT/'selection.json').exists() else None
        prior=ROOT/'normalization-request.json'
        versions=({'products':selection['bronze_version']} if selection else
                  {'products':read_json(prior)['input_range']['bronze_version']} if prior.exists() else pinned(options,LAKE+'/bronze', ['products']))
        db=MySQL(config)
        try:
            path=ROOT/'silver-inputs.json'
            if path.exists():inputs=read_json(path)
            elif db.one('SELECT COUNT(*) AS n FROM information_schema.TABLES WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s',(config['database'],'pipeline_review_events'))['n']:
                from product_pipeline.review import export_inputs
                with db.read_snapshot():inputs=export_inputs(db,[],as_of=now(),product_keys=selection['product_keys'] if selection else None)['silver']
                atomic_json(path,inputs)
            else:
                inputs={k:input_snapshot(k) for k in KINDS}
                inputs['mappings']=mapping_snapshot(db,evidence_ref='mysql://pj3_products/category_mappings',version='current')['snapshot']
                atomic_json(path,inputs)
            scope=dict(bronze_path=LAKE+'/bronze/products',bronze_version=versions['products'],inputs_hash=digest(inputs))
            # Never change an existing registered epoch's input contract on resume.
            prior=ROOT/'normalization-request.json'
            catalog_mode=(read_json(prior)['input_range'].get('normalized_input')=='danawa_products/observation-binding-v1'
                          if prior.exists() else os.environ.get('PIPELINE_COLLECTOR_NORMALIZED','1')=='1')
            if catalog_mode:scope['normalized_input']='danawa_products/observation-binding-v1'
            if selection:scope['window_hash']=selection['content_hash']
            reuse=ROOT/'silver-reuse-selection.json'
            if reuse.exists():scope['reuse_selection_hash']=digest(read_json(reuse))
            reg=registered(db,'NORMALIZATION',scope)
        finally:db.close()
        value=dict(environment_id='pj3-danawa',mongo_database='pj3_products_raw',bronze_path=LAKE+'/bronze/products',silver_root=LAKE+'/silver',checkpoint_path=LAKE+'/checkpoints/silver',max_records=100)
        atomic_json(ROOT/'silver.json',value)
        args=['--config',str(ROOT/'silver.json'),'--bronze-version',str(versions['products']),'--inputs',str(path),'--registration',str(reg),'--state-dir',str(ROOT/'silver-state'),'--batch-writes']
        if catalog_mode:args+=['--collector-normalized']
        if selection:args+=['--selection',str(ROOT/'selection.json')]
        if reuse.exists():args+=['--reuse-selection',str(reuse)]
        from product_pipeline.windows import checked_stage
        checked_stage(run,args,ROOT/'silver-state','silver')
        return dict(status='COMPLETED',stage=stage)
    if stage=='gold':
        import production_gold_inputs
        import production_work as work
        recovered=production_gold_inputs.legacy_attempt(work)
        if recovered is not None:return recovered
        from product_pipeline.gold.contracts import INPUT_TABLES
        from product_pipeline.gold.cli import main as run
        version_path=ROOT/'silver-versions.json'
        versions=read_json(version_path) if version_path.exists() else pinned(options,LAKE+'/silver',INPUT_TABLES)
        selection=read_json(ROOT/'selection.json') if (ROOT/'selection.json').exists() else None
        inputs={k:dict(status='NOT_PROVIDED',rows=[],evidence_ref=None) for k in ('product_states','embeddings','evaluation_selection','gallery_selection','price_confirmations')}
        inputs.update(contract_version=1,policy=dict(version='pending-intake-v1',evidence_ref='operator://20261019/pending-review-inactive',price_history_mode='UNRESOLVED',price_max_age_seconds=86400,representative_price_kinds=[],bootstrap_single_gallery=True))
        db=MySQL(config)
        try:
            input_path=ROOT/'gold-inputs.json'
            if input_path.exists():inputs=read_json(input_path)
            elif selection:
                # Complete all historical choices outside the DB transaction;
                # unresolved raw IDs get separately registered Silver supplements.
                db.connection.rollback()
                versions,inputs=production_gold_inputs.prepare(work,config,options,selection,versions)
            elif db.one('SELECT COUNT(*) AS n FROM information_schema.TABLES WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s',(config['database'],'pipeline_review_events'))['n'] and db.one('SELECT COUNT(*) AS n FROM pipeline_review_events')['n']:
                from product_pipeline.review import export_inputs
                from product_pipeline.gold.source import SilverSource,iter_groups
                from product_pipeline.gold.spool import review_spool
                from product_pipeline.gold.config import GoldSettings
                spark=session(options)
                try:
                    source=SilverSource(spark,GoldSettings(environment_id='pj3-danawa',silver_root=LAKE+'/silver',gold_root=LAKE+'/gold',checkpoint_path=LAKE+'/checkpoints/gold',max_records=100),versions)
                    if selection:source.restrict_products(selection['product_keys'])
                    # Read bounded tables before opening a MySQL snapshot.
                    # No Spark/S3 I/O while holding the review DB transaction.
                    choices=None
                    choice_path=ROOT/'silver-state/evaluation-selection.json'
                    if choice_path.exists() and selection:
                        from product_pipeline.silver.selection import validate
                        choices=validate(read_json(choice_path),selection)
                    with review_spool(iter_groups(source),directory=ROOT) as groups:
                        with db.read_snapshot():inputs=export_inputs(db,groups,as_of=now(),evaluation_selection=choices)['gold']
                finally:release_spark(spark)
            else:
                if os.environ.get('PIPELINE_CONTINUOUS')=='1':inputs['gallery_selection']=gallery_selection(options,versions)
                inputs['content_hash']=digest(inputs)
            if not input_path.exists():atomic_json(input_path,inputs)
            if not version_path.exists():atomic_json(version_path,versions)
            elif read_json(version_path)!=versions:raise RuntimeError('GOLD_PREPARED_VERSIONS_CHANGED')
            scope=dict(silver_root=LAKE+'/silver',silver_versions=versions,inputs_hash=digest(inputs))
            if selection:scope['window_hash']=selection['content_hash']
            reg=registered(db,'GOLD_BUILD',scope)
        finally:db.close()
        value=dict(environment_id='pj3-danawa',silver_root=LAKE+'/silver',gold_root=LAKE+'/gold',checkpoint_path=LAKE+'/checkpoints/gold',max_records=100)
        atomic_json(ROOT/'gold.json',value)
        atpath=ROOT/'gold-as-of.json'
        if not atpath.exists():atomic_json(atpath,dict(as_of=now()))
        args=['--config',str(ROOT/'gold.json'),'--versions',str(ROOT/'silver-versions.json'),'--inputs',str(ROOT/'gold-inputs.json'),'--registration',str(reg),'--state-dir',str(ROOT/'gold-state'),'--as-of',read_json(atpath)['as_of'],'--batch-writes']
        if selection:args+=['--selection',str(ROOT/'selection.json')]
        from product_pipeline.windows import checked_stage
        checked_stage(run,args,ROOT/'gold-state','gold')
        return dict(status='COMPLETED',stage=stage,gold_state_dir=str(ROOT))
    if stage=='publish':
        gold_root=Path(read_json(ROOT/'gold-result.json').get('gold_state_dir',str(ROOT)))
        if os.environ.get('PIPELINE_CONTINUOUS')=='1':
            from product_pipeline.publication.source import GoldSource
            from product_pipeline.publication.streaming import publish_pending
            runs=sorted((gold_root/'gold-state/runs').glob('*.json'),key=lambda p:p.stat().st_mtime)
            gold=read_json(runs[-1]);assert gold['status']=='COMPLETED' and gold['source_drained']
            spark=session(options);db=MySQL(config)
            try:
                source=GoldSource(spark,gold['export_manifest_ref'],LAKE+'/gold',LAKE+'/checkpoints/gold')
                if (ROOT/'selection.json').exists():
                    from product_pipeline.publication.source import CachedGoldSource
                    with timed('publication_gold_read'):source=CachedGoldSource(source)
                def committed(value):atomic_json(ROOT/'publish-progress.json',dict(**value,updated_at=now()))
                with timed('publication_pending_mysql'):
                    parent=os.environ.get('PIPELINE_ACTIVATION_PARENT')
                    if parent:
                        from production_activation import confirmed_intake
                        result=confirmed_intake(ROOT,parent,db,source)
                    else:
                        result=publish_pending(source,db,ROOT/'pending-plans','25701b75-22f9-5fea-9148-d7a4c006a440',progress=committed)
                review_count=db.one('SELECT COUNT(*) AS n FROM pipeline_review_events')['n']
                db.connection.rollback()
                reviewed=dict(status='WAITING_OWNER_INPUTS',activated=0,waiting=source.manifest['product_count'])
                if review_count:
                    from product_pipeline.publication.reviewed import publish_reviewed
                    owner_root=BASE/'mysql-review-owner'
                    owner_settings=read_json(owner_root/'mysql-review.json')
                    os.environ['PIPELINE_REVIEW_MYSQL_PASSWORD']=read_json(owner_root/'owner.private.json')['password']
                    owner=MySQL(owner_settings)
                    try:
                        with timed('publication_activation_mysql'):
                            reviewed=publish_reviewed(source,owner,ROOT/'reviewed-plans',
                                progress=lambda value:atomic_json(ROOT/'review-progress.json',dict(**value,updated_at=now())))
                    finally:owner.close()
                result['reviewed']=reviewed
                counts=db.one("SELECT COUNT(*) AS products,SUM(status='PENDING_REVIEW') AS pending,SUM(recommendation_ready) AS recommendation_ready FROM products WHERE source_code='danawa'")
                return dict(**result,counts={k:int(v or 0) for k,v in counts.items()})
            finally:db.close();release_spark(spark)
        from product_pipeline.publication.cli import prepare_from_source
        from product_pipeline.publication.source import GoldSource
        from product_pipeline.publication.plan import load_plan
        from product_pipeline.publication.runner import publish,audit_publication
        runs=sorted((gold_root/'gold-state/runs').glob('*.json'),key=lambda p:p.stat().st_mtime)
        gold=read_json(runs[-1]);assert gold['status']=='COMPLETED' and gold['source_drained']
        output=ROOT/'pending-plans'
        db=MySQL(config)
        try:
            if not output.exists():
                spark=session(options)
                try:prepare_from_source(GoldSource(spark,gold['export_manifest_ref'],LAKE+'/gold',LAKE+'/checkpoints/gold'),{},db.target,output,pending_category_id='25701b75-22f9-5fea-9148-d7a4c006a440')
                finally:release_spark(spark)
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


def run_at(stage, directory):
    """Sequential in-process stages keep one Driver without mixing run scopes."""
    from contextlib import redirect_stdout,redirect_stderr
    global ROOT
    previous=ROOT;ROOT=Path(directory);ROOT.mkdir(parents=True,exist_ok=True,mode=0o700)
    started=time.monotonic();before=metrics()
    try:
        with (ROOT/(stage+'.private.log')).open('a') as log,redirect_stdout(log),redirect_stderr(log):
            result=main(stage)
        after=metrics()
        result.update(duration_ms=round((time.monotonic()-started)*1000),finished_at=now(),
            runtime_metrics={k:v-before.get(k,0) for k,v in after.items()})
        atomic_json(ROOT/(stage+'-result.json'),result)
        return result
    except BaseException as exc:
        atomic_json(ROOT/(stage+'-failure.json'),dict(error_code=getattr(exc,'code',type(exc).__name__),lease_failure=getattr(exc,'lease_failure',None),lease_owner_ref=getattr(exc,'lease_owner_ref',None),at=now(),duration_ms=round((time.monotonic()-started)*1000),runtime_metrics={k:v-before.get(k,0) for k,v in metrics().items()}))
        raise
    finally:ROOT=previous

if __name__=='__main__':
    stage=sys.argv[1];began=time.monotonic()
    try:
        result=main(stage);result.update(duration_ms=round((time.monotonic()-began)*1000),finished_at=now())
        atomic_json(ROOT/(stage+'-result.json'),result)
    except Exception as exc:
        atomic_json(ROOT/(stage+'-failure.json'),dict(error_code=getattr(exc,'code',type(exc).__name__),lease_failure=getattr(exc,'lease_failure',None),lease_owner_ref=getattr(exc,'lease_owner_ref',None),driver_code=exc.args[0] if exc.args and isinstance(exc.args[0],int) else None,at=now()))
        raise
