"""Freeze complete historical Silver choices before registering a Gold build."""
import shutil
from pathlib import Path
from product_pipeline.spark_session import measured


def _copy(source,target):
    if target.exists():
        if source.read_bytes()!=target.read_bytes():raise RuntimeError('GOLD_RECOVERY_INPUT_CHANGED')
    else:
        target.parent.mkdir(parents=True,exist_ok=True,mode=0o700);shutil.copyfile(source,target);target.chmod(0o600)


def legacy_attempt(work):
    """Never rewrite an already registered failed Gold input contract."""
    root=work.ROOT
    if not (root/'selection.json').exists() or not (root/'gold-inputs.json').exists() or (root/'gold-preparation.json').exists():return None
    target=root/'gold-recovery-v2';target.mkdir(exist_ok=True,mode=0o700)
    for name in ('selection.json','silver-inputs.json','silver-state/evaluation-selection.json'):
        _copy(root/name,target/name)
    result=work.run_at('gold',target)
    return dict(result,gold_state_dir=str(target),recovery_of=str(root))


@measured('gold_input_preparation')
def prepare(work,config,options,selection,versions):
    from product_pipeline.files import read_json,atomic_json
    from product_pipeline.contracts import digest,now
    from product_pipeline.errors import Conflict,PipelineError
    from product_pipeline.gold.source import SilverSource,iter_groups
    from product_pipeline.gold.spool import review_spool
    from product_pipeline.gold.selection import published_choices,resolve
    from product_pipeline.gold.config import GoldSettings
    from product_pipeline.gold.contracts import INPUT_TABLES
    from product_pipeline.silver.selection import validate
    from product_pipeline.publication.mysql import MySQL
    from product_pipeline.review import export_inputs
    root=work.ROOT
    choices=validate(read_json(root/'silver-state/evaluation-selection.json'),selection)
    supplemental=root/'history-silver-selection.json'
    if supplemental.exists():
        saved=read_json(supplemental)
        if saved['content_hash']!=digest({k:v for k,v in saved.items() if k!='content_hash'}):raise Conflict('GOLD_HISTORY_SELECTION_CHANGED')
        if saved['window_hash']!=selection['content_hash']:raise Conflict('GOLD_HISTORY_SELECTION_CHANGED')
        choices=choices+saved['choices']
        # A previous supplement may have committed before a Driver restart.
        versions=work.pinned(options,work.LAKE+'/silver',INPUT_TABLES)
    for attempt in range(2):
        spark=work.session(options)
        try:
            source=SilverSource(spark,GoldSettings(environment_id='pj3-danawa',silver_root=work.LAKE+'/silver',
                gold_root=work.LAKE+'/gold',checkpoint_path=work.LAKE+'/checkpoints/gold',max_records=100),versions)
            source.restrict_products(selection['product_keys'])
            with review_spool(iter_groups(source),directory=root) as groups:
                db=MySQL(config)
                try:
                    with db.read_snapshot():
                        prior=published_choices(db,selection['product_keys'])
                        selected,missing=resolve(groups,choices,prior)
                        inputs=None if missing else export_inputs(db,groups,as_of=now(),evaluation_selection=selected)['gold']
                finally:db.close()
        finally:work.release_spark(spark)
        if not missing:
            atomic_json(root/'gold-preparation.json',dict(contract_version=2,window_hash=selection['content_hash'],
                silver_versions=versions,choices=selected,inputs_hash=digest(inputs),prepared_at=now()))
            return versions,inputs
        atomic_json(root/'history-selection-required.json',dict(window_hash=selection['content_hash'],missing=missing))
        if attempt:raise PipelineError('GOLD_HISTORY_SUPPLEMENT_INCOMPLETE')
        additions=_supplement(work,options,selection,missing)
        body=dict(window_hash=selection['content_hash'],choices=additions)
        atomic_json(supplemental,dict(body,content_hash=digest(body)))
        choices+=additions;versions=work.pinned(options,work.LAKE+'/silver',INPUT_TABLES)
    raise AssertionError('unreachable')


def _supplement(work,options,selection,missing):
    """At most 100 raw IDs per independently registered immutable supplement."""
    from pyspark.sql import functions as F
    from product_pipeline.files import read_json,atomic_json
    from product_pipeline.contracts import digest
    from product_pipeline.errors import Conflict,PipelineError
    from product_pipeline.windows import seal_window
    from product_pipeline.silver.selection import validate
    from product_pipeline.silver.source import BronzeSource
    from product_pipeline.silver.config import SilverSettings
    root=work.ROOT;result=[]
    missing=sorted(missing,key=lambda m:(m['product_key'],m['raw_document_id']))
    for start in range(0,len(missing),100):
        batch=missing[start:start+100];identity=digest(batch);target=root/'history-silver'/identity
        target.mkdir(parents=True,exist_ok=True,mode=0o700)
        scope=target/'selection.json'
        if not scope.exists():
            spark=work.session(options)
            try:
                source=BronzeSource(spark,SilverSettings(environment_id='pj3-danawa',mongo_database='pj3_products_raw',
                    bronze_path=work.LAKE+'/bronze/products',silver_root=work.LAKE+'/silver',checkpoint_path=work.LAKE+'/checkpoints/silver'),selection['bronze_version'])
                rows=source.frame.filter(F.col('raw_document_id').isin([r['raw_document_id'] for r in batch])).select(
                    'raw_document_id','observation_key','source_code','source_product_id').limit(101).collect()
                actual={(r['source_code']+':'+r['source_product_id'],r['raw_document_id']) for r in rows}
                expected={(r['product_key'],r['raw_document_id']) for r in batch}
                if actual!=expected or len(rows)!=len(batch):raise Conflict('GOLD_HISTORY_BRONZE_SOURCE_MISSING_OR_CHANGED')
                atomic_json(scope,seal_window(selection['bronze_version'],None,[r.asDict() for r in rows]))
            finally:work.release_spark(spark)
        _copy(root/'silver-inputs.json',target/'silver-inputs.json')
        done=target/'silver-result.json'
        if not done.exists():work.run_at('silver',target)
        if read_json(done).get('status')!='COMPLETED':raise PipelineError('GOLD_HISTORY_SILVER_NOT_COMPLETED')
        result+=validate(read_json(target/'silver-state/evaluation-selection.json'),read_json(scope))
    return result
