"""Explicit owner review commands. No inferred approvals or runtime schema writes."""
import argparse
import os
from pathlib import Path
import time

from .console import emit
from .contracts import digest,now
from .errors import PipelineError
from .files import atomic_json,read_json,parse
from . import review
from .publication.mysql import MySQL


def write_new(path,value):
    path=Path(path)
    if path.exists():raise PipelineError('REVIEW_OUTPUT_EXISTS')
    from .contracts import canonical
    if len(canonical(value).encode())>32*1024*1024:raise PipelineError('REVIEW_OUTPUT_TOO_LARGE')
    atomic_json(path,value)


def spark_session():
    from pyspark.sql import SparkSession
    from .spark_storage import storage_options,bind_spark_credentials
    options=storage_options(parse(os.environ.get('PUBLICATION_SPARK_OPTIONS','{}')))
    builder=SparkSession.builder.appName('pipeline-owner-review')
    for key,value in options.items():
        if not key.startswith('spark.') or not isinstance(value,str):raise PipelineError('PUBLISH_SPARK_OPTIONS_INVALID')
        builder=builder.config(key,value)
    spark=(builder.config('spark.sql.session.timeZone','UTC')
        .config('spark.sql.extensions','io.delta.sql.DeltaSparkSessionExtension')
        .config('spark.sql.catalog.spark_catalog','org.apache.spark.sql.delta.catalog.DeltaCatalog').getOrCreate())
    bind_spark_credentials(spark)
    return spark


def main(argv=None):
    p=argparse.ArgumentParser(description='명시적 검수 등록 → Silver/Gold 재평가 → 승인 게시')
    sub=p.add_subparsers(dest='action',required=True)
    for name in ('seal','validate'):
        q=sub.add_parser(name);q.add_argument('--bundle',type=Path,required=True)
        if name=='seal':q.add_argument('--output',type=Path,required=True)
    for name in ('preflight','inventory','template','draft','apply','export-inputs','guard','activate'):
        q=sub.add_parser(name);q.add_argument('--config',type=Path,required=True);q.add_argument('--local-test',action='store_true')
        if name in {'apply','guard','activate'}:q.add_argument('--apply',action='store_true')
        if name in {'inventory','template','draft','export-inputs','guard'}:q.add_argument('--output',type=Path,required=True)
        if name in {'inventory','template'}:q.add_argument('--limit',type=int,default=100)
        if name=='draft':q.add_argument('--requests',type=Path,required=True)
        if name=='apply':q.add_argument('--bundle',type=Path,required=True)
        if name=='export-inputs':
            q.add_argument('--silver-config',type=Path,required=True);q.add_argument('--silver-versions',type=Path,required=True)
            q.add_argument('--as-of',default=None)
        if name=='guard':
            for field in ('gold-export','gold-root','archive-root','product-key'):q.add_argument('--'+field,required=True)
        if name=='activate':q.add_argument('--plan',type=Path,required=True)
    args=p.parse_args(argv);db=spark=None;began=time.monotonic()
    try:
        if args.action in {'seal','validate'}:
            bundle=read_json(args.bundle)
            if args.action=='seal':bundle=review.seal(bundle)
            review.validate(bundle)
            if args.action=='seal':write_new(args.output,bundle)
            result=dict(status='REVIEW_BUNDLE_VALIDATED_NOT_APPLIED',bundle_hash=bundle['content_hash'])
        else:
            if args.action in {'apply','guard','activate'} and not args.apply:raise PipelineError('REVIEW_EXPLICIT_APPLY_REQUIRED')
            db=MySQL(read_json(args.config),local_test=args.local_test)
            if args.action=='preflight':result=dict(status='REVIEW_OWNER_VERIFIED',actor=review.preflight(db),target=db.target)
            elif args.action in {'inventory','template'}:
                if not 1<=args.limit<=1000:raise PipelineError('REVIEW_BUNDLE_LIMIT_OR_TARGET')
                db.preflight()
                with db.read_snapshot():
                    products=db.all('SELECT id,product_key,name,status,category_id,source_refs_json FROM products WHERE source_code=%s ORDER BY product_key LIMIT %s',('danawa',args.limit))
                    images=[]
                    for product in products:
                        images.extend(db.all('SELECT i.id,i.product_id,i.source_version,i.source_url,i.raw_document_id,i.active,i.usage_review_json,a.checksum,a.object_key FROM product_images i JOIN assets a ON a.id=i.asset_id WHERE i.product_id=%s ORDER BY i.display_order,i.id',(product['id'],)))
                    categories=db.all('SELECT id,code,name,active,recognition_enabled FROM categories ORDER BY code')
                if args.action=='template':
                    review.preflight(db)
                    requests=[dict(kind='PRODUCT',payload=dict(product_key=r['product_key'],service_category_id=None,active=False,supported=False,
                        applicable_attributes=[],raw_document_ids=sorted({s['raw_document_id'] for s in parse(r['source_refs_json'])}))) for r in products]
                    requests.extend(dict(kind='IMAGE',payload=dict(image_id=r['id'],source_version=r['source_version'],checksum=r['checksum'],
                        scopes=[],permission_evidence_ref=None,asset_policy=None)) for r in images)
                    requests.append(dict(kind='POLICY',payload=dict(price_history_mode=None,price_max_age_seconds=None,representative_price_kinds=[])))
                    value=review.draft(db,requests)
                else:value=dict(status='INVENTORY_NOT_APPROVAL',target=db.target,products=products,images=images,categories=categories)
                write_new(args.output,value)
                result=dict(status='DRAFT_NOT_APPLIED' if args.action=='template' else 'INVENTORY_WRITTEN',products=len(products),images=len(images))
            elif args.action=='draft':
                review.preflight(db);value=review.draft(db,read_json(args.requests));write_new(args.output,value)
                result=dict(status='DRAFT_NOT_APPLIED',items=len(value['items']))
            elif args.action=='apply':result=review.apply(db,read_json(args.bundle))
            elif args.action=='export-inputs':
                from .gold.config import GoldSettings
                from .gold.source import SilverSource
                review.preflight(db);spark=spark_session()
                source=SilverSource(spark,GoldSettings.load(args.silver_config),read_json(args.silver_versions))
                def groups():
                    after=None
                    while True:
                        keys=source.products(after)
                        if not keys:return
                        for key in keys:yield from source.groups(key)
                        after=keys[-1]
                with db.read_snapshot():value=review.export_inputs(db,groups(),as_of=args.as_of or now())
                # A sealed export binds owner decisions to the pinned Silver input versions.
                value=review.seal(dict(value,silver_versions=source.versions,target=db.target))
                write_new(args.output,value)
                result=dict(status='REVIEW_INPUTS_EXPORTED',content_hash=value['content_hash'],blocked=len(value['blocked']))
            elif args.action=='guard':
                from .publication.source import GoldSource
                from .publication.plan import make_plan
                if args.output.exists():raise PipelineError('REVIEW_OUTPUT_EXISTS')
                spark=spark_session();source=GoldSource(spark,args.gold_export,args.gold_root,args.archive_root)
                group=next((g for g in source.groups() if g['receipt']['product_key']==args.product_key),None)
                if group is None:raise PipelineError('REVIEW_GOLD_PRODUCT_NOT_FOUND')
                value=review.approve_guard(db,group,source.manifest['gold_versions'],price_history_mode=source.price_history_mode,as_of=now(),
                    owner_inputs=source.read(source.manifest['input_refs']['inputs']))
                plan=make_plan([group],source.manifest['gold_versions'],export_hash=source.manifest['content_hash'],
                    as_of=source.manifest['as_of'],guard_hashes={args.product_key:value['guard_hash']},target=db.target,
                    price_history_mode=source.price_history_mode,price_max_age_seconds=source.price_max_age_seconds,activate_pending=True)
                write_new(args.output,plan);result=dict(status='GUARD_REGISTERED_ACTIVATION_PLAN_READY',plan_id=plan['plan_id'])
            else:
                from .publication.plan import load_plan
                from .publication.runner import publish
                from .publication.audit import audit_publication
                review.preflight(db);plan=load_plan(args.plan)
                if plan.get('publication_mode')!='ACTIVATE_REVIEWED':raise PipelineError('REVIEW_ACTIVATION_PLAN_REQUIRED')
                result=publish(plan,db)
                with db.read_snapshot():result=dict(result,audit=audit_publication(plan,db))
        emit(dict(result,duration_ms=round((time.monotonic()-began)*1000)));return 0
    except Exception as exc:
        emit(dict(status='FAILED',error_code=getattr(exc,'code',type(exc).__name__),duration_ms=round((time.monotonic()-began)*1000)));return 2
    finally:
        if db:db.close()
        if spark:spark.stop()


if __name__=='__main__':raise SystemExit(main())
