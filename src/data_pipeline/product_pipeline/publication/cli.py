"""Explicit prepare/preflight/publish commands; neither implicit writes nor DDL."""
import argparse
import os
from pathlib import Path
import time

from ..console import emit
from ..contracts import digest
from ..errors import PipelineError
from ..files import atomic_json,read_json,parse
from .schema import CONTRACT_HASH,reference_ddl
from .mysql import MySQL
from .plan import make_plan,load_plan
from .runner import publish
from .preparation import prepare_from_source



def main(argv=None):
    p=argparse.ArgumentParser(description='문서 계약 기반 Gold→MySQL 게시')
    sub=p.add_subparsers(dest='action',required=True)
    ddl=sub.add_parser('reference-ddl');ddl.add_argument('--output',type=Path,required=True)
    migration=sub.add_parser('migration-check',help='적용/롤백 SQL과 코드 계약의 일치 검사. DB 연결·DDL 없음')
    migration.add_argument('--directory',type=Path,default=Path(__file__).resolve().parents[2]/'sql/migrations')
    prep=sub.add_parser('prepare')
    for name in ('gold-export','gold-root','archive-root','guard-hashes','mysql-config','output'):prep.add_argument('--'+name,required=True)
    prep.add_argument('--batch-size',type=int,default=100)
    prep.add_argument('--pending-category-id',help='명시적 검수 대기 적재. 비활성 UNMAPPED 카테고리 UUID 필요')
    for name in ('preflight','publish'):
        q=sub.add_parser(name);q.add_argument('--config',required=True,type=Path);q.add_argument('--local-test',action='store_true')
        if name=='publish':
            q.add_argument('--plan',required=True,type=Path);q.add_argument('--apply',action='store_true')
    inspect=sub.add_parser('schema-diff')
    inspect.add_argument('--config',type=Path,required=True);inspect.add_argument('--output',type=Path,required=True)
    inspect.add_argument('--local-test',action='store_true')
    audit=sub.add_parser('audit')
    audit.add_argument('--config',type=Path,required=True);audit.add_argument('--plan',type=Path,required=True)
    audit.add_argument('--local-test',action='store_true')
    args=p.parse_args(argv);db=None;spark=None;began=time.monotonic()
    try:
        if args.action=='migration-check':
            from .migration import verify_bundle
            emit({**verify_bundle(args.directory),'duration_ms':round((time.monotonic()-began)*1000)});return 0
        if args.action=='reference-ddl':
            args.output.parent.mkdir(parents=True,exist_ok=True)
            with args.output.open('x',encoding='utf-8',newline='\n') as f:f.write(reference_ddl())
            emit(dict(status='REFERENCE_DDL_WRITTEN_NOT_APPLIED',contract_hash=CONTRACT_HASH));return 0
        if args.action=='prepare':
            from pyspark.sql import SparkSession
            from .source import GoldSource
            options=parse(os.environ.get('PUBLICATION_SPARK_OPTIONS','{}'))
            from ..spark_storage import storage_options,bind_spark_credentials
            options=storage_options(options)
            builder=SparkSession.builder.appName('gold-publication-plan')
            for k,v in options.items():
                if not k.startswith('spark.') or not isinstance(v,str):raise PipelineError('PUBLISH_SPARK_OPTIONS_INVALID')
                builder=builder.config(k,v)
            spark=(builder.config('spark.sql.session.timeZone','UTC')
                .config('spark.sql.extensions','io.delta.sql.DeltaSparkSessionExtension')
                .config('spark.sql.catalog.spark_catalog','org.apache.spark.sql.delta.catalog.DeltaCatalog').getOrCreate())
            bind_spark_credentials(spark)
            config=read_json(args.mysql_config)
            target=digest(dict(target_id=config['target_id'],database=config['database']))
            result=prepare_from_source(GoldSource(spark,args.gold_export,args.gold_root,args.archive_root),
                read_json(args.guard_hashes),target,args.output,batch_size=args.batch_size,pending_category_id=args.pending_category_id)
        elif args.action=='schema-diff':
            from .mysql import schema_difference
            db=MySQL(read_json(args.config),local_test=args.local_test)
            if args.output.exists():raise PipelineError('SCHEMA_DIFF_OUTPUT_EXISTS')
            result=schema_difference(db);atomic_json(args.output,result)
        else:
            if args.action=='publish' and not args.apply:raise PipelineError('PUBLICATION_EXPLICIT_APPLY_REQUIRED')
            plan=load_plan(args.plan) if args.action in {'publish','audit'} else None
            db=MySQL(read_json(args.config),local_test=args.local_test)
            if args.action=='audit':
                from .audit import audit_publication
                db.preflight()
                with db.read_snapshot():result=audit_publication(plan,db)
            else:result=publish(plan,db) if plan else db.preflight()
        emit({**result,'duration_ms':round((time.monotonic()-began)*1000)});return 0
    except Exception as exc:
        emit(dict(status='FAILED',error_code=exc.code if isinstance(exc,PipelineError) else type(exc).__name__,
                  duration_ms=round((time.monotonic()-began)*1000)));return 2
    finally:
        if db:db.close()
        if spark:spark.stop()


if __name__=='__main__':raise SystemExit(main())
