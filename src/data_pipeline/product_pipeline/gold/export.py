"""Seal the exact catalog selection and aggregate distinct counts at pinned Gold versions."""
from ..contracts import digest
from ..errors import Conflict,PipelineError
from ..spark_session import measured,bump,timed


@measured('gold_export_finalize')
def finalize(spark,sink,checkpoint,pages,*,as_of,source_drained,input_refs,max_categories=10000):
    from pyspark import StorageLevel
    from pyspark.sql import functions as F
    from pyspark.sql.types import StructType,StructField,StringType,ArrayType
    with timed('gold_export_version_lookup'):
        versions=sink.versions_many(sink.paths) if hasattr(sink,'versions_many') else {t:sink.version(t) for t in sink.paths}
    # Only the small page reference list stays on Driver; selection rows are read by Spark.
    schema=StructType([StructField('selections',ArrayType(StructType([
        StructField('product_key',StringType()),StructField('evaluation_key',StringType())])))])
    if not pages:raise PipelineError('GOLD_EMPTY_SELECTION')
    selection=(spark.read.schema(schema).option('multiLine','true').json(pages)
        .select(F.explode('selections').alias('r')).select('r.*'))
    cached=[]
    def keep(frame):
        frame=frame.persist(StorageLevel.DISK_ONLY);cached.append(frame);return frame
    selection=keep(selection)
    try:
        counts=selection.groupBy('product_key').agg(F.count('*').alias('n'),
            F.sum(F.col('evaluation_key').isNull().cast('long')).alias('null_evaluations'))
        with timed('gold_export_selection_validate'):
            stats=counts.agg(F.sum('n').alias('expected'),
                F.max((F.col('product_key').isNull()|(F.col('null_evaluations')>0)).cast('int')).alias('invalid'),
                F.max((F.col('n')!=1).cast('int')).alias('duplicate')).first()
        if stats['invalid']:raise Conflict('GOLD_SELECTION_INVALID')
        if stats['duplicate']:raise Conflict('GOLD_SELECTION_DUPLICATE_PRODUCT')
        expected=int(stats['expected'] or 0)
        bump('gold_export_selected_products',expected)
        def selected(table):
            data=spark.read.format('delta').option('versionAsOf',versions[table]).load(sink.paths[table])
            return data.join(selection,(data.product_key==selection.product_key)&
                (data.gold_evaluation_key==selection.evaluation_key),'left_semi')
        catalog=keep(selected('product_catalog'))
        receipts=selected('pipeline_evaluations')
        with timed('gold_export_catalog_receipt_check'):
            checks=catalog.agg(F.count('*').alias('n')).unionByName(receipts.agg(F.count('*').alias('n'))).collect()
        if len(checks)!=2 or any(r['n']!=expected for r in checks):raise Conflict('GOLD_SELECTION_OUTPUT_MISSING')
        categories=catalog.select('product_key','gold_evaluation_key','service_category_id')
        contribution=keep(selected('quality_daily'))
        images=keep(selected('product_images').join(categories,['product_key','gold_evaluation_key'],'inner'))
        def grouped(frame):
            return frame.withColumn('category_group',F.coalesce('service_category_id',F.lit('__UNMAPPED__'))).rollup('category_group')
        def scope(frame):
            return (frame.withColumn('scope',F.when(F.col('_global')==1,F.lit('GLOBAL')).otherwise(F.lit('CATEGORY')))
                .withColumn('category_group',F.when(F.col('_global')==1,F.lit('__GLOBAL__')).otherwise(F.col('category_group'))).drop('_global'))
        # Rollup computes GLOBAL distinct files from the original image rows,
        # never by adding category-level counts (shared checksums stay distinct).
        with timed('gold_export_quality_rollup'):
            base=scope(grouped(catalog).agg(F.grouping('category_group').alias('_global'),F.countDistinct('product_key').alias('products'),
                F.sum(F.col('catalog_ready').cast('long')).alias('catalog_ready'),
                F.sum(F.col('recommendation_ready').cast('long')).alias('recommendation_ready')))
            observations=scope(grouped(contribution).agg(F.grouping('category_group').alias('_global'),F.sum('observation_count').alias('observations'),
                F.sum('evaluation_count').alias('evaluations'),
                F.sum('applicable_attribute_count').alias('applicable_attributes'),
                F.sum('confirmed_applicable_attribute_count').alias('confirmed_applicable_attributes'),
                F.sum((F.col('applicability_status')!='CONFIRMED_OWNER_INPUT').cast('long')).alias('applicability_unprovided_products')))
            files=scope(grouped(images).agg(F.grouping('category_group').alias('_global'),F.countDistinct('image_relation_key').alias('image_relations'),
                F.countDistinct('image_checksum').alias('unique_files')))
            rows=(base.join(observations,['scope','category_group'],'left').join(files,['scope','category_group'],'left')
                  .na.fill(0,subset=['image_relations','unique_files']).limit(max_categories+2).collect()) if expected else []
        if sum(r['scope']=='CATEGORY' for r in rows)>max_categories:raise PipelineError('GOLD_QUALITY_CATEGORY_LIMIT')
        metrics=[]
        for row in rows:
            metrics.append(dict(**row.asDict(),metric_date=as_of[:10],
                attribute_applicability_status='OWNER_POLICY_PARTIAL' if row['applicability_unprovided_products'] else 'OWNER_POLICY_COMPLETE'))
        bump('gold_export_cached_frames',len(cached));bump('gold_export_rollup_batches')
    finally:
        for frame in reversed(cached):frame.unpersist()
    summary=dict(contract_version=1,stage='GOLD_QUALITY_DAILY',as_of=as_of,
        coverage=('FULL_FIXED_WINDOW' if 'selection' in input_refs else 'FULL_FIXED_SILVER_RANGE') if source_drained else 'PARTIAL_FIXED_SILVER_RANGE',
        gold_versions=versions,metrics=sorted(metrics,key=lambda r:(r['scope'],r['category_group'])))
    with timed('gold_export_summary_archive'):
        summary_ref=checkpoint.archive(summary)
    manifest=dict(contract_version=1,stage='GOLD_EXPORT',as_of=as_of,source_drained=source_drained,
        selection_pages=pages,product_count=expected,gold_versions=versions,quality_summary_ref=summary_ref,
        input_refs=input_refs,mysql_published=False,publication_status='BLOCKED_DDL_GUARD_REQUIRED')
    manifest['content_hash']=digest(manifest)
    with timed('gold_export_manifest_archive'):
        manifest_ref=checkpoint.archive(manifest)
    return dict(export_manifest_ref=manifest_ref,quality_summary_ref=summary_ref,product_count=expected)
