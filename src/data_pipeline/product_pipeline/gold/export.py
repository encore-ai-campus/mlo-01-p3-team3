"""Seal the exact catalog selection and aggregate distinct counts at pinned Gold versions."""
from ..contracts import digest
from ..errors import Conflict,PipelineError


def finalize(spark,sink,checkpoint,pages,*,as_of,source_drained,input_refs,max_categories=10000):
    from pyspark.sql import functions as F
    from pyspark.sql.types import StructType,StructField,StringType,ArrayType
    versions={t:sink.version(t) for t in sink.paths}
    # Only the small page reference list stays on Driver; selection rows are read by Spark.
    schema=StructType([StructField('selections',ArrayType(StructType([
        StructField('product_key',StringType()),StructField('evaluation_key',StringType())])))])
    if not pages:raise PipelineError('GOLD_EMPTY_SELECTION')
    selection=(spark.read.schema(schema).option('multiLine','true').json(pages)
        .select(F.explode('selections').alias('r')).select('r.*'))
    if selection.filter('product_key IS NULL OR evaluation_key IS NULL').limit(1).count():
        raise Conflict('GOLD_SELECTION_INVALID')
    if selection.groupBy('product_key').count().filter('count != 1').limit(1).count():
        raise Conflict('GOLD_SELECTION_DUPLICATE_PRODUCT')
    expected=selection.count()
    def selected(table):
        data=spark.read.format('delta').option('versionAsOf',versions[table]).load(sink.paths[table])
        return data.join(selection,(data.product_key==selection.product_key)&
            (data.gold_evaluation_key==selection.evaluation_key),'left_semi')
    catalog=selected('product_catalog')
    receipts=selected('pipeline_evaluations')
    if catalog.count()!=expected or receipts.count()!=expected:raise Conflict('GOLD_SELECTION_OUTPUT_MISSING')
    # Category and GLOBAL counts are calculated independently to avoid summing shared files.
    categories=catalog.select('product_key','gold_evaluation_key','service_category_id')
    contribution=selected('quality_daily')
    images=selected('product_images').join(categories,['product_key','gold_evaluation_key'],'inner')
    metrics=[]
    for scope in ('CATEGORY','GLOBAL'):
        c=catalog if scope=='CATEGORY' else catalog.withColumn('service_category_id',F.lit('__GLOBAL__'))
        q=contribution if scope=='CATEGORY' else contribution.withColumn('service_category_id',F.lit('__GLOBAL__'))
        i=images if scope=='CATEGORY' else images.withColumn('service_category_id',F.lit('__GLOBAL__'))
        # Use a non-service sentinel in this report only; NULL category remains explicit in catalogs.
        c=c.withColumn('category_group',F.coalesce('service_category_id',F.lit('__UNMAPPED__')))
        q=q.withColumn('category_group',F.coalesce('service_category_id',F.lit('__UNMAPPED__')))
        i=i.withColumn('category_group',F.coalesce('service_category_id',F.lit('__UNMAPPED__')))
        base=c.groupBy('category_group').agg(F.countDistinct('product_key').alias('products'),
            F.sum(F.col('catalog_ready').cast('long')).alias('catalog_ready'),
            F.sum(F.col('recommendation_ready').cast('long')).alias('recommendation_ready'))
        observations=q.groupBy('category_group').agg(F.sum('observation_count').alias('observations'),
            F.sum('evaluation_count').alias('evaluations'),
            F.sum('applicable_attribute_count').alias('applicable_attributes'),
            F.sum('confirmed_applicable_attribute_count').alias('confirmed_applicable_attributes'),
            F.sum((F.col('applicability_status')!='CONFIRMED_OWNER_INPUT').cast('long')).alias('applicability_unprovided_products'))
        files=i.groupBy('category_group').agg(F.countDistinct('image_relation_key').alias('image_relations'),
            F.countDistinct('image_checksum').alias('unique_files'))
        rows=(base.join(observations,'category_group','left').join(files,'category_group','left')
              .na.fill(0,subset=['image_relations','unique_files']).limit(max_categories+1).collect())
        if len(rows)>max_categories:raise PipelineError('GOLD_QUALITY_CATEGORY_LIMIT')
        for row in rows:
            metrics.append(dict(scope=scope,**row.asDict(),metric_date=as_of[:10],
                attribute_applicability_status='OWNER_POLICY_PARTIAL' if row['applicability_unprovided_products'] else 'OWNER_POLICY_COMPLETE'))
    summary=dict(contract_version=1,stage='GOLD_QUALITY_DAILY',as_of=as_of,
        coverage='FULL_FIXED_SILVER_RANGE' if source_drained else 'PARTIAL_FIXED_SILVER_RANGE',
        gold_versions=versions,metrics=sorted(metrics,key=lambda r:(r['scope'],r['category_group'])))
    summary_ref=checkpoint.archive(summary)
    manifest=dict(contract_version=1,stage='GOLD_EXPORT',as_of=as_of,source_drained=source_drained,
        selection_pages=pages,product_count=expected,gold_versions=versions,quality_summary_ref=summary_ref,
        input_refs=input_refs,mysql_published=False,publication_status='BLOCKED_DDL_GUARD_REQUIRED')
    manifest['content_hash']=digest(manifest)
    return dict(export_manifest_ref=checkpoint.archive(manifest),quality_summary_ref=summary_ref,product_count=expected)
