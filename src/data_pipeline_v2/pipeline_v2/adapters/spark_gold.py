"""Gold selection with DataFrame joins, windows and aggregations on Executors."""
from ..domain.transform import VERSION


def normalized_schema():
    from pyspark.sql import types as T
    from pipeline_v2.legacy.silver.schemas import columns, ARRAYS, LONGS
    from pipeline_v2.legacy.silver.normalize import TABLES
    tables = T.StructType([T.StructField(table,T.ArrayType(T.StructType([
        T.StructField(k,T.ArrayType(T.StringType()) if k in ARRAYS else
            T.LongType() if k in LONGS else T.StringType(),True) for k in columns(table)])))
        for table in TABLES])
    image_strings = ('url observed_url role origin selector resolution_status sha256 format extension acquisition_state '
        'source_url image_slot source_version validator_status asset_id bucket object_key raw_document_id raw_payload_hash raw_collected_at').split()
    image = T.StructType([T.StructField(k,T.StringType()) for k in image_strings] +
        [T.StructField(k,T.LongType()) for k in ('order','bytes','width','height')] +
        [T.StructField('available',T.BooleanType())])
    raw = T.StructType([T.StructField(k,T.StringType()) for k in
        ('_id','observation_key','document_hash','collected_at','source_code','source_product_id')])
    return T.StructType([T.StructField('raw_ref',raw),T.StructField('direct_images',T.ArrayType(image)),T.StructField('tables',tables)])


def build_gold(history, policies, gold_policy, retracted=None):
    from pyspark.sql import functions as F, Window
    parsed = history.select('product_key',F.from_json('normalized_json',normalized_schema()).alias('n')).persist()
    refs = parsed.groupBy('product_key').agg(
        F.sort_array(F.collect_list(F.struct(F.col('n.raw_ref.observation_key').alias('key'),F.col('n.raw_ref').alias('value')))).alias('_refs'),
        F.min('n.raw_ref.collected_at').alias('first_collected_at'),
        F.min('n.raw_ref.source_code').alias('source_code'),F.min('n.raw_ref.source_product_id').alias('source_product_id'),
        *[F.sort_array(F.flatten(F.collect_list('n.tables.'+table))).alias(name) for table,name in
          [('price_observations','price_observations'),('link_observations','links'),('product_attributes','attributes')]])
    base = refs.withColumn('source_refs',F.transform('_refs',lambda x:x['value'])).drop('_refs')

    def facts(table):
        nested = normalized_schema()['tables'].dataType[table].dataType.elementType
        return parsed.select('product_key',F.explode('n.tables.'+table).alias('v')).select(
            'product_key',*[F.col('v.'+field.name).alias(field.name) for field in nested if field.name != 'product_key'])

    # Equal-time conflicts are excluded; the previous unambiguous observation remains eligible.
    def latest(frame, fields, by=('product_key',), at='observed_at'):
        valid = frame.filter(F.col(at).isNotNull())
        for field in fields:
            valid = valid.filter(F.col(field).isNotNull())
        chosen = (valid.groupBy(*by,at)
            .agg(F.countDistinct(F.struct(*fields)).alias('_variants'),
                 F.min(F.struct(F.col('raw_document_id'),*[F.col(k) for k in fields])).alias('_value'))
            .filter(F.col('_variants') == 1))
        window = Window.partitionBy(*by).orderBy(F.col(at).desc())
        return (chosen.withColumn('_rank',F.row_number().over(window)).filter('_rank = 1')
            .select(*by,F.col(at).alias('_at'),F.col('_value.raw_document_id').alias('_raw'),
                    *[F.col('_value.'+k).alias(k) for k in fields]))

    observations = facts('product_observations')
    scalar_names = ('display_name','manufacturer','brand','model','width_mm','depth_mm','height_mm')
    long_values = observations.select('product_key','observed_at','raw_document_id',F.explode(F.array(*[
        F.struct(F.lit(name).alias('field'),F.col(name).cast('string').alias('value')) for name in scalar_names])).alias('field_value'))
    long_values = long_values.select('product_key','observed_at','raw_document_id','field_value.*').filter(
        F.col('value').isNotNull() & F.col('observed_at').isNotNull())
    candidates = long_values.groupBy('product_key','field','observed_at').agg(
        F.countDistinct('value').alias('_variants'),F.min(F.struct('raw_document_id','value')).alias('_value')).filter('_variants = 1')
    selected = candidates.withColumn('_rank',F.row_number().over(
        Window.partitionBy('product_key','field').orderBy(F.col('observed_at').desc()))).filter('_rank = 1')
    scalar_map = selected.groupBy('product_key').agg(F.map_from_entries(F.collect_list(F.struct('field',
        F.struct(F.col('_value.value').alias('value'),'observed_at',F.col('_value.raw_document_id').alias('raw_document_id'))))).alias('_scalars'))
    base = base.join(scalar_map,'product_key','left')
    lineage = []
    for name in scalar_names:
        selection = F.col('_scalars')[name]
        base = base.withColumn(name,selection['value'])
        lineage.append(F.when(selection.isNotNull(),F.struct(selection['observed_at'].alias('observed_at'),
            selection['raw_document_id'].alias('raw_document_id'))).alias(name))
    category = latest(observations.filter("mapping_status = 'MAPPED'"),['service_category_id','service_category_code'])
    base = base.join(category.select('product_key',F.col('service_category_id').alias('category_id'),
        F.col('service_category_code').alias('category_code')),'product_key','left')

    links = facts('link_observations')
    comparison = latest(links.filter("link_kind = 'COMPARISON' AND structure_status = 'VALID'"),['source_url'])
    base = base.join(comparison.select('product_key',F.col('source_url').alias('comparison_url'),
        F.struct(F.col('_at').alias('observed_at'),F.col('_raw').alias('raw_document_id')).alias('_lineage_link')),'product_key','left')
    lineage.append(F.col('_lineage_link').alias('comparison_url'))

    def add_array(frame, name, value):
        nonlocal base
        grouped = frame.groupBy('product_key').agg(F.sort_array(F.collect_list(value)).alias(name))
        dtype = grouped.schema[name].dataType
        base = base.join(grouped,'product_key','left').withColumn(name,F.coalesce(F.col(name),F.array().cast(dtype)))

    images = parsed.select('product_key',F.explode('n.direct_images').alias('image')).select('product_key','image.*')
    image_window = Window.partitionBy('product_key','image_slot')
    recent = images.withColumn('_max_at',F.max('raw_collected_at').over(image_window)).filter('raw_collected_at = _max_at')
    # A never-acquired equal-time ref of a stored image's source URL is not a conflicting variant
    # (spark_product._without_unacquired_twins).
    blank = lambda name:F.col(name).isNull() | (F.col(name) == '')
    acquired = recent.filter(F.coalesce(F.col('available'),F.lit(False)) & ~blank('sha256') & ~blank('object_key')
        & F.col('source_url').isNotNull()).select('product_key','image_slot','source_url').distinct().withColumn('_acquired',F.lit(True))
    recent = (recent.join(acquired,['product_key','image_slot','source_url'],'left')
        .filter(~(~F.coalesce(F.col('available'),F.lit(False)) & blank('sha256') & blank('object_key')
                  & F.coalesce(F.col('_acquired'),F.lit(False)))).drop('_acquired'))
    variants = recent.groupBy('product_key','image_slot').agg(
        F.countDistinct(F.struct('sha256','bucket','object_key','available')).alias('_variants'))
    recent = recent.join(variants,['product_key','image_slot']).filter('_variants = 1')
    recent = recent.withColumn('_rank',F.row_number().over(image_window.orderBy('raw_document_id'))).filter('_rank = 1')
    usable = recent.filter(F.col('available'))
    for name in ('sha256','bucket','object_key','bytes','width','height','format'):
        usable = usable.filter(F.col(name).isNotNull() & (F.col(name).cast('string') != '') & (F.col(name).cast('string') != '0'))
    image_columns = [f.name for f in normalized_schema()['direct_images'].dataType.elementType]
    image_values = usable.select('product_key',F.struct(*[F.col(k) for k in image_columns]).alias('image'))
    # Order is explicitly by source order; array contents retain every image evidence field.
    grouped = image_values.groupBy('product_key').agg(
        F.sort_array(F.collect_list(F.struct(F.col('image.order').alias('order'),F.col('image').alias('value')))).alias('_images'))
    image_type = normalized_schema()['direct_images'].dataType
    base = base.join(grouped,'product_key','left').withColumn('images',
        F.coalesce(F.transform('_images',lambda x:x['value']),F.array().cast(image_type))).drop('_images')

    prices = facts('price_observations')

    identities = facts('normalized_offers')
    identity_window = Window.partitionBy('product_key','offer_key').orderBy(F.col('observed_at').desc_nulls_last(),'raw_document_id')
    identities = identities.withColumn('_rank',F.row_number().over(identity_window)).filter('_rank = 1').drop('_rank')
    identity = identities.select('product_key','offer_key',
        F.struct(*[F.col(k) for k in identities.columns]).alias('identity'))
    selected_prices = latest(prices.filter("numeric_status = 'CONFIRMED' AND currency = 'KRW' AND offer_key IS NOT NULL"),
        ['amount','currency','condition_json'],by=('product_key','offer_key'))
    price = prices.alias('p').join(selected_prices.alias('s'),
        (F.col('p.product_key') == F.col('s.product_key')) & (F.col('p.offer_key') == F.col('s.offer_key')) &
        (F.col('p.raw_document_id') == F.col('s._raw')) & (F.col('p.observed_at') == F.col('s._at')), 'inner')
    price = price.select(F.col('p.product_key').alias('product_key'),F.col('p.offer_key').alias('offer_key'),
        F.struct(*[F.col('p.'+k).alias(k) for k in prices.columns]).alias('price')).dropDuplicates(['product_key','offer_key'])
    offers = identity.join(price,['product_key','offer_key'])
    add_array(offers,'offers',F.struct('identity','price',F.lit(False).alias('purchase_ready')))

    issues = F.filter(F.array(
        F.when(F.col('display_name').isNull() | (F.col('display_name') == ''),F.lit('NAME_MISSING_OR_CONFLICT')),
        F.when(F.col('category_id').isNull(),F.lit('CATEGORY_UNMAPPED_OR_CONFLICT')),
        F.when(F.size('images') == 0,F.lit('IMAGE_UNAVAILABLE'))),lambda x:x.isNotNull())
    ready = F.col('display_name').isNotNull() & (F.col('display_name') != '') & F.col('category_id').isNotNull()
    if gold_policy.get('require_image'):
        ready = ready & (F.size('images') > 0)
    if gold_policy.get('require_comparison_link'):
        ready = ready & F.col('comparison_url').isNotNull()
    # Explicit retractions travel with the product so publication can drop that evidence.
    if retracted is None:
        base = base.withColumn('retracted_raw_document_ids',F.array().cast('array<string>'))
    else:
        base = base.join(retracted.select('product_key','retracted_raw_document_ids'),'product_key','left').withColumn(
            'retracted_raw_document_ids',F.coalesce('retracted_raw_document_ids',F.array().cast('array<string>')))
    base = (base.join(policies.select('product_key','normalization_policy_hash'),'product_key')
        .withColumn('withdrawn',F.lit(False))
        .withColumn('lineage',F.struct(*lineage)).withColumn('quality_reason_codes',issues)
        .withColumn('catalog_ready',ready).withColumn('recommendation_ready',F.lit(False))
        .withColumn('purchase_link_ready',F.lit(False)).withColumn('comparison_link_ready',F.col('comparison_url').isNotNull())
        .withColumn('policy_version',F.lit(VERSION))
        .withColumn('outcome',F.when(ready,F.lit('OUTPUT_COMMITTED')).otherwise(F.lit('QUALITY_WAIT'))))
    names = [n for n in base.columns if not n.startswith('_')]
    output = base.select('product_key',F.to_json(F.struct(*[F.col(k) for k in names]),
        options={'ignoreNullFields':'false'}).alias('payload_json'))
    output._pipeline_cached_frames = [parsed]
    return output
