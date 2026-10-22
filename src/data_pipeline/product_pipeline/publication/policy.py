"""Pure publication projections, lineage retention and value validation."""
from decimal import Decimal
from uuid import UUID
from ..contracts import canonical, digest, utc
from ..errors import Conflict, PipelineError
from ..files import parse
from .values import sql_time


def uuid_hex(value):
    try:return UUID(value).hex
    except (ValueError,TypeError) as exc:raise PipelineError('PUBLICATION_UUID_INVALID') from exc



def money(value):
    number=Decimal(str(value))
    if not number.is_finite() or number<=0 or number!=number.to_integral_value() or number>9223372036854775807:
        raise PipelineError('PUBLICATION_KRW_INTEGER_REQUIRED')
    return int(number)



def guarded_projection(old, incoming, lineage, versions):
    """Per-field event time, not arrival time. NULL never erases a verified value."""
    values={};proofs=dict(versions)
    for name,value in incoming.items():
        if value is None:continue
        proof=lineage.get(name) or {};stamp=proof.get('observed_at')
        previous=proofs.get(name)
        if previous and not stamp and old and old.get(name)==value:continue
        if previous and stamp:
            if utc(stamp)<previous['observed_at']:continue
            if utc(stamp)==previous['observed_at'] and digest(value)!=previous['value_hash']:
                raise Conflict('PUBLICATION_EQUAL_TIME_CONFLICT')
        if old and old.get(name) is not None and old[name]!=value and not stamp:
            raise PipelineError('PUBLICATION_FIELD_TIME_UNAVAILABLE')
        # Decimal DB values and canonical source decimal strings represent the same number.
        values[name]=value
        if stamp:proofs[name]=dict(observed_at=utc(stamp),value_hash=digest(value))
    return values,proofs



def snapshot_ref(item, plan, source_rows):
    catalog = item['group']['tables']['product_catalog'][0]
    return canonical(dict(schema_version=2, plan_id=plan['plan_id'], product_key=item['product_key'],
        publication_as_of=plan['as_of'],publication_mode=plan.get('publication_mode','STANDARD'),
        export_hash=plan['export_hash'], gold_evaluation_key=catalog['gold_evaluation_key'],
        input_fingerprint=catalog['input_fingerprint'], gold_versions=plan['gold_versions'],
        source_rows=[dict(table=table,row_key=row['row_key'],record_hash=row['record_hash'])
                     for table,row in source_rows]))



def product_reference(item,plan,old,values):
    catalog=item['group']['tables']['product_catalog'][0]
    ref=parse(snapshot_ref(item,plan,[('product_catalog',catalog),('pipeline_evaluations',item['group']['receipt'])]))
    if not old:return canonical(ref)
    retained=False
    for service,source in [('name','display_name'),('model_name','model'),('manufacturer','manufacturer'),('brand','brand'),
                           ('width_mm','width_mm'),('depth_mm','depth_mm'),('height_mm','height_mm'),
                           ('price_krw','price_krw'),('comparison_url','comparison_url')]:
        actual=values.get(service,old.get(service));incoming=catalog.get(source)
        if actual is not None:
            different=incoming is None or (Decimal(str(actual))!=Decimal(str(incoming)) if service.endswith(('_mm','_krw')) else actual!=incoming)
            retained=retained or different
    retained=retained or any(parse(values[name])!=parse(catalog[name]) for name in ('attributes_json','field_lineage_json','source_refs_json'))
    retained=retained or values.get('first_collected_at')!=sql_time(catalog['first_collected_at'])
    if retained:
        previous=parse(old['gold_snapshot_ref_json'])
        refs=[{k:v for k,v in previous.items() if k!='retained_snapshot_refs'},*previous.get('retained_snapshot_refs',[])]
        unique={r['gold_evaluation_key']:r for r in refs if r['gold_evaluation_key']!=ref['gold_evaluation_key']}
        ref['retained_snapshot_refs']=list(unique.values())
        refs=parse(values['source_refs_json'])+parse(old['source_refs_json'])
        values['source_refs_json']=canonical(list({canonical(r):r for r in refs}.values()))
    return canonical(ref)



def attribute_projection(previous, incoming):
    """A failed/newer scrape must not erase a prior confirmed option attribute."""
    if not isinstance(previous,list) or not isinstance(incoming,list):
        raise PipelineError('PUBLICATION_ATTRIBUTE_ARRAY_REQUIRED')
    def key(row):
        return canonical([row.get(k) for k in ('attribute_name','measurement_target','component','option_key')])
    retained = [r for r in previous if r.get('attribute_status')=='CONFIRMED']
    result = list(incoming)
    for old in retained:
        identity=key(old)
        candidates=[r for r in incoming if key(r)==identity and r.get('attribute_status')=='CONFIRMED']
        if not candidates:
            if old not in result: result.append(old)
            continue
        old_time=old.get('observed_at')
        new_time=max((r.get('observed_at') or '' for r in candidates),default='')
        semantic=lambda r: canonical([r.get(k) for k in ('value_type','value_text','value_decimal','value_min','value_max','codes','unit')])
        if old_time and new_time and utc(new_time)==utc(old_time):
            # A product may carry several confirmed dimensions under an unresolved
            # axis. Compare complete value sets, not every old/new cross pair.
            prior={semantic(r) for r in retained if key(r)==identity and r.get('observed_at') and utc(r['observed_at'])==utc(old_time)}
            current={semantic(r) for r in candidates if r.get('observed_at') and utc(r['observed_at'])==utc(old_time)}
            if prior!=current:raise Conflict('PUBLICATION_ATTRIBUTE_EQUAL_TIME_CONFLICT')
        if not new_time and any(semantic(r)!=semantic(old) for r in candidates):
            raise PipelineError('PUBLICATION_ATTRIBUTE_TIME_UNAVAILABLE')
        if old_time and (not new_time or utc(new_time)<utc(old_time)):
            result=[r for r in result if key(r)!=identity or r.get('attribute_status')!='CONFIRMED']+[old]
    return result



def extension_values(old, catalog, values, *, newer):
    """Retained scalar values carry their original state and lineage, never new proof."""
    if old and not newer: return {}
    lineage=parse(catalog['field_lineage_json']);statuses=parse(catalog['field_status_json'])
    previous_lineage=parse(old['field_lineage_json']) if old else {}
    previous_status=parse(old['field_status_json']) if old else {}
    for service, source in [('name','display_name'),('model_name','model'),('manufacturer','manufacturer'),
                            ('brand','brand'),('width_mm','width_mm'),('depth_mm','depth_mm'),('height_mm','height_mm')]:
        if old and old.get(service) is not None and service not in values:
            lineage[source]=previous_lineage.get(source)
            statuses[source]=previous_status.get(source,'UNRESOLVED')
    incoming=parse(catalog['attributes_json'])
    attrs=attribute_projection(parse(old['attributes_json']) if old else [], incoming)
    refs=parse(catalog['source_refs_json'])
    retained_scalar=old and any(old.get(service) is not None and service not in values for service in
        ('name','model_name','manufacturer','brand','width_mm','depth_mm','height_mm'))
    if old and (retained_scalar or attrs!=incoming or (old.get('price_krw') is not None and catalog['price_krw'] is None)):
        refs=list({canonical(r):r for r in [*parse(old['source_refs_json']),*refs]}.values())
    return dict(attributes_json=canonical(attrs),field_status_json=canonical(statuses),
        field_lineage_json=canonical(lineage),source_refs_json=canonical(refs),
        catalog_ready=catalog['catalog_ready'],recommendation_ready=catalog['recommendation_ready'],
        comparison_link_ready=catalog['comparison_link_ready'],purchase_link_ready=catalog['purchase_link_ready'],
        quality_reason_codes=canonical(catalog['quality_reason_codes']),publication_status=catalog['publication_status'],
        publication_reason_codes=canonical(catalog['publication_reason_codes']),embedding_json=catalog['embedding_json'])
