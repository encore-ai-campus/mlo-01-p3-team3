"""Per-product Silver selection and Gold, run inside Executor partitions after one product shuffle.

Native Gold (`spark_gold.build_gold`) needed a shuffle per join/window, about 50 Spark jobs per
batch even for three products. Every rule here is per product, so one grouped pass computes the
same payload: values are projected through the same schema `from_json` used, arrays are ordered
like `sort_array`, and "latest" keeps the native tie rules (equal-time conflicts are excluded).
"""
import json
from itertools import groupby

from ..domain.contracts import canonical, digest, Invalid
from ..domain.transform import related_policy, VERSION

SCALARS = ('display_name','manufacturer','brand','model','width_mm','depth_mm','height_mm')
IMAGE_STRINGS = ('url observed_url role origin selector resolution_status sha256 format extension acquisition_state '
    'source_url image_slot source_version validator_status asset_id bucket object_key raw_document_id raw_payload_hash raw_collected_at').split()
IMAGE_LONGS = ('order','bytes','width','height')
RAW_FIELDS = ('_id','observation_key','document_hash','collected_at','source_code','source_product_id')


def _text(value):
    # from_json into a string field keeps any JSON token as its text; the source is canonical JSON.
    if value is None or isinstance(value, str):
        return value
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'))


def _long(value):
    return value if isinstance(value, int) and not isinstance(value, bool) else None


def _strings(value):
    return [_text(v) for v in value] if isinstance(value, list) else None


def schema():
    """Field layout of the normalized observation, as `spark_gold.normalized_schema` declares it."""
    from product_pipeline.silver.schemas import columns, ARRAYS, LONGS
    from product_pipeline.silver.normalize import TABLES
    tables = {table:[(k,_strings if k in ARRAYS else _long if k in LONGS else _text) for k in columns(table)] for table in TABLES}
    image = [(k,_text) for k in IMAGE_STRINGS] + [(k,_long) for k in IMAGE_LONGS] + [('available',lambda v:v if isinstance(v,bool) else None)]
    return tables, image


def _struct(value, fields):
    if not isinstance(value, dict):
        return None
    return {name:convert(value.get(name)) for name,convert in fields}


def project(normalized, layout):
    tables, image = layout
    raw = _struct(normalized.get('raw_ref'), [(k,_text) for k in RAW_FIELDS])
    images = normalized.get('direct_images')
    images = [_struct(v, image) for v in images] if isinstance(images, list) else None
    found = normalized.get('tables') if isinstance(normalized.get('tables'), dict) else None
    projected = None
    if found is not None:
        projected = {}
        for table, fields in tables.items():
            rows = found.get(table)
            projected[table] = [_struct(v, fields) for v in rows] if isinstance(rows, list) else None
    return dict(raw_ref=raw, direct_images=images, tables=projected)


def order(value):
    """Spark ascending order: null first, structs field by field, arrays element by element."""
    if value is None:
        return (0,)
    if isinstance(value, dict):
        return (1, tuple(order(v) for v in value.values()))
    if isinstance(value, list):
        return (1, tuple(order(v) for v in value))
    return (1, value)


def _distinct(values):
    return len({canonical(v) for v in values})


def latest(rows, fields, at='observed_at'):
    """Newest time whose candidates agree on `fields`; evidence is the smallest (raw id, fields)."""
    groups = {}
    for row in rows:
        if row.get(at) is None or any(row.get(f) is None for f in fields):
            continue
        groups.setdefault(row[at], []).append(row)
    chosen = [(when, min(([r.get('raw_document_id')] + [r[f] for f in fields] for r in group), key=order))
              for when, group in groups.items() if _distinct([[r[f] for f in fields] for r in group]) == 1]
    if not chosen:
        return None
    when, value = max(chosen, key=lambda v:v[0])
    return dict(_at=when, _raw=value[0], **{f:value[i+1] for i,f in enumerate(fields)})


def facts(parsed, table, product_key):
    rows = []
    for n in parsed:
        for value in ((n['tables'] or {}).get(table) or []):
            if value is not None:
                rows.append(dict(product_key=product_key, **{k:v for k,v in value.items() if k != 'product_key'}))
    return rows


def _flat_sorted(parsed, table):
    values = [v for n in parsed for v in ((n['tables'] or {}).get(table) or [])]
    return sorted(values, key=order)


def gold(product_key, normalized, policy_hash, gold_policy, retracted, layout):
    parsed = [project(json.loads(v), layout) for v in normalized]
    refs = [(n['raw_ref']['observation_key'] if n['raw_ref'] else None, n['raw_ref']) for n in parsed]
    raw = [n['raw_ref'] for n in parsed if n['raw_ref']]
    def least(name):
        values = [r[name] for r in raw if r[name] is not None]
        return min(values) if values else None
    output = dict(product_key=product_key, first_collected_at=least('collected_at'), source_code=least('source_code'),
        source_product_id=least('source_product_id'), price_observations=_flat_sorted(parsed,'price_observations'),
        links=_flat_sorted(parsed,'link_observations'), attributes=_flat_sorted(parsed,'product_attributes'),
        source_refs=[v for _,v in sorted(refs, key=lambda kv:order({'key':kv[0],'value':kv[1]}))])
    observations = facts(parsed,'product_observations',product_key)
    lineage = {}
    for name in SCALARS:
        candidates = [dict(observed_at=r['observed_at'],raw_document_id=r.get('raw_document_id'),value=_text(r.get(name)))
                      for r in observations if r.get(name) is not None and r.get('observed_at') is not None]
        selected = latest(candidates,['value'])
        output[name] = selected['value'] if selected else None
        lineage[name] = dict(observed_at=selected['_at'],raw_document_id=selected['_raw']) if selected else None
    category = latest([r for r in observations if r.get('mapping_status') == 'MAPPED'],['service_category_id','service_category_code'])
    output.update(category_id=category['service_category_id'] if category else None,
        category_code=category['service_category_code'] if category else None)
    links = facts(parsed,'link_observations',product_key)
    comparison = latest([r for r in links if r.get('link_kind') == 'COMPARISON' and r.get('structure_status') == 'VALID'],['source_url'])
    output['comparison_url'] = comparison['source_url'] if comparison else None
    lineage['comparison_url'] = dict(observed_at=comparison['_at'],raw_document_id=comparison['_raw']) if comparison else None
    output['images'] = _images(parsed)
    output['offers'] = _offers(parsed, product_key)
    name = output['display_name']
    ready = name is not None and name != '' and output['category_id'] is not None
    if gold_policy.get('require_image'):
        ready = ready and bool(output['images'])
    if gold_policy.get('require_comparison_link'):
        ready = ready and output['comparison_url'] is not None
    issues = [code for code, failed in (('NAME_MISSING_OR_CONFLICT',name is None or name == ''),
        ('CATEGORY_UNMAPPED_OR_CONFLICT',output['category_id'] is None),('IMAGE_UNAVAILABLE',not output['images'])) if failed]
    output.update(retracted_raw_document_ids=sorted(retracted or []),normalization_policy_hash=policy_hash,withdrawn=False,
        lineage=lineage,quality_reason_codes=issues,catalog_ready=ready,recommendation_ready=False,purchase_link_ready=False,
        comparison_link_ready=output['comparison_url'] is not None,policy_version=VERSION,
        outcome='OUTPUT_COMMITTED' if ready else 'QUALITY_WAIT')
    return output


def _images(parsed):
    slots = {}
    for n in parsed:
        for image in (n['direct_images'] or []):
            if image is not None and image['image_slot'] is not None:
                slots.setdefault(image['image_slot'], []).append(image)
    usable = []
    for values in slots.values():
        newest = max((v['raw_collected_at'] for v in values if v['raw_collected_at'] is not None), default=None)
        recent = [v for v in values if newest is not None and v['raw_collected_at'] == newest]
        if not recent or _distinct([[v['sha256'],v['bucket'],v['object_key'],v['available']] for v in recent]) != 1:
            continue
        # Native ordered by raw_document_id only; the full row breaks a remaining tie deterministically.
        image = min(recent, key=lambda v:(order(v['raw_document_id']),order(v)))
        if image['available'] and all(image[k] is not None and str(image[k]) not in ('','0')
                                      for k in ('sha256','bucket','object_key','bytes','width','height','format')):
            usable.append(image)
    return [v for _,v in sorted(((image['order'],image) for image in usable), key=lambda kv:order({'order':kv[0],'value':kv[1]}))]


def _offers(parsed, product_key):
    identities = {}
    for row in facts(parsed,'normalized_offers',product_key):
        if row.get('offer_key') is not None:
            identities.setdefault(row['offer_key'], []).append(row)
    prices = facts(parsed,'price_observations',product_key)
    offers = []
    for key, rows in identities.items():
        # observed_at DESC NULLS LAST, raw_document_id; the full row breaks a remaining tie.
        identity = min(rows, key=lambda r:(r.get('observed_at') is None, _desc(r.get('observed_at')), order(r.get('raw_document_id')), order(r)))
        candidates = [p for p in prices if p.get('offer_key') == key and p.get('numeric_status') == 'CONFIRMED' and p.get('currency') == 'KRW']
        selected = latest(candidates,['amount','currency','condition_json'])
        if selected is None:
            continue
        # Native joined the selection back to every price row of that offer, raw id and time.
        matched = [p for p in prices if p.get('offer_key') == key and p.get('raw_document_id') == selected['_raw']
                   and selected['_raw'] is not None and p.get('observed_at') == selected['_at']]
        if not matched:
            continue
        offers.append(dict(identity=identity, price=min(matched, key=order), purchase_ready=False))
    return sorted(offers, key=order)


class _desc:
    __slots__ = ('value',)
    def __init__(self, value): self.value = value
    def __lt__(self, other): return self.value is not None and other.value is not None and self.value > other.value
    def __eq__(self, other): return self.value == other.value


def withdrawal_payload(product_key, retracted):
    source, identifier = product_key.split(':',1)
    return dict(product_key=product_key,source_code=source,source_product_id=identifier,withdrawn=True,
        retracted_raw_document_ids=sorted(retracted),catalog_ready=False,recommendation_ready=False,
        outcome='SOURCE_RETRACTED',policy_version=VERSION)


def dumps(value):
    return json.dumps(value, ensure_ascii=False, separators=(',', ':'), allow_nan=False)


def sha(text):
    import hashlib
    return hashlib.sha256(text.encode()).hexdigest()


INDEX, CACHED, RAW = 0, 1, 2


def product_pass(rows, context):
    """Executor function over rows sorted by (product_key, kind, row_key).

    Rows are the product's Bronze index entries (kind 0), its cached normalized rows (kind 1) and
    the admitted raw of index entries (kind 2, keyed by the index row key) that may need parsing.
    Yields (kind, row_key, product_key, payload_json) for new normalized rows, holds, policies
    (a count marker only) and Gold. Rules match the earlier Silver/Gold DataFrame plan. A product
    whose needed raw was not supplied yields only `missing` rows (index row key, origin job) and
    is computed again by the caller with that raw.
    """
    from .batch_local import parse_observation
    layout = schema()
    key, limits = context['job_id'], context['limits']
    for product, group in groupby(rows, key=lambda r:r['product_key']):
        index, cached, raw = {}, {}, {}
        for row in group:
            if row['kind'] == INDEX:
                value = json.loads(row['payload_json'])
                # Same event text is one observation (the plan's dropDuplicates on product and event).
                index.setdefault(canonical(value['event']), dict(value,_row_key=row['row_key']))
            elif row['kind'] == CACHED:
                cached[row['row_key']] = row['payload_json']
            else:
                raw[row['row_key']] = row['payload_json']
        retracted = set(context['retractions'].get(product, ()))
        kept = [v for v in index.values() if v['event']['raw_document_id'] not in retracted]
        if index and not kept:
            if retracted:
                # Every retained observation of this product was explicitly retracted.
                yield ('gold', sha(key+':'+product), product, dumps(withdrawal_payload(product, retracted)))
                yield ('withdrawn', sha(key+':'+product), product, '')
            continue
        if not kept:
            continue
        size = sum(len(canonical(v['dependencies'])) for v in kept if v.get('dependencies') is not None)
        if len(kept) > limits['history_observations'] or size > limits['history_bytes']:
            # An oversized history holds that product only; the batch and other products go on.
            yield ('hold', sha(key+':history:'+product), product,
                   dumps(dict(reason='PRODUCT_HISTORY_LIMIT',observations=len(kept),bytes=size)))
            continue
        policy = related_policy(context['policy'], [v['dependencies'] for v in kept])
        normal = {k:policy[k] for k in ('mappings','rules')}
        policy_hash = digest(dict(version=VERSION,**{k:policy.get(k,[]) for k in ('mappings','rules')}))
        desired = {}
        for value in kept:
            desired.setdefault(digest(dict(event=value['event']['event_id'],policy=normal,code=VERSION)), value)
        selected = {k:cached[k] for k in desired if k in cached}
        # Older index rows may still carry the raw inline (copied from the v2 namespace).
        needed = {k:v for k,v in desired.items() if k not in cached}
        missing = [v for v in needed.values() if v.get('item') is None and v['_row_key'] not in raw]
        if missing:
            for value in missing:
                yield ('missing', value['_row_key'], product, value.get('origin_job') or '')
            continue
        yield ('policy', sha(product), product, '')
        for identity, value in needed.items():
            event = value['event']
            item = value.get('item') or json.loads(raw[value['_row_key']])
            if digest(item) != event['payload_hash']:
                raise Invalid('IMMUTABLE_SOURCE_UNAVAILABLE')
            result = parse_observation(item, policy)
            body = canonical(dict(quality=result['quality'],reason=result['reason'],
                normalized_json=canonical(result['normalized']) if result['normalized'] is not None else None,
                raw_document_id=event['raw_document_id'],event_id=event['event_id'],
                image_revision=item['image_state']['image_state_revision']))
            yield ('normalized', identity, event['product_key'], body)
            selected[identity] = body
        latest_by_raw = {}
        for row_key in sorted(selected):
            payload = selected[row_key]
            value = json.loads(payload)
            if value.get('quality') == 'QUALITY_HOLD':
                yield ('hold', row_key, product, payload)
                continue
            if value.get('quality') != 'VALID':
                continue
            revision = value.get('image_revision')
            revision = int(revision) if revision is not None else None
            # image_revision DESC NULLS LAST, then row_key: the first row per raw document wins.
            rank = (revision is None, -(revision or 0), row_key)
            raw = value.get('raw_document_id')
            if raw not in latest_by_raw or rank < latest_by_raw[raw][0]:
                latest_by_raw[raw] = (rank, row_key, payload, value)
        chosen = list(latest_by_raw.values())
        size = sum(len(v[2]) for v in chosen)
        if chosen and size > limits['history_bytes']:
            yield ('hold', sha(key+':history:'+product), product,
                   dumps(dict(reason='PRODUCT_HISTORY_LIMIT',observations=len(chosen),bytes=size)))
            continue
        for _, row_key, payload, _ in chosen:
            yield ('silver', row_key, product, '')
        if chosen:
            output = gold(product, [v[3]['normalized_json'] for v in chosen], policy_hash, context['policy'].get('gold',{}),
                sorted(retracted) if retracted else None, layout)
            yield ('gold', sha(key+':'+product), product, dumps(output))
