"""Read back only the service rows referenced by one immutable publication."""
from datetime import datetime
from decimal import Decimal
import json

from ..domain.contracts import digest, Invalid


def normalized(value):
    if isinstance(value,datetime):return value.isoformat(timespec='milliseconds')
    if isinstance(value,Decimal):return format(value.normalize(),'f')
    if isinstance(value,dict):
        return {k:normalized(json.loads(v) if k.endswith('_json') and isinstance(v,str) else v) for k,v in value.items()}
    if isinstance(value,(list,tuple)):return [normalized(v) for v in value]
    return value


def service_rows(db, group, key):
    result=[]
    def collect(table, identity):
        value=db.get(table,identity,locking=False)
        if value is None:raise Invalid('SQL_SERVICE_ROW_MISSING')
        result.append(dict(table=table,identity=identity,hash=digest(normalized(value))))
        return value
    product=collect('products',dict(product_key=group['product_key']))
    collect('pipeline_product_versions',dict(product_key=group['product_key']))
    for image in group['images']:
        collect('assets',dict(bucket=image['bucket'],object_key=image['object_key']))
        collect('product_images',dict(product_id=product['id'],source_code=group['source_code'],
            source_product_id=group['source_product_id'],image_variant=image['image_slot'],
            raw_document_id=image['raw_document_id'],source_version=image['source_version']))
    offers={o['identity']['offer_key'] for o in group['offers']}
    for offer in sorted(offers):collect('offers',dict(offer_key=offer))
    for price in group['price_observations']:
        if price.get('offer_key') not in offers or price['currency']!='KRW' or price['numeric_status']!='CONFIRMED':continue
        snapshot='direct-price:'+digest(dict(offer=price['offer_key'],raw=price['raw_document_id'],condition=price['condition_fingerprint'],amount=price['amount']))
        collect('offer_snapshots',dict(snapshot_key=snapshot))
    for link in group['links']:
        collect('product_link_observations',dict(product_id=product['id'],link_observation_key=link['link_observation_key'],gold_evaluation_key=key))
    return result


def verify_service(db, records):
    for proof in records:
        actual=db.get(proof['table'],proof['identity'],locking=False)
        if actual is None or digest(normalized(actual))!=proof['hash']:raise Invalid('SQL_SERVICE_READBACK_CHANGED')
