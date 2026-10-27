"""Recover a Danawa seller item from its preserved bridge URL, never click tokens."""
import re
from urllib.parse import parse_qs, urlsplit

from ..contracts import digest

VERSION = 'danawa-bridge-item-v1'


def bridge_identity(doc, seller):
    if doc.get('source_code') != 'danawa':
        return None
    try:
        url = urlsplit(seller.get('url') or '')
        if (url.scheme != 'https' or url.hostname != 'prod.danawa.com'
                or url.path != '/bridge/loadingBridge.html' or url.username or url.password
                or url.port not in (None, 443)):
            return None
        query = parse_qs(url.query, keep_blank_values=True)
        if any(len(query.get(k, [])) != 1 for k in ('pcode', 'cmpnyc', 'link_pcode')):
            return None
        product, merchant, item = (query[k][0] for k in ('pcode', 'cmpnyc', 'link_pcode'))
        if (product != doc['source_product_id'] or merchant != seller.get('seller_id')
                or not re.fullmatch(r'[A-Za-z0-9_-]{1,191}', merchant)
                or not re.fullmatch(r'[A-Za-z0-9_.-]{1,191}', item)
                or item in {'0', '-1'}):
            return None
        if seller.get('source_offer_id') not in (None, '', item):
            return None
        option = seller.get('option_key')
        if option is not None and (not isinstance(option, str) or not option or len(option) > 191):
            return None
        # UNKNOWN is a scoped missing-value marker, not evidence of no options.
        option = option or 'UNKNOWN'
        return dict(offer_key='offer-v1:' + digest(['danawa', product, merchant, item, option]),
                    source_seller_id=merchant, source_offer_id=item, option_key=option,
                    option_status='IDENTIFIED' if option != 'UNKNOWN' else 'UNKNOWN',
                    rule=dict(version=VERSION, evidence_ref='repo://product_pipeline/silver/offer_identity.py'),
                    source_identity=dict(pcode=product, cmpnyc=merchant, link_pcode=item),
                    source_path='seller.url', excluded_query_keys=sorted(set(query) - {'pcode', 'cmpnyc', 'link_pcode'}))
    except (TypeError, ValueError, KeyError):
        return None
