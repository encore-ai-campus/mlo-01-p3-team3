"""Product-scoped evaluation identity; physical snapshots remain provenance."""
from ..contracts import digest

VERSION = 'product-gold-2.0.0'
MODE = 'PRODUCT_INPUTS_V2'


def policy_inputs(policy):
    # The ledger-wide hash is an export diagnostic, not a product dependency.
    return {k: v for k, v in policy.items() if k != 'review_set_hash'}


def binding(receipt, input_refs):
    """Seal receipt reuse against the inputs actually checked by the Gold runner."""
    return dict(receipt_hash=receipt['record_hash'],
                input_fingerprint=receipt['input_fingerprint'],
                validated_input_refs_hash=digest(input_refs or {}))


def next_due(groups, owner, policy, as_of):
    from datetime import datetime,timedelta
    from ..contracts import utc
    stamps=[]
    def visit(value):
        if isinstance(value,dict):
            for k,v in value.items():
                if k in {'valid_from','checked_at','expires_at','observed_at'} and v:
                    if utc(v)>utc(as_of):stamps.append(utc(v))
                elif k.endswith('_json') and isinstance(v,str):
                    from ..files import parse
                    visit(parse(v))
                elif isinstance(v,(dict,list)):visit(v)
        elif isinstance(value,list):
            for v in value:visit(v)
    visit(owner)
    for group in groups:
        visit(group['tables'])
        for row in group['tables']['price_observations']:
            if row.get('observed_at'):
                end=datetime.fromisoformat(utc(row['observed_at']).replace('Z','+00:00'))+timedelta(seconds=policy['price_max_age_seconds'],milliseconds=1)
                end=utc(end.isoformat())
                if end>utc(as_of):stamps.append(end)
    return min(stamps,default=None)
