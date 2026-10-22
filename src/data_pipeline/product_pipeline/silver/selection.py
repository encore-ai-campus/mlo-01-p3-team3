"""Technical selection of this window's completed Silver receipts, not approval."""
import re
from ..contracts import digest
from ..errors import PipelineError
from ..windows import validate_window


def seal(window,choices):
    validate_window(window)
    if not isinstance(choices,list) or len(choices)!=len(window['observation_keys']):raise PipelineError('SILVER_SELECTION_INCOMPLETE')
    try:
        if sorted(c['observation_key'] for c in choices)!=window['observation_keys']:raise ValueError()
        if sorted({c['product_key'] for c in choices})!=window['product_keys']:raise ValueError()
        if len({c['raw_document_id'] for c in choices})!=len(choices):raise ValueError()
        for c in choices:
            if not re.fullmatch('[a-f0-9]{24}',c['raw_document_id']) or not re.fullmatch('silver-eval-v1:[a-f0-9]{64}',c['evaluation_key']):raise ValueError()
            if c['evidence_ref']!='silver-receipt://'+c['evaluation_key']:raise ValueError()
    except (KeyError,TypeError,ValueError):raise PipelineError('SILVER_SELECTION_INVALID') from None
    body=dict(contract_version=1,window_hash=window['content_hash'],choices=sorted(choices,key=lambda c:c['observation_key']))
    return dict(body,content_hash=digest(body))


def validate(value,window):
    expected=seal(window,value['choices'])
    if value!=expected:raise PipelineError('SILVER_SELECTION_HASH_CONFLICT')
    return value['choices']


def recover(log_text,raw_rows,window):
    """Recover legacy completion evidence; never choose by time or hash order."""
    from ..files import parse
    records=[parse(line) for line in log_text.splitlines() if line.startswith('{') and line.endswith('}')]
    completed=[r for r in records if r.get('status')=='COMPLETED' and r.get('processed')==len(window['observation_keys'])]
    if not completed:raise PipelineError('RECOVERY_SILVER_COMPLETION_REQUIRED')
    latest={r['raw_document_id']:r for r in records if r.get('stage')=='silver_observation'}
    raw={str(r['_id']):r for r in raw_rows}
    if set(raw)!=set(latest):raise PipelineError('RECOVERY_RAW_SET_CHANGED')
    choices=[]
    for key,r in latest.items():
        doc=raw[key]
        choices.append(dict(raw_document_id=key,product_key=doc['source_code']+':'+doc['source_product_id'],
            observation_key=doc['observation_key'],evaluation_key=r['evaluation_key'],evidence_ref='silver-receipt://'+r['evaluation_key']))
    return seal(window,choices)
