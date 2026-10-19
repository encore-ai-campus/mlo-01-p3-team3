"""Optional server-side adapter. Does not import or alter the recommendation application."""
import json
import copy
import uuid
import math
import re
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit,urlencode

def normalize_placement(value):
    """UI slot aliases; mobile/My slots are intentionally not enabled."""
    slots={'editor-right':'EDITOR_RIGHT','result-right':'RESULT_RIGHT',
           'EDITOR_RIGHT':'EDITOR_RIGHT','RESULT_RIGHT':'RESULT_RIGHT'}
    if not isinstance(value,str) or value not in slots:
        raise ValueError('Unsupported placement')
    return slots[value]

def _origin(value):
    if not isinstance(value,str) or any(ord(c)<=32 or ord(c)==127 for c in value) or '\\' in value:
        raise ValueError('Invalid ad-server origin')
    u=urlsplit(value)
    if u.scheme not in ('http','https') or not u.hostname or u.username or u.password or u.query or u.fragment or u.path not in ('','/'):
        raise ValueError('Ad-server URL must be an origin without a path')
    u.port  # Reject malformed ports at configuration time.
    return value.rstrip('/')

def recommendation_context(state,*,request_id,visitor_id,context_id,placement,confirmed_signals):
    """Caller owns signal confirmation and UUID lifetime; no object inference or LLM here."""
    from .policy import SOURCES,PLACEMENTS
    for value in (request_id,context_id):uuid.UUID(str(value))
    placement=normalize_placement(placement)
    if not isinstance(visitor_id,str) or not 16<=len(visitor_id)<=128 or not visitor_id.isascii() or not all(c.isalnum() or c in '-_' for c in visitor_id):
        raise ValueError('Pseudonymous visitor key required')
    if not isinstance(confirmed_signals,list) or len(confirmed_signals)>30:raise ValueError('Invalid signals')
    for s in confirmed_signals:
        if not isinstance(s,dict) or set(s)!={'key','source'} or s['source'] not in SOURCES:
            raise ValueError('Confirmed summary signals required')
        if not isinstance(s['key'],str) or not 1<=len(s['key'])<=64 or not s['key'].isascii() or not all(c.isalnum() or c in '-_' for c in s['key']):
            raise ValueError('Summary key required')
    items=state.get('recommendations',{}).get('items',[])
    ids=list(dict.fromkeys(str(x['product_id']) for x in items if x.get('product_id')))
    if len(ids)>100 or any(len(x)>180 for x in ids):raise ValueError('Too many or invalid product IDs')
    return {'request_id':str(request_id),'visitor_id':visitor_id,'context_id':str(context_id),
        'placement':placement,'signals':copy.deepcopy(confirmed_signals),'organic_product_ids':ids}

class AdServerClient:
    """Instantiate in the application backend only. Browser must never receive this token."""
    def __init__(self,base_url,token,*,timeout_seconds,public_base_url=None):
        self.base=_origin(base_url)
        self.public_base=_origin(public_base_url if public_base_url is not None else base_url)
        if isinstance(timeout_seconds,bool) or not isinstance(timeout_seconds,(int,float)) or not math.isfinite(timeout_seconds) or timeout_seconds<=0:
            raise ValueError('Finite positive timeout required')
        if not isinstance(token,str) or len(token)<32:raise ValueError('Service token required')
        self.token=token;self.timeout=timeout_seconds
    def request(self,path,payload=None):
        if not path.startswith('/internal/v1/') or '..' in path or '\\' in path:
            raise ValueError('Internal API path required')
        data=None if payload is None else json.dumps(payload,allow_nan=False).encode()
        req=Request(self.base+path,data=data,headers={'Authorization':'Bearer '+self.token,'Content-Type':'application/json'})
        try:
            with urlopen(req,timeout=self.timeout) as response:
                raw=response.read(131073)
                if len(raw)>131072:raise ValueError('Response too large')
                result=json.loads(raw)
                if not isinstance(result,dict):raise ValueError('Object response required')
                return result
        except HTTPError as e:
            # Input/auth faults are errors, not NO_FILL. Do not retry with new IDs.
            if e.code<500:return {'status':'ERROR','http_status':e.code}
            return {'status':'UNAVAILABLE','displayable':False}
        except (URLError,TimeoutError,OSError,ValueError):return {'status':'UNAVAILABLE','displayable':False}
    def decide(self,payload):return self.request('/internal/v1/decisions/',payload)
    def ui_display(self,payload):
        result=self.request('/internal/v1/ui/decisions/',payload)
        public=self.browser_payload(result)
        if result.get('media_id')=='sketchon' and result.get('slot_id') in ('editor-right','result-right'):
            public.update(media_id=result['media_id'],slot_id=result['slot_id'])
        return public

    def validate_ui_campaign(self,payload):
        """Operator backend only. Preview never imports or approves a UI campaign."""
        return self.request('/internal/v1/ui/campaigns/validate/',payload)
    def event(self,decision_id,payload):
        result=self.request(f'/internal/v1/decisions/{uuid.UUID(str(decision_id))}/events/',payload)
        return self.browser_event_payload(result)

    def browser_event_payload(self,result):
        """Use only on a trusted server response, never on browser-supplied navigation."""
        public={k:copy.deepcopy(result[k]) for k in
                ('event_id','status','reason','cost_applied','http_status','displayable') if k in result}
        nav=result.get('navigation')
        if nav is not None:
            safe={'allowed':False,'url':None,'reason':'LINK_UNAVAILABLE'}
            if isinstance(nav,dict) and nav.get('allowed') is True:
                value=nav.get('url')
                try:
                    if not isinstance(value,str) or any(ord(c)<=32 or ord(c)==127 for c in value) or '\\' in value:
                        raise ValueError('Invalid destination')
                    if value=='/test-ended/' or re.fullmatch(r'/test-destination/[A-Za-z0-9_-]+/',value):
                        target=self.public_base+value
                    else:
                        u=urlsplit(value)
                        if u.scheme!='https' or not u.hostname or u.username or u.password:
                            raise ValueError('Invalid destination')
                        u.port
                        target=value  # Ad server already applies its destination allowlist.
                    safe={'allowed':True,'url':target,'reason':nav.get('reason','')}
                except ValueError:pass
            public['navigation']=safe
        return public
    def lookup(self,decision_id,visitor_id):
        return self.request(f'/internal/v1/decisions/{uuid.UUID(str(decision_id))}/?'+urlencode({'visitor_id':visitor_id}))
    def browser_payload(self,result):
        """Only a display projection. Application backend separately relays owned events."""
        public={k:copy.deepcopy(result[k]) for k in ('decision_id','context_id','placement','status','displayable',
            'expires_at','creative','policy_version','test_mode','reason','cost_applied') if k in result}
        creative=public.get('creative')
        if creative:
            expected=f"/creative/{uuid.UUID(public['decision_id'])}/"
            path=creative.get('image_url','')
            if not isinstance(path,str) or not path.startswith(expected) or not re.fullmatch(r'[A-Za-z0-9_.:=\-]+/',path[len(expected):]):
                raise ValueError('Unexpected creative path')
            creative['image_url']=self.public_base+path
        return public

    def display(self,payload):
        """Application proxy convenience: never return the raw decision to the UI."""
        return self.browser_payload(self.decide(payload))
