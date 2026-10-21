"""Owned, same-origin advertisement delivery. Does not import the ad-server DB."""
import hashlib
import json
import logging
from uuid import UUID, NAMESPACE_URL, uuid5

from django.conf import settings
from django.http import JsonResponse, Http404
from django.utils.crypto import salted_hmac
from django.views.decorators.http import require_POST
from sketch.access import owned_project, visitor_id
from .ad_client import AdServerClient

log = logging.getLogger(__name__)
LIMIT = 64


def response(value, status=200):
    return JsonResponse(value, status=status, headers={'Cache-Control':'private, no-store'})


def unavailable():
    return response({'status':'UNAVAILABLE','displayable':False})


def client():
    config = settings.SKETCHON_AD_DELIVERY
    if not config['enabled']:
        return None
    try:
        return AdServerClient(config['internal_origin'], config['token'],
            public_base_url=config['public_origin'], timeout_seconds=config['timeout'])
    except (ValueError, TypeError):
        return None


def body(request, allowed):
    if request.content_type != 'application/json' or len(request.body) > 8192:
        raise ValueError('Invalid body')
    value = json.loads(request.body)
    if not isinstance(value,dict) or set(value)-allowed:
        raise ValueError('Unexpected fields')
    return value


def identity(request):
    actor = 'user:'+str(request.user.pk) if request.user.is_authenticated else 'guest'
    return salted_hmac('sketchon-ad-visitor', actor+':'+visitor_id(request)).hexdigest()


def scope(request, value):
    slot = value.get('slot_id')
    if slot == 'editor-right' and value.get('project_id') and not value.get('run_id'):
        project = owned_project(request, UUID(str(value['project_id'])))
        return {'slot_id':slot,'project_id':str(project.pk)}, []
    if slot == 'result-right' and value.get('run_id') and not value.get('project_id'):
        from recommendations.views import owned_run
        from recommendations import services
        run_id = str(UUID(str(value['run_id'])))
        _, owner = owned_run(request, run_id)
        state = services.reader().state(run_id, owner)
        result = state.get('recommendations')
        if not isinstance(result,dict) or state.get('job',{}).get('status') in ('QUEUED','STAGING','RUNNING','RETRY_WAIT'):
            return {'slot_id':slot,'run_id':run_id}, None
        items = result.get('items',[])
        ids = list(dict.fromkeys(row['product_id'] for row in items
            if isinstance(row,dict) and isinstance(row.get('product_id'),str) and row['product_id']))
        if len(ids)>100 or any(len(x)>180 for x in ids):
            raise ValueError('Invalid product IDs')
        return {'slot_id':slot,'run_id':run_id}, ids
    raise ValueError('Unsupported scope')


@require_POST
def display(request):
    try:
        value = body(request, {'slot_id','project_id','run_id'})
        owned_scope, products = scope(request, value)
        if products is None:
            return response({'status':'WAITING','displayable':False})
        adapter = client()
        if adapter is None:
            return unavailable()
        owner = identity(request)
        canonical = json.dumps([owner,owned_scope,products],sort_keys=True,separators=(',',':'))
        context = str(uuid5(NAMESPACE_URL, salted_hmac('sketchon-ad-context',canonical).hexdigest()))
        payload = {'media_id':'sketchon','slot_id':owned_scope['slot_id'],
            'request_id':str(uuid5(UUID(context),'display-request')),'context_id':context,
            'visitor_id':owner,'signals':[],'organic_product_ids':products}
        result = adapter.ui_display(payload)
        if result.get('context_id') not in (None, context):
            return unavailable()
        if result.get('decision_id'):
            decision = str(UUID(result['decision_id']))
            bindings = dict(request.session.get('ad_delivery_bindings',{}))
            bindings.pop(decision,None)
            bindings[decision] = {'owner':owner,'scope':owned_scope,'context_id':context}
            request.session['ad_delivery_bindings'] = dict(list(bindings.items())[-LIMIT:])
        return response(result)
    except (Http404, FileNotFoundError):
        return response({'status':'NOT_FOUND','displayable':False},404)
    except PermissionError:
        return response({'status':'FORBIDDEN','displayable':False},403)
    except (ValueError, TypeError, KeyError, UnicodeError):
        return response({'status':'ERROR','displayable':False},422)
    except OSError:
        return unavailable()


@require_POST
def event(request, decision_id):
    try:
        value = body(request, {'event_id','kind','occurred_at','rendered','fraction','continuous_ms','active'})
        bindings = request.session.get('ad_delivery_bindings',{})
        binding = bindings.get(str(decision_id))
        if not binding or binding['owner'] != identity(request):
            return response({'status':'FORBIDDEN'},403)
        scope(request,binding['scope'])  # Recheck access at event time.
        value['event_id'] = str(UUID(str(value['event_id'])))
        adapter = client()
        if adapter is None:
            return unavailable()
        return response(adapter.event(decision_id,{**value,'visitor_id':binding['owner']}))
    except (Http404, FileNotFoundError):
        return response({'status':'NOT_FOUND'},404)
    except PermissionError:
        return response({'status':'FORBIDDEN'},403)
    except (ValueError, TypeError, KeyError, UnicodeError):
        return response({'status':'ERROR'},422)
    except OSError:
        return unavailable()
