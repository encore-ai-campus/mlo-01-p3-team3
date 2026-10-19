import hmac
import json
import logging
from functools import wraps
from urllib.parse import urlsplit
from django.conf import settings
from django.core.exceptions import ValidationError
from django.core.signing import BadSignature
from django.db import DatabaseError
from django.http import HttpResponse, JsonResponse, Http404
from django.shortcuts import render
from django.views.decorators.clickjacking import xframe_options_exempt
from django.views.decorators.csrf import csrf_exempt, ensure_csrf_cookie
from django.views.decorators.http import require_GET, require_http_methods
from .assets import banner, manifest
from .creative import verify_creative
from .engine import decide, public_decision, own_decision, record_event, event_response, report, saved_policy
from .errors import AdError
from .models import Decision
from .policy import TestPolicy, TARGETS

log = logging.getLogger(__name__)

def guarded(fn):
    @wraps(fn)
    def run(*args, **kwargs):
        try:
            response=fn(*args, **kwargs)
        except AdError as e:
            response=JsonResponse({'status':'UNAVAILABLE' if e.status==503 else 'ERROR',
                'error':{'code':e.code,'message':e.message},'cost_applied':False},status=e.status)
        except (Decision.DoesNotExist, Http404):
            response=JsonResponse({'error':{'code':'NOT_FOUND','message':'찾을 수 없습니다.'}},status=404)
        except (ValueError, TypeError, ValidationError):
            response=JsonResponse({'error':{'code':'INVALID_REQUEST','message':'요청 형식을 확인하세요.'}},status=422)
        except DatabaseError:
            response=JsonResponse({'status':'UNAVAILABLE','error':{'code':'STORAGE_UNAVAILABLE',
                'message':'광고 저장소를 사용할 수 없습니다.'}},status=503)
        response['Cache-Control']='no-store'
        return response
    return run

def service_auth(request):
    TestPolicy.load()
    expected='Bearer '+settings.AD_SERVICE_TOKEN
    if not hmac.compare_digest(request.headers.get('Authorization','').encode(),expected.encode()):
        raise AdError('UNAUTHORIZED','서버 인증이 필요합니다.',401)

def operator_auth(request):
    TestPolicy.load()
    if not request.user.is_authenticated or not request.user.is_staff or not request.user.has_perms(
        ['advertising.view_decision','advertising.change_campaign']):
        raise AdError('OPERATOR_REQUIRED','권한이 있는 운영자로 로그인하세요.',403)

def body(request):
    if request.content_type!='application/json':raise AdError('JSON_REQUIRED','JSON 요청이 필요합니다.',415)
    if len(request.body)>32768:raise AdError('BODY_TOO_LARGE','요청 크기를 줄이세요.',413)
    def invalid_constant(value):raise ValueError(value)
    try:return json.loads(request.body,parse_constant=invalid_constant)
    except (ValueError,UnicodeDecodeError):raise AdError('INVALID_JSON','유효한 JSON이 필요합니다.',400)

@require_GET
def home(request):
    return render(request,'advertising/home.html',{'templates':manifest()})

@require_GET
def templates(request):
    return JsonResponse({'mode':'TEMPLATES_ONLY','templates':manifest(),'test_mode':True})

@guarded
@require_GET
def health(request):
    rows=manifest();ready=sum(r['status']=='READY' for r in rows)
    # Phase 3 intentionally needs no database or phase-4 settings.
    return JsonResponse({'mode':'TEMPLATES_ONLY','ready':ready,'expected':10,
        'operations_requested':settings.AD_TEST_OPERATIONS},status=200 if ready==10 else 503)

@require_http_methods(['GET','HEAD'])
def asset(request,template_id):
    try:a=banner(template_id)
    except AdError as e:return JsonResponse({'error':{'code':e.code}},status=e.status)
    etag='"'+a['sha256']+'"'
    response=HttpResponse(status=304) if request.headers.get('If-None-Match')==etag else HttpResponse(
        b'' if request.method=='HEAD' else a['data'],content_type='image/png')
    response['ETag']=etag
    response['Cache-Control']='no-cache'
    response['X-Content-Type-Options']='nosniff'
    return response

@guarded
@require_http_methods(['GET','HEAD'])
def decision_asset(request,decision_id,token):
    try:
        if not verify_creative(decision_id,token):raise Http404()
    except (BadSignature,ValueError):raise Http404()
    d=Decision.objects.get(pk=decision_id)
    if not public_decision(d,saved_policy(d))['displayable']:
        raise AdError('CREATIVE_NOT_SERVING','현재 표시할 수 없는 광고입니다.',410)
    a=banner(d.snapshot['template_id'])
    return HttpResponse(b'' if request.method=='HEAD' else a['data'],content_type='image/png')

def ancestors():
    allowed=[]
    for origin in settings.AD_FRAME_ANCESTORS.split():
        if origin=="'self'":allowed.append(origin);continue
        u=urlsplit(origin)
        if (u.scheme in ('http','https') and u.hostname and not u.username and not u.password
            and not u.path and not u.query and not u.fragment):allowed.append(origin)
    return ' '.join(allowed) or "'none'"

@xframe_options_exempt
@guarded
@require_GET
def embed(request,template_id):
    a=banner(template_id)
    response=render(request,'advertising/embed.html',{'asset':a})
    response['Content-Security-Policy']=f"default-src 'self'; img-src 'self'; style-src 'unsafe-inline'; frame-ancestors {ancestors()}; base-uri 'none'"
    return response

@csrf_exempt
@guarded
@require_http_methods(['POST'])
def decisions_api(request):
    service_auth(request)
    d=decide(body(request))
    return JsonResponse(public_decision(d,saved_policy(d)))

@guarded
@require_GET
def decision_api(request,decision_id):
    service_auth(request)
    d=own_decision(decision_id,request.GET.get('visitor_id'))
    return JsonResponse(public_decision(d,saved_policy(d)))

@csrf_exempt
@guarded
@require_http_methods(['POST'])
def events_api(request,decision_id):
    service_auth(request)
    d,e=record_event(decision_id,body(request))
    return JsonResponse(event_response(d,e))

@guarded
@require_GET
def reports_api(request):
    service_auth(request)
    return JsonResponse(report())

@guarded
@require_GET
def ui_contract_api(request):
    service_auth(request)
    from .ui_contract import ui_contract
    return JsonResponse(ui_contract())

@csrf_exempt
@guarded
@require_http_methods(['POST'])
def ui_decisions_api(request):
    service_auth(request)
    from .ui_contract import decision_request,display_response
    d=decide(decision_request(body(request)))
    return JsonResponse(display_response(public_decision(d,saved_policy(d))))

@csrf_exempt
@guarded
@require_http_methods(['POST'])
def ui_campaign_validation_api(request):
    service_auth(request)
    from .ui_contract import campaign_preview
    from .policy import destination
    from .engine import validate_asset
    from types import SimpleNamespace
    result=campaign_preview(body(request))
    draft=result['revision_draft']
    destination(draft['destination'],TestPolicy.load())
    validate_asset(SimpleNamespace(template_id=draft['template_id'],asset_sha256=''),TestPolicy.load())
    return JsonResponse(result)

@ensure_csrf_cookie
@guarded
@require_GET
def sandbox(request):
    operator_auth(request)
    from .sandbox_ui import SIGNALS
    from .models import Campaign
    from uuid import UUID
    focus=None
    try:focus=Campaign.objects.filter(pk=UUID(request.GET.get('campaign',''))).first()
    except ValueError:pass
    return render(request,'advertising/sandbox.html',{'signals':[(key,SIGNALS.get(key,key)) for key in sorted(TARGETS)],'focus_campaign':focus})

@guarded
@require_http_methods(['POST'])
def sandbox_decision(request):
    operator_auth(request)
    d=decide(body(request));data=public_decision(d,saved_policy(d))
    data['operator_trace']={'candidates':d.snapshot.get('candidate_bids'),
        'representatives':d.snapshot.get('representative_campaign_ids'),'excluded':d.diagnostics}
    from .sandbox_ui import decision_summary
    data['summary']=decision_summary(d,data['displayable'])
    return JsonResponse(data)

@guarded
@require_http_methods(['POST'])
def sandbox_event(request,decision_id):
    operator_auth(request)
    d,e=record_event(decision_id,body(request))
    from .sandbox_ui import event_summary
    data=event_response(d,e);data['summary']=event_summary(d,e)
    return JsonResponse(data)

@guarded
@require_GET
def sandbox_report(request):
    operator_auth(request)
    from .sandbox_ui import report_summary
    data=report();data['summary']=report_summary(data)
    return JsonResponse(data)

@require_GET
def test_destination(request,slug=None):
    return render(request,'advertising/destination.html',{'slug':slug})
