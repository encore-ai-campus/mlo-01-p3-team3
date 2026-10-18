import hashlib,json,logging
from contextvars import ContextVar
from functools import wraps
from uuid import UUID
from django.db.models import F
from django.db.models.signals import pre_save
from django.dispatch import receiver
from django.utils import timezone
from django.core.exceptions import RequestDataTooBig
from .models import GenerationAttempt,RawReceipt

current_request=ContextVar('operations_request',default=None)
log=logging.getLogger(__name__)


class ActorContextMiddleware:
    def __init__(self,get_response):self.get_response=get_response
    def __call__(self,request):
        token=current_request.set(request)
        try:return self.get_response(request)
        finally:current_request.reset(token)


@receiver(pre_save,sender='sketch.SketchEvent')
def stamp_event(sender,instance,**kwargs):
    request=current_request.get()
    if not instance._state.adding or request is None:return
    instance.actor_kind='member' if request.user.is_authenticated else 'guest'
    instance.actor_account_id=request.user.pk if request.user.is_authenticated else None
    instance.visitor_id=request.session.get('sketch_owner','')
    instance.flow_id=instance.project.flow_id
    reported=instance.payload.get('client_actor_kind')
    instance.reported_actor_kind=reported if reported in ('member','guest') else ('unknown' if 'client_time' in instance.payload else instance.actor_kind)


def track_generation(view):
    @wraps(view)
    def wrapped(request,project_id):
        from sketch.models import Project,Composition
        from sketch.access import project_scope
        row=None
        try:
            data=json.loads(request.body);identifier=UUID(data.get('request_id'))
            project=Project.objects.filter(project_scope(request),pk=project_id).first()
            if project:
                signature=hashlib.sha256(json.dumps({k:data.get(k) for k in ('document','revision','text_requirements','style_preferences')},sort_keys=True).encode()).hexdigest()
                row,_=GenerationAttempt.objects.get_or_create(request_id=identifier,defaults={'project':project,'input_hash':signature,
                    'actor_kind':'member' if request.user.is_authenticated else 'guest','actor_account':request.user if request.user.is_authenticated else None})
        except (ValueError,TypeError,AttributeError,RequestDataTooBig):pass
        try:
            response=view(request,project_id)
        except Exception:
            if row:finish_attempt(row,'FAILED','internal_error',metadata={'project':project,'input_hash':signature})
            raise
        if row:
            result=Composition.objects.filter(pk=row.request_id,project_id=project.id).first()
            metadata={'project':project,'input_hash':signature,'actor_kind':'member' if request.user.is_authenticated else 'guest','actor_account':request.user if request.user.is_authenticated else None}
            if result:finish_attempt(row,'SUCCEEDED','',result,metadata)
            else:finish_attempt(row,{429:'LIMITED',409:'CONFLICT'}.get(response.status_code,'FAILED'),'http_'+str(response.status_code),metadata=metadata)
        return response
    return wrapped


def finish_attempt(row,status,error_code,result=None,metadata=None):
    now=timezone.now()
    values={'status':status,'error_code':error_code,'finished_at':now,'duration_ms':max(0,int((now-row.started_at).total_seconds()*1000)),'result':result}
    values.update(metadata or {})
    # A successful concurrent retry is authoritative and cannot be overwritten
    # by an older failure or conflict returning later.
    GenerationAttempt.objects.filter(pk=row.pk).exclude(status='SUCCEEDED').update(**values)


def record_raw_receipt(project,sample,body,status,warnings,has_file):
    try:sample_id=UUID(str(sample.get('sample_id')))
    except (ValueError,TypeError,AttributeError):sample_id=None
    strokes=sample.get('strokes',[]) if isinstance(sample,dict) else []
    strokes=strokes if isinstance(strokes,list) else []
    defaults={'byte_count':len(body),'stroke_count':len(strokes),
        'point_count':sum(len(s.get('points',[])) for s in strokes if isinstance(s,dict) and isinstance(s.get('points',[]),list)),
        'capture_status':str(sample.get('capture_status','complete'))[:20] if isinstance(sample,dict) else '',
        'warnings':[str(w)[:300] for w in warnings[:20]],'has_file':has_file}
    row,created=RawReceipt.objects.get_or_create(project=project,sample_id=sample_id,checksum=hashlib.sha256(body).hexdigest(),validation_status=status,defaults=defaults)
    if not created:RawReceipt.objects.filter(pk=row.pk).update(retry_count=F('retry_count')+1,last_received_at=timezone.now())
    return row


def track_raw(view):
    @wraps(view)
    def wrapped(request,project_id):
        response=view(request,project_id)
        from sketch.models import Project
        from sketch.access import project_scope
        project=Project.objects.filter(project_scope(request),pk=project_id).first()
        if project and response.status_code in (200,201,409,422,503):
            try:
                body=request.body if request.content_type=='application/json' else request.POST.get('sample','').encode()
                sample=json.loads(body)
                result=json.loads(response.content)
                status={409:'CONFLICT',422:'INVALID',503:'STORAGE_ERROR'}.get(response.status_code,'REVIEW' if result.get('warnings') else 'PASS')
                if response.status_code in (200,201):
                    # Storage uses canonical JSON, so receipt hashes the persisted bytes.
                    from pathlib import Path
                    from django.conf import settings
                    body=(Path(settings.SKETCH_RAW_ROOT)/(str(UUID(sample['sample_id']))+'.json')).read_bytes()
                record_raw_receipt(project,sample,body,status,result.get('warnings',[]) or ([result['error']] if result.get('error') else []),response.status_code in (200,201))
            except Exception:log.exception('Raw receipt metadata recording failed; immutable Raw retained')
        return response
    return wrapped
