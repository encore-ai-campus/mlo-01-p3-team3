import json
from datetime import datetime, time, timedelta
from pathlib import Path
from uuid import UUID

from django.conf import settings
from django.contrib.auth import get_user_model
from django.core.paginator import Paginator
from django.db import transaction
from django.db.models import Count, Q
from django.http import FileResponse, Http404, HttpResponse
from django.shortcuts import get_object_or_404, redirect, render
from django.utils import timezone
from django.utils.dateparse import parse_date
from django.views.decorators.http import require_GET, require_POST, require_http_methods

from sketch.models import Composition, GuestGenerationUsage, Project, SketchEvent
from .ad_gateway import integration_status
from .forms import CampaignForm
from .models import AdCampaign, GenerationAttempt, OperatorAudit, RawReceipt
from .permissions import allowed, audit, ops_required, MENU as NAV


def page(request, tab, title, **context):
    permissions={key:perm for key,_,_,perm in NAV}
    required=permissions.get(tab,{'project_detail':'view_projects','campaign_form':'manage_ads'}.get(tab))
    return render(request, 'operations/'+tab+'.html', dict(context, tab=tab, title=title,
        ops_permission=required,
        nav=[(key,label,url) for key,label,url,perm in NAV if allowed(request.user,perm)],
        sensitive=allowed(request.user,'view_sensitive'), can_review_raw=allowed(request.user,'review_raw'),
        can_review_ads=allowed(request.user,'review_ads'), integration=integration_status()))


def period(request):
    today=timezone.localdate()
    def date_value(key,fallback):
        try:return parse_date(request.GET.get(key,'')) or fallback
        except ValueError:return fallback
    start=date_value('from',today-timedelta(days=6))
    end=date_value('to',today)
    if end<start:start,end=end,start
    end=min(end,start+timedelta(days=min(365,(datetime.max.date()-start).days-1)))
    tz=timezone.get_current_timezone()
    return start,end,timezone.make_aware(datetime.combine(start,time.min),tz),timezone.make_aware(datetime.combine(end+timedelta(days=1),time.min),tz)


def filtered(request, rows, field, search_fields=()):
    start,end,lower,upper=period(request)
    rows=rows.filter(**{field+'__gte':lower,field+'__lt':upper})
    query=request.GET.get('q','').strip()[:100]
    if query and search_fields:
        condition=Q()
        for name in search_fields:condition|=Q(**{name+'__icontains':query})
        rows=rows.filter(condition)
    params=request.GET.copy();params.pop('page',None)
    return dict(rows=Paginator(rows,40).get_page(request.GET.get('page')),start=start,end=end,q=query,query_string=params.urlencode())


@require_GET
@ops_required('view_console')
def dashboard(request):
    start,end,lower,upper=period(request)
    events=SketchEvent.objects.filter(event_time__gte=lower,event_time__lt=upper)
    attempts=GenerationAttempt.objects.filter(started_at__gte=lower,started_at__lt=upper)
    counts=dict(attempts.values('status').annotate(n=Count('request_id')).values_list('status','n'))
    metrics=[('생성된 결과',Composition.objects.filter(created_at__gte=lower,created_at__lt=upper).count()),
        ('행동 이벤트',events.count()),('식별 가능한 방문 ID',events.exclude(visitor_id='').values('visitor_id').distinct().count()),
        ('검수 대기 Raw',RawReceipt.objects.filter(review_status='PENDING').count())]
    funnel=list(events.exclude(flow_id=None).values('event_type').annotate(flows=Count('flow_id',distinct=True)).order_by('-flows')[:15])
    return page(request,'dashboard','운영 개요',metrics=metrics,attempts=counts,funnel=funnel,start=start,end=end,
        legacy_events=events.filter(flow_id=None).count(),campaigns=AdCampaign.objects.count())


@require_GET
@ops_required('view_people')
def people(request):
    query=request.GET.get('q','').strip()[:100]
    users=get_user_model().objects.filter(Q(username__icontains=query)|Q(first_name__icontains=query)).order_by('-date_joined')
    visitors=Project.objects.exclude(owner='').values('owner').annotate(projects=Count('id'),members=Count('account',distinct=True)).order_by('-projects')
    if query:visitors=visitors.filter(owner__icontains=query)
    usage=GuestGenerationUsage.objects.filter(usage_date=timezone.localdate()).order_by('-successful_count')[:40]
    quotas=[{'ip':row.client_ip if allowed(request.user,'view_sensitive') else 'IP 비공개','count':row.successful_count} for row in usage]
    if quotas and allowed(request.user,'view_sensitive'):
        audit(request,'quota.ip_viewed',request.user,after={'date':str(timezone.localdate()),'rows':len(quotas)})
    return page(request,'people','이용자',users=Paginator(users,40).get_page(request.GET.get('page')),visitors=visitors[:40],quotas=quotas,q=query)


@require_GET
@ops_required('view_projects')
def projects(request):
    rows=Project.objects.select_related('account').annotate(results=Count('composition')).order_by('-updated_at')
    return page(request,'projects','작업 · 결과',**filtered(request,rows,'updated_at',('title','owner','account__username')))


@require_GET
@ops_required('view_projects')
def project_detail(request, project_id):
    project=get_object_or_404(Project.objects.select_related('account','saved_project'),pk=project_id)
    sensitive=allowed(request.user,'view_sensitive')
    if sensitive:audit(request,'project.detail_viewed',project)
    return page(request,'project_detail','작업 상세',project=project,results=project.composition_set.order_by('-created_at')[:30],
        events=project.sketchevent_set.order_by('event_time')[:300],attempts=project.generationattempt_set.order_by('-started_at')[:40],
        document=json.dumps(project.document,ensure_ascii=False,indent=2) if sensitive else '',
        context=json.dumps(project.request_context,ensure_ascii=False,indent=2) if sensitive else '')


@require_GET
@ops_required('view_behavior')
def events(request):
    rows=SketchEvent.objects.select_related('project','actor_account').order_by('-event_time')
    kind=request.GET.get('actor','')
    if kind in ('member','guest','unknown'):rows=rows.filter(actor_kind=kind)
    flow=request.GET.get('flow','')
    if flow:
        try:rows=rows.filter(flow_id=UUID(flow))
        except ValueError:rows=rows.none()
    result=filtered(request,rows,'event_time',('event_type','visitor_id','project__title'))
    # Payloads can contain user text. Only the protected detail view exposes them.
    return page(request,'events','행동 데이터',**result,actor=kind,flow=flow)


@require_GET
@ops_required('view_collection')
def collection(request):
    rows=RawReceipt.objects.select_related('project','reviewer').order_by('-last_received_at')
    status=request.GET.get('status','')
    if status in ('PASS','REVIEW','INVALID','CONFLICT','STORAGE_ERROR'):rows=rows.filter(validation_status=status)
    return page(request,'collection','스케치 수집',**filtered(request,rows,'last_received_at',('project__owner','checksum','capture_status')),status=status)


@require_POST
@ops_required('review_raw')
def index_collection(request):
    from .raw_index import index_raw
    summary=index_raw(limit=1000)
    audit(request,'raw.indexed',request.user,after=summary)
    return redirect('/ops/collection/?indexed='+str(summary['indexed']))


@require_POST
@ops_required('review_raw')
def review_raw(request,receipt_id):
    decision=request.POST.get('review_status');reason=request.POST.get('reason','').strip()
    if decision not in ('APPROVED','REJECTED','HOLD') or not 2<=len(reason)<=500:return HttpResponse('검수 상태와 2~500자 사유가 필요합니다.',status=400)
    with transaction.atomic():
        row=get_object_or_404(RawReceipt.objects.select_for_update(),pk=receipt_id)
        if decision=='APPROVED' and (not row.has_file or row.validation_status not in ('PASS','REVIEW')):return HttpResponse('원본 보존 및 자동 검증을 통과한 샘플만 승인할 수 있습니다.',status=400)
        previous=row.review_status
        row.review_status=decision;row.review_reason=reason;row.reviewer=request.user;row.reviewed_at=timezone.now()
        row.save(update_fields=['review_status','review_reason','reviewer','reviewed_at'])
        audit(request,'raw.reviewed',row,reason,{'review_status':previous},{'review_status':decision})
    return redirect('/ops/collection/')


def media_response(request, instance, field, action):
    if not field:raise Http404
    try:response=FileResponse(field.open('rb'))
    except OSError:raise Http404 from None
    audit(request,action,instance)
    response['X-Content-Type-Options']='nosniff'
    return response


@require_GET
@ops_required('view_sensitive')
def project_photo(request,project_id):
    row=get_object_or_404(Project,pk=project_id)
    return media_response(request,row,row.photo,'photo.viewed')


@require_GET
@ops_required('view_sensitive')
def result_image(request,result_id):
    row=get_object_or_404(Composition,pk=result_id)
    return media_response(request,row,row.image,'result.viewed')


@require_GET
@ops_required('view_sensitive')
def raw_file(request,receipt_id):
    import hashlib
    row=get_object_or_404(RawReceipt,pk=receipt_id,has_file=True)
    if not row.sample_id:raise Http404
    path=Path(settings.SKETCH_RAW_ROOT)/(str(row.sample_id)+'.json')
    try:body=path.read_bytes()
    except OSError:raise Http404 from None
    if hashlib.sha256(body).hexdigest()!=row.checksum:raise Http404('원본 체크섬이 다릅니다.')
    audit(request,'raw.downloaded',row)
    response=HttpResponse(body,content_type='application/json');response['Content-Disposition']=f'attachment; filename="{row.sample_id}.json"'
    response['X-Content-Type-Options']='nosniff'
    return response


@require_GET
@ops_required('manage_ads')
def ads(request):
    return page(request,'ads','광고 관리',**filtered(request,AdCampaign.objects.order_by('-updated_at'),'updated_at',('title','campaign_id','advertiser')))


def campaign_snapshot(row):
    return {key:str(getattr(row,key)) for key in ('campaign_id','title','description','advertiser','creative','landing_url','category','slot_id','bid_units','starts_at','ends_at','active','review_status','content_version','reviewed_version','revision')}


@require_http_methods(['GET','POST'])
@ops_required('manage_ads')
def campaign_edit(request,campaign_id=None):
    row=get_object_or_404(AdCampaign,pk=campaign_id) if campaign_id else None
    if request.method=='GET':
        form=CampaignForm(instance=row,initial={'revision':row.revision if row else 0})
    else:
        with transaction.atomic():
            if row:row=get_object_or_404(AdCampaign.objects.select_for_update(),pk=campaign_id)
            before=campaign_snapshot(row) if row else {}
            form=CampaignForm(request.POST,request.FILES,instance=row)
            if form.is_valid():
                if row and form.cleaned_data['revision']!=row.revision:return HttpResponse('다른 운영자가 변경했습니다. 새로고침 후 다시 확인해 주세요.',status=409)
                saved=form.save(commit=False)
                material=set(form.changed_data)-{'reason','revision','active','bid_units'}
                if row and material:
                    saved.content_version+=1;saved.review_status='PENDING';saved.reviewed_version=None
                    saved.reviewed_by=None;saved.reviewed_at=None;saved.review_reason=''
                saved.revision+=1
                saved.save()
                audit(request,'ad.updated' if row else 'ad.created',saved,form.cleaned_data['reason'],before,campaign_snapshot(saved))
                return redirect('/ops/ads/')
    return page(request,'campaign_form','광고 초안 수정' if row else '광고 초안 등록',form=form,campaign=row)


def mutation_input(request):
    reason=request.POST.get('reason','').strip()
    try:revision=int(request.POST.get('revision',''))
    except ValueError:return None
    return (revision,reason) if 2<=len(reason)<=500 else None


@require_POST
@ops_required('review_ads')
def campaign_review(request,campaign_id):
    value=mutation_input(request);decision=request.POST.get('decision')
    if not value or decision not in ('APPROVED','REJECTED','HOLD'):return HttpResponse('결정과 변경 사유가 필요합니다.',status=400)
    with transaction.atomic():
        row=get_object_or_404(AdCampaign.objects.select_for_update(),pk=campaign_id)
        if row.revision!=value[0]:return HttpResponse('심사할 내용이 변경되었습니다. 새로고침해 주세요.',status=409)
        if decision=='APPROVED' and (not row.creative or not row.landing_url.startswith('https://') or row.ends_at<=timezone.now()):return HttpResponse('배너·HTTPS 연결 주소·유효한 기간을 먼저 확인해 주세요.',status=400)
        before=campaign_snapshot(row)
        row.review_status=decision;row.review_reason=value[1];row.reviewed_by=request.user;row.reviewed_at=timezone.now();row.reviewed_version=row.content_version;row.revision+=1
        row.save();audit(request,'ad.reviewed',row,value[1],before,campaign_snapshot(row))
    return redirect('/ops/ads/')


@require_POST
@ops_required('manage_ads')
def campaign_toggle(request,campaign_id):
    value=mutation_input(request)
    if not value:return HttpResponse('현재 버전과 변경 사유가 필요합니다.',status=400)
    with transaction.atomic():
        row=get_object_or_404(AdCampaign.objects.select_for_update(),pk=campaign_id)
        if row.revision!=value[0]:return HttpResponse('내용이 변경되었습니다.',status=409)
        before=campaign_snapshot(row);row.active=not row.active;row.revision+=1
        row.save(update_fields=['active','revision','updated_at']);audit(request,'ad.toggled',row,value[1],before,campaign_snapshot(row))
    return redirect('/ops/ads/')


@require_GET
@ops_required('manage_ads')
def campaign_image(request,campaign_id):
    row=get_object_or_404(AdCampaign,pk=campaign_id)
    return media_response(request,row,row.creative,'ad.creative_viewed')


@require_GET
@ops_required('view_ad_reports')
def ad_reports(request):
    return page(request,'reports','광고 성과')


@require_GET
@ops_required('view_ops_audit')
def audit_log(request):
    rows=OperatorAudit.objects.select_related('actor').order_by('-created_at')
    return page(request,'audit','운영 감사 이력',**filtered(request,rows,'created_at',('action','actor__username','object_id','reason')))
