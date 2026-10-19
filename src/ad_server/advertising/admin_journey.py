"""Navigation and visible form defaults only. Approval/activation remain explicit."""
import uuid
from datetime import timedelta
from urllib.parse import urlencode
from django.contrib import messages
from django.http import HttpResponseRedirect
from django.urls import reverse
from django.utils import timezone


def admin_url(model,action='add',pk=None,**query):
    url=reverse('admin:advertising_'+model+'_'+action,args=[pk] if pk is not None else None)
    return url+('?' + urlencode(query) if query else '')


def guided(request):return request.GET.get('guided')=='1' or request.POST.get('_guided')=='1'

def can_view(user,model):return user.has_perm('advertising.view_'+model) or user.has_perm('advertising.change_'+model)


def _object(model,value):
    try:key=uuid.UUID(str(value))
    except (ValueError,TypeError):return None
    return model.objects.filter(pk=key).first()


def initial_values(model,request,initial):
    if not guided(request):return initial
    from .models import Advertiser,MockContract,Campaign
    data=dict(initial);now=timezone.localtime();name=model.__name__
    if name=='MockContract':
        data.setdefault('starts_at',now-timedelta(minutes=5));data.setdefault('ends_at',now+timedelta(days=7))
        advertiser=_object(Advertiser,request.GET.get('advertiser'))
        if advertiser:data.setdefault('reference',(advertiser.name+' 모의 계약')[:120])
    if name=='Campaign':
        contract=_object(MockContract,request.GET.get('contract'))
        if contract:
            data['advertiser']=str(contract.advertiser_id)
            data.setdefault('name',(contract.advertiser.name+' 캠페인')[:120])
    if name=='Revision':
        campaign=_object(Campaign,request.GET.get('campaign'))
        if campaign:
            data.setdefault('title',campaign.name[:120]);data.setdefault('alt_text',campaign.name+' 시연 배너')
            data.setdefault('kind','BRAND');data.setdefault('template_id','desk');data.setdefault('bid_points',900)
            data.setdefault('placements',['EDITOR_RIGHT','RESULT_RIGHT']);data.setdefault('is_general',True)
            data.setdefault('destination','/test-destination/campaign-'+campaign.pk.hex[:12]+'/')
            data.setdefault('starts_at',max(now-timedelta(minutes=5),campaign.contract.starts_at))
            data.setdefault('ends_at',min(now+timedelta(days=7),campaign.contract.ends_at))
    return data


def journey_context(model,request,obj):
    name=model.__name__
    if name not in ('Advertiser','MockContract','Campaign','Revision'):return {}
    stage={'Advertiser':1,'MockContract':2,'Campaign':3,'Revision':4}[name]
    label={'Advertiser':'저장하고 계약 등록','MockContract':'저장하고 캠페인 만들기',
           'Campaign':'저장하고 광고 버전 작성','Revision':'제출하고 심사 화면으로'}[name]
    review_url=None
    if name=='Campaign' and obj and obj.active_revision_id and not obj.pending_change:
        stage=6;label='집행 설정 저장하고 배너 확인'
    if name=='Campaign' and obj and obj.pending_change and obj.revisions.exists():label='저장하고 제출 버전 확인'
    if name=='Revision' and obj:
        stage=5;label=None
        if obj.status=='PENDING' and request.user.has_perms(['advertising.change_revision','advertising.change_campaign']):
            review_url=reverse('admin:advertising_revision_review_single',args=[obj.pk])
    target_permission={'Advertiser':'advertising.add_mockcontract','MockContract':'advertising.add_campaign',
                       'Campaign':'advertising.add_revision','Revision':'advertising.view_revision'}[name]
    if name=='Campaign' and obj and obj.active_revision_id:target_permission='advertising.change_campaign'
    target_allowed=request.user.has_perm(target_permission)
    if target_permission.startswith('advertising.view_'):target_allowed=can_view(request.user,target_permission.removeprefix('advertising.view_'))
    if label and not target_allowed:label=None
    labels=['광고주','계약','캠페인','광고 작성','승인','집행 · 확인']
    titles={1:'1. 광고주 등록',2:'2. 모의 계약 확인',3:'3. 캠페인 연결',4:'4. 광고 내용 작성',5:'5. 제출한 광고 심사',6:'6. 집행 켜고 배너 확인'}
    descriptions={1:'업체 이름을 입력하고 상태를 ‘사용 중’으로 저장하세요.',
                  2:'연결한 광고주와 계약 기간을 확인하세요. 계약 상태를 ‘승인’으로 선택해야 다음 단계로 진행합니다.',
                  3:'광고주와 계약을 연결합니다. 처음에는 ‘광고 집행 켜기’를 해제한 채 저장하세요.',
                  4:'배너·포인트·기간·표시 위치를 확인하고 제출하세요. 이후 심사 화면으로 이어집니다.',
                  5:'제출 내용은 읽기 전용입니다. 아래에서 사유를 입력해 승인·반려하세요. 변경은 새 버전으로 제출합니다.',
                  6:'승인된 버전을 확인한 뒤 ‘광고 집행 켜기’를 체크하세요. 저장하면 비딩 시연으로 이동합니다.'}
    if stage==5 and obj and obj.status=='APPROVED':
        titles[5]='광고 승인 완료';descriptions[5]='아래 캠페인 집행 설정에서 집행을 켜세요. 승인만으로 배너가 표시되지는 않습니다.'
    elif stage==5 and obj and obj.status=='REJECTED':
        titles[5]='광고 반려';descriptions[5]='내용을 확인하고 아래 바로가기에서 새 버전을 작성하세요. 기존 제출 내용은 수정하지 않습니다.'
    elif stage==5 and not review_url:
        descriptions[5]='제출 내용을 확인하는 화면입니다. 심사 권한이 있는 운영자가 승인·반려할 수 있습니다.'
    return {'ad_journey':True,'ad_guided':guided(request),
            'ad_guide_title':titles[stage],'ad_guide_description':descriptions[stage],
            'ad_steps':[{'number':index+1,'label':text,'current':index+1==stage} for index,text in enumerate(labels)],
            'ad_next_label':label,'ad_review_url':review_url,
            'ad_campaign_url':admin_url('campaign','change',obj.campaign_id,guided=1) if name=='Revision' and obj and request.user.has_perm('advertising.change_campaign') else None,
            'ad_new_revision_url':admin_url('revision',campaign=obj.campaign_id,guided=1) if name=='Revision' and obj and request.user.has_perm('advertising.add_revision') else None}


def next_response(model_admin,request,obj):
    if '_next_step' not in request.POST:return None
    name=type(obj).__name__;user=request.user
    def warning(message):
        model_admin.message_user(request,message,messages.WARNING)
        if model_admin.has_change_permission(request,obj):return HttpResponseRedirect(admin_url(obj._meta.model_name,'change',obj.pk,guided=1))
        return None
    if name=='Advertiser' and user.has_perm('advertising.add_mockcontract'):
        if obj.status!='ACTIVE':return warning('등록은 저장했습니다. 다음 단계로 가려면 광고주 상태를 ‘사용 중’으로 설정하세요.')
        return HttpResponseRedirect(admin_url('mockcontract',advertiser=obj.pk,guided=1))
    if name=='MockContract' and user.has_perm('advertising.add_campaign'):
        if obj.status!='APPROVED':return warning('계약은 저장했습니다. 상태를 ‘승인’으로 확인한 뒤 다음 단계로 진행하세요.')
        if obj.advertiser.status!='ACTIVE':return warning('연결한 광고주가 ‘사용 중’이 아닙니다. 광고주 상태를 먼저 확인하세요.')
        return HttpResponseRedirect(admin_url('campaign',advertiser=obj.advertiser_id,contract=obj.pk,guided=1))
    if name=='Campaign':
        if obj.enabled and user.has_perm('advertising.view_decision'):
            return HttpResponseRedirect('/sandbox/?'+urlencode({'campaign':obj.pk}))
        if obj.pending_change:
            latest=obj.revisions.order_by('-version').first()
            if latest and can_view(user,'revision'):
                return HttpResponseRedirect(admin_url('revision','change',latest.pk,guided=1))
        if obj.active_revision_id and not obj.pending_change:
            return warning('저장은 완료했습니다. 배너 선정에 참여하려면 ‘광고 집행 켜기’를 체크하고 저장하세요. 승인만으로 집행되지 않습니다.')
        if user.has_perm('advertising.add_revision'):
            return HttpResponseRedirect(admin_url('revision',campaign=obj.pk,guided=1))
    if name=='Revision' and can_view(user,'revision'):
        return HttpResponseRedirect(admin_url('revision','change',obj.pk,guided=1))
    return None
