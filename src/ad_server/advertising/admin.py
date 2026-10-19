from django.contrib import admin, messages
from django.core.exceptions import ValidationError
from django.forms.models import model_to_dict
from django import forms
from django.db import transaction
from django.http import HttpResponseBadRequest,HttpResponseForbidden,HttpResponseRedirect
from django.urls import path,reverse
from django.views.decorators.http import require_POST
from . import models as m
from .assets import banner
from .engine import review_revision, gate
from .errors import AdError
from .policy import TestPolicy
from .admin_ui import KoreanAdminMixin, display_method

class AuditedAdmin(KoreanAdminMixin,admin.ModelAdmin):
    def has_delete_permission(self,request,obj=None):return False
    @transaction.atomic
    def save_model(self,request,obj,form,change):
        policy=TestPolicy.load();gate()
        old=model_to_dict(type(obj).objects.get(pk=obj.pk)) if change else None
        obj.full_clean()
        super().save_model(request,obj,form,change)
        # UUID/datetime serialization is explicit; secrets and visitor fields aren't admin-editable.
        m.Audit.objects.create(operation='OPERATOR_CHANGE',object_id=str(obj.pk),actor=request.user.get_username(),
            detail={'model':obj._meta.label,'policy_version':policy.version,'before':{k:str(v) for k,v in (old or {}).items()},
                    'after':{k:str(v) for k,v in model_to_dict(obj).items()}})

@admin.register(m.Advertiser)
class AdvertiserAdmin(AuditedAdmin):
    list_display=('name','status','is_test','id')
    list_filter=('status',)
    readonly_fields=('is_test',)
    fields=('name','status','is_test')

@admin.register(m.MockContract)
class ContractAdmin(AuditedAdmin):
    list_display=('reference','advertiser','status','starts_at','ends_at')
    list_filter=('status',)
    def raw_readonly_fields(self,request,obj=None):return ('advertiser',) if obj else ()
    fieldsets=(('계약 정보',{'fields':('reference','advertiser','status')}),
               ('계약 유효 기간',{'fields':('starts_at','ends_at')}))

class StrictPointField(forms.IntegerField):
    def to_python(self,value):
        if (type(value) is bool or (isinstance(value,str) and (not value.isascii() or not value.isdigit()))
                or type(value) is float):
            raise ValidationError('1~10,000 정수 포인트를 입력하세요. 반올림하지 않습니다.')
        return super().to_python(value)

class RevisionForm(forms.ModelForm):
    bid_points=StrictPointField(label='가상 입찰 포인트',min_value=1,max_value=10000,
        help_text='1~10,000 정수 포인트. 실제 금액이나 결제가 아닙니다.')
    placements=forms.MultipleChoiceField(label='표시할 위치',choices=[('EDITOR_RIGHT','편집 화면 오른쪽'),('RESULT_RIGHT','결과 화면 오른쪽')],
        widget=forms.CheckboxSelectMultiple,help_text='표시할 자리를 하나 이상 선택하세요.')
    class Meta:
        model=m.Revision
        fields='__all__'

@admin.register(m.Campaign)
class CampaignAdmin(AuditedAdmin):
    list_display=('name','advertiser','enabled','blocked','pending_change','active_revision')
    list_filter=('enabled','blocked','pending_change')
    readonly_fields=('active_revision','pending_change')
    def raw_readonly_fields(self,request,obj=None):
        # Reassigning an existing campaign to a different owner/contract would reattribute history.
        return self.readonly_fields+(('advertiser','contract') if obj else ())
    fieldsets=(('캠페인 연결 정보',{'fields':('name','advertiser','contract')}),
               ('집행 설정',{'fields':('enabled','blocked')}),
               ('심사 상태 확인',{'fields':('pending_change','active_revision')}))

@admin.register(m.Revision)
class RevisionAdmin(KoreanAdminMixin,admin.ModelAdmin):
    form=RevisionForm
    object_history_template='admin/ad_revision_history.html'
    def history_view(self,request,object_id,extra_context=None):
        # Let Django enforce object access before evaluating the audit query.
        response=super().history_view(request,object_id,extra_context)
        if hasattr(response,'context_data') and response.context_data and 'object' in response.context_data:
            obj=response.context_data['object']
            response.context_data.update(ad_reviews=m.Audit.objects.filter(
                object_id=str(obj.pk),operation__in=('APPROVED','REJECTED')).order_by('-created_at'),
                ad_return_url=reverse('admin:advertising_revision_change',args=[obj.pk])+'?guided=1')
        return response
    list_display=('campaign','version','template_id','bid_points','status','reviewed_by')
    list_filter=('status','template_id')
    actions=('approve','reject')
    readonly_fields=('version','status','asset_sha256','reviewed_by','reviewed_at')
    def get_urls(self):
        return [path('<uuid:object_id>/review/',self.admin_site.admin_view(require_POST(self.review_single)),name='advertising_revision_review_single')]+super().get_urls()

    def review_single(self,request,object_id):
        if not request.user.has_perms(['advertising.change_revision','advertising.change_campaign']):
            return HttpResponseForbidden('심사와 캠페인 변경 권한이 필요합니다.')
        obj=self.get_object(request,str(object_id))
        if obj is None:
            self.message_user(request,'제출 버전을 찾을 수 없습니다. 목록에서 다시 선택하세요.',messages.ERROR)
            return HttpResponseRedirect(reverse('admin:advertising_revision_changelist'))
        decision=request.POST.get('decision');reason=request.POST.get('_review_reason','').strip()
        if decision not in ('APPROVED','REJECTED') or not 2<=len(reason)<=500:
            self.message_user(request,'심사 결과를 저장하지 않았습니다. 공백을 제외한 심사 사유를 2~500자로 입력하고 승인 또는 반려를 선택하세요.',messages.ERROR)
            return HttpResponseRedirect(reverse('admin:advertising_revision_change',args=[obj.pk])+'?guided=1#review-panel')
        try:review_revision(obj.pk,decision=='APPROVED',request.user.get_username(),reason)
        except (AdError,ValidationError) as error:
            self.message_user(request,str(error),messages.ERROR)
            return HttpResponseRedirect(reverse('admin:advertising_revision_change',args=[obj.pk])+'?guided=1')
        if decision=='APPROVED':
            self.message_user(request,'승인했습니다. 다음 화면에서 ‘광고 집행 켜기’를 직접 체크하고 저장하세요. 아직 집행은 시작하지 않았습니다.',messages.SUCCESS)
            return HttpResponseRedirect(reverse('admin:advertising_campaign_change',args=[obj.campaign_id])+'?guided=1')
        self.message_user(request,'반려했습니다. 내용을 고쳐 새 버전으로 제출하세요.',messages.WARNING)
        return HttpResponseRedirect(reverse('admin:advertising_revision_change',args=[obj.pk])+'?guided=1')
    fieldsets=(('1. 캠페인과 광고 유형',{'fields':('campaign','kind','product_id','product_verified')}),
               ('2. 배너와 입찰',{'fields':('template_id','title','alt_text','bid_points','is_general')}),
               ('3. 표시 위치와 기간',{'fields':('placements','starts_at','ends_at')}),
               ('4. 클릭 후 이동',{'fields':('destination',)}),
               ('심사 이력 · 자동 기록',{'fields':('version','status','reviewed_by','reviewed_at','asset_sha256'),'classes':('collapse',)}))
    def raw_readonly_fields(self,request,obj=None):
        return tuple(f.name for f in m.Revision._meta.fields) if obj else self.readonly_fields
    def has_delete_permission(self,request,obj=None):return False
    def has_change_permission(self,request,obj=None):
        return False if obj else super().has_change_permission(request,obj)
    def save_model(self,request,obj,form,change):
        TestPolicy.load()
        if change:raise ValidationError('새 버전을 등록하세요.')
        obj.save()
        m.Audit.objects.create(operation='REVISION_REQUESTED_BY_OPERATOR',object_id=str(obj.pk),
            actor=request.user.get_username(),detail={'campaign':str(obj.campaign_id),'version':obj.version})
    def review(self,request,queryset,approved):
        if not request.user.has_perms(['advertising.change_revision','advertising.change_campaign']):
            self.message_user(request,'심사·캠페인 변경 권한이 필요합니다.',messages.ERROR);return
        for obj in queryset:
            try:review_revision(obj.pk,approved,request.user.get_username(),'관리자 명시 심사')
            except (AdError,ValidationError) as e:self.message_user(request,str(e),messages.ERROR)
            else:self.message_user(request,f'{obj}: {"승인 (별도 활성화 필요)" if approved else "거절"}',messages.SUCCESS)
    @admin.action(description='선택한 제출 버전 승인 (활성화는 별도)')
    def approve(self,request,queryset):self.review(request,queryset,True)
    @admin.action(description='선택한 제출 버전 거절')
    def reject(self,request,queryset):self.review(request,queryset,False)

@admin.register(m.DefaultNotice)
class NoticeAdmin(AuditedAdmin):
    list_display=('title','template_id','approved','active')
    readonly_fields=('asset_sha256',)
    def save_model(self,request,obj,form,change):
        # A default notice is separately approved, never a bypass to a capped campaign.
        obj.asset_sha256=banner(obj.template_id)['sha256']
        super().save_model(request,obj,form,change)

class ReadOnlyAdmin(KoreanAdminMixin,admin.ModelAdmin):
    def raw_readonly_fields(self,request,obj=None):
        return tuple(f.name for f in self.model._meta.fields if f.name not in (self.exclude or ()))
    def has_add_permission(self,request):return False
    def has_change_permission(self,request,obj=None):return False
    def has_delete_permission(self,request,obj=None):return False

@admin.register(m.Decision)
class DecisionAdmin(ReadOnlyAdmin):
    list_display=('id','placement','status','campaign','created_at','rendered_at','clicked_at')
    list_filter=('status','placement')
    exclude=('visitor_hash','input_hash')

@admin.register(m.Event)
class EventAdmin(ReadOnlyAdmin):
    list_display=('id','decision','kind','status','reason','received_at')
    list_filter=('kind','status')

@admin.register(m.Audit)
class AuditAdmin(ReadOnlyAdmin):
    list_display=('operation','object_id','actor','created_at')

@admin.register(m.PolicyRecord)
class PolicyAdmin(ReadOnlyAdmin):
    list_display=('version','checksum','created_at')

for model,model_admin in admin.site._registry.items():
    if model._meta.app_label=='advertising':
        for field in model._meta.fields:
            setattr(type(model_admin),'_display_'+field.name,display_method(field.name))
