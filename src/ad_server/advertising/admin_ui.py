"""Display-only Korean labels; model fields, stored values and APIs stay unchanged."""
import json
from datetime import datetime
from django.contrib import admin
from django.db import models
from django.utils import timezone
from django.utils.html import format_html

MODEL_TITLES={
 'Advertiser':'광고주','MockContract':'모의 계약','Campaign':'캠페인 · 집행 관리',
 'Revision':'광고 버전 · 심사','DefaultNotice':'기본 안내 배너','Decision':'광고 선정 이력',
 'Event':'노출 · 클릭 이력','Audit':'운영 변경 기록','PolicyRecord':'적용 정책 이력'}
FIELDS={
 'id':'관리 번호','name':'이름','status':'상태','is_test':'시험용 광고주',
 'advertiser':'광고주','reference':'계약 이름','starts_at':'시작 일시','ends_at':'종료 일시',
 'contract':'모의 계약','enabled':'광고 집행 켜기','blocked':'정책 차단',
 'pending_change':'새 버전 심사 대기','active_revision':'현재 승인된 버전',
 'campaign':'캠페인','version':'버전 번호','template_id':'배너 이미지',
 'product_id':'연결 상품 ID','product_verified':'상품 연결 확인 완료','kind':'광고 유형',
 'title':'광고 제목','alt_text':'이미지 설명','bid_points':'가상 입찰 포인트',
 'placements':'표시할 위치','is_general':'일반 광고 후보로 사용','destination':'클릭 후 이동 주소',
 'asset_sha256':'승인 이미지 검증값','reviewed_by':'심사 담당자','reviewed_at':'심사 일시',
 'approved':'안내 배너 승인','active':'안내 배너 사용','context':'요청 맥락 번호',
 'placement':'표시 위치','snapshot':'선정 당시 정보','diagnostics':'후보 제외 사유',
 'policy_version':'적용 정책 버전','created_at':'생성 일시','expires_at':'표시 만료 일시',
 'rendered_at':'이미지 표시 일시','visible_at':'가시 노출 일시','clicked_at':'클릭 일시',
 'decision':'광고 선정 번호','fingerprint':'사건 중복 검증값','occurred_at':'사건 발생 일시',
 'received_at':'서버 수신 일시','reason':'처리 사유','payload':'수신 사건 정보',
 'eligible_at_receipt':'수신 당시 집행 가능','operation':'변경 종류','object_id':'대상 관리 번호',
 'actor':'처리 담당자','detail':'변경 상세','checksum':'설정 검증값','configuration':'정책 설정',
 'notice':'기본 안내 배너','visitor_hash':'방문자 검증값','input_hash':'입력 검증값'}
VALUES={
 'PENDING':'심사 대기','ACTIVE':'사용 중','BLOCKED':'차단','APPROVED':'승인','REJECTED':'반려',
 'BRAND':'브랜드 배너','PRODUCT':'상품 연결 배너','SELECTED':'광고 선정','DEFAULT':'기본 안내',
 'NO_FILL':'선정 가능한 광고 없음','VALID':'집계 인정','DIAGNOSTIC':'진단 기록','DUPLICATE':'중복 제외',
 'RENDERED':'이미지 표시','VISIBLE':'가시 노출','CLICK':'클릭','LOAD_ERROR':'이미지 오류',
 'EDITOR_RIGHT':'편집 화면 오른쪽','RESULT_RIGHT':'결과 화면 오른쪽',
 'REVISION_SUBMITTED':'새 광고 버전 제출','REVISION_REQUESTED_BY_OPERATOR':'운영자 버전 등록',
 'OPERATOR_CHANGE':'운영 설정 변경','DEMO_ACTIVATED':'시연 광고 활성화',
 'RENDER_CONFIRMED':'이미지 표시 확인','RENDER_LINKED':'표시 이력에 연결',
 'LINKED_TO_RENDER':'표시 이력에 연결','AWAITING_RENDER':'표시 사건 수신 대기',
 'NOT_ELIGIBLE':'집행 조건 미충족','DECISION_ALREADY_COUNTED':'이미 집계한 사건',
 'ACTIVE':'사용 중','SAFE_ENDED_OR_PAUSED':'중지·종료된 광고의 안전한 이동',
 'DECISION_EXPIRED':'광고 표시 기간 만료','POLICY_BLOCKED':'정책 차단',
 'VISIBILITY_THRESHOLD_NOT_MET':'가시 노출 기준 미충족','RENDER_JOIN_EXPIRED':'표시 연결 대기 만료'}
HELP={
 'status':'광고주는 ‘사용 중’, 계약은 ‘승인’ 상태여야 광고 선정에 참여합니다.',
 'enabled':'승인된 버전이 있을 때만 켤 수 있습니다. 심사 승인만으로 자동 집행되지 않습니다.',
 'blocked':'차단할 때는 ‘광고 집행 켜기’를 함께 해제하세요.',
 'bid_points':'1~10,000의 정수 포인트입니다. 실제 금액·결제·차감이 아닙니다.',
 'is_general':'처음 시연할 때 체크하세요. 확인된 관심 맥락이 없어도 일반 후보로 참여합니다.',
 'destination':'시연은 /test-destination/영문식별자/를 사용하세요. 외부 HTTPS 주소는 허용 호스트 등록이 필요합니다.',
 'product_id':'브랜드 배너는 비워 두세요. 상품 연결 배너만 확인된 상품 ID를 입력합니다.',
 'product_verified':'자동 검증이 아닙니다. 운영자가 실제 상품 대응을 확인한 경우에만 체크합니다.',
 'placements':'편집·결과 화면 중 표시할 자리를 선택하세요. My·모바일 광고는 현재 제외합니다.',
 'template_id':'준비된 원본 배너 10종에서 선택합니다. 새 파일 업로드는 지원하지 않습니다.',
 'alt_text':'이미지를 볼 수 없는 사용자에게 전달할 짧은 설명입니다.',
 'starts_at':'한국 시간(KST) 기준입니다. 현재 시각이 집행 기간 안에 있어야 선정됩니다.',
 'ends_at':'시작 일시보다 뒤로 지정하세요.',
 'advertiser':'앞 단계에서 등록한 광고주를 선택하세요.',
 'contract':'같은 광고주의 승인된 모의 계약을 선택하세요.'}
GUIDES={
 'Advertiser':('1. 광고주 등록','광고주 이름을 입력하고 상태를 ‘사용 중’으로 저장하세요. 다음은 모의 계약 등록입니다.'),
 'MockContract':('2. 모의 계약 등록','같은 광고주를 선택하고 상태를 ‘승인’으로 설정한 뒤 계약 기간을 입력하세요.'),
 'Campaign':('3. 캠페인 등록 · 6. 집행 켜기','처음 등록할 때는 집행을 끕니다. 광고 버전을 제출·승인한 뒤 이 화면에서 ‘광고 집행 켜기’를 체크하세요.'),
 'Revision':('4. 광고 버전 제출 → 5. 심사','캠페인·배너·포인트·위치·기간을 입력해 제출합니다. 목록에서 대기 버전을 선택해 승인하고 캠페인에서 집행을 켜세요. 변경은 새 버전으로 제출합니다.'),
 'DefaultNotice':('기본 안내 배너','선정할 광고가 없을 때 사용하는 별도 승인 안내입니다. 광고 후보의 노출 제한을 우회하지 않습니다.'),
 'Decision':('광고 선정 이력','어떤 광고가 선택됐는지 확인합니다. 만료·중지·차단 상태에 따라 현재 표시 가능 여부는 달라질 수 있습니다.'),
 'Event':('노출 · 클릭 이력','이미지 표시·가시 노출·클릭을 구분합니다. 진단 기록과 중복 사건은 정상 성과에 더하지 않습니다.'),
 'Audit':('운영 변경 기록','등록·심사·활성화 등 변경 이력을 읽기 전용으로 확인합니다.'),
 'PolicyRecord':('적용 정책 이력','모의 운영에 적용한 정책 버전과 설정을 확인합니다.'),
}


def shown(value,field_name='',model_name=''):
    if value is None or value=='':return '—'
    if isinstance(value,bool):return '예' if value else '아니요'
    if isinstance(value,datetime):return timezone.localtime(value).strftime('%Y.%m.%d %H:%M')+' (한국 시간)'
    if field_name=='template_id':
        from .policy import TEMPLATES
        return dict((key,name) for key,name,_ in TEMPLATES).get(value,value)
    if field_name=='kind' and model_name=='Event':return VALUES.get(value,value)
    if field_name=='status' and value=='PENDING' and model_name=='Event':return '표시 사건 연결 대기'
    if isinstance(value,(dict,list)):
        if field_name=='placements':return ' · '.join(VALUES.get(x,x) for x in value)
        return format_html('<pre class="ad-data">{}</pre>',json.dumps(value,ensure_ascii=False,indent=2,default=str))
    return VALUES.get(value,value) if isinstance(value,str) and field_name in ('status','kind','placement','operation','reason') else str(value)


def display_method(name):
    def value(self,obj):
        result=shown(getattr(obj,name),name,type(obj).__name__)
        if name in ('status','enabled','blocked','pending_change','approved','active'):
            return format_html('<span class="ad-status">{}</span>',result)
        return result
    value.short_description=FIELDS.get(name,name)
    value.admin_order_field=name
    return value


def translated_filter(name):
    class Filter(admin.SimpleListFilter):
        title=FIELDS.get(name,name)
        parameter_name=name+'__display'
        def lookups(self,request,model_admin):
            field=model_admin.model._meta.get_field(name)
            if isinstance(field,models.BooleanField):return [('1','예'),('0','아니요')]
            values=model_admin.get_queryset(request).order_by(name).values_list(name,flat=True).distinct()
            return [(str(v),shown(v,name,model_admin.model.__name__)) for v in values]
        def queryset(self,request,queryset):
            if self.value() is None:return queryset
            field=queryset.model._meta.get_field(name)
            value=self.value()
            if isinstance(field,models.BooleanField):
                if value not in ('0','1'):return queryset.none()
                value=value=='1'
            return queryset.filter(**{name:value})
    return Filter


class KoreanAdminMixin:
    change_list_template='admin/ad_change_list.html'
    change_form_template='admin/ad_change_form.html'

    class Media:
        css={'all':('advertising/admin.css',)}

    def raw_readonly_fields(self,request,obj=None):return self.readonly_fields

    def get_changeform_initial_data(self,request):
        from .admin_journey import initial_values
        return initial_values(self.model,request,super().get_changeform_initial_data(request))

    def response_add(self,request,obj,post_url_continue=None):
        from .admin_journey import next_response
        response=next_response(self,request,obj)
        if response:
            self.message_user(request,'입력한 내용을 저장했습니다. 화면의 안내를 따라 진행하세요.')
            return response
        return super().response_add(request,obj,post_url_continue)

    def response_change(self,request,obj):
        from .admin_journey import next_response
        response=next_response(self,request,obj)
        return response if response else super().response_change(request,obj)

    def get_readonly_fields(self,request,obj=None):
        return tuple('_display_'+name for name in self.raw_readonly_fields(request,obj))

    def get_form(self,request,obj=None,**kwargs):
        names=self.raw_readonly_fields(request,obj)
        kwargs['exclude']=list(kwargs.get('exclude') or self.exclude or ())+list(names)+list(self.get_readonly_fields(request,obj))
        declared={n:None for n in names if n in self.form.declared_fields}
        if declared:kwargs['form']=type('ReadonlyDisplayForm',(self.form,),declared)
        return super().get_form(request,obj,**kwargs)

    def formfield_for_dbfield(self,db_field,request,**kwargs):
        kwargs['label']=FIELDS.get(db_field.name,db_field.verbose_name)
        if db_field.name in HELP:kwargs['help_text']=HELP[db_field.name]
        formfield=super().formfield_for_dbfield(db_field,request,**kwargs)
        if formfield is not None and db_field.choices:
            formfield.choices=[(key,shown(key,db_field.name,self.model.__name__)) if key else (key,label) for key,label in formfield.choices]
        return formfield

    def get_list_display(self,request):
        return tuple('_display_'+name if hasattr(self.model,name) else name for name in self.list_display)

    def get_list_filter(self,request):return tuple(translated_filter(name) for name in self.list_filter)

    def get_fieldsets(self,request,obj=None):
        fieldsets=super().get_fieldsets(request,obj)
        readonly=set(self.raw_readonly_fields(request,obj))
        def convert(field):
            if isinstance(field,(tuple,list)):return tuple(convert(x) for x in field)
            return '_display_'+field if field in readonly else field
        return [(title,{**options,'fields':tuple(convert(f) for f in options['fields'])}) for title,options in fieldsets]

    def _context(self,extra_context=None,add=False):
        name=self.model.__name__;title,description=GUIDES[name]
        return {**(extra_context or {}),'ad_guide_title':title,'ad_guide_description':description,
                'ad_model_label':MODEL_TITLES[name],
                'title':MODEL_TITLES[name]+(' 등록' if add else ''),'subtitle':None}

    def changelist_view(self,request,extra_context=None):
        return super().changelist_view(request,extra_context=self._context(extra_context))

    def changeform_view(self,request,object_id=None,form_url='',extra_context=None):
        from .admin_journey import journey_context
        obj=self.get_object(request,object_id) if object_id else None
        context=self._context(extra_context,add=object_id is None)
        context.update(journey_context(self.model,request,obj))
        return super().changeform_view(request,object_id,form_url,extra_context=context)
