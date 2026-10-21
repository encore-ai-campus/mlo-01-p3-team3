import uuid
from django.core.exceptions import ValidationError
from django.db import models
from .policy import TEMPLATE_IDS, PLACEMENTS

class Advertiser(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    name = models.CharField(max_length=120)
    status = models.CharField(max_length=12, choices=[(x,x) for x in ('PENDING','ACTIVE','BLOCKED')], default='PENDING')
    is_test = models.BooleanField(default=True, editable=False)
    def __str__(self): return self.name

class MockContract(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    advertiser = models.ForeignKey(Advertiser, on_delete=models.PROTECT)
    reference = models.CharField(max_length=120)
    status = models.CharField(max_length=12, choices=[(x,x) for x in ('PENDING','APPROVED','BLOCKED')], default='PENDING')
    starts_at = models.DateTimeField()
    ends_at = models.DateTimeField()
    def clean(self):
        if self.starts_at and self.ends_at and self.starts_at >= self.ends_at:
            raise ValidationError('모의 계약 시작은 종료보다 빨라야 합니다.')
    def __str__(self): return self.reference

class Campaign(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    advertiser = models.ForeignKey(Advertiser, on_delete=models.PROTECT)
    contract = models.ForeignKey(MockContract, on_delete=models.PROTECT)
    name = models.CharField(max_length=120)
    enabled = models.BooleanField(default=False)
    blocked = models.BooleanField(default=False)
    pending_change = models.BooleanField(default=True, editable=False)
    active_revision = models.ForeignKey('Revision', null=True, blank=True, on_delete=models.PROTECT, related_name='+', editable=False)
    def clean(self):
        if self.contract_id and self.advertiser_id and self.contract.advertiser_id != self.advertiser_id:
            raise ValidationError('광고주와 모의 계약이 일치해야 합니다.')
        if self.enabled and (not self.active_revision_id or self.pending_change or self.blocked):
            raise ValidationError('승인된 현재 버전이 있고 변경 보류·차단이 해제돼야 활성화할 수 있습니다.')
    def __str__(self): return self.name

class Revision(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    campaign = models.ForeignKey(Campaign, on_delete=models.PROTECT, related_name='revisions')
    version = models.PositiveIntegerField(editable=False, default=1)
    template_id = models.CharField(max_length=32, choices=[(x,x) for x in sorted(TEMPLATE_IDS)])
    product_id = models.CharField(max_length=180, blank=True)
    product_verified = models.BooleanField(default=False)
    kind = models.CharField(max_length=12, choices=[('BRAND','브랜드 배너'),('PRODUCT','상품형')], default='BRAND')
    title = models.CharField(max_length=120)
    alt_text = models.CharField(max_length=200)
    bid_points = models.PositiveIntegerField()
    placements = models.JSONField(default=list, help_text='["EDITOR_RIGHT", "RESULT_RIGHT"] 중 명시적으로 선택')
    is_general = models.BooleanField(default=False)
    destination = models.CharField(max_length=1200)
    starts_at = models.DateTimeField()
    ends_at = models.DateTimeField()
    status = models.CharField(max_length=12, choices=[(x,x) for x in ('PENDING','APPROVED','REJECTED')], default='PENDING', editable=False)
    asset_sha256 = models.CharField(max_length=64, blank=True, editable=False)
    reviewed_by = models.CharField(max_length=150, blank=True, editable=False)
    reviewed_at = models.DateTimeField(null=True, editable=False)
    class Meta:
        constraints=[models.UniqueConstraint(fields=['campaign','version'],name='ads_revision_version')]
    def clean(self):
        from .policy import integer
        from .errors import AdError
        try: integer(self.bid_points,'bid')
        except AdError as e: raise ValidationError(e.message)
        if not isinstance(self.placements,list) or not self.placements or any(x not in PLACEMENTS for x in self.placements):
            raise ValidationError('PC 편집·결과 광고 자리만 명시적으로 선택하세요.')
        if self.starts_at and self.ends_at and self.starts_at >= self.ends_at:
            raise ValidationError('시작 시각은 종료보다 빨라야 합니다.')
        if self.kind == 'PRODUCT' and (not self.product_id or not self.product_verified):
            raise ValidationError('상품형 광고는 상품 ID와 대응 확인이 필요합니다.')
        if self.kind == 'BRAND' and self.product_id:
            raise ValidationError('상품 연결이 있으면 상품형으로 등록하세요.')
    def save(self,*args,**kwargs):
        if not self._state.adding:
            raise ValidationError('제출 버전은 수정할 수 없습니다. 새 버전을 등록하세요.')
        if self._state.adding:
            from django.db import transaction
            from .engine import gate
            with transaction.atomic():
                gate()
                maximum=Revision.objects.filter(campaign_id=self.campaign_id).aggregate(n=models.Max('version'))['n'] or 0
                self.version=maximum+1
                self.status='PENDING'
                self.asset_sha256=''
                self.reviewed_by=''
                self.reviewed_at=None
                self.clean()
                self.full_clean()
                result=super().save(*args,**kwargs)
                Campaign.objects.filter(pk=self.campaign_id).update(enabled=False,pending_change=True)
                Audit.objects.create(operation='REVISION_SUBMITTED',object_id=str(self.pk),detail={
                    'version':self.version,'campaign':str(self.campaign_id),'title':self.title,
                    'bid_points':self.bid_points,'destination':self.destination,'template_id':self.template_id,
                    'placements':self.placements,'starts_at':self.starts_at.isoformat(),'ends_at':self.ends_at.isoformat()})
                return result
        return super().save(*args,**kwargs)
    def __str__(self): return f'{self.campaign.name} v{self.version}'

class DefaultNotice(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    template_id = models.CharField(max_length=32, choices=[(x,x) for x in sorted(TEMPLATE_IDS)])
    title = models.CharField(max_length=120)
    approved = models.BooleanField(default=False)
    active = models.BooleanField(default=False)
    asset_sha256 = models.CharField(max_length=64)
    def clean(self):
        if self.active and not self.approved:raise ValidationError('기본 안내도 별도 승인이 필요합니다.')
        if self.approved:
            from .engine import validate_asset
            from .policy import TestPolicy
            from .errors import AdError
            try:validate_asset(self,TestPolicy.load())
            except AdError as ex:raise ValidationError(ex.message)

class ServingGate(models.Model):
    id = models.PositiveIntegerField(primary_key=True, default=1)

class PolicyRecord(models.Model):
    version = models.CharField(primary_key=True,max_length=100)
    checksum = models.CharField(max_length=64)
    configuration = models.JSONField()
    created_at = models.DateTimeField(auto_now_add=True)

class Context(models.Model):
    id = models.UUIDField(primary_key=True)
    visitor_hash = models.CharField(max_length=64)
    fingerprint = models.CharField(max_length=64)
    created_at = models.DateTimeField(auto_now_add=True)

class Decision(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    context = models.ForeignKey(Context,on_delete=models.PROTECT)
    placement = models.CharField(max_length=24)
    visitor_hash = models.CharField(max_length=64)
    input_hash = models.CharField(max_length=64)
    status = models.CharField(max_length=12)
    campaign = models.ForeignKey(Campaign,on_delete=models.PROTECT,null=True)
    revision = models.ForeignKey(Revision,on_delete=models.PROTECT,null=True)
    notice = models.ForeignKey(DefaultNotice,on_delete=models.PROTECT,null=True)
    snapshot = models.JSONField(default=dict)
    diagnostics = models.JSONField(default=dict)
    policy_version = models.CharField(max_length=100)
    created_at = models.DateTimeField()
    expires_at = models.DateTimeField()
    rendered_at = models.DateTimeField(null=True)
    visible_at = models.DateTimeField(null=True)
    clicked_at = models.DateTimeField(null=True)
    class Meta:
        constraints=[models.UniqueConstraint(fields=['context','placement'],name='ads_context_slot')]

class RequestKey(models.Model):
    id = models.UUIDField(primary_key=True)
    decision = models.ForeignKey(Decision,on_delete=models.PROTECT)
    input_hash = models.CharField(max_length=64)

class Event(models.Model):
    id = models.UUIDField(primary_key=True)
    decision = models.ForeignKey(Decision,on_delete=models.PROTECT,related_name='events')
    kind = models.CharField(max_length=16)
    fingerprint = models.CharField(max_length=64)
    occurred_at = models.DateTimeField()
    received_at = models.DateTimeField()
    status = models.CharField(max_length=16)
    reason = models.CharField(max_length=80)
    payload = models.JSONField(default=dict)
    eligible_at_receipt = models.BooleanField(default=False)

class Audit(models.Model):
    operation = models.CharField(max_length=64)
    object_id = models.CharField(max_length=100)
    actor = models.CharField(max_length=150,blank=True)
    detail = models.JSONField(default=dict)
    created_at = models.DateTimeField(auto_now_add=True)
