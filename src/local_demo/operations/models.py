import uuid
from django.conf import settings
from django.db import models


class GenerationAttempt(models.Model):
    request_id=models.UUIDField(primary_key=True)
    project=models.ForeignKey('sketch.Project',on_delete=models.PROTECT)
    input_hash=models.CharField(max_length=64)
    actor_kind=models.CharField(max_length=12)
    actor_account=models.ForeignKey(settings.AUTH_USER_MODEL,null=True,on_delete=models.PROTECT)
    status=models.CharField(max_length=16,default='RUNNING',db_index=True)
    started_at=models.DateTimeField(auto_now_add=True)
    finished_at=models.DateTimeField(null=True)
    duration_ms=models.PositiveIntegerField(null=True)
    error_code=models.CharField(max_length=60,blank=True)
    result=models.ForeignKey('sketch.Composition',null=True,on_delete=models.PROTECT)


class RawReceipt(models.Model):
    id=models.UUIDField(primary_key=True,default=uuid.uuid4,editable=False)
    sample_id=models.UUIDField(null=True,blank=True)
    project=models.ForeignKey('sketch.Project',on_delete=models.PROTECT)
    checksum=models.CharField(max_length=64)
    byte_count=models.PositiveIntegerField()
    stroke_count=models.PositiveIntegerField(default=0)
    point_count=models.PositiveIntegerField(default=0)
    capture_status=models.CharField(max_length=20,blank=True)
    validation_status=models.CharField(max_length=20)
    warnings=models.JSONField(default=list)
    has_file=models.BooleanField(default=False)
    first_received_at=models.DateTimeField(auto_now_add=True)
    last_received_at=models.DateTimeField(auto_now=True)
    retry_count=models.PositiveIntegerField(default=0)
    review_status=models.CharField(max_length=16,default='PENDING')
    review_reason=models.CharField(max_length=500,blank=True)
    reviewer=models.ForeignKey(settings.AUTH_USER_MODEL,null=True,on_delete=models.PROTECT)
    reviewed_at=models.DateTimeField(null=True)

    class Meta:
        constraints=[models.UniqueConstraint(fields=['project','sample_id','checksum','validation_status'],name='raw_receipt_identity'),
            models.UniqueConstraint(fields=['project','checksum','validation_status'],condition=models.Q(sample_id__isnull=True),name='raw_receipt_without_sample')]


class AdCampaign(models.Model):
    id=models.UUIDField(primary_key=True,default=uuid.uuid4,editable=False)
    campaign_id=models.SlugField(max_length=48,unique=True)
    title=models.CharField(max_length=80)
    description=models.CharField(max_length=500,blank=True)
    advertiser=models.CharField(max_length=100,blank=True)
    creative=models.ImageField(upload_to='ops/ad_creatives/',blank=True)
    landing_url=models.URLField(blank=True)
    category=models.CharField(max_length=40)
    slot_id=models.CharField(max_length=40)
    bid_units=models.PositiveIntegerField(default=1)
    starts_at=models.DateTimeField()
    ends_at=models.DateTimeField()
    active=models.BooleanField(default=False)
    review_status=models.CharField(max_length=16,default='PENDING')
    review_reason=models.CharField(max_length=500,blank=True)
    reviewed_by=models.ForeignKey(settings.AUTH_USER_MODEL,null=True,on_delete=models.PROTECT)
    reviewed_at=models.DateTimeField(null=True)
    content_version=models.PositiveIntegerField(default=1)
    reviewed_version=models.PositiveIntegerField(null=True)
    revision=models.PositiveIntegerField(default=0)
    policy_version=models.CharField(max_length=50,default='sketchon-demo-policy-v1')
    created_at=models.DateTimeField(auto_now_add=True)
    updated_at=models.DateTimeField(auto_now=True)


class OperatorAudit(models.Model):
    actor=models.ForeignKey(settings.AUTH_USER_MODEL,on_delete=models.PROTECT)
    action=models.CharField(max_length=60)
    object_type=models.CharField(max_length=40)
    object_id=models.CharField(max_length=100)
    before=models.JSONField(default=dict)
    after=models.JSONField(default=dict)
    reason=models.CharField(max_length=500,blank=True)
    created_at=models.DateTimeField(auto_now_add=True)

    class Meta:
        permissions=[('view_console','운영 대시보드 조회'),('view_people','이용자 조회'),('view_projects','작업 조회'),
          ('view_behavior','행동데이터 조회'),('view_collection','수집 메타데이터 조회'),('view_sensitive','민감 원본 열람'),
          ('review_raw','Raw 검수'),('manage_ads','광고 초안 관리'),('review_ads','광고 심사'),
          ('view_ad_reports','광고 성과 조회'),('view_ops_audit','운영 감사 이력 조회')]
