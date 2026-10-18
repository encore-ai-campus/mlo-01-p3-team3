"""Local execution ledger. These are not the future pipeline/service schemas."""
import uuid

from django.db import models
from django.utils import timezone


class QueueControl(models.Model):
    scope = models.CharField(max_length=64, primary_key=True)
    config_key = models.CharField(max_length=64, blank=True)
    contract = models.JSONField(default=dict)
    ready_until = models.DateTimeField(null=True)


class WorkerPresence(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    scope = models.CharField(max_length=64, db_index=True)
    config_key = models.CharField(max_length=64)
    expires_at = models.DateTimeField()


class RecommendationJob(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    scope = models.CharField(max_length=64, db_index=True)
    owner_id = models.UUIDField()
    request_id = models.UUIDField()
    input_hash = models.CharField(max_length=64)
    status = models.CharField(max_length=16, default="STAGING", db_index=True)
    available_at = models.DateTimeField(default=timezone.now, db_index=True)
    lease_token = models.UUIDField(default=uuid.uuid4)
    lease_until = models.DateTimeField(null=True)
    retry_generation = models.PositiveIntegerField(default=0)
    claims = models.PositiveIntegerField(default=0)
    recoveries = models.PositiveIntegerField(default=0)
    error = models.CharField(max_length=250, blank=True)
    created_at = models.DateTimeField(default=timezone.now)
    started_at = models.DateTimeField(null=True)
    finished_at = models.DateTimeField(null=True)

    class Meta:
        constraints = [models.UniqueConstraint(fields=["scope", "owner_id", "request_id"], name="direct_request_once")]
        indexes = [models.Index(fields=["scope", "status", "available_at"], name="direct_ready_jobs")]


class RecommendationAttempt(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    job = models.ForeignKey(RecommendationJob, on_delete=models.PROTECT, related_name="executions")
    lease_token = models.UUIDField()
    status = models.CharField(max_length=16, default="RUNNING")
    started_at = models.DateTimeField(default=timezone.now)
    finished_at = models.DateTimeField(null=True)
    duration_ms = models.PositiveBigIntegerField(null=True)
    wait_ms = models.PositiveBigIntegerField(default=0)


class ProviderGate(models.Model):
    key = models.CharField(max_length=64, primary_key=True)
    next_call_at = models.DateTimeField(default=timezone.now)


class ProviderPermit(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    gate = models.ForeignKey(ProviderGate, on_delete=models.CASCADE)
    job = models.ForeignKey(RecommendationJob, on_delete=models.CASCADE)
    lease_token = models.UUIDField()
    expires_at = models.DateTimeField(db_index=True)
