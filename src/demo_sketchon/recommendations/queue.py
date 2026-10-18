"""Short, fenced transactions; no files, SDK calls or sleeps inside transactions."""
import hashlib
from datetime import timedelta
from uuid import uuid4

from django.conf import settings
from django.db import transaction
from django.db.models import Q
from django.utils import timezone

from interior_recommender.errors import LeaseLost
from .execution_config import config_key, limits, scope
from .models import (ProviderGate, ProviderPermit, QueueControl,
                     RecommendationAttempt, RecommendationJob, WorkerPresence)

ACTIVE = ("STAGING", "QUEUED", "RUNNING", "RETRY_WAIT")


class QueueUnavailable(Exception):
    pass


def control():
    QueueControl.objects.get_or_create(scope=scope())
    return QueueControl.objects.select_for_update().get(scope=scope())


def advertise(contract):
    with transaction.atomic():
        row = control()
        current = timezone.now()
        # Differently configured workers may not replace the advertised contract
        # while the previous worker is still healthy.
        if row.ready_until and row.ready_until > current and (row.config_key != config_key() or row.contract != contract):
            raise QueueUnavailable("다른 설정의 추천 워커가 실행 중입니다.")
        row.contract = contract
        row.config_key = config_key()
        row.ready_until = current + timedelta(seconds=limits()["lease_seconds"])
        row.save()
        presence = WorkerPresence.objects.create(scope=scope(), config_key=config_key(), expires_at=row.ready_until)
        return str(presence.pk)


def ready_contract():
    row = QueueControl.objects.filter(scope=scope(), config_key=config_key(), ready_until__gt=timezone.now()).first()
    if row is None or not WorkerPresence.objects.filter(scope=scope(), config_key=config_key(), expires_at__gt=timezone.now()).exists():
        raise QueueUnavailable("추천 준비 중입니다. 잠시 후 다시 요청해 주세요.")
    return row.contract


def reserve(owner, request_id, input_hash):
    """Reserve global capacity and a unique request before writing any files."""
    with transaction.atomic():
        control()
        existing = RecommendationJob.objects.filter(scope=scope(), owner_id=owner, request_id=request_id).first()
        if existing:
            if existing.input_hash != input_hash:
                raise ValueError("같은 요청 ID의 입력이 다릅니다.")
            if existing.status == "STAGING":
                raise QueueUnavailable("입력을 저장하고 있습니다. 같은 요청으로 잠시 후 다시 시도해 주세요.")
            if existing.status != "STAGING_FAILED":
                return existing, False
        if RecommendationJob.objects.filter(scope=scope(), status__in=ACTIVE).count() >= limits()["max_pending"]:
            raise QueueUnavailable("처리 대기 중인 요청이 많습니다. 잠시 후 다시 시도해 주세요.")
        if existing:
            row = existing
            row.status, row.lease_token, row.error, row.finished_at = "STAGING", uuid4(), "", None
            row.lease_until = timezone.now() + timedelta(seconds=limits()["staging_seconds"])
            row.save()
        else:
            row = RecommendationJob.objects.create(scope=scope(), owner_id=owner, request_id=request_id,
                input_hash=input_hash, lease_until=timezone.now() + timedelta(seconds=limits()["staging_seconds"]))
        return row, True


def staged(job):
    changed = RecommendationJob.objects.filter(pk=job.pk, status="STAGING", lease_token=job.lease_token).update(
        status="QUEUED", lease_until=None, available_at=timezone.now(), error="")
    if not changed:
        raise LeaseLost("입력 접수 권한이 만료되었습니다.")


def staging_failed(job):
    RecommendationJob.objects.filter(pk=job.pk, status="STAGING", lease_token=job.lease_token).update(
        status="STAGING_FAILED", lease_until=None, finished_at=timezone.now(), error="입력 보존에 실패했습니다. 같은 요청으로 다시 시도해 주세요.")


def retry(job_id):
    with transaction.atomic():
        control()
        job = RecommendationJob.objects.get(pk=job_id, scope=scope())
        if job.status in ACTIVE or job.status == "SUCCEEDED":
            return job
        if RecommendationJob.objects.filter(scope=scope(), status__in=ACTIVE).count() >= limits()["max_pending"]:
            raise QueueUnavailable("처리 대기 중인 요청이 많습니다. 잠시 후 다시 시도해 주세요.")
        job.status, job.error, job.finished_at = "QUEUED", "", None
        job.retry_generation += 1
        job.available_at = max(job.available_at, timezone.now())
        job.save()
        return job


def claim():
    current = timezone.now()
    with transaction.atomic():
        control()
        if RecommendationJob.objects.filter(scope=scope(), status="RUNNING", lease_until__gt=current).count() >= limits()["max_inflight"]:
            return None
        due = Q(status__in=("QUEUED", "RETRY_WAIT"), available_at__lte=current) | Q(status="RUNNING", lease_until__lte=current)
        job = RecommendationJob.objects.filter(due, scope=scope()).order_by("available_at", "created_at").first()
        if job is None:
            return None
        recovering = job.status == "RUNNING"
        job.executions.filter(status="RUNNING").update(status="ABANDONED", finished_at=current)
        if recovering and job.recoveries >= limits()["max_recoveries"] * (job.retry_generation + 1):
            job.status, job.error, job.finished_at = "FAILED", "작업 복구 상한에 도달했습니다. 다시 시도해 주세요.", current
            job.save()
            return None
        # No completion duration is invented for a process that disappeared.
        job.lease_token, job.status = uuid4(), "RUNNING"
        job.lease_until = current + timedelta(seconds=limits()["lease_seconds"])
        job.started_at = job.started_at or current
        job.claims += 1
        job.recoveries += int(recovering)
        job.save()
        attempt = RecommendationAttempt.objects.create(job=job, lease_token=job.lease_token,
            wait_ms=max(0, round((current - job.available_at).total_seconds() * 1000)))
        return {"id": str(job.pk), "token": str(job.lease_token), "generation": job.retry_generation,
                "execution_id": str(attempt.pk), "wait_ms": attempt.wait_ms}


def assert_lease(claimed):
    if not RecommendationJob.objects.filter(pk=claimed["id"], status="RUNNING", lease_token=claimed["token"],
                                            lease_until__gt=timezone.now()).exists():
        raise LeaseLost("Execution lease expired")


def heartbeat(claims, presence):
    current = timezone.now()
    expiry = current + timedelta(seconds=limits()["lease_seconds"])
    if presence:
        QueueControl.objects.filter(scope=scope(), config_key=config_key()).update(ready_until=expiry)
        WorkerPresence.objects.filter(pk=presence).update(expires_at=expiry)
    for claimed in claims:
        updated = RecommendationJob.objects.filter(pk=claimed["id"], status="RUNNING", lease_token=claimed["token"],
                                                   lease_until__gt=current).update(lease_until=expiry)
        if updated:
            ProviderPermit.objects.filter(job_id=claimed["id"], lease_token=claimed["token"]).update(expires_at=expiry)


def retire(presence):
    with transaction.atomic():
        row = control()
        WorkerPresence.objects.filter(pk=presence).delete()
        if not WorkerPresence.objects.filter(scope=scope(), expires_at__gt=timezone.now()).exists():
            row.ready_until = timezone.now()
            row.save(update_fields=["ready_until"])


def finish(claimed, status, duration_ms, *, retry_at=None, error=""):
    with transaction.atomic():
        changed = RecommendationJob.objects.filter(pk=claimed["id"], status="RUNNING", lease_token=claimed["token"],
            lease_until__gt=timezone.now()).update(status=status, available_at=retry_at or timezone.now(),
            lease_until=None, error=error, finished_at=None if status == "RETRY_WAIT" else timezone.now())
        if not changed:
            raise LeaseLost("Execution no longer owns this job")
        RecommendationAttempt.objects.filter(pk=claimed["execution_id"], lease_token=claimed["token"]).update(
            status=status, finished_at=timezone.now(), duration_ms=duration_ms)


def acquire_provider(claimed, model, kind, provider="gemini"):
    """One account/model gate shared by every worker using this database."""
    cfg = limits()
    group = "embedding" if kind == "embed_text" else "generation" if kind == "generate" else "analysis"
    key = hashlib.sha256((settings.RECOMMENDATION_API_ACCOUNT + ":" + provider + ":" + model).encode()).hexdigest()
    ProviderGate.objects.get_or_create(key=key)
    with transaction.atomic():
        gate = ProviderGate.objects.select_for_update().get(pk=key)
        assert_lease(claimed)
        current = timezone.now()
        ProviderPermit.objects.filter(gate=gate, expires_at__lte=current).delete()
        active = ProviderPermit.objects.filter(gate=gate)
        if gate.next_call_at > current:
            return None, gate.next_call_at
        if active.count() >= cfg.get(group + "_concurrency", 1):
            return None, current + timedelta(seconds=cfg["poll_seconds"])
        permit = ProviderPermit.objects.create(gate=gate, job_id=claimed["id"], lease_token=claimed["token"],
            expires_at=current + timedelta(seconds=cfg["lease_seconds"]))
        gate.next_call_at = current + timedelta(seconds=60 / cfg.get(group + "_rpm", 2))
        gate.save(update_fields=["next_call_at"])
        return str(permit.pk), None


def release_provider(permit):
    ProviderPermit.objects.filter(pk=permit).delete()


def job_dto(job):
    return {"id": str(job.pk), "action": "prepare", "status": job.status,
            "created_at": job.created_at.isoformat(), "started_at": job.started_at.isoformat() if job.started_at else None,
            "finished_at": job.finished_at.isoformat() if job.finished_at else None,
            "next_retry_at": job.available_at.isoformat() if job.status == "RETRY_WAIT" else None,
            "error": job.error, "execution_backend": "sqlite-jobs-v1"}
