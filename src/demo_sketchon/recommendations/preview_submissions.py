"""Admit an optional preview using the existing job tables (no schema change)."""
from uuid import UUID, uuid5

from django.conf import settings
from interior_recommender.application import preview
from interior_recommender.application.locks import RunBusy, execution_lock
from interior_recommender.config import Settings

from . import queue
from .execution_config import scope
from .models import RecommendationJob


def submit(parent_directory, owner, image_id):
    if not isinstance(image_id, str) or not image_id or len(image_id) > 512:
        raise ValueError("추천된 상품을 선택하세요.")
    source = preview.selected_source(parent_directory, owner, image_id)
    if source["demo"] != settings.INTERIOR_RECOMMENDER_DEMO:
        raise ValueError("데모와 실제 실행을 혼합할 수 없습니다.")
    model_settings = Settings.from_env()
    fingerprint = preview.source_hash(source, model_settings)
    request_id = str(uuid5(UUID(source["request"]["parent_run_id"]), fingerprint))
    existing = RecommendationJob.objects.filter(scope=scope(), owner_id=owner, request_id=request_id).first()
    if existing is None or existing.status == "STAGING_FAILED":
        queue.ready_contract()
    job, created = queue.reserve(owner, request_id, fingerprint)
    if created:
        directory = settings.INTERIOR_RECOMMENDER_OUTPUT_ROOT / "runs" / str(job.id)
        try:
            directory.mkdir(parents=True, exist_ok=True)
            with execution_lock(directory):
                preview.snapshot(directory, job.id, source, model_settings, "sqlite-jobs-v1")
                queue.staged(job)
        except RunBusy:
            raise queue.QueueUnavailable("미리보기를 접수하고 있습니다. 잠시 후 다시 눌러주세요.") from None
        except Exception:
            queue.staging_failed(job)
            raise
        job.refresh_from_db()
    return str(job.id), queue.job_dto(job)
