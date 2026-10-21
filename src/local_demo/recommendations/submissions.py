"""Cheap header validation and immutable input storage before durable enqueue."""
import io
import json
from uuid import UUID, uuid4

from django.conf import settings
from PIL import Image, UnidentifiedImageError
from interior_recommender.assets import checksum, read_json, write_bytes, write_json
from interior_recommender.application.locks import RunBusy, execution_lock
from interior_recommender.domain.recommendation import CONTEXT_VERSION
from interior_recommender.pipeline import contract_hash, now
from interior_recommender.schemas import InputRequest
from interior_recommender.services import PROMPT_VERSION, SCHEMA_VERSION

from . import queue
from .models import RecommendationJob
from .execution_config import scope


def image_size(data, *, webp=False):
    try:
        with Image.open(io.BytesIO(data)) as image:
            if image.format not in ({"PNG", "JPEG", "WEBP"} if webp else {"PNG", "JPEG"}):
                raise ValueError("지원하지 않는 이미지 형식입니다.")
            if image.width * image.height > 16_000_000:
                raise ValueError("사진은 1,600만 화소 이하여야 합니다.")
            return image.size[::-1] if image.getexif().get(274) in {5, 6, 7, 8} else image.size
    except (UnidentifiedImageError, Image.DecompressionBombError) as error:
        raise ValueError("이미지 형식을 확인해 주세요.") from error


def submit(room, sketch, text, document, region, owner, request_id, metadata):
    owner, request_id = str(UUID(owner)), str(UUID(request_id))
    if not room or not (text.strip() or sketch):
        raise ValueError("방 사진과 텍스트 또는 스케치를 입력하세요.")
    size = image_size(room, webp=True)
    if sketch and image_size(sketch) != size:
        raise ValueError("스케치와 사진 크기가 다릅니다.")
    raw_hash = checksum(room + (sketch or b"") + json.dumps(
        [text, document["strokes"], region, document, metadata], sort_keys=True).encode())
    # Replays do not require a live worker, nor redo normalization/API calls.
    existing = RecommendationJob.objects.filter(scope=scope(), owner_id=owner, request_id=request_id).first()
    if existing and existing.status != "STAGING_FAILED":
        job, _ = queue.reserve(owner, request_id, raw_hash)
        return str(job.id), queue.job_dto(job)
    event_path = settings.INTERIOR_RECOMMENDER_OUTPUT_ROOT / "events" / f"{owner}-{request_id}.json"
    if existing is None and event_path.exists():
        saved = read_json(event_path)
        if saved["hash"] != raw_hash:
            raise ValueError("같은 요청 ID의 입력이 다릅니다.")
        run_id = str(UUID(saved["run_id"]))
        directory = settings.INTERIOR_RECOMMENDER_OUTPUT_ROOT / "runs" / run_id
        manifest = read_json(directory / "manifest.json")
        if manifest["request"]["owner_id"] != owner or "direct_contract" not in manifest:
            raise PermissionError
        previous = read_json(directory / "job.json") if (directory / "job.json").exists() else {}
        completed = previous.get("status") == "SUCCEEDED"
        return run_id, {"status": "SUCCEEDED" if completed else "FAILED", "action": "prepare",
            "error": "" if completed else "이전 실행 방식의 작업입니다. 입력 수정 후 새 추천을 요청해 주세요.",
            "execution_backend": "legacy-file"}
    contract = queue.ready_contract()
    job, created = queue.reserve(owner, request_id, raw_hash)
    if not created:
        return str(job.id), queue.job_dto(job)
    directory = settings.INTERIOR_RECOMMENDER_OUTPUT_ROOT / "runs" / str(job.id)
    try:
        directory.mkdir(parents=True, exist_ok=True)
        with execution_lock(directory):
            snapshot(directory, room, sketch, text, document, region, owner, request_id, metadata, contract, job)
            queue.staged(job)
    except RunBusy:
        raise queue.QueueUnavailable("입력을 저장하고 있습니다. 같은 요청으로 잠시 후 다시 시도해 주세요.") from None
    except Exception:
        queue.staging_failed(job)
        raise
    job.refresh_from_db()
    return str(job.id), queue.job_dto(job)


def snapshot(directory, room, sketch, text, document, region, owner, request_id, metadata, contract, job):
    assets = {}
    for name, data in (("inputs/original-room.bin", room), ("inputs/original-sketch.bin", sketch)):
        if data:
            write_bytes(directory / name, data)
            assets[name] = {"asset_id": str(uuid4()), "checksum": checksum(data)}
    request = InputRequest(room_image="derived/room.png", sketch_image="derived/sketch.png" if sketch else None,
        text=text, categories=contract["categories"], requested_region=region).model_dump(mode="json")
    request.update(owner_id=owner, source_event_id=request_id, revision_no=1, parent_run_id=None,
                   sketch_strokes=document["strokes"], canvas_document=document, external_project=metadata)
    manifest = {"run_id": str(job.id), "created_at": now(), "mode": "LOCAL_DIRECT_SAMPLE",
                "execution_backend": "sqlite-jobs-v1", "bronze_verified": False,
                "web_demo": settings.INTERIOR_RECOMMENDER_DEMO, "prompt_version": PROMPT_VERSION,
                "schema_version": SCHEMA_VERSION, "settings": contract["settings"], "request": request,
                "templates": [], "assets": assets, "stages": {}, "status": "QUEUED", "selected_template": None,
                "direct_contract": {"context_version": CONTEXT_VERSION, "ranking_policy": contract["policy"],
                                    "index_version": contract["index_version"]}}
    manifest["input_hash"] = contract_hash(manifest)
    # A complete manifest is the final file, so a crashed admission can be recovered.
    write_json(directory / "manifest.json", manifest)
