"""Session-owned Application endpoints; replace providers, not UI event handlers."""
import hashlib
import json
import logging
from uuid import UUID, uuid4, uuid5
from datetime import timedelta
from django.core.exceptions import RequestDataTooBig
from django.conf import settings
from django.utils import timezone
from django.db import transaction
from django.http import JsonResponse, Http404
from django.shortcuts import get_object_or_404
from django.views.decorators.http import require_GET, require_POST
from .models import Composition, RecommendationRun, SketchEvent, SceneObjectRevision
from . import validation
from . import platform
from .views import serialize_result
from .access import project_scope

log = logging.getLogger(__name__)


def body(request):
    try:
        value = json.loads(request.body)
        if not isinstance(value, dict): raise ValueError("JSON 객체가 필요합니다.")
        return value
    except (ValueError, UnicodeError, RequestDataTooBig):
        raise ValueError("요청 형식과 크기를 확인해 주세요.") from None


def run_dto(run):
    return {"schema_version": 1, "recommendation_run_id": str(run.id), "job_id": str(run.id),
            "status": run.status, "status_url": f"/api/jobs/{run.id}/", "mode": run.mode,
            "scene_object_id": str(run.scene_object_id), "items": run.items, **run.metadata}


@require_GET
def scene(request, scene_id):
    result = get_object_or_404(Composition.objects.filter(project_scope(request, "project__")), id=scene_id)
    response = JsonResponse(serialize_result(result))
    response["Cache-Control"] = "private, no-store"
    return response


def lease_duration():
    return timedelta(seconds=max(10, int(getattr(settings, "ROOMPICK_RECOMMENDATION_LEASE_SECONDS", 120))))


def expired(run, now):
    return (run.lease_expires_at <= now) if run.lease_expires_at else (run.created_at + lease_duration() <= now)


@require_GET
def job(request, job_id):
    run = get_object_or_404(RecommendationRun.objects.filter(project_scope(request, "composition__project__")), id=job_id)
    if run.status == "RUNNING" and expired(run, timezone.now()):
        RecommendationRun.objects.filter(id=run.id, status="RUNNING", lease_token=run.lease_token).update(
            status="FAILED", metadata={"error": "중단되거나 시간이 초과된 작업입니다. 같은 요청으로 다시 시도해 주세요."})
        run.refresh_from_db()
    response = JsonResponse(run_dto(run)); response["Cache-Control"] = "private, no-store"
    return response


@require_POST
def recommend(request, object_id):
    run = None; token = None
    try:
        data = body(request)
        scene_id = UUID(data.get("scene_generation_id")); request_id = UUID(data.get("request_id"))
        result = get_object_or_404(Composition.objects.select_related("project").filter(project_scope(request, "project__")), id=scene_id)
        obj = next((row for row in platform.scene_objects(result) if row["id"] == str(object_id)), None)
        if not obj: return JsonResponse({"error": "물체가 변경됐습니다. 현재 결과에서 다시 선택해 주세요."}, status=409)
        if obj["change_type"] == "EXISTING" or (obj["change_type"] == "UNCERTAIN" and data.get("user_confirmed") is not True):
            return JsonResponse({"error": "추가·교체한 물체인지 먼저 확인해 주세요."}, status=422)
        signature = hashlib.sha256(json.dumps({"scene_object_id":obj["id"],"category_id":obj["category_id"],"bbox":obj["bbox"],"version":obj["version"],"user_confirmed":data.get("user_confirmed") is True},sort_keys=True).encode()).hexdigest()
        legacy = dict(obj); legacy.pop("user_corrected", None); legacy["preview_url"] = legacy["preview_url"].split("?", 1)[0]
        legacy_signature = hashlib.sha256(json.dumps({"object":legacy,"user_confirmed":data.get("user_confirmed") is True},sort_keys=True).encode()).hexdigest()
        now = timezone.now(); proposed_token = uuid4()
        with transaction.atomic():
            if not Composition.objects.filter(id=result.id, version=result.version).update(version=result.version):
                return JsonResponse({"error": "결과 배치가 변경됐습니다. 현재 결과에서 다시 선택해 주세요."}, status=409)
            current_object = next((row for row in platform.scene_objects(result) if row["id"] == str(object_id)), None)
            if current_object != obj:
                return JsonResponse({"error": "검색 조건이 변경됐습니다. 현재 결과에서 다시 선택해 주세요."}, status=409)
            run, created = RecommendationRun.objects.get_or_create(id=request_id, defaults={
                "composition": result, "scene_object_id": object_id, "input_version": result.version,
                "input_hash": signature, "status": "RUNNING", "lease_token": proposed_token,
                "lease_expires_at": now + lease_duration(), "attempt_count": 1})
            if run.input_hash == legacy_signature and run.composition_id == result.id:
                RecommendationRun.objects.filter(id=run.id, input_hash=legacy_signature).update(input_hash=signature)
                run.input_hash = signature
            if run.composition_id != result.id or run.input_hash != signature:
                return JsonResponse({"error": "같은 요청 ID에 다른 입력을 사용할 수 없습니다."}, status=409)
            if not created:
                if run.status == "SUCCEEDED": return JsonResponse(run_dto(run))
                if run.status == "RUNNING" and not expired(run, now): return JsonResponse(run_dto(run), status=202)
                claimed = RecommendationRun.objects.filter(id=run.id, status=run.status, lease_token=run.lease_token).update(
                    status="RUNNING", lease_token=proposed_token, lease_expires_at=now + lease_duration(),
                    attempt_count=run.attempt_count + 1, metadata={})
                if not claimed:
                    run.refresh_from_db(); return JsonResponse(run_dto(run), status=202)
            token = proposed_token
        # No external service is configured here. Providers run outside transactions.
        # A future Ad adapter must deduplicate its work using this unchanged request_id.
        response = platform.provider().recommend(scene_object=obj, request_id=str(run.id), context=result.generation_context)
        items = platform.normalize_recommendations(response, run.id)
        updated = RecommendationRun.objects.filter(id=run.id, status="RUNNING", lease_token=token, lease_expires_at__gt=timezone.now()).update(
            status="SUCCEEDED", mode=response["mode"], items=items, lease_expires_at=None,
            metadata={"notice": str(response.get("notice", ""))[:500], "index_version_id": response.get("index_version_id")})
        if not updated:
            RecommendationRun.objects.filter(id=run.id, status="RUNNING", lease_token=token, lease_expires_at__lte=timezone.now()).update(
                status="FAILED", metadata={"error":"작업 시간이 초과됐습니다. 같은 요청으로 다시 시도해 주세요."})
        run.refresh_from_db()
        return JsonResponse(run_dto(run), status=201 if updated else 202)
    except Http404:
        raise
    except (TypeError, ValueError, AttributeError):
        if run is None or token is None: return JsonResponse({"error": "추천 요청 ID와 입력을 확인해 주세요."}, status=400)
        return failed_attempt(run, token, 502, "추천 정보를 확인하지 못했습니다. 생성 결과를 유지하고 다시 시도해 주세요.")
    except Exception:
        if run is None or token is None: raise
        log.exception("Recommendation provider failed for run %s", run.id)
        return failed_attempt(run, token, 503, "추천 서버에 연결하지 못했습니다. 생성 결과는 유지됩니다.")


def failed_attempt(run, token, status, message):
    updated = RecommendationRun.objects.filter(id=run.id, status="RUNNING", lease_token=token).update(
        status="FAILED", lease_expires_at=None, metadata={"error": message})
    if not updated:
        run.refresh_from_db(); return JsonResponse(run_dto(run), status=202)
    return JsonResponse({"error": message}, status=status)


@require_POST
def revise_object(request, object_id):
    try:
        data = body(request); scene_id = UUID(data.get("scene_generation_id")); request_id = UUID(data.get("request_id"))
        category = data.get("category_id"); bbox = data.get("bbox")
        if not isinstance(category, str) or category not in platform.CATEGORIES or category == "other": raise ValueError("데모 품목을 선택해 주세요.")
        if not isinstance(bbox, dict) or set(bbox) != {"x", "y", "width", "height"}: raise ValueError("검색 영역을 확인해 주세요.")
        for key in ("x", "y"): validation.number(bbox[key])
        for key in ("width", "height"): validation.number(bbox[key], .01)
        if bbox["x"] + bbox["width"] > 1.000001 or bbox["y"] + bbox["height"] > 1.000001: raise ValueError("검색 영역은 사진 안에 있어야 합니다.")
        signature = hashlib.sha256(json.dumps({"object_id":str(object_id),"scene_id":str(scene_id),"category_id":category,"bbox":bbox},sort_keys=True).encode()).hexdigest()
        with transaction.atomic():
            result = get_object_or_404(Composition.objects.select_for_update().filter(project_scope(request, "project__")), id=scene_id)
            existing = SceneObjectRevision.objects.filter(request_id=request_id).first()
            if existing:
                if existing.composition_id != result.id or existing.input_hash != signature: return JsonResponse({"error":"같은 수정 요청 ID의 내용이 다릅니다."},status=409)
                return JsonResponse(serialize_result(result))
            obj = next((row for row in platform.scene_objects(result) if row["id"] == str(object_id)),None)
            if not obj: return JsonResponse({"error":"물체가 변경됐습니다. 현재 결과에서 다시 선택해 주세요."},status=409)
            if not Composition.objects.filter(id=result.id, version=result.version).update(version=result.version):
                return JsonResponse({"error":"결과 배치가 변경됐습니다. 현재 결과에서 다시 선택해 주세요."},status=409)
            SceneObjectRevision.objects.create(composition=result, placement_id=obj["placement_id"], base_version=result.version,
                previous_object_id=object_id, category_id=category, bbox=bbox, request_id=request_id, input_hash=signature)
        return JsonResponse(serialize_result(result),status=201)
    except (ValueError, TypeError, AttributeError):
        return JsonResponse({"error":"품목과 검색 영역의 입력값을 확인해 주세요."},status=400)


def owned_item(request, data, item_id):
    try: run_id = UUID(data.get("recommendation_run_id"))
    except (ValueError, TypeError, AttributeError): raise ValueError("추천 실행 ID가 필요합니다.") from None
    run = get_object_or_404(RecommendationRun.objects.select_related("composition__project").filter(project_scope(request, "composition__project__")), id=run_id, status="SUCCEEDED")
    item = next((row for row in run.items if row["id"] == str(item_id)), None)
    if item is None: raise ValueError("추천 항목이 일치하지 않습니다.")
    return run, item


@require_POST
def item_event(request, item_id):
    try:
        data = body(request); run, item = owned_item(request, data, item_id)
        kind = data.get("event_type")
        if kind not in {"PRODUCT_IMPRESSION", "PRODUCT_SELECTED", "PRODUCT_REJECTED"}: raise ValueError("지원하지 않는 반응입니다.")
        event_id = uuid5(item_id, "impression") if kind == "PRODUCT_IMPRESSION" else UUID(data.get("event_id"))
        payload = {"recommendation_run_id": str(run.id), "recommendation_item_id": str(item_id),
                   "product_id": item["product_id"], "impression_id": str(uuid5(item_id, "impression")), "mode": run.mode}
        with transaction.atomic():
            SketchEvent.objects.get_or_create(event_id=uuid5(item_id, "impression"), defaults={"project": run.composition.project, "event_type": "PRODUCT_IMPRESSION", "payload": payload})
            event, created = SketchEvent.objects.get_or_create(event_id=event_id, defaults={"project": run.composition.project, "event_type": kind, "payload": payload})
            if event.project_id != run.composition.project_id or event.event_type != kind or event.payload != payload:
                raise ValueError("같은 이벤트 ID의 내용이 다릅니다.")
        return JsonResponse({"accepted": True, "event_id": str(event_id), "impression_id": payload["impression_id"]})
    except (ValueError, TypeError, AttributeError):
        return JsonResponse({"error": "상품 반응의 종류와 ID를 확인해 주세요."}, status=400)


@require_POST
def link(request, item_id):
    try:
        data = body(request); run, item = owned_item(request, data, item_id)
        if run.mode != "live" or not item["link_available"]:
            return JsonResponse({"error": "예시 상품이거나 연결이 준비되지 않은 상품입니다."}, status=409)
        if data.get("link_kind", "COMPARISON") != "COMPARISON": raise ValueError("지원하지 않는 연결 종류입니다.")
        target = platform.provider().resolve_link(item=item, link_kind="COMPARISON")
        url = platform.validated_link(target["url"])
        event_id = UUID(data.get("event_id"))
        payload = {"recommendation_run_id": str(run.id), "recommendation_item_id": str(item_id), "product_id": item["product_id"], "link_kind": "COMPARISON"}
        event, created = SketchEvent.objects.get_or_create(event_id=event_id, defaults={"project": run.composition.project, "event_type": "COMPARISON_LINK_CLICKED", "payload": payload})
        if not created and (event.project_id != run.composition.project_id or event.event_type != "COMPARISON_LINK_CLICKED" or event.payload != payload): raise ValueError("이벤트 ID가 일치하지 않습니다.")
        return JsonResponse({"url": url, "link_kind": "COMPARISON"})
    except (ValueError, TypeError, AttributeError, KeyError):
        return JsonResponse({"error": "상품 연결 정보를 확인하지 못했습니다."}, status=400)
    except Http404:
        raise
    except Exception:
        log.exception("Product link provider failed")
        return JsonResponse({"error": "상품 연결이 지연되고 있습니다. 다시 시도해 주세요."}, status=503)
