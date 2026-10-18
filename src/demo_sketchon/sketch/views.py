import io
import hashlib
import copy
import json
import logging
import warnings
from uuid import UUID, uuid4

from django.conf import settings
from django.core.exceptions import RequestDataTooBig
from django.core.files.base import ContentFile
from django.db import transaction
from django.http import FileResponse, HttpResponse, JsonResponse
from django.shortcuts import get_object_or_404, redirect, render
from django.templatetags.static import static
from django.utils import timezone
from django.views.decorators.csrf import ensure_csrf_cookie
from django.views.decorators.http import require_GET, require_http_methods
from PIL import Image, ImageOps, UnidentifiedImageError

from . import validation
from .models import Composition, Project, SketchEvent
from .mock_composition import compose, furniture, render_placements
from .catalog import CATEGORIES, PRODUCTS
from .stickers import STICKERS
from .raw_samples import MAX_RAW_BYTES, save_immutable, strict_constant, validate_sample
from .quota import charge_guest_generation, GuestLimitReached, quota_status


from .access import visitor_id as owner, project_scope, owned_project, owned_result


def serialize(project):
    return {"id": str(project.id), "writer_id":project.owner, "title": project.title, "width": project.width, "height": project.height,
            "photo_url": f"/api/projects/{project.id}/photo/" if project.photo else None,
            "document": project.document, "request_context": project.request_context, "revision": project.revision, "locked": project.locked,
            "editor_url": f"/editor/{project.id}/", "updated_at": project.updated_at.isoformat()}


@require_GET
@ensure_csrf_cookie
def home(request):
    project = Project.objects.create(owner=owner(request), account=request.user if request.user.is_authenticated else None, title="Home 방문")
    SketchEvent.objects.create(project=project, event_type="home.viewed", payload={})
    return render(request, "sketch/home.html", {"initial": {"id": str(project.id)}, "visitor_id": project.owner, "platform_config": {"mode": "mock"}})


@require_GET
@ensure_csrf_cookie
def editor(request, project_id=None):
    if project_id:
        project = owned_project(request, project_id)
    else:
        project = Project.objects.create(owner=owner(request), account=request.user if request.user.is_authenticated else None, title="나의 공간 스케치")
    if project.locked:
        result = project.composition_set.order_by("-created_at").first()
        return redirect("result-page", result_id=result.id)
    SketchEvent.objects.create(project=project, event_type="page.viewed", payload={"page": "editor"})
    return render(request, "sketch/editor.html", {"initial": serialize(project), "visitor_id": project.owner, "platform_config": {"mode": "mock"},
                                               "stickers": [{**row, 'url': static(row['image'])} for row in STICKERS]})


def prepare_photo(upload):
    if upload.size > 10 * 1024 * 1024:
        raise ValueError("사진은 10MB 이하로 선택해 주세요.")
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("error", Image.DecompressionBombWarning)
            with Image.open(upload) as image:
                if image.format not in {"JPEG", "MPO", "PNG", "WEBP"}:
                    raise ValueError("JPG·PNG·WebP 파일을 선택해 주세요.")
                if image.width * image.height > 25000000:
                    raise ValueError(f"사진 해상도는 2,500만 픽셀 이하여야 합니다. 현재 사진은 {image.width}×{image.height}픽셀입니다.")
                # Some phone JPEGs contain MPO auxiliary images; use the primary photo.
                image.seek(0)
                image.load()
                image = ImageOps.exif_transpose(image)
                image.thumbnail((2048, 2048))
                background = Image.new("RGB", image.size, "white")
                if "A" in image.getbands():
                    background.paste(image.convert("RGB"), mask=image.getchannel("A"))
                else:
                    background.paste(image.convert("RGB"))
                buffer = io.BytesIO()
                background.save(buffer, format="JPEG", quality=90)
                return ContentFile(buffer.getvalue()), image.width, image.height
    except (UnidentifiedImageError, OSError, Image.DecompressionBombError, Image.DecompressionBombWarning):
        raise ValueError("읽을 수 없는 사진입니다. JPG·PNG·WebP 파일을 선택해 주세요.") from None


@require_http_methods(["GET", "POST"])
def projects(request):
    session_owner = owner(request)
    if request.method == "GET":
        rows = Project.objects.filter(project_scope(request)).order_by("-updated_at")
        return JsonResponse({"projects": [{"id": str(row.id), "title": row.title, "updated_at": row.updated_at.isoformat()} for row in rows[:100]]})
    try:
        project = Project(owner=session_owner, account=request.user if request.user.is_authenticated else None, title=validation.title(request.POST.get("title", "나의 공간")))
        source_id = request.POST.get('source_project_id')
        if source_id:
            source = owned_project(request, UUID(source_id))
            project.saved_project = source.saved_project
        upload = request.FILES.get("photo")
        content = None
        original_bytes = None
        if upload:
            content, project.width, project.height = prepare_photo(upload)
            upload.seek(0)
            original_bytes = upload.read()
            project.original_checksum = hashlib.sha256(original_bytes).hexdigest()
        with transaction.atomic():
            if original_bytes is not None:
                # Preserve the exact uploaded bytes separately from the display derivative.
                project.original_photo.save(f"{project.id}.bin", ContentFile(original_bytes), save=False)
            if content:
                project.photo.save(f"{project.id}.jpg", content, save=False)
            project.save()
            SketchEvent.objects.create(project=project, event_type="project.created", payload={"has_photo": bool(upload), "width": project.width, "height": project.height})
            if upload:
                SketchEvent.objects.create(project=project, event_type="photo.uploaded", payload={"width": project.width, "height": project.height, "bytes": upload.size})
        return JsonResponse(serialize(project), status=201)
    except ValueError as error:
        return JsonResponse({"error": str(error)}, status=400)
    except Exception:
        if "project" in locals():
            if project.photo:
                project.photo.delete(save=False)
            if project.original_photo:
                project.original_photo.delete(save=False)
        raise


@require_http_methods(["GET", "POST"])
def project_detail(request, project_id):
    project = owned_project(request, project_id)
    if request.method == "GET":
        return JsonResponse(serialize(project))
    if project.locked:
        return JsonResponse({"error": "검색이 완료된 작업입니다. 결과 페이지에서 다시 그리기를 선택해 주세요."}, status=409)
    try:
        payload = json.loads(request.body)
        if not isinstance(payload, dict):
            raise ValueError("JSON 객체가 필요합니다.")
        safe_title = validation.title(payload.get("title"))
        safe_document = validation.document(payload.get("document"))
        safe_events = validation.events(payload.get("events", []))
        safe_context = project.request_context
        if "request_context" in payload:
            from .platform import generation_context
            context_payload = payload["request_context"]
            if not isinstance(context_payload, dict) or set(context_payload) - {"text_requirements", "style_preferences"}:
                raise ValueError("공간 설명과 분위기 입력을 확인해 주세요.")
            safe_context = generation_context(context_payload)
        revision = payload.get("revision")
        if type(revision) is not int or revision < 0:
            raise ValueError("저장 버전이 올바르지 않습니다.")
        with transaction.atomic():
            count = Project.objects.filter(id=project.id, revision=revision, locked=False).update(
                title=safe_title, document=safe_document, request_context=safe_context, revision=revision + 1, updated_at=timezone.now())
            if count != 1:
                return JsonResponse({"error": "다른 탭에서 저장된 내용이 있습니다. JSON을 내려받아 보관한 뒤 다시 불러와 주세요."}, status=409)
            for identifier, event_type, event_payload in safe_events:
                existing, created = SketchEvent.objects.get_or_create(event_id=identifier, defaults={"project": project, "event_type": event_type, "payload": event_payload})
                if not created and (existing.project_id != project.id or existing.event_type != event_type or existing.payload != event_payload):
                    raise ValueError("중복 이벤트 ID의 내용이 다릅니다.")
            SketchEvent.objects.create(project=project, event_type="sketch.saved", payload={"revision": revision + 1, "stroke_count": len(safe_document["strokes"]), "item_count": len(safe_document["items"])})
        project.refresh_from_db()
        return JsonResponse(serialize(project))
    except (ValueError, UnicodeDecodeError, RequestDataTooBig) as error:
        return JsonResponse({"error": str(error) if not isinstance(error, (json.JSONDecodeError, RequestDataTooBig)) else "데이터가 올바르지 않거나 5MB를 초과했습니다."}, status=400)


@require_GET
def photo(request, project_id):
    project = owned_project(request, project_id)
    if not project.photo:
        return JsonResponse({"error": "사진이 없습니다."}, status=404)
    try:
        response = FileResponse(project.photo.open("rb"), content_type="image/jpeg")
    except FileNotFoundError:
        return JsonResponse({"error": "저장된 사진 파일을 찾을 수 없습니다."}, status=404)
    response["Cache-Control"] = "private, no-store"
    return response


@require_http_methods(["POST"])
def collect_events(request, project_id):
    project = owned_project(request, project_id)
    try:
        if request.content_type == "application/json":
            payload = json.loads(request.body)
            if not isinstance(payload, dict):
                raise ValueError("JSON 객체가 필요합니다.")
            incoming = payload.get("events")
        else:
            incoming = json.loads(request.POST.get("events", "null"))
        safe_events = validation.events(incoming)
        with transaction.atomic():
            for identifier, event_type, event_payload in safe_events:
                existing, created = SketchEvent.objects.get_or_create(event_id=identifier, defaults={"project": project, "event_type": event_type, "payload": event_payload})
                if not created and (existing.project_id != project.id or existing.event_type != event_type or existing.payload != event_payload):
                    raise ValueError("중복 이벤트 ID의 내용이 다릅니다.")
        return JsonResponse({"accepted": len(safe_events)})
    except (ValueError, UnicodeDecodeError, RequestDataTooBig) as error:
        return JsonResponse({"error": "이벤트 형식 또는 크기를 확인해 주세요." if isinstance(error, (json.JSONDecodeError, RequestDataTooBig)) else str(error)}, status=400)


@require_http_methods(["POST"])
def collect_sample(request, project_id):
    project = owned_project(request, project_id)
    try:
        if request.content_type == "application/json":
            body = request.body
        elif request.content_type == "multipart/form-data":
            body = request.POST.get("sample", "").encode("utf-8")
        else:
            return JsonResponse({"error": "JSON 또는 종료 시 multipart 샘플이 필요합니다."}, status=415)
        if len(body) > MAX_RAW_BYTES:
            return JsonResponse({"error": "원본 샘플은 4MB까지 제출할 수 있습니다."}, status=413)
        sample = json.loads(body.decode("utf-8"), parse_constant=strict_constant)
    except RequestDataTooBig:
        return JsonResponse({"error": "원본 샘플 크기 제한을 초과했습니다."}, status=413)
    except (ValueError, UnicodeError):
        return JsonResponse({"error": "엄격한 UTF-8 JSON이 필요합니다."}, status=400)
    try:
        warnings = validate_sample(sample, project)
    except (ValueError, TypeError, KeyError, AttributeError) as error:
        return JsonResponse({"error": str(error), "quality_status": "quarantine"}, status=422)
    try:
        idempotent = save_immutable(settings.SKETCH_RAW_ROOT, sample)
    except FileExistsError as error:
        return JsonResponse({"error": str(error)}, status=409)
    except OSError:
        logging.getLogger(__name__).exception("스케치 원본 저장 실패")
        return JsonResponse({"error": "원본을 저장하지 못했습니다. 임시 데이터는 유지됩니다."}, status=503)
    return JsonResponse({"sample_id": sample["sample_id"], "idempotent": idempotent,
                         "quality_status": "review" if warnings else "pass", "warnings": warnings}, status=200 if idempotent else 201)


def serialize_result(result):
    root = f"/api/results/{result.id}"
    from .platform import scene_objects
    return {"schema_version": 1, "status": "SUCCEEDED", "writer_id":result.project.owner, "scene_generation_id": str(result.id), "scene_objects": scene_objects(result), "category_options": {key: value for key, value in CATEGORIES.items() if key != "other"}, "generation_context": result.generation_context, "mode": "mock", "result_id": str(result.id), "project_id": str(result.project_id),
        "revision": result.revision, "version": result.version,
        "image_url": root + f"/image/?v={result.version}", "download_url": root + f"/image/?download=1&v={result.version}",
        "data_url": root + "/data/", "placements": result.placements, "initial_placements": result.initial_placements or result.placements, "page_url": f"/results/{result.id}/",
        "photo_url": f"/api/projects/{result.project_id}/photo/",
        "notice": "모의 결과입니다. 텍스트 규칙과 샘플 그래픽으로 흐름을 재현하며 실제 AI 예측·상품 검색은 수행하지 않습니다."}


@require_http_methods(["POST"])
def search(request, project_id):
    project = owned_project(request, project_id)
    result = None
    try:
        payload = json.loads(request.body)
        if not isinstance(payload, dict):
            raise ValueError("JSON 객체가 필요합니다.")
        document = validation.document(payload.get("document"))
        from .platform import generation_context
        context = generation_context(payload)
        try:
            identifier = UUID(payload.get("request_id"))
        except (ValueError, TypeError, AttributeError):
            raise ValueError("검색 요청 ID가 올바르지 않습니다.") from None
        revision = payload.get("revision")
        if type(revision) is not int or revision < 0:
            raise ValueError("작업 버전이 올바르지 않습니다.")
        existing = Composition.objects.filter(id=identifier).first()
        if existing:
            if existing.project_id != project.id or existing.input_document != document or existing.generation_context != context or existing.revision != revision + 1:
                raise ValueError("중복 검색 요청의 내용이 다릅니다.")
            return JsonResponse(serialize_result(existing))
        if project.locked:
            return JsonResponse({"error": "검색이 완료된 작업입니다. 다시 그리기를 선택해 주세요."}, status=409)
        if revision != project.revision:
            return JsonResponse({"error": "다른 탭에서 작업이 변경됐습니다. 새 작업에서 다시 검색해 주세요."}, status=409)
        content, placements = compose(project, document, context.get("text_requirements", ""), context.get("style_preferences", []))
        result = Composition(id=identifier, project=project, input_document=document, placements=placements, initial_placements=copy.deepcopy(placements), revision=revision + 1, generation_context=context)
        with transaction.atomic():
            # Re-read after acquiring the write transaction: a concurrent replay
            # may have completed while image composition ran outside the lock.
            Project.objects.select_for_update().get(pk=project.pk)
            existing = Composition.objects.filter(id=identifier).first()
            if existing:
                if existing.project_id != project.id or existing.input_document != document or existing.generation_context != context or existing.revision != revision + 1:
                    raise ValueError("중복 검색 요청의 내용이 다릅니다.")
                return JsonResponse(serialize_result(existing))
            if not Project.objects.filter(id=project.id, revision=revision, locked=False).update(document=document, request_context=context, revision=revision + 1, locked=True, updated_at=timezone.now()):
                return JsonResponse({"error": "작업 버전이 변경됐습니다. 다시 시도해 주세요."}, status=409)
            charge_guest_generation(request)
            result.image.save(f"{result.id}.png", ContentFile(content), save=False)
            result.save()
            SketchEvent.objects.create(project=project, event_type="search.completed", payload={"mode": "mock", "result_id": str(result.id), "placement_count": len(placements)})
        return JsonResponse(serialize_result(result), status=201)
    except GuestLimitReached as error:
        SketchEvent.objects.create(project=project, event_type='guest.generation_limited', payload={})
        return JsonResponse({'error':str(error), 'quota':quota_status(request)}, status=429)
    except (ValueError, UnicodeDecodeError, RequestDataTooBig) as error:
        if result and result.image:
            result.image.delete(save=False)
        return JsonResponse({"error": "검색 데이터 형식 또는 크기를 확인해 주세요." if isinstance(error, (json.JSONDecodeError, RequestDataTooBig)) else str(error)}, status=400)
    except Exception:
        if result and result.image:
            result.image.delete(save=False)
        raise


@require_GET
def result_image(request, result_id):
    result = owned_result(request, result_id)
    try:
        response = FileResponse(result.image.open("rb"), content_type="image/png", as_attachment=request.GET.get("download") == "1", filename="sketchon-result.png")
    except FileNotFoundError:
        return JsonResponse({"error": "합성 결과 이미지 파일을 찾을 수 없습니다."}, status=404)
    response["Cache-Control"] = "private, no-store"
    return response


@require_GET
@ensure_csrf_cookie
def result_page(request, result_id):
    result = owned_result(request, result_id)
    labels = list(dict.fromkeys(CATEGORIES[row["category"]] for row in result.placements))
    SketchEvent.objects.create(project=result.project, event_type="result.viewed", payload={"result_id": str(result.id)})
    return render(request, "sketch/result.html", {"initial": serialize_result(result), "visitor_id": result.project.owner,
        "visitor_label": result.project.owner[:8], "prediction": ", ".join(labels),
        "width": result.project.width, "height": result.project.height, "platform_config": {"mode": "mock"}})


@require_GET
def result_sprite(request, result_id, placement_id):
    result = owned_result(request, result_id)
    item = next((row for row in result.placements if row["id"] == placement_id), None)
    if not item:
        return JsonResponse({"error": "배치 요소를 찾을 수 없습니다."}, status=404)
    product = next(row for row in PRODUCTS if row["id"] == item["product_id"])
    stream = io.BytesIO()
    preview = furniture(product["category"], product["color"], item.get("variant"), item.get("render_style"))
    preview.thumbnail((640, 640), Image.Resampling.LANCZOS)
    preview.save(stream, format="PNG")
    response = HttpResponse(stream.getvalue(), content_type="image/png")
    response["Cache-Control"] = "private, no-store"
    return response


@require_http_methods(["POST"])
def update_placements(request, result_id):
    result = owned_result(request, result_id)
    new_image = None
    committed = False
    try:
        payload = json.loads(request.body)
        if not isinstance(payload, dict) or type(payload.get("version")) is not int:
            raise ValueError("배치 저장 버전이 올바르지 않습니다.")
        incoming = payload.get("placements")
        if not isinstance(incoming, list) or len(incoming) != len(result.placements):
            raise ValueError("결과의 배치 요소가 일치하지 않습니다.")
        placements = []
        for existing, row in zip(result.placements, incoming):
            if not isinstance(row, dict) or set(row) != {"id", "x", "y", "width", "height"} or row["id"] != existing["id"]:
                raise ValueError("결과의 배치 ID가 일치하지 않습니다.")
            for key in ("x", "y"):
                validation.number(row[key])
            for key in ("width", "height"):
                validation.number(row[key], .04)
            if row["x"] + row["width"] > 1.000001 or row["y"] + row["height"] > 1.000001:
                raise ValueError("가구는 사진 영역 안에 배치해 주세요.")
            placements.append({**existing, **row})
        if payload["version"] != result.version:
            return JsonResponse({"error": "다른 탭에서 결과가 변경됐습니다. 페이지를 새로고침해 주세요."}, status=409)
        content = render_placements(result.project, placements)
        old_image = result.image.name
        result.image.save(f"{result.id}-{uuid4()}.png", ContentFile(content), save=False)
        new_image = result.image.name
        with transaction.atomic():
            count = Composition.objects.filter(id=result.id, version=payload["version"]).update(
                placements=placements, image=new_image, version=result.version + 1)
            if not count:
                result.image.storage.delete(new_image)
                return JsonResponse({"error": "결과 버전이 변경됐습니다. 페이지를 새로고침해 주세요."}, status=409)
            SketchEvent.objects.create(project=result.project, event_type="result.adjusted", payload={"result_id": str(result.id), "version": result.version + 1})
        committed = True
        try:
            result.image.storage.delete(old_image)
        except OSError:
            logging.getLogger(__name__).warning("Old composition image cleanup deferred; current result is saved.")
        result.refresh_from_db()
        return JsonResponse(serialize_result(result))
    except (ValueError, UnicodeDecodeError, RequestDataTooBig) as error:
        if new_image and not committed:
            result.image.storage.delete(new_image)
        return JsonResponse({"error": "배치 형식 또는 크기를 확인해 주세요." if isinstance(error, (json.JSONDecodeError, RequestDataTooBig)) else str(error)}, status=400)
    except Exception:
        if new_image and not committed:
            result.image.storage.delete(new_image)
        raise


@require_http_methods(["POST"])
def redraw(request, result_id):
    result = owned_result(request, result_id)
    try:
        payload = json.loads(request.body) if request.body and request.content_type == "application/json" else {}
        if not isinstance(payload, dict) or payload.get("mode", "blank") not in ("blank", "sketch", "new"):
            raise ValueError("다시 그리기 방식을 확인해 주세요.")
        mode = payload.get("mode", "blank")
    except (ValueError, UnicodeDecodeError):
        return JsonResponse({"error": "다시 그리기 요청이 올바르지 않습니다."}, status=400)
    with transaction.atomic():
        project = Project.objects.create(owner=result.project.owner, account=result.project.account, saved_project=result.project.saved_project if mode != "new" else None, title=result.project.title,
            photo=result.project.photo.name if mode != "new" else "",
            original_photo=result.project.original_photo.name if mode != "new" else "",
            original_checksum=result.project.original_checksum if mode != "new" else "",
            width=result.project.width if mode != "new" else 1200, height=result.project.height if mode != "new" else 800,
            document=copy.deepcopy(result.input_document) if mode == "sketch" else {"strokes": [], "items": []},
            request_context=copy.deepcopy(result.generation_context) if mode == "sketch" else {})
        SketchEvent.objects.create(project=project, event_type="sketch.restarted", payload={"previous_result_id": str(result.id), "mode": mode})
    return JsonResponse(serialize(project), status=201)


@require_http_methods(["POST"])
def crop_photo(request, project_id):
    source_project = owned_project(request, project_id)
    if source_project.locked:
        return JsonResponse({"error": "결과 페이지에서 다시 그리기를 선택해 주세요."}, status=409)
    project = None
    try:
        payload = json.loads(request.body)
        if not isinstance(payload, dict) or set(payload) != {"x", "y", "width", "height"} or not source_project.photo:
            raise ValueError("사진과 자르기 영역이 필요합니다.")
        for key in ("x", "y"):
            validation.number(payload[key])
        for key in ("width", "height"):
            validation.number(payload[key], .001)
        if payload["x"] + payload["width"] > 1.000001 or payload["y"] + payload["height"] > 1.000001:
            raise ValueError("사진 안에서 자르기 영역을 선택해 주세요.")
        with source_project.photo.open("rb") as source, Image.open(source) as original:
            x, y = round(payload["x"] * original.width), round(payload["y"] * original.height)
            right = min(original.width, round((payload["x"] + payload["width"]) * original.width))
            bottom = min(original.height, round((payload["y"] + payload["height"]) * original.height))
            if right - x < 4 or bottom - y < 4:
                raise ValueError("자르기 영역은 가로·세로 4픽셀 이상이어야 합니다.")
            image = original.crop((x, y, right, bottom)).convert("RGB")
            stream = io.BytesIO(); image.save(stream, format="JPEG", quality=90)
        project = Project(owner=source_project.owner, account=source_project.account, saved_project=source_project.saved_project, title=source_project.title, width=image.width, height=image.height,
            original_photo=source_project.original_photo.name, original_checksum=source_project.original_checksum,
            request_context=copy.deepcopy(source_project.request_context))
        with transaction.atomic():
            project.photo.save(f"{project.id}.jpg", ContentFile(stream.getvalue()), save=False)
            project.save()
            SketchEvent.objects.create(project=project, event_type="photo.cropped", payload={"previous_project_id": str(source_project.id), **payload})
        return JsonResponse(serialize(project), status=201)
    except (ValueError, UnicodeDecodeError, RequestDataTooBig) as error:
        return JsonResponse({"error": "자르기 데이터 형식을 확인해 주세요." if isinstance(error, (json.JSONDecodeError, RequestDataTooBig)) else str(error)}, status=400)
    except Exception:
        if project and project.photo:
            project.photo.delete(save=False)
        raise


@require_GET
def result_data(request, result_id):
    result = owned_result(request, result_id)
    response = JsonResponse({**serialize_result(result), "input_document": result.input_document,
        "width": result.project.width, "height": result.project.height, "created_at": result.created_at.isoformat()}, json_dumps_params={"ensure_ascii": False})
    response["Content-Disposition"] = 'attachment; filename="sketchon-result.json"'
    response["Cache-Control"] = "private, no-store"
    return response


@require_GET
def result_sample(request, result_id, placement_id, variant):
    """Local illustrative cards only; no seller image or catalog lookup."""
    result = owned_result(request, result_id)
    item = next((row for row in result.placements if row["id"] == placement_id), None)
    category = request.GET.get("category", item["category"] if item else "")
    if not item or variant not in {"cream", "wood", "sage"} or category not in CATEGORIES or category == "other":
        return JsonResponse({"error":"샘플 이미지를 찾을 수 없습니다."}, status=404)
    stream = io.BytesIO(); image = furniture(category, "#d7ccba", variant, "photo")
    image.thumbnail((640, 640), Image.Resampling.LANCZOS); image.save(stream, format="PNG")
    response = HttpResponse(stream.getvalue(), content_type="image/png")
    response["Cache-Control"] = "private, no-store"
    return response
