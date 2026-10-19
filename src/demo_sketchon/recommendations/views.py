"""Same-origin HTTP and SketchOn ownership; ranking remains in DirectStudio."""
import functools
import json
import time
from pathlib import Path
from uuid import UUID, NAMESPACE_URL, uuid5

from django.conf import settings
from django.core.exceptions import ImproperlyConfigured, RequestDataTooBig
from django.http import FileResponse, Http404, HttpResponseRedirect, JsonResponse
from django.db import DatabaseError
from django.middleware.csrf import get_token
from django.shortcuts import redirect, render
from django.urls import reverse
from django.utils.http import http_date
from django.views.decorators.http import require_GET, require_POST
from interior_recommender.assets import read_json
from interior_recommender.catalog.server import ServerCatalogError
from interior_recommender.errors import LeaseLost
from sketch.access import owned_project, visitor_id
from sketch.validation import document as validate_document
from . import queue, services, submissions, preview_submissions
from .execution_config import scope
from .models import RecommendationJob


def unavailable(request, message):
    response = JsonResponse({"error": message}, status=503, headers={"Retry-After": "3"})
    # SessionMiddleware skips session saves on 5xx. Persist a newly assigned
    # guest identity so a storage/queue failure can be retried with the same
    # owner + request_id rather than creating a second reservation.
    try:
        if not request.user.is_authenticated and request.session.modified and request.session.get("sketch_owner"):
            request.session.save()
            age = None if request.session.get_expire_at_browser_close() else request.session.get_expiry_age()
            response.set_cookie(settings.SESSION_COOKIE_NAME, request.session.session_key, max_age=age,
                expires=http_date(time.time() + age) if age else None,
                domain=settings.SESSION_COOKIE_DOMAIN, path=settings.SESSION_COOKIE_PATH,
                secure=settings.SESSION_COOKIE_SECURE, httponly=settings.SESSION_COOKIE_HTTPONLY,
                samesite=settings.SESSION_COOKIE_SAMESITE)
    except DatabaseError:
        # If the session database is unavailable, do not replace the original
        # retryable response with an exception containing connection details.
        pass
    return response


def api(view):
    @functools.wraps(view)
    def wrapped(request, *args, **kwargs):
        try:
            return view(request, *args, **kwargs)
        except queue.QueueUnavailable as error:
            return unavailable(request, str(error))
        except PermissionError:
            return JsonResponse({"error": "이 추천에 접근할 수 없습니다."}, status=403)
        except (FileNotFoundError, Http404):
            return JsonResponse({"error": "추천 실행 또는 이미지를 찾을 수 없습니다."}, status=404)
        except RequestDataTooBig:
            return JsonResponse({"error": "요청 크기가 너무 큽니다."}, status=413)
        except ImproperlyConfigured:
            return unavailable(request, "추천 카탈로그·모델·차원 설정을 확인해 주세요.")
        except ServerCatalogError:
            return unavailable(request, '상품 DB·인덱스·이용 상태를 확인할 수 없습니다. 잠시 후 다시 시도해 주세요.')
        except (OSError, LeaseLost):
            return unavailable(request, "추천 파일을 처리하지 못했습니다. 잠시 후 다시 시도해 주세요.")
        except (ValueError, TypeError, KeyError):
            return JsonResponse({"error": "사진·스케치 크기, 입력 형식과 요청 ID를 확인해 주세요."}, status=400)
    return wrapped


def actor(request):
    if request.user.is_authenticated:
        return {"account_id": request.user.pk}
    return {"visitor_id": visitor_id(request)}


def engine_owner(access):
    # User IDs need not be UUIDs. The engine's owner UUID is always server-derived.
    return str(uuid5(NAMESPACE_URL, "sketchon:" + json.dumps(access, sort_keys=True)))


def owned_run(request, run_id):
    root = Path(settings.INTERIOR_RECOMMENDER_OUTPUT_ROOT).resolve() / "runs"
    directory = (root / str(UUID(str(run_id)))).resolve()
    if not directory.is_relative_to(root):
        raise PermissionError
    manifest = read_json(directory / "manifest.json")
    access = manifest["request"]["external_project"]["sketchon_access"]
    if access.get("project_id"):
        owned_project(request, access["project_id"])
    elif access != actor(request):
        raise PermissionError
    owner = engine_owner(access)
    if manifest["request"]["owner_id"] != owner:
        raise PermissionError
    return directory, owner


@require_GET
@api
def index(request):
    run_id = request.GET.get("run")
    if not run_id:
        return redirect("editor")
    directory, _ = owned_run(request, run_id)
    manifest = read_json(directory / "manifest.json")
    if "preview_contract" in manifest:
        return redirect(reverse("recommendations:index") + "?run=" + manifest["request"]["parent_run_id"])
    project = manifest["request"].get("external_project", {})
    visitor_id(request)
    get_token(request)
    editor_url = reverse("project-editor", args=[project["id"]]) if project.get("id") else reverse("editor")
    return render(request, "recommendations/direct.html", {"demo": settings.INTERIOR_RECOMMENDER_DEMO,
                                                          "editor_url": editor_url})


@require_GET
def csrf(request):
    visitor_id(request)
    return JsonResponse({"csrf_token": get_token(request), "demo": settings.INTERIOR_RECOMMENDER_DEMO,
                         "mode": "LOCAL_DIRECT_SAMPLE", "bronze_verified": False})


@require_GET
def preview_client(request):
    from importlib.resources import files
    return FileResponse(files("interior_recommender.web").joinpath("ui/product-preview.mjs").open("rb"), content_type="text/javascript")


def form_json(request, name, default):
    raw = request.POST.get(name)
    return json.loads(raw) if raw else default


@require_POST
@api
def submit(request):
    photo = request.FILES.get("photo")
    sketch = request.FILES.get("sketch")
    if not photo or photo.size > 15 * 1024 * 1024 or (sketch and sketch.size > 15 * 1024 * 1024):
        return JsonResponse({"error": "15MiB 이하의 사진과 스케치를 보내주세요."}, status=400)
    document = form_json(request, "document", {"strokes": [], "items": []})
    if len(json.dumps(document).encode("utf-8")) > 2 * 1024 * 1024:
        raise ValueError("Canvas document too large")
    validate_document(document)
    text = "\n".join([request.POST.get("text", "").strip(), *[
        item.get("text", "") for item in document["items"] if item.get("kind") == "text"
    ]]).strip()
    if len(text) > 10000:
        raise ValueError("Text too long")
    project = form_json(request, "project", {})
    if not isinstance(project, dict):
        raise ValueError("Invalid project")
    raw_photo = photo.read()
    access = actor(request)
    metadata = {}
    if project.get("id"):
        source = owned_project(request, str(UUID(str(project["id"]))))
        if type(project.get("revision")) is not int or source.revision != project["revision"]:
            return JsonResponse({"error": "작업이 변경되었습니다. 최신 입력으로 다시 시도해 주세요."}, status=409)
        # Bind the uploaded drawing to the actual editor photo, including crop/EXIF.
        if not source.photo:
            raise ValueError("Project photo required")
        with source.photo.open("rb") as stream:
            if stream.read() != raw_photo:
                raise ValueError("Project photo mismatch")
        metadata = {"id": str(source.id), "revision": source.revision, "width": source.width, "height": source.height}
        metadata["style_preferences"] = source.request_context.get("style_preferences", [])
        metadata["style_policy"] = "color-first-v2"
        access = {"project_id": str(source.id)}
    metadata["sketchon_access"] = access
    owner = engine_owner(access)
    run_id, job = submissions.submit(raw_photo, sketch.read() if sketch else None, text, document,
                             form_json(request, "requested_region", None), owner, request.POST["request_id"], metadata)
    return JsonResponse({"run_id": run_id, "job": job,
                         "state_path": reverse("recommendations:state", args=[run_id]),
                         "page_url": reverse("recommendations:index") + "?run=" + run_id}, status=202)


@require_GET
@api
def state(request, run_id):
    _, owner = owned_run(request, run_id)
    result = services.reader().state(str(run_id), owner)
    job = RecommendationJob.objects.filter(pk=run_id, scope=scope()).first()
    if job:
        result["job"] = queue.job_dto(job)
    elif result.get("job", {}).get("status") == "RUNNING":
        result["job"] = {**result["job"], "status": "FAILED",
                         "error": "이전 실행 방식의 작업입니다. 입력 수정 후 새 추천을 요청해 주세요."}
    result["retry_supported"] = job is not None
    result["demo"] = settings.INTERIOR_RECOMMENDER_DEMO
    return JsonResponse(result)


@require_POST
@api
def submit_preview(request, run_id):
    directory, owner = owned_run(request, run_id)
    data = json.loads(request.body or b"{}")
    if not isinstance(data, dict):
        raise ValueError("JSON object required")
    preview_id, job = preview_submissions.submit(directory, owner, data.get("image_id"))
    return JsonResponse({"run_id": preview_id, "parent_run_id": str(run_id), "job": job,
                         "state_path": reverse("recommendations:state", args=[preview_id])}, status=202)


@require_POST
@api
def retry(request, run_id):
    _, owner = owned_run(request, run_id)
    body = json.loads(request.body or b"{}")
    if not isinstance(body, dict) or body.get("retry") is not True:
        raise ValueError("Explicit retry required")
    if not RecommendationJob.objects.filter(pk=run_id, scope=scope()).exists():
        raise ValueError("이전 파일 기반 실행은 보존됩니다. 입력 수정 후 새 요청을 제출해 주세요.")
    job = queue.retry(run_id)
    return JsonResponse({"run_id": str(run_id), "job": queue.job_dto(job)}, status=202)


@require_GET
@api
def product_link(request, run_id):
    _, owner = owned_run(request, run_id)
    url = services.reader().product_link(str(run_id), owner, request.GET.get("image_id"))
    return HttpResponseRedirect(url, headers={"Cache-Control": "private, no-store", "Referrer-Policy": "no-referrer"})


@require_GET
@api
def asset(request, run_id, name):
    directory, _ = owned_run(request, run_id)
    manifest = read_json(directory / "manifest.json")
    if name not in manifest["assets"] or not name.lower().endswith((".png", ".jpg", ".jpeg")):
        raise FileNotFoundError
    path = (directory / name).resolve()
    if not path.is_relative_to(directory):
        raise PermissionError
    content_type = "image/png" if path.suffix.lower() == ".png" else "image/jpeg"
    return FileResponse(path.open("rb"), content_type=content_type, headers={"Cache-Control": "private, no-store"})
