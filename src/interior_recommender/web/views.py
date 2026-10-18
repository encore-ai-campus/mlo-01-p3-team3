import functools
import json
import threading
from pathlib import Path
from uuid import UUID, uuid4

from django.conf import settings
from django.http import FileResponse, JsonResponse, HttpResponseRedirect
from django.middleware.csrf import get_token
from django.shortcuts import render
from django.views.decorators.http import require_GET, require_POST

from ..assets import read_json
from ..schemas import Template
from .workflow import Studio

_studio = None
_guard = threading.Lock()


def studio():
    global _studio
    with _guard:
        if _studio is None:
            if getattr(settings, "WEB_LEGACY_GENERATION", False):
                _studio = Studio(settings.WEB_OUTPUT_ROOT, settings.WEB_TEMPLATE_MANIFEST, settings.WEB_DEMO,
                                 danawa_index=getattr(settings, "WEB_DANAWA_INDEX", None))
            else:
                from ..application.direct import DirectStudio
                policy_path = getattr(settings, "WEB_RANKING_POLICY", None)
                _studio = DirectStudio(settings.WEB_OUTPUT_ROOT, products=getattr(settings, "WEB_PRODUCTS", None),
                                       danawa_index=getattr(settings, "WEB_DANAWA_INDEX", None), demo=settings.WEB_DEMO,
                                       policy=read_json(policy_path) if policy_path else None)
        return _studio


def owner(request):
    if hasattr(request, "bridge_owner"):
        return request.bridge_owner
    if "mvp_owner" not in request.session:
        request.session["mvp_owner"] = str(uuid4())
    return str(UUID(request.session["mvp_owner"]))


def api(view):
    @functools.wraps(view)
    def wrapped(request, *args, **kwargs):
        try:
            return view(request, *args, **kwargs)
        except PermissionError:
            return JsonResponse({"error": "이 실행에 접근할 수 없습니다."}, status=403)
        except FileNotFoundError:
            return JsonResponse({"error": "요청한 실행 또는 파일을 찾을 수 없습니다."}, status=404)
        except (ValueError, TypeError, KeyError, OSError):
            # User input errors have no request credentials, but avoid dumping validation inputs.
            return JsonResponse({"error": "요청 형식 또는 파일을 확인하세요. 사진과 스케치 크기·품목·실행 ID를 확인해 주세요."}, status=400)
    return wrapped


def body(request):
    value = json.loads(request.body or b"{}")
    if not isinstance(value, dict):
        raise ValueError("JSON object required")
    return value


def form_json(request, key, default=None):
    raw = request.POST.get(key)
    return json.loads(raw) if raw else default


@require_GET
def index(request):
    owner(request)
    get_token(request)
    return render(request, "direct.html" if getattr(studio(), "direct", False) else "studio.html", {"demo": settings.WEB_DEMO})


@require_GET
def csrf(request):
    owner(request)
    return JsonResponse({"csrf_token": get_token(request), "demo": settings.WEB_DEMO,
                         "mode": "LOCAL_SAMPLE", "bronze_verified": False})


@require_GET
@api
def catalog(request):
    owner(request)
    templates = studio().catalog()
    source = studio().danawa.info() if studio().danawa else {"source": "templates", "count": len(templates)}
    # Absolute source paths are internal and must not be exposed to the browser.
    return JsonResponse({"templates": [{**t, "image": f"catalog/{t['id']}"} for t in templates],
                         "demo": settings.WEB_DEMO, "reference_catalog": source})


@require_POST
@api
def register_template(request):
    owner(request)
    file = request.FILES.get("image")
    if not file or file.size > 15 * 1024 * 1024 or request.POST.get("reviewed") != "true":
        return JsonResponse({"error": "15MiB 이하의 템플릿 이미지와 검수 확인이 필요합니다."}, status=400)
    template = Template(id=str(uuid4()), version=1, category_code=request.POST["category_code"].strip(), image="upload.png",
                        review_status="APPROVED", active=True, provenance=request.POST["provenance"].strip(),
                        attributes=form_json(request, "attributes", {}))
    studio().add_template(file.read(), template)
    return JsonResponse({"template_id": template.id}, status=201)


@require_POST
@api
def submit(request):
    room = request.FILES.get("photo") or request.FILES.get("room")
    sketch = request.FILES.get("sketch")
    if not room or room.size > 15 * 1024 * 1024 or (sketch and sketch.size > 15 * 1024 * 1024):
        return JsonResponse({"error": "15MiB 이하의 방 사진을 보내주세요."}, status=400)
    document = form_json(request, "document", {"strokes": [], "items": []})
    if not isinstance(document, dict) or not isinstance(document.get("strokes", []), list) or not isinstance(document.get("items", []), list):
        raise ValueError("Invalid canvas document")
    if len(json.dumps(document)) > 2 * 1024 * 1024:
        return JsonResponse({"error": "스케치 원문은 2MiB 이하로 보내주세요."}, status=400)
    project = form_json(request, "project", {})
    # Do not fetch project.photo_url server-side; bytes must be attached to this request.
    project = {key: project[key] for key in ("id", "revision", "width", "height") if key in project}
    text_items = [str(item.get("text", "")) for item in document.get("items", []) if isinstance(item, dict) and item.get("kind") == "text"]
    text = "\n".join([request.POST.get("text", "").strip(), *text_items]).strip()
    if len(text) > 10000:
        raise ValueError("Text too long")
    raw_room = room.read()
    run_id = studio().submit(raw_room, sketch.read() if sketch else None, text, document.get("strokes", []),
                             form_json(request, "requested_region"), owner(request), request.POST["request_id"],
                             canvas_document=document, project_metadata=project)
    job = studio().launch(run_id, owner(request), "prepare")
    return JsonResponse({"run_id": run_id, "job": job, "state_path": f"api/runs/{run_id}"}, status=202)


@require_GET
@api
def state(request, run_id):
    return JsonResponse(studio().state(str(run_id), owner(request)))


@require_POST
@api
def submit_preview(request, run_id):
    target = studio()
    if not getattr(target, "direct", False):
        raise FileNotFoundError
    run = target.submit_preview(str(run_id), owner(request), body(request).get("image_id"))
    job = target.launch(run, owner(request), "prepare")
    return JsonResponse({"run_id": run, "parent_run_id": str(run_id), "job": job,
                         "state_path": f"api/runs/{run}"}, status=202)


@require_GET
@api
def product_link(request, run_id):
    target = studio()
    if not getattr(target, "direct", False):
        raise FileNotFoundError
    response = HttpResponseRedirect(target.product_link(str(run_id), owner(request), request.GET.get("image_id")))
    response["Cache-Control"] = "private, no-store"
    response["Referrer-Policy"] = "no-referrer"
    return response


@require_POST
@api
def selection(request, run_id):
    data = body(request)
    new_id = studio().clone_selection(str(run_id), owner(request), data.get("template_id"), data.get("category_code"))
    job = studio().launch(new_id, owner(request), "prepare")
    return JsonResponse({"run_id": new_id, "job": job}, status=202)


@require_POST
@api
def action(request, run_id, name):
    data = body(request)
    job = studio().launch(str(run_id), owner(request), name, retry=bool(data.get("retry", False)))
    return JsonResponse({"run_id": str(run_id), "job": job}, status=202)


@require_GET
@api
def asset(request, run_id, name):
    directory = studio().directory(str(run_id), owner(request))
    manifest = read_json(directory / "manifest.json")
    if name not in manifest["assets"] or not name.lower().endswith((".png", ".jpg", ".jpeg")):
        raise FileNotFoundError
    target = (directory / name).resolve()
    if directory not in target.parents:
        raise PermissionError
    return FileResponse(target.open("rb"), content_type="image/png", headers={"Cache-Control": "private, no-store"})


@require_GET
@api
def catalog_image(request, template_id):
    owner(request)
    template = next((t for t in studio().catalog() if t["id"] == str(template_id)), None)
    if template is None:
        raise FileNotFoundError
    return FileResponse((studio().root / template["image"]).open("rb"), content_type="image/png")


@require_GET
def static_file(request, name):
    allowed = {"studio.css": "text/css", "studio.js": "text/javascript", "direct.js": "text/javascript", "roompick-client.mjs": "text/javascript", "product-preview.mjs": "text/javascript"}
    if name not in allowed:
        return JsonResponse({"error": "Not found"}, status=404)
    return FileResponse((Path(__file__).parent / "ui" / name).open("rb"), content_type=allowed[name])
