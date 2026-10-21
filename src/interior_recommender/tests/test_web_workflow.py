import io
import json
import time
from uuid import uuid4

import pytest
from PIL import Image, ImageDraw
from django.conf import settings as django_settings
from django.core.files.uploadedfile import SimpleUploadedFile
from django.test import Client, override_settings

from interior_recommender.assets import read_json
from interior_recommender.web.__main__ import configure
from interior_recommender.web.workflow import Studio
from interior_recommender.web import views
from interior_recommender.tests.test_pipeline import image_bytes


def test_read_json_retries_windows_replacement(tmp_path, monkeypatch):
    from pathlib import Path
    target = tmp_path / "state.json"
    target.write_text('{"ok": true}', encoding="utf-8")
    original = Path.read_text
    attempts = []

    def read(path, *args, **kwargs):
        attempts.append(path)
        if len(attempts) == 1:
            raise PermissionError("Windows replacement in progress")
        return original(path, *args, **kwargs)

    monkeypatch.setattr(Path, "read_text", read)
    assert read_json(target) == {"ok": True}
    assert len(attempts) == 2


def test_roompick_bridge_rebuilds_csrf_parsed_upload(web, monkeypatch):
    import httpx
    from django.test import RequestFactory
    from interior_recommender.web.roompick_bridge import bridge
    monkeypatch.setenv("MVP_BRIDGE_TOKEN", "test-secret")
    request = RequestFactory().post("/ai-mvp/api/runs", {
        "photo": SimpleUploadedFile("room.png", b"photo-bytes", content_type="image/png"),
        "text": "의자 배치", "request_id": str(uuid4()),
    })
    request.session = {}
    assert request.POST["text"] == "의자 배치"  # CSRF middleware parses this first.
    captured = {}

    def send(method, url, **kwargs):
        captured.update(kwargs)
        return httpx.Response(202, json={"run_id": "test"})

    monkeypatch.setattr(httpx, "request", send)
    assert bridge(request, "api/runs").status_code == 202
    assert captured["files"][0][1][1] == b"photo-bytes"
    assert captured["data"]["text"] == ["의자 배치"]
    assert "Content-Type" not in captured["headers"]
    assert captured["headers"]["Authorization"] == "Bearer test-secret"


@pytest.fixture
def web(tmp_path, monkeypatch):
    if not django_settings.configured:
        configure(tmp_path / "django")
    root = tmp_path / "web"
    service = Studio(root, demo=True)
    from interior_recommender.schemas import Template
    service.add_template(image_bytes(), Template(id="chair-demo", version=1, category_code="CHAIR",
                         image="unused.png", review_status="APPROVED", active=True, provenance="test fixture"))
    monkeypatch.setattr(views, "_studio", service)
    with override_settings(WEB_OUTPUT_ROOT=root, WEB_DEMO=True, WEB_BRIDGE_TOKEN="test-bridge-secret",
                           ALLOWED_HOSTS=["testserver", "127.0.0.1"]):
        client = Client(enforce_csrf_checks=True)
        assert client.get("/").status_code == 200
        yield client, service
    service.pool.shutdown(wait=True)


def post(client, url, data, *, json_body=False):
    kwargs = {"HTTP_X_CSRFTOKEN": client.cookies["csrftoken"].value}
    if json_body:
        return client.post(url, json.dumps(data), content_type="application/json", **kwargs)
    return client.post(url, data, **kwargs)


def wait(service, run_id, owner):
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        state = service.state(run_id, owner)
        if state.get("job", {}).get("status") != "RUNNING":
            assert state["job"]["status"] == "SUCCEEDED", json.dumps(state["job"], ensure_ascii=True)
            return state
        time.sleep(.01)
    pytest.fail("Background operation did not finish")


def submit(client, request_id=None, **extra):
    return post(client, "/api/runs", {"photo": SimpleUploadedFile("room.png", image_bytes(), content_type="image/png"),
                "request_id": request_id or str(uuid4()), "text": "의자 하나를 왼쪽에 배치해주세요.",
                "document": json.dumps({"strokes": [], "items": []}), **extra})


def test_canvas_to_plan_selection_preview_generate_compare(web):
    client, service = web
    response = submit(client, project=json.dumps({"id": str(uuid4()), "revision": 3, "width": 100, "height": 80}))
    assert response.status_code == 202
    original_id = response.json()["run_id"]
    owner = client.session["mvp_owner"]
    original = wait(service, original_id, owner)
    assert original["status"] == "NEEDS_TEMPLATE_SELECTION"
    assert "generate" not in original["stages"]
    assert original["candidates"][0]["template_id"] == "chair-demo"
    response = post(client, f"/api/runs/{original_id}/selection", {"template_id": "chair-demo"}, json_body=True)
    assert response.status_code == 202
    chosen_id = response.json()["run_id"]
    assert chosen_id != original_id
    chosen = wait(service, chosen_id, owner)
    assert chosen["status"] == "READY_TO_GENERATE"
    assert chosen["parent_run_id"] == original_id and chosen["revision_no"] == 2
    assert "generate" not in chosen["stages"]
    assert [ref["role"] for ref in chosen["preview"]["images"]] == ["room_image", "template"]
    assert owner not in chosen["preview"]["prompt"]
    response = post(client, f"/api/runs/{chosen_id}/actions/generate", {}, json_body=True)
    assert response.status_code == 202
    result = wait(service, chosen_id, owner)
    assert result["status"] == "COMPLETED"
    assert result["comparison"]["objects"][0]["change_type"] == "ADDED"
    assert result["comparison"]["objects"][0]["eligible_for_selection"]
    assert client.get(f"/assets/{chosen_id}/generated.png").status_code == 200
    count = len(result["stages"]["generate"]["attempts"])
    post(client, f"/api/runs/{chosen_id}/actions/generate", {}, json_body=True)
    assert len(service.state(chosen_id, owner)["stages"]["generate"]["attempts"]) == count


def test_canvas_document_text_and_transparent_sketch_are_preserved(web):
    client, service = web
    original = Image.new("RGBA", (100, 80), (0, 0, 0, 0))
    ImageDraw.Draw(original).line((10, 10, 50, 50), fill="black", width=3)
    stream = io.BytesIO(); original.save(stream, format="PNG")
    raw = stream.getvalue()
    drawing = {"strokes": [{"tool": "pen", "points": [{"x": .1, "y": .1}, {"x": .5, "y": .5}]}],
               "items": [{"kind": "text", "text": "나무 의자", "x": .2, "y": .3}]}
    response = submit(client, text="", document=json.dumps(drawing),
                      sketch=SimpleUploadedFile("sketch.png", raw, content_type="image/png"))
    assert response.status_code == 202
    run_id = response.json()["run_id"]
    wait(service, run_id, client.session["mvp_owner"])
    directory = service.directory(run_id, client.session["mvp_owner"])
    manifest = read_json(directory / "manifest.json")
    assert manifest["request"]["text"] == "나무 의자"
    assert manifest["request"]["canvas_document"] == drawing
    assert (directory / "inputs/original-sketch.bin").read_bytes() == raw
    with Image.open(directory / "inputs/sketch_image.png") as image:
        assert image.getpixel((0, 0)) == (255, 255, 255)
        assert image.getpixel((20, 20)) == (0, 0, 0)


def test_same_event_is_idempotent_and_different_body_is_rejected(web):
    client, service = web
    event = str(uuid4())
    first = submit(client, event).json()["run_id"]
    assert submit(client, event).json()["run_id"] == first
    assert submit(client, event, text="다른 요청").status_code == 400


def test_photo_only_is_rejected_and_csrf_enforced(web):
    client, service = web
    assert submit(client, text="").status_code == 400
    assert client.post("/api/runs", {}).status_code == 403
    assert post(client, "/api/runs", {"text": "chair", "request_id": str(uuid4())}).status_code == 400


def test_runs_are_scoped_to_browser_and_bridge_is_authenticated(web):
    client, service = web
    run_id = submit(client).json()["run_id"]
    stranger = Client(); stranger.get("/")
    assert stranger.get(f"/api/runs/{run_id}").status_code == 403
    assert client.get("/api/csrf", REMOTE_ADDR="192.168.0.44").status_code == 403
    assert client.get("/api/csrf", REMOTE_ADDR="192.168.0.44", HTTP_AUTHORIZATION="Bearer wrong",
                      HTTP_X_MVP_OWNER=str(uuid4())).status_code == 403
    assert client.get("/api/csrf", REMOTE_ADDR="192.168.0.44", HTTP_AUTHORIZATION="Bearer test-bridge-secret",
                      HTTP_X_MVP_OWNER=str(uuid4())).status_code == 200


def test_webp_photo_and_missing_template_handled(web):
    client, service = web
    image = Image.new("RGB", (100, 80), "white")
    data = io.BytesIO(); image.save(data, format="WEBP")
    response = submit(client, text="소파", photo=SimpleUploadedFile("room.webp", data.getvalue(), content_type="image/webp"))
    assert response.status_code == 202
    state = wait(service, response.json()["run_id"], client.session["mvp_owner"])
    assert state["status"] == "TEMPLATE_UNAVAILABLE" and "generate" not in state["stages"]
