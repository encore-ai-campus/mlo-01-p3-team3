from uuid import uuid4

import pytest
from django.conf import settings
from django.test import Client, override_settings

from interior_recommender.product_links import comparison_url
from interior_recommender.assets import read_json, write_json
from interior_recommender.web import views
from interior_recommender.web.__main__ import configure
from interior_recommender.web.direct import DirectStudio
from interior_recommender.tests.test_pipeline import image_bytes
from interior_recommender.tests.test_web_workflow import wait


@pytest.mark.parametrize("url", [
    "https://prod.danawa.com/info/?pcode=2", "https://prod.danawa.com.evil.com/info/?pcode=1",
    "javascript:alert(1)", "http://prod.danawa.com/info/?pcode=1",
    "https://user@prod.danawa.com/info/?pcode=1", "https://prod.danawa.com/info/?pcode=1&pcode=2",
    "https://prod.danawa.com/info/?pcode=1\r\nLocation: https://evil.com",
])
def test_reject_unsafe_or_different_product_url(url):
    assert comparison_url(url, "danawa:1") is None


def test_owned_result_redirects_only_to_saved_product(tmp_path, monkeypatch):
    if not settings.configured:
        configure(tmp_path / "django")
    service = DirectStudio(tmp_path / "web", demo=True)
    monkeypatch.setattr(views, "_studio", service)
    url = "https://prod.danawa.com/info/?pcode=1"
    service.index.products[0].product_id = "danawa:1"
    service.index.products[0].comparison_url = url
    try:
        with override_settings(ALLOWED_HOSTS=["testserver"], WEB_BRIDGE_TOKEN=""):
            client = Client()
            session = client.session
            owner = str(uuid4())
            session["mvp_owner"] = owner
            session.save()
            client.cookies[settings.SESSION_COOKIE_NAME] = session.session_key
            run = service.submit(image_bytes(), None, "의자", [], None, owner, str(uuid4()))
            service.launch(run, owner, "prepare")
            result = wait(service, run, owner)
            item = next(i for i in result["recommendations"]["items"] if i["product_id"] == "danawa:1")
            path = f"/api/runs/{run}/product-link"
            response = client.get(path, {"image_id": item["image_id"], "next": "https://evil.com"})
            assert response.status_code == 302 and response["Location"] == url
            assert Client().get(path, {"image_id": item["image_id"]}).status_code == 403
            assert client.get(path, {"image_id": "missing"}).status_code == 404
            missing = next(i for i in result["recommendations"]["items"] if i["product_id"] != "danawa:1")
            assert client.get(path, {"image_id": missing["image_id"]}).status_code == 404
            directory = service.directory(run, owner)
            saved = read_json(directory / "recommendations.json")
            saved["items"][0]["name"] = "tampered"
            write_json(directory / "recommendations.json", saved)
            assert client.get(path, {"image_id": item["image_id"]}).status_code == 400
    finally:
        service.pool.shutdown(wait=True)


def test_bridge_preserves_only_allowed_product_redirects(tmp_path, monkeypatch):
    import httpx
    from django.test import RequestFactory
    from interior_recommender.web.roompick_bridge import bridge
    if not settings.configured:
        configure(tmp_path / "django")
    monkeypatch.setenv("MVP_BRIDGE_TOKEN", "test")
    request = RequestFactory().get("/ai-mvp/api/runs/test/product-link")
    request.session = {}
    for url, status in [("https://prod.danawa.com/info/?pcode=1", 302), ("https://evil.com/?pcode=1", 502)]:
        monkeypatch.setattr(httpx, "request", lambda *args, **kwargs: httpx.Response(302, headers={"Location": url}))
        response = bridge(request, "api/runs/test/product-link")
        assert response.status_code == status
        if status == 302:
            assert response["Location"] == url
