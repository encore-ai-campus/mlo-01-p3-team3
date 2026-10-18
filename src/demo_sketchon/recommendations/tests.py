"""Exercise the mounted Django API with the approved local catalog, without paid calls."""
import io
import json
import tempfile
import time
from pathlib import Path
from unittest.mock import patch
from uuid import uuid4

from django.conf import settings
from django.contrib.auth import get_user_model
from django.core.files.uploadedfile import SimpleUploadedFile
from django.test import Client, TestCase, override_settings
from PIL import Image
from interior_recommender.application.direct import DirectDemoAdapter, DirectStudio
from sketch.models import Project
from . import services

BASE = "/interior-recommand/"


def picture(size=(64, 48)):
    output = io.BytesIO()
    Image.new("RGB", size, "white").save(output, "PNG")
    return output.getvalue()


class IntegrationTests(TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix=".recommendation-test-", dir=settings.BASE_DIR)
        self.addCleanup(self.temp.cleanup)
        self.override = override_settings(
            INTERIOR_RECOMMENDER_OUTPUT_ROOT=Path(self.temp.name) / "runs-output",
            INTERIOR_RECOMMENDER_DEMO=True,
            MEDIA_ROOT=Path(self.temp.name) / "media",
            SKETCH_RAW_ROOT=Path(self.temp.name) / "raw",
        )
        self.override.enable()
        self.addCleanup(self.override.disable)
        self.calls = []
        calls = self.calls

        class OfflineAdapter(DirectDemoAdapter):
            def call(self, kind, *args, **kwargs):
                calls.append(kind)
                return super().call(kind, *args, **kwargs)

        self.engine = DirectStudio(settings.INTERIOR_RECOMMENDER_OUTPUT_ROOT,
                                   products=settings.INTERIOR_RECOMMENDER_PRODUCTS,
                                   demo=True, adapter_factory=OfflineAdapter)
        self.addCleanup(lambda: self.engine.pool.shutdown(wait=True))
        self.patch = patch.object(services, "_studio", self.engine)
        self.patch.start()
        self.addCleanup(self.patch.stop)

    def submit(self, client=None, request_id=None, **extra):
        data = {"photo": SimpleUploadedFile("room.png", picture(), "image/png"),
                "text": "의자", "request_id": request_id or str(uuid4())}
        data.update(extra)
        return (client or self.client).post(BASE + "api/runs", data)

    def completed(self, client, run_id):
        for _ in range(150):
            response = client.get(BASE + "api/runs/" + run_id)
            self.assertEqual(response.status_code, 200, response.content)
            state = response.json()
            if state.get("job", {}).get("status") != "RUNNING":
                self.assertEqual(state["job"]["status"], "SUCCEEDED", state)
                return state
            time.sleep(.02)
        self.fail("Recommendation did not complete")

    def test_sketchon_page_existing_pages_and_namespaced_assets(self):
        response = self.client.get(BASE)
        self.assertRedirects(response, "/editor/", fetch_redirect_response=False)
        self.assertEqual(self.calls, [])
        run = self.submit().json()["run_id"]
        response = self.client.get(BASE, {"run":run})
        self.assertContains(response, "스케치·온 SketchOn")
        self.assertContains(response, "sketchon-logo-approved.png")
        self.assertContains(response, "recommendations/direct.mjs")
        self.assertNotContains(response, "RoomPick")
        self.assertNotContains(response, 'id="request-form"')
        self.assertNotContains(response, 'type="file"')
        self.assertContains(response, 'id="requested-text"')
        self.completed(self.client, run)
        self.assertEqual(self.client.get("/").status_code, 200)
        self.assertContains(self.client.get("/editor/"), "공간 만들기")
        self.assertEqual(len(self.engine.index.products), 295)
        self.assertEqual(self.engine.index.dimension, 768)

    def test_submit_result_assets_links_and_retry_reuse_successful_calls(self):
        request_id = str(uuid4())
        response = self.submit(request_id=request_id)
        self.assertEqual(response.status_code, 202, response.content)
        run = response.json()["run_id"]
        self.assertTrue(response.json()["page_url"].startswith(BASE))
        state = self.completed(self.client, run)
        self.assertEqual(state["flow"], "direct")
        self.assertFalse(state["bronze_verified"])
        items = state["recommendations"]["items"]
        self.assertTrue(items)
        image = self.client.get(BASE + f"assets/{run}/{items[0]['image']}")
        self.assertEqual(image.status_code, 200)
        self.assertEqual(image["Content-Type"], "image/png")
        image.close()
        link = self.client.get(BASE + f"api/runs/{run}/product-link", {"image_id": items[0]["image_id"]})
        self.assertEqual(link.status_code, 302)
        self.assertTrue(link["Location"].startswith("https://prod.danawa.com/info/"))
        self.assertEqual(link["Referrer-Policy"], "no-referrer")
        self.assertEqual(self.submit(request_id=request_id).json()["run_id"], run)
        retry = self.client.post(BASE + f"api/runs/{run}/actions/prepare", {"retry": True}, content_type="application/json")
        self.assertEqual(retry.status_code, 202)
        self.completed(self.client, run)
        self.assertEqual(self.calls, ["direct_context", "embed_text"])
        self.assertEqual(self.submit(request_id=request_id, text="다른 입력").status_code, 400)

    def test_foreign_sessions_and_forged_bridge_cannot_read_or_retry(self):
        run = self.submit().json()["run_id"]
        state = self.completed(self.client, run)
        item = state["recommendations"]["items"][0]
        stranger = Client()
        for path in [f"api/runs/{run}", f"assets/{run}/{item['image']}", f"api/runs/{run}/product-link?image_id={item['image_id']}"]:
            self.assertEqual(stranger.get(BASE + path, HTTP_AUTHORIZATION="Bearer fake", HTTP_X_MVP_OWNER="fake").status_code, 403)
        self.assertEqual(stranger.post(BASE + f"api/runs/{run}/actions/prepare", {"retry": True}, content_type="application/json").status_code, 403)

    def test_csrf_uses_sketchon_cookie_and_is_required(self):
        client = Client(enforce_csrf_checks=True)
        self.assertEqual(self.submit(client).status_code, 403)
        token = client.get(BASE + "api/csrf").json()["csrf_token"]
        self.assertIn("roompick_csrftoken", client.cookies)
        self.assertEqual(self.submit(client, HTTP_X_CSRFTOKEN=token).status_code, 403)  # form field is not a header
        response = client.post(BASE + "api/runs", {"photo": SimpleUploadedFile("room.png", picture()), "text":"의자", "request_id":str(uuid4())}, HTTP_X_CSRFTOKEN=token)
        self.assertEqual(response.status_code, 202, response.content)
        self.completed(client, response.json()["run_id"])

    def test_invalid_inputs_and_hidden_legacy_routes(self):
        self.assertEqual(self.submit(text="").status_code, 400)
        self.assertEqual(self.submit(request_id="bad-id").status_code, 400)
        self.assertEqual(self.submit(document="[]").status_code, 400)
        self.assertEqual(self.submit(project="[]").status_code, 400)
        self.assertEqual(self.submit(project=json.dumps({"id":"invalid-project"})).status_code, 400)
        self.assertEqual(self.submit(sketch=SimpleUploadedFile("sketch.png", picture((32,32)))).status_code, 400)
        self.assertEqual(self.client.get(BASE + f"api/runs/{uuid4()}").status_code, 404)
        self.assertEqual(self.client.post(BASE + f"api/runs/{uuid4()}/actions/generate").status_code, 404)
        self.assertEqual(self.client.get(BASE + "api/templates").status_code, 404)

    def test_only_registered_images_and_returned_product_links_are_served(self):
        run = self.submit().json()["run_id"]
        self.completed(self.client, run)
        for name in ["manifest.json", "inputs/original-room.bin", "../manifest.json", "products/not-registered.png"]:
            self.assertEqual(self.client.get(BASE + f"assets/{run}/{name}").status_code, 404)
        self.assertEqual(self.client.get(BASE + f"api/runs/{run}/product-link", {"image_id":"other", "next":"https://evil.test/"}).status_code, 404)

    def test_standalone_member_run_follows_account_across_sessions(self):
        user = get_user_model().objects.create_user(username="first")
        self.client.force_login(user)
        run = self.submit().json()["run_id"]
        self.completed(self.client, run)
        other = Client(); other.force_login(user)
        self.assertEqual(other.get(BASE + f"api/runs/{run}").status_code, 200)
        self.client.logout()
        self.assertEqual(self.client.get(BASE + f"api/runs/{run}").status_code, 403)
        other.force_login(get_user_model().objects.create_user(username="second"))
        self.assertEqual(other.get(BASE + f"api/runs/{run}").status_code, 403)

    def test_canvas_run_uses_project_access_after_guest_claim_and_account_switch(self):
        dto = self.client.post("/api/projects/", {"photo":SimpleUploadedFile("room.png", picture())}).json()
        project = Project.objects.get(pk=dto["id"])
        with project.photo.open("rb") as stream:
            photo = stream.read()
        metadata = json.dumps({"id":str(project.id), "revision":project.revision})
        response = self.submit(project=metadata, photo=SimpleUploadedFile("room.jpg", photo))
        self.assertEqual(response.status_code, 202, response.content)
        run = response.json()["run_id"]
        self.completed(self.client, run)
        self.assertEqual(self.submit(project=metadata).status_code, 400)  # mismatched photo
        self.assertEqual(self.submit(project=json.dumps({"id":str(project.id),"revision":99})).status_code, 409)
        user = get_user_model().objects.create_user(username="owner")
        project.account=user; project.save(update_fields=["account"])
        member = Client(); member.force_login(user)
        self.assertEqual(member.get(BASE + f"api/runs/{run}").status_code, 200)
        self.assertEqual(self.client.get(BASE + f"api/runs/{run}").status_code, 404)
        member.force_login(get_user_model().objects.create_user(username="another"))
        self.assertEqual(member.get(BASE + f"api/runs/{run}").status_code, 404)

    def test_catalog_configuration_error_does_not_expose_paths(self):
        with patch.object(services, "_studio", None), override_settings(INTERIOR_RECOMMENDER_PRODUCTS=Path(self.temp.name)/"missing.json"):
            response = self.submit()
        self.assertEqual(response.status_code, 503)
        self.assertNotIn(self.temp.name, response.content.decode())

    def test_optional_style_is_separate_from_hard_text_and_used_only_in_query(self):
        seen = {}

        class StyleAdapter(DirectDemoAdapter):
            def call(self, kind, prompt, *args, **kwargs):
                seen[kind] = prompt
                return super().call(kind, prompt, *args, **kwargs)

        self.engine.factory = StyleAdapter
        dto = self.client.post("/api/projects/", {"photo":SimpleUploadedFile("room.png", picture())}).json()
        project = Project.objects.get(pk=dto["id"])
        project.request_context = {"text_requirements":"방과 어울리는 의자를 추천해주세요", "style_preferences":["오크 우드"]}
        project.save(update_fields=["request_context"])
        with project.photo.open("rb") as stream:
            photo = stream.read()
        response = self.submit(text="방과 어울리는 의자를 추천해주세요",
                               photo=SimpleUploadedFile("room.jpg", photo),
                               project=json.dumps({"id":str(project.id),"revision":project.revision}))
        self.assertEqual(response.status_code, 202, response.content)
        state = self.completed(self.client, response.json()["run_id"])
        self.assertNotIn("오크 우드", seen["direct_context"])
        self.assertNotIn("오크 우드", seen["embed_text"])
        self.assertIn("light brown beige color", seen["embed_text"])
        self.assertEqual(state["text"], "방과 어울리는 의자를 추천해주세요")
        self.assertEqual(state["external_project"]["style_policy"], "color-first-v2")
        self.assertEqual(state["external_project"]["style_preferences"], ["오크 우드"])
        self.assertEqual(state["context"]["required"], [])
        self.assertIsNone(state["context"]["preferred_color"])  # Preserve the AI receipt.
        self.assertEqual(state["recommendations"]["color_preference"],
                         {"value":"brown", "source":"style", "style_color":"brown"})
        self.assertTrue(state["recommendations"]["items"])
        for item in state["recommendations"]["items"]:
            self.assertEqual(item["metadata"]["category_code"], "CHAIR")
            self.assertIsNone(item["metadata"]["material"])
            self.assertNotIn("required:0:material", item["checks"])

    def test_explicit_material_uses_query_without_blocking_missing_material(self):
        seen = {}

        class MaterialAdapter(DirectDemoAdapter):
            def call(self, kind, prompt, *args, **kwargs):
                seen[kind] = prompt
                result = super().call(kind, prompt, *args, **kwargs)
                if kind == "direct_context":
                    payload = json.loads(result["payload"]["text"])
                    payload["required"] = [{"field":"material", "operator":"EQ", "value":"wood", "source_quote":"원목"}]
                    result["payload"]["text"] = json.dumps(payload)
                return result

        self.engine.factory = MaterialAdapter
        response = self.submit(text="원목 의자를 추천해주세요")
        self.assertEqual(response.status_code, 202, response.content)
        state = self.completed(self.client, response.json()["run_id"])
        self.assertIn("원목", seen["embed_text"])
        self.assertEqual(state["status"], "COMPLETED")
        self.assertEqual(state["recommendations"]["ranking_version"], "weighted-content-v2")
        self.assertEqual(len(state["recommendations"]["items"]), 6)
        for item in state["recommendations"]["items"]:
            self.assertEqual(item["metadata"]["category_code"], "CHAIR")
            self.assertIsNone(item["metadata"]["material"])
            self.assertEqual(item["preference_checks"], {"required:0:material":"UNKNOWN"})
            self.assertNotIn("required:0:material", item["checks"])
