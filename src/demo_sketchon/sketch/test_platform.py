import hashlib
import io
import tempfile
from uuid import uuid4, uuid5
from unittest.mock import patch
from django.core.files.uploadedfile import SimpleUploadedFile
from django.test import Client, TestCase, override_settings
from PIL import Image
from .models import Project, RecommendationRun, Composition, SketchEvent
from .platform import validated_link, normalize_recommendations


class PlatformTests(TestCase):
    def setUp(self):
        self.media = tempfile.TemporaryDirectory(); self.addCleanup(self.media.cleanup)
        self.override = override_settings(MEDIA_ROOT=self.media.name)
        self.override.enable(); self.addCleanup(self.override.disable)
        stream = io.BytesIO(); Image.new("RGB", (320, 240), "#ddd5c3").save(stream, format="PNG")
        self.original = stream.getvalue()
        self.project = self.client.post("/api/projects/", {"title": "나의 공간", "photo": SimpleUploadedFile("room.png", self.original, content_type="image/png")}).json()

    def generate(self, **extra):
        return self.client.post(f"/api/projects/{self.project['id']}/scene-generations/", {"request_id": str(uuid4()),
            "revision": self.project["revision"], "document": {"strokes": [], "items": []},
            "text_requirements": "크림색 소파를 배치해 주세요", **extra}, content_type="application/json")

    def recommendation(self, scene, request_id=None, client=None, object_id=None):
        return (client or self.client).post(f"/api/scene-objects/{object_id or scene['scene_objects'][0]['id']}/recommendations/",
            {"scene_generation_id": scene["result_id"], "request_id": request_id or str(uuid4()), "user_confirmed": False}, content_type="application/json")

    def test_upload_preserves_exact_original_separately_from_jpeg_derivative(self):
        project = Project.objects.get(id=self.project["id"])
        with project.original_photo.open("rb") as original: self.assertEqual(original.read(), self.original)
        self.assertEqual(project.original_checksum, hashlib.sha256(self.original).hexdigest())
        with project.photo.open("rb") as derivative: self.assertNotEqual(derivative.read(), self.original)
        self.assertEqual(Client().get(self.project["photo_url"]).status_code, 404)

    def test_text_only_generation_preserves_input_and_context_and_is_idempotent(self):
        request_id = str(uuid4()); first = self.generate(request_id=request_id)
        self.assertEqual(first.status_code, 201, first.content)
        second = self.generate(request_id=request_id)
        self.assertEqual(second.status_code, 200)
        self.assertEqual(first.json()["result_id"], second.json()["result_id"])
        result = first.json(); data = self.client.get(result["data_url"]).json()
        self.assertEqual(data["input_document"], {"strokes": [], "items": []})
        self.assertIn("소파", data["generation_context"]["text_requirements"])
        self.assertEqual(result["scene_objects"][0]["classification_basis"], "DEMO_ASSUMPTION")
        self.assertEqual(self.generate(request_id=request_id, text_requirements="다른 요구").status_code, 400)

    def test_invalid_requirements_do_not_lock_or_create_result(self):
        for extra in ({"text_requirements": []}, {"text_requirements": "x"*1001}, {"style_preferences": ["unsupported"]}):
            self.assertEqual(self.generate(**extra).status_code, 400)
        self.assertFalse(Project.objects.get(id=self.project["id"]).locked)
        self.assertEqual(Composition.objects.count(), 0)

    def test_recommendation_is_owned_idempotent_and_not_a_generation(self):
        scene = self.generate().json(); request_id = str(uuid4())
        with patch("sketch.platform.DemoRecommendationProvider.recommend", wraps=__import__("sketch.platform", fromlist=["DemoRecommendationProvider"]).DemoRecommendationProvider().recommend) as provider:
            first = self.recommendation(scene, request_id); second = self.recommendation(scene, request_id)
            self.assertEqual(first.status_code, 201, first.content); self.assertEqual(second.status_code, 200)
            self.assertEqual(first.json()["items"], second.json()["items"]); self.assertEqual(provider.call_count, 1)
        self.assertEqual(Composition.objects.count(), 1); self.assertEqual(RecommendationRun.objects.count(), 1)
        self.assertTrue(all(item["price"] is None and not item["link_available"] for item in first.json()["items"]))
        self.assertEqual(self.recommendation(scene, client=Client()).status_code, 404)
        self.assertEqual(Client().get(f"/api/jobs/{request_id}/").status_code, 404)

    def test_provider_failure_keeps_scene_and_same_request_can_retry(self):
        scene = self.generate().json(); request_id = str(uuid4())
        with self.assertLogs("sketch.platform_views", level="ERROR"), patch("sketch.platform.DemoRecommendationProvider.recommend", side_effect=TimeoutError("offline")):
            self.assertEqual(self.recommendation(scene, request_id).status_code, 503)
        self.assertEqual(self.client.get(scene["page_url"]).status_code, 200)
        self.assertEqual(self.recommendation(scene, request_id).status_code, 201)
        self.assertEqual(Composition.objects.count(), 1)

    def test_moving_object_invalidates_old_crop_and_keeps_old_recommendation(self):
        scene = self.generate().json(); first = self.recommendation(scene).json()
        placements = [{"id": row["id"], "x": .1, "y": .2, "width": .3, "height": .2} for row in scene["placements"]]
        next_scene = self.client.post(f"/api/results/{scene['result_id']}/placements/", {"version": 0, "placements": placements}, content_type="application/json").json()
        self.assertNotEqual(next_scene["scene_objects"][0]["id"], scene["scene_objects"][0]["id"])
        self.assertEqual(self.recommendation(scene).status_code, 409)
        self.assertEqual(self.recommendation(next_scene).status_code, 201)
        self.assertEqual(RecommendationRun.objects.count(), 2)

    def test_impressions_are_deduplicated_and_response_does_not_count_as_view(self):
        scene = self.generate().json(); run = self.recommendation(scene).json(); item = run["items"][0]
        self.assertEqual(SketchEvent.objects.filter(event_type="PRODUCT_IMPRESSION").count(), 0)
        path = f"/api/recommendation-items/{item['id']}/events/"
        payload = {"recommendation_run_id": run["recommendation_run_id"], "event_type": "PRODUCT_IMPRESSION", "event_id": str(uuid4())}
        for _ in range(3): self.assertEqual(self.client.post(path, payload, content_type="application/json").status_code, 200)
        self.assertEqual(SketchEvent.objects.filter(event_type="PRODUCT_IMPRESSION").count(), 1)
        payload.update(event_type="PRODUCT_SELECTED", event_id=str(uuid4()))
        self.assertEqual(self.client.post(path, payload, content_type="application/json").status_code, 200)
        self.assertEqual(Client().post(path, payload, content_type="application/json").status_code, 404)
        self.assertEqual(self.client.post(f"/api/recommendation-items/{item['id']}/link/", {"recommendation_run_id": run["recommendation_run_id"], "event_id": str(uuid4())}, content_type="application/json").status_code, 409)

    def test_unsafe_link_and_media_schemes_are_rejected(self):
        for value in ("http://prod.danawa.com/info/?pcode=1", "https://prod.danawa.com.evil.test/", "javascript:alert(1)", "https://user@prod.danawa.com/", "https://prod.danawa.com:8080/"):
            with self.assertRaises(ValueError): validated_link(value)
        self.assertEqual(validated_link("https://prod.danawa.com/info/?pcode=1"), "https://prod.danawa.com/info/?pcode=1")
        with self.assertRaises(ValueError): normalize_recommendations({"mode":"mock", "items":[{"name":"sample", "image_url":"javascript:alert(1)"}]}, uuid4())

    def test_photo_event_accepts_upload_size_without_relaxing_general_number_limits(self):
        from .validation import events
        for size in (2_180_238, 10 * 1024 * 1024):
            self.assertEqual(len(events([{"event_id":str(uuid4()), "event_type":"photo.selected", "payload":{"bytes":size, "type":"image/png"}}])), 1)
        with self.assertRaises(ValueError): events([{"event_id":str(uuid4()), "event_type":"button.clicked", "payload":{"number":2_180_238}}])

    def test_malformed_preference_types_are_input_errors(self):
        for value in ([{}], [[]], [None], [42], None, "크림 패브릭"):
            with self.subTest(value=value):
                self.assertEqual(self.generate(style_preferences=value).status_code, 400)
        self.assertEqual(Composition.objects.count(), 0)

    def test_expired_recommendation_is_reclaimed_with_same_id(self):
        from django.utils import timezone
        from datetime import timedelta
        scene = self.generate().json(); request_id = str(uuid4())
        self.recommendation(scene, request_id)
        old_token = uuid4()
        RecommendationRun.objects.filter(id=request_id).update(status="RUNNING", items=[], lease_token=old_token, lease_expires_at=timezone.now()-timedelta(seconds=1))
        response = self.recommendation(scene, request_id)
        self.assertEqual(response.status_code, 201, response.content)
        run = RecommendationRun.objects.get(id=request_id)
        self.assertEqual(run.attempt_count, 2); self.assertNotEqual(run.lease_token, old_token)
        self.assertEqual(run.status, "SUCCEEDED"); self.assertEqual(RecommendationRun.objects.count(), 1)

    def test_active_recommendation_is_not_dispatched_twice(self):
        from django.utils import timezone
        from datetime import timedelta
        scene = self.generate().json(); request_id = str(uuid4()); self.recommendation(scene, request_id)
        RecommendationRun.objects.filter(id=request_id).update(status="RUNNING", lease_token=uuid4(), lease_expires_at=timezone.now()+timedelta(seconds=60))
        with patch("sketch.platform.DemoRecommendationProvider.recommend") as provider:
            self.assertEqual(self.recommendation(scene, request_id).status_code, 202)
            provider.assert_not_called()

    def test_job_lookup_marks_an_abandoned_attempt_retryable(self):
        from django.utils import timezone
        from datetime import timedelta
        scene = self.generate().json(); request_id = str(uuid4()); self.recommendation(scene, request_id)
        RecommendationRun.objects.filter(id=request_id).update(status="RUNNING", lease_token=None, lease_expires_at=None, created_at=timezone.now()-timedelta(minutes=10))
        self.assertEqual(self.client.get(f"/api/jobs/{request_id}/").json()["status"], "FAILED")
        self.assertEqual(self.recommendation(scene, request_id).status_code, 201)

    def test_late_attempt_cannot_overwrite_a_new_owner(self):
        from sketch.platform import DemoRecommendationProvider
        scene = self.generate().json(); request_id = str(uuid4()); self.recommendation(scene, request_id)
        winning_items = RecommendationRun.objects.get(id=request_id).items
        winning_items[0]["name"] = "winning result"
        RecommendationRun.objects.filter(id=request_id).update(status="FAILED")
        original = DemoRecommendationProvider().recommend
        def delayed(**kwargs):
            RecommendationRun.objects.filter(id=request_id).update(lease_token=uuid4(), status="SUCCEEDED", items=winning_items)
            return original(**kwargs)
        with patch("sketch.platform.DemoRecommendationProvider.recommend", side_effect=delayed):
            self.assertEqual(self.recommendation(scene, request_id).status_code, 202)
        self.assertEqual(RecommendationRun.objects.get(id=request_id).items[0]["name"], "winning result")

    def test_pre_update_request_hash_still_reuses_the_original_result(self):
        import json
        scene = self.generate().json(); request_id = str(uuid4()); self.recommendation(scene, request_id)
        obj = dict(scene["scene_objects"][0]); obj.pop("user_corrected", None); obj["preview_url"] = obj["preview_url"].split("?", 1)[0]
        legacy = hashlib.sha256(json.dumps({"object":obj,"user_confirmed":False},sort_keys=True).encode()).hexdigest()
        RecommendationRun.objects.filter(id=request_id).update(input_hash=legacy)
        with patch("sketch.platform.DemoRecommendationProvider.recommend") as provider:
            self.assertEqual(self.recommendation(scene, request_id).status_code, 200); provider.assert_not_called()

    def test_search_correction_preserves_image_history_and_old_recommendation(self):
        from .models import SceneObjectRevision
        scene = self.generate().json(); self.recommendation(scene)
        original = Composition.objects.get(id=scene["result_id"]); image_name = original.image.name
        payload = {"scene_generation_id":scene["result_id"],"request_id":str(uuid4()),"category_id":"bed","bbox":{"x":.1,"y":.2,"width":.4,"height":.3}}
        path = f"/api/scene-objects/{scene['scene_objects'][0]['id']}/revisions/"
        first = self.client.post(path,payload,content_type="application/json")
        self.assertEqual(first.status_code, 201, first.content)
        updated = first.json(); obj = updated["scene_objects"][0]
        self.assertEqual(obj["category_id"],"bed"); self.assertEqual(obj["bbox"],payload["bbox"])
        self.assertNotEqual(obj["id"],scene["scene_objects"][0]["id"])
        self.assertEqual(Composition.objects.get(id=original.id).image.name,image_name)
        self.assertEqual(Composition.objects.count(),1); self.assertEqual(RecommendationRun.objects.count(),1)
        self.assertEqual(self.client.post(path,payload,content_type="application/json").status_code,200)
        self.assertEqual(SceneObjectRevision.objects.count(),1)
        self.assertEqual(self.recommendation(scene).status_code,409)
        self.assertEqual(self.recommendation(updated).status_code,201)
        self.assertEqual(Client().post(path,payload,content_type="application/json").status_code,404)
        payload["request_id"]=str(uuid4()); payload["bbox"]["width"]=1
        self.assertEqual(self.client.post(path,payload,content_type="application/json").status_code,400)
        self.assertEqual(SceneObjectRevision.objects.count(),1)

    def test_material_choice_and_sample_images_have_visible_variants(self):
        from .mock_composition import compose
        scene = self.generate(style_preferences=["세이지 그린"]).json()
        self.assertEqual(scene["placements"][0]["variant"],"sage")
        project = Project.objects.get(id=self.project["id"])
        cream,_ = compose(project,{"strokes":[],"items":[]},"소파",["크림 패브릭"])
        sage,_ = compose(project,{"strokes":[],"items":[]},"소파",["세이지 그린"])
        self.assertNotEqual(hashlib.sha256(cream).digest(),hashlib.sha256(sage).digest())
        run=self.recommendation(scene).json(); urls=[item["image_url"] for item in run["items"]]
        self.assertEqual(len(set(urls)),3)
        hashes=[]
        for url in urls:
            response=self.client.get(url); self.assertEqual(response.status_code,200)
            hashes.append(hashlib.sha256(response.content).digest())
        self.assertEqual(len(set(hashes)),3)
        self.assertEqual(Client().get(urls[0]).status_code,404)
