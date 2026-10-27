import copy
import json
import tempfile
from pathlib import Path
from uuid import uuid4

from django.test import Client, TestCase, override_settings

from .models import Project, SketchEvent


class CollectionTests(TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.raw_root = Path(self.directory.name) / "raw"
        settings = override_settings(SKETCH_RAW_ROOT=self.raw_root)
        settings.enable()
        self.addCleanup(settings.disable)
        self.writer = uuid4().hex
        session = self.client.session
        session["sketch_owner"] = self.writer
        session.save()
        self.project = Project.objects.create(owner=self.writer, title="수집 검증")

    def sample(self):
        stroke_id = str(uuid4())
        document = {"strokes": [{"id": stroke_id, "tool": "pen", "color": "#245847", "width": .008,
                                "pointer_type": "mouse", "points": [{"x": .2, "y": .3, "pressure": .5}]}], "items": []}
        return {"schema_version": "roompick-sketch-1.0.0", "sample_id": str(uuid4()),
                "project_id": str(self.project.id), "writer_id": self.writer,
                "created_at": "2026-10-13T00:00:00.000Z", "label_id": None,
                "label_status": "unverified", "quality_status": "pending",
                "coordinate_system": "normalized_xy_top_left_down", "time_origin": "first_pointerdown_ms",
                "canvas_width": 600, "canvas_height": 400, "device_pixel_ratio": 2,
                "project_width": 1200, "project_height": 800,
                "strokes": [{"stroke_id": stroke_id, "stroke_order": 0, "tool": "pen", "ink_hex": "#245847",
                             "stroke_width_css_px": 4.8, "canvas_width": 600, "canvas_height": 400,
                             "device_pixel_ratio": 2, "pointer_type": "mouse", "end_reason": "up",
                             "points": [{"x": .2, "y": .3, "t": 0, "event_type": "down", "source": "native", "in_bounds": True, "pressure": 0},
                                        {"x": 1.04, "y": .3, "t": 20, "event_type": "up", "source": "native", "in_bounds": False, "pressure": 0}]}],
                "operations": [{"op_id": str(uuid4()), "type": "stroke.completed", "t": 20,
                                "client_time": "2026-10-13T00:00:00.020Z", "target_stroke_ids": [stroke_id], "target_item_ids": [],
                                "document_patch": {"strokes": {"upsert": document["strokes"], "remove": [], "order": [stroke_id]}}}],
                "initial_document": {"strokes": [], "items": []}, "final_document": document,
                "final_active_stroke_ids": [stroke_id], "duration_ms": 20, "undo_count": 0,
                "submission_reason": "page_exit", "result_id": None}

    def test_collection_limit_is_reported_as_review_without_mutating_raw(self):
        sample = self.sample()
        sample["capture_status"] = "limited"
        sample["submission_reason"] = "capture_limit"
        response = self.submit(sample)
        self.assertEqual(response.status_code, 201)
        self.assertIn("capture_limit", response.json()["warnings"])

    def test_photo_sticker_is_replayed_as_item_without_fabricated_pen_strokes(self):
        sample = self.sample()
        item = {'id':str(uuid4()),'kind':'sticker','sticker_id':'sofa-cream',
                'x':.2,'y':.3,'width':.4,'height':.2}
        sample.update(strokes=[], final_active_stroke_ids=[], final_document={'strokes':[], 'items':[item]})
        sample['operations'] = [{'op_id':str(uuid4()),'type':'item.added','t':20,
            'client_time':'2026-10-13T00:00:00.020Z','target_stroke_ids':[], 'target_item_ids':[item['id']],
            'document_patch':{'items':{'upsert':[item], 'remove':[], 'order':[item['id']]}}}]
        response = self.submit(sample)
        self.assertEqual(response.status_code, 201, response.content)
        saved = json.loads((self.raw_root / f"{sample['sample_id']}.json").read_text(encoding='utf-8'))
        self.assertEqual(saved, sample)

    def submit(self, sample=None, client=None):
        return (client or self.client).post(f"/api/projects/{self.project.id}/samples/", sample or self.sample(), content_type="application/json")

    def test_raw_sample_retains_outside_points_without_changing_editor_state(self):
        sample = self.sample()
        response = self.submit(sample)
        self.assertEqual(response.status_code, 201, response.content)
        saved = json.loads((self.raw_root / f"{sample['sample_id']}.json").read_text(encoding="utf-8"))
        self.assertEqual(saved, sample)
        self.assertEqual(saved["strokes"][0]["points"][-1]["x"], 1.04)
        self.assertEqual(response.json()["quality_status"], "review")
        self.project.refresh_from_db()
        self.assertEqual(self.project.document, {"strokes": [], "items": []})
        self.assertEqual(self.project.revision, 0)
        self.assertFalse(self.project.locked)

    def test_identical_retry_is_idempotent_and_collision_never_overwrites(self):
        sample = self.sample()
        self.assertEqual(self.submit(sample).status_code, 201)
        target = self.raw_root / f"{sample['sample_id']}.json"
        original = target.read_bytes()
        response = self.submit(sample)
        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.json()["idempotent"])
        sample["submission_reason"] = "project_changed"
        self.assertEqual(self.submit(sample).status_code, 409)
        self.assertEqual(target.read_bytes(), original)
        self.assertEqual(len(list(self.raw_root.iterdir())), 1)

    def test_other_session_and_forged_writer_cannot_submit(self):
        self.assertEqual(self.submit(client=Client()).status_code, 404)
        sample = self.sample()
        sample["writer_id"] = uuid4().hex
        self.assertEqual(self.submit(sample).status_code, 422)
        self.assertFalse(self.raw_root.exists())

    def test_invalid_point_and_inconsistent_replay_are_rejected(self):
        cases = []
        sample = self.sample()
        sample["strokes"][0]["points"][-1]["t"] = -1
        cases.append(sample)
        sample = self.sample()
        sample["strokes"][0]["points"][-1]["in_bounds"] = True
        cases.append(sample)
        sample = self.sample()
        sample["final_document"]["strokes"][0]["points"][0]["x"] = .9
        sample["operations"] = []
        cases.append(sample)
        sample = self.sample()
        sample["final_active_stroke_ids"] = []
        cases.append(sample)
        for invalid in cases:
            with self.subTest(sample=invalid):
                self.assertEqual(self.submit(invalid).status_code, 422)
        self.assertFalse(self.raw_root.exists())

    def test_clear_and_undo_replay_preserve_deleted_raw_stroke(self):
        sample = self.sample()
        stroke_id = sample["strokes"][0]["stroke_id"]
        cleared = {"op_id": str(uuid4()), "type": "sketch.cleared", "t": 30,
                   "client_time": "2026-10-13T00:00:00.030Z", "target_stroke_ids": [stroke_id], "target_item_ids": [],
                   "document_patch": {"strokes": {"upsert": [], "remove": [stroke_id], "order": []}}}
        restored = copy.deepcopy(sample["operations"][0])
        restored.update(op_id=str(uuid4()), type="history.undo", t=40)
        sample["operations"] += [cleared, restored]
        sample["duration_ms"] = 40
        sample["undo_count"] = 1
        self.assertEqual(self.submit(sample).status_code, 201)
        saved = json.loads((self.raw_root / f"{sample['sample_id']}.json").read_text(encoding="utf-8"))
        self.assertEqual(len(saved["strokes"]), 1)
        self.assertEqual(saved["final_active_stroke_ids"], [stroke_id])

    def test_beacon_requires_csrf_and_strict_json_rejects_nan(self):
        protected = Client(enforce_csrf_checks=True)
        session = protected.session
        session["sketch_owner"] = self.writer
        session.save()
        url = f"/api/projects/{self.project.id}/samples/"
        self.assertEqual(protected.post(url, self.sample(), content_type="application/json").status_code, 403)
        self.client.get(f"/editor/{self.project.id}/")
        response = self.client.post(url, {"sample": json.dumps(self.sample()), "csrfmiddlewaretoken": self.client.cookies["roompick_csrftoken"].value})
        self.assertEqual(response.status_code, 201)
        invalid = json.dumps(self.sample()).replace('"duration_ms": 20', '"duration_ms": NaN')
        self.assertEqual(self.client.post(url, invalid, content_type="application/json").status_code, 400)
        self.assertEqual(self.client.post(url, "x" * (4 * 1024 * 1024 + 1), content_type="application/json").status_code, 413)

    def test_home_start_click_uses_existing_event_pipeline(self):
        response = self.client.get("/")
        self.assertIsNotNone(response.context.get("initial"), "초기 화면에 수집 대상 작업이 전달되지 않았습니다.")
        home_project = response.context["initial"]
        event = {"event_id": str(uuid4()), "event_type": "button.clicked", "payload": {"button": "start-sketch", "page": "home"}}
        result = self.client.post(f"/api/projects/{home_project['id']}/events/", {"events": [event]}, content_type="application/json")
        self.assertEqual(result.status_code, 200)
        self.assertEqual(SketchEvent.objects.get(event_id=event["event_id"]).payload["page"], "home")
