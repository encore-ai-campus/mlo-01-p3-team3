import copy
import io
import json
import tempfile
from unittest.mock import patch
from uuid import uuid4

from django.core.files.uploadedfile import SimpleUploadedFile
from django.test import Client, TestCase, override_settings
from PIL import Image


class SketchTests(TestCase):
    def setUp(self):
        self.media = tempfile.TemporaryDirectory()
        self.addCleanup(self.media.cleanup)
        self.override = override_settings(MEDIA_ROOT=self.media.name)
        self.override.enable()
        self.addCleanup(self.override.disable)

    def create(self, client=None, photo=None):
        data = {"title": "우리 방"}
        if photo:
            data["photo"] = photo
        response = (client or self.client).post("/api/projects/", data)
        self.assertEqual(response.status_code, 201, response.content)
        return response.json()

    def document(self):
        return {"strokes": [{"tool": "pen", "color": "#245847", "width": 0.008,
                             "category": "plant", "pointer_type": "touch",
                             "points": [{"x": 0.5, "y": 0.5, "pressure": 0.5}]}],
                "items": [{"id": str(uuid4()), "product_id": "plant-01", "x": 0.3, "y": 0.4, "width": 0.2, "height": 0.3}]}

    def save(self, project, document=None, **extra):
        return self.client.post(f"/api/projects/{project['id']}/", {
            "title": "우리 방", "revision": project["revision"],
            "document": document if document is not None else self.document(), "events": [], **extra,
        }, content_type="application/json")

    def test_normalized_drawing_and_items_survive_reload(self):
        project = self.create()
        document = self.document()
        response = self.save(project, document)
        self.assertEqual(response.status_code, 200, response.content)
        reloaded = self.client.get(f"/api/projects/{project['id']}/").json()
        self.assertEqual(reloaded["document"], document)
        self.assertEqual(reloaded["revision"], 1)
        self.assertEqual(len(self.client.get("/api/projects/").json()["projects"]), 1)

    def test_partial_erase_and_group_metadata_are_saved_and_validated(self):
        from sketch import validation
        doc = self.document()
        stroke = doc["strokes"][0]
        stroke["id"] = str(uuid4())
        stroke["group_id"] = str(uuid4())
        stroke["cuts"] = [{"tool": "eraser", "color": "#ffffff", "width": .01, "pointer_type": "mouse",
                           "points": [{"x": .5, "y": .5, "pressure": .5}]}]
        doc["items"] = [{"id": str(uuid4()), "kind": "rectangle", "color": "#245847", "x": .2, "y": .2,
                         "width": .3, "height": .3, "stroke_width": .02, "flip_x": False, "flip_y": False,
                         "group_id": stroke["group_id"], "cuts": copy.deepcopy(stroke["cuts"])}]
        self.assertEqual(validation.document(doc), doc)
        project = self.create()
        self.assertEqual(self.save(project, doc).status_code, 200)
        self.assertEqual(self.client.get(f"/api/projects/{project['id']}/").json()["document"], doc)
        for mutate in (lambda d: d["strokes"][0].update(group_id="bad"),
                       lambda d: d["items"][0]["cuts"][0].update(tool="pen"),
                       lambda d: d["items"][0]["cuts"][0]["points"][0].update(x=2),
                       lambda d: d["items"][0]["cuts"][0].update(cuts=[])):
            invalid = copy.deepcopy(doc); mutate(invalid)
            with self.assertRaises(ValueError): validation.document(invalid)

    def test_oversize_upload_has_explicit_limit_message(self):
        response = self.client.post("/api/projects/", {"photo": SimpleUploadedFile("large.png", b"x" * (10 * 1024 * 1024 + 1), content_type="image/png")})
        self.assertEqual(response.status_code, 400)
        self.assertIn("10MB", response.json()["error"])

    def test_pen_erase_masks_do_not_erase_later_overlapping_strokes(self):
        from sketch.mock_composition import visible_ink
        stroke = {"tool": "pen", "width": .01, "points": [{"x": .2, "y": .2}]}
        stroke["cuts"] = [{"tool": "eraser", "width": .04, "points": [{"x": .2, "y": .2}]}]
        self.assertIsNone(visible_ink({"strokes": [stroke], "items": []}, 100, 100))
        doc = {"strokes": [stroke, {"tool": "pen", "width": .01, "points": [{"x": .2, "y": .2}]}], "items": []}
        self.assertIsNotNone(visible_ink(doc, 100, 100))

    def test_another_session_cannot_read_write_or_view_photo(self):
        project = self.create()
        stranger = Client()
        for path in (f"/api/projects/{project['id']}/", f"/api/projects/{project['id']}/photo/"):
            self.assertEqual(stranger.get(path).status_code, 404)
        self.assertEqual(stranger.post(f"/api/projects/{project['id']}/", {}, content_type="application/json").status_code, 404)
        self.assertEqual(stranger.get("/api/projects/").json()["projects"], [])

    def test_invalid_coordinates_do_not_change_saved_document(self):
        project = self.create()
        for value in (-0.1, 1.1, True, float("nan"), "0.5"):
            document = self.document()
            document["strokes"][0]["points"][0]["x"] = value
            self.assertEqual(self.save(project, document).status_code, 400)
        self.assertEqual(self.client.get(f"/api/projects/{project['id']}/").json()["revision"], 0)

    def test_stale_revision_cannot_overwrite_new_save(self):
        project = self.create()
        self.assertEqual(self.save(project).status_code, 200)
        self.assertEqual(self.save(project, {"strokes": [], "items": []}).status_code, 409)
        self.assertEqual(len(self.client.get(f"/api/projects/{project['id']}/").json()["document"]["strokes"]), 1)

    def test_invalid_image_and_spoofed_image_are_rejected(self):
        for name in ("bad.txt", "fake.jpg"):
            response = self.client.post("/api/projects/", {"title": "방", "photo": SimpleUploadedFile(name, b"not an image", content_type="image/jpeg")})
            self.assertEqual(response.status_code, 400)

    def test_jpg_with_mpo_metadata_uploads_first_frame_as_private_jpeg(self):
        buffer = io.BytesIO()
        Image.new("RGB", (64, 48), "blue").save(buffer, format="MPO", save_all=True,
            append_images=[Image.new("RGB", (64, 48), "red")])
        project = self.create(photo=SimpleUploadedFile("phone.jpg", buffer.getvalue(), content_type="image/jpeg"))
        self.assertEqual((project["width"], project["height"]), (64, 48))
        response = self.client.get(project["photo_url"])
        with Image.open(io.BytesIO(b"".join(response.streaming_content))) as image:
            self.assertEqual(image.format, "JPEG")
            self.assertEqual(getattr(image, "n_frames", 1), 1)
            red, green, blue = image.getpixel((32, 24))
            self.assertGreater(blue, 240)
            self.assertLess(red, 15)
        self.assertEqual(Client().get(project["photo_url"]).status_code, 404)

    def test_resolution_limit_is_distinct_from_file_size_and_format_errors(self):
        with patch("sketch.views.Image.open") as opened:
            image = opened.return_value.__enter__.return_value
            image.format, image.width, image.height = "JPEG", 5001, 5000
            response = self.client.post("/api/projects/", {"photo": SimpleUploadedFile("room.jpg", b"small", content_type="image/jpeg")})
            self.assertEqual(response.status_code, 400)
            self.assertIn("해상도", response.json()["error"])
            self.assertIn("5001×5000", response.json()["error"])
            image.load.assert_not_called()

    def test_photo_is_decoded_reencoded_and_private(self):
        buffer = io.BytesIO()
        Image.new("RGB", (320, 240), "white").save(buffer, format="PNG")
        project = self.create(photo=SimpleUploadedFile("room.png", buffer.getvalue(), content_type="image/png"))
        self.assertEqual((project["width"], project["height"]), (320, 240))
        response = self.client.get(project["photo_url"])
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response["Content-Type"], "image/jpeg")
        b"".join(response.streaming_content)
        self.assertEqual(Client().get(project["photo_url"]).status_code, 404)

    def test_save_events_are_atomic_and_deduplicated(self):
        from sketch.models import SketchEvent
        project = self.create()
        event = {"event_id": str(uuid4()), "event_type": "stroke.completed", "payload": {"pointer_type": "touch", "point_count": 1}}
        self.assertEqual(self.save(project, events=[event]).status_code, 200)
        latest = self.client.get(f"/api/projects/{project['id']}/").json()
        self.assertEqual(self.save(latest, events=[event]).status_code, 200)
        self.assertEqual(SketchEvent.objects.filter(event_id=event["event_id"]).count(), 1)
        bad_event = copy.deepcopy(event)
        bad_event["event_type"] = "invalid.event"
        latest = self.client.get(f"/api/projects/{project['id']}/").json()
        self.assertEqual(self.save(latest, events=[bad_event]).status_code, 400)
        self.assertEqual(self.client.get(f"/api/projects/{project['id']}/").json()["revision"], 2)

    def test_csrf_is_required(self):
        protected = Client(enforce_csrf_checks=True)
        self.assertEqual(protected.post("/api/projects/", {"title": "방"}).status_code, 403)
        self.assertEqual(protected.get("/").status_code, 200)
        token = protected.cookies["roompick_csrftoken"].value
        self.assertEqual(protected.post("/api/projects/", {"title": "방"}, HTTP_X_CSRFTOKEN=token).status_code, 201)

    def test_malformed_json_and_invalid_item_are_rejected(self):
        project = self.create()
        path = f"/api/projects/{project['id']}/"
        for body in ("[1]", "{broken"):
            self.assertEqual(self.client.post(path, body, content_type="application/json").status_code, 400)
        document = self.document()
        document["items"][0]["product_id"] = "nonexistent"
        self.assertEqual(self.save(project, document).status_code, 400)

    def test_actions_are_collected_without_saving_drawing(self):
        from sketch.models import SketchEvent
        self.client.get("/")
        visitor = self.client.session["sketch_owner"]
        project = self.create()
        event = {"event_id": str(uuid4()), "event_type": "stroke.completed", "payload": {"stroke": self.document()["strokes"][0]}}
        path = f"/api/projects/{project['id']}/events/"
        for _ in range(2):
            self.assertEqual(self.client.post(path, {"events": [event]}, content_type="application/json").status_code, 200)
        recorded = SketchEvent.objects.get(event_id=event["event_id"])
        self.assertEqual(recorded.payload["stroke"]["points"][0]["x"], .5)
        self.assertEqual(recorded.project.owner, visitor)
        self.assertEqual(self.client.get(f"/api/projects/{project['id']}/").json()["revision"], 0)
        self.assertEqual(Client().post(path, {"events": [event]}, content_type="application/json").status_code, 404)

    def test_beacon_form_records_button_click(self):
        from sketch.models import SketchEvent
        project = self.create()
        event = {"event_id": str(uuid4()), "event_type": "button.clicked", "payload": {"button": "undo"}}
        response = self.client.post(f"/api/projects/{project['id']}/events/", {"events": json.dumps([event])})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(SketchEvent.objects.get(event_id=event["event_id"]).payload["button"], "undo")

    def annotations(self):
        return {"strokes": [], "items": [{"id": str(uuid4()), "kind": "text", "text": "화분", "color": "#245847", "font_size": .04,
                    "x": .5, "y": .5, "width": .2, "height": .08}]}

    def photo_project(self):
        buffer = io.BytesIO()
        Image.new("RGB", (320, 240), "#eeeeee").save(buffer, format="PNG")
        return self.create(photo=SimpleUploadedFile("room.png", buffer.getvalue(), content_type="image/png"))

    def search(self, project, document=None, request_id=None):
        return self.client.post(f"/api/projects/{project['id']}/search/", {
            "document": document if document is not None else self.annotations(),
            "revision": project["revision"], "request_id": request_id or str(uuid4()),
        }, content_type="application/json")

    def test_text_and_shapes_are_accepted_without_category_hint(self):
        project = self.create()
        document = self.annotations()
        for kind in ("rectangle", "ellipse", "line", "arrow"):
            document["items"].append({"id": str(uuid4()), "kind": kind, "color": "#245847", "stroke_width": .008,
                "x": .1, "y": .1, "width": .2, "height": .2, "flip_x": False, "flip_y": True})
        self.assertEqual(self.save(project, document).status_code, 200)

    def test_search_creates_private_composite_and_result_snapshot(self):
        project = self.photo_project()
        document = self.annotations()
        response = self.search(project, document)
        self.assertEqual(response.status_code, 201, response.content)
        result = response.json()
        self.assertEqual(result["mode"], "mock")
        self.assertEqual(result["placements"][0]["product_id"], "plant-01")
        image_response = self.client.get(result["image_url"])
        image = Image.open(io.BytesIO(b"".join(image_response.streaming_content)))
        self.assertEqual(image.size, (320, 240))
        self.assertGreater(len(image.getcolors(maxcolors=100000)), 1)
        data = self.client.get(result["data_url"]).json()
        self.assertEqual(data["input_document"], document)
        self.assertEqual(data["result_id"], result["result_id"])
        self.assertEqual(Client().get(result["image_url"]).status_code, 404)
        self.assertEqual(Client().get(result["data_url"]).status_code, 404)
        latest = self.client.get(f"/api/projects/{project['id']}/").json()
        self.assertEqual(self.save(latest, {"strokes": [], "items": []}).status_code, 409)
        self.assertEqual(self.client.get(result["data_url"]).json()["input_document"], document)

    def test_search_requires_photo_and_annotations(self):
        self.assertEqual(self.search(self.create()).status_code, 400)
        project = self.photo_project()
        self.assertEqual(self.search(project, {"strokes": [], "items": []}).status_code, 400)
        erased = self.document()
        erased["items"] = []
        erased["strokes"][0]["tool"] = "eraser"
        self.assertEqual(self.search(project, erased).status_code, 400)

    def test_search_is_idempotent_and_rejects_stale_revision(self):
        project = self.photo_project()
        document, request_id = self.annotations(), str(uuid4())
        first = self.search(project, document, request_id)
        self.assertEqual(first.status_code, 201, first.content)
        second = self.search(project, document, request_id)
        self.assertEqual(second.status_code, 200)
        self.assertEqual(first.json()["result_id"], second.json()["result_id"])
        self.assertEqual(self.search(project, document).status_code, 409)

    def test_demo_ui_does_not_prompt_furniture_category_or_camera(self):
        html = self.client.get("/editor/").content.decode()
        self.assertNotIn('id="category"', html)
        self.assertNotIn('id="camera"', html)
        self.assertNotIn('id="products"', html)
        self.assertIn('id="search"', html)
        self.assertIn('id="inline-text"', html)

    def test_home_editor_and_result_are_separate_pages(self):
        home = self.client.get("/").content.decode()
        self.assertIn('href="/editor/"', home)
        self.assertNotIn('id="surface"', home)
        editor = self.client.get("/editor/").content.decode()
        self.assertIn('id="toolbar"', editor)
        self.assertIn('id="inline-text"', editor)
        project = self.photo_project()
        result = self.search(project).json()
        page = self.client.get(result["page_url"])
        self.assertContains(page, "님이 원하시는")
        self.assertNotContains(page, 'id="pen"')
        self.assertNotContains(page, 'id="upload"')
        self.assertEqual(Client().get(result["page_url"]).status_code, 404)

    def test_search_locks_editing_and_redraw_keeps_photo_with_empty_document(self):
        project = self.photo_project()
        result = self.search(project).json()
        latest = self.client.get(f"/api/projects/{project['id']}/").json()
        self.assertEqual(self.save(latest).status_code, 409)
        self.assertEqual(self.search(latest).status_code, 409)
        self.assertEqual(self.client.get(f"/editor/{project['id']}/").status_code, 302)
        response = self.client.post(f"/api/results/{result['result_id']}/redraw/")
        self.assertEqual(response.status_code, 201)
        new = response.json()
        self.assertNotEqual(new["id"], project["id"])
        self.assertEqual(new["document"], {"strokes": [], "items": []})
        self.assertIsNotNone(new["photo_url"])
        photo = self.client.get(new["photo_url"])
        self.assertEqual(photo.status_code, 200)
        b"".join(photo.streaming_content)
        self.assertEqual(self.client.get(new["editor_url"]).status_code, 200)
        self.assertEqual(Client().post(f"/api/results/{result['result_id']}/redraw/").status_code, 404)

    def test_result_moves_resize_are_saved_to_download_and_versioned(self):
        project = self.photo_project()
        result = self.search(project).json()
        placements = [{"id": row["id"], "x": .1, "y": .1, "width": .3, "height": .4} for row in result["placements"]]
        path = f"/api/results/{result['result_id']}/placements/"
        response = self.client.post(path, {"version": 0, "placements": placements}, content_type="application/json")
        self.assertEqual(response.status_code, 200, response.content)
        updated = self.client.get(result["data_url"]).json()
        self.assertEqual(updated["placements"][0]["x"], .1)
        self.assertEqual(updated["placements"][0]["height"], .4)
        self.assertEqual(updated["version"], 1)
        self.assertEqual(self.client.post(path, {"version": 0, "placements": placements}, content_type="application/json").status_code, 409)
        placements[0]["x"] = 1.2
        self.assertEqual(self.client.post(path, {"version": 1, "placements": placements}, content_type="application/json").status_code, 400)
        self.assertEqual(Client().post(path, {}, content_type="application/json").status_code, 404)
        sprite = self.client.get(f"/api/results/{result['result_id']}/sprites/{result['placements'][0]['id']}/")
        self.assertEqual(sprite.status_code, 200)

    def test_old_image_cleanup_failure_does_not_remove_saved_result(self):
        from sketch.models import Composition
        result = self.search(self.photo_project()).json()
        composition = Composition.objects.get(id=result["result_id"])
        placements = [{"id": row["id"], "x": .2, "y": .2, "width": .2, "height": .2} for row in result["placements"]]
        with self.assertLogs("sketch.views", level="WARNING"), patch.object(composition.image.storage, "delete", side_effect=OSError("download still using old image")):
            response = self.client.post(f"/api/results/{result['result_id']}/placements/", {"version": 0, "placements": placements}, content_type="application/json")
        self.assertEqual(response.status_code, 200)
        image = self.client.get(response.json()["image_url"])
        self.assertEqual(image.status_code, 200)
        b"".join(image.streaming_content)

    def test_whole_sketch_is_one_product_not_one_per_element(self):
        project = self.photo_project()
        document = self.annotations()
        document["strokes"] = self.document()["strokes"] * 3
        document["items"].extend({"id": str(uuid4()), "kind": kind, "color": "#245847", "stroke_width": .008,
            "x": .1, "y": .1, "width": .3, "height": .3, "flip_x": False, "flip_y": False} for kind in ("rectangle", "ellipse", "line"))
        response = self.search(project, document)
        self.assertEqual(response.status_code, 201, response.content)
        self.assertEqual(len(response.json()["placements"]), 1)
        self.assertEqual(response.json()["placements"][0]["product_id"], "plant-01")

    def test_crop_changes_photo_dimensions_and_keeps_visitor_id(self):
        project = self.photo_project()
        visitor = self.client.session["sketch_owner"]
        response = self.client.post(f"/api/projects/{project['id']}/crop/", {"x": .25, "y": .25, "width": .5, "height": .5}, content_type="application/json")
        self.assertEqual(response.status_code, 201, response.content)
        cropped = response.json()
        self.assertEqual((cropped["width"], cropped["height"]), (160, 120))
        self.assertEqual(cropped["document"], {"strokes": [], "items": []})
        self.assertEqual(self.client.session["sketch_owner"], visitor)
        self.assertEqual(Client().post(f"/api/projects/{project['id']}/crop/", {}, content_type="application/json").status_code, 404)
        self.assertEqual(self.client.post(f"/api/projects/{project['id']}/crop/", {"x": .9, "y": 0, "width": .5, "height": .5}, content_type="application/json").status_code, 400)

    def test_redraw_restore_and_new_upload_keep_same_visitor(self):
        project = self.photo_project()
        document = self.annotations()
        result = self.search(project, document).json()
        visitor = self.client.session["sketch_owner"]
        for mode in ("blank", "sketch", "new"):
            response = self.client.post(f"/api/results/{result['result_id']}/redraw/", {"mode": mode}, content_type="application/json")
            self.assertEqual(response.status_code, 201)
            next_project = response.json()
            self.assertEqual(next_project["document"], document if mode == "sketch" else {"strokes": [], "items": []})
            self.assertEqual(bool(next_project["photo_url"]), mode != "new")
            self.client.get(next_project["editor_url"])
            self.assertEqual(self.client.session["sketch_owner"], visitor)
        self.client.get("/")
        self.client.get("/editor/")
        self.assertEqual(self.client.session["sketch_owner"], visitor)

    def test_fill_mask_is_validated_and_saved(self):
        project = self.create()
        fill = {"id": str(uuid4()), "kind": "fill", "color": "#ee3344", "x": .1, "y": .1, "width": .3, "height": .3,
            "parent_id": None, "mask_width": 3, "mask_height": 2, "runs": [[0, 0, 2], [1, 0, 2]]}
        self.assertEqual(self.save(project, {"strokes": [], "items": [fill]}).status_code, 200)
        latest = self.client.get(f"/api/projects/{project['id']}/").json()
        fill["runs"] = [[0, -1, 5]]
        self.assertEqual(self.save(latest, {"strokes": [], "items": [fill]}).status_code, 400)
