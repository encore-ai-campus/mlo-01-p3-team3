import io
import tempfile
from uuid import uuid4
from unittest.mock import patch

from django.core.files.uploadedfile import SimpleUploadedFile
from django.test import TestCase, override_settings
from PIL import Image


class ExtensionTests(TestCase):
    def setUp(self):
        self.media = tempfile.TemporaryDirectory()
        self.addCleanup(self.media.cleanup)
        self.override = override_settings(MEDIA_ROOT=self.media.name)
        self.override.enable()
        self.addCleanup(self.override.disable)
        stream = io.BytesIO()
        Image.new('RGB', (320, 240), '#ddd5c3').save(stream, format='PNG')
        self.project = self.client.post('/api/projects/', {'photo': SimpleUploadedFile('room.png', stream.getvalue(), content_type='image/png')}).json()

    def generate(self, **extra):
        return self.client.post(f"/api/projects/{self.project['id']}/search/", {
            'request_id': str(uuid4()), 'revision': self.project['revision'],
            'document': {'strokes': [], 'items': []}, 'text_requirements': '크림색 소파', **extra,
        }, content_type='application/json')

    def test_description_generates_without_marks_and_exposes_original_baseline(self):
        response = self.generate()
        self.assertEqual(response.status_code, 201, response.content)
        data = response.json()
        self.assertEqual(data['initial_placements'], data['placements'])

    def test_project_serialization_has_empty_request_context(self):
        self.assertEqual(self.project.get('request_context'), {})

    def test_only_one_style_can_be_selected(self):
        self.assertEqual(self.generate(style_preferences=['크림 패브릭', '세이지 그린']).status_code, 400)

    def test_saved_context_survives_crop_and_sketch_redraw_only(self):
        context = {'text_requirements': '소파를 창가에', 'style_preferences': ['세이지 그린']}
        saved = self.client.post(f"/api/projects/{self.project['id']}/", {
            'revision': 0, 'title': '공간', 'document': {'strokes': [], 'items': []}, 'request_context': context,
        }, content_type='application/json')
        self.assertEqual(saved.status_code, 200, saved.content)
        self.assertEqual(saved.json()['request_context'], context)
        cropped = self.client.post(f"/api/projects/{self.project['id']}/crop/", {
            'x': .1, 'y': .1, 'width': .8, 'height': .8,
        }, content_type='application/json').json()
        self.assertEqual(cropped['request_context'], context)
        self.project = saved.json()
        scene = self.generate(**context).json()
        for mode in ['sketch', 'blank', 'new']:
            response = self.client.post(f"/api/results/{scene['result_id']}/redraw/", {'mode': mode}, content_type='application/json')
            self.assertEqual(response.json()['request_context'], context if mode == 'sketch' else {})

    def test_placement_move_preserves_initial_placements(self):
        scene = self.generate().json()
        moved = [{key: row[key] for key in ['id', 'x', 'y', 'width', 'height']} for row in scene['placements']]
        moved[0]['x'] = 0
        response = self.client.post(f"/api/results/{scene['result_id']}/placements/", {'version': 0, 'placements': moved}, content_type='application/json')
        self.assertEqual(response.status_code, 200, response.content)
        self.assertEqual(response.json()['initial_placements'], scene['placements'])
        self.assertNotEqual(response.json()['placements'], scene['placements'])

    def test_legacy_sprite_keeps_vector_artwork(self):
        from .models import Composition
        from .mock_composition import furniture, render_placements
        from .catalog import PRODUCTS
        scene = self.generate().json()
        result = Composition.objects.get(id=scene['result_id'])
        for row in result.placements:
            row.pop('variant', None)
            row.pop('render_style', None)
        result.save(update_fields=['placements'])
        row = result.placements[0]
        product = next(product for product in PRODUCTS if product['id'] == row['product_id'])
        stream = io.BytesIO()
        furniture(product['category'], product['color']).save(stream, format='PNG')
        response = self.client.get(f"/api/results/{result.id}/sprites/{row['id']}/")
        self.assertEqual(response.content, stream.getvalue())
        expected_image = render_placements(result.project, result.placements)
        moved = [{key: item[key] for key in ['id', 'x', 'y', 'width', 'height']} for item in result.placements]
        self.client.post(f"/api/results/{result.id}/placements/", {'version': 0, 'placements': moved}, content_type='application/json')
        response = self.client.get(f"/api/results/{result.id}/image/")
        self.assertEqual(b''.join(response.streaming_content), expected_image)

    def test_correction_cannot_commit_after_concurrent_move(self):
        from .models import Composition, SceneObjectRevision
        from . import platform
        scene = self.generate().json()
        original = platform.scene_objects
        def moved(result):
            objects = original(result)
            Composition.objects.filter(id=result.id).update(version=result.version + 1)
            return objects
        with patch('sketch.platform.scene_objects', side_effect=moved):
            response = self.client.post(f"/api/scene-objects/{scene['scene_objects'][0]['id']}/revisions/", {
                'scene_generation_id': scene['result_id'], 'request_id': str(uuid4()),
                'category_id': 'bed', 'bbox': {'x': .1, 'y': .1, 'width': .3, 'height': .3},
            }, content_type='application/json')
        self.assertEqual(response.status_code, 409, response.content)
        self.assertEqual(SceneObjectRevision.objects.count(), 0)

    def test_recommendation_cannot_start_with_concurrently_moved_object(self):
        from .models import Composition, RecommendationRun
        from . import platform
        scene = self.generate().json()
        original = platform.scene_objects
        def moved(result):
            objects = original(result)
            Composition.objects.filter(id=result.id).update(version=result.version + 1)
            return objects
        with patch('sketch.platform.scene_objects', side_effect=moved):
            response = self.client.post(f"/api/scene-objects/{scene['scene_objects'][0]['id']}/recommendations/", {
                'scene_generation_id': scene['result_id'], 'request_id': str(uuid4()),
            }, content_type='application/json')
        self.assertEqual(response.status_code, 409, response.content)
        self.assertEqual(RecommendationRun.objects.count(), 0)

    def test_invalid_saved_context_is_rejected_without_mutation(self):
        for context in [None, [], {'text_requirements': 'a' * 1001}, {'style_preferences': ['bad']}, {'unexpected': True}]:
            response = self.client.post(f"/api/projects/{self.project['id']}/", {
                'revision': 0, 'title': '공간', 'document': {'strokes': [], 'items': []}, 'request_context': context,
            }, content_type='application/json')
            self.assertEqual(response.status_code, 400, response.content)
        self.assertEqual(self.client.get(f"/api/projects/{self.project['id']}/").json()['revision'], 0)

    def test_generation_requires_photo_and_nonempty_input(self):
        self.assertEqual(self.generate(text_requirements='  ').status_code, 400)
        self.project = self.client.post('/api/projects/', {'title': '빈 공간'}).json()
        self.assertEqual(self.generate().status_code, 400)

    def test_request_id_cannot_be_reused_with_another_input_revision(self):
        request_id = str(uuid4())
        self.assertEqual(self.generate(request_id=request_id).status_code, 201)
        self.assertEqual(self.generate(request_id=request_id, revision=99).status_code, 400)

    def test_save_context_omission_keeps_existing_value_for_older_clients(self):
        from .models import Project
        context = {'text_requirements': '창가 소파'}
        Project.objects.filter(id=self.project['id']).update(request_context=context)
        response = self.client.post(f"/api/projects/{self.project['id']}/", {
            'revision': 0, 'title': '공간', 'document': {'strokes': [], 'items': []},
        }, content_type='application/json')
        self.assertEqual(response.json()['request_context'], context)

    def test_requirements_event_supports_description_limit_without_relaxing_other_events(self):
        from .validation import events
        event = {'event_id': str(uuid4()), 'event_type': 'requirements.changed', 'payload': {'text': 'a' * 1000}}
        self.assertEqual(len(events([event])), 1)
        event['payload']['text'] += 'a'
        with self.assertRaises(ValueError): events([event])
        event['payload']['text'] = 'a' * 101
        event['event_type'] = 'button.clicked'
        with self.assertRaises(ValueError): events([event])
        self.assertEqual(len(events([{'event_id': str(uuid4()), 'event_type': 'style.changed', 'payload': {'style': '세이지 그린'}}])), 1)
