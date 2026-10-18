import io
from datetime import datetime, timezone as dt_timezone
from unittest.mock import patch
from uuid import uuid4
from django.core.files.uploadedfile import SimpleUploadedFile
from django.contrib.auth import get_user_model
from django.test import Client, TestCase, override_settings
from . import tests as fixtures
from .models import Composition, GuestGenerationUsage, Project


class QuotaTests(TestCase):
    setUp = fixtures.SketchTests.setUp
    create = fixtures.SketchTests.create
    def generate(self, client=None, request_id=None):
        client = client or self.client
        stream = io.BytesIO()
        from PIL import Image
        Image.new('RGB',(80,60),'white').save(stream,'PNG')
        project = self.create(client, SimpleUploadedFile('room.png',stream.getvalue(),'image/png'))
        data = {'document':{'strokes':[], 'items':[]}, 'revision':0,
                'request_id':str(request_id or uuid4()), 'text_requirements':'화분'}
        return client.post(f"/api/projects/{project['id']}/search/", data, content_type='application/json'), project, data

    def test_three_successes_fourth_denied_same_ip_even_other_session(self):
        for _ in range(3): self.assertEqual(self.generate()[0].status_code,201)
        other = Client(REMOTE_ADDR='127.0.0.1')
        response, _, _ = self.generate(other)
        self.assertEqual(response.status_code,429)
        self.assertEqual(GuestGenerationUsage.objects.get().successful_count,3)
        self.assertEqual(Composition.objects.count(),3)
        self.assertEqual(self.client.get('/api/account/status/').json()['quota']['remaining'],0)

    def test_replay_does_not_charge_and_success_download_unlimited(self):
        response, project, data = self.generate()
        for _ in range(5):
            self.assertEqual(self.client.post(f"/api/projects/{project['id']}/search/", data, content_type='application/json').status_code,200)
            download = self.client.get(response.json()['download_url'])
            self.assertEqual(download.status_code,200)
            download.close()
        self.assertEqual(GuestGenerationUsage.objects.get().successful_count,1)

    def test_failed_file_write_rolls_back_usage_and_lock(self):
        response, project, data = self.generate()
        clone = self.client.post(f"/api/results/{response.json()['result_id']}/redraw/", {'mode':'sketch'},content_type='application/json').json()
        data['request_id']=str(uuid4())
        with patch('django.core.files.storage.FileSystemStorage._save', side_effect=OSError('disk full')):
            with self.assertRaises(OSError):
                self.client.post(f"/api/projects/{clone['id']}/search/",data,content_type='application/json')
        self.assertEqual(GuestGenerationUsage.objects.get().successful_count,1)
        self.assertFalse(Project.objects.get(pk=clone['id']).locked)

    def test_midnight_kst_and_untrusted_forwarded_ip(self):
        before = datetime(2026,10,16,14,59,59,tzinfo=dt_timezone.utc)
        after = datetime(2026,10,16,15,0,0,tzinfo=dt_timezone.utc)
        with patch('sketch.quota.timezone.now',return_value=before):
            self.generate()
            self.assertEqual(self.client.get('/api/account/status/').json()['quota']['used'],1)
        with patch('sketch.quota.timezone.now',return_value=after):
            self.assertEqual(self.client.get('/api/account/status/').json()['quota']['used'],0)
            self.generate()
        self.assertEqual(GuestGenerationUsage.objects.count(),2)
        with patch('sketch.quota.timezone.now', return_value=after):
            self.assertEqual(self.client.get('/api/account/status/', HTTP_X_FORWARDED_FOR='1.2.3.4').json()['quota']['used'],1)

    def test_member_has_no_generation_limit(self):
        user=get_user_model().objects.create_user('member',password='Demo-member-2468!')
        self.client.force_login(user)
        for _ in range(4): self.assertEqual(self.generate()[0].status_code,201)
        self.assertEqual(GuestGenerationUsage.objects.count(),0)

    def test_same_request_race_recheck_avoids_double_charge(self):
        from . import views
        original = views.compose
        stream = io.BytesIO()
        from PIL import Image
        Image.new('RGB',(80,60),'white').save(stream,'PNG')
        p = self.create(photo=SimpleUploadedFile('r.png',stream.getvalue(),'image/png'))
        data={'document':{'strokes':[],'items':[]},'revision':0,'request_id':str(uuid4()),'text_requirements':'화분'}
        def concurrent(*args):
            with patch('sketch.views.compose', original):
                self.assertEqual(self.client.post(f"/api/projects/{p['id']}/search/",data,content_type='application/json').status_code,201)
            return original(*args)
        with patch('sketch.views.compose',side_effect=concurrent):
            self.assertEqual(self.client.post(f"/api/projects/{p['id']}/search/",data,content_type='application/json').status_code,200)
        self.assertEqual(GuestGenerationUsage.objects.get().successful_count,1)
