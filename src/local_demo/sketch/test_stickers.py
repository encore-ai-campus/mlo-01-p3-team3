import hashlib
from pathlib import Path
from uuid import uuid4

from django.conf import settings
from django.test import TestCase, Client
from PIL import Image

from . import validation
from .stickers import STICKERS


class StickerTests(TestCase):
    def item(self):
        return {'id': str(uuid4()), 'kind':'sticker', 'sticker_id':'sofa-cream',
                'x':.2, 'y':.3, 'width':.4, 'height':.2}

    def test_photo_registry_matches_existing_assets(self):
        self.assertEqual(len(STICKERS), 7)
        self.assertEqual(len({s['id'] for s in STICKERS}), 7)
        for row in STICKERS:
            asset = Path(settings.BASE_DIR) / 'sketch/static' / row['image']
            self.assertEqual(hashlib.sha256(asset.read_bytes()).hexdigest(), row['sha256'])
            with Image.open(asset) as image:
                self.assertEqual(image.size, (row['width'], row['height']))
                self.assertIn('A', image.getbands(), row['id'])
                alpha = image.getchannel('A')
                self.assertGreater(alpha.histogram()[0] / (image.width * image.height), .1, row['id'])
                self.assertGreaterEqual(alpha.getextrema()[1], 200, row['id'])
                for point in [(0,0), (image.width-1,0), (0,image.height-1), (image.width-1,image.height-1)]:
                    self.assertEqual(alpha.getpixel(point), 0, row['id'])

    def test_sticker_save_restore_events_and_ownership(self):
        project = self.client.post('/api/projects/', {}).json()
        item = self.item()
        document = {'strokes':[], 'items':[item]}
        response = self.client.post(f"/api/projects/{project['id']}/", {
            'title':'사진 스티커', 'revision':0, 'document':document, 'events':[
                {'event_id':str(uuid4()), 'event_type':'item.added', 'payload':{'item':item}}]}, content_type='application/json')
        self.assertEqual(response.status_code, 200, response.content)
        self.assertEqual(self.client.get(f"/api/projects/{project['id']}/").json()['document'], document)
        self.assertEqual(Client().get(f"/api/projects/{project['id']}/").status_code, 404)
        page = self.client.get(f"/editor/{project['id']}/")
        self.assertContains(page, '사진 스티커로 배치해 보세요')
        self.assertContains(page, 'data-sticker=', count=7)
        self.assertNotContains(page, 'id="preview-mock"')

    def test_sticker_rejects_external_sources_unknown_ids_and_outside_bounds(self):
        for changes in [{'sticker_id':'../sofa-cream'}, {'sticker_id':[]},
                        {'sticker_id':'missing'}, {'src':'https://evil.test/a.png'},
                        {'x':.9}, {'width':0}, {'color':'#ffffff'}]:
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                validation.document({'strokes':[], 'items':[{**self.item(), **changes}]})
        item = self.item()
        item.update(group_id=str(uuid4()), cuts=[{'tool':'eraser','color':'#ffffff','width':.01,
                    'pointer_type':'mouse','points':[{'x':.4,'y':.4,'pressure':.5}]}])
        self.assertEqual(validation.document({'strokes':[], 'items':[item]})['items'][0], item)
