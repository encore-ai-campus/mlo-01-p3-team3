import copy
from uuid import uuid4
from unittest.mock import patch
from django.contrib.auth import get_user_model
from django.test import Client, TestCase
from . import test_quota as fixtures
from .models import Composition, Project, SavedProjectVersion, WishlistEntry


class LibraryTests(TestCase):
    setUp = fixtures.QuotaTests.setUp
    create = fixtures.QuotaTests.create
    generate = fixtures.QuotaTests.generate
    def member(self):
        self.client.post('/accounts/signup/', {'username':'libuser','nickname':'취향', 'password1':'Library-user-2468!', 'password2':'Library-user-2468!'})

    def saved(self):
        self.member()
        response, project, data=self.generate()
        result=response.json()
        payload={'request_id':str(uuid4()),'result_version':result['version'],'title':'나의 거실'}
        saved=self.client.post(f"/api/results/{result['result_id']}/save/",payload,content_type='application/json')
        self.assertEqual(saved.status_code,201,saved.content)
        return result, saved.json(), payload

    def test_saved_image_immutable_restore_and_next_version(self):
        result, saved, payload=self.saved()
        version=SavedProjectVersion.objects.get(pk=saved['version_id'])
        old_image=version.image.read(); version.image.close()
        composition=Composition.objects.get(pk=result['result_id'])
        composition.version+=1
        composition.placements[0]['x']=0.1
        composition.save(update_fields=['version','placements'])
        version.refresh_from_db()
        self.assertEqual(version.image.read(),old_image); version.image.close()
        self.assertNotEqual(version.snapshot['placements'],composition.placements)
        restored=self.client.post(f"/api/saved-versions/{version.pk}/edit/").json()
        p=Project.objects.get(pk=restored['id'])
        self.assertFalse(p.locked)
        self.assertEqual(p.document,composition.input_document)
        self.assertEqual(str(p.saved_project_id),saved['project_id'])
        generated=self.client.post(f"/api/projects/{p.pk}/search/", {'request_id':str(uuid4()), 'revision':0,'document':p.document,**p.request_context},content_type='application/json').json()
        again=self.client.post(f"/api/results/{generated['result_id']}/save/",{'request_id':str(uuid4()),'result_version':0,'title':'나의 거실'},content_type='application/json')
        self.assertEqual(again.json()['version_number'],2)

    def test_save_idempotency_stale_version_and_other_member_denied(self):
        result,saved,payload=self.saved()
        url=f"/api/results/{result['result_id']}/save/"
        self.assertEqual(self.client.post(url,payload,content_type='application/json').json()['version_id'],saved['version_id'])
        payload['request_id']=str(uuid4())
        self.assertEqual(self.client.post(url,payload,content_type='application/json').json()['version_id'],saved['version_id'])
        self.assertEqual(SavedProjectVersion.objects.count(),1)
        payload['result_version']=50
        self.assertEqual(self.client.post(url,payload,content_type='application/json').status_code,409)
        self.client.post('/accounts/logout/')
        self.assertEqual(self.client.post(url,payload,content_type='application/json').status_code,401)
        other=get_user_model().objects.create_user('other',password='Other-2468!')
        self.client.force_login(other)
        self.assertEqual(self.client.get(saved['image_url']).status_code,404)
        self.assertEqual(self.client.post(f"/api/saved-versions/{saved['version_id']}/edit/").status_code,404)

    def test_compare_only_own_versions(self):
        result,saved,_=self.saved()
        url=f"/my/compare/?left={saved['version_id']}&right={saved['version_id']}"
        self.assertEqual(self.client.get(url).status_code,200)
        self.client.force_login(get_user_model().objects.create_user('other'))
        self.assertEqual(self.client.get(url).status_code,404)

    def test_wishlist_stable_across_recommendation_requests(self):
        self.member()
        response,_,_=self.generate()
        result=response.json(); obj=result['scene_objects'][0]
        def recommendation():
            return self.client.post(f"/api/scene-objects/{obj['id']}/recommendations/", {'request_id':str(uuid4()),'scene_generation_id':result['result_id'],'user_confirmed':True},content_type='application/json').json()
        first=recommendation()
        payload={'recommendation_run_id':first['recommendation_run_id'],'item_id':first['items'][0]['id']}
        a=self.client.post('/api/wishlist/',payload,content_type='application/json')
        self.assertEqual(a.status_code,201,a.content)
        second=recommendation()
        payload={'recommendation_run_id':second['recommendation_run_id'],'item_id':second['items'][0]['id']}
        b=self.client.post('/api/wishlist/',payload,content_type='application/json')
        self.assertEqual(b.json()['id'],a.json()['id'])
        self.assertEqual(WishlistEntry.objects.count(),1)
        self.assertIsNone(WishlistEntry.objects.get().item_snapshot['product_id'])
        self.assertEqual(self.client.post(f"/api/wishlist/{a.json()['id']}/remove/").status_code,200)
        self.assertFalse(WishlistEntry.objects.exists())

    def test_save_file_failure_rolls_back_library_and_lineage(self):
        self.member()
        result=self.generate()[0].json()
        with patch('django.core.files.storage.FileSystemStorage._save',side_effect=OSError('disk full')):
            with self.assertRaises(OSError):
                self.client.post(f"/api/results/{result['result_id']}/save/",{'request_id':str(uuid4()),'result_version':0,'title':'실패'},content_type='application/json')
        self.assertEqual(SavedProjectVersion.objects.count(),0)
        self.assertIsNone(Project.objects.get(pk=result['project_id']).saved_project_id)

    def test_replacing_photo_keeps_saved_project_lineage(self):
        _,saved,_=self.saved()
        restored=self.client.post(f"/api/saved-versions/{saved['version_id']}/edit/").json()
        replaced=self.client.post('/api/projects/',{'title':'사진 교체','source_project_id':restored['id']}).json()
        self.assertEqual(str(Project.objects.get(pk=replaced['id']).saved_project_id),saved['project_id'])
        self.client.force_login(get_user_model().objects.create_user('foreign'))
        self.assertEqual(self.client.post('/api/projects/',{'source_project_id':restored['id']}).status_code,404)
