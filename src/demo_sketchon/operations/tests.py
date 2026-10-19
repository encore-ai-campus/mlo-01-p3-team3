import io,json,tempfile
from uuid import uuid4
from unittest.mock import patch
from django.contrib.auth import get_user_model
from django.contrib.auth.models import Permission
from django.core.files.uploadedfile import SimpleUploadedFile
from django.test import Client,TestCase,override_settings
from PIL import Image


class OperationsTests(TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.settings_override=override_settings(MEDIA_ROOT=self.tmp.name,SKETCH_RAW_ROOT=self.tmp.name+'/raw');self.settings_override.enable();self.addCleanup(self.settings_override.disable)
        self.staff=get_user_model().objects.create_user('operator',password='Operator-demo-2468!',is_staff=True,is_superuser=True)

    def project(self,client=None):
        output=io.BytesIO();Image.new('RGB',(80,60),'white').save(output,'PNG')
        return (client or self.client).post('/api/projects/',{'photo':SimpleUploadedFile('room.png',output.getvalue(),'image/png')}).json()

    def test_console_requires_staff_and_permission(self):
        self.assertEqual(self.client.get('/ops/').status_code,302)
        ordinary=get_user_model().objects.create_user('ordinary',password='Member-demo-2468!')
        self.client.force_login(ordinary);self.assertEqual(self.client.get('/ops/').status_code,403)
        ordinary.is_staff=True;ordinary.save();self.assertEqual(self.client.get('/ops/').status_code,403)
        self.client.force_login(self.staff)
        for path in ('','people/','projects/','events/','collection/','ads/','ad-reports/','audit/'):
            self.assertEqual(self.client.get('/ops/'+path).status_code,200,path)

    def test_generation_tracking_success_replay_and_failure(self):
        from operations.models import GenerationAttempt
        p=self.project();data={'request_id':str(uuid4()),'revision':0,'document':{'strokes':[],'items':[]},'text_requirements':'화분'}
        url=f"/api/projects/{p['id']}/search/"
        self.assertEqual(self.client.post(url,data,content_type='application/json').status_code,201)
        self.assertEqual(self.client.post(url,data,content_type='application/json').status_code,200)
        row=GenerationAttempt.objects.get(request_id=data['request_id']);self.assertEqual(row.status,'SUCCEEDED');self.assertIsNotNone(row.duration_ms)
        p=self.project();data['request_id']=str(uuid4())
        with patch('sketch.views.compose',side_effect=RuntimeError('secret must not be logged')):
            with self.assertRaises(RuntimeError):self.client.post(f"/api/projects/{p['id']}/search/",data,content_type='application/json')
        row=GenerationAttempt.objects.get(request_id=data['request_id']);self.assertEqual(row.status,'FAILED');self.assertNotIn('secret',row.error_code)

    def test_flow_and_event_actor_are_preserved_after_claim(self):
        from sketch.models import Project,SketchEvent
        self.client.get('/')
        home=Project.objects.order_by('-updated_at').first()
        response=self.client.get('/editor/',{'flow_id':str(home.flow_id)})
        p=response.context['initial'];self.assertEqual(p['flow_id'],str(home.flow_id))
        event=SketchEvent.objects.filter(project_id=p['id']).latest('event_time');self.assertEqual(event.actor_kind,'guest')
        self.client.post('/accounts/signup/',{'username':'member','nickname':'회원','password1':'Member-demo-2468!','password2':'Member-demo-2468!'})
        event.refresh_from_db();self.assertEqual(event.actor_kind,'guest');self.assertIsNone(event.actor_account_id)

    def test_raw_receipt_and_review_do_not_rewrite_raw(self):
        from pathlib import Path
        from operations.models import RawReceipt
        from operations.tracking import record_raw_receipt
        from sketch.models import Project
        p=Project.objects.get(pk=self.project()['id']);sample={'sample_id':str(uuid4()),'capture_status':'limited','strokes':[],'operations':[]}
        body=json.dumps(sample).encode();root=Path(self.tmp.name)/'raw';root.mkdir();file=root/(sample['sample_id']+'.json');file.write_bytes(body)
        receipt=record_raw_receipt(p,sample,body,'REVIEW',['capture limited'],True)
        record_raw_receipt(p,sample,body,'REVIEW',['capture limited'],True)
        self.assertEqual(RawReceipt.objects.count(),1)
        self.client.force_login(self.staff)
        result=self.client.post(f'/ops/collection/{receipt.id}/review/',{'review_status':'APPROVED','reason':'검수 완료'})
        self.assertEqual(result.status_code,302);receipt.refresh_from_db();self.assertEqual(receipt.review_status,'APPROVED');self.assertEqual(file.read_bytes(),body)

    def test_ad_approval_edit_requires_review_and_revision(self):
        from operations.models import AdCampaign
        self.client.force_login(self.staff)
        stream=io.BytesIO();Image.new('RGB',(30,60),'green').save(stream,'PNG')
        data={'campaign_id':'plant-demo','title':'화분 광고','category':'plant','slot_id':'result-right','landing_url':'https://example.com/plant',
              'bid_units':500,'starts_at':'2026-01-01T00:00','ends_at':'2030-01-01T00:00','active':'on','reason':'데모 등록','revision':0,
              'creative':SimpleUploadedFile('ad.png',stream.getvalue(),'image/png')}
        self.assertEqual(self.client.post('/ops/ads/new/',data).status_code,302)
        campaign=AdCampaign.objects.get(campaign_id='plant-demo')
        self.assertEqual(self.client.post(f'/ops/ads/{campaign.id}/review/',{'revision':campaign.revision,'decision':'APPROVED','reason':'인테리어 정책 적합'}).status_code,302)
        campaign.refresh_from_db();self.assertEqual(campaign.review_status,'APPROVED')
        data.pop('creative');data['title']='새 문구';data['revision']=campaign.revision
        self.assertEqual(self.client.post(f'/ops/ads/{campaign.id}/edit/',data).status_code,302)
        campaign.refresh_from_db();self.assertEqual(campaign.review_status,'PENDING')
        self.assertEqual(self.client.post(f'/ops/ads/{campaign.id}/review/',{'revision':0,'decision':'APPROVED','reason':'오래된 화면'}).status_code,409)

    def test_sensitive_media_needs_separate_permission_and_csrf(self):
        p=self.project();staff=get_user_model().objects.create_user('limited',is_staff=True)
        self.client.force_login(staff)
        self.assertEqual(self.client.get(f"/ops/media/projects/{p['id']}/photo/").status_code,403)
        self.client.force_login(self.staff)
        response=self.client.get(f"/ops/media/projects/{p['id']}/photo/");self.assertEqual(response.status_code,200);self.assertTrue(b"".join(response.streaming_content))
        csrf=Client(enforce_csrf_checks=True);csrf.force_login(self.staff)
        self.assertEqual(csrf.post('/ops/collection/index/').status_code,403)

    def test_invalid_date_filter_does_not_crash(self):
        self.client.force_login(self.staff)
        self.assertEqual(self.client.get('/ops/',{'from':'2026-99-99','to':'bad'}).status_code,200)

    def test_raw_hash_matches_stored_canonical_bytes(self):
        from pathlib import Path
        from operations.models import RawReceipt
        from sketch.test_collection import CollectionTests
        from types import SimpleNamespace
        from sketch.models import Project
        project=Project.objects.get(pk=self.project()['id'])
        sample=CollectionTests.sample(SimpleNamespace(project=project,writer=project.owner))
        sample['project_width']=project.width;sample['project_height']=project.height
        result=self.client.post(f'/api/projects/{project.id}/samples/',json.dumps(sample,indent=2),content_type='application/json')
        self.assertIn(result.status_code,(200,201))
        row=RawReceipt.objects.get();self.client.force_login(self.staff)
        response=self.client.get(f'/ops/media/raw/{row.id}/')
        self.assertEqual(response.status_code,200)
        self.assertEqual(response.content,(Path(self.tmp.name)/'raw'/(str(row.sample_id)+'.json')).read_bytes())

    def test_unknown_raw_id_retries_share_one_receipt(self):
        from operations.models import RawReceipt
        from operations.tracking import record_raw_receipt
        from sketch.models import Project
        row=Project.objects.get(pk=self.project()['id'])
        record_raw_receipt(row,{},b'{}','INVALID',['missing sample'],False)
        record_raw_receipt(row,{},b'{}','INVALID',['missing sample'],False)
        self.assertEqual(RawReceipt.objects.count(),1)

    def test_successful_generation_cannot_be_overwritten_by_late_failure(self):
        from operations.models import GenerationAttempt
        from operations.tracking import finish_attempt
        from sketch.models import Project
        project=Project.objects.get(pk=self.project()['id'])
        row=GenerationAttempt.objects.create(request_id=uuid4(),project=project,input_hash='a'*64,actor_kind='guest')
        finish_attempt(row,'SUCCEEDED','');finish_attempt(row,'CONFLICT','http_409');row.refresh_from_db()
        self.assertEqual(row.status,'SUCCEEDED');self.assertEqual(row.error_code,'')

    def test_client_actor_claim_is_separate_from_authenticated_actor(self):
        from sketch.models import SketchEvent
        p=self.project();identifier=str(uuid4())
        response=self.client.post(f"/api/projects/{p['id']}/events/",{'events':[{'event_id':identifier,'event_type':'button.clicked','payload':{'client_actor_kind':'member','client_time':'2026-10-16T00:00:00Z'}}]},content_type='application/json')
        self.assertEqual(response.status_code,200)
        row=SketchEvent.objects.get(pk=identifier);self.assertEqual(row.actor_kind,'guest');self.assertEqual(row.reported_actor_kind,'member');self.assertIsNone(row.actor_account_id)

    def test_role_command_only_grants_scoped_permissions_to_existing_user(self):
        from django.core.management import call_command,CommandError
        user=get_user_model().objects.create_user('hilee')
        call_command('configure_ops_roles',username='hilee')
        user.refresh_from_db();self.assertTrue(user.is_staff);self.assertFalse(user.is_superuser)
        self.assertTrue(user.has_perm('operations.view_console'));self.assertFalse(user.has_perm('auth.change_user'))
        with self.assertRaises(CommandError):call_command('configure_ops_roles',username='missing')

    def test_invalid_raw_cannot_be_approved(self):
        from operations.tracking import record_raw_receipt
        from sketch.models import Project
        row=record_raw_receipt(Project.objects.get(pk=self.project()['id']),{},b'{}','INVALID',[],False)
        self.client.force_login(self.staff)
        self.assertEqual(self.client.post(f'/ops/collection/{row.id}/review/',{'review_status':'APPROVED','reason':'승인 시도'}).status_code,400)

    def test_ad_server_is_never_called_and_metrics_are_unconnected(self):
        self.client.force_login(self.staff)
        with patch('urllib.request.urlopen',side_effect=AssertionError('No ad network call allowed')):
            response=self.client.get('/ops/ad-reports/')
        self.assertContains(response,'미연결');self.assertNotContains(response,'0.00%')

    def test_non_operator_cannot_write_campaign_or_raw_review(self):
        user=get_user_model().objects.create_user('member');self.client.force_login(user)
        for url in ('/ops/ads/new/',f'/ops/ads/{uuid4()}/review/',f'/ops/ads/{uuid4()}/toggle/',f'/ops/collection/{uuid4()}/review/'):
            self.assertEqual(self.client.post(url,{}).status_code,403)

    def test_scoped_operator_account_revalidation_uses_page_permission(self):
        user=get_user_model().objects.create_user('collection_only',is_staff=True)
        user.user_permissions.add(Permission.objects.get(content_type__app_label='operations',codename='view_collection'))
        self.client.force_login(user)
        self.assertEqual(self.client.get('/ops/collection/').status_code,200)
        status=self.client.get('/api/account/status/',{'ops_permission':'view_collection'}).json()
        self.assertTrue(status['can_access_ops']);self.assertTrue(status['ops_page_access']);self.assertEqual(status['ops_url'],'/ops/collection/')
        status=self.client.get('/api/account/status/',{'ops_permission':'manage_ads'}).json();self.assertFalse(status['ops_page_access'])

    def test_failed_request_corrected_with_same_id_records_committed_success(self):
        from operations.models import GenerationAttempt
        p=self.project();identifier=str(uuid4());url=f"/api/projects/{p['id']}/search/"
        payload={'request_id':identifier,'revision':0,'document':{'invalid':True},'text_requirements':'화분'}
        self.assertEqual(self.client.post(url,payload,content_type='application/json').status_code,400)
        self.assertEqual(GenerationAttempt.objects.get(pk=identifier).status,'FAILED')
        payload['document']={'strokes':[],'items':[]}
        self.assertEqual(self.client.post(url,payload,content_type='application/json').status_code,201)
        row=GenerationAttempt.objects.get(pk=identifier);self.assertEqual(row.status,'SUCCEEDED');self.assertEqual(str(row.result_id),identifier)

    def test_quota_ip_read_is_audited(self):
        from sketch.models import GuestGenerationUsage
        from operations.models import OperatorAudit
        from django.utils import timezone
        GuestGenerationUsage.objects.create(client_ip='192.0.2.3',usage_date=timezone.localdate(),successful_count=1)
        self.client.force_login(self.staff);self.client.get('/ops/people/')
        self.assertTrue(OperatorAudit.objects.filter(action='quota.ip_viewed',actor=self.staff).exists())

    def test_existing_creative_form_uses_protected_preview(self):
        from operations.models import AdCampaign
        from django.core.files.base import ContentFile
        from django.utils import timezone
        from datetime import timedelta
        row=AdCampaign.objects.create(campaign_id='preview',title='미리보기',category='plant',slot_id='result-right',starts_at=timezone.now(),ends_at=timezone.now()+timedelta(days=5))
        data=io.BytesIO();Image.new('RGB',(10,10),'green').save(data,'PNG');row.creative.save('creative.png',ContentFile(data.getvalue()))
        self.client.force_login(self.staff);response=self.client.get(f'/ops/ads/{row.id}/edit/')
        self.assertNotContains(response,'ops/ad_creatives');self.assertContains(response,f'/ops/ads/{row.id}/image/')
