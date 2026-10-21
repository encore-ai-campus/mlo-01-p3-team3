import json
import tempfile
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta,datetime,timezone as dt_timezone
from pathlib import Path
from unittest.mock import patch
from django.conf import settings
from django.core.exceptions import ValidationError
from django.db import close_old_connections
from django.test import SimpleTestCase,TestCase,TransactionTestCase,override_settings
from django.utils import timezone
from . import engine as e
from .assets import banner,manifest
from .errors import AdError
from .models import Advertiser,MockContract,Campaign,Revision,Decision,Event,DefaultNotice,Audit
from .policy import TestPolicy,integer,destination

class DatabaseConfigurationTests(SimpleTestCase):
    def env(self,**changes):
        return {'AD_DB_BACKEND':'mysql','AD_MYSQL_DATABASE':'ads_test_config',
                'AD_MYSQL_USER':'test_user','AD_MYSQL_PASSWORD':'offline-only-secret',
                'AD_MYSQL_HOST':'not-contacted.invalid',**changes}

    def test_sqlite_default_is_unchanged(self):
        from adconfig.database import database_config
        config=database_config({},Path('/tmp'))
        self.assertEqual(config['ENGINE'],'django.db.backends.sqlite3')
        self.assertEqual(config['OPTIONS']['transaction_mode'],'IMMEDIATE')

    def test_mysql_config_and_separate_test_database(self):
        from adconfig.database import database_config
        config=database_config(self.env(AD_MYSQL_TEST_DATABASE=''),Path('/tmp'))
        self.assertEqual(config['TEST']['NAME'],'test_ads_test_config')
        self.assertEqual(config['OPTIONS']['isolation_level'],'read committed')
        self.assertNotIn('transaction_mode',config['OPTIONS'])
        self.assertIn('INNODB',config['OPTIONS']['init_command'])
        config=database_config(self.env(AD_MYSQL_SSL_CA='/example/ca.pem'),Path('/tmp'))
        self.assertEqual(config['OPTIONS']['ssl_mode'],'VERIFY_IDENTITY')

    def test_invalid_config_fails_without_secret_output(self):
        from adconfig.database import database_config
        from django.core.exceptions import ImproperlyConfigured
        for changes in [{'AD_DB_BACKEND':'other'},{'AD_MYSQL_HOST':''},{'AD_MYSQL_PASSWORD':''},
                        {'AD_MYSQL_PORT':'0'},{'AD_MYSQL_PORT':'abc'},
                        {'AD_MYSQL_DATABASE':'bad/name'},
                        {'AD_MYSQL_TEST_DATABASE':'ads_test_config'}]:
            with self.assertRaises(ImproperlyConfigured) as caught:
                database_config(self.env(**changes),Path('/tmp'))
            self.assertNotIn('offline-only-secret',str(caught.exception))

    def test_offline_command_never_connects_or_discloses_credentials(self):
        from django.core.management import call_command
        from django.db.backends.base.base import BaseDatabaseWrapper
        from io import StringIO
        output=StringIO()
        with patch.object(BaseDatabaseWrapper,'connect',side_effect=AssertionError('No database connection allowed')):
            call_command('check_ad_database_config',stdout=output)
        self.assertIn('OFFLINE ONLY',output.getvalue())
        self.assertNotIn('PASSWORD',output.getvalue())

    def test_mysql_backend_loads_without_connecting(self):
        from importlib.util import find_spec
        if find_spec('MySQLdb') is None:self.skipTest('Optional mysqlclient not installed')
        from adconfig.database import database_config
        from django.db.utils import ConnectionHandler
        from django.db.backends.base.base import BaseDatabaseWrapper
        config=database_config(self.env(),Path('/tmp'))
        with patch.object(BaseDatabaseWrapper,'connect',side_effect=AssertionError('No database connection allowed')):
            connection=ConnectionHandler({'default':config})['default']
            params=connection.get_connection_params()
            self.assertEqual(params['charset'],'utf8mb4')
            self.assertIsNone(connection.connection)


TOKEN='service-test-token-not-production-00000000'
SECRET='test-secret-isolated-adserver-000000000000'
VISITOR='anonymous-test-browser-000001'

class PolicyMixin:
    def setUp(self):
        super().setUp();self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        config=json.loads((settings.BASE_DIR/'test_policy.example.json').read_text())
        config.update(frequency_limit=100)
        self.policy_path=Path(self.temp.name)/'policy.json';self.policy_path.write_text(json.dumps(config))
        self.override=override_settings(AD_TEST_OPERATIONS=True,AD_SERVICE_TOKEN=TOKEN,SECRET_KEY=SECRET,
            AD_TEST_POLICY_FILE=str(self.policy_path))
        self.override.enable();self.addCleanup(self.override.disable);self.now=timezone.now()
    def configure(self,**changes):
        data=json.loads(self.policy_path.read_text());data.update(changes);self.policy_path.write_text(json.dumps(data))
    def campaign(self,*,bid=500,template='desk',general=True,advertiser=None,cid=None,product=None):
        a=advertiser or Advertiser.objects.create(name='가상 광고주',status='ACTIVE')
        contract=MockContract.objects.create(advertiser=a,reference='TEST-CONTRACT',status='APPROVED',
            starts_at=self.now-timedelta(days=1),ends_at=self.now+timedelta(days=2))
        c=Campaign.objects.create(advertiser=a,contract=contract,name='테스트 캠페인',**({'id':cid} if cid else {}))
        r=Revision.objects.create(campaign=c,template_id=template,title='광고 제목',alt_text='배너 대체 텍스트',
            bid_points=bid,placements=['EDITOR_RIGHT','RESULT_RIGHT'],is_general=general,
            destination='/test-destination/sample/',starts_at=self.now-timedelta(hours=1),ends_at=self.now+timedelta(days=1),
            kind='PRODUCT' if product else 'BRAND',product_id=product or '',product_verified=bool(product))
        e.review_revision(r.pk,True,'test-operator');c.refresh_from_db();c.enabled=True;c.save();return c
    def request(self,**changes):
        return {'request_id':str(uuid.uuid4()),'visitor_id':VISITOR,'context_id':str(uuid.uuid4()),
            'placement':'EDITOR_RIGHT','signals':[{'key':'notebook','source':'CONFIRMED_TEXT'}],
            'organic_product_ids':[],**changes}
    def event(self,d,kind,*,at=None,**extra):
        now=at or self.now
        return e.record_event(d.pk,{'event_id':str(uuid.uuid4()),'visitor_id':VISITOR,'kind':kind,
            'occurred_at':now.isoformat(),**extra},now=now)
    def assertAdError(self,code,fn,*args,**kwargs):
        with self.assertRaises(AdError) as caught:fn(*args,**kwargs)
        self.assertEqual(caught.exception.code,code)

class BannerTests(TestCase):
    def test_all_ten_verified_without_database(self):
        with self.assertNumQueries(0):
            rows=manifest();response=self.client.get('/')
        self.assertEqual(response.status_code,200);self.assertEqual(len(rows),10)
        self.assertTrue(all(x['status']=='READY' and x['width']==600 and x['height']==1200 for x in rows))
    def test_public_template_and_health(self):
        self.assertEqual(len(self.client.get('/api/v1/templates/').json()['templates']),10)
        self.assertEqual(self.client.get('/health/').json()['ready'],10)
    def test_fixed_asset_whitelist_etag_head(self):
        a=self.client.get('/assets/desk/');self.assertEqual(a['Content-Type'],'image/png')
        self.assertEqual(self.client.get('/assets/desk/',HTTP_IF_NONE_MATCH=a['ETag']).status_code,304)
        self.assertEqual(self.client.head('/assets/desk/').content,b'')
        for path in ['/assets/thumbs/','/assets/thumbs.db','/ad_banner_image/thumbs.db']:
            self.assertEqual(self.client.get(path).status_code,404)
    @override_settings(AD_FRAME_ANCESTORS="'self' http://127.0.0.1:8003 bad;origin")
    def test_embed_frame_allowlist_and_ratio(self):
        r=self.client.get('/embed/desk/');self.assertEqual(r.status_code,200)
        self.assertNotIn('X-Frame-Options',r)
        self.assertIn("frame-ancestors 'self' http://127.0.0.1:8003",r['Content-Security-Policy'])
        self.assertContains(r,'object-fit:contain');self.assertContains(r,'광고')
    def test_missing_and_invalid_images_fail_independently(self):
        with tempfile.TemporaryDirectory() as directory,override_settings(AD_BANNER_ROOT=Path(directory)):
            self.assertEqual(self.client.get('/assets/desk/').status_code,503)
            self.assertEqual(self.client.get('/health/').status_code,503)
            self.assertEqual(self.client.get('/').status_code,200)
            (Path(directory)/'04_desk.png').write_bytes(b'not-image')
            self.assertEqual(self.client.get('/assets/desk/').status_code,503)
    @override_settings(AD_TEST_OPERATIONS=False)
    def test_operations_disabled_by_default(self):
        self.assertEqual(self.client.post('/internal/v1/decisions/',{},content_type='application/json').status_code,503)

class PolicyTests(PolicyMixin,TestCase):
    def test_explicit_config_token_and_secret_required(self):
        self.assertEqual(TestPolicy.load().version,'test-local-1')
        with override_settings(AD_SERVICE_TOKEN='short'):self.assertAdError('POLICY_NOT_CONFIGURED',TestPolicy.load)
        with override_settings(SECRET_KEY='short'):self.assertAdError('POLICY_NOT_CONFIGURED',TestPolicy.load)
        self.configure(frequency_limit=None);self.assertAdError('POLICY_NOT_CONFIGURED',TestPolicy.load)
    def test_bid_strict_type_range(self):
        for value in [True,False,0,10001,1.2,'100',float('nan'),float('inf')]:
            self.assertAdError('INVALID_BID',integer,value,'bid')
        self.assertEqual(integer(10000,'bid'),10000)
    def test_destination_no_open_redirect(self):
        p=TestPolicy.load()
        for value in ['javascript:alert(1)','//evil.test/','https://evil.test/','/test-destination/a/?next=evil','/admin/','/test-destination/a%2fb/']:
            self.assertAdError('INVALID_DESTINATION',destination,value,p)
        self.assertEqual(destination('/test-destination/a/',p),'/test-destination/a/')
    def test_raw_photo_and_pii_not_accepted(self):
        self.assertAdError('INVALID_REQUEST',e.decide,self.request(photo='base64'))
        self.assertAdError('INVALID_VISITOR',e.decide,self.request(visitor_id='IP:127.0.0.1'))
        self.assertAdError('UNSUPPORTED_PLACEMENT',e.decide,self.request(placement='MOBILE'))
    def test_operations_auth_and_staff(self):
        self.assertEqual(self.client.post('/internal/v1/decisions/',{},content_type='application/json').status_code,401)
        self.assertEqual(self.client.get('/sandbox/').status_code,403)
        r=self.client.post('/internal/v1/decisions/',self.request(),content_type='application/json',HTTP_AUTHORIZATION='Bearer '+TOKEN)
        self.assertEqual(r.status_code,200);self.assertEqual(r.json()['status'],'NO_FILL')
    def test_csrf_operator_route_not_exempt(self):
        from django.test import Client
        self.assertEqual(Client(enforce_csrf_checks=True).post('/sandbox/decisions/',{},content_type='application/json').status_code,403)

class BiddingTests(PolicyMixin,TestCase):
    def test_related_beats_higher_general(self):
        related=self.campaign(bid=100,general=False);self.campaign(bid=10000,template='drinks')
        d=e.decide(self.request());self.assertEqual(d.campaign_id,related.pk);self.assertEqual(d.snapshot['selection_reason'],'RELATED')
    def test_general_fallback_and_no_fill_idempotency(self):
        req=self.request(signals=[]);d=e.decide(req);self.assertEqual(d.status,'NO_FILL')
        c=self.campaign();self.assertEqual(e.decide(req).pk,d.pk)
        n=e.decide(self.request(signals=[]));self.assertEqual(n.campaign_id,c.pk);self.assertEqual(n.snapshot['selection_reason'],'GENERAL_NO_CONTEXT')
    def test_tie_order_and_advertiser_representative(self):
        c=self.campaign(bid=500,cid=uuid.UUID(int=2))
        low=self.campaign(bid=300,cid=uuid.UUID(int=1),advertiser=c.advertiser)
        other=self.campaign(bid=500,cid=uuid.UUID(int=3))
        d=e.decide(self.request());self.assertEqual(d.campaign_id,c.pk)
        self.assertEqual(len(d.snapshot['representative_campaign_ids']),2)
        self.assertNotIn(str(low.pk),d.snapshot['representative_campaign_ids'])
    def test_high_bid_blocked_pending_paused_ended_excluded(self):
        c=self.campaign(bid=10000);Campaign.objects.filter(pk=c.pk).update(blocked=True)
        self.assertEqual(e.decide(self.request()).status,'NO_FILL')
        Campaign.objects.filter(pk=c.pk).update(blocked=False,enabled=False)
        self.assertEqual(e.decide(self.request()).status,'NO_FILL')
        Campaign.objects.filter(pk=c.pk).update(enabled=True,pending_change=True)
        self.assertEqual(e.decide(self.request()).status,'NO_FILL')
        Campaign.objects.filter(pk=c.pk).update(pending_change=False)
        self.assertEqual(e.decide(self.request(),now=self.now+timedelta(days=3)).status,'NO_FILL')
    def test_product_duplicate_and_brand_not_category_exclusion(self):
        c=self.campaign(product='danawa:42');d=e.decide(self.request(organic_product_ids=['danawa:42']))
        self.assertEqual(d.status,'NO_FILL');self.assertEqual(d.diagnostics['ORGANIC_DUPLICATE'],1)
        other=self.campaign(bid=100);self.assertEqual(e.decide(self.request(organic_product_ids=['danawa:42'])).campaign_id,other.pk)
    def test_revision_change_pauses_and_history_immutable(self):
        c=self.campaign();r=c.active_revision;r.title='overwritten'
        with self.assertRaises(ValidationError):r.save()
        r.pk=None;r._state.adding=True;r.status='PENDING';r.asset_sha256='';r.save()
        c.refresh_from_db();self.assertTrue(c.pending_change);self.assertFalse(c.enabled)
        self.assertEqual(r.version,2);self.assertEqual(e.decide(self.request()).status,'NO_FILL')
        e.review_revision(r.pk,False,'operator','보완 필요')
        self.assertEqual(Revision.objects.filter(campaign=c,status='APPROVED').count(),1)
        self.assertTrue(Audit.objects.filter(operation='REJECTED').exists())
    def test_changed_asset_stops_new_and_old_display(self):
        c=self.campaign();d=e.decide(self.request())
        with patch('advertising.engine.banner',return_value={**banner('desk'),'sha256':'changed'}):
            self.assertEqual(e.decide(self.request()).status,'NO_FILL')
            self.assertFalse(e.public_decision(d,TestPolicy.load())['displayable'])
    def test_expiration_hides_without_reauction(self):
        self.campaign();req=self.request();d=e.decide(req)
        late=d.expires_at+timedelta(seconds=1)
        self.assertEqual(e.decide(req,now=late).pk,d.pk)
        self.assertFalse(e.public_decision(d,TestPolicy.load(),late)['displayable'])
    def test_request_alias_conflicts_and_ownership(self):
        d=e.decide(req:=self.request());alias={**req,'request_id':str(uuid.uuid4())}
        self.assertEqual(e.decide(alias).pk,d.pk)
        self.assertAdError('IDEMPOTENCY_CONFLICT',e.decide,{**req,'placement':'RESULT_RIGHT'})
        self.assertAdError('CONTEXT_CONFLICT',e.decide,{**alias,'request_id':str(uuid.uuid4()),'signals':[]})
        self.assertAdError('FORBIDDEN',e.own_decision,d.pk,'other-anonymous-browser-00001')
    def test_policy_versions_immutable_and_frozen_timers(self):
        d=e.decide(self.request());self.configure(decision_ttl_seconds=1)
        self.assertAdError('POLICY_VERSION_CONFLICT',e.decide,self.request())
        self.configure(version='test-local-2')
        self.assertEqual(e.saved_policy(d).decision_ttl_seconds,600)
        self.assertEqual(e.decide(self.request()).policy_version,'test-local-2')
    def test_approved_default_is_not_a_campaign_win(self):
        a=banner('desk');DefaultNotice.objects.create(template_id='desk',title='안내',approved=True,active=True,asset_sha256=a['sha256'])
        d=e.decide(self.request());self.assertEqual(d.status,'DEFAULT');self.assertIsNone(d.campaign_id)
        self.assertEqual(e.report()['rows'],[])
    def test_default_notice_cannot_bypass_asset_limits(self):
        self.configure(asset_min_width=1000)
        DefaultNotice.objects.create(template_id='desk',title='안내',approved=True,active=True,asset_sha256=banner('desk')['sha256'])
        self.assertEqual(e.decide(self.request()).status,'NO_FILL')
    def test_public_response_hides_competitor_private_fields(self):
        self.campaign();d=e.decide(self.request());public=json.dumps(e.public_decision(d,TestPolicy.load()))
        for forbidden in ['candidate_bids','visitor_hash','destination','bid_points','advertiser_name']:
            self.assertNotIn(forbidden,public)

class EventTests(PolicyMixin,TestCase):
    def setUp(self):super().setUp();self.c=self.campaign();self.d=e.decide(self.request(),now=self.now)
    def test_render_event_replay_and_conflicting_payload(self):
        d,event=self.event(self.d,'RENDERED',rendered=True)
        body={'event_id':str(event.pk),'visitor_id':VISITOR,**event.payload}
        self.assertEqual(e.record_event(d.pk,body,now=self.now)[1].pk,event.pk)
        self.assertAdError('EVENT_CONFLICT',e.record_event,d.pk,{**body,'kind':'LOAD_ERROR'})
        self.assertEqual(Event.objects.count(),1)
    def test_fast_click_pending_then_render_no_implicit_visible(self):
        _,click=self.event(self.d,'CLICK');self.assertEqual(click.status,'PENDING')
        self.event(self.d,'RENDERED',rendered=True,at=self.now+timedelta(milliseconds=150))
        click.refresh_from_db();self.d.refresh_from_db()
        self.assertEqual(click.status,'VALID');self.assertIsNotNone(self.d.clicked_at);self.assertIsNone(self.d.visible_at)
    def test_join_timeout_is_diagnostic(self):
        _,click=self.event(self.d,'CLICK')
        self.event(self.d,'RENDERED',rendered=True,at=self.now+timedelta(seconds=11))
        click.refresh_from_db();self.assertEqual(click.reason,'RENDER_JOIN_EXPIRED')
    def test_pending_without_render_can_expire(self):
        _,click=self.event(self.d,'CLICK');e.reconcile(self.d,TestPolicy.load(),self.now+timedelta(seconds=11))
        click.refresh_from_db();self.assertEqual(click.status,'DIAGNOSTIC')
    def test_visibility_conditions_and_duplicate(self):
        self.event(self.d,'RENDERED',rendered=True)
        for fraction,ms,active in [(.49,1000,True),(.5,999,True),(.5,1000,False)]:
            _,event=self.event(self.d,'VISIBLE',fraction=fraction,continuous_ms=ms,active=active)
            self.assertEqual(event.status,'DIAGNOSTIC')
        _,valid=self.event(self.d,'VISIBLE',fraction=.5,continuous_ms=1000,active=True)
        _,duplicate=self.event(self.d,'VISIBLE',fraction=1,continuous_ms=1500,active=True)
        self.assertEqual(valid.status,'VALID');self.assertEqual(duplicate.status,'DUPLICATE')
    def test_load_error_not_impression(self):
        self.event(self.d,'LOAD_ERROR');self.d.refresh_from_db();self.assertIsNone(self.d.rendered_at)
    def test_pause_safe_navigation_but_not_valid_metric(self):
        Campaign.objects.filter(pk=self.c.pk).update(enabled=False)
        d,event=self.event(self.d,'CLICK');answer=e.event_response(d,event,self.now)
        self.assertTrue(answer['navigation']['allowed']);self.assertFalse(answer['cost_applied'])
        self.assertEqual(event.status,'DIAGNOSTIC');self.assertEqual(answer['navigation']['reason'],'SAFE_ENDED_OR_PAUSED')
    def test_block_forbids_old_navigation(self):
        Campaign.objects.filter(pk=self.c.pk).update(blocked=True)
        d,event=self.event(self.d,'CLICK');self.assertFalse(e.event_response(d,event,self.now)['navigation']['allowed'])
    def test_expired_redirect_only_internal_notice(self):
        d,event=self.event(self.d,'CLICK',at=self.d.expires_at+timedelta(seconds=1))
        self.assertEqual(e.event_response(d,event,self.d.expires_at+timedelta(seconds=1))['navigation']['url'],'/test-ended/')
        self.assertEqual(event.status,'DIAGNOSTIC')
    def test_bad_event_time_and_no_owner(self):
        self.assertAdError('INVALID_TIME',e.record_event,self.d.pk,{'event_id':str(uuid.uuid4()),'visitor_id':VISITOR,
            'kind':'CLICK','occurred_at':(self.now+timedelta(days=1)).isoformat()},now=self.now)
    def test_kst_render_date_and_recompute_stability(self):
        # UTC 14:59:59 = KST 23:59:59. Click arrives next KST date, same impression cohort.
        t=datetime(2026,10,19,14,59,59,tzinfo=dt_timezone.utc)
        Decision.objects.filter(pk=self.d.pk).update(created_at=t,expires_at=t+timedelta(minutes=10))
        Campaign.objects.filter(pk=self.c.pk).update(enabled=True)
        with patch('advertising.engine.currently_serving',return_value=True):
            self.event(self.d,'RENDERED',rendered=True,at=t)
            self.event(self.d,'CLICK',at=t+timedelta(seconds=2))
        one=e.report()['rows'];two=e.report()['rows'];self.assertEqual(one,two)
        self.assertEqual(one[0]['date'],'2026-10-19');self.assertEqual(one[0]['ctr_percent'],100)
        self.assertIsNone(one[0]['visible_ctr_percent'])
    def test_api_nan_invalid_json_and_extra_destination_rejected(self):
        auth={'HTTP_AUTHORIZATION':'Bearer '+TOKEN}
        r=self.client.post('/internal/v1/decisions/',data='{"request_id":NaN}',content_type='application/json',**auth)
        self.assertEqual(r.status_code,400)
        self.assertAdError('INVALID_EVENT',e.record_event,self.d.pk,{'destination':'https://evil.test/'})

class ConcurrencyTests(PolicyMixin,TransactionTestCase):
    def test_cross_tab_atomic_frequency_claim(self):
        self.configure(frequency_limit=1);self.campaign()
        def run(body):
            close_old_connections()
            try:return e.decide(body).status
            finally:close_old_connections()
        with ThreadPoolExecutor(max_workers=2) as pool:statuses=list(pool.map(run,[self.request(),self.request()]))
        self.assertCountEqual(statuses,['SELECTED','NO_FILL'])
    def test_parallel_same_request_single_decision(self):
        self.campaign();request=self.request()
        def run(_):
            close_old_connections()
            try:return str(e.decide(request).pk)
            finally:close_old_connections()
        with ThreadPoolExecutor(max_workers=2) as pool:ids=list(pool.map(run,range(2)))
        self.assertEqual(len(set(ids)),1);self.assertEqual(Decision.objects.count(),1)

class IntegrationTests(PolicyMixin,TestCase):
    def test_application_example_decision_and_expired_click_round_trip(self):
        import runpy
        from pathlib import Path
        from .integration import AdServerClient
        helpers=runpy.run_path(str(Path(__file__).resolve().parents[1]/'examples'/'application_proxy_example.py'))
        client=AdServerClient('http://internal-ad:8004',TOKEN,timeout_seconds=.5,public_base_url='https://public-ads.example')
        self.campaign()
        def relay(path,payload=None):
            response=self.client.post(path,payload,content_type='application/json',HTTP_AUTHORIZATION='Bearer '+TOKEN)
            self.assertEqual(response.status_code,200,response.content)
            return response.json()
        with patch.object(client,'request',side_effect=relay):
            result=helpers['display_for_owned_context'](client,{'recommendations':{'items':[]}},
                request_id=uuid.uuid4(),visitor_id=VISITOR,context_id=uuid.uuid4(),slot_id='editor-right',confirmed_signals=[])
            self.assertTrue(result['displayable'])
            self.assertNotIn('events_path',result)
            self.assertTrue(result['creative']['image_url'].startswith('https://public-ads.example/'))
            Decision.objects.filter(pk=result['decision_id']).update(expires_at=timezone.now()-timedelta(seconds=1))
            answer=helpers['event_for_owned_decision'](client,result['decision_id'],visitor_id=VISITOR,
                event={'event_id':str(uuid.uuid4()),'kind':'CLICK','occurred_at':timezone.now().isoformat()})
            self.assertEqual(answer['navigation']['url'],'https://public-ads.example/test-ended/')
            self.assertFalse(answer['cost_applied'])
            with self.assertRaises(ValueError):
                helpers['event_for_owned_decision'](client,result['decision_id'],visitor_id=VISITOR,event={'visitor_id':'other'})

    def test_public_origin_and_click_projection(self):
        from .integration import AdServerClient
        c=AdServerClient('http://ad-internal:8004',TOKEN,timeout_seconds=.5,public_base_url='https://ads.example')
        for path in ['/test-ended/','/test-destination/example/']:
            raw={'status':'DIAGNOSTIC','operator_trace':'private','navigation':{'allowed':True,'url':path,'reason':'SAFE'}}
            with patch.object(c,'request',return_value=raw):
                result=c.event(uuid.uuid4(),{})
            self.assertEqual(result['navigation']['url'],'https://ads.example'+path)
            self.assertNotIn('operator_trace',result)
            self.assertEqual(raw['navigation']['url'],path)
        self.campaign();d=e.decide(self.request())
        self.assertTrue(c.browser_payload(e.public_decision(d,TestPolicy.load()))['creative']['image_url'].startswith('https://ads.example/creative/'))

    def test_navigation_invalid_or_denied_is_not_followed(self):
        from .integration import AdServerClient
        c=AdServerClient('http://ad:8004',TOKEN,timeout_seconds=.5)
        for url in ['//evil.test/','javascript:alert(1)','http://evil.test/','/admin/','/test-destination/../admin/',
                    '/test-destination/%2e%2e/','https://good.test@evil.test/','https://evil.test\\x','https://evil.test/\n',None]:
            result=c.browser_event_payload({'navigation':{'allowed':True,'url':url}})
            self.assertFalse(result['navigation']['allowed'],url)
            self.assertIsNone(result['navigation']['url'])
        result=c.browser_event_payload({'navigation':{'allowed':False,'url':'https://approved.example/'}})
        self.assertFalse(result['navigation']['allowed'])
        self.assertEqual(c.browser_event_payload({'navigation':{'allowed':True,'url':'https://approved.example/product?id=1'}})['navigation']['url'],
                         'https://approved.example/product?id=1')

    def test_ui_slot_alias_and_unsupported_slots(self):
        from .integration import normalize_placement,recommendation_context
        self.assertEqual(normalize_placement('editor-right'),'EDITOR_RIGHT')
        body=recommendation_context({},request_id=uuid.uuid4(),visitor_id=VISITOR,context_id=uuid.uuid4(),placement='result-right',confirmed_signals=[])
        self.assertEqual(body['placement'],'RESULT_RIGHT')
        for value in ['my-right','mobile',None,[]]:
            with self.assertRaises(ValueError):normalize_placement(value)

    def test_client_rejects_invalid_configuration_and_response(self):
        from .integration import AdServerClient
        from unittest.mock import MagicMock
        for timeout in [float('nan'),float('inf'),True,0,-1]:
            with self.assertRaises(ValueError):AdServerClient('http://ad',TOKEN,timeout_seconds=timeout)
        for origin in ['http://ad/path','http://user:pass@ad','http://ad/#fragment','http://ad\n','http://ad:invalid']:
            with self.assertRaises(ValueError):AdServerClient(origin,TOKEN,timeout_seconds=.5)
        c=AdServerClient('http://ad',TOKEN,timeout_seconds=.5)
        for raw in [b'[]',b'x'*131073,b'not json']:
            response=MagicMock();response.__enter__.return_value.read.return_value=raw
            with patch('advertising.integration.urlopen',return_value=response):
                self.assertEqual(c.display({})['status'],'UNAVAILABLE')

    def test_display_strips_server_fields(self):
        from .integration import AdServerClient
        c=AdServerClient('http://ad',TOKEN,timeout_seconds=.5)
        with patch.object(c,'decide',return_value={'status':'NO_FILL','displayable':False,'operator_trace':{'secret':TOKEN}}):
            self.assertEqual(c.display({}),{'status':'NO_FILL','displayable':False})

    def test_recommendation_state_unchanged_and_no_inference(self):
        from .integration import recommendation_context
        state={'context':{'search_terms':'private text'},'recommendations':{'items':[{'product_id':'danawa:1'}],
            'unverified_items':[{'product_id':'danawa:unknown'}]}}
        original=json.dumps(state)
        r=recommendation_context(state,request_id=uuid.uuid4(),visitor_id=VISITOR,context_id=uuid.uuid4(),
            placement='RESULT_RIGHT',confirmed_signals=[])
        self.assertEqual(r['organic_product_ids'],['danawa:1']);self.assertEqual(r['signals'],[])
        self.assertEqual(json.dumps(state),original);self.assertNotIn('private text',json.dumps(r))
    def test_backend_client_timeout_and_no_fill_separation(self):
        from .integration import AdServerClient
        from urllib.error import URLError,HTTPError
        client=AdServerClient('http://127.0.0.1:8004',TOKEN,timeout_seconds=.2)
        with patch('advertising.integration.urlopen',side_effect=URLError('timeout')):
            self.assertEqual(client.decide(self.request())['status'],'UNAVAILABLE')
        with patch('advertising.integration.urlopen',side_effect=HTTPError('',401,'denied',{},None)):
            self.assertEqual(client.decide(self.request())['status'],'ERROR')
    def test_browser_projection_absolute_image_and_no_internal_path(self):
        from .integration import AdServerClient
        c=AdServerClient('http://127.0.0.1:8004',TOKEN,timeout_seconds=.5)
        self.campaign();d=e.decide(self.request());result=c.browser_payload(e.public_decision(d,TestPolicy.load()))
        self.assertTrue(result['creative']['image_url'].startswith(f'http://127.0.0.1:8004/creative/{d.pk}/'))
        self.assertNotIn('events_path',result);self.assertNotIn(TOKEN,json.dumps(result))
    def test_signed_image_checks_current_block_and_expiry(self):
        c=self.campaign();d=e.decide(self.request());path=e.public_decision(d,TestPolicy.load())['creative']['image_url']
        self.assertEqual(self.client.get(path).status_code,200)
        Campaign.objects.filter(pk=c.pk).update(blocked=True)
        self.assertEqual(self.client.get(path).status_code,410)
        Campaign.objects.filter(pk=c.pk).update(blocked=False)
        Decision.objects.filter(pk=d.pk).update(expires_at=self.now-timedelta(seconds=1))
        self.assertEqual(self.client.get(path).status_code,410)
        self.assertEqual(self.client.get(path.replace('/creative/','/creative/',1).rstrip('/')+'tampered/').status_code,404)

class UICompatibilityTests(PolicyMixin,TestCase):
    def setUp(self):
        super().setUp()
        self.auth={'HTTP_AUTHORIZATION':'Bearer '+TOKEN}

    def draft(self,**changes):
        return {'campaign_id':'plant-demo','title':'UI 광고 초안','description':'설명','advertiser':'로컬 광고주',
                'creative':'ops/ad_creatives/unused.webp','landing_url':'/test-destination/ui-example/',
                'category':'plant','slot_id':'result-right','bid_units':'500','active':'on','review_status':'APPROVED',
                'starts_at':'2026-10-19T09:00','ends_at':'2026-10-20T09:00','template_id':'desk',
                'revision':'3','content_version':'1','reviewed_version':'1',**changes}

    def test_ui_contract_is_authenticated_and_not_ui_connection_claim(self):
        path='/internal/v1/ui/contract/'
        self.assertEqual(self.client.get(path).status_code,401)
        with self.assertNumQueries(0):result=self.client.get(path,**self.auth).json()
        self.assertEqual(result['slots']['editor-right'],'EDITOR_RIGHT')
        self.assertFalse(result['local_ui_approval_transferred'])
        self.assertEqual(result['unsupported_slots'],['my-right'])

    def test_ui_decision_alias_and_idempotency(self):
        self.campaign()
        request=self.request();request['slot_id']='result-right';request.pop('placement')
        first=self.client.post('/internal/v1/ui/decisions/',request,content_type='application/json',**self.auth)
        second=self.client.post('/internal/v1/ui/decisions/',request,content_type='application/json',**self.auth)
        self.assertEqual(first.status_code,200)
        self.assertEqual(first.json()['decision_id'],second.json()['decision_id'])
        self.assertEqual(first.json()['slot_id'],'result-right')
        self.assertEqual(first.json()['placement'],'RESULT_RIGHT')
        self.assertEqual(Decision.objects.count(),1)

    def test_ui_unsupported_slot_and_media_no_decision(self):
        for changes in [{'slot_id':'my-right'},{'slot_id':'editor-right','media_id':'other'},
                        {'slot_id':'editor-right','category':'desk'}]:
            request=self.request();request.pop('placement');request.update(changes)
            result=self.client.post('/internal/v1/ui/decisions/',request,content_type='application/json',**self.auth)
            self.assertEqual(result.status_code,422)
        self.assertEqual(Decision.objects.count(),0)

    def test_campaign_validation_no_db_write_no_approval_transfer(self):
        with self.assertNumQueries(0):
            response=self.client.post('/internal/v1/ui/campaigns/validate/',self.draft(),content_type='application/json',**self.auth)
        self.assertEqual(response.status_code,200,response.content)
        result=response.json()
        self.assertEqual(result['revision_draft']['bid_points'],500)
        self.assertEqual(result['revision_draft']['placements'],['RESULT_RIGHT'])
        self.assertTrue(result['revision_draft']['starts_at'].endswith('+09:00'))
        self.assertEqual(result['required_review_status'],'PENDING')
        self.assertFalse(result['activation_allowed']);self.assertFalse(result['persisted'])
        self.assertFalse(result['category_mapping_applied'])
        self.assertNotIn('ops/ad_creatives',json.dumps(result))
        self.assertEqual(Campaign.objects.count(),0)

    def test_pending_hold_rejected_never_become_approved(self):
        from .ui_contract import campaign_preview
        for status in ['PENDING','HOLD','REJECTED','APPROVED']:
            result=campaign_preview(self.draft(review_status=status))
            self.assertEqual(result['required_review_status'],'PENDING')
            self.assertFalse(result['activation_allowed'])

    def test_validation_rejects_bad_bid_period_slot_template(self):
        changes=[{'bid_units':True},{'bid_units':'1.5'},{'bid_units':10001},{'bid_units':'1.0'},
                 {'ends_at':'2026-10-19T08:00'},{'slot_id':'my-right'},{'template_id':None},
                 {'review_status':'INVALID'},{'active':'maybe'},{'category':'unknown'},
                 {'landing_url':'https://not-approved.example/'},{'title':''}]
        for change in changes:
            with self.subTest(change=change):
                response=self.client.post('/internal/v1/ui/campaigns/validate/',self.draft(**change),content_type='application/json',**self.auth)
                self.assertEqual(response.status_code,422,response.content)
        self.assertEqual(Campaign.objects.count(),0)

    def test_upload_never_read_and_category_does_not_select_template(self):
        with patch('urllib.request.urlopen',side_effect=AssertionError('Do not fetch UI images')):
            response=self.client.post('/internal/v1/ui/campaigns/validate/',self.draft(creative='https://example.com/image'),content_type='application/json',**self.auth)
        self.assertEqual(response.status_code,200)
        without=self.draft();without.pop('template_id')
        response=self.client.post('/internal/v1/ui/campaigns/validate/',without,content_type='application/json',**self.auth)
        self.assertEqual(response.json()['error']['code'],'TEMPLATE_SELECTION_REQUIRED')

    def test_client_ui_response_normalizes_public_image_and_click(self):
        from .integration import AdServerClient
        self.campaign()
        client=AdServerClient('http://private-ad',TOKEN,timeout_seconds=.5,public_base_url='https://public-ad.example')
        def relay(path,payload):
            result=self.client.post(path,payload,content_type='application/json',**self.auth)
            self.assertEqual(result.status_code,200,result.content)
            return result.json()
        request=self.request();request.pop('placement');request['slot_id']='editor-right'
        with patch.object(client,'request',side_effect=relay):
            result=client.ui_display(request)
            self.assertEqual(result['media_id'],'sketchon')
            self.assertTrue(result['creative']['image_url'].startswith('https://public-ad.example/creative/'))
            self.assertNotIn('events_path',result)
            response=client.event(result['decision_id'],{'event_id':str(uuid.uuid4()),'visitor_id':VISITOR,
                                 'kind':'CLICK','occurred_at':timezone.now().isoformat()})
            self.assertEqual(response['navigation']['url'],'https://public-ad.example/test-destination/sample/')

    def test_compatibility_routes_disabled_by_default(self):
        with override_settings(AD_TEST_OPERATIONS=False):
            self.assertEqual(self.client.get('/internal/v1/ui/contract/',**self.auth).status_code,503)
            self.assertEqual(self.client.post('/internal/v1/ui/campaigns/validate/',self.draft(),content_type='application/json',**self.auth).status_code,503)


class AdministrationTests(PolicyMixin,TestCase):
    def setUp(self):
        super().setUp()
        from django.contrib.auth import get_user_model
        # Test database only; no persistent credentials or operator account created.
        self.user=get_user_model().objects.create_superuser(username='isolated-test-operator')
        self.client.force_login(self.user)
    def test_guided_advertiser_save_prefills_next_contract(self):
        response=self.client.post('/admin/advertising/advertiser/add/?guided=1',{
            'name':'단계 안내 광고주','status':'ACTIVE','_next_step':'1','_guided':'1'})
        self.assertEqual(response.status_code,302)
        advertiser=Advertiser.objects.get(name='단계 안내 광고주')
        self.assertIn('/mockcontract/add/',response.url)
        self.assertIn(str(advertiser.pk),response.url)
        form=self.client.get(response.url).context['adminform'].form
        self.assertEqual(str(form.initial['advertiser']),str(advertiser.pk))
        self.assertIn('모의 계약',form.initial['reference'])
        self.assertEqual(MockContract.objects.count(),0)

    def test_guided_inactive_advertiser_never_automatically_activates(self):
        response=self.client.post('/admin/advertising/advertiser/add/?guided=1',{
            'name':'대기 광고주','status':'PENDING','_next_step':'1'})
        advertiser=Advertiser.objects.get(name='대기 광고주')
        self.assertEqual(advertiser.status,'PENDING')
        self.assertIn('/advertiser/'+str(advertiser.pk)+'/change/',response.url)
        self.assertEqual(MockContract.objects.count(),0)

    def test_guided_contract_keeps_advertiser_and_prefills_campaign(self):
        advertiser=Advertiser.objects.create(name='연결 확인',status='ACTIVE')
        data={'reference':'연결 계약','advertiser':str(advertiser.pk),'status':'APPROVED',
              'starts_at_0':'2026-10-01','starts_at_1':'00:00:00','ends_at_0':'2026-12-01','ends_at_1':'00:00:00',
              '_next_step':'1'}
        response=self.client.post('/admin/advertising/mockcontract/add/?guided=1',data)
        self.assertEqual(response.status_code,302)
        contract=MockContract.objects.get(reference='연결 계약')
        form=self.client.get(response.url).context['adminform'].form
        self.assertEqual(str(form.initial['contract']),str(contract.pk))
        self.assertEqual(str(form.initial['advertiser']),str(advertiser.pk))
        self.assertEqual(Campaign.objects.count(),0)

    def test_guided_revision_defaults_are_visible_not_automatic_submission(self):
        c=self.campaign()
        response=self.client.get('/admin/advertising/revision/add/?guided=1&campaign='+str(c.pk))
        form=response.context['adminform'].form
        self.assertEqual(form.initial['bid_points'],900)
        self.assertEqual(form.initial['placements'],['EDITOR_RIGHT','RESULT_RIGHT'])
        self.assertEqual(Revision.objects.count(),1)
        self.assertContains(response,'제출하고 심사 화면으로')

    def test_single_review_post_approves_without_activating_and_replay_safe(self):
        c=self.campaign();prior=c.active_revision
        fields={f.name:getattr(prior,f.name) for f in Revision._meta.fields if f.editable and f.name not in ('id','campaign')}
        submitted=Revision.objects.create(campaign=c,**fields)
        url=f'/admin/advertising/revision/{submitted.pk}/review/'
        self.assertEqual(self.client.get(url).status_code,405)
        audit_count=Audit.objects.count()
        for reason in ('','   ','x','x'*501):
            invalid=self.client.post(url,{'decision':'APPROVED','_review_reason':reason},follow=True)
            self.assertContains(invalid,'심사 결과를 저장하지 않았습니다.')
            self.assertContains(invalid,'id="review-panel"')
            submitted.refresh_from_db();self.assertEqual(submitted.status,'PENDING')
            self.assertEqual(Audit.objects.count(),audit_count)
        history=self.client.get(f'/admin/advertising/revision/{submitted.pk}/history/')
        self.assertContains(history,'아직 심사 기록이 없습니다.')
        response=self.client.post(url,{'decision':'APPROVED','_review_reason':'제출 내용 확인'})
        self.assertIn('/campaign/'+str(c.pk)+'/change/',response.url)
        submitted.refresh_from_db();c.refresh_from_db()
        self.assertEqual(submitted.status,'APPROVED');self.assertFalse(c.enabled)
        history=self.client.get(f'/admin/advertising/revision/{submitted.pk}/history/')
        self.assertContains(history,'제출 내용 확인')
        self.assertContains(history,'광고 심사 이력')
        again=self.client.post(url,{'decision':'APPROVED','_review_reason':'반복 요청'})
        self.assertEqual(again.status_code,302)
        self.assertEqual(Audit.objects.filter(operation='APPROVED',object_id=str(submitted.pk)).count(),1)

    def test_single_review_permission_and_csrf_enforced(self):
        c=self.campaign()
        from django.contrib.auth import get_user_model
        limited=get_user_model().objects.create_user(username='no-review',is_staff=True)
        self.client.force_login(limited)
        self.assertEqual(self.client.post(f'/admin/advertising/revision/{c.active_revision_id}/review/',{
            'decision':'APPROVED','_review_reason':'권한 없음'}).status_code,403)
        from django.test import Client
        guarded=Client(enforce_csrf_checks=True);guarded.force_login(self.user)
        self.assertEqual(guarded.post(f'/admin/advertising/revision/{c.active_revision_id}/review/',{
            'decision':'APPROVED','_review_reason':'토큰 없음'}).status_code,403)

    def test_guided_enable_redirects_to_preview_without_forcing_selection(self):
        c=self.campaign()
        response=self.client.post(f'/admin/advertising/campaign/{c.pk}/change/?guided=1',{
            'name':c.name,'enabled':'on','_next_step':'1'})
        self.assertEqual(response.status_code,302)
        self.assertEqual(response.url,'/sandbox/?campaign='+str(c.pk))
        page=self.client.get(response.url)
        self.assertContains(page,'지금 확인할 캠페인')
        self.assertEqual(Decision.objects.count(),0)

    def test_guided_save_does_not_enable_approved_campaign_implicitly(self):
        c=self.campaign();Campaign.objects.filter(pk=c.pk).update(enabled=False)
        response=self.client.post(f'/admin/advertising/campaign/{c.pk}/change/?guided=1',{
            'name':c.name,'_next_step':'1'})
        c.refresh_from_db();self.assertFalse(c.enabled)
        self.assertIn('/campaign/'+str(c.pk)+'/change/',response.url)
    def test_korean_bid_summary_uses_snapshot_and_preserves_private_boundary(self):
        high=self.campaign(bid=700);low=self.campaign(bid=500)
        result=self.client.post('/sandbox/decisions/',self.request(),content_type='application/json').json()
        summary=result['summary']
        self.assertEqual(summary['bid_points'],700)
        self.assertEqual(summary['candidate_count'],2)
        self.assertEqual([row['bid_points'] for row in summary['bids']],[700,500])
        self.assertEqual(summary['bids'][0]['outcome'],'선정')
        self.assertIn('관련된 광고',summary['explanation'])
        Revision.objects.filter(pk=high.active_revision_id).update(bid_points=1)
        from .sandbox_ui import decision_summary
        d=Decision.objects.get(pk=result['decision_id'])
        self.assertEqual(decision_summary(d,True)['bid_points'],700)
        internal=self.client.post('/internal/v1/decisions/',self.request(),content_type='application/json',HTTP_AUTHORIZATION='Bearer '+TOKEN).json()
        self.assertNotIn('summary',internal)
        self.assertNotIn('operator_trace',internal)

    def test_bid_summary_same_advertiser_rep_and_empty_state(self):
        high=self.campaign(bid=700);self.campaign(bid=500,advertiser=high.advertiser)
        result=self.client.post('/sandbox/decisions/',self.request(),content_type='application/json').json()['summary']
        self.assertEqual(result['representative_count'],1)
        self.assertEqual(result['bids'][1]['outcome'],'같은 광고주 대표에서 제외')
        Campaign.objects.update(enabled=False)
        data=self.client.post('/sandbox/decisions/',self.request(),content_type='application/json').json()
        self.assertEqual(data['status'],'NO_FILL');self.assertEqual(data['summary']['bids'],[])
        self.assertIsNone(data['summary']['bid_points'])
        self.assertIn('집행 중지',data['summary']['excluded'][0]['label'])

    def test_readable_report_counts_canonical_events_and_no_zero_denominator(self):
        self.campaign();d=e.decide(self.request(),now=self.now)
        first=self.client.get('/sandbox/report/').json()['summary']
        self.assertEqual(first['rendered'],0);self.assertIsNone(first['ctr_percent'])
        self.event(d,'RENDERED',rendered=True);self.event(d,'RENDERED',rendered=True)
        self.event(d,'CLICK');self.event(d,'CLICK')
        result=self.client.get('/sandbox/report/').json()['summary']
        self.assertEqual(result['rendered'],1);self.assertEqual(result['clicked'],1)
        self.assertEqual(result['ctr_percent'],100)
        self.assertEqual(result['rows'][0]['slot_label'],'편집 오른쪽')

    def test_event_summary_is_korean_and_current_markers(self):
        self.campaign();d=e.decide(self.request(),now=self.now)
        response=self.client.post(f'/sandbox/decisions/{d.pk}/events/',{
            'event_id':str(uuid.uuid4()),'visitor_id':VISITOR,'kind':'RENDERED','rendered':True,
            'occurred_at':timezone.now().isoformat()},content_type='application/json')
        data=response.json()
        self.assertEqual(data['summary']['kind_label'],'이미지 표시')
        self.assertEqual(data['summary']['state_label'],'집계 인정')
        self.assertTrue(data['summary']['rendered']);self.assertFalse(data['summary']['clicked'])

    def test_sandbox_options_localized_and_raw_json_collapsed(self):
        response=self.client.get('/sandbox/')
        self.assertContains(response,'사진에서 확인된 물체')
        self.assertContains(response,'편집 화면 오른쪽')
        self.assertContains(response,'개발용 상세 정보')
        self.assertNotContains(response,'<option>CONFIRMED_TEXT</option>')
        self.assertContains(response,'노출')
    def test_korean_home_and_display_labels_keep_values_unchanged(self):
        response=self.client.get('/admin/')
        for label in ['광고주 등록','모의 계약 등록','캠페인 만들기','광고 버전 제출','광고 심사','집행 켜기']:
            self.assertContains(response,label)
        response=self.client.get('/admin/advertising/advertiser/add/')
        form=response.context['adminform'].form
        self.assertEqual(form.fields['status'].label,'상태')
        self.assertIn(('ACTIVE','사용 중'),list(form.fields['status'].choices))
        self.assertEqual(Advertiser._meta.get_field('name').name,'name')
        self.assertEqual(Advertiser._meta.get_field('status').choices[1],('ACTIVE','ACTIVE'))

    def test_checkbox_revision_submission_preserves_review_boundary(self):
        c=self.campaign()
        response=self.client.get('/admin/advertising/revision/add/')
        form=response.context['adminform'].form
        from django.forms import CheckboxSelectMultiple
        self.assertIsInstance(form.fields['placements'].widget,CheckboxSelectMultiple)
        self.assertEqual(form.fields['bid_points'].label,'가상 입찰 포인트')
        response=self.client.post('/admin/advertising/revision/add/',{
            'campaign':str(c.pk),'template_id':'desk','kind':'BRAND','title':'한글 화면 제출 검증',
            'alt_text':'시연 광고','bid_points':'950','is_general':'on',
            'placements':['EDITOR_RIGHT','RESULT_RIGHT'],'destination':'/test-destination/korean/',
            'starts_at_0':'2026-10-01','starts_at_1':'00:00:00','ends_at_0':'2026-12-01','ends_at_1':'00:00:00',
            '_save':'저장'})
        self.assertEqual(response.status_code,302,response.context and response.context['adminform'].form.errors)
        submitted=Revision.objects.get(campaign=c,version=2)
        self.assertEqual(submitted.status,'PENDING');self.assertEqual(submitted.bid_points,950)
        self.assertEqual(submitted.placements,['EDITOR_RIGHT','RESULT_RIGHT'])
        c.refresh_from_db();self.assertFalse(c.enabled)

    def test_readonly_owner_labels_do_not_make_owner_editable(self):
        c=self.campaign()
        response=self.client.get(f'/admin/advertising/campaign/{c.pk}/change/')
        form=response.context['adminform'].form
        self.assertNotIn('advertiser',form.fields);self.assertNotIn('contract',form.fields)
        self.assertContains(response,'현재 승인된 버전')
        for path in ['/admin/advertising/defaultnotice/add/','/admin/advertising/policyrecord/',
                     '/admin/advertising/event/','/admin/advertising/audit/']:
            self.assertEqual(self.client.get(path).status_code,200,path)

    def test_localized_status_filter_uses_original_database_value(self):
        self.campaign()
        response=self.client.get('/admin/advertising/revision/?status__display=APPROVED')
        self.assertEqual(response.status_code,200)
        self.assertEqual(response.context['cl'].queryset.count(),1)
        self.assertContains(response,'승인')
    def test_admin_and_sandbox_pages_render_with_no_service_key(self):
        for path in ['/admin/','/admin/advertising/advertiser/','/admin/advertising/mockcontract/',
            '/admin/advertising/campaign/','/admin/advertising/revision/add/','/sandbox/']:
            response=self.client.get(path);self.assertEqual(response.status_code,200,path)
            self.assertNotIn(TOKEN,response.content.decode())
    def test_admin_is_unavailable_when_operations_off(self):
        with override_settings(AD_TEST_OPERATIONS=False):
            self.assertEqual(self.client.get('/admin/').status_code,503)
    def test_admin_approve_then_explicit_activation(self):
        c=self.campaign();previous=c.active_revision
        data={f.name:getattr(previous,f.name) for f in Revision._meta.fields if f.editable and f.name not in ('id','campaign')}
        r=Revision.objects.create(campaign=c,**data)
        response=self.client.post('/admin/advertising/revision/',{'action':'approve','_selected_action':[str(r.pk)]})
        self.assertEqual(response.status_code,302);r.refresh_from_db();c.refresh_from_db()
        self.assertEqual(r.status,'APPROVED');self.assertFalse(c.enabled);self.assertFalse(c.pending_change)
        response=self.client.get(f'/admin/advertising/revision/{r.pk}/change/')
        self.assertEqual(response.status_code,200);self.assertNotContains(response,'name="_save"')
    def test_integer_admin_field_rejects_fractional_string(self):
        from .admin import StrictPointField
        field=StrictPointField(min_value=1,max_value=10000)
        for bad in ['1.0','1.2','NaN','inf','10001',True]:
            with self.assertRaises(ValidationError):field.clean(bad)
        self.assertEqual(field.clean('100'),100)
    def test_model_invalid_bid_never_saved(self):
        c=self.campaign();prior=c.active_revision
        for bad in [True,'100',1.5,10001]:
            with self.assertRaises(ValidationError):
                Revision.objects.create(campaign=c,template_id='desk',title='invalid',alt_text='invalid',bid_points=bad,
                    placements=['EDITOR_RIGHT'],destination='/test-destination/a/',starts_at=prior.starts_at,ends_at=prior.ends_at)
        self.assertEqual(Revision.objects.count(),1)
    def test_operator_endpoint_new_context_and_report(self):
        self.campaign();response=self.client.post('/sandbox/decisions/',self.request(),content_type='application/json')
        self.assertEqual(response.status_code,200);self.assertEqual(response.json()['status'],'SELECTED')
        self.assertIn('operator_trace',response.json())
        self.assertEqual(self.client.get('/sandbox/report/').status_code,200)
    def test_seed_idempotent_and_retention_preview_safe(self):
        from django.core.management import call_command
        from io import StringIO
        stream=StringIO();call_command('seed_ads_demo',stdout=stream);call_command('seed_ads_demo',stdout=stream)
        self.assertEqual(Campaign.objects.count(),2)
        self.assertEqual(Revision.objects.count(),2)
        d=e.decide(self.request());call_command('purge_test_events',stdout=stream)
        self.assertTrue(Decision.objects.filter(pk=d.pk).exists())
    def test_rejected_newer_version_prevents_old_submission_approval(self):
        c=self.campaign();prior=c.active_revision
        fields={f.name:getattr(prior,f.name) for f in Revision._meta.fields if f.editable and f.name not in ('id','campaign')}
        older=Revision.objects.create(campaign=c,**fields);newer=Revision.objects.create(campaign=c,**fields)
        e.review_revision(newer.pk,False,'operator','거절')
        self.assertAdError('NEWER_REVISION_PENDING',e.review_revision,older.pk,True,'operator')
