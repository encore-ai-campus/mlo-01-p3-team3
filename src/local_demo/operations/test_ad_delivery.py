"""Test the UI proxy using a separate test DB and no live ad/network calls."""
from unittest.mock import patch, MagicMock
from uuid import uuid4
from django.contrib.auth import get_user_model
from django.test import Client, TestCase, override_settings
from sketch.models import Project

CONFIG={'enabled':True,'internal_origin':'http://ad.test:8004','public_origin':'https://ads.example',
        'token':'isolated-service-token-for-test-0000000','timeout':1}


@override_settings(SKETCHON_AD_DELIVERY=CONFIG)
class AdDeliveryTests(TestCase):
    def setUp(self):
        session=self.client.session
        session['sketch_owner']=uuid4().hex
        session.save()
        self.project=Project.objects.create(owner=session['sketch_owner'])
        self.body={'slot_id':'editor-right','project_id':str(self.project.pk)}
        self.adapter=MagicMock()
        self.adapter.ui_display.side_effect=lambda body:{'status':'SELECTED','displayable':True,
            'decision_id':str(uuid4()),'context_id':body['context_id'],
            'creative':{'image_url':'https://ads.example/creative/example/'}}
        self.patch=patch('operations.ad_delivery.client',return_value=self.adapter)
        self.patch.start();self.addCleanup(self.patch.stop)

    def display(self,body=None,client=None):
        return (client or self.client).post('/api/ads/display/',self.body if body is None else body,content_type='application/json')

    def test_editor_owner_and_server_generated_identity(self):
        self.assertEqual(self.display().status_code,200)
        payload=self.adapter.ui_display.call_args.args[0]
        self.assertEqual(payload['signals'],[])
        self.assertEqual(payload['organic_product_ids'],[])
        self.assertNotEqual(payload['visitor_id'],self.project.owner)
        self.assertNotIn('project_id',payload)
        self.assertNotIn(CONFIG['token'],self.display().content.decode())

    def test_other_session_cannot_display_project(self):
        self.assertEqual(self.display(client=Client()).status_code,404)
        self.adapter.ui_display.assert_not_called()

    def test_browser_cannot_supply_visitor_or_destination(self):
        self.assertEqual(self.display({**self.body,'visitor_id':'forged'}).status_code,422)
        self.assertEqual(self.display({**self.body,'destination':'https://evil.test'}).status_code,422)
        self.adapter.ui_display.assert_not_called()

    def test_requests_are_stable_and_positions_separate(self):
        self.display();first=self.adapter.ui_display.call_args.args[0]
        self.display();second=self.adapter.ui_display.call_args.args[0]
        self.assertEqual(first,second)
        self.assertEqual(self.display({**self.body,'slot_id':'my-right'}).status_code,422)

    def test_csrf_required(self):
        strict=Client(enforce_csrf_checks=True)
        self.assertEqual(self.display(client=strict).status_code,403)

    def test_event_requires_prior_binding(self):
        result=self.client.post(f'/api/ads/{uuid4()}/events/',{},content_type='application/json')
        self.assertEqual(result.status_code,403)
        self.adapter.event.assert_not_called()

    def test_bound_event_injects_owner_and_rechecks_project(self):
        result=self.display().json()
        self.adapter.event.return_value={'status':'VALID'}
        data={'event_id':str(uuid4()),'kind':'RENDERED','occurred_at':'2026-10-19T00:00:00Z','rendered':True}
        path=f"/api/ads/{result['decision_id']}/events/"
        self.assertEqual(self.client.post(path,data,content_type='application/json').status_code,200)
        self.assertEqual(self.adapter.event.call_args.args[1]['visitor_id'],self.adapter.ui_display.call_args.args[0]['visitor_id'])
        self.assertEqual(self.client.post(path,{**data,'visitor_id':'forged'},content_type='application/json').status_code,422)
        self.project.owner=uuid4().hex;self.project.save()
        self.assertEqual(self.client.post(path,data,content_type='application/json').status_code,404)

    def test_login_change_rejects_old_binding(self):
        result=self.display().json()
        user=get_user_model().objects.create_user(username='ad-review-user')
        self.client.force_login(user)
        self.assertEqual(self.client.post(f"/api/ads/{result['decision_id']}/events/",{},content_type='application/json').status_code,403)

    def test_unavailable_hides_without_raising(self):
        with patch('operations.ad_delivery.client',return_value=None):
            self.assertFalse(self.display().json()['displayable'])

    def test_run_scope_uses_only_displayed_product_ids(self):
        run=str(uuid4())
        with patch('recommendations.views.owned_run',return_value=(None,'owned')) as owned, patch('recommendations.services.reader') as reader:
            reader.return_value.state.return_value={'recommendations':{'items':[{'product_id':'danawa:1'},{'product_id':'danawa:1'}], 'unverified_items':[{'product_id':'danawa:2'}]}}
            self.assertEqual(self.display({'slot_id':'result-right','run_id':run}).status_code,200)
            self.assertEqual(self.adapter.ui_display.call_args.args[0]['organic_product_ids'],['danawa:1'])
            owned.assert_called_once()

    def test_run_input_change_gets_new_context(self):
        with patch('recommendations.views.owned_run',return_value=(None,'owned')), patch('recommendations.services.reader') as reader:
            run={'slot_id':'result-right','run_id':str(uuid4())}
            reader.return_value.state.return_value={'recommendations':{'items':[{'product_id':'danawa:1'}]}}
            self.display(run);first=self.adapter.ui_display.call_args.args[0]['context_id']
            reader.return_value.state.return_value={'recommendations':{'items':[{'product_id':'danawa:2'}]}}
            self.display(run);self.assertNotEqual(first,self.adapter.ui_display.call_args.args[0]['context_id'])

    def test_waiting_run_does_not_call_ad_server(self):
        with patch('recommendations.views.owned_run',return_value=(None,'owned')), patch('recommendations.services.reader') as reader:
            reader.return_value.state.return_value={'job':{'status':'RUNNING'}}
            self.assertEqual(self.display({'slot_id':'result-right','run_id':str(uuid4())}).json()['status'],'WAITING')
            self.adapter.ui_display.assert_not_called()

    def test_wrong_context_response_not_bound(self):
        self.adapter.ui_display.side_effect=None
        self.adapter.ui_display.return_value={'context_id':str(uuid4()),'decision_id':str(uuid4()),'displayable':True}
        self.assertFalse(self.display().json()['displayable'])
        self.assertNotIn('ad_delivery_bindings',self.client.session)
