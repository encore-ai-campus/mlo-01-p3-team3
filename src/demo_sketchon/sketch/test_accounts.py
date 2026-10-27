import json
from django.contrib.auth import get_user_model
from django.test import Client, TestCase
from .models import Project


class AccountTests(TestCase):
    def signup(self, client=None, username='member'):
        return (client or self.client).post('/accounts/signup/', {
            'username': username, 'nickname': '우리집',
            'password1': 'Demo-member-2468!', 'password2': 'Demo-member-2468!',
        })

    def test_signup_claim_preserves_owner(self):
        p = self.client.post('/api/projects/', {'title':'익명'}).json()
        old = Project.objects.get(pk=p['id']).owner
        response = self.signup()
        self.assertEqual(response.status_code, 302)
        project = Project.objects.get(pk=p['id'])
        self.assertEqual(project.owner, old)
        self.assertEqual(project.account.username, 'member')
        self.assertTrue(self.client.get('/api/account/status/').json()['authenticated'])

    def test_logout_cannot_read_claimed_project_and_other_account_cannot_claim(self):
        p = self.client.post('/api/projects/', {}).json()
        self.signup()
        self.assertEqual(self.client.get('/accounts/logout/').status_code, 405)
        self.client.post('/accounts/logout/')
        self.assertEqual(self.client.get(f"/api/projects/{p['id']}/").status_code, 404)
        self.signup(username='second')
        self.assertEqual(self.client.get(f"/api/projects/{p['id']}/").status_code, 404)

    def test_account_reads_own_project_from_other_session(self):
        p = self.client.post('/api/projects/', {}).json()
        self.signup()
        second = Client()
        second.post('/accounts/login/', {'username':'member','password':'Demo-member-2468!'})
        dto = second.get(f"/api/projects/{p['id']}/").json()
        self.assertEqual(dto['writer_id'], Project.objects.get(pk=p['id']).owner)
        status = second.get('/api/account/status/', {'project_id':p['id']})
        self.assertTrue(status.json()['project_access'])
        self.assertIn('no-store', status['Cache-Control'])

    def test_external_next_rejected_and_nickname_required(self):
        response = self.client.post('/accounts/signup/?next=https://evil.test/', {
            'username':'member','nickname':'  ','password1':'Demo-member-2468!', 'password2':'Demo-member-2468!'})
        self.assertEqual(response.status_code, 200)
        self.assertFalse(get_user_model().objects.filter(username='member').exists())
        self.signup()
        self.client.post('/accounts/logout/')
        response = self.client.post('/accounts/login/?next=https://evil.test/', {'username':'member','password':'Demo-member-2468!'})
        self.assertEqual(response.status_code, 302)
        self.assertEqual(response['Location'], '/')

    def test_profile_and_password_keep_session(self):
        self.signup()
        self.client.post('/my/profile/', {'action':'nickname','nickname':'새이름'})
        self.assertEqual(self.client.get('/api/account/status/').json()['nickname'], '새이름')
        self.client.post('/my/profile/', {'action':'password','old_password':'Demo-member-2468!', 'new_password1':'New-member-2468!', 'new_password2':'New-member-2468!'})
        self.assertTrue(self.client.get('/api/account/status/').json()['authenticated'])
        self.client.post('/accounts/logout/')
        self.assertEqual(self.client.post('/accounts/login/', {'username':'member','password':'New-member-2468!'}).status_code, 302)

    def test_auth_posts_require_csrf(self):
        client = Client(enforce_csrf_checks=True)
        self.assertEqual(client.post('/accounts/login/', {}).status_code, 403)
        self.assertEqual(client.post('/accounts/signup/', {}).status_code, 403)
        self.assertEqual(client.post('/accounts/logout/', {}).status_code, 403)

    def test_signup_completion_notice_once_with_authenticated_session(self):
        response = self.signup()
        self.assertEqual(response.status_code, 302)
        first = self.client.get(response['Location'])
        self.assertContains(first, '가입이 완료되었습니다!')
        self.assertTrue(self.client.get('/api/account/status/').json()['authenticated'])
        self.assertNotContains(self.client.get(response['Location']), '가입이 완료되었습니다!')

    def test_invalid_signup_and_regular_login_do_not_show_signup_notice(self):
        response = self.client.post('/accounts/signup/', {
            'username':'invalid','nickname':'','password1':'short','password2':'mismatch'})
        self.assertEqual(response.status_code, 200)
        self.assertNotContains(response, '가입이 완료되었습니다!')
        self.signup()
        self.client.get('/')
        self.client.post('/accounts/logout/')
        response = self.client.post('/accounts/login/', {'username':'member','password':'Demo-member-2468!'})
        self.assertNotContains(self.client.get(response['Location']), '가입이 완료되었습니다!')
