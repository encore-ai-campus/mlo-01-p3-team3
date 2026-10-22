"""The dev recommendation editor and the #17 operations console coexist."""
from django.contrib.auth import get_user_model
from django.test import TestCase


class AdminRecommendationIntegrationTests(TestCase):
    def test_dev_editor_keeps_stickers_ops_navigation_and_visit_flow(self):
        operator = get_user_model().objects.create_user(
            "integration-operator", is_staff=True, is_superuser=True
        )
        self.client.force_login(operator)
        home = self.client.get("/")
        start_url = home.context["editor_start_url"]
        self.assertContains(home, 'href="/ops/"')
        self.assertContains(home, start_url)
        response = self.client.get(start_url)
        self.assertContains(response, "상품 추천받기 ↗", count=3)
        self.assertContains(response, "data-sticker=", count=7)
        self.assertContains(response, "recommendations/direct.css")
        self.assertNotContains(response, 'id="preview-mock"')
        self.assertEqual(str(response.context["initial"]["flow_id"]), start_url.split("flow_id=")[1])
        self.assertEqual(self.client.get("/ops/").status_code, 200)
        self.assertContains(self.client.get("/interior-recommand/api/csrf"), "csrf_token")

    def test_recommendation_route_does_not_bypass_ops_permissions(self):
        self.assertEqual(self.client.get("/ops/").status_code, 302)
        member = get_user_model().objects.create_user("integration-member")
        self.client.force_login(member)
        self.assertEqual(self.client.get("/ops/").status_code, 403)
        self.assertEqual(self.client.get("/interior-recommand/api/csrf").status_code, 200)
