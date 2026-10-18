"""The existing editor remains the entry to mock composition, not direct ranking."""
from pathlib import Path
from django.test import TestCase


class ExistingUIEntryTests(TestCase):
    def test_home_and_editor_keep_existing_primary_flow(self):
        self.assertContains(self.client.get('/'), '로그인 없이 하루 3회')
        response = self.client.get('/editor/')
        self.assertContains(response, '공간 만들기 ↗', count=3)
        self.assertContains(response, '결과·상품 찾기')
        self.assertNotContains(response, 'id="preview-mock"')
        self.assertNotContains(response, 'recommendations/direct.css')
        self.assertNotContains(response, '상품 추천받기')
        for control in ('undo', 'redo', 'fill', 'text', 'box-select', 'crop', 'search-mobile'):
            self.assertContains(response, f'id="{control}"')

    def test_all_create_buttons_use_existing_composition_handler(self):
        script = (Path(__file__).parent / 'static/sketch/editor.mjs').read_text(encoding='utf-8')
        self.assertNotIn('recommendDirectly', script)
        self.assertNotIn("$('preview-mock')", script)
        for control in ('search', 'search-top', 'search-mobile'):
            self.assertIn(f"$('{control}').addEventListener('click', searchProducts)", script)
        self.assertIn('platform.generate(', script)
