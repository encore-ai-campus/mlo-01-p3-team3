"""The demo must not import the preserved dev application's Django apps."""
from pathlib import Path

from django.conf import settings
from django.test import SimpleTestCase

import config
import operations
import recommendations
import sketch


class DemoBoundaryTests(SimpleTestCase):
    def test_django_apps_resolve_inside_local_demo(self):
        root = Path(settings.BASE_DIR).resolve()
        self.assertEqual(root.name, 'local_demo')
        for module in (config, sketch, operations, recommendations):
            with self.subTest(module=module.__name__):
                self.assertTrue(Path(module.__file__).resolve().is_relative_to(root))

    def test_cookies_do_not_overwrite_dev_sessions(self):
        self.assertEqual(settings.SESSION_COOKIE_NAME, 'local_demo_sessionid')
        self.assertEqual(settings.CSRF_COOKIE_NAME, 'local_demo_csrftoken')

    def test_catalog_and_runtime_are_explicit_demo_defaults(self):
        from config.settings import LOCAL_DEMO_RUNTIME, PROJECT_ROOT

        self.assertEqual(LOCAL_DEMO_RUNTIME, PROJECT_ROOT / 'outputs/local-demo-v43')
        self.assertNotEqual(LOCAL_DEMO_RUNTIME, PROJECT_ROOT / 'src/demo_sketchon')
