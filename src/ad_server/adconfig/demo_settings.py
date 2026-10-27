"""Opt-in LAN demo; isolated from both default SQLite and MySQL databases."""
import os
from django.core.exceptions import ImproperlyConfigured
from .settings import *

if os.environ.get('AD_ISOLATED_LAN_DEMO')!='1':
    raise ImproperlyConfigured('Use examples/run_lan_demo.py to start the isolated demo')
DEMO_RUNTIME=BASE_DIR/'runtime'/'lan_demo'
DEMO_RUNTIME.mkdir(exist_ok=True)
DATABASES={'default':{'ENGINE':'django.db.backends.sqlite3','NAME':DEMO_RUNTIME/'ads-demo.sqlite3',
                     'OPTIONS':{'timeout':20,'transaction_mode':'IMMEDIATE'},
                     'TEST':{'NAME':str(DEMO_RUNTIME/'test-only.sqlite3')}}}
AD_TEST_OPERATIONS=True
AD_TEST_POLICY_FILE=str(DEMO_RUNTIME/'policy.json')
SESSION_COOKIE_NAME='sketchon_ads_lan_demo_session'
CSRF_COOKIE_NAME='sketchon_ads_lan_demo_csrf'
