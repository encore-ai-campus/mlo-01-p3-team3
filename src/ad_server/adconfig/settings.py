import os
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
RUNTIME = BASE_DIR / 'runtime'
RUNTIME.mkdir(exist_ok=True)
SECRET_KEY = os.environ.get('AD_SERVER_SECRET_KEY', 'ad-server-local-template-test-only')
DEBUG = False
ALLOWED_HOSTS = os.environ.get('AD_SERVER_HOSTS', '127.0.0.1,localhost,[::1]').split(',')
ROOT_URLCONF = 'adconfig.urls'
INSTALLED_APPS = ['adconfig.apps.AdAdminConfig', 'django.contrib.auth', 'django.contrib.contenttypes',
                  'django.contrib.sessions', 'django.contrib.messages', 'django.contrib.staticfiles', 'advertising.apps.AdvertisingConfig']
MIDDLEWARE = ['django.middleware.security.SecurityMiddleware', 'django.contrib.sessions.middleware.SessionMiddleware',
              'django.middleware.common.CommonMiddleware', 'django.middleware.csrf.CsrfViewMiddleware',
              'django.contrib.auth.middleware.AuthenticationMiddleware', 'django.contrib.messages.middleware.MessageMiddleware',
              'advertising.middleware.AdminModeMiddleware',
              'django.middleware.clickjacking.XFrameOptionsMiddleware']
TEMPLATES = [{'BACKEND': 'django.template.backends.django.DjangoTemplates', 'APP_DIRS': True,
              'DIRS': [BASE_DIR / 'advertising' / 'templates'],
              'OPTIONS': {'context_processors': ['django.template.context_processors.request',
                         'django.contrib.auth.context_processors.auth', 'django.contrib.messages.context_processors.messages']}}]
from .database import database_config
DATABASES = {'default': database_config(os.environ, RUNTIME)}
STATIC_URL = '/static/'
STATIC_ROOT = RUNTIME / 'static'
LANGUAGE_CODE = 'ko-kr'
TIME_ZONE = 'Asia/Seoul'
USE_TZ = True
DEFAULT_AUTO_FIELD = 'django.db.models.BigAutoField'
WSGI_APPLICATION = 'adconfig.wsgi.application'
SESSION_COOKIE_NAME = 'sketchon_ads_session'
CSRF_COOKIE_NAME = 'sketchon_ads_csrf'
SESSION_COOKIE_HTTPONLY = True
SESSION_COOKIE_SAMESITE = 'Lax'
AD_BANNER_ROOT = BASE_DIR / 'ad_banner_image'
AD_TEST_OPERATIONS = os.environ.get('AD_TEST_OPERATIONS', '0') == '1'
AD_SERVICE_TOKEN = os.environ.get('AD_SERVICE_TOKEN', '')
AD_TEST_POLICY_FILE = os.environ.get('AD_TEST_POLICY_FILE', '')
AD_FRAME_ANCESTORS = os.environ.get('AD_FRAME_ANCESTORS', "'self'")
