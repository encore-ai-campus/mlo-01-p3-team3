import os
from pathlib import Path
from interior_recommender.config import load_environment

load_environment()

BASE_DIR = Path(__file__).resolve().parent.parent
SECRET_KEY = os.environ.get("ROOMPICK_SECRET_KEY", "roompick-local-development-only")
DEBUG = os.environ.get("ROOMPICK_DEBUG", "1") == "1"
ALLOWED_HOSTS = os.environ.get("ROOMPICK_HOSTS", "localhost,127.0.0.1,[::1],192.168.0.44").split(",")
INSTALLED_APPS = ["django.contrib.auth", "django.contrib.contenttypes", "django.contrib.sessions", "django.contrib.staticfiles", "sketch", "operations.apps.OperationsConfig", "recommendations"]
MIDDLEWARE = [
    "django.middleware.security.SecurityMiddleware",
    "django.contrib.sessions.middleware.SessionMiddleware",
    "django.contrib.auth.middleware.AuthenticationMiddleware",
    "operations.tracking.ActorContextMiddleware",
    "sketch.access.PrivateResponseMiddleware",
    "django.middleware.common.CommonMiddleware",
    "django.middleware.csrf.CsrfViewMiddleware",
    "django.middleware.clickjacking.XFrameOptionsMiddleware",
]
ROOT_URLCONF = "config.urls"
TEMPLATES = [{"BACKEND": "django.template.backends.django.DjangoTemplates", "APP_DIRS": True,
              "OPTIONS": {"context_processors": ["django.template.context_processors.request", "django.contrib.auth.context_processors.auth", "sketch.access.account_context"]}}]
LOGIN_URL = '/accounts/login/'
AUTH_PASSWORD_VALIDATORS = [
    {"NAME": "django.contrib.auth.password_validation.UserAttributeSimilarityValidator"},
    {"NAME": "django.contrib.auth.password_validation.MinimumLengthValidator"},
    {"NAME": "django.contrib.auth.password_validation.CommonPasswordValidator"},
    {"NAME": "django.contrib.auth.password_validation.NumericPasswordValidator"},
]
WSGI_APPLICATION = "config.wsgi.application"
DATABASES = {"default": {"ENGINE": "django.db.backends.sqlite3", "NAME": os.environ.get("ROOMPICK_DB_PATH", BASE_DIR / "db.sqlite3"), "OPTIONS": {"timeout": 20, "transaction_mode": "IMMEDIATE"}}}
STATIC_URL = "/static/"
MEDIA_ROOT = Path(os.environ.get("ROOMPICK_MEDIA_ROOT", BASE_DIR / "media"))
SKETCH_RAW_ROOT = Path(os.environ.get("ROOMPICK_RAW_ROOT", BASE_DIR / "data" / "sketch_raw"))
DATA_UPLOAD_MAX_MEMORY_SIZE = 35 * 1024 * 1024
FILE_UPLOAD_MAX_MEMORY_SIZE = 2 * 1024 * 1024
LANGUAGE_CODE = "ko-kr"
TIME_ZONE = "Asia/Seoul"
USE_TZ = True
DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"
SESSION_COOKIE_AGE = 365 * 24 * 60 * 60
SESSION_COOKIE_HTTPONLY = True
SESSION_COOKIE_SAMESITE = "Lax"
SESSION_COOKIE_NAME = "roompick_sessionid"
CSRF_COOKIE_NAME = "roompick_csrftoken"

# Local immutable product snapshot; recommendation requests never query a product DB.
INTERIOR_RECOMMENDER_PRODUCTS = Path(os.environ.get(
    "INTERIOR_RECOMMENDER_PRODUCTS",
    BASE_DIR.parent / "interior_recommender/sample/crawl_test/full/catalog-with-links/products.json",
)).resolve()
INTERIOR_RECOMMENDER_DEMO = os.environ.get("INTERIOR_RECOMMENDER_DEMO", "0") == "1"
INTERIOR_RECOMMENDER_OUTPUT_ROOT = Path(os.environ.get(
    "INTERIOR_RECOMMENDER_OUTPUT_ROOT",
    BASE_DIR.parent.parent / "outputs" / ("sketchon-direct-demo" if INTERIOR_RECOMMENDER_DEMO else "sketchon-direct"),
)).resolve()
INTERIOR_RECOMMENDER_RANKING_POLICY = os.environ.get("INTERIOR_RECOMMENDER_RANKING_POLICY")
if not DEBUG:
    if SECRET_KEY == "roompick-local-development-only":
        raise ValueError("ROOMPICK_SECRET_KEY is required when DEBUG is off")
    SESSION_COOKIE_SECURE = True
    CSRF_COOKIE_SECURE = True
