import argparse
import secrets
import os
import atexit
from pathlib import Path

import django
from django.conf import settings
from django.core.management import call_command


def configure(output_root=None, templates=None, demo=False, host="127.0.0.1", danawa_index=None, *, products=None, ranking_policy=None, legacy_generation=False):
    from ..config import load_environment
    load_environment()
    token = os.getenv("MVP_BRIDGE_TOKEN", "")
    default_index = Path("outputs/danawa_embeddings/index.json")
    selected_index = Path(danawa_index).resolve() if danawa_index else (default_index.resolve() if default_index.exists() else None)
    if host not in {"127.0.0.1", "localhost"} and not token:
        raise ValueError("LAN 실행에는 서버 간 공유 MVP_BRIDGE_TOKEN 설정이 필요합니다.")
    root = Path(output_root or "outputs/web").resolve()
    root.mkdir(parents=True, exist_ok=True)
    secret = root / ".session-secret"
    if not secret.exists():
        secret.write_text(secrets.token_urlsafe(48), encoding="utf-8")
    settings.configure(
        DEBUG=False, SECRET_KEY=secret.read_text(encoding="utf-8"),
        ALLOWED_HOSTS=["127.0.0.1", "localhost", "[::1]"] + ([host] if host != "0.0.0.0" else ["*"]), ROOT_URLCONF="interior_recommender.web.urls",
        MIDDLEWARE=["django.middleware.security.SecurityMiddleware", "interior_recommender.web.middleware.BridgeAuthentication", "django.contrib.sessions.middleware.SessionMiddleware",
                    "django.middleware.csrf.CsrfViewMiddleware", "django.middleware.clickjacking.XFrameOptionsMiddleware"],
        SESSION_ENGINE="django.contrib.sessions.backends.signed_cookies", SESSION_COOKIE_HTTPONLY=True,
        SESSION_COOKIE_SAMESITE="Strict", CSRF_COOKIE_SAMESITE="Strict", INSTALLED_APPS=[],
        TEMPLATES=[{"BACKEND": "django.template.backends.django.DjangoTemplates", "DIRS": [str(Path(__file__).parent / "ui")]}],
        DATA_UPLOAD_MAX_MEMORY_SIZE=35 * 1024 * 1024, FILE_UPLOAD_MAX_MEMORY_SIZE=16 * 1024 * 1024,
        WEB_OUTPUT_ROOT=root, WEB_TEMPLATE_MANIFEST=Path(templates).resolve() if templates else None, WEB_DEMO=demo,
        WEB_BRIDGE_TOKEN=token,
        WEB_DANAWA_INDEX=selected_index,
        WEB_PRODUCTS=Path(products).resolve() if products else None,
        WEB_RANKING_POLICY=Path(ranking_policy).resolve() if ranking_policy else None,
        WEB_LEGACY_GENERATION=legacy_generation,
    )
    django.setup()


def main():
    parser = argparse.ArgumentParser(description="RoomPick Canvas API and CPU product recommendations")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument("--output-root", type=Path, default=Path("outputs/web"))
    parser.add_argument("--templates", type=Path, help="Approved template manifest to import")
    parser.add_argument("--danawa-index", type=Path, help="Saved embed-directory index.json; preferred over --templates")
    parser.add_argument("--demo", action="store_true", help="Synthetic API responses for UI testing; no external calls")
    parser.add_argument("--products", type=Path, help="Product metadata and precomputed vector manifest")
    parser.add_argument("--ranking-policy", type=Path, help="Manual initial weights JSON; frozen per request")
    parser.add_argument("--legacy-generation", action="store_true", help="Explicitly use the historical image generation workflow")
    args = parser.parse_args()
    if args.templates and not args.legacy_generation:
        parser.error("--templates requires --legacy-generation; direct recommendations use --products")
    configure(args.output_root, args.templates, args.demo, args.host, args.danawa_index,
              products=args.products, ranking_policy=args.ranking_policy, legacy_generation=args.legacy_generation)
    from . import views
    atexit.register(lambda: views._studio.pool.shutdown(wait=False, cancel_futures=True) if views._studio else None)
    call_command("runserver", f"{args.host}:{args.port}", use_reloader=False, skip_checks=True)


if __name__ == "__main__":
    main()
