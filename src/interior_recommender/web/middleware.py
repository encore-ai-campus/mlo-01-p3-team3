import hmac
from uuid import UUID

from django.conf import settings
from django.http import JsonResponse


class BridgeAuthentication:
    """Trust server-to-server bridge headers only after constant-time token verification."""
    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        supplied = request.headers.get("Authorization", "")
        token = settings.WEB_BRIDGE_TOKEN
        if supplied:
            if not token or not hmac.compare_digest(supplied, "Bearer " + token):
                return JsonResponse({"error": "Bridge 인증에 실패했습니다."}, status=403)
            try:
                request.bridge_owner = str(UUID(request.headers.get("X-MVP-Owner", "")))
            except ValueError:
                return JsonResponse({"error": "Bridge 사용자 식별자가 올바르지 않습니다."}, status=403)
            # Upstream bridge performs the browser's same-origin CSRF check.
            request._dont_enforce_csrf_checks = True
        elif request.META.get("REMOTE_ADDR") not in {"127.0.0.1", "::1", None}:
            return JsonResponse({"error": "원격 접속은 인증된 서버 Bridge를 사용하세요."}, status=403)
        return self.get_response(request)
