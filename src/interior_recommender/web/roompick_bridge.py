"""Drop-in SAME-ORIGIN reverse proxy for the existing RoomPick Django application."""
import os
from uuid import uuid4

import httpx
from django.http import HttpResponse, JsonResponse
from django.middleware.csrf import get_token


def bridge(request, route=""):
    """Mount at path('ai-mvp/<path:route>', bridge) plus path('ai-mvp/', bridge)."""
    upstream = os.environ.get("AI_MVP_UPSTREAM", "http://127.0.0.1:8765").rstrip("/")
    get_token(request)
    token = os.environ.get("MVP_BRIDGE_TOKEN", "")
    if not token:
        return JsonResponse({"error": "RoomPick 서버의 MVP_BRIDGE_TOKEN 설정이 필요합니다."}, status=503)
    if ".." in route or "\\" in route or route.startswith("/"):
        return JsonResponse({"error": "Invalid route"}, status=400)
    if "ai_mvp_owner" not in request.session:
        request.session["ai_mvp_owner"] = str(uuid4())
    headers = {"Authorization": "Bearer " + token, "X-MVP-Owner": request.session["ai_mvp_owner"],
               "Content-Type": request.headers.get("Content-Type", "application/json")}
    if request.content_type == "multipart/form-data":
        # CSRF validation has already parsed multipart POSTs. Rebuild the form
        # instead of accessing the consumed request.body stream.
        fields = {key: request.POST.getlist(key) for key in request.POST}
        uploads = [(key, (file.name, file.read(), file.content_type))
                   for key in request.FILES for file in request.FILES.getlist(key)]
        if sum(len(value[1][1]) for value in uploads) > 35 * 1024 * 1024:
            return JsonResponse({"error": "요청 파일이 너무 큽니다."}, status=413)
        headers.pop("Content-Type")
        payload = {"data": fields, "files": uploads}
    else:
        if len(request.body) > 35 * 1024 * 1024:
            return JsonResponse({"error": "요청 파일이 너무 큽니다."}, status=413)
        payload = {"content": request.body}
    try:
        response = httpx.request(request.method, upstream + "/" + route, params=request.GET,
                                 headers=headers, **payload, timeout=30, follow_redirects=False)
    except httpx.HTTPError:
        return JsonResponse({"error": "AI MVP 서버에 연결할 수 없습니다."}, status=502)
    return HttpResponse(response.content, status=response.status_code,
                        content_type=response.headers.get("content-type", "application/json"),
                        headers={"Cache-Control": "private, no-store"})
