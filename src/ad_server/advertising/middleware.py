from django.http import JsonResponse
from .policy import TestPolicy
from .errors import AdError

class AdminModeMiddleware:
    def __init__(self,get_response):self.get_response=get_response
    def __call__(self,request):
        if request.path.startswith('/admin/'):
            try:TestPolicy.load()
            except AdError as e:return JsonResponse({'error':{'code':e.code,'message':e.message}},status=e.status)
        return self.get_response(request)
