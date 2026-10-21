from functools import wraps
from django.contrib.auth.views import redirect_to_login
from django.http import HttpResponseForbidden
from .models import OperatorAudit

MENU = [('dashboard','운영 개요','/ops/','view_console'), ('people','이용자','/ops/people/','view_people'),
        ('projects','작업 · 결과','/ops/projects/','view_projects'), ('events','행동 데이터','/ops/events/','view_behavior'),
        ('collection','스케치 수집','/ops/collection/','view_collection'), ('ads','광고 관리','/ops/ads/','manage_ads'),
        ('reports','광고 성과','/ops/ad-reports/','view_ad_reports'), ('audit','감사 이력','/ops/audit/','view_ops_audit')]


def allowed(user,permission):
    return user.is_authenticated and user.is_active and user.is_staff and user.has_perm('operations.'+permission)


def entry_url(user):
    return next((url for _,_,url,permission in MENU if allowed(user,permission)), '')


def page_access(user,permission):
    return permission in dict(OperatorAudit._meta.permissions) and allowed(user,permission)


def ops_required(permission):
    def decorate(view):
        @wraps(view)
        def wrapped(request,*args,**kwargs):
            if not request.user.is_authenticated:return redirect_to_login(request.get_full_path())
            if not allowed(request.user,permission):return HttpResponseForbidden('운영 권한이 필요합니다.')
            return view(request,*args,**kwargs)
        return wrapped
    return decorate


def audit(request,action,obj,reason='',before=None,after=None):
    return OperatorAudit.objects.create(actor=request.user,action=action,object_type=obj.__class__.__name__,object_id=str(obj.pk),
        reason=reason[:500],before=before or {},after=after or {})
