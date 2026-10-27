import ipaddress
from datetime import datetime, time, timedelta
from zoneinfo import ZoneInfo
from django.db import connection
from django.db.models import F
from django.utils import timezone
from .models import GuestGenerationUsage

KST = ZoneInfo('Asia/Seoul')
LIMIT = 3


class GuestLimitReached(Exception):
    pass


def client_ip(request):
    try:
        ip = ipaddress.ip_address(request.META.get('REMOTE_ADDR', ''))
        return str(ip.ipv4_mapped if isinstance(ip, ipaddress.IPv6Address) and ip.ipv4_mapped else ip)
    except ValueError:
        # Missing/invalid address must never create a fresh quota identity.
        return '0.0.0.0'


def quota_day(at=None):
    return (at or timezone.now()).astimezone(KST).date()


def quota_status(request, at=None):
    day = quota_day(at)
    reset_at = datetime.combine(day + timedelta(days=1), time(), KST).isoformat()
    if request.user.is_authenticated:
        return {'unlimited':True, 'used':0, 'limit':None, 'remaining':None, 'reset_at':reset_at}
    used = GuestGenerationUsage.objects.filter(client_ip=client_ip(request), usage_date=day).values_list('successful_count', flat=True).first() or 0
    return {'unlimited':False, 'used':used, 'limit':LIMIT, 'remaining':max(0,LIMIT-used), 'reset_at':reset_at}


def charge_guest_generation(request, at=None):
    if request.user.is_authenticated:
        return
    if not connection.in_atomic_block:
        raise RuntimeError('Quota charge requires generation transaction')
    row, _ = GuestGenerationUsage.objects.get_or_create(client_ip=client_ip(request), usage_date=quota_day(at))
    if not GuestGenerationUsage.objects.filter(pk=row.pk, successful_count__lt=LIMIT).update(successful_count=F('successful_count')+1, updated_at=timezone.now()):
        raise GuestLimitReached('비회원은 한국 시간 기준 하루 3회까지 생성할 수 있어요. 로그인하면 계속 이용할 수 있습니다.')
