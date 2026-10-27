from django.core.management.base import BaseCommand
from django.db import transaction
from django.utils import timezone
from advertising.engine import gate,reconcile,saved_policy
from advertising.models import Decision
from advertising.policy import TestPolicy

class Command(BaseCommand):
    help='선도착 사건 연결·대기 만료를 재검사 (누적 과금·카운터 없음)'
    @transaction.atomic
    def handle(self,**options):
        TestPolicy.load();gate();count=0;now=timezone.now()
        for d in Decision.objects.filter(events__status='PENDING').distinct():
            reconcile(d,saved_policy(d),now);count+=1
        self.stdout.write(f'결정 {count}개 재검사 완료')
