from datetime import timedelta
from django.core.management.base import BaseCommand,CommandError
from django.db import transaction
from django.utils import timezone
from advertising.engine import gate
from advertising.models import Decision,Event,RequestKey,Context,Audit
from advertising.policy import TestPolicy

class Command(BaseCommand):
    help='명시적 테스트 보관일 이전 결정·사건·가명 맥락·감사 삭제 (기본은 미리보기)'
    def add_arguments(self,parser):parser.add_argument('--confirm-delete-test-data',action='store_true')
    @transaction.atomic
    def handle(self,**options):
        p=TestPolicy.load();gate();cutoff=timezone.now()-timedelta(days=p.event_retention_days)
        ids=list(Decision.objects.filter(created_at__lt=cutoff,expires_at__lt=cutoff).values_list('pk',flat=True))
        self.stdout.write(f'테스트 보관일 {p.event_retention_days}: 결정 {len(ids)}개 대상 (보고 결과도 삭제됨)')
        if not options['confirm_delete_test_data']:return
        Event.objects.filter(decision_id__in=ids).delete();RequestKey.objects.filter(decision_id__in=ids).delete()
        Decision.objects.filter(pk__in=ids).delete()
        Context.objects.filter(created_at__lt=cutoff,decision__isnull=True).delete()
        Audit.objects.filter(created_at__lt=cutoff).delete()
        Audit.objects.create(operation='TEST_RETENTION_PURGE',detail={'decisions':len(ids),'cutoff':cutoff.isoformat(),'policy':p.version})
        self.stdout.write('삭제 완료. 이는 P-05 운영 보관 정책 확정이 아닙니다.')
