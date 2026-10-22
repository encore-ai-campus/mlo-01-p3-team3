import uuid
from datetime import timedelta
from django.core.management.base import BaseCommand
from django.db import transaction
from django.utils import timezone
from advertising.models import Advertiser,MockContract,Campaign,Revision,Audit
from advertising.engine import review_revision
from advertising.policy import TestPolicy

def uid(key):return uuid.uuid5(uuid.NAMESPACE_URL,'sketchon-ads-local-demo/'+key)

class Command(BaseCommand):
    help='명시적 4차 테스트 모드에 가상 광고주 2개·경쟁 배너 생성 (실상품·실계약 아님)'
    @transaction.atomic
    def handle(self,**options):
        TestPolicy.load();now=timezone.now()
        for key,bid in [('alpha',700),('beta',500)]:
            a,_=Advertiser.objects.get_or_create(pk=uid(key),defaults={'name':f'가상 광고주 {key}','status':'ACTIVE'})
            contract,_=MockContract.objects.get_or_create(pk=uid(key+'-contract'),defaults={'advertiser':a,
                'reference':f'TEST-ONLY-{key}','status':'APPROVED','starts_at':now-timedelta(days=1),'ends_at':now+timedelta(days=30)})
            c,created=Campaign.objects.get_or_create(pk=uid(key+'-campaign'),defaults={'advertiser':a,'contract':contract,'name':f'주변기기 테스트 {key}'})
            if not created:continue  # Never silently reactivate or overwrite operator changes.
            r=Revision.objects.create(campaign=c,template_id='desk',title=f'가상 주변기기 {key}',
                alt_text='가상 주변기기 광고 배너',bid_points=bid,placements=['EDITOR_RIGHT','RESULT_RIGHT'],
                is_general=True,destination=f'/test-destination/{key}/',starts_at=now-timedelta(hours=1),ends_at=now+timedelta(days=7))
            review_revision(r.pk,True,'seed_ads_demo','명시적 모의 자료 생성')
            Campaign.objects.filter(pk=c.pk).update(enabled=True)
            Audit.objects.create(operation='DEMO_ACTIVATED',object_id=str(c.pk),actor='seed_ads_demo')
        self.stdout.write('모의 데이터 준비 완료. 재실행은 기존 자료를 변경하지 않습니다.')
