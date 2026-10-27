"""Delivery configuration status, separate from local campaign synchronization."""
from django.conf import settings
MEDIA_ID='sketchon'

def ad_context(request):
    return {'ad_delivery_enabled':settings.SKETCHON_AD_DELIVERY['enabled']}

def integration_status():
    enabled=settings.SKETCHON_AD_DELIVERY['enabled']
    return {'connected':False,'configured':enabled,
      'state':'송출 설정됨 · 캠페인 미동기화' if enabled else '미연결','mode':'로컬 운영 데모',
      'notice':'광고 표시는 별도 광고 서버의 승인·집행 자료를 사용합니다. 이 화면의 로컬 캠페인은 광고 서버와 동기화되지 않습니다.' if enabled else
      '광고 서버는 아직 연결하지 않았습니다. 아래 캠페인은 데모 초안이며 실제 배너 송출·입찰·노출·클릭 실적을 발생시키지 않습니다.'}
