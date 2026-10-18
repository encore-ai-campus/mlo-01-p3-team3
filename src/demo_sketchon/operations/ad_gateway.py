"""Future Ad-server boundary. Demo never initiates network calls or live delivery."""
MEDIA_ID='sketchon'


def integration_status():
    return {'connected':False,'state':'미연결','mode':'로컬 운영 데모',
      'notice':'광고 서버는 아직 연결하지 않았습니다. 아래 캠페인은 데모 초안이며 실제 배너 송출·입찰·노출·클릭 실적을 발생시키지 않습니다.'}
