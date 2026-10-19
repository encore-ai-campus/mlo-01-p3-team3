"""Operator-only Korean summaries of stored decisions/events; never changes ranking."""
from django.utils import timezone
from .models import Campaign

SIGNALS={'moisturizer':'보습 제품','humidifier':'가습기','tumbler':'텀블러','notebook':'노트북',
         'phone':'휴대폰','coffee':'커피','book':'책','glasses':'안경','bread':'빵','cctv':'보안 카메라'}
REASONS={
 'RELATED':'확인한 관심 품목과 관련된 광고 중에서 비교했습니다.',
 'GENERAL_NO_CONTEXT':'관심 품목을 지정하지 않아 일반 광고 후보를 비교했습니다.',
 'GENERAL_NO_RELATED':'관련된 광고 후보가 없어 일반 광고 후보를 비교했습니다.',
 'APPROVED_DEFAULT_NOTICE':'집행 가능한 광고가 없어 별도로 승인된 기본 안내를 표시합니다.',
 'NO_ELIGIBLE_CANDIDATE':'승인·기간·표시 위치·노출 한도를 충족하는 후보가 없습니다.',
 'POLICY_BLOCKED':'정책 차단','NOT_ACTIVE_APPROVED':'미승인·집행 중지·변경 심사 대기',
 'CONTRACT_UNAVAILABLE':'계약 미승인 또는 유효 기간 밖','OUTSIDE_PERIOD':'광고 집행 기간 밖',
 'PLACEMENT_MISMATCH':'표시 위치 불일치','PRODUCT_UNVERIFIED':'상품 연결 미확인',
 'ORGANIC_DUPLICATE':'일반 추천 상품과 중복','CREATIVE_OR_DESTINATION_UNAVAILABLE':'배너·이동 주소 사용 불가',
 'FREQUENCY_LIMIT':'같은 방문자에 대한 노출 한도 도달',
 'RENDER_CONFIRMED':'이미지가 정상 표시되어 집계했습니다.',
 'VISIBLE_CONFIRMED':'면적 50% 이상·연속 1초 노출이 확인되어 집계했습니다.',
 'VISIBILITY_THRESHOLD_NOT_MET':'면적 50%·연속 1초·활성 화면 기준을 충족하지 못했습니다.',
 'AWAITING_RENDER':'이미지 표시 기록이 도착하면 연결합니다.',
 'RENDER_LINKED':'표시 기록에 연결된 클릭입니다.', 'LINKED_TO_RENDER':'표시 기록과 연결해 집계했습니다.',
 'RENDER_JOIN_EXPIRED':'이미지 표시 기록 연결 대기 시간이 지났습니다.',
 'DECISION_ALREADY_COUNTED':'이미 집계한 사건으로 중복 가산하지 않습니다.',
 'NOT_ELIGIBLE':'현재 집행 조건을 충족하지 않습니다.',
 'ACTIVE':'승인된 광고의 안내 페이지로 이동합니다.',
 'SAFE_ENDED_OR_PAUSED':'중지·종료된 광고의 안전한 주소로만 이동하며 성과에 가산하지 않습니다.',
 'DECISION_EXPIRED':'표시 기간이 만료되어 종료 안내로 이동합니다.',
 'LOAD_ERROR':'이미지를 불러오지 못했습니다.', 'LINK_UNAVAILABLE':'이동 주소를 사용할 수 없습니다.'}
EVENTS={'RENDERED':'이미지 표시','VISIBLE':'가시 노출','CLICK':'클릭','LOAD_ERROR':'이미지 오류'}
STATES={'VALID':'집계 인정','DUPLICATE':'중복 제외','PENDING':'표시 기록 연결 대기','DIAGNOSTIC':'진단 기록'}


def reason_label(code):return REASONS.get(code,'기타 진단 항목')


def decision_summary(d,displayable):
    bids=d.snapshot.get('candidate_bids') or {}
    ids=list(bids)
    campaigns={str(c.pk):c for c in Campaign.objects.filter(pk__in=ids).select_related('advertiser')}
    representatives=set(d.snapshot.get('representative_campaign_ids') or [])
    ordered=sorted(ids,key=lambda key:(-bids[key],key))
    eligible=sorted(representatives,key=lambda key:(-bids.get(key,0),key))
    ranks={key:index+1 for index,key in enumerate(eligible)}
    rows=[]
    for key in ordered:
        c=campaigns.get(key)
        selected=key==str(d.campaign_id)
        rows.append({'campaign_name':c.name if c else '이전 캠페인',
                     'advertiser_name':c.advertiser.name if c else '이전 광고주',
                     'bid_points':bids[key],'rank':ranks.get(key),'selected':selected,
                     'outcome':'선정' if selected else '다음 순위' if key in representatives else '같은 광고주 대표에서 제외'})
    code=d.snapshot.get('selection_reason')
    explanation=reason_label(code)
    if d.status=='SELECTED':
        explanation+=' 광고주별 대표 후보를 정한 뒤 입찰 포인트가 높은 광고를 선정했습니다. 동점은 캠페인 관리 번호 순서로 결정합니다.'
    current=Campaign.objects.filter(pk=d.campaign_id).first() if d.campaign_id else None
    return {'state_label':{'SELECTED':'광고 선정 완료','DEFAULT':'기본 안내 표시','NO_FILL':'선정 가능한 광고 없음'}.get(d.status,'확인이 필요합니다'),
            'displayable':bool(displayable),'explanation':explanation,
            'selected_title':d.snapshot.get('title','선정된 광고가 없습니다'),
            'campaign_name':current.name if current else None,
            'advertiser_name':d.snapshot.get('advertiser_name'),
            'bid_points':d.snapshot.get('bid_points'),'candidate_count':len(rows),
            'representative_count':len(representatives),'bids':rows,
            'excluded':[{'label':reason_label(code),'count':count} for code,count in d.diagnostics.items()],
            'expires_label':timezone.localtime(d.expires_at).strftime('%H:%M:%S')+' (한국 시간)',
            'rendered':d.rendered_at is not None,'visible':d.visible_at is not None,'clicked':d.clicked_at is not None,
            'history_note':'입찰 포인트·선정 결과는 당시 기록이며, 캠페인 이름은 현재 등록명입니다.'}


def event_summary(d,event):
    return {'kind_label':EVENTS.get(event.kind,'사건'),'state_label':STATES.get(event.status,'확인 필요'),
            'reason_label':reason_label(event.reason),'rendered':d.rendered_at is not None,
            'visible':d.visible_at is not None,'clicked':d.clicked_at is not None}


def report_summary(data):
    rows=data['rows']
    names={str(c.pk):c.name for c in Campaign.objects.filter(pk__in={row['campaign_id'] for row in rows})}
    rendered=sum(row['rendered'] for row in rows)
    visible=sum(row['visible'] for row in rows)
    clicked=sum(row['valid_clicks'] for row in rows)
    return {'rendered':rendered,'visible':visible,'clicked':clicked,
            'ctr_percent':100*clicked/rendered if rendered else None,
            'selection_count':data['selection_counts'].get('SELECTED',0),
            'no_fill_count':data['selection_counts'].get('NO_FILL',0),
            'rows':[{'date':row['date'],'campaign_name':names.get(row['campaign_id'],'이전 캠페인'),
                     'slot_label':{'EDITOR_RIGHT':'편집 오른쪽','RESULT_RIGHT':'결과 오른쪽'}.get(row['placement'],'기타'),
                     'rendered':row['rendered'],'visible':row['visible'],'clicked':row['valid_clicks'],
                     'ctr_percent':row['ctr_percent']} for row in sorted(rows,key=lambda row:(row['date'],row['campaign_id'],row['revision_id']),reverse=True)],
            'diagnostics':[{'label':reason_label(code),'count':count} for code,count in data['diagnostic_counts'].items()],
            'generated_label':timezone.localtime().strftime('%H:%M:%S')+' (한국 시간)'}
