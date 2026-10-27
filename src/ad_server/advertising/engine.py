"""Isolated phase-4 test engine. No model calls, purchase claims, budgets or CPC ledger."""
import hashlib
import hmac
import json
import math
import uuid
from dataclasses import asdict
from collections import Counter
from datetime import datetime,timedelta
from zoneinfo import ZoneInfo
from django.conf import settings
from django.db import transaction
from django.db.models import Q
from django.utils import timezone
from .assets import banner
from .creative import creative_path
from .errors import AdError
from .models import (Advertiser,MockContract,Campaign,Revision,DefaultNotice,ServingGate,Context,
                     Decision,RequestKey,Event,Audit,PolicyRecord)
from .policy import TestPolicy,TARGETS,TARGET_MAP_VERSION,SOURCES,PLACEMENTS,destination

def digest(value): return hashlib.sha256(json.dumps(value,sort_keys=True,separators=(',',':'),ensure_ascii=False).encode()).hexdigest()
def visitor(value):
    if not isinstance(value,str) or not 16<=len(value)<=128 or not value.isascii() or not all(c.isalnum() or c in '-_' for c in value):
        raise AdError('INVALID_VISITOR','가명 방문자 키가 필요합니다.')
    return hmac.new(settings.SECRET_KEY.encode(),value.encode(),'sha256').hexdigest()
def uuid_value(value):
    try: return uuid.UUID(str(value))
    except (ValueError,TypeError,AttributeError): raise AdError('INVALID_ID','UUID 형식을 확인하세요.')
def timestamp(value):
    try:
        result=datetime.fromisoformat(value.replace('Z','+00:00'))
        if result.tzinfo is None: raise ValueError()
        return result
    except (ValueError,TypeError,AttributeError): raise AdError('INVALID_TIME','시간대가 있는 ISO 시각이 필요합니다.')
def gate():
    ServingGate.objects.get_or_create(pk=1)
    ServingGate.objects.select_for_update().get(pk=1)

def bind_policy(policy):
    config=json.loads(json.dumps(asdict(policy)))
    record,created=PolicyRecord.objects.get_or_create(version=policy.version,
        defaults={'checksum':digest(config),'configuration':config})
    if record.checksum!=digest(config):
        raise AdError('POLICY_VERSION_CONFLICT','설정 변경 시 새 테스트 정책 버전을 사용하세요.',409)

def saved_policy(d):
    TestPolicy.load()  # The explicit activation switch must remain enabled.
    return TestPolicy(**d.snapshot['test_policy'])

def validate_asset(revision,policy):
    asset=banner(revision.template_id)
    if (asset['bytes']>policy.asset_max_bytes or asset['width']<policy.asset_min_width
            or asset['height']<policy.asset_min_height):
        raise AdError('INVALID_CREATIVE','명시적 테스트 소재 제한을 충족하지 않습니다.')
    if revision.asset_sha256 and revision.asset_sha256 != asset['sha256']:
        raise AdError('CREATIVE_CHANGED','승인 후 소재가 변경됐습니다.')
    return asset

@transaction.atomic
def review_revision(revision_id,approve,actor,reason=''):
    policy=TestPolicy.load();gate();bind_policy(policy)
    r=Revision.objects.select_related('campaign__advertiser','campaign__contract').get(pk=revision_id)
    if r.status!='PENDING': raise AdError('ALREADY_REVIEWED','이미 심사한 버전입니다.',409)
    if Revision.objects.filter(campaign=r.campaign,version__gt=r.version).exists():
        raise AdError('NEWER_REVISION_PENDING','최신 제출 버전을 심사하세요.',409)
    if approve:
        r.full_clean();asset=validate_asset(r,policy);destination(r.destination,policy)
        if r.campaign.contract.advertiser_id!=r.campaign.advertiser_id:
            raise AdError('CONTRACT_MISMATCH','광고주와 계약이 다릅니다.')
        Revision.objects.filter(pk=r.pk).update(status='APPROVED',asset_sha256=asset['sha256'],reviewed_by=actor,reviewed_at=timezone.now())
        Campaign.objects.filter(pk=r.campaign_id).update(active_revision=r,pending_change=False,enabled=False)
    else:
        Revision.objects.filter(pk=r.pk).update(status='REJECTED',reviewed_by=actor,reviewed_at=timezone.now())
        Campaign.objects.filter(pk=r.campaign_id).update(pending_change=True,enabled=False)
    Audit.objects.create(operation='APPROVED' if approve else 'REJECTED',object_id=str(r.pk),actor=actor,
                         detail={'reason':reason,'policy_version':policy.version,'campaign':str(r.campaign_id),'version':r.version})

def current_safety(d,policy):
    if d.status=='DEFAULT':
        n=DefaultNotice.objects.filter(pk=d.notice_id,active=True,approved=True).first()
        return bool(n and validate_asset(n,policy)['sha256']==d.snapshot['asset_sha256'])
    if d.status!='SELECTED': return False
    c=Campaign.objects.select_related('advertiser','contract').get(pk=d.campaign_id)
    if c.blocked or c.advertiser.status=='BLOCKED' or c.contract.status=='BLOCKED':return False
    destination(d.snapshot['destination'],TestPolicy.load())
    return banner(d.snapshot['template_id'])['sha256']==d.snapshot['asset_sha256']

def currently_serving(d,policy,now):
    if d.status!='SELECTED' or not current_safety(d,policy) or now>=d.expires_at:return False
    c=Campaign.objects.select_related('advertiser','contract','active_revision').get(pk=d.campaign_id)
    return bool(c.enabled and not c.pending_change and c.active_revision_id==d.revision_id
                and c.advertiser.status=='ACTIVE' and c.contract.status=='APPROVED'
                and c.contract.advertiser_id==c.advertiser_id
                and c.contract.starts_at<=now<c.contract.ends_at
                and timestamp(d.snapshot['starts_at'])<=now<timestamp(d.snapshot['ends_at']))

def public_decision(d,policy,now=None):
    now=now or timezone.now()
    try: displayable=currently_serving(d,policy,now) if d.status=='SELECTED' else (d.status=='DEFAULT' and now<d.expires_at and current_safety(d,policy))
    except AdError: displayable=False
    creative=None
    if displayable:
        creative={k:d.snapshot[k] for k in ('template_id','title','alt_text')}
        creative.update(image_url=creative_path(d),width=300,height=600,label='광고')
    return {'decision_id':str(d.pk),'context_id':str(d.context_id),'placement':d.placement,'status':d.status,
            'displayable':bool(displayable),'expires_at':d.expires_at.isoformat(),'creative':creative,
            'policy_version':d.policy_version,'test_mode':True,'reason':d.snapshot.get('selection_reason'),
            'events_path':f'/internal/v1/decisions/{d.pk}/events/','cost_applied':False}

def clean_request(body):
    allowed={'request_id','visitor_id','context_id','placement','signals','organic_product_ids'}
    if not isinstance(body,dict) or set(body)-allowed:raise AdError('INVALID_REQUEST','허용된 맥락 요약 필드만 전달하세요.')
    for k in ('request_id','context_id','visitor_id','placement'):
        if k not in body:raise AdError('INVALID_REQUEST',f'{k}가 필요합니다.')
    uuid_value(body['request_id']);uuid_value(body['context_id']);visitor(body['visitor_id'])
    if body['placement'] not in PLACEMENTS:raise AdError('UNSUPPORTED_PLACEMENT','PC 편집·결과 자리만 지원합니다.')
    signals=body.get('signals',[]);products=body.get('organic_product_ids',[])
    if not isinstance(signals,list) or len(signals)>30:raise AdError('INVALID_SIGNALS','맥락 요약을 확인하세요.')
    for s in signals:
        if (not isinstance(s,dict) or set(s)!={'key','source'} or s['source'] not in SOURCES
                or not isinstance(s['key'],str) or not 1<=len(s['key'])<=64
                or not s['key'].isascii() or not all(c.isalnum() or c in '-_' for c in s['key'])):
            raise AdError('INVALID_SIGNALS','허용된 출처와 분류 요약 키만 전달하세요.')
    if not isinstance(products,list) or len(products)>100 or any(not isinstance(x,str) or not 1<=len(x)<=180 for x in products):
        raise AdError('INVALID_PRODUCTS','일반 추천 상품 ID 목록을 확인하세요.')
    return {**body,'signals':signals,'organic_product_ids':products}

@transaction.atomic
def decide(body,now=None):
    policy=TestPolicy.load();body=clean_request(body);now=now or timezone.now();gate();bind_policy(policy)
    key=uuid_value(body['request_id']);owner=visitor(body['visitor_id']);cid=uuid_value(body['context_id'])
    ih=digest({k:v for k,v in body.items() if k!='request_id'})
    previous=RequestKey.objects.select_related('decision').filter(pk=key).first()
    if previous:
        if previous.input_hash!=ih:raise AdError('IDEMPOTENCY_CONFLICT','같은 요청 키의 본문이 변경됐습니다.',409)
        return previous.decision
    ch=digest({'signals':body['signals'],'organic_product_ids':body['organic_product_ids']})
    context,created=Context.objects.get_or_create(pk=cid,defaults={'visitor_hash':owner,'fingerprint':ch})
    if context.visitor_hash!=owner or context.fingerprint!=ch:raise AdError('CONTEXT_CONFLICT','새 맥락 ID로 요청하세요.',409)
    old=Decision.objects.filter(context=context,placement=body['placement']).first()
    if old:
        if old.input_hash!=ih:raise AdError('CONTEXT_CONFLICT','맥락과 자리의 요청이 변경됐습니다.',409)
        RequestKey.objects.create(pk=key,decision=old,input_hash=ih);return old
    related={target for signal in body['signals'] for target in TARGETS.get(signal['key'],[])}
    candidates=[];general=[];reasons=Counter()
    for c in Campaign.objects.select_related('advertiser','contract','active_revision').order_by('id'):
        r=c.active_revision
        reason=None
        if c.blocked or c.advertiser.status=='BLOCKED' or c.contract.status=='BLOCKED':reason='POLICY_BLOCKED'
        elif not r or r.status!='APPROVED' or not c.enabled or c.pending_change or c.advertiser.status!='ACTIVE':reason='NOT_ACTIVE_APPROVED'
        elif c.contract.advertiser_id!=c.advertiser_id or c.contract.status!='APPROVED' or not c.contract.starts_at<=now<c.contract.ends_at:reason='CONTRACT_UNAVAILABLE'
        elif not r.starts_at<=now<r.ends_at:reason='OUTSIDE_PERIOD'
        elif body['placement'] not in r.placements:reason='PLACEMENT_MISMATCH'
        elif r.kind=='PRODUCT' and (not r.product_verified or not r.product_id):reason='PRODUCT_UNVERIFIED'
        elif r.product_id and r.product_id in body['organic_product_ids']:reason='ORGANIC_DUPLICATE'
        else:
            try:validate_asset(r,policy);destination(r.destination,policy)
            except AdError:reason='CREATIVE_OR_DESTINATION_UNAVAILABLE'
        if reason:reasons[reason]+=1;continue
        window=now-timedelta(seconds=policy.frequency_window_seconds)
        claims=Decision.objects.filter(visitor_hash=owner,campaign=c,status='SELECTED').filter(
            Q(rendered_at__gt=window)|Q(rendered_at__isnull=True,expires_at__gt=now)).count()
        if claims>=policy.frequency_limit:reasons['FREQUENCY_LIMIT']+=1;continue
        if r.template_id in related:candidates.append(c)
        if r.is_general:general.append(c)
    selection_reason='RELATED' if candidates else 'GENERAL_NO_CONTEXT' if not related else 'GENERAL_NO_RELATED'
    pool=candidates or general
    by_advertiser={}
    for c in sorted(pool,key=lambda c:(-c.active_revision.bid_points,str(c.pk))):by_advertiser.setdefault(c.advertiser_id,c)
    winner=next(iter(sorted(by_advertiser.values(),key=lambda c:(-c.active_revision.bid_points,str(c.pk)))),None)
    snapshot={'selection_reason':selection_reason,'relation_version':TARGET_MAP_VERSION,'relation_checksum':digest(TARGETS),
              'test_policy':asdict(policy),'candidate_campaign_ids':[str(c.pk) for c in pool],
              'representative_campaign_ids':[str(c.pk) for c in by_advertiser.values()],
              'candidate_bids':{str(c.pk):c.active_revision.bid_points for c in pool},
              'brand_product_comparison':'NOT_APPLICABLE_WITHOUT_PRODUCT_ID'}
    kwargs={}
    if winner:
        r=winner.active_revision
        snapshot.update(template_id=r.template_id,title=r.title,alt_text=r.alt_text,bid_points=r.bid_points,
                        destination=r.destination,asset_sha256=r.asset_sha256,starts_at=r.starts_at.isoformat(),
                        ends_at=r.ends_at.isoformat(),advertiser_name=winner.advertiser.name)
        kwargs={'status':'SELECTED','campaign':winner,'revision':r}
    else:
        notice=DefaultNotice.objects.filter(active=True,approved=True).order_by('id').first()
        if notice:
            try:
                a=validate_asset(notice,policy)
                if a['sha256']!=notice.asset_sha256:notice=None
            except AdError:notice=None
        if notice:
            kwargs={'status':'DEFAULT','notice':notice}
            snapshot.update(template_id=notice.template_id,title=notice.title,alt_text=notice.title,asset_sha256=notice.asset_sha256,selection_reason='APPROVED_DEFAULT_NOTICE')
        else:kwargs={'status':'NO_FILL'};snapshot['selection_reason']='NO_ELIGIBLE_CANDIDATE'
    d=Decision.objects.create(context=context,visitor_hash=owner,placement=body['placement'],input_hash=ih,
          snapshot=snapshot,diagnostics=dict(reasons),policy_version=policy.version,created_at=now,
          expires_at=now+timedelta(seconds=policy.decision_ttl_seconds),**kwargs)
    RequestKey.objects.create(pk=key,decision=d,input_hash=ih)
    return d

def own_decision(decision_id,visitor_id):
    d=Decision.objects.get(pk=uuid_value(decision_id))
    if d.visitor_hash!=visitor(visitor_id):raise AdError('FORBIDDEN','다른 방문자의 결정입니다.',403)
    return d

def click_destination(d,policy,now):
    if now>=d.expires_at:return {'allowed':True,'url':'/test-ended/','reason':'DECISION_EXPIRED','eligible':False}
    if not current_safety(d,policy):return {'allowed':False,'url':None,'reason':'POLICY_BLOCKED','eligible':False}
    if d.status!='SELECTED':return {'allowed':False,'url':None,'reason':'NOT_A_CAMPAIGN','eligible':False}
    eligible=currently_serving(d,policy,now)
    url=destination(d.snapshot['destination'],TestPolicy.load())
    if not eligible and url.startswith('/test-destination/'):
        url='/test-ended/'
    return {'allowed':True,'url':url,
            'reason':'ACTIVE' if eligible else 'SAFE_ENDED_OR_PAUSED','eligible':eligible}

def reconcile(d,policy,now):
    pending=d.events.filter(status='PENDING').order_by('received_at','id')
    for event in pending:
        if now-event.received_at>timedelta(seconds=min(policy.click_join_seconds,policy.correction_seconds)):
            event.status='DIAGNOSTIC';event.reason='RENDER_JOIN_EXPIRED'
        elif not d.rendered_at:continue
        elif event.eligible_at_receipt and currently_serving(d,policy,now):
            field='clicked_at' if event.kind=='CLICK' else 'visible_at'
            if getattr(d,field) is None:
                setattr(d,field,event.received_at);event.status='VALID';event.reason='LINKED_TO_RENDER'
            else:event.status='DUPLICATE';event.reason='DECISION_ALREADY_COUNTED'
        else:event.status='DIAGNOSTIC';event.reason='NOT_ELIGIBLE'
        event.save(update_fields=['status','reason'])
    d.save(update_fields=['rendered_at','visible_at','clicked_at'])

@transaction.atomic
def record_event(decision_id,body,now=None):
    TestPolicy.load();now=now or timezone.now();gate()
    if not isinstance(body,dict) or set(body)-{'event_id','visitor_id','kind','occurred_at','rendered','fraction','continuous_ms','active'}:
        raise AdError('INVALID_EVENT','허용된 사건 필드만 전달하세요.')
    eid=uuid_value(body.get('event_id'));d=own_decision(decision_id,body.get('visitor_id'))
    policy=saved_policy(d)
    fp=digest({'decision_id':str(d.pk),**body})
    old=Event.objects.filter(pk=eid).first()
    if old:
        if old.fingerprint!=fp:raise AdError('EVENT_CONFLICT','같은 사건 ID의 내용이 변경됐습니다.',409)
        reconcile(d,policy,now);old.refresh_from_db()
        return d,old
    kind=body.get('kind')
    if kind not in {'RENDERED','VISIBLE','CLICK','LOAD_ERROR'}:raise AdError('INVALID_EVENT','사건 종류를 확인하세요.')
    occurred=timestamp(body.get('occurred_at'))
    if occurred>now+timedelta(seconds=policy.clock_skew_seconds) or occurred<d.created_at-timedelta(seconds=policy.clock_skew_seconds):
        raise AdError('INVALID_TIME','사건 시각이 결정 범위를 벗어났습니다.')
    if kind=='RENDERED' and body.get('rendered') is not True:raise AdError('INVALID_RENDER','실제 렌더링 성공 신호가 필요합니다.')
    if kind=='VISIBLE':
        f=body.get('fraction');ms=body.get('continuous_ms');active=body.get('active')
        if type(f) not in (int,float) or not math.isfinite(f) or not 0<=f<=1 or type(ms) is not int or ms<0 or type(active) is not bool:
            raise AdError('INVALID_VISIBILITY','가시성 신호 형식을 확인하세요.')
    try:eligible=currently_serving(d,policy,now)
    except AdError:eligible=False
    state,reason='DIAGNOSTIC','NOT_ELIGIBLE'
    if kind=='LOAD_ERROR':reason='LOAD_ERROR'
    elif kind=='CLICK':
        try:move=click_destination(d,policy,now)
        except AdError:move={'allowed':False,'url':None,'reason':'LINK_UNAVAILABLE','eligible':False}
        eligible=move['eligible']
        if eligible:
            if d.clicked_at:state,reason='DUPLICATE','DECISION_ALREADY_COUNTED'
            elif d.rendered_at:d.clicked_at=now;state,reason='VALID','RENDER_LINKED'
            else:state,reason='PENDING','AWAITING_RENDER'
        else:reason=move['reason']
    elif eligible and kind=='RENDERED':
        if d.rendered_at:state,reason='DUPLICATE','DECISION_ALREADY_COUNTED'
        else:d.rendered_at=now;state,reason='VALID','RENDER_CONFIRMED'
    elif eligible and kind=='VISIBLE':
        if body['fraction']<.5 or body['continuous_ms']<1000 or not body['active']:reason='VISIBILITY_THRESHOLD_NOT_MET'
        elif d.visible_at:state,reason='DUPLICATE','DECISION_ALREADY_COUNTED'
        elif d.rendered_at:d.visible_at=now;state,reason='VALID','VISIBLE_CONFIRMED'
        else:state,reason='PENDING','AWAITING_RENDER'
    payload={k:v for k,v in body.items() if k not in {'visitor_id','event_id'}}
    e=Event.objects.create(pk=eid,decision=d,kind=kind,fingerprint=fp,occurred_at=occurred,received_at=now,
                           status=state,reason=reason,payload=payload,eligible_at_receipt=eligible)
    d.save(update_fields=['rendered_at','visible_at','clicked_at'])
    reconcile(d,policy,now)
    e.refresh_from_db();return d,e

def event_response(d,e,now=None):
    result={'event_id':str(e.pk),'status':e.status,'reason':e.reason,'cost_applied':False}
    if e.kind=='CLICK':
        try:move=click_destination(d,TestPolicy.load(),now or timezone.now())
        except AdError:move={'allowed':False,'url':None,'reason':'LINK_UNAVAILABLE'}
        result['navigation']={k:move[k] for k in ('allowed','url','reason')}
    return result

def report():
    # Recompute from canonical decision markers: no additive counter drift.
    rows={}
    for d in Decision.objects.filter(status='SELECTED',rendered_at__isnull=False).iterator():
        day=d.rendered_at.astimezone(ZoneInfo('Asia/Seoul')).date().isoformat()
        key=(day,str(d.campaign_id),str(d.revision_id),d.placement)
        row=rows.setdefault(key,{'date':day,'campaign_id':key[1],'revision_id':key[2],'placement':key[3],
                                'rendered':0,'visible':0,'valid_clicks':0,'visible_valid_clicks':0})
        row['rendered']+=1;row['visible']+=int(d.visible_at is not None);row['valid_clicks']+=int(d.clicked_at is not None)
        row['visible_valid_clicks']+=int(d.visible_at is not None and d.clicked_at is not None)
    for row in rows.values():
        row['ctr_percent']=100*row['valid_clicks']/row['rendered'] if row['rendered'] else None
        row['visible_ctr_percent']=100*row['visible_valid_clicks']/row['visible'] if row['visible'] else None
    counts=Counter(Decision.objects.values_list('status',flat=True))
    diagnostics=Counter(Event.objects.exclude(status='VALID').values_list('reason',flat=True))
    for excluded in Decision.objects.values_list('diagnostics',flat=True):diagnostics.update(excluded)
    return {'rows':list(rows.values()),'selection_counts':dict(counts),'diagnostic_counts':dict(diagnostics),
            'generated_at':timezone.now().isoformat(),'timezone':'Asia/Seoul',
            'test_mode':True,'cost_applied':False,'budget_adopted':False}
