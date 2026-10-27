"""Compatibility boundary for the current SketchOn UI. No UI imports or DB writes."""
from datetime import datetime
from zoneinfo import ZoneInfo
from .errors import AdError
from .integration import normalize_placement
from .policy import TEMPLATE_IDS, integer

MEDIA_ID='sketchon'
UI_CATEGORIES={'sofa','bed','desk','plant','lamp'}


def ui_contract():
    return {'contract_version':'sketchon-ui-v1','media_id':MEDIA_ID,
            'slots':{'editor-right':'EDITOR_RIGHT','result-right':'RESULT_RIGHT'},
            'unsupported_slots':['my-right'], 'campaign_validation_only':True,
            'review_statuses':['PENDING','APPROVED','REJECTED','HOLD'],
            'template_selection':'EXPLICIT_ONLY','uploaded_creatives_supported':False,
            'local_ui_approval_transferred':False,'cost_applied':False}


def decision_request(body):
    allowed={'media_id','request_id','visitor_id','context_id','slot_id','signals','organic_product_ids'}
    if not isinstance(body,dict) or set(body)-allowed or body.get('media_id',MEDIA_ID)!=MEDIA_ID:
        raise AdError('INVALID_UI_REQUEST','지원하는 매체와 UI 요청 필드를 확인하세요.')
    try:placement=normalize_placement(body.get('slot_id'))
    except ValueError:raise AdError('UNSUPPORTED_PLACEMENT','편집·결과의 PC 광고 위치만 지원합니다.') from None
    return {**{k:v for k,v in body.items() if k not in {'media_id','slot_id'}},'placement':placement}


def display_response(result):
    aliases={'EDITOR_RIGHT':'editor-right','RESULT_RIGHT':'result-right'}
    return {**result,'media_id':MEDIA_ID,'slot_id':aliases[result['placement']]}


def _number(value,name):
    # Django form payloads are strings; never truncate a decimal or coerce bool.
    if isinstance(value,str) and value.isascii() and value.isdecimal():value=int(value)
    return integer(value,name)


def _time(value):
    try:
        if not isinstance(value,str):raise ValueError()
        parsed=datetime.fromisoformat(value.replace('Z','+00:00'))
        # Current UI labels datetime-local inputs explicitly as KST.
        if parsed.tzinfo is None:parsed=parsed.replace(tzinfo=ZoneInfo('Asia/Seoul'))
        return parsed
    except ValueError:raise AdError('INVALID_TIME','UI 일시는 ISO 시각 또는 KST datetime-local 형식이어야 합니다.') from None


def campaign_preview(body):
    allowed={'campaign_id','title','description','advertiser','creative','landing_url','category','slot_id',
             'bid_units','starts_at','ends_at','active','review_status','review_reason','reviewed_by','reviewed_at',
             'content_version','reviewed_version','revision','policy_version','reason','template_id','media_id','id'}
    if not isinstance(body,dict) or set(body)-allowed or body.get('media_id',MEDIA_ID)!=MEDIA_ID:
        raise AdError('INVALID_UI_CAMPAIGN','지원하는 캠페인 필드만 전달하세요.')
    try:placement=normalize_placement(body.get('slot_id'))
    except ValueError:raise AdError('UNSUPPORTED_PLACEMENT','My·모바일 광고는 현재 집행하지 않습니다.') from None
    template=body.get('template_id')
    if not isinstance(template,str) or template not in TEMPLATE_IDS:
        raise AdError('TEMPLATE_SELECTION_REQUIRED','UI 품목이나 업로드 경로로 소재를 추정하지 않습니다. 기존 PNG 소재를 명시하세요.')
    status=body.get('review_status','PENDING')
    if status not in ('PENDING','APPROVED','REJECTED','HOLD'):
        raise AdError('INVALID_REVIEW_STATUS','UI 심사 상태를 확인하세요.')
    if body.get('active',False) not in (True,False,'True','False','true','false','on','off','') or type(body.get('active',False)) not in (bool,str):
        raise AdError('INVALID_ACTIVE','집행 요청 활성 값을 확인하세요.')
    for key,limit,required in [('campaign_id',48,True),('title',80,True),('advertiser',100,False),('description',500,False)]:
        value=body.get(key,'')
        if not isinstance(value,str) or len(value)>limit or (required and not value.strip()):
            raise AdError('INVALID_UI_CAMPAIGN',f'{key} 값을 확인하세요.')
    category=body.get('category')
    if not isinstance(category,str) or category not in UI_CATEGORIES:
        raise AdError('INVALID_CATEGORY','현행 UI 가구 품목을 확인하세요.')
    start,end=_time(body.get('starts_at')),_time(body.get('ends_at'))
    if start>=end:raise AdError('INVALID_PERIOD','종료 일시는 시작 뒤여야 합니다.')
    destination=body.get('landing_url')
    if not isinstance(destination,str) or not destination:
        raise AdError('INVALID_DESTINATION','목적지 주소를 명시하세요.')
    mapped={'template_id':template,'title':body['title'],'alt_text':body['title'],
            'bid_points':_number(body.get('bid_units'),'bid'),'placements':[placement],
            'destination':destination,'starts_at':start.isoformat(),'ends_at':end.isoformat()}
    return {'status':'VALIDATED_ONLY','persisted':False,'displayable':False,'cost_applied':False,
            'source_campaign_id':body['campaign_id'],'source_review_status':status,
            'source_category':category,'category_mapping_applied':False,
            'revision_draft':mapped,'required_review_status':'PENDING','activation_allowed':False,
            'requires_advertiser_and_contract_binding':True,
            'warnings':['UI_APPROVAL_NOT_TRANSFERRED','UPLOAD_NOT_IMPORTED','BID_CHANGE_REQUIRES_NEW_REVIEW']}
