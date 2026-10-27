import json
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlsplit
from django.conf import settings
from .errors import AdError

TEMPLATES = (
 ('moisturizer', '보습', '01_moisturizer.png'), ('mask', '마스크', '02_mask.png'),
 ('tumbler', '텀블러', '03_tumbler.png'), ('desk', '주변기기', '04_desk.png'),
 ('phone', '휴대폰 액세서리', '05_phone.png'), ('drinks', '음료', '06_drinks.png'),
 ('stationery', '문구', '07_stationery.png'), ('lenses', '렌즈', '08_lenses.png'),
 ('snacks', '간식', '09_snacks.png'), ('security', '보안', '10_security.png'),
)
TEMPLATE_IDS = {row[0] for row in TEMPLATES}
# v2 section 5.1. Signals are summary keys, never uploaded photos or raw text.
TARGET_MAP_VERSION = 'proposal-v2-relations-1'
TARGETS = {
 'moisturizer': ['moisturizer'], 'humidifier': ['mask'], 'tumbler': ['tumbler', 'drinks'],
 'notebook': ['desk'], 'phone': ['phone'], 'coffee': ['drinks'], 'book': ['stationery'],
 'glasses': ['lenses'], 'bread': ['snacks'], 'cctv': ['security'],
}
# The source table also mentions humidifier→humidifier, book→book, glasses→glasses,
# bread→bread. Those creative types are not among the ten supplied templates.
SOURCES = {'PHOTO_OBJECT', 'CONFIRMED_TEXT', 'REQUEST_CATEGORY'}
PLACEMENTS = {'EDITOR_RIGHT', 'RESULT_RIGHT'}

def integer(value, name, low=1, high=10000):
    if type(value) is not int or not low <= value <= high:
        raise AdError('INVALID_' + name.upper(), f'{name}: {low}~{high} 정수여야 합니다.')
    return value

@dataclass(frozen=True)
class TestPolicy:
    version: str
    frequency_window_seconds: int
    frequency_limit: int
    decision_ttl_seconds: int
    click_join_seconds: int
    correction_seconds: int
    clock_skew_seconds: int
    event_retention_days: int
    asset_max_bytes: int
    asset_min_width: int
    asset_min_height: int
    destination_hosts: tuple

    @classmethod
    def load(cls):
        if not settings.AD_TEST_OPERATIONS:
            raise AdError('OPERATIONS_DISABLED', '3차 배너 연결 모드입니다.', 503)
        if (len(settings.AD_SERVICE_TOKEN) < 32 or not settings.AD_TEST_POLICY_FILE
                or len(settings.SECRET_KEY)<32 or settings.SECRET_KEY=='ad-server-local-template-test-only'):
            raise AdError('POLICY_NOT_CONFIGURED', '4차 테스트 인증키와 명시적 정책 설정이 필요합니다.', 503)
        try:
            data = json.loads(Path(settings.AD_TEST_POLICY_FILE).read_text(encoding='utf-8'))
            if data.pop('test_only', None) is not True:
                raise ValueError('test_only required')
            for key in ('frequency_window_seconds', 'frequency_limit', 'decision_ttl_seconds', 'click_join_seconds',
                        'correction_seconds', 'event_retention_days', 'asset_max_bytes', 'asset_min_width', 'asset_min_height'):
                integer(data.get(key), key, 1, 100000000)
            integer(data.get('clock_skew_seconds'), 'clock_skew_seconds', 0, 300)
            hosts = data.get('destination_hosts')
            if not isinstance(hosts, list) or any(not isinstance(h, str) or not h or '/' in h or ':' in h for h in hosts):
                raise ValueError('destination_hosts')
            if not isinstance(data.get('version'), str) or not data['version'].startswith('test-'):
                raise ValueError('test policy version required')
            data['destination_hosts'] = tuple(hosts)
            return cls(**data)
        except (ValueError, TypeError, KeyError, OSError, AdError) as exc:
            raise AdError('POLICY_NOT_CONFIGURED', '테스트 정책 파일의 필수 항목을 확인하세요.', 503) from exc

def destination(value, policy):
    if not isinstance(value, str) or any(c.isspace() or ord(c) < 32 for c in value) or '\\' in value:
        raise AdError('INVALID_DESTINATION', '승인 가능한 목적지 형식이 아닙니다.')
    # A sandbox internal detail is the only relative route accepted.
    if value.startswith('/test-destination/') and '?' not in value and '#' not in value:
        suffix = value.removeprefix('/test-destination/').strip('/')
        if suffix and suffix.isascii() and suffix.replace('-', '').isalnum():
            return value
    try:
        u = urlsplit(value)
        if (u.scheme == 'https' and u.hostname in policy.destination_hosts and not u.username and not u.password
                and u.port in (None, 443) and u.netloc == u.hostname and not u.fragment):
            return value
    except ValueError:
        pass
    raise AdError('INVALID_DESTINATION', '허용 호스트의 HTTPS 목적지 또는 테스트 상세 경로만 허용합니다.')
