# 최신 UI 광고 연동 계약

2026-10-19 현재 로컬 구현 기준. UI/Application과 광고 서버를 HTTP로 연결하며 DB와 관리 계정은 분리한다.

## 현재 연결 상태

| 영역 | 상태 |
|---|---|
| 편집 editor-right | 프로젝트 소유권 확인 후 배너 선정·표시·이벤트 전송. 사진 교체 시 맥락 갱신 |
| 직접 추천 result-right | 추천 결과 준비 후 실제 items 상품 ID로 광고 중복 제외 |
| My·모바일 | 광고 요청·표시 대상에서 제외. PC도 창 폭 1,280px 미만이면 숨김 |
| operations/ad_delivery.py | /api/ads/display/ 및 결정별 events 중계. 세션·CSRF·소유권 검증 |
| operations/ad_gateway.py | 활성화 설정 안내. UI 운영 캠페인의 광고 서버 동기화는 미구현 |
| 광고 서버 관리자 | 광고주·계약·버전 승인·집행·모의 비딩 관리 |

UI는 빈 signals로 일반 후보를 요청한다. 확인된 이미지 객체 기반 맞춤 광고 선정은 별도다. 브라우저에 서버 인증값을 전달하지 않는다. 승인만으로 자동 집행되지 않는다.

## 서버 간 API

기존 4차 모의 운영 설정과 서버 Bearer 인증이 필요하다. 기본 AD_TEST_OPERATIONS=0에서는 503이다. 토큰을 브라우저에 제공하지 않는다.

| 경로 | 메서드 | 기능 |
|---|---|---|
| /internal/v1/ui/contract/ | GET | 위치·소재·심사 경계 확인. DB 조회 없음 |
| /internal/v1/ui/decisions/ | POST | slot_id 변환 후 기존 선정 엔진 호출. 선정 결과 DB 저장 |
| /internal/v1/ui/campaigns/validate/ | POST | 캠페인 필드 검증·변환 미리보기. DB 조회·저장 없음 |

조회 입력 예:

```json
{
  "media_id": "sketchon",
  "request_id": "00000000-0000-4000-8000-000000000001",
  "context_id": "00000000-0000-4000-8000-000000000002",
  "visitor_id": "pseudonymous-browser-key-00001",
  "slot_id": "result-right",
  "signals": [],
  "organic_product_ids": []
}
```

Application에서 소유권 확인 후 `AdServerClient.ui_display(body)`를 호출한다. 공개 응답에 media_id/slot_id를 유지하고 이미지 주소는 public_base_url 기준 절대 주소로 변환한다. 직접 HTTP 응답의 상대 이미지 경로를 UI origin에 붙이지 않는다. category는 조회 입력으로 받지 않으며 확정된 맥락 요약은 기존 signals 계약을 따른다. 빈 signals는 승인된 일반 후보를 사용한다.

동일 요청은 request_id를 재사용하며 기존 맥락·자리 중복 방지·빈도 제한을 그대로 적용한다. 클릭·노출은 `AdServerClient.event()`를 사용한다. Application이 소유권과 방문자를 확인하고 실제 사건만 중계한다. navigation.allowed가 참인 경우에만 이동하며 내부 상세·종료 안내 경로는 광고 서버 공개 주소로 보정한다. HTTPS 목적지는 기존 서버 허용 호스트 정책을 따른다.

## 캠페인 대응

| UI 필드 | 서버 검증·대응 |
|---|---|
| campaign_id | source_campaign_id로 보존. 서버 UUID와 동일시하지 않음 |
| title | title·alt_text |
| bid_units | bid_points. 정수·정수 문자열 1~10000만 허용 |
| slot_id | editor-right/result-right → EDITOR_RIGHT/RESULT_RIGHT. my-right 거절 |
| landing_url | 기존 서버 목적지 허용 정책으로 검증 |
| starts_at/ends_at | 시간대 포함 ISO 또는 UI의 KST datetime-local. 종료가 시작 뒤여야 함 |
| category | sofa/bed/desk/plant/lamp 원본 요약만 유지. 타기팅으로 자동 변환하지 않음 |
| creative | 업로드 경로를 읽거나 다운로드하지 않음 |
| template_id | 기존 PNG 10종 중 하나를 연동 요청에 명시적으로 추가해야 함 |
| review_status | PENDING/APPROVED/REJECTED/HOLD를 원본 상태로만 유지. 서버 승인 미승계 |
| active | 입력 형식 확인만 수행. 자동 활성화하지 않음 |
| advertiser | 이름만으로 광고주·계약 자동 생성 안 함 |
| revision/content_version/reviewed_version | UI 버전과 서버 제출 버전을 동일시하지 않음 |

UI 폼에는 template_id가 없으므로 운영자가 소재를 선택하는 연결 단계가 필요하다. UI 가구 품목 desk와 이름이 같은 desk 배너는 주변기기 소재이므로 자동 대응하지 않는다. UI의 임의 업로드 이미지를 송출하지 않는다.

검증 API는 UI 필드와 template_id를 JSON으로 받는다. `AdServerClient.validate_ui_campaign(body)`를 Application 운영자 백엔드에서 호출한다. VALIDATED_ONLY, persisted=false, displayable=false, activation_allowed=false를 반환하며 revision_draft는 별도 제출 전 검토용이다. 실제 등록에는 광고주·계약 바인딩이 추가로 필요하다. 저장·동기화·승인·활성화 API가 아니다.

UI에서 APPROVED라도 서버의 새 제출 버전은 PENDING으로 재심사해야 한다. HOLD를 승인으로 바꾸지 않는다. UI에서는 bid_units 변경만으로 승인이 취소되지 않을 수 있으나 광고 서버에서는 입찰 변경도 새 버전 심사 대상이다. 실제 연결 전에 심사 원장과 권한을 합의해야 한다.

## 검수와 남은 범위

광고 서버 자동 테스트 88개와 UI 자동 테스트 34개를 격리된 SQLite에서 검수했다. 브라우저에서 편집·추천 결과 배너와 표시·클릭·오류 처리를 확인했다. MySQL 연결·동시성, UI 운영 캠페인 저장/승인 동기화, 운영 배포는 별도 검수 대상이다.

이 문서의 campaigns/validate는 검증 미리보기이며 실제 집행 API가 아니다. 광고 운영은 광고 서버 관리자에서 수행한다.
