# SketchOn 독립 광고 테스트 서버

첨부 정책 `ad-policy-bidding-proposal_ver2.md`의 3차 필수 범위와 4차 모의 운영을 구현했다. 기존 Django·추천·수집 소스와 DB는 사용하거나 변경하지 않는다. 기본값은 이 디렉토리의 `runtime/ads.sqlite3`이며 MySQL 선택 설정도 준비했다. 실제 MySQL에는 접속·반영하지 않았다. [연결 준비와 테이블 생성 절차](examples/MYSQL_SETUP.md)를 따른다.

## 구현 범위

| 구분 | 구현 |
| --- | --- |
| 3차 | 기존 PNG 10종 검증, 개별 이미지 제공, 배너 선택·로드 확인 화면, 다른 화면에 붙일 수 있는 독립 표시 경로 |
| 4차 테스트 | 가상 광고주·모의 계약·캠페인 관리자, 새 버전 제출·승인·거절, 별도 활성화·중지·차단 |
| 선정 | 확인된 맥락 요약 → 적격 검사 → 관련 후보 우선 → 일반 후보 → 광고주별 대표 → 입찰 내림차순·캠페인 UUID 문자열 오름차순 |
| 중복·한도 | 요청·맥락·자리별 결정 재사용, NO_FILL 재사용, 방문자·캠페인 빈도 제한, 동시 요청 직렬화 |
| 측정 | 실제 렌더링·가시 노출·클릭 분리, 사건 재전송 중복 방지, 선도착 사건 연결 대기, KST 노출일 성과 재계산 |
| 연동 준비 | 서버 간 인증 API, 기존 추천 응답을 읽는 선택적 어댑터, 장애를 UNAVAILABLE로 반환하는 클라이언트 |

**현재 demo_sketchon의 편집·직접 추천 화면에 광고 송출을 연결했다.** Application이 소유권을 확인하고 광고 선정·이벤트를 중계한다. 현재 UI 신호는 빈 배열로 일반 후보를 사용한다. 실제 사진에서 물체를 검출하는 기능은 추가하지 않았다. 4차 테스트 화면에서 확인 품목을 고르거나, 기존 분석 결과에서 확정된 요약을 전달한다. 추가 LLM 호출은 없다.

실결제·CPC 예약/차감·예산 원장·회원 개인화·광고주 직접 로그인·모바일 집행·추가 소재 업로드는 구현 범위에서 제외했다. 기존 10종만 제공한다. 관리자 대리 운영이며 자동 심사·자동 승인은 없다.

## 1. 3차 배너 확인 실행

Windows PowerShell에서 실행한다. Python 3.12 이상을 권장하며, 이번 검수는 Python 3.12·Django 5.2에서 수행했다.

```powershell
cd C:\sketch_platform\src\ad_server
# .venv가 없을 때만 생성
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
.\.venv\Scripts\python.exe manage.py check_banners
.\.venv\Scripts\python.exe manage.py runserver 127.0.0.1:8004 --insecure
```

- 확인 화면: `http://127.0.0.1:8004/`
- 배너 목록: `/api/v1/templates/`
- 개별 이미지: `/assets/desk/` 등
- 독립 배너: `/embed/desk/` 등
- 상태 확인: `/health/` (이미지 10종 준비 상태만 검사)

3차 화면은 DB 초기화·광고주·운영 설정 없이 작동한다. 배너 10개를 한꺼번에 집행하지 않고 선택한 1개만 보여준다. 원본 600×1200, 표시 300×600, `object-fit: contain`으로 자르거나 왜곡하지 않는다. 고정 목록 외의 파일과 `thumbs.db`는 제공하지 않는다.

`runserver --insecure`는 이 로컬 테스트에서 관리자 정적 파일을 제공하기 위한 실행 방법이다. 운영 배포는 별도 WSGI/정적 파일 서버 구성이 필요하다. 현재 호스트는 로컬만 허용한다. 내부망 사용 시 `AD_SERVER_HOSTS`에 승인 호스트를 명시하고 별도로 방화벽·TLS를 구성한다.

## 2. 4차 모의 운영 켜기

기본값은 꺼짐이다. 운영 정책의 P-01~P-09는 아직 확정되지 않았다. `test_policy.example.json`의 숫자는 **로컬 검증용 예시이며 실제 승인 정책이 아니다.** 예시를 명시적으로 선택해야 작동한다. 설정을 변경할 때 `version`도 `test-local-2`처럼 변경해야 한다. 같은 버전의 내용 변경은 거절한다.

같은 PowerShell 창에서:

```powershell
cd C:\sketch_platform\src\ad_server
$env:AD_TEST_OPERATIONS = '1'
$env:AD_TEST_POLICY_FILE = (Resolve-Path .\test_policy.example.json).Path
$env:AD_SERVER_SECRET_KEY = (& .\.venv\Scripts\python.exe -c "import secrets; print(secrets.token_urlsafe(48))")
$env:AD_SERVICE_TOKEN = (& .\.venv\Scripts\python.exe -c "import secrets; print(secrets.token_urlsafe(48))")
.\.venv\Scripts\python.exe manage.py migrate
.\.venv\Scripts\python.exe manage.py seed_ads_demo
.\.venv\Scripts\python.exe manage.py createsuperuser
.\.venv\Scripts\python.exe manage.py runserver 127.0.0.1:8004 --insecure
```

키는 화면·문서·Git·브라우저에 복사하지 않는다. 창을 닫으면 위 환경변수는 사라진다. 지속 운영 시 별도 비밀 관리로 같은 SECRET_KEY를 안전하게 유지해야 가명 방문자 대조·세션이 유지된다. `.env.example`은 참고용이며 자동으로 읽지 않는다.

`/admin/`에서 로그인 후 `/sandbox/`로 이동한다. 시연 화면은 선정 이유·포인트 비교표·이번 광고 반응·전체 누적 현황을 한글로 표시한다. 원문 JSON은 접힌 개발용 상세 정보에서만 확인한다. 테스트 자료는 가상 광고주 2명, 같은 주변기기 배너의 700/500 포인트 경쟁 캠페인 2개다. 실제 광고주·계약·상품을 의미하지 않는다. `seed_ads_demo` 재실행은 운영자가 변경한 기존 자료를 덮거나 재활성화하지 않는다.

### 관리자 처리 순서

처음 등록은 홈의 **새 광고 등록 시작**을 사용한다. 저장 후 광고주·계약·캠페인이 다음 화면에 이어지며, 제출 상세에서 직접 심사 후 집행 설정으로 이동한다. 승인과 집행은 별도이며 비딩 시연에서 새 광고를 강제로 선정하지 않는다. [처음부터 배너 확인까지](examples/LAN_DEMO.md#처음부터-새-광고-등록하기)를 따른다.

관리자 메뉴·필드·상태는 한글로 표시한다. 홈의 6단계 바로가기를 따라 등록·승인·집행을 진행한다. 표시 위치는 체크박스로 선택하며 DB 필드명과 API 값은 유지한다. [내부망 시연 사용법](examples/LAN_DEMO.md)을 함께 참고한다.

1. Advertisers: 가상 광고주를 만들고 `ACTIVE`로 설정한다.
2. Mock contracts: 같은 광고주에 모의 계약·승인 상태·유효 기간을 등록한다.
3. Campaigns: 광고주와 계약을 연결한다. 처음에는 비활성이다. 생성 후 소유 광고주·계약은 바꾸지 않는다.
4. Revisions: 해당 캠페인의 소재·문구·목적지·기간·자리·입찰 포인트를 등록한다. `placements`는 JSON 배열 `['EDITOR_RIGHT']`가 아니라 **`["EDITOR_RIGHT", "RESULT_RIGHT"]`**처럼 입력한다.
5. 목록에서 버전을 선택하고 ‘제출 버전 승인’ 또는 ‘거절’ 작업을 실행한다. 승인 시 소재 해시와 목적지를 검증한다. 가장 최근 제출만 심사할 수 있다.
6. 승인 후 Campaigns의 `enabled`를 켠다. 승인과 활성화는 별도다.
7. 변경은 Revisions에 **새 버전**을 추가한다. 제출 즉시 신규 집행을 중지하며 과거 제출·승인본은 수정할 수 없다.
8. 일반 중지는 `enabled=False`다. 정책 차단은 `enabled=False`와 `blocked=True`를 함께 저장한다. 활성 상태를 유지한 채 차단만 켜면 모델 검증에서 거절된다. Audit와 읽기 전용 결정·사건 화면으로 결과를 대조한다.

입찰은 1~10,000 정수 포인트다. 잘못된 값을 보정하지 않는다. 브랜드 배너는 상품 ID 없이 등록할 수 있다. 상품형은 운영자가 실제 상품 ID 대응을 확인하고 `product_verified`를 체크해야 한다. 이는 자동 상품 검증 결과가 아니다.

목적지는 `/test-destination/식별자/` 또는 테스트 설정의 `destination_hosts`에 명시한 HTTPS 호스트만 허용한다. 예시 설정은 외부 호스트가 비어 있으므로 테스트 내부 상세만 허용한다. 금지 콘텐츠에 대한 최종 판단은 운영자가 하고 차단한다.

## 3. 기존 서비스와 연결할 계약

최신 로컬 dev UI를 대조해 서버 측 [UI 호환 계약](examples/UI_INTEGRATION.md)을 추가했다. 현재 UI의 operations/ad_delivery.py와 ad_client.py가 실제 광고 조회·이벤트를 중계한다. ad_gateway의 미동기화 표시는 운영 캠페인 동기화 상태이며 배너 송출 상태와 별개다. slot_id 기반 조회와 캠페인 필드 검증 API를 준비했으며 검증만으로 로컬 캠페인을 저장·승인·집행하지 않는다.

**3차:** 기존 편집·결과의 PC 오른쪽 독립 광고 자리에 `/embed/<template_id>/`를 iframe으로 삽입하거나 `/assets/<template_id>/`를 이미지로 연결한다. 홈·로그인·회원가입·관리자 집행, 모바일 위치는 포함하지 않는다. iframe 사용 시 서버 시작 전에 허용 부모 origin을 설정한다.

```powershell
$env:AD_FRAME_ANCESTORS = "'self' http://127.0.0.1:8003"
```

**4차:** 브라우저 → 기존 Application 서버 → 광고 서버로 호출한다. 브라우저는 서비스 인증키를 갖지 않는다. Application 서버가 자기 방문자·작업 소유권을 검증하고 사건을 중계한다. 직접 브라우저 호출을 위한 CORS·공개 결정 API는 열지 않았다.

| 메서드·경로 | 용도 | 인증 |
| --- | --- | --- |
| POST `/internal/v1/decisions/` | 선정·같은 결정 재사용 | 서버 Bearer 키 |
| GET `/internal/v1/decisions/{id}/?visitor_id=...` | 본인 결정·현재 표시 가능 여부 | 서버 Bearer 키 + 가명 방문자 대조 |
| POST `/internal/v1/decisions/{id}/events/` | 렌더링·가시 노출·클릭·소재 오류 | 서버 Bearer 키 + 가명 방문자 대조 |
| GET `/internal/v1/reports/` | 전체 모의 성과·진단 | 서버 Bearer 키. 광고주 공개용 아님 |
| GET `/creative/{id}/{서명}/` | 해당 결정의 이미지 제공 시 현재 상태 재검사 | 결정에 한정한 서버 서명. 서비스 인증키가 아님 |
| `/sandbox/` 및 `/sandbox/...` | 운영자 테스트 화면·사건 | 관리자 세션 + 권한 + POST CSRF |

선정 입력:

```json
{
  "request_id": "00000000-0000-4000-8000-000000000001",
  "visitor_id": "pseudonymous-browser-key-00001",
  "context_id": "00000000-0000-4000-8000-000000000002",
  "placement": "EDITOR_RIGHT",
  "signals": [{"key": "notebook", "source": "CONFIRMED_TEXT"}],
  "organic_product_ids": ["danawa:실제_표시된_상품ID"]
}
```

방문자 키는 16~128자 ASCII 영문·숫자·하이픈·밑줄인 가명값이다. IP·로그인 ID·사진·원문 텍스트를 보내지 않는다. 광고 DB에는 방문자 키 원문 대신 HMAC을 보관한다. 서버 접근 로그에도 URL·요청 자료를 별도 무제한 보관하지 않게 운영 설정이 필요하다.

`source`: `PHOTO_OBJECT`, `CONFIRMED_TEXT`, `REQUEST_CATEGORY`. 사진 인식 결과·확정 텍스트·요청 품목을 구분한다. 광고 서버가 사진을 분석하지 않는다. `signals=[]`면 승인된 일반 후보를 사용한다. 확정되지 않은 상품/물체를 추정해서 넣지 않는다.

현재 10종에 대응하는 연결표는 `advertising/policy.py`의 `TARGETS`다. 예: notebook→desk, coffee→drinks, humidifier→mask. 원문에 있는 가습기·책·안경·빵 자체 광고는 해당 소재가 없으므로 임의 생성하지 않았다. 연결표 변경은 운영 검토 후 코드와 `TARGET_MAP_VERSION`을 함께 바꾸는 방식이다.

`advertising/integration.py`의 `recommendation_context()`는 기존 `state.recommendations.items[].product_id`만 읽고 원문을 변경하지 않는다. `AdServerClient`는 명시적 시간 제한을 받아 서버 간 호출하며, `browser_payload()`는 공개 표시 필드만 추리고 이미지 URL을 광고 서버 절대 경로로 만든다. `public_base_url`에는 브라우저가 접근할 광고 서버 origin을 지정한다. 생략하면 내부 API용 `base_url`과 같은 주소를 사용한다. 두 주소는 하위 경로 없이 origin만 설정한다. `event()`는 서버가 허용한 내부 상세·종료 안내 경로를 공개 광고 서버 절대 주소로 보정하며 위험한 주소는 거절한다. `display()`는 UI 공개 필드만 반환한다. `editor-right`·`result-right` 별칭은 지원하지만 `my-right`는 지원하지 않는다. [Application 연동 예제](examples/application_proxy_example.py)는 사용자·프로젝트 소유권 검사를 호출자가 수행하는 전제이며 UI 라우트를 자동 설치하지 않는다. 현재 UI는 operations/ad_client.py의 클라이언트를 사용한다. 이 예제 파일을 별도로 설치하거나 라우트에 연결할 필요는 없다.

같은 방문자·맥락·자리의 재전송은 요청 UUID가 달라도 같은 결정을 재사용한다. 같은 UUID 본문 변경은 409, 의미 있는 입력 변경은 새 `context_id`를 사용한다. 응답 만료 후 배너를 숨기며 만료만으로 재경매하지 않는다. 응답의 `context_id`가 현재 화면 맥락과 다르면 폐기한다. 새로고침·다른 탭에서 가명 방문자 키를 새로 만들지 않는다.

`NO_FILL`은 정상 후보 없음, `DEFAULT`는 별도 승인 안내, `UNAVAILABLE`/503은 장애·미설정이다. 장애 시 광고 영역을 접고 추천·편집을 계속한다. 재시도는 같은 요청/사건 ID를 사용하며 횟수·대기는 P-04 확정 전 클라이언트에서 임의 무한 반복하지 않는다. 제공 클라이언트는 자동 재시도하지 않는다.

4차 응답의 이미지 경로는 결정별 서명 경로다. 로드할 때 승인 상태·차단·만료를 다시 검사하고 캐시하지 않는다. 3차의 고정 `/assets/` 경로를 4차 집행에 대신 사용하지 않는다. 이미 브라우저에 표시된 바이트를 서버가 원격 삭제하는 기능은 없으므로, 실제 서비스는 만료 숨김·맥락 변경·사건 거절 처리를 함께 연결해야 한다.

### 사건과 안전한 클릭

모든 사건은 `event_id`, `visitor_id`, `kind`, 시간대가 있는 `occurred_at`을 보낸다.

| kind | 추가 필드 | 인정 |
| --- | --- | --- |
| RENDERED | `rendered: true` | 실제 이미지 로드·표시, 결정당 최대 1회 |
| VISIBLE | `fraction`, `continuous_ms`, `active` | 면적 0.5 이상·연속 1000ms 이상·활성, 결정당 최대 1회 |
| CLICK | 없음 | 표시 노출 연결 + 집행/소유권/차단 검사, 결정당 최대 1회 |
| LOAD_ERROR | 없음 | 진단만 기록 |

브라우저의 연속 시간은 숨김·비활성·면적 미달 때 초기화해야 한다. 테스트 화면은 IntersectionObserver와 visibilitychange로 이를 처리한다. 서버는 신뢰된 Application 중계를 전제로 신호 형식·한계·소유권을 검증한다. 브라우저 측정 주장의 진실성을 독립 증명하거나 완전한 부정 클릭 탐지를 제공하지는 않는다.

빠른 클릭은 1초 미만이라고 버리지 않는다. 표시 노출이 뒤늦게 오면 설정된 대기·정정 기간 안에 연결한다. 연결되지 않으면 진단 처리한다. 클릭으로 가시 노출을 추가하지 않는다. 같은 사건 ID·같은 본문은 재사용, 다른 본문은 409다.

클릭 응답의 `navigation.allowed`, `url`, `reason`을 사용한다. 브라우저가 목적지를 제출할 수 없다. 일반 중지·기간 종료·변경 보류는 과거 승인 목적지의 안전성을 재검사하고 성과 없이 이동을 허용한다. 테스트 내부 상세는 종료 안내로 보낸다. 정책 차단은 이동을 금지하고, 결정 만료는 내부 종료 안내만 허용한다. 링크 반환은 구매·외부 페이지 로딩 성공을 의미하지 않는다.

## 4. 저장·동시성·보고

- 광고주 → 모의 계약 → 캠페인 → 제출 버전 → 결정 → 사건을 별도 테이블에 보관한다. 감사·테스트 정책 버전은 별도로 남긴다.
- 결정에는 승인 소재 해시·문구·목적지·기간·후보 입찰·대표·연결표·테스트 설정을 고정한다. 공개 응답에는 경쟁 입찰·후보·방문자·계약을 넣지 않는다.
- 현재 상태가 차단됐거나 승인 파일의 해시가 바뀌면 과거 결정도 표시·위험 이동을 막는다. 변경 중 미승인 URL은 사용하지 않는다.
- SQLite `IMMEDIATE` 트랜잭션과 고유 키로 이 **단일 테스트 DB**에 대한 동시 선정을 직렬화한다. 미렌더링·유효 결정도 잠정 노출 한도에 포함해 복수 탭이 한도를 뚫지 못하게 한다. 만료 후 미렌더링 건은 한도에서 빠진다. 이는 비용 예약이 아니다. 대규모 운영·DB 교체는 추가 검수가 필요하다.
- 사건 시각은 UTC 저장, 성과 날짜는 연결된 표시 노출의 KST 날짜다. 클릭 수신일로 이중 집계하지 않는다.
- 보고는 결정의 확정 표식에서 재계산한다. 선택·NO_FILL·기본 안내 수, 진단 사유, 캠페인·버전·자리·KST 노출일별 표시/가시/클릭/CTR을 구분한다. 분모 0은 `null`이다. 비용·매출·잔액은 만들지 않는다.

선도착 사건의 대기 만료 재검사:

```powershell
.\.venv\Scripts\python.exe manage.py reconcile_ad_events
```

보관 일수는 현재 테스트 설정일 뿐 P-05 확정이 아니다. 자동 스케줄러는 만들지 않았다. 모의 검증 후 삭제는 다음 미리보기를 확인하고 명시적으로 수행한다. 오래된 결정·사건·가명 맥락·감사를 삭제하므로 해당 시점의 과거 보고도 줄어든다. 실제 운영 보관·익명 집계 정책과는 별도다. 캠페인/승인 설정은 이 명령으로 삭제하지 않는다.

```powershell
.\.venv\Scripts\python.exe manage.py purge_test_events
# 삭제 실행이 필요할 때만
.\.venv\Scripts\python.exe manage.py purge_test_events --confirm-delete-test-data
```

## 5. 검증

```powershell
.\.venv\Scripts\python.exe manage.py check
.\.venv\Scripts\python.exe manage.py test advertising --noinput
.\.venv\Scripts\python.exe manage.py check_banners
```

자동 검증은 기본 SQLite 설정에서 별도 `runtime/test-ads.sqlite3`를 사용하고 종료 시 제거한다. `AD_DB_BACKEND=mysql`이면 실제 MySQL 테스트 DB를 생성·삭제하므로 연결 승인 전에는 실행하지 않는다. 접속 없는 설정 확인은 `python manage.py check_ad_database_config`를 사용한다. 가상환경·로컬 DB·캐시·환경변수 파일은 `.gitignore` 대상이다. 기존 추천 DB·MongoDB·외부 API·Gemini를 호출하지 않는다.

검수 결과: 자동 테스트 88개가 격리된 임시 SQLite 테스트 DB에서 통과했다. MySQL 드라이버 로딩·설정 점검은 실제 DB 연결 없이 확인했으며 실제 MySQL의 마이그레이션·동시성은 미검증이다. 구성 검사와 마이그레이션 일관성 검사도 통과했다. 3차 10종 로드·600×1200 원본·300×600 표시·전환·독립 표시 경로를 Edge/Playwright PC 1440×1000·모바일 크기 390×844에서 확인했다. 가로 넘침·예상치 못한 콘솔 오류는 없었다. 모바일 크기 확인은 테스트 도구의 반응형 검수이며 모바일 광고 집행 위치 확정을 뜻하지 않는다. 4차 관리자·심사·모의 선정·사건·집계는 격리 테스트 DB에서 검증했다. 별도 테스트 DB에서 관리자·UI 광고 연결을 브라우저로 검수했다. 테스트 계정은 소스에 포함하지 않으며 수신 환경에서 생성한다. 실제 운영 배포는 미검증이다.

실제 분석 신호의 확인 경로, P-01~P-09 운영 확정, 서비스 간 TLS/프록시, 운영 DB 부하, 회원/광고주 권한 확대는 별도 통합 검증이 필요하다. 이 구현을 실제 서비스 통합 완료로 해석하지 않는다.

심사 사유가 없거나 길이 조건에 맞지 않으면 저장 없이 제출 화면에 오류를 안내한다. 제출 버전 이력에는 승인·반려의 담당자·일시·사유를 표시하며, 기록이 없으면 빈 상태 안내와 복귀 링크를 제공한다.
