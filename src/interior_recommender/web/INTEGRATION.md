# 기존 RoomPick Canvas → 직접 상품 추천 연결

## 역할별 구현 안내

연동 변경 전 [웹 담당 라우팅](../../../docs/interior-recommender/roles/web.md)과 [현재 호출 구조](../../../docs/interior-recommender/call-structure.md)를 읽는다. 기본 직접 추천 실행은 `application/direct.py`, 추천 규칙은 `domain/`, 카탈로그 준비는 `catalog/`에 있다. 아래 기존 생성 연동 예제의 생성 버튼·템플릿 절차는 `--legacy-generation`용 이력이며 직접 추천의 기본 사용자 절차로 적용하지 않는다.

2026-10-15 기본 웹은 이미지 생성 없는 콘텐츠 추천이다. [실행·상품 준비·제약](../README.md)을 먼저 읽는다. 이 저장소에서 원격 RoomPick의 소스를 수정/배포하지 않았다. 기존 브리지·CSRF·소유자 세션 계약을 유지하며 로컬 파일 저장을 Bronze 완료로 표시하지 않는다.

## 서버와 화면

```powershell
.\.venv\Scripts\python.exe -m interior_recommender.web --products outputs/product-catalog-v1/products.json --output-root outputs/direct-web --port 8765
```

API 없는 확인은 --demo --output-root outputs/direct-demo다. LAN 공유 토큰/서버 측 브리지 설정은 아래 이전 문서의 보안 계약을 유지한다. 제공처 키나 MVP_BRIDGE_TOKEN을 브라우저에 넣지 않는다.

기존 roompick-client.mjs의 submitCanvas를 그대로 사용한다. 방 사진 bytes와 별도 sketch/document/text/project/request_id를 보내며 기존 Canvas의 그리기·텍스트 외 새 필수 절차는 없다. resultUrl(run_id)은 /ai-mvp/?run=...로 직접 추천 화면을 연다. 생성 미리보기·템플릿 선택·생성 버튼은 신규 화면에 없다.

| API | 직접 추천 동작 |
|---|---|
| GET api/csrf | 세션·CSRF 토큰 |
| POST api/runs | 원본/입력 고정·비동기 prepare 접수 |
| GET api/runs/{id} | flow=direct·context·recommendations·job 조회 |
| POST api/runs/{id}/actions/prepare | 같은 고정 정책/인덱스로 실패 단계 재시도·성공 결과 재사용 |
| GET assets/{id}/{path} | 소유자 검증 후 manifest 등록 입력/반환 상품 이미지 |

generation/compare/selection 액션은 직접 추천에서 거절한다. 목록은 items(필수 조건 확인)와 unverified_items(필수 정보 미확인)로 나누며 UNKNOWN을 충족으로 표시하지 않는다. 이미지-only 카탈로그는 상품/품목이 검증되지 않았음을 안내한다. 구매 링크는 검증 API 통합 전 제공하지 않는다. 외부 응답 검증 실패의 내용을 새로 해석하려면 입력을 새로 접수한다.

초기 가중치는 수동 similarity .85 / preferred_color .15이며 무선호는 similarity 1이다. context/query/ranking, receipts, direct_contract의 재현 계약과 mmap 카탈로그 준비는 MVP 안내를 따른다. 실제 DB/Kafka/Bronze/FAISS·광고 서버 HTTP·전체 서비스 v2 API는 별도 구현이다.

<details>
<summary>이전 생성 웹·원격 브리지 연결 이력 — 생성 화면에는 --legacy-generation 필요</summary>

# 기존 RoomPick Canvas 연결

대상 화면은 `http://192.168.0.44:8003/editor/`다. 2026-10-15 읽기 확인 결과 `editor.mjs`는 `project.photo_url`, `project.id/revision/width/height`, `drawing.strokes/items`, `renderArtwork(target, size)`, `searchProducts()`를 사용한다. 현재 `/api/projects/{id}/search/`는 모의 결과다. 이 저장소에 원격 RoomPick 소스가 없으므로 원격 코드를 수정하거나 배포하지 않았다.

이 구현은 기존 그리기·지우기·도형·텍스트·수집 기능 다음에 연결하는 API와 생성 결과 뷰어다. 새 Canvas 편집기를 만들지 않는다. 로컬 샘플 흐름이며 MySQL·Kafka·실제 Delta Bronze 확인은 연결하지 않았다. 파일 보존 성공을 Bronze 완료로 표시하지 않는다.

## 1. AI MVP 서버 실행

현재 웹 버전은 `scene-context-local-v5`, 로컬 계산 정책은 `semantic-results-v1`이다. 연동 입력·선택·생성 버튼은 동일하다. 의미 해석 응답의 품목·속성·추가 의미 구절로 검색 문장을 조립하고, 벡터 정렬·좌표 변환·선택 가능 판정은 서버 코드에서 처리한다. 의미 해석·임베딩·시각 검사는 계속 API를 사용한다. 새 버전은 서버 재시작 후 새 요청에 적용하며 상세 출력/이전 버전 복구 계약은 [MVP 안내](../README.md)를 따른다.

프로젝트 루트에서 의존성을 설치한다. 이미 설치했다면 생략할 수 있다.

```powershell
.\.venv\Scripts\python.exe -m pip install -r .\src\requirements-mvp.lock.txt
.\.venv\Scripts\python.exe -m interior_recommender.web --legacy-generation --templates .\src\interior_recommender\samples\templates.json
```

기본 주소는 `http://127.0.0.1:8765/`다. `src/interior_recommender/.env`의 기존 Gemini·Cloudflare 설정을 자동으로 읽는다.

2026-10-15 사용자 요청에 따라 로컬 MVP에서는 `outputs/danawa_embeddings/index.json`이 있으면 다나와 저장 벡터를 우선 사용한다. `--danawa-index .\outputs\danawa_embeddings\index.json`으로 경로를 명시할 수도 있다. 이 모드에서는 `--templates`의 chair-01 대신 다나와 후보를 표시하고, 선택한 이미지를 생성 참조로 전달한다. 이는 문서의 서비스용 템플릿/상품 분리 설계와 구분되는 MVP 시험 동작이다.

완료 벡터의 모델·차원·전처리·이미지 및 출력 checksum을 검증하고 입력 실행에 이미지와 벡터를 복사한다. 이미지 재임베딩 없이 요청 텍스트 벡터와 비교해 상위 6개를 표시한다. 현재 다나와 인덱스에는 품목 메타데이터가 없어 전체 인덱스를 검색하고 미검증 상태를 표시하며, 품목 일치나 검은색 조건 충족을 보증하지 않는다. 모델·차원·전처리가 다르면 중단하며 유효하지 않은 벡터는 제외한다. 시작 후 원본 이미지가 변경되면 새 입력을 거절한다.

인덱스가 없는 환경에서는 검수 완료·활성 템플릿을 별도 카탈로그로 복사한다. 다른 템플릿 목록은 `--templates`로 지정하거나 뷰어의 템플릿 등록을 사용한다. 추가 등록한 템플릿은 새 입력 실행부터 적용된다.

외부 호출 없는 동작 점검:

```powershell
.\.venv\Scripts\python.exe -m interior_recommender.web --legacy-generation --templates .\src\interior_recommender\samples\templates.json --output-root .\outputs\web-demo --port 8766 --demo
```

데모의 해석·질의 벡터·생성·비교는 고정/합성 응답이다. 다나와 인덱스가 있으면 이미지 벡터는 재사용하지만 질의가 합성이므로 검색 품질 검증에 사용하지 않는다. 실제 API 모드와 데모는 출력 폴더를 분리한다. 검색 소스 변경은 새 입력부터 적용되며 기존 실행은 생성 당시 카탈로그를 유지한다.

## 2. RoomPick 서버에서 같은 출처로 중계

브라우저가 RoomPick의 `/ai-mvp/`를 호출하고, RoomPick Django 서버가 AI 서버로 전달한다. 브라우저 JavaScript에는 Gemini·Cloudflare 키나 Bridge 토큰을 넣지 않는다. 별도 출처 CORS를 열 필요가 없다.

양쪽 서버의 비밀 환경 설정에 **동일한 무작위 공유값** `MVP_BRIDGE_TOKEN`을 넣는다. AI 서버는 `src/interior_recommender/.env`에서 읽는다. RoomPick 서버도 자신의 `.env`를 `load_dotenv`로 읽거나 기존 서버 비밀 설정으로 전달해야 한다. 이 값은 제공처 키와 다른 서버 간 인증 값이다. 로그·소스·브라우저에 노출하지 않는다.

RoomPick 서버 설정:

```dotenv
AI_MVP_UPSTREAM=http://AI서버주소:8765
MVP_BRIDGE_TOKEN=양쪽서버에_동일한_무작위값
```

같은 머신이면 upstream은 `http://127.0.0.1:8765`다. 다른 노드에서 접근할 때는 AI 서버에서 실제 접속받을 IP와 포트를 지정한다. LAN 바인딩은 Bridge 토큰이 설정되어 있어야 실행된다.

```powershell
.\.venv\Scripts\python.exe -m interior_recommender.web --legacy-generation --host 0.0.0.0 --port 8765 --templates .\src\interior_recommender\samples\templates.json
```

AI 서버의 직접 LAN 요청은 토큰 없이 거절한다. RoomPick 쪽 방화벽/포트 연결은 해당 환경에서 확인해야 한다. 이 서버는 로컬/LAN MVP용 개발 서버이며 운영 배포 구성은 아니다.

기존 RoomPick 프로젝트에 `roompick_bridge.py`를 복사하거나 이 패키지를 설치해 import하고, URLconf에 다음 두 경로를 추가한다.

```python
from django.urls import path
from interior_recommender.web.roompick_bridge import bridge

urlpatterns += [
    path('ai-mvp/', bridge),
    path('ai-mvp/<path:route>', bridge),
]
```

복사 방식이라면 실제 복사한 모듈 경로에서 `bridge`를 import한다. 중계 서버에 `httpx`가 필요하다. 기존 Django SessionMiddleware와 CsrfViewMiddleware를 유지하고 중계 view를 `csrf_exempt`로 만들지 않는다. 브라우저 CSRF는 RoomPick이 검증하며, 인증된 서버 간 요청만 AI 서버에서 검증된 owner 헤더로 처리한다. RoomPick 설정의 `DATA_UPLOAD_MAX_MEMORY_SIZE`도 35 MiB 이상으로 설정해 사진 요청을 중계할 수 있게 한다.

```python
DATA_UPLOAD_MAX_MEMORY_SIZE = 35 * 1024 * 1024
```

## 3. 기존 Canvas 검색 버튼 연결

`roompick-editor.example.mjs`의 import와 `searchProducts()` 예제를 기존 `editor.mjs`에 반영한다. 기존 두 검색 버튼의 이벤트 리스너는 유지할 수 있다. 버튼 문구는 `공간 생성하기`로 바꾸면 동작이 분명해진다. 새 JS 모듈 `roompick-client.mjs`는 Bridge를 통해 자동 제공된다.

`renderArtwork`를 원본 사진 크기의 별도 투명 Canvas에 호출하므로 화면의 선택 테두리·자르기 UI·커서는 전송되지 않는다. 모든 펜 획·텍스트·도형·채우기가 스케치 이미지에 들어간다. 브라우저는 원본 사진 Blob과 스케치 PNG를 분리해서 보낸다. 서버는 스케치 원본을 보존하고 API용 사본만 흰 배경으로 합성한다. 업로드된 WebP 사진도 원본을 유지하면서 API용 PNG로 변환한다.

Canvas의 텍스트 도형은 `document.items[].text`에서 추출한다. 별도 요구사항 입력란을 추가할 경우 `id="requirements"`의 textarea를 두면 연결 예제가 그 값도 전송한다. 기존 편집기 모듈의 지역 변수는 외부에서 읽을 수 없으므로 단순히 script 태그 하나만 추가하는 방식이 아니라 기존 모듈 안에 연결한다.

AI 실행 UUID를 기존 모의 검색의 `result_id`로 취급하지 않는다. 기존 수집기는 `collector.submit(null)`로 원문 저장 동작을 유지하고, AI 쪽에는 외부 project ID/revision/document를 별도로 보존한다. 실제 통합에서 두 서버의 기록 관계는 별도 필드/API 계약으로 연결해야 한다.

## 전송 계약

`POST /ai-mvp/api/runs`, multipart/form-data:

| 필드 | 내용 |
|---|---|
| photo | 방 사진 파일, 필수, PNG/JPEG/WebP 15 MiB 이하 |
| sketch | 사진과 동일 크기의 스케치 전용 PNG, 선택 |
| text | 텍스트 요구사항. 스케치와 모두 비어 있으면 거절 |
| document | 기존 `{strokes: [], items: []}` 원문 JSON. 최대 2 MiB |
| project | `{id, revision, width, height}` JSON |
| request_id | 같은 요청 재전송에 유지할 UUID |
| requested_region | 선택적 원본 사진 기준 0~1 `{x_min,y_min,x_max,y_max}` JSON |

사진 바이트가 필수다. 서버는 `photo_url`만 받아 외부 주소를 다운로드하지 않는다. 같은 request_id의 동일 입력은 기존 실행을 반환하고 다른 입력은 거절한다. 수정 입력은 새 request_id로 전송한다.

202 응답의 `run_id`를 `/ai-mvp/?run={run_id}`로 열면 비동기 상태·품목 확인·템플릿 후보·생성 요청 미리보기·생성/원본 비교 화면이 이어진다. 템플릿 선택은 새 입력 버전을 만들고, 생성 버튼을 누르기 전에는 Cloudflare를 호출하지 않는다. 비교 분석 재시도는 기존 생성 이미지를 사용한다.

| 후속 경로 | 용도 |
|---|---|
| GET api/runs/{id} | 상태·해석·템플릿 후보·미리보기·비교 결과 |
| POST api/runs/{id}/selection | `{template_id}` 또는 `{category_code}`로 새 입력 버전 생성 |
| POST api/runs/{id}/actions/generate | 검증된 미리보기로 생성 후 원본 비교 |
| POST api/runs/{id}/actions/compare | `{retry:true}`로 생성 없이 비교 재시도 |
| assets/{id}/{name} | 해당 브라우저/Bridge 소유 실행의 이미지 |

템플릿 벡터는 Gemini Embedding 2로 만들고 모델·차원·전처리·이미지 checksum별로 재사용한다. 검색용 텍스트 벡터로 같은 품목 안에서 유사도 정렬하며 점수를 정확도 확률로 표시하지 않는다. 색상/재질/형태의 명시 조건과 템플릿 메타데이터 차이를 함께 보여주고 사용자가 선택한다. 임의의 유사도 임계치로 다른 품목을 자동 채택하지 않는다.

현재 로컬 생성 범위는 요청 가구 한 개다. 서비스용 원천 보존 확인, 여러 물체 slot, 추천/노출, 품목·영역 수동 수정, MySQL/Kafka/Delta 연결은 추가 통합 범위다. 원본·결과 비교는 기존 유지/추가/교체/불확실을 구분하고 기존 유지·불확실 물체를 자동 검색하지 않는다.

## 참조 구조 분석과 결과 검증

템플릿 선택 뒤 `reference` 단계가 선택 이미지만 Gemini에 보내 `reference.json`을 만든다. 부품·지지 방식·부품 연결·보존 조건·불확실한 부분을 기록하고 불분명한 다리/바퀴 수는 `null`로 둔다. 동일 이미지 checksum·분석 모델·프롬프트 버전·스키마·데모 여부의 캐시를 재사용한다. 미리보기의 `reference_structure`와 화면의 구조 분석 펼침 영역에 이 내용을 표시한다.

단순 배치 네모는 가구 모양으로 전달하지 않는다. `plan.sketch_role`은 배치용/가구 형상/없음/불확실을 구분하고 `shape_evidence`는 텍스트/상세 스케치/없음을 기록한다. 형상 근거가 없거나 배치 스케치만 근거로 제시되면 `shape=null`로 검증한다.

생성 후 비교 API 입력은 원본 방·생성 결과·선택 참조 순서다. `comparison.reference_fidelity`에 `status=MATCH|MISMATCH|UNCERTAIN`, `differences`, `reason`을 저장한다. 구조 차이 또는 판단 불가이면 상태 `NEEDS_STRUCTURE_REVIEW`와 검토 안내를 표시하고 추가/교체 물체도 `eligible_for_selection=false`로 보류한다. 이는 모델 판정이며 구조 일치를 보장하지 않는다.

자동 재생성은 하지 않는다. 비교 재시도는 생성 이미지를 그대로 사용한다. 같은 참조로 새 요청 준비 버튼은 기존 selection API로 새 입력 버전을 만든 뒤 생성 요청을 미리 보여준다. 생성 버튼을 별도로 눌러야 새 Cloudflare 호출이 발생한다. 구조 분석 실패는 생성 전에 중단하고 비교 실패는 이미 생성한 이미지를 보존한다.

웹 프롬프트 버전은 `scene-placement-without-sketch-v3`다. 업데이트 후 사진·스케치로 새 입력을 제출한다. 이전 실행의 생성 요청을 조용히 새 프롬프트로 바꾸지 않는다. CLI/일괄 임베딩 버전과 기존 저장 벡터는 유지한다. 첫 선택 시 구조 분석 1회와 비교 입력 증가에 따른 Gemini 비용을 고려한다. 세부 테스트와 제한은 [MVP 안내](../README.md#canvas-연결-mvp와-참조-구조-보존-2026-10-15)에 둔다.

## 배치용 스케치와 치수선·문자 방지

Canvas의 전송 계약은 그대로다. 사진·스케치·문서·텍스트 원천을 모두 저장하고 Gemini가 스케치 역할과 배치 좌표를 해석한다. `plan.placement_region`은 원본 방 사진 기준 0~1 좌표이며 확실하지 않으면 `null`이다. `PLACEMENT_ONLY`·`NONE`·`UNCERTAIN` 스케치는 생성 API에 보내지 않는다. `FURNITURE_SHAPE`인 상세 스케치만 생성 참조에 남긴다. 미리보기의 이미지 수와 번호는 실제 전송 목록에 맞춘다.

생성 API에는 명시적 `requested_region` 또는 해석한 `placement_region`을 정사각 여백 사본 기준으로 변환해 전달한다. 명시적 영역이 우선이며 원천 좌표는 변경하지 않는다. `generation_request.json`의 `sketch_policy`에 전송 여부, `original_placement_region`/`api_requested_region`에 원본/사본 좌표, `placement_region_source`에 출처를 기록한다. 좌표가 없으면 배치 문장을 사용하며 임의 영역을 만들지 않는다.

비교 응답의 `annotation_check`는 `status=CLEAR|DETECTED|UNCERTAIN`, `findings`, `reason`이다. 새 치수선·라벨·정체불명 문자·스케치 흔적을 원본과 비교한다. 발견·판단 불가·검사 누락이면 `NEEDS_ANNOTATION_REVIEW`로 표시하고 기본 추천 선택을 보류한다. 주석 검토가 구조 검토보다 우선하며 두 판정은 모두 화면에 표시한다. 별도 API 호출이나 자동 재생성을 추가하지 않는다. 결과 이미지를 보존하고 기존 비교 재시도 또는 새 입력 준비/명시적 생성 흐름을 사용한다.


## 공간 크기 계획과 결과 검토 (2026-10-15)

`scene-context-scale-v4`는 선택 참조 구조 분석 뒤 원본 방·선택 참조 두 장으로 Gemini 크기 계획을 추가 호출한다. 별도 크기 입력란은 추가하지 않는다. 텍스트·스케치 라벨의 명시 크기 의도를 먼저 보존하고, 없으면 주변 물체의 불확실한 통상 치수 범위·배치 깊이·원근과 가구 유형으로 사진상 폭/높이 하한·상한을 추정한다. 스케치의 면적을 실제 치수나 고정 크기로 취급하지 않는다.

미리보기 `scale_plan`은 원본 좌표, `api_scale_bounds`는 정사각 여백 사본 기준이다. 결과 `comparison.scale_check`는 `APPROPRIATE / TOO_SMALL / TOO_LARGE / OUT_OF_RANGE / UNCERTAIN`과 `room_geometry`, 근거를 제공한다. 배경·카메라 유지가 확인된 단일 대상의 영역을 코드로 대조하며, 모델의 원근 부적정 판정도 통과시키지 않는다. LOW 근거·검사 누락·배경 변화·대상 불명은 판단 보류다.

구조 MATCH·주석 CLEAR·크기 APPROPRIATE가 모두 성립해야 기본 추천 선택이 가능하다. 크기 실패는 `NEEDS_SCALE_REVIEW`이며 주석·구조 실패가 함께 있으면 그 상태가 우선한다. 뷰어는 모든 검사 결과를 표시한다. 생성 이미지를 유지하고 비교만 재시도하거나 같은 참조의 새 요청을 준비한 뒤 명시적으로 생성한다. 자동 재생성은 없다.

서버 재시작 후 새 Canvas 요청으로 적용한다. 기존 실행의 계획·미리보기를 재작성하지 않는다. 이전 결과에 고정 계획이 없으면 비교 재시도에서도 크기는 미확인이다. 크기 분석 실패는 준비 재시도로 복구하며 추가 Gemini 호출 비용이 생긴다. CLI·일괄 임베딩·원격 RoomPick 배포는 이번 변경에 포함하지 않는다. 상세 저장·검증 경계는 [MVP 안내](../README.md)와 [서비스 정책](../../../docs/design/11-scale-policy.md)을 따른다.

</details>
