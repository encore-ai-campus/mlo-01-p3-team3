# Application과 외부 AI: 입력 맥락 기반 추천

2026-10-18 선택 생성 추가: [상품 배치 미리보기](../interior-recommender/product-preview.md)는 추천 상품 선택 후 `application.preview`가 저장된 방·상품·스케치를 확인하고 코드로 프롬프트/좌표를 조립해 이미지 API만 호출한다. Cloudflare/Gemini native async·별도 생성 한도를 사용하며 해석/질의 임베딩/생성물 분석은 추가 호출하지 않는다. 성공 추천과 생성 상태/실패는 분리한다.

2026-10-18 로컬 구현: [방 특징 추천](../interior-recommender/interior-ranking.md)의 `direct-context-v3`는 palette/materials/shapes/styles와 관찰 근거·불확실성을 별도 수신한다. `interior-content-v3`는 이를 검색 질의와 소프트 가점에 반영한다. API 호출은 기존 해석/질의 임베딩 각 1회이며 사용자 필수조건·검증된 상품 사실·추정 공간 치수와 분리한다. 현재 MySQL 연결은 후속 범위다.

2026-10-18 로컬 구현 추가: SketchOn은 SQLite `recommendations` 작업 원장과 별도 워커·native async Gemini·제한된 공유 풀을 사용한다. [실행/상태/복구/스키마와 후속 연결](../interior-recommender/execution.md)을 따른다. 아래 MySQL·Kafka·Bronze·광고 서버의 운영 계약은 후속 통합 범위이며 이번 migration이 해당 구축 완료를 뜻하지 않는다.

2026-10-15 현행 설계. 상세 책임·응답 구획·코드 검증 예시는 [직접 추천 계약](12-direct-recommendation.md)을 따른다. 직접 추천 로컬 웹 경로를 구현했으며 서비스 DB/API v2 전환은 별도다.

## AI와 해석 이후 코드의 책임

멀티모달 API는 사진·텍스트·스케치의 의미·공간 관계·배치·크기 근거를 해석한다. 코드는 requirements/room_context/placement/size_estimate의 형식·수치·좌표·참조 존재를 검증하고 근거 유형에 따라 크기 필터/점수 사용을 제한한다. 코드 검증을 시각적 사실 검증으로 표현하지 않는다. 상품 규격·가격·URL은 확인된 원장에서만 조회한다.

이미지 생성 API, 참조 템플릿 분석, 생성 후 비교 API를 신규 추천에 호출하지 않는다. 임베딩은 외부 API를 유지하고 광고 서버가 검색 질의를 조립한다. 모델 식별자·계정 접근·지원 입력/차원·전송 범위는 구현 전에 확인하며 이름 선택을 실제 호출 완료로 기록하지 않는다.

## 요청과 상태

POST /api/projects/{id}/recommendations로 input_revision_id 기반 run을 접수하고 기존 작업 상태/결과 조회와 이벤트/링크 API를 유지한다. 신규 요청에서 generation_run_id·scene_object_id·template_id·생성 크롭은 요구하지 않는다. v2 계약과 신규 FK는 별도 마이그레이션이다.

RECEIVED→BRONZE_PENDING→INTERPRETING→VALIDATING→SEARCHING→COMPLETED 또는 NO_RESULTS를 상태 후보로 둔다. 모호한 품목·형식 오류·시스템 오류는 구분하고 크기 UNKNOWN만으로 전체 추천을 실패시키지 않는다. 같은 ID 재전달·고정 입력·성공 응답 재사용·늦은 run 보호를 적용한다.

## 비동기 I/O·스레드풀·동기 트랜잭션 처리 경계

웹과 작업 Consumer는 분리한다. 외부 해석/임베딩·저장소·내부 HTTP는 비동기 I/O, 이미지/파일·동기 SDK는 제한된 실행 영역, DB 변경은 짧은 트랜잭션을 사용한다. API 대기 중 잠금을 유지하지 않고 작업별 재시도·예산·동시성 상한을 둔다. 단순 async 선언이나 대기열 추가로 CPU 처리량이 늘어난다고 설명하지 않는다.

## 이번 운영 추천과 후속 고도화

초기 추천은 라벨 없이 scikit-learn `cosine_similarity`와 수동 초기 가중치로 수행한다. `weighted-content-v1`은 선호 색상이 있으면 `0.85 × 유사도 + 0.15 × 색상 일치`, 없으면 유사도만 사용한다. 필수 조건 불일치는 제외하고 결측·미지원 필수 조건은 UNKNOWN 목록으로 분리한다. 가중치는 학습값이 아니며 점수는 확률이 아니다. 사람의 6만 이미지 전수 적합/부적합 라벨링·LogisticRegression 학습·직렬화 모델 파일은 초기 실행 조건이 아니다. 반응 자료를 충분히 확보한 뒤 지도학습을 별도 평가한다.

로컬 구현은 `src/interior_recommender/recommendation.py`와 `src/interior_recommender/web/direct.py`다. `python -m interior_recommender.web` 기본 경로가 직접 추천이며 기존 Canvas의 `POST /api/runs`를 재사용한다. 해석 1회와 질의 임베딩 1회를 기본으로 하며 성공 receipt를 재사용한다. 별도 입력 절차·템플릿 선택·이미지 생성은 없다. 실제 DB·Kafka·Delta Bronze·광고 서버 HTTP·FAISS·상품 링크 검증 API 통합 및 운영 배포는 아직 미구현이다. 논리 서비스 계약을 로컬 파일 구현 완료와 혼동하지 않는다.

<details>
<summary>이전 설계 이력 — 이미지 생성 중심 흐름과 당시 계약</summary>

# Django와 AI 연결

Django는 MySQL을 사용해 사용자 서비스를 제공한다. AI는 외부 멀티모달 API로 연결하고 전용 AI 서버는 추가하지 않는다. Application 서버(app-01)의 Kafka Consumer 프로세스가 API를 호출한다. 3차 흐름은 입력 해석·템플릿 기반 공간 구성·결과 물체 분석과 유사 상품 추천이다. 광고 서버는 별도 ad-01 후보 VM에서 검색·추천과 상품 페이지 연결 결정·집계 조회를 담당한다.

[문서 안내](index.md) · [데이터 모델](03-data-model.md) · [Kafka 처리](05-kafka-jobs.md)

## AI와 해석 이후 코드의 책임

2026-10-15 반영. 사용자가 별도 품목·영역 입력 절차를 수행하도록 바꾸지 않는다. 외부 AI는 사진·스케치·텍스트의 의미 해석, 물체·영역·참조 구조 관찰, 공간 크기 계획, 생성 전후 시각 비교와 임베딩·이미지 생성을 담당한다. Application/광고 서버 코드는 검증된 해석 결과의 계산·검색·조립·판정을 담당한다. 임베딩을 일반 코드로 생성하거나 자유로운 시각 이해를 규칙으로 대체한 것이 아니다.

| 처리 | 소유 책임 |
|---|---|
| 검색 문장 | 해석된 품목·색상·재질·근거 있는 형태·추가 의미 조건을 코드로 조립. 추가 조건은 `search_terms` 구절로 받고 부정 조건을 유지한다. 배치 좌표·주석을 검색어로 복사하지 않는다. |
| 후보·순위 | 승인·품목·현재 상태 검증, 정규화 벡터 유사도·동점 규칙·카드 구성은 코드. 미제공 속성을 조건 불일치로 확정하거나 가격·URL을 추측하지 않는다. |
| 생성 요청 | 좌표 범위·양의 영역 검증, 원본→여백 사본 변환, 참조 역할·순서·고정 프롬프트 조립은 코드. |
| 결과 판정 | AI 시각 근거에 고정 수치 크기 범위와 품목·추가/교체·구조·주석·크기 통과 조건을 적용한다. 불확실·누락을 정상으로 승격하지 않는다. |

로컬 웹 MVP는 `scene-context-local-v5`와 `semantic-results-v1`을 구현한다. `plan.json.search_text`는 코드가 만들고 AI 응답 스키마에는 없다. AI는 별도 의미 해석 구절 `search_terms`를 반환한다. 원응답은 receipt, 코드 가공 결과는 단계 JSON으로 구분하고 검색 결과·생성 미리보기·비교 판정에 로컬 처리 버전을 남긴다. 비교 출력에는 원래 시각 크기 판정과 최종 로컬 판정·검사 통과 여부를 함께 남긴다. 서비스 DB·Kafka·FAISS 통합은 아직 구현하지 않았다.

CLI/일괄 임베딩 계약은 유지한다. 이전 웹 실행의 완료 출력은 재작성하지 않고 새 준비·생성은 새 요청에서 진행한다. v4 비교 재시도는 기존 고정 크기 계획을 사용하며 더 이전의 계획 없는 결과는 크기를 보류한다. 로컬 계산에 새로운 과금 분석 행을 만들지 않는다. 이미 코드였던 처리의 공통화도 포함하므로 호출 횟수·비용 절감 또는 동시 사용자 처리량 개선을 보장하지 않는다.

## Application 책임

사용자 인증 또는 세션 관리, 사진과 스케치 등록, 상품 비교, 추천 이력, 템플릿 기반 공간 구성, 물체 선택, 광고 서버가 제공하는 상품 카드·링크의 표시, 운영 화면을 Django로 구현한다. 웹 Canvas의 스케치 레이어는 사진과 분리한다. React 등 프런트엔드 선택은 미확정이다.

Django 웹 프로세스와 작업 프로세스를 분리하고 Django 개발 서버를 운영 용도로 사용하지 않는다. MySQL을 Django의 기본 DB로 사용하며 MongoDB는 수집·재처리 경로에서 접근한다. Django ORM을 MongoDB로 교체하는 앞선 안은 사용하지 않는다.

사용자 모델은 첫 마이그레이션 전에 UUID PK를 선언한 커스텀 모델로 구성하고 AUTH_USER_MODEL을 설정한다. 프로젝트 엔터티의 id는 default=uuid.uuid4로 자동 생성한다. assets·projects·jobs의 별도 public_id를 두지 않으며 API의 {id}와 반환 job_id는 실제 UUID PK다. PK와 FK의 저장·직렬화 및 재처리 규칙은 [데이터 모델](03-data-model.md#uuid-생성과-보존-규칙)을 따른다.

Django·AI Consumer·작업 복구 프로세스는 초기에는 공통 환경 설정의 현재 MySQL Primary에서 읽고 쓴다. Replica로 임의 전환하거나 읽기를 분산하지 않는다. 수동 장애 전환 후 모든 프로세스의 접속 설정·연결 풀을 갱신한다. DB 응답 유실은 기존 job·event_id·업무 키로 완료 여부를 대조하고 재시도하며 저장된 AI 결과는 재사용한다. 복제 유실 가능성이 있는 외부 호출·Kafka 발행은 [DB 장애 복구](06-operations-migration.md#db-복제와-장애-복구)에 따라 별도 대조한다.

## API 초안

| 작업 | 경로 후보 |
|---|---|
| 프로젝트 생성 | POST /api/projects |
| 사진 등록과 업로드 시작 | POST /api/projects/{id}/assets |
| 스케치 저장 | POST /api/projects/{id}/sketch-revisions |
| 추천 요청 | POST /api/projects/{id}/recommendations |
| 작업 상태 | GET /api/jobs/{id} |
| 추천 결과 | GET /api/recommendations/{id} |
| 상품과 판매처 비교 | GET /api/products/{id} |
| 입력·텍스트·이용 데이터 버전 접수 | POST /api/projects/{id}/input-revisions |
| 공간 구성 요청과 명시적 다시 생성 | POST /api/projects/{id}/scene-generations |
| 품목·템플릿 후보 선택 | POST /api/scene-generations/{id}/selections |
| 결과 물체의 품목·영역 수정 | POST /api/scene-objects/{id}/revisions |
| 선택 물체 상품 추천 | POST /api/scene-objects/{id}/recommendations |
| 선택과 거절 기록 | POST /api/events |
| 추천 상품 페이지 연결 | POST /api/recommendation-items/{id}/link |

입력과 선택 API는 event_id를 받으며 동일 이벤트를 재전송하면 기존 버전·실행을 반환한다. 명시적 다시 생성은 새 event_id와 EXPLICIT_REGENERATE로 접수한다. 물체 수정은 이전 ID를 덮어쓰지 않고 새 ID를 반환한다.

API 계약은 구현 전에 소유자 권한, UUID id 직렬화, 페이지 구분, 입력 상한, CSRF와 인증 방식을 포함해 확정한다. 상품 링크는 선택 추천 항목의 권한을 확인한 뒤 광고 서버의 resolve-link API로 결정한다. 클릭 이벤트를 기록한 후 검증 주소를 반환하며 사용자 웹이 그 주소로 이동한다. 임의 URL을 전달받아 이동시키지 않는다.

## 사용자 추천 흐름

1. 사진·선택적 스케치·텍스트를 원천 보존하고 작업을 등록한다. 사용자 입력의 Bronze 보존 확인 후 AI 해석을 진행하며 실패 입력도 유지한다.
2. Consumer가 이미지·텍스트를 외부 멀티모달 API에 전달해 품목·형태·조건을 얻는다. 미지정 속성은 추측으로 확정하지 않는다.
3. 서버가 분석 조건으로 템플릿을 선택하고 Object Storage에서 읽어 사진·스케치 위치와 함께 이미지 생성 API에 전달한다.
4. scene_generation_runs에 입력·사용 템플릿·생성 결과를 연결해 저장한다. 외부 AI가 원본 사진과 생성 결과를 요청·템플릿 근거와 함께 비교해 물체의 품목·영역 및 기존 유지·추가·교체·구분 불확실을 분석한다. 추가·교체 물체를 기본 추천 대상으로 제시하고 불확실 물체는 사용자 확인 전 검색하지 않는다. 선택·수정한 물체를 scene_objects로 확정한다.
5. Application은 선택 물체의 추가·교체 구분 또는 사용자 확인 근거를 검증한 뒤 recommendation_runs와 추천 검색 jobs를 생성한다. Consumer가 광고 서버 내부 API에 선택 물체·영역 이미지·품목·조건을 전달한다. 기존 유지 및 확인 전 불확실 물체는 검색 요청을 거절한다.
6. 광고 서버가 임베딩 API와 준비된 상품 인덱스로 유사 상품을 검색한다. 광고 서버가 추천 상품과 링크·카드 정보를 결정하고 Application이 추천 항목·검색 버전·선택적 가격 snapshot·link_snapshot_json을 같은 run에 확정한다.
7. 화면에서 실제 표시된 상품의 노출 이벤트를 보내고 선택·명시적 거절·다나와 연결 클릭을 기록한다. 이동 대상 검증은 광고 서버에 요청한다. 새 추천 요청은 새 run이며 생성 결과는 유지한다.

Application은 소유자 권한과 요청 맥락을 검증한다. 템플릿 선택·공간 생성은 Application, 실제 상품 검색·추천 순위·상품 페이지 연결 결정·집계 조회는 광고 서버의 책임이다. 내부 API·이벤트와 장애 규칙은 [광고 서버](07-ad-server.md)를 따른다.

생성 입력 변경마다 새 input_revisions와 생성 실행을 만들고, 재전송은 같은 event_id로 기존 실행을 반환한다. 후보 선택도 새 입력 버전으로 접수한다. requested_regions_json은 원본 사진의 배치 의도이고 scene_objects의 영역은 생성 결과 기준이다. 최신 실행만 현재 화면을 갱신하며 완료된 이전 결과와 실패 원인은 이력으로 보존한다.

## AI 처리 경계

| 처리 | 입력 | 출력과 용도 |
|---|---|---|
| 스케치 카테고리 인식 | 스케치 레이어와 지원 카테고리 목록 | 확정 품목 또는 후보 품목 목록, 미지정 특징 |
| 공간 구성 | 사진·스케치 위치·선택 템플릿 | 템플릿을 참조한 결과 이미지 |
| 결과 물체 분석 | 원본 사진·구성 결과 이미지, 요청 조건·사용 템플릿·목표 영역 | 물체별 품목·영역, 기존 유지·추가·교체·구분 불확실과 판단 근거, 수정 후보 |
| 상품 이미지 검증 | 상품 이미지와 제공처 정보 | 카테고리 후보, 불일치, 검수 사유 |

상품 이미지 분석은 핵심 흐름 이후 보조 기능으로 연결한다. 색상·재질 등 해석을 확장하면 추정값으로 구분하고 3차 성공 조건에 추가하지 않는다.

AI API Consumer는 Django와 같은 Application 서버(app-01)에서 실행하되 웹 요청 프로세스와 분리한다. 같은 코드베이스의 Django 관리 명령으로 구현하고 별도 컨테이너나 systemd 서비스로 관리할 수 있다. 웹 서버 시작 시 Consumer를 함께 실행하지 않는다. 이 프로세스는 모델을 호스팅하지 않으며 실제 추론은 외부 API 제공처에서 수행한다. 모델 가중치와 GPU 및 AI 전용 VM은 배치하지 않는다.

```text
Application 서버(app-01)
  ├─ Django 웹 프로세스 → Kafka
  ├─ 작업 복구 프로세스 → Kafka
  └─ AI API Consumer ← Kafka
       ├─ 이미지 읽기와 API 요청 준비
       ├─ 외부 멀티모달 API 호출
       └─ 응답 검증과 MySQL 결과 저장
```

Consumer의 동시 호출·이미지 크기·재시도·시간 초과·호출 예산을 제한해 웹 프로세스와의 자원 경쟁을 관리한다. API 키는 환경별 비밀 설정으로 전달하고 저장소나 Kafka 메시지에 넣지 않는다. 제공처와 모델은 평가 후 선택하고 교체할 수 있도록 호출 코드를 어댑터로 분리한다. API 방식 결정이 특정 모델 채택을 의미하지는 않는다.

워커가 Object Storage (이미지용)에서 이미지를 가져와 모델의 지원 형식에 맞춰 바이트, 업로드 파일 참조, 또는 외부에서 접근 가능한 짧은 유효기간의 URL로 전달한다. 내부 전용 주소나 object_key만으로 외부 모델이 이미지를 읽을 수 있다고 가정하지 않는다.

## 비동기 I/O·스레드풀·동기 트랜잭션 처리 경계

2026년 10월 14일 결정 기준으로 접수된 사용자 작업의 내부 처리에 비동기 I/O와 제한된 스레드풀을 사용한다. 현재 범위는 외부 응답 대기를 겹쳐 처리하고 이벤트 루프의 정체를 줄이는 것이다. HTTP 트래픽 분산, 웹 요청 워커 증설, 서버 추가 및 접속 수에 따른 확장 설계는 이번 범위에서 다루지 않는다. 아래 내용은 구현·성능 시험 완료 기록이 아니다.

웹은 입력과 작업을 저장하고 job_id를 반환한다. 외부 AI 호출은 웹 요청과 수명을 분리한 작업 프로세스에서 실행한다. 외부 통신은 비동기 HTTP 또는 SDK를 우선 사용하며, 동기 전용 SDK는 제한된 스레드풀로 호출한다. 별도 스레드에서 동기 함수를 실행하는 방식과 비동기 네트워크 호출은 구분한다. async def 선언만으로 동기 HTTP·파일·계산 작업이 비차단 처리로 바뀌지는 않는다.

| 구간·작업 | 실행 방식 | 완료 기준·경계 |
|---|---|---|
| 작은 입력 형식·길이·좌표·지원 품목 검사, 짧은 결과 조립 | 현재 실행 흐름에서 즉시 처리 | 큰 파일 디코딩·대량 순회와 구분하며 작은 연산마다 스레드 전환하지 않음 |
| 사진·스케치·템플릿·결과의 Object Storage 업로드·다운로드 | 비동기 I/O, 동기 전용 SDK는 스레드풀 | 파일 저장·checksum·접근 가능성 확인 후 DB 참조 등록 |
| 이미지 디코딩·형식/해상도 검사·EXIF 보정·RGB 변환·리사이즈·패딩·크롭·인코딩, 큰 파일 checksum | 제한된 스레드풀의 동기 함수 | 입력·결과 바이트와 버전을 고정하고 완료된 바이트·메타데이터만 다음 단계에 전달 |
| 로컬 이미지·AI 응답·실행 manifest 읽기와 쓰기, fsync·파일 교체 | 제한된 스레드풀의 동기 함수 | 같은 실행의 파일 갱신을 직렬화하고 원자적 파일 교체·저장 결과 확인 |
| 입력 해석·공간 생성·물체 분석·템플릿 임베딩 API | 비동기 I/O | 요청별 timeout, 제공처·모델별 동시 호출·호출률·예산 제한 |
| 광고 서버 추천·링크 검증·운영 집계 내부 HTTP | 비동기 I/O | Application의 권한 검증과 광고 서버의 판단 책임 유지 |
| 입력 버전·생성 실행·단계 작업 접수 | 짧은 동기 DB 트랜잭션 | event_id 중복·본문 일치 확인 후 관련 행을 함께 저장 |
| Bronze 완료 확인·작업 시작·시도 등록 | 짧은 동기 DB 트랜잭션 | 보존 참조 대조, 단계 job 고유성·lease·attempt_count 원자적 갱신 |
| 생성 결과·분석 물체·추천 항목·단계 완료·다음 단계 job 저장 | 짧은 동기 DB 트랜잭션 | 결과 참조와 상태를 함께 확정하고 lease_token 검증, 최신 결과는 조건부 갱신 |
| 노출·선택·거절·클릭·관심 카테고리 갱신 | 짧은 동기 DB 트랜잭션 | 소유자·추천 항목 관계·중복 키 확인. 링크 검증 HTTP는 트랜잭션 밖에서 실행 |
| 실패·재시도 시각·예산 대기 저장, 만료 lease 회수 | 짧은 동기 DB 트랜잭션 | 실행 상태를 저장한 후 실행 슬롯 반환, 복구는 기존 작업·이벤트 ID 유지 |
| 상태·결과·상품 등 단순 조회 | 비동기 ORM 또는 짧은 동기 조회 함수 | 여러 변경을 묶는 경우와 달리 조회 전체에 쓰기 트랜잭션을 일괄 적용하지 않음 |

Object Storage와 DB는 하나의 트랜잭션이 아니다. DB 등록 전에 파일 저장을 확인하고, DB 실패로 생긴 미참조 파일은 보관·정리 정책을 적용한다. 외부 API 대기·파일 처리·스레드풀 대기를 DB 트랜잭션 안에 넣지 않는다. 사용자 입력·물체 크롭을 포함한 임베딩과 FAISS 검색의 광고 서버 실행 경계는 [광고 서버](07-ad-server.md#비동기-호출과-동기-검색-처리)를 따른다.

### 작업 프로세스 실행 구조

```text
Application(app-01)
  Django 웹 프로세스: 입력·작업 접수, 권한 확인, 상태·결과 조회
  별도 작업 프로세스
    ├─ asyncio: AI·저장소·내부 HTTP의 비동기 I/O
    ├─ 제한된 스레드풀: 이미지·파일·동기 전용 SDK
    ├─ 동기 DB 함수: 짧은 트랜잭션과 연결 관리
    └─ Kafka 수신·완료 관리, 재시도·미진행 작업 복구
```

HTTP 클라이언트는 작업 프로세스의 이벤트 루프 수명에 맞춰 재사용하고 종료 시 닫는다. 요청마다 이벤트 루프나 스레드풀을 새로 만들지 않는다. 동기 ORM은 thread_sensitive 호환성을 지키는 어댑터로 호출하고 연결·커서를 다른 스레드에 전달하지 않는다. Channels를 사용하는 구간은 database_sync_to_async로 연결 정리를 포함할 수 있다. 일반 작업 프로세스도 스레드별 DB 연결 정리 책임을 명시한다. DB 어댑터는 임의의 다중 스레드 DB 병렬화 수단으로 사용하지 않는다.

같은 job·실행의 변경 가능한 상태와 파일 갱신은 실행권으로 보호하며 작업별 객체를 분리한다. 스레드 작업의 await가 취소되거나 시간 초과돼도 이미 실행 중인 동기 함수가 즉시 중단된다고 가정하지 않는다. 실제 완료·실행권 상태를 대조한 뒤 재처리하고 늦은 결과 저장은 lease_token으로 차단한다. 이미지 처리의 멀티코어 속도 향상은 라이브러리와 GIL 해제 여부에 따라 검증하며 CPU 병목이 확인되면 별도 프로세스 실행을 검토한다.

### 순차 의존과 내부 동시 실행

한 실행은 Bronze 확인 → 입력 해석 → 템플릿 선택 → 생성 → 물체 분석 순서를 유지한다. 각 호출을 await해도 기다리는 동안 다른 독립 작업은 진행할 수 있다. 서로 다른 실행, 독립적인 참조 파일 준비, 사용자 확인을 마친 여러 물체 검색만 제한된 동시 실행 대상으로 삼는다. 같은 실행의 의존 단계를 동시에 시작하거나 추천 실패로 생성 API를 반복 호출하지 않는다.

```text
실행권 확보 [동기 트랜잭션 종료]
  → 파일 준비·전처리 [비동기 저장소 I/O + 스레드풀]
  → 외부 API [비동기 I/O]
  → 응답 검증·파일 저장 [스레드풀 + 비동기 저장소 I/O]
  → 결과·단계 완료·다음 작업 등록 [동기 트랜잭션 종료]
  → 다음 이벤트 발행 [Kafka]
```

재시도·예산 대기는 상태와 next_retry_at을 저장하고 실행 슬롯을 반환한다. API 호출 중 대기와 재시도 대기를 구분하며 긴 sleep으로 스레드·호출 슬롯을 점유하지 않는다. 동시 실행·취소·offset commit·재발행 계약은 [Kafka](05-kafka-jobs.md#작업-내부-동시-실행과-offset-관리), 측정과 상한 결정은 [운영 검증](06-operations-migration.md#작업-내부-동시-처리-검증)을 따른다.

[Django 비동기·트랜잭션](https://docs.djangoproject.com/en/5.2/topics/async/) · [Channels DB 접근](https://channels.readthedocs.io/en/stable/topics/databases.html) · [Python 스레드풀](https://docs.python.org/3/library/concurrent.futures.html) · [Python asyncio](https://docs.python.org/3/library/asyncio-task.html)

## AI 모델 후보

2026년 10월 14일 기준 이미지 생성은 Cloudflare Workers AI의 FLUX.2 klein 4B로 변경하고 테스트 출력은 512×512로 선택한다. 출력 해상도는 설정으로 관리하며 이후 새 실행에서 1024×1024를 사용할 수 있다. 임베딩 gemini-embedding-2는 유지하고 해석·물체 분석 gemini-3.5-flash-lite는 초기 추천 상태다. 해석·생성·임베딩은 별도 API 호출이다.

| 역할 | 모델 | 상태 |
|---|---|---|
| 템플릿 제작·반복 공간 생성 | @cf/black-forest-labs/flux-2-klein-4b | 사용자 선택, 테스트 512×512 |
| 상품·템플릿·물체 영역 벡터 | gemini-embedding-2 | 선택 유지, 계정 model_id·호출 확인 필요 |
| 입력 해석·물체 분석 | gemini-3.5-flash-lite | 초기 추천 |
| 유료 생성 대안 | gemini-3.1-flash-lite-image, gemini-nano-banana-2.1 | 참고 후보, 자동 전환하지 않음 |

[FLUX.2 klein 4B](https://developers.cloudflare.com/workers-ai/models/flux-2-klein-4b/)는 이미지 생성·편집에 사용하고, [Flash-Lite](https://ai.google.dev/gemini-api/docs/models/gemini-3.5-flash-lite)는 이미지 입력 해석과 JSON 출력, [Gemini Embedding 2](https://ai.google.dev/gemini-api/docs/embeddings)는 검색 벡터를 담당한다. 초기 생성은 Cloudflare 무료 할당량을 사용하고 Google 해석·임베딩의 예산은 별도로 관리한다.

### 생성 API와 해상도

Consumer는 Cloudflare account_id와 API token을 환경별 비밀 설정에서 읽고 REST API에 multipart/form-data로 prompt·참조 이미지 바이트·width·height를 전달한다. 테스트 기본 설정은 width=512, height=512다. 설정을 1024×1024로 바꾸면 새 실행에만 적용하며 기존 결과·실행은 유지한다. 참조 이미지 최대 4장과 전처리·파일 저장·반환 이미지 검증 계약은 [생성 요청](08-template-generation.md#생성-api와-해상도-계약)을 따른다.

### 비용과 모델 변경

Cloudflare Workers AI는 계정의 하루 총 10,000 Neurons 무료 할당량을 공유한다. Neurons는 벡터 차원이나 토큰 수가 아닌 제공처 연산량 단위다. 생성·템플릿 준비·재생성과 다른 Workers AI 사용량을 함께 확인한다. Free 플랜의 한도 초과는 BUDGET_WAIT로 대기하며 유료 모델이나 플랜으로 자동 전환하지 않는다. 한도는 UTC 00:00(한국 09:00)에 초기화한다. 실제 무료 제공·요금은 [Cloudflare 요금표](https://developers.cloudflare.com/workers-ai/platform/pricing/)를 확인한다.

생성 입력 변경의 정상 재생성과 실패 재시도 호출 수·사용량·추정 비용을 구분한다. Google 호출은 [Gemini 요금표](https://ai.google.dev/gemini-api/docs/pricing)를 별도로 적용한다. 무료 한도의 장수 추정은 참조 크기와 출력 해상도에 따른 계산값이며 [생성 무료 한도](08-template-generation.md#생성-무료-한도와-용량-추정)를 따른다. BRD의 입력 의도 반영·물체 영역·수정/재시도 시나리오로 품질을 검증한다. 사용자 결과 수정·명시적 재생성·기술 오류 복구는 유지한다.

## 입력과 출력 및 품질

```json
{
  "decision": "ACCEPTED",
  "category_code": "CHAIR",
  "reason_code": "SUPPORTED_CATEGORY"
}
```

카테고리는 지원 집합으로 제한한다. 불명확하면 후보 품목을 보여주고 선택받으며 원하는 품목이 후보에 없으면 스케치·텍스트 수정을 안내한다. 선택 대기 중 생성하지 않는다. 구조화 출력 형식의 유효성과 내용의 정확성은 별개다. 모델 자기보고 confidence를 확률이나 정답 보장으로 쓰지 않고, 채택 조건과 기각 조건을 평가 자료로 검증한다.

모델과 버전, prompt_version, 입력 hash, 출력 schema_version, 응답, 지연, 오류를 ai_analysis_runs에 기록한다. 가격·실측 규격·재고·구매 URL은 수집 데이터로 확정하고 모델이 만들어내게 하지 않는다. 이미지로 제품 동일성이나 설치 가능성을 보증하지 않는다.

웹·내부 API, 입력 파일 준비·전처리, 외부 AI 호출, 응답 검증·결과 저장 등 로그에 남기는 모든 작업은 [공통 소요시간 기록 계약](06-operations-migration.md#모든-작업-로그의-소요시간-기록-계약)에 따라 duration_ms를 기록한다. AI 호출마다 ai_analysis_runs.latency_ms와 호출 종료 로그에 같은 측정값을 남기며 성공·실패·시간 초과 모두 포함한다. 외부 호출 구간과 상위 Consumer 시도 전체 구간을 분리하고 재시도 대기시간을 호출 지연에 포함하지 않는다.

상품 분석 재사용 키는 input_hash, 모델, prompt_version, schema_version이다. 스케치는 revision과 입력 hash로 재사용 여부를 결정한다. 변동 가격과 재고까지 AI 캐시에 고정하지 않는다. 이미지 속 문구와 상품 설명은 분석 대상 데이터로 취급하고 실행 지시로 받아들이지 않는다.

## 템플릿 기반 공간 구성

대분류·소분류별 템플릿을 저장소에서 읽어 생성 API에 전달한다. 저장된 다나와 상품 원본 이미지는 검색·상품 비교에 사용하며 공간 생성 재료로 쓰지 않는다. 도매꾹 이미지는 별도 템플릿 제작 참고 자료로 구분한다. 내부 주소나 object_key만으로 외부 AI가 파일을 읽는다고 가정하지 않는다. Consumer가 이미지 바이트 또는 지원되는 업로드 참조로 전달한다. 템플릿 제작·검수·임베딩과 입력·사용 버전은 [템플릿과 생성](08-template-generation.md)을 따른다. placements의 기존 실제 상품 배치안을 신규 흐름에 적용하지 않는다.

원본·결과의 대응, 기본 추천 대상, 사용자 확인 및 분석 실패 복구는 [원본·결과 비교 계약](08-template-generation.md#원본결과-비교와-추천-대상-구분)을 따른다. 사용자 구분 확인만으로 공간 생성 작업을 다시 만들지 않는다.

## MCP와 Celery

이미지 해석에 MCP는 필수가 아니다. 고정된 모델 호출은 워커에서 직접 수행한다. 4차의 자연어 재탐색에서 모델이 search_products나 get_product 도구를 선택하게 될 때 MCP를 검토한다. 초기 비동기는 Kafka Consumer와 MySQL 작업 관리이므로 Celery와 작업 큐용 Redis를 추가하지 않는다.

## 행동 기록과 관심 카테고리

PRODUCT_IMPRESSION, TEXT_UPDATED, SCENE_GENERATION_REQUESTED, SCENE_OBJECT_SELECTED, PHOTO_UPLOADED, SKETCH_STARTED, SKETCH_MOVED, SKETCH_COMPLETED, RECOMMENDATION_REQUESTED, PRODUCT_SELECTED, PRODUCT_REJECTED, REDRAW_REQUESTED, RERECOMMENDATION_REQUESTED, PURCHASE_LINK_CLICKED를 기록한다. SKETCH_MOVED는 이동마다 요청을 보내기보다 좌표와 시점을 묶어 전송한다. 상품 선택 시 관심 카테고리를 갱신한다.

## 평가

지원 품목과 평가 자료를 정하고 입력 의도 반영·품목/영역 일치·시각적 추천 유사성·유효 판매 링크를 측정한다. UNKNOWN·수정·실패를 정상 결과와 구분하고 텍스트만 입력한 흐름도 검증한다. 실제 노출·반응 집계 검증은 [광고 서버](07-ad-server.md#장애와-검증)를 따른다.

생성·임베딩 모델 선택은 위 표를 따르고 해석 모델 최종 채택·예산·전송 데이터 보관 기간·처리 지연 상한은 운영 설정에서 확정한다. 결정 결과는 [운영 문서](06-operations-migration.md)에 반영한다.

[이미지 입력 API 예시](https://ai.google.dev/gemini-api/docs/image-understanding) · [MCP 이미지 결과](https://py.sdk.modelcontextprotocol.io/servers/media/)

## 다나와 상품·판매 링크 응답

광고 서버의 추천 응답은 내부 product_id·실제 source_product_id와 다나와 연결 주소·link_snapshot_json을 구분한다. 확인된 Offer가 있을 때만 판매처·일반가격·가격/배송 조건·조회 시각을 함께 제공한다. 미제공 값은 NULL/상태로 반환한다. 클릭 시 광고 서버가 연결 대상을 다시 검증하고 미확정·만료 주소는 연결 실패로 안내한다. arbitrary next/URL을 입력받아 redirect하지 않는다.

비교 이동은 COMPARISON_LINK_CLICKED, 쇼핑몰 구매 이동은 PURCHASE_LINK_CLICKED다. 둘 다 구매 완료가 아니며 별도 집계한다. 해당 행동으로 공간 생성 job을 만들지 않는다.

이번 추천 응답의 서비스 상품 출처는 다나와다. 기존 도매꾹 이미지는 [템플릿 자료](10-domeggook-reuse.md)로만 관리하고 추천 상품·이미지·가격·링크에 사용하지 않는다.

## 모델·환경 확인 상태

생성 모델은 Cloudflare Workers AI의 @cf/black-forest-labs/flux-2-klein-4b로 변경했다. [공식 모델 문서](https://developers.cloudflare.com/workers-ai/models/flux-2-klein-4b/)와 [REST 입력 계약](https://developers.cloudflare.com/changelog/post/2026-01-15-flux-2-klein-4b-workers-ai/)을 확인했다. Gemini 해석·임베딩은 기존 선택을 유지한다. 실제 팀 계정의 Cloudflare·Google 호출·권한·쿼터는 미검증이며 모델 선택과 호출 성공을 구분한다.

임베딩은 공식 [가이드](https://ai.google.dev/gemini-api/docs/embeddings)의 gemini-embedding-2와 모델 목록의 gemini-embedding-2-preview 표기가 일치하지 않아, 계정의 모델 조회와 소량 호출로 정확한 model_id를 확인한다. 선택한 model_id·SDK 버전·전처리·차원을 고정하며 기존 다른 버전 벡터와 혼합하지 않는다. 계정 검증 전 변경된 모델이 실행된 것으로 기록하지 않는다. 요금은 호출 당시 공식 기준과 실제 usage를 사용한다.


## 공간 크기 계획과 비교 책임

Application Consumer는 [공간 크기 정책](11-scale-policy.md)에 따라 고정 원본·선택 템플릿으로 크기 계획을 준비하고 생성에 전달한다. 생성 후 비교에서 과소·과대·판단 보류를 구분하고 구조·주석 검사와 함께 추천 선택 조건을 검증한다. 광고 서버가 추정 치수를 상품 실측 규격으로 사용하지 않도록 경계를 유지한다. 로컬 웹 MVP는 참조 분석 뒤 scale 단계 Gemini 호출 1회를 추가하고 기존 3장 비교 응답을 확장했으며 전용 깊이 모델·서비스 큐/DB 통합은 구현하지 않았다. 비용·시도·실패와 결과 참조는 기존 작업 보존 계약을 따른다.

</details>
