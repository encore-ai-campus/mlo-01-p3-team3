# AI와 작업 속성 사전 — 직접 추천 변경 안내

2026-10-15 신규 직접 추천의 필드/관계는 [직접 추천 속성](09-direct-recommendation.md)과 [통합 계약](../12-direct-recommendation.md)을 먼저 따른다. 기존 입력·상품·노출/행동·작업 공통 컬럼은 유지하되 아래의 생성/scene_object/template 필수 연결·생성 이벤트는 신규 흐름에 적용하지 않는다. 이전 필드 목록은 이력/공통 참조를 위해 보존하며 실제 DDL 전환 완료를 뜻하지 않는다.

신규 run은 input_revision_id와 검증된 context_id를 사용하고 크기 추정·관찰과 명시 요구·상품 실측·행동 라벨을 분리한다. 학습 데이터/라벨은 오프라인으로 관리하고 검증된 scikit-learn 모델은 CPU 운영 추천에 연결한다. run별 모델 버전·특징·점수를 고정한다.

<details>
<summary>이전 상세 필드 — 공통 속성과 생성 중심 이력</summary>

# AI와 작업 속성 사전

현재 논리 설계의 각 속성에 무엇을 저장하는지 설명한다. 형식·값·상태 코드는 후보이며, 예시는 테스트 설명용으로 실제 계정이나 상품을 뜻하지 않는다.

[사전 안내](index.md) · [데이터 모델](../03-data-model.md) · [전체 ERD](../03-erd-full.md)

## 엔터티 바로가기

- [ai_analysis_runs](#ai_analysis_runs): 외부 AI API의 호출 실행
- [jobs](#jobs): 비동기 업무 작업
- [job_attempts](#job_attempts): 작업을 실행한 개별 시도
- [pipeline_runs](#pipeline_runs): 상품 가공 파이프라인의 실행
- [pipeline_items](#pipeline_items): 파이프라인이 처리한 원천별 결과
- [publishing_runs](#publishing_runs): Delta Gold를 MySQL에 반영한 실행

## ai_analysis_runs

이 엔터티는 외부 AI API의 호출 실행에 해당한다. 같은 이름의 속성이라도 아래 업무 의미를 기준으로 해석한다.

### ai_analysis_runs id

외부 AI API의 호출 실행 한 건의 UUID 기본키.

- 형식 후보: UUID (Django UUIDField, MySQL CHAR(32))
- 예시: `4694cf38-7e90-418f-8544-ab9d5989f54e`
- 해석: 신규 행 생성 시 UUID4를 한 번 발급한다. API와 메시지에도 같은 id를 사용하며 재처리·이관에서는 보존한다. 제공처 ID와 이벤트 재전송 식별자와는 용도가 다르다. UUID만으로 접근 권한을 보장하지 않는다.

### ai_analysis_runs job_id

이 외부 AI 호출을 요청한 비동기 작업의 내부 ID.

- 형식 후보: UUID (Django UUIDField, MySQL CHAR(32))
- 예시: `898b82f3-8bae-4f73-8413-321d84c297f0`
- 해석: [jobs.id](04-ai-jobs.md#jobs-id)를 참조하는 MySQL FK다. 참조 대상 PK와 같은 UUID 타입을 사용한다.

### ai_analysis_runs analysis_type

AI 분석 작업의 종류.

- 형식 후보: 문자열
- 예시: `SKETCH_CATEGORY`
- 해석: PRODUCT_IMAGE_CHECK 등 상품 보조 분석과 구분한다. 최종 코드 집합은 구현 전에 정한다.

### ai_analysis_runs input_asset_id

모델에 전달한 스케치 또는 상품 이미지의 내부 ID.

- 형식 후보: UUID (Django UUIDField, MySQL CHAR(32))
- 예시: `ee47322f-d90a-466a-830b-51d482224f9d`
- 해석: [assets.id](02-products.md#assets-id)를 참조하는 MySQL FK다. 참조 대상 PK와 같은 UUID 타입을 사용한다. 이미지 참조여도 파일 본문을 저장하지 않는다.

### ai_analysis_runs input_hash

실제 모델 입력 이미지와 전처리의 식별 해시.

- 형식 후보: 문자열
- 예시: `sha256:input123...`
- 해석: 입력 재사용 판단에 사용한다. 모델·프롬프트·전처리 버전도 함께 고려한다.

### ai_analysis_runs source_revision

분석 입력의 논리 버전.

- 형식 후보: 문자열
- 예시: `sketch:project-001:2`
- 해석: 스케치 또는 상품 이미지 버전이다. 분석 종류에 따라 다르므로 특정 테이블 FK로 단정하지 않는다.

### ai_analysis_runs provider

모델 API 제공처 코드.

- 형식 후보: 문자열
- 예시: `google`
- 해석: 생성은 cloudflare, Gemini 해석·임베딩은 google로 기록한다. 모델 이름과 구분하고 제공처별 어댑터와 데이터 정책 추적에 쓴다.

### ai_analysis_runs model_version

실제 호출한 모델과 버전.

- 형식 후보: 문자열
- 예시: `@cf/black-forest-labs/flux-2-klein-4b`
- 해석: 생성은 Cloudflare FLUX.2 klein 4B, 임베딩은 gemini-embedding-2다. 해석 모델의 추천 상태와 실제 호출을 구분한다. 제공처의 응답 메타데이터 버전도 필요 시 기록한다.

### ai_analysis_runs prompt_version

호출한 지시문 템플릿 버전.

- 형식 후보: 문자열
- 예시: `sketch-category-v1`
- 해석: 모델 버전이나 결과 JSON 구조 버전과 다르다.

### ai_analysis_runs schema_version

AI 출력 계약의 버전.

- 형식 후보: 문자열
- 예시: `1`
- 해석: 어떤 카테고리·판단 필드를 검증했는지 기록한다. 원천 문서 schema_version과 다른 계약이다.

### ai_analysis_runs output_json

검증한 AI 분석 결과.

- 형식 후보: JSON
- 예시: `{"decision":"ACCEPTED","category_code":"CHAIR"}`
- 해석: 제공처의 가격·규격 사실을 대체하지 않는다. 응답 형식과 내용을 각각 검증한다.

### ai_analysis_runs result_asset_id

큰 AI 응답을 별도 저장한 파생 결과 파일의 내부 ID.

- 형식 후보: UUID (Django UUIDField, MySQL CHAR(32))
- 예시: `ee47322f-d90a-466a-830b-51d482224f9d`
- 해석: [assets.id](02-products.md#assets-id)를 참조하는 MySQL FK다. 참조 대상 PK와 같은 UUID 타입을 사용한다. 이미지 참조여도 파일 본문을 저장하지 않는다. 해당 단계에서 연결이 없으면 NULL을 허용하는 후보이다.

### ai_analysis_runs status

AI 호출과 결과 저장의 실행 상태.

- 형식 후보: 문자열
- 예시: `SUCCEEDED`
- 해석: 출력 decision의 ACCEPTED와 구분한다. API 실행에 성공해도 카테고리는 UNKNOWN일 수 있다.

### ai_analysis_runs latency_ms

외부 AI 요청 전송 직전부터 응답 본문 수신 완료 또는 오류·시간 초과 확정까지의 호출 소요시간.

- 형식 후보: 정수
- 예시: `850`
- 해석: 단조 증가 시계로 측정한 밀리초 단위의 0 이상 정수이며 호출 로그의 duration_ms와 같은 값이다. 스트리밍은 마지막 본문 수신까지 포함한다. 전처리·파일 준비·응답 검증·결과 저장·호출 간 재시도 대기는 제외하고 별도 작업 로그에 기록한다. 실패·시간 초과 호출도 기록한다. 장애 후 추정값으로 이 필드를 채우지 않으며 측정값을 잃으면 NULL로 두고 복구 로그에 사유를 남긴다. [공통 시간 기록 계약](../06-operations-migration.md#모든-작업-로그의-소요시간-기록-계약)을 따른다.

### ai_analysis_runs error_code

AI 호출 실패를 구분하는 코드.

- 형식 후보: 문자열
- 예시: `MODEL_TIMEOUT`
- 해석: 제공처 원문 오류와 내부 표준 코드를 매핑한다. 성공 시 NULL 가능하다.

### ai_analysis_runs created_at

외부 AI API의 호출 실행 행을 DB에 처음 등록한 시각.

- 형식 후보: UTC 날짜와 시각
- 예시: `2026-10-12T01:00:00Z`
- 해석: UTC로 저장한다. collected_at·occurred_at·started_at 등 업무 발생 시각과 다를 수 있다.

### ai_analysis_runs updated_at

외부 AI API의 호출 실행 행을 마지막으로 수정한 시각.

- 형식 후보: UTC 날짜와 시각
- 예시: `2026-10-12T01:01:00Z`
- 해석: UTC로 저장한다. 행의 최종 갱신 시각이며 원천 가격의 수집 시각이나 파일 내용 변경 시각을 대신하지 않는다.

## jobs

이 엔터티는 비동기 업무 작업에 해당한다. 같은 이름의 속성이라도 아래 업무 의미를 기준으로 해석한다.

### jobs id

비동기 업무 작업 한 건의 UUID 기본키.

- 형식 후보: UUID (Django UUIDField, MySQL CHAR(32))
- 예시: `898b82f3-8bae-4f73-8413-321d84c297f0`
- 해석: 신규 행 생성 시 UUID4를 한 번 발급한다. API와 메시지에도 같은 id를 사용하며 재처리·이관에서는 보존한다. 제공처 ID와 이벤트 재전송 식별자와는 용도가 다르다. UUID만으로 접근 권한을 보장하지 않는다.

### jobs type

비동기 업무 작업의 종류.

- 형식 후보: 문자열
- 예시: `SKETCH_RECOGNITION`
- 해석: 상품 분석 등 종류별 Consumer와 재시도 규칙을 선택하는 기준이다.

### jobs input_refs_json

작업 입력을 찾을 참조 목록.

- 형식 후보: JSON
- 예시: `{"sketch_revision_id":"5eb7bf86-3512-4344-8f24-a354f9183ce7","asset_id":"ee47322f-d90a-466a-830b-51d482224f9d","input_hash":"sha256:..."}`
- 해석: 이미지 본문이나 API 키를 넣지 않는다. 입력 버전까지 고정해 재시도 시 같은 입력을 사용한다.

### jobs status

작업의 현재 처리 상태.

- 형식 후보: 문자열
- 예시: `RUNNING`
- 해석: PENDING·RUNNING·SUCCEEDED·RETRY_WAIT·BUDGET_WAIT·FAILED를 구분한다. 사용자가 그림을 선택했는지와 다르다.

### jobs attempt_count

지금까지 시작한 실행 시도 수.

- 형식 후보: 정수
- 예시: `2`
- 해석: Kafka 수신 횟수가 아니다. lease 확보와 시도 생성 시 원자적으로 갱신한다.

### jobs max_attempts

해당 작업의 자동 실행 시도 상한.

- 형식 후보: 정수
- 예시: `3`
- 해석: 초기 후보값이다. 오류 유형에 따라 재시도 없이 실패할 수 있다.

### jobs next_retry_at

다음 자동 재시도를 시작해도 되는 시각.

- 형식 후보: UTC 날짜와 시각
- 예시: `2026-10-12T03:05:00Z`
- 해석: 시각 저장만으로 실행되지 않는다. 발행기가 도래한 대상을 찾아 재발행한다.

### jobs lease_owner

현재 작업을 맡은 Consumer 프로세스 식별자.

- 형식 후보: 문자열
- 예시: `app-115-consumer-01`
- 해석: AI 모델 서버 주소가 아니다. 일정 시간의 실행 권한을 가진 프로세스를 표시한다.

### jobs lease_token

이번 실행 권한을 구분하는 고유 토큰.

- 형식 후보: 문자열
- 예시: `lease-uuid-001`
- 해석: 작업을 회수하면 새 값으로 바꾼다. 결과 저장 시 현재 토큰과 일치하는지 확인해 늦게 돌아온 이전 워커를 차단한다.

### jobs lease_expires_at

현재 실행 권한이 만료되는 시각.

- 형식 후보: UTC 날짜와 시각
- 예시: `2026-10-12T03:10:00Z`
- 해석: 만료된 RUNNING 작업을 회수하는 기준이다. 자동 회수·갱신 코드를 구현해야 효과가 있으며 DB 잠금 자체가 아니다.

### jobs started_at

작업 실행을 최초 시작한 시각.

- 형식 후보: UTC 날짜와 시각
- 예시: `2026-10-12T03:00:00Z`
- 해석: 시도별 시작은 job_attempts에서 관리한다. 재시도마다 이 값을 덮어쓰지 않고 최초 실행 시각을 유지한다.

### jobs finished_at

작업이 최종 성공 또는 최종 실패한 시각.

- 형식 후보: UTC 날짜와 시각
- 예시: `2026-10-12T03:01:00Z`
- 해석: 재시도 대기 중에는 최종 완료로 표시하지 않는다.

### jobs result_ref_json

완료된 업무 결과를 찾을 참조.

- 형식 후보: JSON
- 예시: `{"recommendation_run_id":"1aef5145-108f-468b-b1ab-15239ff6601e","ai_analysis_run_id":"4694cf38-7e90-418f-8544-ab9d5989f54e"}`
- 해석: 큰 AI 응답 본문과 구분하며 실패 시 NULL 가능하다.

### jobs last_error_json

가장 최근 실패의 구조화 정보.

- 형식 후보: JSON
- 예시: `{"code":"MODEL_TIMEOUT","attempt_no":2}`
- 해석: 전체 시도 이력을 대체하지 않는다. 비밀값이나 불필요한 개인정보를 제거한다.

### jobs created_at

비동기 업무 작업 행을 DB에 처음 등록한 시각.

- 형식 후보: UTC 날짜와 시각
- 예시: `2026-10-12T01:00:00Z`
- 해석: UTC로 저장한다. collected_at·occurred_at·started_at 등 업무 발생 시각과 다를 수 있다.

### jobs updated_at

비동기 업무 작업 행을 마지막으로 수정한 시각.

- 형식 후보: UTC 날짜와 시각
- 예시: `2026-10-12T01:01:00Z`
- 해석: UTC로 저장한다. 행의 최종 갱신 시각이며 원천 가격의 수집 시각이나 파일 내용 변경 시각을 대신하지 않는다.

## job_attempts

이 엔터티는 작업을 실행한 개별 시도에 해당한다. 같은 이름의 속성이라도 아래 업무 의미를 기준으로 해석한다.

### job_attempts id

작업을 실행한 개별 시도 한 건의 UUID 기본키.

- 형식 후보: UUID (Django UUIDField, MySQL CHAR(32))
- 예시: `89c258bf-66dd-47f8-b8c6-4099cfc93c68`
- 해석: 신규 행 생성 시 UUID4를 한 번 발급한다. API와 메시지에도 같은 id를 사용하며 재처리·이관에서는 보존한다. 제공처 ID와 이벤트 재전송 식별자와는 용도가 다르다. UUID만으로 접근 권한을 보장하지 않는다.

### job_attempts job_id

이번 시도가 수행한 업무 작업의 내부 ID.

- 형식 후보: UUID (Django UUIDField, MySQL CHAR(32))
- 예시: `898b82f3-8bae-4f73-8413-321d84c297f0`
- 해석: [jobs.id](04-ai-jobs.md#jobs-id)를 참조하는 MySQL FK다. 참조 대상 PK와 같은 UUID 타입을 사용한다.

### job_attempts attempt_no

같은 작업 안의 실행 시도 순번.

- 형식 후보: 정수
- 예시: `2`
- 해석: job_id와 복합 고유키 후보다. 원래 요청을 다시 수신한 횟수와 다르다.

### job_attempts worker_id

이 시도를 실제 수행한 프로세스 식별자.

- 형식 후보: 문자열
- 예시: `app-115-consumer-01`
- 해석: 별도 AI VM을 뜻하지 않는다. jobs.lease_owner와 같은 식별 체계가 가능하다.

### job_attempts started_at

해당 실행 시도를 시작한 시각.

- 형식 후보: UTC 날짜와 시각
- 예시: `2026-10-12T03:00:30Z`
- 해석: jobs의 최초 시작 시각과 구분한다.

### job_attempts finished_at

이 시도가 성공 또는 실패로 종료된 시각.

- 형식 후보: UTC 날짜와 시각
- 예시: `2026-10-12T03:01:00Z`
- 해석: 작업 전체의 최종 종료 시각과 다를 수 있다. 중단된 시도는 복구 정책으로 마감한다.

### job_attempts status

개별 시도의 실행 결과.

- 형식 후보: 문자열
- 예시: `FAILED`
- 해석: 작업은 재시도 후 성공하더라도 이전 FAILED 시도는 유지한다.

### job_attempts error_code

개별 시도의 표준 오류 코드.

- 형식 후보: 문자열
- 예시: `MODEL_TIMEOUT`
- 해석: 성공 시 NULL이며 오류별 재시도 가능성을 판단한다.

### job_attempts error_message

개별 시도의 오류 설명.

- 형식 후보: 긴 문자열
- 예시: `모델 응답 제한 시간 초과`
- 해석: 자세한 스택은 서비스 로그로 연결하고 키·개인정보를 노출하지 않는다.

### job_attempts created_at

작업을 실행한 개별 시도 행을 DB에 처음 등록한 시각.

- 형식 후보: UTC 날짜와 시각
- 예시: `2026-10-12T01:00:00Z`
- 해석: UTC로 저장한다. collected_at·occurred_at·started_at 등 업무 발생 시각과 다를 수 있다.

## pipeline_runs

이 엔터티는 상품 가공 파이프라인의 실행에 해당한다. 같은 이름의 속성이라도 아래 업무 의미를 기준으로 해석한다.

### pipeline_runs id

상품 가공 파이프라인의 실행 한 건의 UUID 기본키.

- 형식 후보: UUID (Django UUIDField, MySQL CHAR(32))
- 예시: `e3835573-86c9-4138-b036-b95ce2bb3aa3`
- 해석: 신규 행 생성 시 UUID4를 한 번 발급한다. API와 메시지에도 같은 id를 사용하며 재처리·이관에서는 보존한다. 제공처 ID와 이벤트 재전송 식별자와는 용도가 다르다. UUID만으로 접근 권한을 보장하지 않는다.

### pipeline_runs type

상품 파이프라인 실행의 종류.

- 형식 후보: 문자열
- 예시: `NORMALIZATION`
- 해석: BRONZE_INGEST·NORMALIZATION·GOLD_BUILD 등의 후보로 단계와 책임을 구분한다.

### pipeline_runs input_range_json

이 실행이 처리할 원천 범위와 버전.

- 형식 후보: JSON
- 예시: `{"collection_run_id":"collect-20261012-001","delta_version":7}`
- 해석: 재실행에 필요한 원천 ID 묶음·시점·테이블 버전을 기록한다.

### pipeline_runs code_version

적용한 파이프라인 코드 버전.

- 형식 후보: 문자열
- 예시: `normalizer-v1`
- 해석: 모델 버전과 다르다. 배포 commit 등 재현 가능한 식별자를 사용할 수 있다.

### pipeline_runs status

해당 가공 실행의 상태.

- 형식 후보: 문자열
- 예시: `SUCCEEDED`
- 해석: 원천별 일부 실패를 어떻게 표현할지 상태 정책을 정하고 pipeline_items와 대조한다.

### pipeline_runs started_at

해당 가공 실행을 시작한 시각.

- 형식 후보: UTC 날짜와 시각
- 예시: `2026-10-12T01:05:00Z`
- 해석: 수집 시각과 다르다.

### pipeline_runs finished_at

해당 가공 실행을 종료한 시각.

- 형식 후보: UTC 날짜와 시각
- 예시: `2026-10-12T01:06:00Z`
- 해석: 성공·실패 판정과 함께 기록하며 실행 중에는 NULL이다.

### pipeline_runs created_at

상품 가공 파이프라인의 실행 행을 DB에 처음 등록한 시각.

- 형식 후보: UTC 날짜와 시각
- 예시: `2026-10-12T01:00:00Z`
- 해석: UTC로 저장한다. collected_at·occurred_at·started_at 등 업무 발생 시각과 다를 수 있다.

### pipeline_runs updated_at

상품 가공 파이프라인의 실행 행을 마지막으로 수정한 시각.

- 형식 후보: UTC 날짜와 시각
- 예시: `2026-10-12T01:01:00Z`
- 해석: UTC로 저장한다. 행의 최종 갱신 시각이며 원천 가격의 수집 시각이나 파일 내용 변경 시각을 대신하지 않는다.

## pipeline_items

이 엔터티는 파이프라인이 처리한 원천별 결과에 해당한다. 같은 이름의 속성이라도 아래 업무 의미를 기준으로 해석한다.

### pipeline_items id

파이프라인이 처리한 원천별 결과 한 건의 UUID 기본키.

- 형식 후보: UUID (Django UUIDField, MySQL CHAR(32))
- 예시: `3fe6ed23-130d-4d53-98d5-a873be8891a1`
- 해석: 신규 행 생성 시 UUID4를 한 번 발급한다. API와 메시지에도 같은 id를 사용하며 재처리·이관에서는 보존한다. 제공처 ID와 이벤트 재전송 식별자와는 용도가 다르다. UUID만으로 접근 권한을 보장하지 않는다.

### pipeline_items run_id

이 원천을 처리한 파이프라인 실행의 내부 ID.

- 형식 후보: UUID (Django UUIDField, MySQL CHAR(32))
- 예시: `e3835573-86c9-4138-b036-b95ce2bb3aa3`
- 해석: [pipeline_runs.id](04-ai-jobs.md#pipeline_runs-id)를 참조하는 MySQL FK다. 참조 대상 PK와 같은 UUID 타입을 사용한다.

### pipeline_items raw_document_id

이 처리 항목이 사용한 원천 문서 ID.

- 형식 후보: 문자열
- 예시: `507f1f77bcf86cd799439011`
- 해석: MongoDB 추적용이며 FK가 아니다. 원천부터 오류를 재현할 수 있게 한다.

### pipeline_items product_key

처리 중 매칭하거나 생성한 통합 상품 키.

- 형식 후보: 문자열
- 예시: `product-chair-a-beige`
- 해석: 매칭 실패 시 NULL 가능하다. 실패 기록에서도 남길 수 있도록 강제 FK로 단정하지 않는다.

### pipeline_items offer_key

처리한 판매 항목의 안정적인 키.

- 형식 후보: 문자열
- 예시: `danawa:19170035:beige`
- 해석: 생성 이전에 실패하면 NULL 가능하다. MySQL 내부 offer_id와 구분한다.

### pipeline_items status

원천 한 건의 가공 결과.

- 형식 후보: 문자열
- 예시: `REJECTED`
- 해석: SUCCESS·REJECTED·FAILED 등의 후보다. 필수 정보 미충족과 시스템 장애를 구분한다.

### pipeline_items error_code

원천별 검증 또는 처리 오류 코드.

- 형식 후보: 문자열
- 예시: `MISSING_PURCHASE_URL`
- 해석: 어떤 규칙 때문에 추천에서 제외됐는지 또는 재처리 대상인지 확인한다.

### pipeline_items error_message

원천별 오류 상세.

- 형식 후보: 긴 문자열
- 예시: `구매 주소 미제공으로 추천 제외`
- 해석: 원천 payload를 수정하지 않고 실패 근거만 남긴다.

### pipeline_items created_at

파이프라인이 처리한 원천별 결과 행을 DB에 처음 등록한 시각.

- 형식 후보: UTC 날짜와 시각
- 예시: `2026-10-12T01:00:00Z`
- 해석: UTC로 저장한다. collected_at·occurred_at·started_at 등 업무 발생 시각과 다를 수 있다.

### pipeline_items updated_at

파이프라인이 처리한 원천별 결과 행을 마지막으로 수정한 시각.

- 형식 후보: UTC 날짜와 시각
- 예시: `2026-10-12T01:01:00Z`
- 해석: UTC로 저장한다. 행의 최종 갱신 시각이며 원천 가격의 수집 시각이나 파일 내용 변경 시각을 대신하지 않는다.

## publishing_runs

이 엔터티는 Delta Gold를 MySQL에 반영한 실행에 해당한다. 같은 이름의 속성이라도 아래 업무 의미를 기준으로 해석한다.

### publishing_runs id

Delta Gold를 MySQL에 반영한 실행 한 건의 UUID 기본키.

- 형식 후보: UUID (Django UUIDField, MySQL CHAR(32))
- 예시: `00da5a69-bf7f-482b-b8bf-37bb0ab7dcf3`
- 해석: 신규 행 생성 시 UUID4를 한 번 발급한다. API와 메시지에도 같은 id를 사용하며 재처리·이관에서는 보존한다. 제공처 ID와 이벤트 재전송 식별자와는 용도가 다르다. UUID만으로 접근 권한을 보장하지 않는다.

### publishing_runs execution_id

MySQL 반영 실행을 식별하는 안정적인 ID.

- 형식 후보: 문자열
- 예시: `publish-20261012-001`
- 해석: staging 데이터 묶음과 재실행을 연결한다. pipeline_run_id FK와는 다른 식별자다.

### publishing_runs delta_versions_json

반영에 사용한 Gold 테이블별 Delta 버전.

- 형식 후보: JSON
- 예시: `{"product_catalog":7,"current_offers":11}`
- 해석: 테이블마다 버전이 다르므로 전체 Lake의 버전 하나로 취급하지 않는다.

### publishing_runs record_counts_json

검증한 staging 및 반영 건수.

- 형식 후보: JSON
- 예시: `{"products":120,"offers":180}`
- 해석: 실제 통과·반영·실패 건수 항목의 계약을 고정한다.

### publishing_runs status

Gold의 MySQL 반영 상태.

- 형식 후보: 문자열
- 예시: `SUCCEEDED`
- 해석: 가공 실행 성공과 서비스 DB 반영 성공은 별개다.

### publishing_runs started_at

반영 실행을 시작한 시각.

- 형식 후보: UTC 날짜와 시각
- 예시: `2026-10-12T01:07:00Z`
- 해석: Gold 생성 시각이나 가격 수집 시각과 다르다.

### publishing_runs finished_at

반영 실행이 종료된 시각.

- 형식 후보: UTC 날짜와 시각
- 예시: `2026-10-12T01:08:00Z`
- 해석: 실패에도 기록하고 성공 판정은 별도 status로 확인한다.

### publishing_runs created_at

Delta Gold를 MySQL에 반영한 실행 행을 DB에 처음 등록한 시각.

- 형식 후보: UTC 날짜와 시각
- 예시: `2026-10-12T01:00:00Z`
- 해석: UTC로 저장한다. collected_at·occurred_at·started_at 등 업무 발생 시각과 다를 수 있다.

### publishing_runs updated_at

Delta Gold를 MySQL에 반영한 실행 행을 마지막으로 수정한 시각.

- 형식 후보: UTC 날짜와 시각
- 예시: `2026-10-12T01:01:00Z`
- 해석: UTC로 저장한다. 행의 최종 갱신 시각이며 원천 가격의 수집 시각이나 파일 내용 변경 시각을 대신하지 않는다.

</details>
