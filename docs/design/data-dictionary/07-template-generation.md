# 템플릿과 입력 및 생성 속성 사전 — 직접 추천 변경 안내

2026-10-15 신규 직접 추천의 필드/관계는 [직접 추천 속성](09-direct-recommendation.md)과 [통합 계약](../12-direct-recommendation.md)을 먼저 따른다. 기존 입력·상품·노출/행동·작업 공통 컬럼은 유지하되 아래의 생성/scene_object/template 필수 연결·생성 이벤트는 신규 흐름에 적용하지 않는다. 이전 필드 목록은 이력/공통 참조를 위해 보존하며 실제 DDL 전환 완료를 뜻하지 않는다.

신규 run은 input_revision_id와 검증된 context_id를 사용하고 크기 추정·관찰과 명시 요구·상품 실측·행동 라벨을 분리한다. 초기는 라벨 없이 scikit-learn 유사도와 수동 가중합으로 실제 추천한다. run별 정책/가중치·인덱스 버전·점수 기여분을 고정하고 학습 라벨은 후속 평가에만 사용한다.

템플릿·생성 실행·사용 템플릿·생성 비용 필드는 신규 서비스 범위가 아니다. 입력 버전/자산 보존은 유지하고 생성 ID를 필수로 요구하지 않도록 별도 마이그레이션한다.

<details>
<summary>이전 상세 필드 — 공통 속성과 생성 중심 이력</summary>

# 템플릿과 입력 및 생성 속성 사전

입력 버전·템플릿·생성 실행·결과 물체와 외부 호출 이력을 설명한다. UUID PK·FK와 UTC 시각의 공통 규칙을 적용한다. 속성은 설계 계약이며 DB 구현 완료를 뜻하지 않는다.

[사전 안내](index.md) · [템플릿과 생성](../08-template-generation.md) · [데이터 모델](../03-data-model.md) · [전체 ERD](../03-erd-full.md)

## 엔터티 바로가기

- [templates](#templates): 템플릿 이미지의 불변 버전
- [template_embeddings](#template_embeddings): 템플릿 이미지 벡터
- [input_revisions](#input_revisions): 사용자 생성 입력 스냅샷
- [scene_generation_runs](#scene_generation_runs): 템플릿 기반 공간 생성 실행
- [scene_generation_templates](#scene_generation_templates): 실행별 사용 템플릿과 위치

## templates

템플릿 이미지의 불변 버전를 저장한다.

### templates id

템플릿 이미지 버전의 기본키.

- 형식: UUID (Django UUIDField, MySQL CHAR(32))
- 예시: `c86bad86-1a86-4ac1-a856-dd00d103885e`
- 해석: UUID4를 최초 등록 시 발급하고 확정된 이미지 버전을 덮어쓰지 않는다.

### templates template_key

같은 템플릿 계열의 안정적인 키.

- 형식: 문자열
- 예시: `sofa-two-seat-beige`
- 해석: version_no와 복합 UNIQUE다. 상품 product_key와 다르다.

### templates version_no

계열 안의 수정 순번.

- 형식: 정수
- 예시: `1`
- 해석: 이미지·특징 변경 시 증가시켜 새 행을 만든다.

### templates category_id

템플릿의 표준 가구 품목.

- 형식: UUID (Django UUIDField, MySQL CHAR(32))
- 예시: `ceb96b55-ee4d-46fa-9204-9db0c5f07a2d`
- 해석: [categories.id](02-products.md#categories-id)를 참조한다.

### templates asset_id

검수할 템플릿 이미지 파일.

- 형식: UUID (Django UUIDField, MySQL CHAR(32))
- 예시: `ee47322f-d90a-466a-830b-51d482224f9d`
- 해석: [assets.id](02-products.md#assets-id)를 참조한다. 파일은 불변 버전별 객체 키에 저장한다.

### templates attributes_json

템플릿의 형태·색상·스타일 조건.

- 형식: JSON
- 예시: `{"shape":"TWO_SEAT","color":"BEIGE","material":"FABRIC"}`
- 해석: 소분류와 함께 검색 조건으로 사용하며 실제 상품 치수로 표시하지 않는다.

### templates provenance_json

제작 출처·모델·지시·이용 조건과 검수 기록.

- 형식: JSON
- 예시: `{"method":"GENERATED","provider":"cloudflare","model":"@cf/black-forest-labs/flux-2-klein-4b","width":512,"height":512,"prompt_version":"template-v1","reviewer":"operator"}`
- 해석: 제작 자료 참조·이용 조건·검수 시각도 기록한다. 템플릿 사용만으로 원천 이용 조건 해결을 보장하지 않는다.

### templates review_status

검수 상태.

- 형식: 문자열
- 예시: `APPROVED`
- 해석: PENDING·APPROVED·REJECTED. APPROVED만 선택 가능하다.

### templates active

새 생성에서 선택할 수 있는지.

- 형식: 불리언
- 예시: `true`
- 해석: 활성·검수·파일 존재를 함께 검증한다. 비활성화해도 과거 사용 이력은 유지한다.

### templates created_at

템플릿 버전 등록 시각.

- 형식: UTC 날짜와 시각
- 예시: `2026-10-13T01:00:00Z`
- 해석: UTC로 저장한다.

## template_embeddings

템플릿 이미지 벡터를 저장한다.

### template_embeddings id

템플릿 벡터 레코드의 기본키.

- 형식: UUID (Django UUIDField, MySQL CHAR(32))
- 예시: `cea5aa95-8d7a-4e4b-ad7e-394b99af5d1e`
- 해석: 최초 UUID4를 보존하며 재전송은 같은 레코드로 처리한다.

### template_embeddings template_id

벡터 입력 템플릿 버전.

- 형식: UUID (Django UUIDField, MySQL CHAR(32))
- 예시: `c86bad86-1a86-4ac1-a856-dd00d103885e`
- 해석: [templates.id](#templates-id)를 참조한다. 해당 버전의 asset를 입력으로 사용한다.

### template_embeddings image_checksum

입력 이미지 내용 체크섬.

- 형식: 문자열
- 예시: `sha256:abc123`
- 해석: 모델·전처리·차원과 함께 재사용을 판정한다.

### template_embeddings model_version

임베딩 모델 식별자와 버전.

- 형식: 문자열
- 예시: `gemini-embedding-2`
- 해석: 채택 모델이다. 다른 모델의 벡터와 혼합하지 않는다.

### template_embeddings preprocessing_version

이미지 크기·방향·정규화 처리 계약.

- 형식: 문자열
- 예시: `image-v1`
- 해석: 질의와 검색 대상에 호환되는 규칙을 사용한다.

### template_embeddings dimension

벡터 원소 수.

- 형식: 정수
- 예시: `768`
- 해석: 초기 제안 차원으로 설정 가능하다. 모델·인덱스와 일치시킨다.

### template_embeddings vector_ref_json

Delta Silver의 템플릿 벡터 참조.

- 형식: JSON
- 예시: `{"table":"silver.template_vectors","version":7,"embedding_id":"cea5aa95-8d7a-4e4b-ad7e-394b99af5d1e"}`
- 해석: 벡터 본문은 Delta에 보존하고 MySQL에는 행 참조를 둔다. Delta FK는 아니다.

### template_embeddings status

벡터 생성과 보존 상태.

- 형식: 문자열
- 예시: `READY`
- 해석: PENDING·READY·FAILED 후보이며 Silver 보존 확인 후 READY다.

### template_embeddings created_at

벡터 처리 레코드 생성 시각.

- 형식: UTC 날짜와 시각
- 예시: `2026-10-13T01:10:00Z`
- 해석: UTC로 기록한다.

## input_revisions

사용자 생성 입력 스냅샷를 저장한다.

### input_revisions id

접수한 입력 스냅샷의 기본키.

- 형식: UUID (Django UUIDField, MySQL CHAR(32))
- 예시: `f261af77-af11-438c-9f5a-8a3cdcab96f3`
- 해석: 새 생성 입력 이벤트마다 발급하며 수정하지 않고 이력으로 보존한다.

### input_revisions project_id

입력이 속한 탐색 공간.

- 형식: UUID (Django UUIDField, MySQL CHAR(32))
- 예시: `d16156e5-0634-4463-a39a-37b61109aabf`
- 해석: [projects.id](03-user-flow.md#projects-id)를 참조한다.

### input_revisions revision_no

프로젝트 내 입력 순번.

- 형식: 정수
- 예시: `2`
- 해석: project_id와 복합 UNIQUE이며 최신 실행 판정에 사용한다.

### input_revisions event_id

이번 접수의 중복 방지 이벤트 ID.

- 형식: 문자열
- 예시: `4ba2b9b7-2db9-4ff4-a741-7a0f7287fcc0`
- 해석: 같은 이벤트 재전송에서 유지한다. 같은 내용의 새 이벤트는 새 버전을 허용한다.

### input_revisions room_asset_id

이 버전에서 사용하는 원본 공간 사진.

- 형식: UUID (Django UUIDField, MySQL CHAR(32))
- 예시: `ee47322f-d90a-466a-830b-51d482224f9d`
- 해석: [assets.id](02-products.md#assets-id)를 참조하며 projects의 현재 사진 변경과 독립적으로 보존한다.

### input_revisions sketch_revision_id

선택적인 스케치 버전.

- 형식: UUID (Django UUIDField, MySQL CHAR(32))
- 예시: `5eb7bf86-3512-4344-8f24-a354f9183ce7`
- 해석: [sketch_revisions.id](03-user-flow.md#sketch_revisions-id)를 참조한다. 텍스트만 입력한 경우 NULL이다.

### input_revisions text_requirements

그 시점의 텍스트 원문.

- 형식: 텍스트
- 예시: `베이지색 소파를 놓아줘`
- 해석: 원문을 보존하며 없으면 NULL이다. 해석 결과로 덮어쓰지 않는다.

### input_revisions event_payload_json

이용 데이터·입력 변경·선택의 원문.

- 형식: JSON
- 예시: `{"event_type":"TEXT_UPDATED","text":"베이지색 소파를 놓아줘"}`
- 해석: 생성 조건 후보 선택과 입력 변경을 기록한다. 노출·상품 선택·거절·클릭은 user_events에 기록하며 생성 원인이 아니다.

### input_revisions requested_regions_json

원본 사진 기준 생성 전 배치 의도.

- 형식: JSON
- 예시: `{"asset_id":"ee47322f-d90a-466a-830b-51d482224f9d","width_px":1200,"height_px":800,"regions":[[0.2,0.5,0.7,0.9]]}`
- 해석: 0~1 좌표의 기준 이미지·크기를 포함한다. 스케치 없는 경우 빈 영역 목록을 허용한다.

### input_revisions payload_checksum

입력 전체 스냅샷 대조용 체크섬.

- 형식: 문자열
- 예시: `sha256:input123`
- 해석: 정해진 원문·참조·파일 checksum 인코딩으로 계산한다. 재생성을 차단하는 유일 키로 쓰지 않는다.

### input_revisions bronze_manifest_json

확인된 Bronze 적재 위치와 버전.

- 형식: JSON
- 예시: `{"table":"bronze.user_inputs","version":9,"payload_checksum":"sha256:input123"}`
- 해석: 보존 확인 전 NULL이다. input_revision_id·파일 checksum을 접수 내용과 대조한다.

### input_revisions status

보존과 AI 시작 상태.

- 형식: 문자열
- 예시: `BRONZE_PERSISTED`
- 해석: RECEIVED·BRONZE_PENDING·BRONZE_PERSISTED·AI_PENDING 후보. 보존 실패는 jobs에서 별도 추적한다.

### input_revisions created_at

입력 접수 시각.

- 형식: UTC 날짜와 시각
- 예시: `2026-10-13T02:00:00Z`
- 해석: UTC이며 텍스트 수정·재생성 순서를 추적한다.

## scene_generation_runs

템플릿 기반 공간 생성 실행를 저장한다.

### scene_generation_runs id

공간 구성 실행의 기본키.

- 형식: UUID (Django UUIDField, MySQL CHAR(32))
- 예시: `a22e9887-3d5e-4323-8ec8-c7348bb3bfd5`
- 해석: 같은 실행의 실패 재시도는 id를 유지한다.

### scene_generation_runs input_revision_id

이번 실행의 불변 입력 버전.

- 형식: UUID (Django UUIDField, MySQL CHAR(32))
- 예시: `f261af77-af11-438c-9f5a-8a3cdcab96f3`
- 해석: [input_revisions.id](#input_revisions-id)를 참조한다. 입력 버전당 실행 최대 하나를 UNIQUE로 제한한다.

### scene_generation_runs job_id

이미지 생성 단계의 작업.

- 형식: UUID (Django UUIDField, MySQL CHAR(32))
- 예시: `898b82f3-8bae-4f73-8413-321d84c297f0`
- 해석: [jobs.id](04-ai-jobs.md#jobs-id)를 참조한다. 보존·선택 대기에서는 NULL이며 해석·물체 분석은 별도 단계 job이다.

### scene_generation_runs result_asset_id

저장 완료된 공간 생성 결과.

- 형식: UUID (Django UUIDField, MySQL CHAR(32))
- 예시: `ee47322f-d90a-466a-830b-51d482224f9d`
- 해석: [assets.id](02-products.md#assets-id)를 참조한다. 완료 전 NULL이며 분석 실패에도 결과를 유지한다.

### scene_generation_runs trigger_type

생성 입력 변경과 명시적 다시 생성의 구분.

- 형식: 문자열
- 예시: `USER_DATA`
- 해석: USER_DATA·EXPLICIT_REGENERATE. 실패 재시도는 새로운 trigger로 만들지 않는다.

### scene_generation_runs selection_json

품목·템플릿 후보와 확정 조건.

- 형식: JSON
- 예시: `{"state":"NEEDS_CATEGORY_SELECTION","category_candidates":["SOFA","CHAIR"]}`
- 해석: 선택 정책과 후보 출처를 기록한다. 실제 사용 template UUID는 연결 테이블에 고정한다.

### scene_generation_runs models_json

단계별 실제 모델·지시 버전.

- 형식: JSON
- 예시: `{"generation_provider":"cloudflare","generation":"@cf/black-forest-labs/flux-2-klein-4b","output_width":512,"output_height":512,"preprocessing_version":"flux-input-v1","embedding":"gemini-embedding-2","prompt_version":"scene-v1"}`
- 해석: 생성 제공처·모델·출력 크기·전처리·지시 버전을 접수 시 고정한다. 테스트 512×512와 이후 1024×1024는 새 실행별로 기록하며 재시도에서 바꾸지 않는다. 해석 모델은 추천 상태와 실제 사용을 구분하고 호출별 실제 값은 ai_analysis_runs에도 기록한다.

### scene_generation_runs status

전체 실행 상태.

- 형식: 문자열
- 예시: `WAITING_SELECTION`
- 해석: PENDING_BRONZE·WAITING_SELECTION·BUDGET_WAIT·RUNNING·SUCCEEDED·FAILED·SUPERSEDED 후보. 단계 실패 이력은 jobs로 추적한다.

### scene_generation_runs created_at

실행 등록 시각.

- 형식: UTC 날짜와 시각
- 예시: `2026-10-13T02:00:00Z`
- 해석: UTC로 기록한다.

### scene_generation_runs finished_at

실행 완료·종료 시각.

- 형식: UTC 날짜와 시각
- 예시: `2026-10-13T02:00:10Z`
- 해석: 종료 전 NULL이다. 최신 여부는 시간 대신 입력 순번으로 판정한다.

## scene_generation_templates

실행별 사용 템플릿과 위치를 저장한다.

### scene_generation_templates id

실행에 사용한 템플릿 연결의 기본키.

- 형식: UUID (Django UUIDField, MySQL CHAR(32))
- 예시: `9ece0e8e-47f1-45fc-9098-d69615249615`
- 해석: 복합 키로 중복 등록을 방지한다.

### scene_generation_templates generation_run_id

해당 공간 생성 실행.

- 형식: UUID (Django UUIDField, MySQL CHAR(32))
- 예시: `a22e9887-3d5e-4323-8ec8-c7348bb3bfd5`
- 해석: [scene_generation_runs.id](#scene_generation_runs-id)를 참조한다.

### scene_generation_templates template_id

선택한 불변 템플릿 버전.

- 형식: UUID (Django UUIDField, MySQL CHAR(32))
- 예시: `c86bad86-1a86-4ac1-a856-dd00d103885e`
- 해석: [templates.id](#templates-id)를 참조한다. 실행 중 버전을 교체하지 않는다.

### scene_generation_templates slot_key

어떤 요청 물체에 쓸 템플릿인지.

- 형식: 문자열
- 예시: `requested-object-1`
- 해석: generation_run_id와 복합 UNIQUE다. 같은 템플릿을 여러 영역에 사용해도 슬롯은 구분한다.

### scene_generation_templates requested_region_json

이 템플릿의 생성 전 목표 영역.

- 형식: JSON
- 예시: `{"asset_id":"ee47322f-d90a-466a-830b-51d482224f9d","width_px":1200,"height_px":800,"box":[0.2,0.5,0.7,0.9]}`
- 해석: 스케치가 없으면 NULL일 수 있다. 생성 결과 물체의 bounding_box와 별개다.

## 기존 엔터티의 생성 연결 속성

### projects latest_generation_run_id

현재 요청한 최신 생성 실행.

- 형식: UUID (Django UUIDField, MySQL CHAR(32))
- 예시: `a22e9887-3d5e-4323-8ec8-c7348bb3bfd5`
- 해석: [scene_generation_runs.id](#scene_generation_runs-id)를 참조한다. 실행이 없으면 NULL이며 같은 프로젝트 소속을 검증한다.

### scene_objects generation_run_id

물체를 얻은 공간 생성 실행.

- 형식: UUID (Django UUIDField, MySQL CHAR(32))
- 예시: `a22e9887-3d5e-4323-8ec8-c7348bb3bfd5`
- 해석: [scene_generation_runs.id](#scene_generation_runs-id)를 참조한다. result_asset와 project가 해당 실행·입력과 일치하는지 검증한다.

### ai_analysis_runs job_attempt_id

이 외부 호출이 속한 작업 시도.

- 형식: UUID (Django UUIDField, MySQL CHAR(32))
- 예시: `89c258bf-66dd-47f8-b8c6-4099cfc93c68`
- 해석: [job_attempts.id](04-ai-jobs.md#job_attempts-id)를 참조한다. 같은 job 소속을 검증한다.

### ai_analysis_runs usage_json

제공처 사용량과 비용 추정 기준.

- 형식: JSON
- 예시: `{"input_tokens":100,"output_tokens":30,"pricing_date":"2026-10-13","provider_request_id":"request-123"}`
- 해석: 입력·출력·이미지·추론 사용량과 요금 기준을 구분한다. 정보 미확보는 NULL이다.

### ai_analysis_runs estimated_cost_usd

해당 호출의 추정 USD 비용.

- 형식: DECIMAL, USD 비용
- 예시: `0.01`
- 해석: 청구 확정 금액이 아니다. 사용량 불명은 NULL이며 0으로 대신하지 않는다. 소수 자릿수는 DDL에서 확정한다.


## 공간 크기 분석 출력 계약

[공간 크기 정책](../11-scale-policy.md#저장호출-계약)의 scale_plan·api_scale_bounds·scale_check는 생성 실행에 연결한 분석 JSON이며 새 DB 컬럼이 아니다. 원본 사진 기준 폭/높이 비율과 여백 사본 기준 범위를 구분한다. anchors의 typical_size_mm는 통상 치수 가정과 불확실성을 함께 보존하며 상품 실제 규격으로 표시하지 않는다. 실행·입력·참조·정책·모델 버전과 출력 checksum으로 추적한다. 실제 서비스 저장 매핑은 통합 시 확정하며 로컬 파일을 MySQL 저장 완료로 해석하지 않는다.

</details>
