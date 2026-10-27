# 상품 Silver 설계 v1

2026-10-16 기준안, 2026-10-17 구현 상태 갱신. Silver 정규화·품질·Delta 저장·재평가 코드를 추가했다. 실제 실행·입력 형식·검증 경계는 [실행 문서](silver_execution.md)를 따른다. 이 문서는 Silver 범위이며 Gold는 [별도 실행](gold_execution.md)으로 구현했다. MySQL 게시는 Silver와 분리된 [게시기](mysql_publication.md)로 구현했다. 승인·임베딩·수집기 수정은 타 담당/별도 범위이며 실제 서비스 계약·운영 인수는 남아 있다. [46항목 대조표](silver_gold_field_mapping.md)를 요구사항 추적 기준으로 사용한다. 상위 문서/서비스 DDL은 수정하지 않는다.

## 1. 입력과 처리 경계

Bronze `raw_products`의 고정 Delta version·Raw ID·document_hash·raw_envelope_json·payload_json을 입력으로 한다. 원문은 Bronze 계약과 같은 canonical 해시 규칙으로 대조하고 Silver 결과로 덮어쓰지 않는다. 이미지 상태와 매핑·검수·링크·속성 사전은 아래의 별도 고정 입력이다. 크롤러의 제품 목록을 다시 요청하거나 링크를 임의로 외부 검증하지 않는다.

| 입력 | 출처와 동결 기준 | 미제공과 장애 구분 |
|---|---|---|
| 원천 | Bronze의 고정 version 및 원천 해시 | 존재해야 할 행/파일 미확인은 RETRY_WAIT, 성공 receipt 금지 |
| 이미지 상태 | ingestion_observations의 같은 observation_key·Raw ID·vector/revision/fingerprint를 primary/majority 조회 | hash·슬롯·checksum 대조. 명시 UNCONFIRMED와 DB 조회 실패는 다름 |
| 서비스 분류 매핑 | category_mappings 담당의 불변 export 또는 일관 읽기 snapshot | NOT_PROVIDED이면 미매핑 보존. 연결/권한/읽기 오류를 빈 매핑으로 처리하지 않음 |
| 이용 검수 | 상품·source_version·정확한 이미지/권리 근거·scope별 검수 snapshot | NOT_PROVIDED이면 PENDING. assets 수준 승인만으로 상품별 이용 승인 금지 |
| 링크 현재 상태 | 담당 검증기의 상품·URL·옵션·상태·checked_at·expires_at snapshot | NOT_PROVIDED이면 NOT_CHECKED. 구조 검사는 Silver 자체 수행 가능 |
| 별칭·속성·축 규칙 | 담당이 제공한 버전 고정 사전·매핑 | 미제공이면 원문/미매핑 유지. 임의 코드나 축 추정 금지 |
| 실행 등록 | pipeline_runs의 NORMALIZATION 실행 UUID와 등록 근거 | 수집 job_id로 대체하지 않음. 실제 등록 연동 미제공이면 운영 실행은 대기, 로컬 fixture 실험과 구분 |

snapshot은 version ID만 보관하지 않고 실제 행 집합·삭제/부재·NULL·scope·유효성 상태와 content hash를 보존한다. 제공받지 못한 입력은 `{status: NOT_PROVIDED, rows: []}` 같은 명시적 부재로 동결한다. 이 표현은 팀 승인을 대체하는 가짜 버전이 아니다. 실제 데이터 형식 adapter는 담당 export/DDL에 맞추고 설정 preflight로 검증한다.

예정 snapshot 공통 포장은 contract_version·kind·provider_version(미제공이면 NULL)·content_hash·status·rows·evidence_ref다. captured_at은 진단용이며 의미 hash에서 제외한다. 매핑 행은 source_code·원천 분류 namespace/경로·service category ID/code·active/excluded·유효 범위, 검수 행은 상품/source_version/checksum/scope·판정·권리/감사 근거·유효 범위, 링크 행은 상품/판매/옵션·URL/종류·검증 결과·checked_at/expires_at·검증 정책을 포함해야 한다. 실제 담당 export가 다르면 adapter로 변환하며 없는 근거는 생성하지 않는다.

등록 UUID의 서비스 저장소 계약을 지킨다. `processing_run_id`는 상위 사전의 MySQL pipeline_runs.id이며 운영에서 등록 확인을 거친다. 별도 `execution_id`는 매 시도의 UUID로 실행 로그에만 사용한다. 테스트 fixture UUID는 테스트용이라고 표시하며 MySQL 등록 완료로 주장하지 않는다. 파이프라인의 운영 실행 등록 방식은 실제 DDL/담당 인터페이스를 제공받아 연결할 항목이다.

## 2. 테이블과 행 기준

아래 경로는 Delta용 저장소 root 아래의 **신규 논리 스키마**다. 제안서의 `silver_` 접두어 테이블명은 경로 `silver/`로 대응한다. 로컬 실제 Delta 테이블을 생성·검증했으며 실제 VM/lake에는 아직 배포하지 않았다.

| 경로 | 한 행의 기준 | 고유 키 | 제안/기존 계약과 관계 |
|---|---|---|---|
| silver/product_observations | 불변 Raw 관측에 대한 한 입력 버전의 상품 평가 | evaluation_key | 제안 상품 관측. 다른 evaluation은 같은 source 관측의 변환 이력 |
| silver/product_attributes | evaluation × 속성 근거 × 옵션/대상/부위 | attribute_key | 제안 속성 관측 |
| silver/price_observations | evaluation × 가격 근거 × 종류/조건/옵션 | price_observation_key | 목록/집계/미확인 가격도 보존. 판매처 Offer와 구분 |
| silver/product_images | evaluation × 상품별 image_slot/source_version 연결 | image_relation_key | 제안 상품–이미지 관측 |
| silver/link_observations | evaluation × URL/종류/판매·옵션 연결 근거 | link_observation_key | 제안 링크 관측 |
| silver/quality_issues | evaluation × 대상 키/필드/문제 코드/근거 | issue_key | 기존 silver/quality_issues 경로를 사용 |
| silver/normalized_offers | evaluation × 확인된 offer_key의 식별 및 가격/링크 자식 참조 | normalized_offer_key | 금액·조건은 price_observations에 별도 보존. 안정 ID 미확인 항목은 억지 생성하지 않음 |
| silver/pipeline_evaluations | stage + raw_document_id + processing_input_fingerprint | evaluation_key | 보강 v1.1의 출력 완료 receipt |

일반 테이블은 평가 버전별 append/insert-only MERGE다. 동일 raw_document_id라도 입력 상태/정책이 바뀌면 새 evaluation으로 결과를 보존한다. 보고서에서 **원천 관측 수는 distinct Raw ID**, 고유 상품 수는 distinct product_key, 평가 수는 evaluation_key로 각각 집계한다. 선택할 평가를 정하지 않은 채 모든 이력을 현재 상품처럼 join하지 않는다.

## 3. 키와 해시

H는 기존 pipeline-json-v1: UTF-8 JSON, ensure_ascii=False·sort_keys=True·separators=(',', ':')·allow_nan=False의 SHA-256이다. 문자열/URL/배열 순서를 임의로 바꾸지 않으며 집합은 계약에 명시한 업무 키 순서로 정렬한 후 hash한다.

processing_input_fingerprint는 [보강 v1.1 5.2](design_amendment_v1_1.md#52-평가-입력과-완료-증거)의 필드명을 유지한다: stage=SILVER, raw_document_id, document_hash, image_state_revision/fingerprint, gallery_selection_evidence_hash, mapping_snapshot_hash, review_snapshot_hash, link_validation_snapshot_hash, normalizer_version, quality_policy_version, gold_policy_version=null, contract_version=1. normalizer_version의 불변 규칙 manifest에는 사용한 별칭·색상·재질·형태·단위·축·가격 규칙 사전 hash를 포함한다. 같은 버전명으로 내용을 바꾸면 충돌이다.

gallery_selection_evidence_hash는 선택한 고정 gallery 슬롯·순서·완전성/대상 근거와 정책상 이미지 검증의 effective 상태를 포함한다. 원격 검증 freshness 정책을 도입하면 그 만료 전이도 이 의미 투영에 반영해 같은 fingerprint의 다른 결과가 되지 않게 한다. 정책이 없으면 NOT_EVALUATED로 두며 최신 확인 시각만으로 영구 유효를 주장하지 않는다. 단순 조회 시각을 추가해 매번 새 평가를 만들지 않는다.

| 키 | 정의/제약 |
|---|---|
| product_key | `danawa:<source_product_id>`; 내부 서비스 UUID 아님 |
| source_evidence_key | H({raw_document_id, source_path, source_value_hash}) |
| evaluation_key | `silver-eval-v1:` + H({stage: SILVER, raw_document_id, processing_input_fingerprint}) |
| attribute_key | `silver-attr-v1:` + H({evaluation_key, source_evidence_key, attribute_name, measurement_target, option_evidence_key, component}) |
| price_observation_key | `silver-price-v1:` + H({evaluation_key, source_evidence_key, price_kind, condition_fingerprint, option_evidence_key}) |
| image_relation_key | `silver-image-v1:` + H({evaluation_key, raw_document_id, image_slot, source_version}) |
| link_observation_key | `silver-link-v1:` + H({evaluation_key, source_evidence_key, link_kind, source_url, offer_key, option_evidence_key}) |
| issue_key | `silver-issue-v1:` + H({evaluation_key, target_key, field_name, issue_code, evidence_hash}) |
| offer_key | 기존 offer-v1:H(tuple) 규칙 유지. source·상품·안정 seller·판매 항목·옵션 확인이 필수 |
| normalized_offer_key | `silver-offer-v1:` + H({evaluation_key, offer_key}) |

source_path는 불변 Raw 안의 JSON pointer다. 배열 위치는 **해당 원문 안의 근거 위치**로만 쓴다. 배열 위치·가격·이름·클릭 토큰을 다른 Raw까지 유지되는 판매 항목 업무 키로 사용하지 않는다. 자식 후보의 동일성/충돌을 검증하고 한 가격의 파생 표시값은 명시 alias 근거 목록으로 묶는다. 단지 금액이 같다는 이유로 다른 조건을 합치지 않는다.

### 가격 조건 fingerprint

condition_json의 필수 key는 condition_schema_version=1, price_scope(COMPARISON_AGGREGATE/SELLER_ITEM/UNCLASSIFIED), discount, membership, coupon, delivery, installation, tax, option, unresolved_evidence_key다. 일곱 조건은 각각 `{state, value}` 객체이며 모든 key를 명시한다. state는 CONFIRMED/UNKNOWN/NOT_PROVIDED/NOT_APPLICABLE/CONFLICT다. 미제공과 의미 미확인을 하나의 NULL 상태로 합치지 않는다.

value는 아래 필수 key를 가진 객체다. 미확인 하위 값은 명시 NULL로 두고 조건 전체를 CONFIRMED로 표시하지 않는다. NOT_APPLICABLE/NOT_PROVIDED는 value=NULL이다. 부분적으로 알려진 조건은 UNKNOWN 또는 CONFLICT와 확보한 value를 보존한다.

| 조건 | value key |
|---|---|
| discount | kind, amount, currency, rate |
| membership | required, program_code |
| coupon | required, coupon_code, discount_amount, currency |
| delivery | included, fee_amount, currency, fee_basis |
| installation | included, fee_amount, currency, fee_basis |
| tax | included, rate, tax_kind |
| option | identity_status, option_key, option_evidence_key |

금액·비율은 부동소수점 대신 정규화 십진 문자열로 기록하며 `1000.0`과 `1000`은 `1000`으로 통일한다. bool은 true/false, 미확인은 NULL이다. NOT_APPLICABLE 하위 값은 업무상 적용 안 됨이 확인된 경우에만 NULL로 허용한다. 예를 들어 비회원 조건이 명시된 경우 membership.required=false, program_code=NULL이 가능하다. 정보가 없는 것을 false로 쓰지 않는다. 무료배송이 명시된 fee_amount=0은 유효 배송 조건이며 상품 가격 0과 구분한다.

일곱 조건 중 UNKNOWN/NOT_PROVIDED/CONFLICT가 있으면 unresolved_evidence_key=source_evidence_key, 모두 확인/적용 제외이면 NULL이다. 이렇게 미확정 조건의 서로 다른 원천이 모두 UNKNOWN이라는 이유로 동일 조건으로 병합되지 않게 한다. 원시 문구와 근거 경로는 evidence_json에 보존하고 진단 시각은 의미 조건에 넣지 않는다. `condition_fingerprint=H(condition_json을 해석한 객체)`다. price_kind·amount·currency·observed_at은 가격 관측의 별도 의미 필드이며 조건이 같다고 다른 가격 종류를 합치지 않는다.

offer_snapshot_key는 현재 Silver 필드가 아니라 **Gold 가격 이력 단계의 생성 키**다. 기존 snapshot-v1 계약의 가격/조건 근거 fingerprint는 H({source_evidence_key, price_kind, price_basis, amount, currency, condition_fingerprint, observed_at, option_evidence_key})로 구체화한다. 여기에는 evaluation_key·이미지 업로드 상태를 넣지 않는다. Gold가 offer_key·raw_document_id·이 fingerprint로 원천 증거 snapshot 키를 만들고, price-effect-v2 이행 여부는 별도 서비스 계약으로 결정한다.

## 4. 공통 필드·타입

타입 S=string, TS=UTC timestamp, L=long, D=decimal(20,4), B=boolean이다. JSON 필드는 canonical string이며 일반 텍스트와 구분한다. 단위·수치·시각은 별도 열에도 저장한다.

모든 Silver 결과에 `evaluation_key:S`, `raw_document_id:S`, `document_hash:S`, `observation_key:S`, `source_code:S`, `source_product_id:S`, `product_key:S`, `collection_run_id:S`, `processing_input_fingerprint:S`, `normalizer_version:S`, `quality_policy_version:S`, `processing_run_id:S`, `schema_version:S`, `processed_at:TS`, `record_hash:S`를 둔다. raw_document_id는 24자리 ObjectId 문자열, processing_run_id는 하이픈 포함 UUID 문자열이다. 이 논리 참조를 Delta FK라고 하지 않는다.

각 테이블의 값·출처 상태에는 `observed_at:TS?`, `time_status:S`, `field_status_json:S`, `evidence_json:S`를 둔다. 필드마다 시각이 다르면 evidence 안의 value-level 시각이 기준이며 상품 wrapper 시각으로 대체하지 않는다. 상품의 wrapper 시각은 `raw_collected_at:TS`로 별도 보존한다.

| 테이블 | 추가 필드와 타입 |
|---|---|
| product_observations | display_name/normalized_name/manufacturer/brand/model:S?, source_category_path_json/collection_category_context_json:S, service_category_id/service_category_code/mapping_version:S?, mapping_status:S, registered_raw/registered_status:S?, registered_year/registered_month:L?, provider_make_raw/provider_make_status:S?, width_mm/depth_mm/height_mm:D?, dimension_status:S |
| product_attributes | attribute_key/source_evidence_key/attribute_name/value_type:S, raw_value_json:S, value_text:S?, value_decimal/value_min/value_max:D?, codes:array<string>, unit/measurement_target/component/option_key/option_evidence_key:S?, option_status/attribute_status:S |
| price_observations | price_observation_key/source_evidence_key/price_kind/price_basis/condition_status:S, amount:D?, currency/currency_evidence/price_text/offer_key/option_key/option_evidence_key:S?, condition_fingerprint/condition_json:S, numeric_status/option_status:S |
| product_images | image_relation_key/image_slot/source_version/source_url/role/origin:S, display_order:L, observed_url/provider_image_version/image_checksum/bucket/object_key:S?, asset_ref_json:S, bytes/width/height:L?, format/extension:S?, acquisition_status/availability/validation_status:S, image_state_revision:L, image_state_fingerprint:S, image_state_checked_at:TS?, review_snapshot_hash/usage_scope_json/review_status:S |
| link_observations | link_observation_key/source_evidence_key/source_url/link_kind/structure_status/link_status:S, comparison_url/offer_key/option_evidence_key:S?, checked_at/expires_at:TS?, validation_snapshot_hash:S?, validation_evidence_json:S |
| quality_issues | issue_key/target_table/target_key/field_name/issue_code/severity/blocking_scope:S, raw_value_json/evidence_json:S, reason:S |
| normalized_offers | normalized_offer_key/offer_key/source_seller_id/source_offer_id/option_key/identity_rule_version/identity_evidence_json:S, price_observation_keys:array<string>, link_observation_keys:array<string> |

nullable는 미확인 값에 한정한다. 키·원천·평가/hash·처리 버전은 nullable로 회피하지 않는다. 정상 값 0·빈 문자열·임의 UUID로 결측을 대체하지 않는다. decimal 한도 초과는 원문+VALUE_OUT_OF_RANGE로 남긴다. 서비스 currency 정책이 없으면 비KRW 반올림/환산을 하지 않는다. KRW는 소수부 0인 양의 정수만 유효하다.

normalized_offers는 안정 판매 항목의 식별만 한 행으로 묶는다. 일반가·회원가·배송/옵션 조건별 모든 가격은 각 price_observation_key로 보존하고 이 부모의 정렬된 참조 배열로 연결한다. 최저가 하나를 Silver 부모에 선택하지 않는다. Gold가 가격 종류·조건·옵션·유효기간을 지정해 현재 Offer를 구성한다. attribute의 dimension_raw/color_raw/material_raw/shape_raw는 raw_value_json, color_codes/material_codes는 codes, 단일 shape_code는 확인된 codes[0]으로 대응한다. 속성 상태는 attribute_status, 상품 전체 치수의 사용 가능 상태는 dimension_status로 구분한다.

record_hash는 키와 의미 필드·값별 근거/시각·사용한 입력 버전으로 계산한다. processing_run_id·재시도 execution_id·processed_at·진단 문구는 의미 hash에서 제외한다. 최초 결과에 연결한 processing_run_id/processed_at은 재실행 시 유지하며 새 실행은 기존 evaluation 참조로 로그에 연결한다. 같은 키의 의미가 다르면 기존 행을 UPDATE하지 않고 충돌로 중단한다. receipt의 completed_at도 최초 확정값을 유지하고 의미 hash에서 제외한다.

pipeline_evaluations에는 evaluation_key/stage/raw_document_id/document_hash/processing_input_fingerprint, 고정 Bronze version, side-input 불변 참조와 hash, output_manifest_json(테이블별 키·record_hash·Delta version·건수), status(OUTPUT_COMMITTED/QUALITY_WAIT), quality_reason_codes, processing_run_id, completed_at을 둔다. 미래의 임의 테이블 version을 추정해 채우지 않는다.

## 5. 값 상태와 품질

| 영역 | 상태와 해석 |
|---|---|
| 필드 | CONFIRMED, NOT_PROVIDED, EXTRACTION_FAILED, CONFLICT, SEMANTICS_UNKNOWN, INFERRED, NOT_APPLICABLE |
| 시각 | OBSERVED, UNKNOWN. 원천 시각이 없으면 적재 시각을 쓰지 않음 |
| 옵션 | IDENTIFIED, NO_OPTION_CONFIRMED, UNKNOWN, CONFLICT. 옵션 미수집을 NO_OPTION으로 바꾸지 않음 |
| 매핑 | MAPPED, UNMAPPED, AMBIGUOUS, EXCLUDED, NOT_PROVIDED |
| 치수 | CONFIRMED, AXIS_UNKNOWN, TARGET_UNKNOWN, OPTION_UNVERIFIED, RANGE, CONFLICT, NOT_PROVIDED |
| 구조/현재 링크 | structure=VALID/INVALID/UNKNOWN; current=VALID/INVALID/EXPIRED/NOT_CHECKED/CONFLICT |
| 파일 | acquisition=VERIFIED/UNAVAILABLE; availability=UNCONFIRMED/AVAILABLE/FAILED/MISSING; validation 상태는 근거 freshness와 hash 대조 결과 |
| 검수 | 기존 scope별 PENDING/APPROVED/DENIED/EXPIRED/REVOKED/CONFLICT의 입력 의미를 보존. ETL이 승인을 만들지 않음 |

CONFIRMED는 그 필드의 근거 확인이며 상품 추천·이미지 외부 전송 승인과 다르다. 유효한 원문에 값이 없음(NOT_PROVIDED), 상세 실패로 확보 못함(EXTRACTION_FAILED), 추출은 됐으나 의미 불명(SEMANTICS_UNKNOWN)을 분리한다.

품질 코드 최소 목록: SOURCE_PRODUCT_ID_CONFLICT, NAME_MISSING/CONFLICT, CATEGORY_UNMAPPED/AMBIGUOUS/EXCLUDED, PRICE_PARSE_FAILED/VALUE_CONFLICT/INVALID_ZERO/INVALID_NEGATIVE, CURRENCY_UNVERIFIED, PRICE_BASIS_UNKNOWN, OFFER_ID_UNVERIFIED, OPTION_UNVERIFIED, DIMENSION_AXIS_UNKNOWN/TARGET_UNKNOWN/CONFLICT, ATTRIBUTE_UNMAPPED, IMAGE_STATE_CONFLICT/UNCONFIRMED/MISSING, REVIEW_PENDING/EXPIRED/REVOKED, LINK_NOT_CHECKED/INVALID/EXPIRED, REGISTERED_MONTH_AMBIGUOUS, FIELD_TIME_UNKNOWN.

severity는 ERROR/WARNING/INFO, blocking_scope는 FIELD/OFFER/CATALOG/RECOMMENDATION/COMPARISON/PURCHASE로 구분한다. 가격·치수 결측만으로 전체 추천 제외하지 않는다. 요청의 필수 조건별 MATCH/DIFFERENT/UNKNOWN 판단은 추천 담당이 수행한다. 인프라 실패는 품질 결측으로 성공 처리하지 않는다.

## 6. 추출 우선순위·수치 규칙

같은 상품/옵션에 대응한 유효 상세 명시값 → 유효 API 구조값 → HTML 목록 명시값 순서로 후보를 검토한다. 무조건 마지막 관측을 선택하지 않는다. 우선순위·대상·유효 시각이 같은데 값이 다르면 충돌이며 확인값 투영을 보류한다. 원문과 모든 후보의 근거는 보존한다.

목록 `price.min`은 일반 Offer가 아닌 LISTING_LOWEST다. 표본의 action_product/comparison_lowest는 제공처 min과 표시값이 같은 원천에서 파생됐음을 어댑터 규칙으로 대조할 수 있다. 명시 원화 기호가 있는 대응 표시값/확인된 currency 계약을 근거로만 KRW를 둔다. API 숫자만 존재하고 통화 근거가 없으면 NULL+CURRENCY_UNVERIFIED다. min과 member/total의 숫자가 같아도 price_kind를 합치지 않는다. 할인·배송·설치 조건은 각 원문에 확인한 값만 채운다.

상세 seller_id와 bridge의 link_pcode는 identity 후보일 뿐이다. 담당의 안정성·유일성 규칙이 없으면 source_offer_id를 확정하지 않고 OFFER_ID_UNVERIFIED로 가격/링크 관측에 남긴다. JSON-LD aggregate Offer도 일반 판매처 Offer로 만들지 않는다.

치수는 mm=1, cm=10, m=1000의 정확한 Decimal 배수만 적용한다. 명시 축 이름과 옵션·대상을 함께 확인해야 width/depth/height 투영이 가능하다. 가로×세로의 세로를 임의로 depth/height로 해석하지 않는다. 좌판·포장·매트리스·제품 전체를 구분하고 범위·조절식은 범위로 보존한다. 색상·재질·형태는 부위·복수 옵션을 유지하며 승인된 사전이 없으면 원문/미매핑 상태로 보존한다.

## 7. 멱등 저장·재평가

1. 단일 Silver Driver가 고정 Bronze 입력과 해당 관측의 Mongo 상태/의존 snapshot을 작은 배치로 읽는다. 원천은 불변 hash로 검증한다.
2. 의미 입력 fingerprint와 evaluation_key를 계산한다. receipt가 있어도 참조 출력·hash·version을 대조한 뒤에만 skip한다.
3. 순수 정규화가 6개 제안 테이블과 확인된 Offer 결과를 만든다. 이미지와 가격을 한 상품 행에 곱집합으로 펼치지 않는다.
4. 사용한 이미지 revision/fingerprint를 다시 읽어 동결 중 변경되지 않았는지 확인한다. 변경·같은 revision의 다른 hash는 성공 처리하지 않는다.
5. 테이블별 insert-only MERGE와 내용 대조를 수행하고 실제 Delta version·행 키·hash를 수집한다.
6. output_manifest를 가진 evaluation receipt를 마지막에 저장한다. 정상 출력은 OUTPUT_COMMITTED, 하위 준비 조건 미충족은 출력 보존 후 QUALITY_WAIT로 기록한다. 단순 선택 속성 결측은 전체 준비 실패 조건으로 확대하지 않는다.
7. 출력 commit 후 receipt 전 중단은 같은 평가를 재반영해 receipt를 복구한다. 테이블 전체·Mongo·파일의 분산 트랜잭션을 가정하지 않는다.

새 원천은 Bronze의 고정 처리 범위를 따라 발견한다. 상태만 바뀐 관측은 ingestion_observations 전체를 observation_key 순서의 작은 페이지로 순환 대조한다. 초기 배치 프로필은 5분 간격의 실행 기회이며 전체 완료시간 보장은 아니다. timestamp 필터만으로 발견하지 않는다. cursor/epoch는 별도 발견 metadata이며 평가 완료 증거가 아니다.

매핑·검수·링크·사전 변경은 해당 행의 관련 의미 투영 hash로 재평가한다. 무관한 전역 snapshot 시각만 바뀌면 모든 관측을 재평가하지 않는다. 승인/링크 만료는 effective status 전이로 입력 hash에 반영한다. 순회 위치보다 앞에서 바뀐 항목은 다음 순환에서 발견한다. 동일한 Silver Driver가 신규 입력과 재평가를 처리하며 독립 작성자를 추가하지 않는다.

영구 보존 원천의 유실·DB/객체 저장소 장애·권한 오류는 실행 실패/RETRY_WAIT와 운영 로그로 남기고 성공 receipt를 만들지 않는다. Silver는 ingestion 운영 vector를 직접 고치지 않는다. 파일 유실 발견은 입력 근거/품질 문제로 기록하고 업로드 복구 담당에 인계한다.

## 8. 검증 기준과 다음 구현

예정 실패 테스트: 상품번호 충돌; 상세 실패와 미제공 구분; 목록/회원/합계 가격 분리; 0/음수/소수 KRW/통화 미확인; 같은 원천 파생값 dedup; 안정 ID 없는 Offer 보류; 옵션이 다른 규격/가격 분리; 축 없는 치수·포장/좌판·범위; 2자리 등록 연도; 같은 파일의 여러 상품 연결; 동일 revision 다른 vector; DB 장애를 결측으로 처리하지 않기; 등록되지 않은 운영 run; 만료/철회; 출력 후 receipt 전 중단; 동일 입력 재실행; side-input 변경/무관 변경; 같은 시간 다른 값; 오래된 값으로 최신값 덮어쓰기 방지.

실제 표본 검증은 세 보관 상품으로 출처·시각·4/4/1 이미지 연결 및 가격 종류를 대조한다. 이 표본의 기대값을 전체 분류 규칙의 정답으로 일반화하지 않는다. 순수 정규화 테스트 → Delta 대역/장애 테스트 → 실제 로컬 Delta → 실제 서버 종단 순서로 인수한다.

구현 코드: `silver` 패키지의 inputs/source, values/normalize, schemas/delta, runner/checkpoint/cli로 책임을 분리했다. `run_silver.py`와 `danawa-silver`를 제공한다. 현재 등록 근거는 담당자 export를 검증하며 실제 MySQL 등록/실시간 확인은 추가 어댑터 영역이다. 재평가 발견은 고정 Bronze 전체를 observation_key 순서로 순환하여 Mongo 상태를 읽는다. 해당 Bronze version에 없는 원천은 새 Bronze version 범위에서 처리한다. 별도 timestamp 필터나 Silver의 원장 UPDATE를 추가하지 않았다. 세부 지원 형식과 검증 상태는 실행 문서·[서버 가이드](deployment.md)를 따른다.
