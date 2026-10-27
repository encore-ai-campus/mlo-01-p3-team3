# Gold → MySQL 게시 필드 계약

2026-10-19: [추천 서버 카탈로그 어댑터](../interior-recommender/server-catalog.md)를 코드로 준비했다. 완료 게시·현재 상품/이미지·이용 범위·MySQL 임베딩 참조와 고정 Delta export를 대조해 기존 추천 인덱스를 만들고 반환/클릭 시 현재 상태를 재검증한다. 등록안은 출력만 하며 DB 등록·인덱스 활성화·Gold 재평가·업무 데이터 게시와 실제 서버 배포는 별도다. 아래 기존 어댑터 미구현 설명은 이전 상태와 구분한다.

2026-10-18 설계 계약. 필수 컬럼·관계는 `publication/schema.py`의 `document-publication-v2`, `sql/publication_reference_v2.sql`, 게시 writer·계획·읽기 전용 audit에 구현했다. 기존 DB 확장은 별도 적용/롤백 SQL을 사용하며 [스키마 반영·인수 가이드](../../src/data_pipeline/docs/mysql_schema_rollout.md)를 따른다. 코드 구현·격리 검증과 실제 운영 DB 적용·업무 게시·추천 어댑터 인수는 구분한다.

[데이터 모델](03-data-model.md) · [상품 속성 사전](data-dictionary/02-products.md#gold-게시-필수-확장-컬럼) · [게시 실행](../../src/data_pipeline/docs/mysql_publication.md)

## 1. 기준과 책임

Gold는 정제된 상품·판매·이미지·링크 정보의 게시 기준이다. MySQL은 승인된 고정 Gold 버전을 서비스 조회용으로 반영한다. 모든 Gold 컬럼은 직접 컬럼, 정규화된 관계, 불변 게시 원문 참조, 미게시와 사유 중 하나로 대응해야 한다. 임의 누락은 허용하지 않는다.

같은 게시 버전·관측 범위에서 값·단위·확인 상태·출처가 일치해야 한다. 최신 Gold와 아직 게시되지 않은 MySQL을 단순 비교하지 않는다. 상품 활성·중복 검수, 이미지 활성·이용 검수, 자산 이용 정책은 서비스 담당 원장이다. Gold 준비 상태로 이를 덮어쓰거나 승인하지 않는다.

## 2. DDL 작성 시 반드시 포함할 필드

기존 상품·판매·자산·이미지·게시 원장 컬럼 및 UUID/FK/업무 키 UNIQUE는 유지한다. 아래는 그 위에 필요한 확장이다. MySQL 8의 JSON을 사용하고, JSON 내부 버전·상태·출처를 함께 검사한다. 시각은 UTC `DATETIME(3)`, UUID PK/FK는 `CHAR(32)`다. 이 표의 NULL은 자료 미제공을 뜻하며 확정값을 임의로 채우지 않는다.

### products

| 필수 컬럼 | MySQL 타입 / NULL | Gold 대응·의미 |
|---|---|---|
| `manufacturer` | `VARCHAR(255)` / 허용 | 제조사. 브랜드에 대입하지 않음 |
| `attributes_json` | `JSON` / 불허 | 원천 색상·재질·형태 등 속성 배열, 원문·옵션·부위·상태·근거를 손실 없이 보존. 없음은 `[]` |
| `field_status_json` | `JSON` / 불허 | Gold 필드별 확정·미확인·충돌 상태 |
| `field_lineage_json` | `JSON` / 불허 | 원천·관측 시각·정규화 근거. 기존 `field_versions_json`으로 대체하지 않음 |
| `source_refs_json` | `JSON` / 불허 | 상품 원천 참조 목록 |
| `price_krw` | `BIGINT` / 허용 | Gold 대표가격. 양의 정수·가격 종류·근거 확인 후 저장. 목록 가격으로 가상 Offer를 만들지 않음 |
| `first_collected_at` | `DATETIME(3)` / 허용 | 원천 최초 수집 시각. 서비스 `created_at`과 별개 |
| `catalog_ready` | `TINYINT(1)` / 불허 | Gold 카탈로그 준비 평가 |
| `recommendation_ready` | `TINYINT(1)` / 불허 | Gold 추천 준비 평가. 현재 서비스 이미지 활성·인덱스 상태와 추가 대조 필요 |
| `comparison_link_ready` | `TINYINT(1)` / 불허 | 비교 링크 준비 평가 |
| `purchase_link_ready` | `TINYINT(1)` / 불허 | 직접 구매 링크 준비 평가 |
| `quality_reason_codes` | `JSON` / 불허 | Gold 품질 사유 코드 배열 |
| `publication_status` | `VARCHAR(32)` / 불허 | Gold 상품 게시 평가 상태. MySQL 게시 실행 상태와 별개 |
| `publication_reason_codes` | `JSON` / 불허 | Gold 게시 평가 사유 코드 배열 |
| `representative_image_id` | `CHAR(32)` / 허용 | `product_images.id` FK. Gold `representative_image_key`를 동일 상품의 관계 UUID로 해석. 해석 실패를 다른 이미지로 임의 대체하지 않음 |
| `embedding_json` | `JSON` / 불허 | Gold 임베딩 메타데이터·참조 원문. 없음은 `null` JSON 값. 벡터 배열을 상품 컬럼에 새로 복제하지 않음 |
| `gold_snapshot_ref_json` | `JSON` / 불허 | 아래 공통 버전·원문 추적 계약 |

기존 `name`, `brand`, `model_name`, `category_id`, `width_mm`, `depth_mm`, `height_mm`, `comparison_url`, `product_key`, `source_code`, `source_product_id`도 필수다. `display_name→name`, `model→model_name`, `service_category_id→category_id`로 대응한다. `service_category_code`는 FK의 `categories.code`와 일치해야 한다. 제조사·브랜드·모델은 서로 다른 필드다.

`representative_image_id`는 같은 상품에 속하는 이미지인지 게시/서비스 검사로 확인한다. 상품→이미지의 순환 참조는 먼저 NULL 상품을 생성하고 이미지 연결 생성 후 같은 트랜잭션에서 갱신한다. FK는 `ON DELETE RESTRICT`로 이력을 보호하고 명시적 연결 해제·보존 절차를 거친다.

### product_images 및 링크 관측

| 테이블.필수 컬럼 | MySQL 타입 / NULL | 의미 |
|---|---|---|
| `product_images.image_relation_key` | `VARCHAR(191)` / 불허 | Gold 원천 이미지 관계 키. 기존 관계 업무 키에 더해 `(product_id,image_relation_key)` UNIQUE |
| `product_images.gold_recommendation_approved` | `TINYINT(1)` / 불허 | Gold 입력 시점 승인 평가. 기존 서비스 `active`와 구분 |
| `product_images.gold_review_snapshot_hash` | `CHAR(64)` / 허용 | Gold가 사용한 검수 snapshot hash |
| `product_images.source_evidence_json` | `JSON` / 불허 | Gold 이미지 `evidence_json` 원문 |
| `product_images.gold_snapshot_ref_json` | `JSON` / 불허 | 게시 버전과 원문 참조 |
| `product_link_observations.id` | `CHAR(32)` / 불허 | 신규 링크 관측 PK. 신규 행만 UUID4 발급 |
| `product_link_observations.product_id` | `CHAR(32)` / 불허 | `products.id` FK |
| `product_link_observations.link_observation_key` | `VARCHAR(191)` / 불허 | Gold 링크 관측 키 |
| `product_link_observations.link_kind` | `VARCHAR(32)` / 불허 | 비교/직접 구매 종류 |
| `product_link_observations.source_url` | `TEXT` / 불허 | 확인 대상 URL 원문 |
| `product_link_observations.offer_key` | `VARCHAR(191)` / 허용 | 판매 항목 논리 참조. 비교 링크는 NULL 가능 |
| `product_link_observations.usable` | `TINYINT(1)` / 불허 | 해당 Gold 평가 시점 사용 가능 여부 |
| `product_link_observations.raw_document_id` | `CHAR(24)` / 허용 | Mongo 원천 논리 참조. 저장소 간 FK 없음 |
| `product_link_observations.evidence_json` | `JSON` / 불허 | 확인 시각·만료·판정·원천 근거 원문 |
| `product_link_observations.gold_snapshot_ref_json` | `JSON` / 불허 | 게시 버전과 원문 참조 |
| `product_link_observations.created_at` | `DATETIME(3)` / 불허 | MySQL에 관측 평가를 보존한 시각 |

링크 관측은 `(product_id,link_observation_key,gold_evaluation_key)` UNIQUE를 적용한다. 이를 위해 `product_link_observations.gold_evaluation_key VARCHAR(191) NOT NULL`도 필수다. 같은 링크의 다른 평가는 새 행이며 동일 평가 재전달은 원문 hash를 대조해 재사용한다. 상품/판매 URL과 별도로 보존하고, 실제 클릭 시 현재 만료·허용 호스트·상품/판매 대응을 재검증한다.

### offers / offer_snapshots / 게시 원장

기존 판매 테이블의 필수 필드는 [상품·판매 사전](data-dictionary/02-products.md)을 따른다. 추가로 `offers.gold_snapshot_ref_json JSON NOT NULL`, `offer_snapshots.gold_snapshot_ref_json JSON NOT NULL`을 둔다. 현재 가격의 조건·종류·원천 근거 및 가격 이력의 `snapshot_key`, `price_effect_key`, `source_evidence_key`, 적격 상태는 고정 게시 원문으로 손실 없이 추적한다. 대표가격 `products.price_krw`를 판매처 가격과 혼용하지 않는다.

`pipeline_publication_staging`은 기존 `id`, `plan_id`, `product_key`, `body_hash`, `body_json`과 `(plan_id,product_key)` UNIQUE 및 게시 원장 FK를 반드시 포함한다. `body_json`은 게시 상품의 Gold 7개 데이터 테이블과 `pipeline_evaluations` receipt를 포함한 불변 원문이다. 서비스 컬럼에 직접 투영하지 않는 필드도 이 원문에 보존한다. 게시 원문은 서비스가 참조하는 동안 삭제하지 않으며 삭제 전 대체 보관 위치·hash·접근 가능성을 검증한다.

`pipeline_publications`의 `plan_id`, `plan_hash`, `gold_versions_json`, `status`, `product_count`, `completed_at`과 `pipeline_product_versions`의 `product_key`, `field_versions_json`, `first_collected_at`, `latest_as_of`, `latest_evaluation`을 유지한다. 기존 guard·staging·버전 테이블은 참조 DDL의 필수 PK/FK/고유 제약을 포함한다.

### 임베딩 및 검색 인덱스

`product_embeddings`와 `search_index_versions`는 [데이터 모델](03-data-model.md)의 기존 논리 설계대로 필수다. 전자는 `id`, `asset_id`, `image_checksum`, `model_version`, `preprocessing_version`, `dimension`, `vector_ref_json`, `status`, `created_at`을 포함한다. 후자는 `id`, `scope`, `model_version`, `preprocessing_version`, `dimension`, `manifest_json`, `status`, `activated_at`, `created_at`을 포함한다. 형식·의미는 [검색 속성 사전](data-dictionary/06-ad-serving.md)을 따른다.

임베딩 담당이 결과를 등록하고 인덱스 담당이 활성 버전을 관리한다. 게시기는 Gold `embedding_json`을 보존하되 이 원장에 임의 등록·활성화하지 않는다. 추천 어댑터는 자산 체크섬·모델·차원·전처리와 활성 인덱스 manifest의 상품/이미지/벡터 대응을 검사한다. 참조 불일치는 INDEX_NOT_READY로 처리한다.

## 3. 공통 버전·컬럼 대응의 완결성

각 `gold_snapshot_ref_json`은 `schema_version`, `plan_id`, `product_key`, `export_hash`, `gold_evaluation_key`, `input_fingerprint`, `gold_versions`, `source_rows`를 필수 키로 갖는다. `source_rows`는 `{table,row_key,record_hash}` 배열이며 고정 staging 원문의 정확한 행으로 연결한다. 저장된 source row의 `processing_run_id`, `processed_at`, `gold_policy_version`, `silver_versions_json`도 이 경로로 조회 가능해야 한다. 원문과 서비스 값을 하나의 평가로 가장하지 않고 필드별 유지값의 관측·출처도 보존한다.

| Gold 전체 필드 범위 | 필수 저장·참조 경로 |
|---|---|
| 모든 테이블 공통 `row_key`, `gold_evaluation_key`, `product_key`, `gold_policy_version`, `processing_run_id`, `processed_at`, `input_fingerprint`, `silver_versions_json`, `record_hash` | 공통 snapshot 참조 + 고정 staging 원문 |
| `product_catalog`의 모든 고유 컬럼 | products/카테고리·대표 이미지 관계 + 위 표의 상태·속성·출처·snapshot. 대표 이미지 원천 키는 원문 보존 |
| `current_offers`의 모든 고유 컬럼 | offers의 현재 값·조건 + snapshot 원문. `purchase_ready` 평가와 현재 URL 유효성은 구분 |
| `offer_history`의 모든 고유 컬럼 | offer_snapshots + snapshot 원문. 선택한 이력 정책에서 묶인 원천 행도 참조 목록으로 보존 |
| `product_images`의 모든 고유 컬럼 | assets의 위치·체크섬·파일 정보 + product_images 관계·검수 평가·근거·snapshot |
| `product_links`의 모든 고유 컬럼 | product_link_observations + 상품/판매의 현재 URL |
| `quality_issues`, `quality_daily`의 모든 컬럼 | 직접 서비스 품질 테이블은 미게시. 고정 staging 원문과 Gold 버전으로 운영 조회. 사용자 반응 집계로 변환하지 않음 |
| `pipeline_evaluations`의 모든 컬럼 | staging의 receipt·고정 Gold 참조. MySQL 게시 완료 상태와 구분 |

이 범위 표와 실제 `gold/storage.py`의 컬럼 집합을 대조해 미분류 컬럼 0개를 인수 조건으로 삼는다. Gold 컬럼 추가 시 이 계약·속성 사전·게시 코드·스키마 검사도 함께 갱신한다.

현재 계획에 포함되지 못한 상품은 서비스/staging에 게시됐다고 간주하지 않는다. 게시 상품 안에서도 미승인 이미지·부적격 가격 이력처럼 서비스 투영에서 제외한 행은 고정 staging 원문으로만 보존하고 제외 사유를 추적한다. 전체 Gold 보존을 이유로 서비스 승인 조건을 완화하지 않는다.

## 4. 갱신과 추천 연결

- 고정 export와 담당 guard를 확인하고 하나의 게시 트랜잭션에서 서비스 필드를 반영한다. 과거 관측의 최신값 덮어쓰기, 같은 시각의 다른 값, NULL 삭제, 시각 없는 교체는 기존 보호 규칙을 유지한다.
- 전체 JSON snapshot 갱신과 기존 확인값 유지가 충돌하면 조용히 새 출처만 붙이지 않는다. 필드별 유지 사유·실제 출처를 기록하거나 게시를 보류한다. 명시적 철회·정정·삭제는 승인된 별도 정책으로 처리한다.
- 기존 UUID와 소유권을 보존한다. 새 이미지 연결은 inactive이며 담당의 활성화가 필요하다. 같은 배치에 없다는 이유로 삭제하지 않는다.
- MySQL→추천 어댑터는 완료된 게시 버전의 상품·옵션·속성 상태·규격·링크·이미지와 호환 벡터를 읽어 `ProductIndex` 입력을 구성한다. 서비스 활성·이용 검수·인덱스 현재 상태도 대조한다. 데이터 확보를 위한 사용자 추가 절차는 만들지 않는다.
- 속성 JSON의 모든 값을 확정 색상·재질로 승격하지 않는다. 옵션·부위·상태를 해석하고 미지원/결측은 UNKNOWN으로 전달한다. 방 맥락·추천 점수·노출·클릭은 별도 서비스 원장이며 상품 ETL이 쓰지 않는다.

## 5. 구현 차이와 인수 기준

현재 v2 게시기는 상품 확장 필드·링크 관측·Gold 원문 참조를 기록하고, 임베딩/인덱스 테이블을 포함한 필수 구조를 검사한다. 임베딩 결과 등록·인덱스 활성화·추천 어댑터 연결은 별도 담당 범위다. 추천 로컬 구현은 `application/direct.py`에서 파일 카탈로그를 읽는다. 2026-10-18 서버 검증 기록의 MySQL 업무 테이블 부재와 시험 스키마 검증을 운영 서비스 구축 완료로 해석하지 않는다.

DDL 작성 시 기존 참조 스키마 전체 + 본 문서의 필수 확장 + 임베딩/인덱스 원장을 함께 대조한다. 이후 ORM/migration, `schema.py`와 참조 SQL, preflight, plan의 계약 버전/hash, runner, 추천 어댑터를 함께 수정한다. 문서만으로 DB에 적용하지 않는다.

인수는 미분류 Gold 컬럼 0개, 필수 MySQL 컬럼/타입/NULL/PK/FK/UNIQUE 일치, 동일 게시 범위의 값·상태·근거·관계 대조, 대표 이미지의 동일 상품 확인, 벡터 버전 호환, 오래된 값·충돌·NULL 보호, 재게시 무중복, 부분 실패 원자성, 현재 검수 소유권 보존, 실제 서비스 조회 성공을 포함한다. 검증을 통과하기 전에는 전체 통합 완료로 표시하지 않는다.
