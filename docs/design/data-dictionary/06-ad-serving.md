# 광고와 추천 속성 사전

선택 물체에서 상품 검색·실제 노출·반응 집계까지 연결하는 속성을 설명한다. UUID PK와 FK는 기존 공통 규칙을 따르며 상세 NULL·삭제 정책은 DDL에서 확정한다. placements는 이전 실제 상품 배치안의 이력으로 초기 신규 흐름에서는 생성하지 않는다.

[사전 안내](index.md) · [광고 서버](../07-ad-server.md) · [데이터 모델](../03-data-model.md) · [전체 ERD](../03-erd-full.md)

## 엔터티 바로가기

- [scene_objects](#scene_objects): 공간 구성 결과의 선택 물체
- [product_embeddings](#product_embeddings): 상품 이미지 벡터 처리 기록
- [search_index_versions](#search_index_versions): 검색 인덱스 배포 버전
- [recommendation_impressions](#recommendation_impressions): 실제 상품 노출
- [ad_metrics_daily](#ad_metrics_daily): 일자별 상품 노출과 반응 집계

## scene_objects

공간 구성 결과의 선택 물체를 관리한다.

### scene_objects id

공간 구성 결과의 물체를 식별하는 UUID.

- 형식: UUID (Django UUIDField, MySQL CHAR(32))
- 예시: `716daa81-b283-42df-a8f1-62aac7c67873`
- 해석: 최초 생성 후 보존한다. 품목·영역 수정은 새 물체 버전으로 기록한다.

### scene_objects project_id

이 물체가 속한 탐색 공간.

- 형식: UUID (Django UUIDField, MySQL CHAR(32))
- 예시: `d16156e5-0634-4463-a39a-37b61109aabf`
- 해석: [projects.id](03-user-flow.md#projects-id)를 참조한다.

### scene_objects result_asset_id

템플릿 기반 공간 구성 결과 이미지.

- 형식: UUID (Django UUIDField, MySQL CHAR(32))
- 예시: `ee47322f-d90a-466a-830b-51d482224f9d`
- 해석: [assets.id](02-products.md#assets-id)를 참조한다. 실제 상품 이미지 합성 결과를 뜻하지 않는다.

### scene_objects crop_asset_id

선택 물체 영역을 잘라낸 검색용 이미지.

- 형식: UUID (Django UUIDField, MySQL CHAR(32))
- 예시: `ee47322f-d90a-466a-830b-51d482224f9d`
- 해석: [assets.id](02-products.md#assets-id)를 참조한다. 분석 중에는 NULL을 허용한다.

### scene_objects category_id

분석하거나 사용자가 확정한 물체 품목.

- 형식: UUID (Django UUIDField, MySQL CHAR(32))
- 예시: `ceb96b55-ee4d-46fa-9204-9db0c5f07a2d`
- 해석: [categories.id](02-products.md#categories-id)를 참조한다. 미확정이면 NULL이며 추천 전 품목을 검증한다.

### scene_objects bounding_box_json

구성 결과 이미지 기준 물체 영역.

- 형식: JSON
- 예시: `[0.2,0.5,0.7,0.9]`
- 해석: 0~1 정규화 좌표이며 실제 mm 규격이 아니다.

### scene_objects status

물체 분석과 검색 입력 준비 상태.

- 형식: 문자열
- 예시: `READY`
- 해석: PENDING·READY·FAILED 후보. 실패 이력은 jobs와 연결한다.

### scene_objects created_at

물체 버전 최초 등록 시각.

- 형식: UTC 날짜와 시각
- 예시: `2026-10-13T01:00:00Z`
- 해석: UTC로 저장하며 확정된 입력을 덮어쓰지 않는다.

## product_embeddings

상품 카탈로그 이미지 자산별 벡터 처리 기록을 관리한다. product_id는 두지 않고 [product_images](02-products.md#product_images)로 해당 이미지를 사용하는 모든 상품을 찾는다. 같은 입력 파일·모델·전처리·차원의 벡터를 공유한다.

### product_embeddings id

상품 이미지 벡터 레코드의 기본키.

- 형식: UUID (Django UUIDField, MySQL CHAR(32))
- 예시: `cb9f07ba-4062-4bf7-87d5-9645d00c6018`
- 해석: 신규 벡터 처리 레코드에 UUID4를 한 번 발급한다.

### product_embeddings asset_id

벡터로 변환한 상품 이미지.

- 형식: UUID (Django UUIDField, MySQL CHAR(32))
- 예시: `ee47322f-d90a-466a-830b-51d482224f9d`
- 해석: [assets.id](02-products.md#assets-id)를 참조한다.

### product_embeddings image_checksum

벡터 입력 이미지 내용의 체크섬.

- 형식: 문자열
- 예시: `sha256:abc123`
- 해석: 이미지 변경 시 기존 벡터를 재사용하지 않는다.

### product_embeddings model_version

임베딩 제공처와 실제 모델 버전.

- 형식: 문자열
- 예시: `gemini-embedding-2`
- 해석: 채택 모델은 Gemini Embedding 2다. 서로 다른 모델의 벡터는 섞어 비교하지 않는다.

### product_embeddings preprocessing_version

크롭·크기·정규화 처리 규칙 버전.

- 형식: 문자열
- 예시: `image-preprocess-v1`
- 해석: 입력 이미지와 검색 이미지에 호환되는 규칙을 적용한다.

### product_embeddings dimension

벡터의 원소 수.

- 형식: 정수
- 예시: `768`
- 해석: 초기 768 제안 차원이며 설정으로 관리한다. 인덱스와 질의 차원이 일치해야 한다.

### product_embeddings vector_ref_json

Delta Silver 벡터 행을 찾는 참조.

- 형식: JSON
- 예시: `{"table":"silver.product_image_vectors","version":12,"embedding_id":"cb9f07ba-4062-4bf7-87d5-9645d00c6018"}`
- 해석: 파일 본문 대신 테이블·버전·행 식별자를 보존한다. Delta FK는 아니다.

### product_embeddings status

벡터 생성 상태.

- 형식: 문자열
- 예시: `READY`
- 해석: PENDING·READY·FAILED 후보. 실패를 정상 벡터로 등록하지 않는다.

### product_embeddings created_at

벡터 레코드 등록 시각.

- 형식: UTC 날짜와 시각
- 예시: `2026-10-13T01:00:00Z`
- 해석: UTC이며 실제 처리 상태와 함께 추적한다.

## search_index_versions

검색 인덱스 배포 버전를 관리한다.

### search_index_versions id

배포된 검색 인덱스 버전의 기본키.

- 형식: UUID (Django UUIDField, MySQL CHAR(32))
- 예시: `1b021d93-0511-43fa-add8-4bc8115bb677`
- 해석: 재구축은 새 버전으로 등록한다.

### search_index_versions scope

인덱스의 자료 구분.

- 형식: 문자열
- 예시: `PRODUCT_SERVICE`
- 해석: 상품 추천·ABO 시험·템플릿 검색을 구분한다.

### search_index_versions model_version

인덱스 벡터의 임베딩 모델 버전.

- 형식: 문자열
- 예시: `gemini-embedding-2`
- 해석: product_embeddings와 질의 모델이 일치해야 한다.

### search_index_versions preprocessing_version

인덱스 입력 처리 규칙 버전.

- 형식: 문자열
- 예시: `image-preprocess-v1`
- 해석: 새 전처리는 새 인덱스로 구축한다.

### search_index_versions dimension

인덱스 벡터 차원.

- 형식: 정수
- 예시: `768`
- 해석: 모델과 질의 차원 일치를 검증한다.

### search_index_versions manifest_json

검색 파일·UUID 매핑·체크섬·입력 버전.

- 형식: JSON
- 예시: `{"index_key":"exports/search/v1/index.faiss","mapping_key":"exports/search/v1/mapping.json","gold_version":8,"checksum":"sha256:..."}`
- 해석: 버킷, 파일별 체크섬, Silver 벡터 버전, FAISS 버전과 거리 규칙도 포함한다. 상품 인덱스는 FAISS 정수 ID를 embedding UUID·asset UUID·checksum으로 변환하고, 고정 상품 연결 입력 버전과 현재 product_images를 대조해 상품 목록으로 확장한다. 템플릿 인덱스의 템플릿 UUID 매핑과 구분한다.

### search_index_versions status

구축과 배포 상태.

- 형식: 문자열
- 예시: `ACTIVE`
- 해석: BUILDING·READY·ACTIVE·RETIRED·FAILED 후보. scope별 활성 버전은 하나로 원자적 전환한다.

### search_index_versions activated_at

광고 서버에서 활성화한 UTC 시각.

- 형식: UTC 날짜와 시각
- 예시: `2026-10-13T01:00:00Z`
- 해석: 활성화 전에는 NULL이다.

### search_index_versions created_at

인덱스 구축 기록 생성 시각.

- 형식: UTC 날짜와 시각
- 예시: `2026-10-13T00:50:00Z`
- 해석: UTC로 저장한다.

## recommendation_impressions

실제 상품 노출를 관리한다.

### recommendation_impressions id

실제 상품 노출 한 건의 기본키.

- 형식: UUID (Django UUIDField, MySQL CHAR(32))
- 예시: `d764ea4f-a3f0-4446-b254-28e367329c16`
- 해석: 반응 이벤트의 impression_id가 참조한다.

### recommendation_impressions recommendation_item_id

화면에서 실제 보인 추천 항목.

- 형식: UUID (Django UUIDField, MySQL CHAR(32))
- 예시: `f8c188ca-66c0-4450-af4c-16d084d848ce`
- 해석: [recommendation_items.id](03-user-flow.md#recommendation_items-id)를 참조한다. 항목별 UNIQUE로 재진입·재전송 중복 노출을 막는다.

### recommendation_impressions category_id

노출 시점에 확정된 상품 품목.

- 형식: UUID (Django UUIDField, MySQL CHAR(32))
- 예시: `ceb96b55-ee4d-46fa-9204-9db0c5f07a2d`
- 해석: [categories.id](02-products.md#categories-id)를 참조한다. 이후 상품 분류 변경에도 과거 집계 분류를 보존한다.

### recommendation_impressions event_id

최초 노출 이벤트의 중복 방지 식별자.

- 형식: 문자열
- 예시: `911f5b4b-cf7a-4bdb-82d9-b8b2ef2d9360`
- 해석: user_events의 PRODUCT_IMPRESSION event_id와 동일하다. 재전송에서 유지한다.

### recommendation_impressions occurred_at

화면에 카드가 보인 시각.

- 형식: UTC 날짜와 시각
- 예시: `2026-10-13T01:05:00Z`
- 해석: 추천 응답 시각이 아니다. 집계 날짜는 검증된 이 시각의 UTC 일자를 사용한다.

### recommendation_impressions created_at

서버가 최초 노출을 등록한 시각.

- 형식: UTC 날짜와 시각
- 예시: `2026-10-13T01:05:01Z`
- 해석: UTC이며 최초 접수만 기록한다.

## ad_metrics_daily

일자별 상품 노출과 반응 집계를 관리한다.

### ad_metrics_daily id

상품·품목·일자별 반응 집계 행.

- 형식: UUID (Django UUIDField, MySQL CHAR(32))
- 예시: `16e1fe85-730b-4deb-ab11-8d1adcd0e27a`
- 해석: 같은 집계 키 재반영에서 id를 유지한다.

### ad_metrics_daily metric_date

노출 발생일 기준 집계 날짜.

- 형식: 날짜
- 예시: `2026-10-13`
- 해석: UTC 일자다. 반응 발생일 기준 통계와 구분한다.

### ad_metrics_daily product_id

집계 대상 상품.

- 형식: UUID (Django UUIDField, MySQL CHAR(32))
- 예시: `e98225a5-5312-48ac-b2c4-d92556705f8a`
- 해석: [products.id](02-products.md#products-id)를 참조한다.

### ad_metrics_daily category_id

노출 당시 품목.

- 형식: UUID (Django UUIDField, MySQL CHAR(32))
- 예시: `ceb96b55-ee4d-46fa-9204-9db0c5f07a2d`
- 해석: [categories.id](02-products.md#categories-id)를 참조한다. NULL을 허용하지 않는다.

### ad_metrics_daily impression_count

유효한 서로 다른 상품 노출 수.

- 형식: 정수 BIGINT
- 예시: `100`
- 해석: 이벤트 중복 제거와 노출 고유키 검증 후 계산한다.

### ad_metrics_daily selection_count

선택 반응이 연결된 서로 다른 노출 수.

- 형식: 정수 BIGINT
- 예시: `12`
- 해석: 같은 노출에서 반복 선택해도 이 지표는 1건이다. 원본 반응 이벤트 건수와 구분한다.

### ad_metrics_daily rejection_count

명시적 거절이 연결된 서로 다른 노출 수.

- 형식: 정수 BIGINT
- 예시: `3`
- 해석: 미선택을 거절로 간주하지 않는다.

### ad_metrics_daily link_click_count

판매 링크 클릭이 연결된 서로 다른 노출 수.

- 형식: 정수 BIGINT
- 예시: `8`
- 해석: 반복 클릭은 이 지표에서 1건이며 구매·매출이 아니다.

### ad_metrics_daily source_versions_json

집계를 계산한 입력과 로직 버전.

- 형식: JSON
- 예시: `{"silver_events":14,"gold_metrics":9,"aggregation_version":"ad-v1"}`
- 해석: 같은 입력의 재집계는 합계 덧셈 대신 결과값으로 Upsert한다.

### ad_metrics_daily computed_at

집계 계산 완료 시각.

- 형식: UTC 날짜와 시각
- 예시: `2026-10-13T01:10:00Z`
- 해석: 운영 화면에 최신성을 표시한다. MySQL 반영 완료는 publishing_runs로 추적한다.

## 기존 엔터티의 광고 연결 속성

### recommendation_runs scene_object_id

선택된 구성 결과 물체.

- 형식: UUID (Django UUIDField, MySQL CHAR(32))
- 예시: `716daa81-b283-42df-a8f1-62aac7c67873`
- 해석: [scene_objects.id](#scene_objects-id)를 참조한다. 신규 추천에서 필수이며 수정된 물체는 새 버전으로 연결한다.

### recommendation_runs index_version_id

검색에 실제 사용한 인덱스.

- 형식: UUID (Django UUIDField, MySQL CHAR(32))
- 예시: `1b021d93-0511-43fa-add8-4bc8115bb677`
- 해석: [search_index_versions.id](#search_index_versions-id)를 참조한다. 성공 전에는 NULL이며 재시도·비교에 사용한다.

### user_events impression_id

행동이 연결된 실제 노출.

- 형식: UUID (Django UUIDField, MySQL CHAR(32))
- 예시: `d764ea4f-a3f0-4446-b254-28e367329c16`
- 해석: [recommendation_impressions.id](#recommendation_impressions-id)를 참조한다. 상품 노출·반응에서 검증하며 비상품 행동은 NULL이다.

### ad_metrics_daily comparison_link_click_count

다나와 비교 페이지 이동이 한 번 이상 연결된 서로 다른 유효 노출 수.

- 형식: 정수
- 예시: `3`
- 해석: COMPARISON_LINK_CLICKED 기준. PURCHASE_LINK_CLICKED의 link_click_count와 별도이며 반복 클릭·재전달은 노출당 한 번 집계한다. 구매 완료가 아니다.
