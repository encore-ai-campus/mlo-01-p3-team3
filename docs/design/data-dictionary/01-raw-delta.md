# 원천과 Delta 속성 사전

2026-10-15: 원천·상품·링크의 공통 필드는 유지한다. 신규 추천 연결·크기 추정·오프라인 특징은 [직접 추천 속성](09-direct-recommendation.md)을 따른다. 생성/템플릿을 전제로 한 예시는 이전 이력이며 AI 추정으로 상품 실측·가격·링크를 덮어쓰지 않는다.

현재 논리 설계의 각 속성에 무엇을 저장하는지 설명한다. 형식·값·상태 코드는 후보이며, 예시는 테스트 설명용으로 실제 계정이나 상품을 뜻하지 않는다.

[사전 안내](index.md) · [데이터 모델](../03-data-model.md) · [전체 ERD](../03-erd-full.md)

## 엔터티 바로가기

- [raw_products](#raw_products): 제공처에서 수집한 원천 문서
- [실버·골드 필드 계약](#실버골드-필드-계약-적용-범위): 입력 매핑·관측 행·현재값 선택·준비 상태
- [실버 테이블](#실버-논리-테이블과-행-기준) · [골드 테이블](#골드-논리-테이블과-활용) · [준비 상태](#추천과-링크-준비-상태)

## raw_products

원천 상품·판매 관측을 불변으로 보존하는 설계다. 실제 출처·원천 상품번호·관측 시각을 유지하며 연결 사이트의 이름으로 변경하지 않는다. 서비스 연결은 [상품 연결 계약](../09-product-links.md)을 따른다.

이 엔터티는 제공처에서 수집한 원천 문서에 해당한다. 같은 이름의 속성이라도 아래 업무 의미를 기준으로 해석한다.

### raw_products _id

MongoDB가 발급한 원천 문서의 식별자.

- 형식 후보: MongoDB ObjectId
- 예시: `507f1f77bcf86cd799439011`
- 해석: 한 번 수집해 보존한 문서 한 건을 식별한다. 같은 상품의 반복 수집은 다른 ID를 갖는다.

### raw_products source_code

수집한 제공처를 구분하는 코드.

- 형식 후보: 문자열
- 예시: `danawa`
- 해석: 판매처 이름의 화면 표현과 분리하고 재수집에서도 같은 코드를 사용한다.

### raw_products source_product_id

제공처가 상품에 부여한 식별자.

- 형식 후보: 문자열
- 예시: `19170035`
- 해석: 프로젝트 product_key와 다르다. 제공처에서 확보하지 못하면 NULL을 허용하는 후보이다.

### raw_products collection_run_id

이번 문서를 확보한 수집 실행 식별자.

- 형식 후보: 문자열
- 예시: `collect-20261012-001`
- 해석: 같은 수집 묶음의 건수 대조와 미발행 원천 복구에 사용한다.

### raw_products collected_at

제공처 응답을 수집한 시각.

- 형식 후보: UTC 날짜와 시각
- 예시: `2026-10-12T01:00:00Z`
- 해석: DB에 등록한 시각과 다를 수 있다. 가격 정보의 최신성을 판단하는 기준이다.

### raw_products source_url

원천을 가져온 제공처 주소. 최초 내보내기에 주소가 없으면 NULL로 두며 판매 URL로 임의 대체하지 않는다.

- 형식 후보: 문자열
- 예시: `https://example.com/products/19170035`
- 해석: 상품 페이지 또는 API 원천 주소다. 실제 구매 주소와 반드시 같지는 않다.

### raw_products payload

제공처 응답 내용을 원본 표현으로 보존한 본문.

- 형식 후보: 문서 또는 원문 문자열
- 예시: `{"title":"의자 A","price":"89,000원"}`
- 해석: 정규화한 가격이나 AI 결과로 덮어쓰지 않는다. 문서 또는 원문 문자열로 보존한다.

### raw_products raw_object_key

정확한 원본 응답 파일을 따로 저장한 객체 키.

- 형식 후보: 문자열
- 예시: `raw/danawa/19170035/response-001.json`
- 해석: 큰 응답이나 응답 바이트 보존이 필요한 경우 사용한다. 버킷과 저장 영역은 구현 시 지정하며 없으면 NULL이다.

### raw_products image_refs

원본 이미지 주소와 확보한 파일의 참조 목록.

- 형식 후보: 배열
- 예시: `[{"source_url":"https://example.com/a.jpg","object_key":"products/19170035/1.jpg"}]`
- 해석: 배열 값이며 이미지 파일 본문을 넣지 않는다. 확보 상태와 참조 형식은 수집 계약으로 정한다.

### raw_products payload_hash

정해진 방식으로 계산한 원천 본문 해시.

- 형식 후보: 문자열
- 예시: `sha256:abc123...`
- 해석: 내용 변경 감지에 사용한다. 해시 방식과 정렬·인코딩 규칙을 고정한다. 상품 동일성의 보장은 아니다.

### raw_products collector_version

수집 코드나 설정의 버전.

- 형식 후보: 문자열
- 예시: `collector-v1`
- 해석: 어떤 로직으로 원천을 확보했는지 재현하기 위해 기록한다.

### raw_products schema_version

원천 문서 포장 구조의 버전.

- 형식 후보: 문자열
- 예시: `1`
- 해석: collector_version과 다르다. payload 외의 필드 배치나 image_refs 계약이 바뀌는 경우 갱신한다.

## Delta 계층 필드

테이블별 전체 DDL은 미정이다. 아래는 데이터 모델 본문에 명시된 추적 필드의 의미이며, 상품·가격의 표준 필드는 상품 사전과 같은 규칙을 따른다.

### bronze raw_document_id

복제한 MongoDB 원천의 ID을 저장한다.

- 예시: `507f1f77bcf86cd799439011`
- 해석: 원천까지 재조회할 수 있게 남기는 참조이며 외래키가 아니다.

### bronze payload_json

원천 payload의 내용을 보존한 처리 입력을 저장한다.

- 예시: `{"price":"89,000원"}`
- 해석: 표준 가격으로 덮어쓰지 않는다. 원본 바이트 보존은 별도 원천 파일을 사용한다.

### bronze collected_at

제공처에서 수집한 시각을 저장한다.

- 예시: `2026-10-12T01:00:00Z`
- 해석: MongoDB 수집 시각과 일치하게 복사한다.

### bronze ingested_at

Bronze 테이블에 적재한 시각을 저장한다.

- 예시: `2026-10-12T01:05:00Z`
- 해석: 수집 시각과 비교해 적재 지연을 확인한다.

### silver offer_key

정규화할 판매 항목의 안정적인 키을 저장한다.

- 예시: `danawa:19170035:sample-mall:sample-offer:UNSPECIFIED`
- 해석: 검증된 판매처·판매 항목·옵션을 구분하며 MySQL offers.offer_key와 같은 규칙을 사용한다. 예시는 가상 값이다. 식별이 불가능한 목록 가격은 Offer 키를 만들지 않는다.

### silver normalizer_version

적용한 정규화 규칙의 버전을 저장한다.

- 예시: `normalizer-v1`
- 해석: 가격·단위·분류 변환 로직이 바뀌면 재처리 범위를 확인한다.

### silver processing_run_id

이 결과를 만든 가공 실행의 식별자을 저장한다.

- 예시: `e3835573-86c9-4138-b036-b95ce2bb3aa3`
- 해석: MySQL pipeline_runs.id의 UUID를 하이픈 포함 문자열로 보존한 논리 참조이며 Delta FK가 아니다. 재처리·이관에서도 원래 실행의 참조를 유지한다.

### gold product_key

출처와 원천 상품 식별자에 기반한 안정 업무 키를 저장한다. 옵션 연결은 따로 보존한다.

- 예시: `danawa:19170035`
- 해석: MySQL products.product_key와 같은 매핑을 사용하고 기존 UUID를 재사용한다. 다른 옵션 속성을 합치거나 변환마다 키를 새로 발급하지 않는다.

### gold offer_key

통합 상품에 연결한 판매 항목 키을 저장한다.

- 예시: `danawa:19170035:sample-mall:sample-offer:UNSPECIFIED`
- 해석: MySQL offers 반영과 동일 판매처 구분의 기준이다.

### gold eligible

서비스 추천 대상으로 사용할 수 있는지을 저장한다.

- 예시: `true`
- 해석: 초기 요약의 추천 가능 여부다. 상세 계약의 상품·추천·비교 링크·구매 링크 준비를 하나의 값으로 대체하지 않으며, 모델 정확도 점수도 아니다.

### raw_products record_type

PRODUCT 또는 OFFER. 실제 원천 종류를 명시한다.

- 형식: 문자열

### raw_products source_record_key

제공처 상품 ID 또는 검증된 판매 항목 키. collection_run_id·응답 hash와 재시작 중복을 막는다.

- 형식: 문자열

### raw_products parser_version

페이지 추출 규칙 버전. 원천 시각을 덮어쓰지 않는다.

- 형식: 문자열

### raw_products response_body_hash

원본 응답 바이트 SHA256. 추출 payload_hash와 다르다.

- 형식: 문자열

## 실버·골드 필드 계약 적용 범위

2026-10-16 실버·골드 필드 제안을 현행 논리 설계에 반영했다. 아래 테이블·필드 이름은 구현 계약이며, 물리 Delta 경로·DDL·enum·복합 키의 최종 확정과 실제 배포는 별도 작업이다. 기존 원문과 상품 UUID를 유지한다. 이 절은 현재 서비스 카탈로그에 적용하며 위의 초기 Delta 필드 요약보다 상세한 기준이다.

원천 보존과 실행 흐름은 [파이프라인](../02-data-pipeline.md#실버골드-필드-적용), 서비스 원장 대응은 [데이터 모델](../03-data-model.md#실버골드와-서비스-원장-대응), 검증은 [운영 검수](../06-operations-migration.md#실버골드-필드-검수)를 따른다.

## 입력과 적용 기준

입력은 API 원본, 수집 관리, 상세 추출, 다운로드 결과로 구분한다. 아래 경로는 전달받은 필드 제안을 처리 계약으로 반영한 것이며, 실제 수집 파일의 값 확보율이나 Bronze 적재 성공을 검증한 결과가 아니다. 수집기별 원문 포장 경로는 표본으로 대조하고, 원문·추출값·파일 검증 결과를 서로 대신하지 않는다.

| 입력 구분 | 대표 필드 | 실버에서의 역할 |
|---|---|---|
| API 원본 | `productCode`, `name.*`, `category.*`, `price.*`, `image.*`, `dates.*` | 상품 식별·목록 정보의 근거 |
| 수집 관리 | `source`, `job_id`, `collected_at`, hash, 버전, `provenance`, `field_status` | 관측 시점·원천 추적·결측 사유 |
| 추출 결과 | `product_id`, `name`, `listing_prices`, `specifications`, `size_text`, `images`, `offers`, `product_url` | 원본과의 대응 검증 및 상세 정보 보강 |
| 다운로드 결과 | `local_path`, `sha256`, `bytes`, 해상도, `status`, `downloaded_at`, `error` | 실제 이미지 파일 검증과 저장소 연결 |

`product_id`는 수집 출력에서는 다나와 상품번호지만, 서비스에서는 내부 UUID를 가리킬 수 있다. 실버 입력에서는 `source_product_id`로 이름을 구분하고 내부 상품 ID와 혼용하지 않는다.

## 실버 입력 필드와 정제 규칙

### 상품 식별과 분류

| 입력 | 논리 출력 필드 | 정제 및 사용 규칙 |
|---|---|---|
| `source`, `productCode` 또는 추출 `product_id` | `source_code`, `source_product_id`, `product_key` | 출처와 문자열 상품번호로 안정 키 구성. 예: `danawa:96056663`. 원본·추출 상품번호 충돌 시 품질 이슈로 분리 |
| `name.full`, `name.product`, 추출 `name` | `display_name`, `normalized_name` | 공백·문자 형식 정리. 상세에서 동일 상품임이 확인된 이름을 우선하고 선택 근거 보존 |
| `name.maker`, `name.brand`, `name.model` | `manufacturer`, `brand`, `model` | 빈 문자열은 NULL. 별칭 사전으로 표기 통일. 제조사를 브랜드에 임의 대입하지 않음 |
| `category.codes`, `category.names` | `source_category_path`, `category_code`, `mapping_status`, `mapping_version` | 원천 분류 경로를 보존하고 매핑표로 서비스 품목에 연결. 미매핑·제외 품목 구분 |
| 추출 `categories[]` | `collection_category_context` | 수집 설정·페이지 관측 분류로 별도 보존. API 카테고리와 같은 값으로 취급하지 않음 |

같은 상품의 반복 수집은 관측 이력으로 유지한다. 이름이나 이미지가 비슷하다는 이유로 다른 상품번호를 합치지 않는다. 옵션 정보는 별도로 보존하고 현재 단계에서는 출처 간 동일 상품 매칭을 도입하지 않는다.

### 가격과 판매 조건

| 입력 | 논리 출력 필드 | 정제 및 사용 규칙 |
|---|---|---|
| `price.min`, `listing_prices[].amount/text` | `amount`, `currency`, `price_kind`, `observed_at` | 숫자·통화 검증 후 목록 최저가로 보존. 같은 원본에서 파생된 값은 중복 관측으로 세지 않음 |
| `price.*Formatted` | 원문 참조 또는 `price_text` | 숫자 파싱 대조에 사용. 숫자 필드와 충돌하면 이슈 기록. 화면 표시는 정제 금액으로 생성 |
| `price.total`, `member`, `memberTotal` | 종류별 가격 관측, `condition_status` | 일반가격·회원가격·합계 계열을 분리. 배송·설치·회원 조건을 이름만 보고 확정하지 않음 |
| `initial`, `initialTypeName`, `promotionMinPriceQ` | 원문 참조, 의미 확인 상태 | 의미 확정 전 서비스 대표가격 선정에서 제외 |
| 상세 `offers[]` | 판매처·판매 항목·옵션 식별자, `offer_key`, 가격 조건 | 안정 식별자가 확인된 항목만 Offer 구성. 집계 가격을 개별 판매처 Offer로 변환하지 않음 |

KRW는 정수 금액으로 처리하고, 통화는 출처 계약에서 확인한 근거와 함께 설정한다. 0·음수·파싱 실패를 정상 판매가격으로 발행하지 않으며 원문과 실패 사유를 남긴다. 가격 결측은 NULL로 두고 0으로 대체하지 않는다.

목록 최저가는 관측 목록 가격으로 표시할 수 있지만 배송·설치·옵션까지 포함한 최종 결제 금액으로 설명하지 않는다. `offer_key`에는 안정 판매처·판매 항목·옵션 식별자를 사용하고 배열 순서나 현재 가격을 넣지 않는다.

### 규격과 추천 속성

| 입력 | 논리 출력 필드 | 정제 및 사용 규칙 |
|---|---|---|
| `spec_text`, `specifications[]`, `size_text` | `width_mm`, `depth_mm`, `height_mm` | 단위를 mm로 변환. 축 의미·상품 옵션·측정 대상이 확인된 값만 배정 |
| 치수 원문과 추출 근거 | `dimension_raw`, `dimension_status`, `measurement_target`, `option_key` | 상품·포장·부속품 등의 크기 구분. 치수 충돌·축 미확인·미제공 상태 보존 |
| 상세 색상 정보 | `color_codes`, `color_raw` | 색상 사전으로 표준화하고 복수 색상·옵션 연결 유지 |
| 상세 재질 정보 | `material_codes`, `material_raw` | 복합 재질과 적용 부위를 유지하며 표기 통일 |
| 상세 형태 정보 | `shape_code`, `shape_raw` | 지원 형태 사전에 매핑. 매핑 불가능하면 원문 보존 |

색상·재질·형태는 현재 필드 요약만으로 독립 필드 확보가 확인되지 않으므로 상세 추출 결과에서 확보 여부를 점검한다. 상품명에서 추출한 후보나 향후 AI 이미지 분석 결과는 별도 추정값으로 관리하고, 원장 확인값을 덮어쓰지 않는다.

치수 정제 예시는 다음과 같다. 예시 값은 실제 수집 결과가 아니다.

| 입력 예시 | 정제 결과 |
|---|---|
| `가로 120cm / 깊이 60cm / 높이 75cm`이며 상품·옵션 확인 | `width_mm=1200`, `depth_mm=600`, `height_mm=750` |
| `120×60×75cm`이며 축 설명 없음 | 원문·파싱 숫자 유지, 축별 값은 NULL, 축 미확인 기록 |
| `포장 크기 130×70×20cm` | 포장 규격으로 보존, 상품 규격 필터에 사용하지 않음 |
| 규격 없음 | 축별 값 NULL, 미제공 상태 유지 |

### 이미지와 링크

| 입력 | 논리 출력 필드 | 정제 및 사용 규칙 |
|---|---|---|
| `image.url`, `listing_image_url`, `images[].url/role/order/origin` | 상품–이미지 연결, `source_url`, `role`, `order`, 원천 참조 | 목록·갤러리 구분. URL만으로 다운로드 완료로 판정하지 않음 |
| `local_path`, `sha256`, `bytes`, 해상도·형식·`status` | `asset_ref`, `image_checksum`, `validation_status` | 파일 존재·디코딩·해시·해상도 대조. 로컬 경로는 배포 저장소 참조와 연결 |
| `image.version` | `source_image_version` | 제공처 이미지 버전으로 보존. 바이트 checksum을 대신하지 않음 |
| 추출 `product_url` | `comparison_url`, `link_status`, `checked_at` | 구성한 URL은 검증 후보. 상품번호 대응·HTTPS·허용 호스트·현재 상태 확인 후 승인 |
| 상세 판매 링크 | 판매 항목 링크와 검증 상태 | 비교 페이지 링크와 직접 판매 링크를 분리 |

동일 checksum의 파일은 재사용하되 상품별 연결·출처·원천 버전은 유지한다. 이미지 이용 검토 상태는 별도 운영 원장에서 입력받고 ETL이 임의 승인하지 않는다. 저장·표시·분석·외부 전송의 이용 범위도 구분한다.

### 날짜와 공통 추적 정보

| 입력 | 논리 출력 필드 | 정제 및 사용 규칙 |
|---|---|---|
| `dates.register` | 등록월 원문·파싱 결과·파싱 상태 | `18.02.`를 임의로 2018년으로 확정하지 않음 |
| `dates.make` | 원문·의미 확인 상태 | 업무 의미 확인 전 제조일로 사용하지 않음 |
| `collected_at` | `observed_at` | UTC 관측 시각. 상품 등록일과 구분 |
| `job_id` | `collection_run_id` | 수집 실행 추적용으로 대응 관계 유지 |
| 원천 ID, hash, 버전, `provenance`, `field_status` | `raw_document_id`, 원천 hash 참조, 필드별 출처·확인 상태 | 원본 응답 hash와 추출 payload hash 구분. 필드의 결측·실패·충돌 사유 보존 |
| 정제 작업에서 생성 | `normalizer_version`, `processing_run_id` | 같은 입력의 재처리와 결과 비교에 사용 |

필드 상태는 확인됨·미제공·추출 실패·충돌·의미 미확인 등을 구분한다. 구체적인 enum은 팀에서 확정하되, NULL만 저장해 원인을 잃지 않도록 한다.

## 실버 논리 테이블과 행 기준

상품·가격·이미지는 한 상품에 여러 건이 연결되므로 분리한다. 테이블별 한 행의 기준을 먼저 합의하면 이미지 수와 가격 수가 곱해져 상품 행이 중복되는 것을 막을 수 있다.

| 논리 테이블 | 한 행의 기준 | 주요 필드 |
|---|---|---|
| `silver_product_observations` | 상품의 수집 관측 1건 | 상품 업무 키, 이름·모델·분류, 관측 시각, 원천 참조 |
| `silver_product_attributes` | 상품/옵션의 속성 관측 1건 | 속성명·값·단위·측정 대상·확인 상태·출처 |
| `silver_price_observations` | 상품/옵션·가격 종류의 관측 1건 | 금액·통화·조건·판매 항목 참조·관측 시각 |
| `silver_product_images` | 상품과 이미지의 연결 관측 1건 | 상품 키·파일 참조·checksum·역할·검증·이용 검토 참조 |
| `silver_link_observations` | 상품/판매 항목 링크의 관측 1건 | URL·링크 종류·검증 상태·확인 시각 |
| `silver_quality_issues` | 품질 문제 1건 | 대상·필드·문제 코드·원천 참조·처리 버전 |

동일 실행의 재전달은 원천 관측 식별자와 자식 항목의 안정 키로 중복을 제거하고 다른 시점의 관측은 보존한다. 실제 복합 키와 hash 계산 규칙은 구현 전에 확정한다.

## 골드 논리 테이블과 활용

### 상품 카탈로그

`gold_product_catalog`는 서비스 상품 단위로 현재 유효 정보를 구성한다. 옵션별 속성이 다르면 옵션 연결을 유지하고, 서로 다른 옵션의 최저가·색상·규격을 조합해 한 상품 조건처럼 발행하지 않는다.

| 필드 묶음 | 논리 필드 | 사용처 |
|---|---|---|
| 식별 | `product_key`, `product_id`, `source_code`, `source_product_id` | MySQL 반영·추천 결과 연결. 내부 UUID는 기존 키 매핑을 재사용 |
| 표시·분류 | `display_name`, `manufacturer`, `brand`, `model`, `category_code` | 상품 카드·품목 필터 |
| 확인된 속성 | 색상·재질·형태, `width_mm`, `depth_mm`, `height_mm`, 옵션 참조 | 명시 필수 조건 대조·선호 색상 계산 |
| 가격 | `price_krw`, `price_kind`, 가격 조건·관측 시각·확인 상태 | 가격 표시·예산 대조. 의미가 호환되는 유효 가격만 선택 |
| 이미지 | 대표 `image_id`, `asset_ref`, `image_checksum` | 카드 표시·이미지 대응 |
| 품질·추적 | 필드별 출처/관측 시각, 준비 상태·사유, 원천/정제/매핑 버전 | 추천 근거·결측 처리·재현 |

골드의 현재 값은 필드별 관측 시각·확인 상태·출처 우선순위로 선택한다. 최신 상세 수집 실패만으로 이전 확인 규격을 NULL로 덮어쓰지 않으며, 이전 값을 유지할 때는 그 값의 관측 시각과 최신 수집 실패를 함께 추적한다. 명시적인 값 철회·상품 비활성화와 단순 수집 실패는 구분한다. 가격·링크의 유효 기간과 동일 시점 충돌 우선순위는 별도 합의가 필요하다.

### 추천 이미지와 벡터

| 논리 테이블 | 주요 필드 | 사용처 |
|---|---|---|
| `gold_recommendation_images` | `product_id`, `image_id`, `image_checksum`, 벡터 참조·행 번호, 임베딩 모델·차원·전처리 버전, `index_version` | 사전 상품 임베딩과 추천 검색 |

벡터는 검증·이용 승인을 거친 이미지에서 별도로 생성하는 파생 데이터다. checksum·모델·전처리 버전을 기준으로 재사용하고 상품–이미지–벡터 행 대응을 검증한다. 사용자 요청 때 전체 상품을 다시 임베딩하지 않는다.

요청별 유사도·색상 기여분·최종 점수는 추천 실행 결과에 저장한다. 현재 직접 추천 정책의 수동 가중합 점수를 상품 골드에 고정 속성으로 저장하지 않는다.

### 링크와 판매 이력 및 품질 집계

| 논리 테이블 | 주요 필드 | 사용처 |
|---|---|---|
| `gold_product_links` | 상품 ID·비교 URL·연결 상태·확인 시각 | 다나와에서 보기 버튼 |
| `gold_offers_current` | 안정 Offer 키·상품/옵션·판매처·가격 조건·직접 링크·확인 상태 | 확인된 현재 판매 정보 |
| `gold_offer_snapshots` | Offer 키·관측 시각·당시 가격/조건·원천 참조 | 판매 조건 이력과 추천 시점 추적 |
| `gold_catalog_quality_daily` | 기준일·품목·관측/고유 상품 수·이미지 검증 수·속성 확보율·추천 준비 수·보류 사유 | 수집 보강과 운영 확인 |

판매처 식별이 없는 목록 가격은 상품 가격 관측으로 유지하고 가상의 Offer를 만들지 않는다. 품질 집계는 상품 수·상품별 이미지 연결 수·고유 파일 수를 구분한다. 속성 확보율은 해당 품목에서 필요한 속성을 기준으로 분모를 정한다.

## 추천과 링크 준비 상태

| 준비 상태 필드 | 충족 기준 | 미충족 시 처리 |
|---|---|---|
| `catalog_ready` | 상품 식별·이름·서비스 품목 매핑 유효 | 골드 보류 사유 기록 |
| `recommendation_ready` | 카탈로그 준비, 활성·이용 승인 이미지, 호환 벡터 확보 | 서비스 추천 인덱스 등록 보류 |
| `comparison_link_ready` | 해당 상품과 대응하는 다나와 URL 검증 완료 | 연결 준비 중 표시·버튼 비활성화 |
| `purchase_link_ready` | 판매처·옵션·판매 조건·직접 구매 URL 검증 완료 | 직접 구매 버튼 제공 보류 |

각 상태에는 실패·보류 사유를 함께 둔다. 수집 성공이나 다운로드 성공만으로 추천·링크 준비 완료를 판정하지 않는다.

가격·치수 결측은 전체 추천 제외 조건으로 두지 않는다. 사용자 필수 조건과 대조할 때 확인된 충족은 MATCH, 불일치는 DIFFERENT로 제외, 결측·미지원은 UNKNOWN 후보로 분리한다. 요청별 조건 충족과 상품 자체의 준비 상태는 별개다. 추정 규격으로 필수 치수 조건을 통과시키지 않는다.

## 공통 필드와 버전 및 준비 상태의 표현

| 필드 묶음 | 논리 형식과 의미 |
|---|---|
| `source_code`, `source_product_id`, `product_key` | 문자열. 제공처 상품번호와 내부 UUID는 별개다. ID 충돌은 품질 문제로 보류한다. |
| `raw_document_id`, 원천 hash | MongoDB ObjectId 문자열 및 원문/추출 hash 참조. DB 간 FK나 상품 동일성 보증이 아니다. |
| `observed_at`, 확인 시각 | UTC 시각. 입력 `collected_at`의 실제 관측 범위를 확인해 사용하며 상품 등록월·파일 저장 시각과 구분한다. |
| 필드별 출처·확인 상태 | 값마다 원천/추출 경로, 관측 시각, 확인됨·미제공·추출 실패·충돌·의미 미확인을 보존한다. NULL 하나로 원인을 합치지 않는다. |
| `dimension_raw`, `dimension_status`, `measurement_target`, `option_key` | 규격 원문·확인 상태·상품/포장/부속품 구분·옵션 참조. 축별 mm는 검증된 숫자 또는 NULL이다. |
| `amount`, `price_krw`, `currency`, `price_kind`, `condition_status` | 금액은 KRW 양의 정수 또는 NULL. 가격 종류·조건 확인 상태를 함께 보존한다. 의미 불명 가격은 대표가격에서 제외한다. |
| `color_codes`, `material_codes`, `shape_code` | 검증한 속성 사전 코드와 원문·옵션·적용 부위 참조. 추정 후보와 확인값을 분리한다. 실제 확보·지원 여부는 표본 검증한다. |
| `catalog_ready`, `recommendation_ready`, `comparison_link_ready`, `purchase_link_ready` | 각각의 검증 결과로 산출한 논리 boolean. `readiness_reasons_json`에 상태별 실패·보류 사유와 평가 버전을 연결한다. 서로를 자동 승인하지 않는다. |
| `normalizer_version`, `processing_run_id`, `mapping_version` | 정제 코드·가공 실행·분류 규칙의 추적 버전. 재전달은 동일 관측으로 멱등 처리하고 다른 시점은 보존한다. |

Gold의 `product_id`는 서비스 UUID 매핑이다. 최초 게시에서 UUID가 정해지면 안정 `product_key`에 연결해 재사용하며 원천 번호를 그대로 UUID로 쓰거나 변환마다 새 UUID를 만들지 않는다. `image_id`는 이미지 자산 UUID(`assets.id`)로 통일하며 상품별 연결 UUID(`product_images.id`)는 별도 참조한다. `asset_ref`에는 버킷/객체 키·checksum을 넣는다. 벡터 참조는 `embedding_id`·Delta 테이블/버전·행 식별자·차원/전처리를 포함하며 행 번호 하나만으로 영속 식별하지 않는다.

`gold_catalog_quality_daily`는 기준일·품목별 원천 관측 수, 고유 상품 수, 상품–이미지 연결 수, 고유 파일 수, 이미지 검증 수, 속성별 확보 분자/분모, 추천 준비 수와 보류 사유별 건수를 분리한다. 분모 0은 미산출이다. 같은 자료의 재집계는 고정 버전으로 재계산해 Upsert하고 누적 더하기로 중복시키지 않는다.

`readiness_reasons_json`과 위 논리 필드의 실제 MySQL 저장 위치·JSON 스키마·상태 코드는 DDL 전에 확정한다. 해당 컬럼이 현재 ORM에 존재하거나 ETL이 실행됐다는 뜻은 아니다. 운영 검수·이용 범위·상품 활성 원장은 ETL이 덮어쓰지 않는다.
