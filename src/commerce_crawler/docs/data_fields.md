# 수집 원본·가공·다운로드 필드 계약

2026-10-16 현재 구현 기준이다. 이 문서는 `commerce_crawler`의 실제 JSONL 출력 계약을 설명한다. [BRD](../../../docs/BRD_3차.md) 전체의 구현 명세나 도매꾹 API 계약을 대신하지 않는다. 다나와 상품을 독립적으로 선택했으며 도매꾹 상품과 일치시켜 뽑은 데이터가 아니다.

기본 CLI는 상품별 JSON과 재개 이력 JSONL 파일에 실시간 저장한다. 표준 JSONL은 종료·Ctrl+C 또는 --export-from으로 요청한 시점의 스냅샷이다. 실시간 저장·재개는 [체크포인트 문서](checkpoint.md)를 따른다.

## 세 파일의 역할과 연결

`product_raw.jsonl`과 `products.jsonl`은 각각 한 줄에 상품 한 건을 저장한다. 연결 키는 `source + product_id`다. `products.raw_payload_hash`가 같은 실행의 `product_raw.payload_hash`와 일치해야 한다. 실행 간 구분에는 `job_id`를 함께 사용한다. `run_report.json`은 상품 한 건이 아니라 실행 전체의 관리 정보다.

| 작업 | 읽을 파일 |
|---|---|
| 상품명·가격·규격 등 활용 | `products.jsonl` |
| 값이 어디에서 추출됐는지 대조 | `products.provenance`와 `product_raw.payload` |
| selector 수정 후 다시 추출 | `product_raw.jsonl`, `run_report.json`, `details/`를 포함한 실행 디렉터리 |
| 실패·생략·요청량·확보율 확인 | `run_report.json` |

`products`는 공통 필드로 정리한 결과다. 모든 치수·무게·카테고리를 완전히 표준화한 Silver 결과를 뜻하지 않는다. 문자열 규격의 단위와 복합 치수는 원문 형태로 남긴다.

수집 후 선택적으로 `product_collector.media`를 실행하면 media/products_with_local_images.jsonl에 기존 상품의 images[]에 다운로드 정보를 병합한다. 로컬 파일 경로·실제 해시·해상도·다운로드 상태는 이 추가 파일에서 확인한다. 기본 products.jsonl은 URL 수집 결과이며 자동 다운로드를 수행하는 것은 아니다. [이미지 내재화 문서](image_localization.md)를 따른다.

## API 원본과 수집기 출력 구분

API 원본 필드는 `product_raw.payload.listings[].api_product` 안의 제공처 상품 객체를 기준으로 한다. 바깥의 payload/listings/api_product라는 wrapper와 source/job_id/collected_at/hash는 수집기가 만든 구조다. GET HTML 대체 경로는 API 객체 대신 카드 HTML을 보존한다.

다음 24개 경로는 2026-10-16 소량 배치의 API 객체 350개에서 모두 키 존재를 확인했다. 이는 모든 값이 비어 있지 않다는 뜻은 아니다.

| 원본 경로 | 의미와 처리 |
|---|---|
| productCode | 다나와 코드. 가공 product_id는 문자열 |
| name.maker, name.brand, name.product, name.full, name.model | 제조사·브랜드·상품명·표시명·모델. 빈 문자열은 원본에 유지하고 가공은 상세 보강 또는 null |
| category.codes, category.names | 제공처 단계별 분류. 가공 categories는 별도의 수집 설정·관측 소속 |
| price.min, price.minFormatted | 목록 최저값·표시 문자열. 가공 listing_prices로 정리 |
| price.total, price.totalFormatted | total 값·표시 문자열. 포함 항목을 이름만 보고 확정하지 않음 |
| price.member, price.memberFormatted | 회원 가격 계열. 적용 회원·할인 조건은 이 표본만으로 미확인 |
| price.memberTotal, price.memberTotalFormatted | 회원 합계 계열. 구성 미확인 |
| price.initial, price.initialTypeName, price.promotionMinPriceQ | 부가 가격 필드. 정확한 업무 의미 미확인, 임의 서비스 가격으로 변환하지 않음 |
| image.url, image.version, image.alt | 목록 이미지·버전·대체 설명. 상세 갤러리와 구분 |
| dates.register, dates.make | 원문 날짜 표현. make를 제조일로 단정하지 않고, 두 자리 register에 세기를 추정하지 않음 |

제품 속성을 API 값으로만 한정하지 않는다. manufacturer/brand/model/name/registered_month 등은 식별된 상세의 명시 값으로 보강될 수 있다. `provenance`로 실제 채택 근거를 확인한다. `product_url`은 pcode로 구성한 다나와 개별 상세 주소이며 제공처 상품 객체의 원래 product_url 필드라고 설명하지 않는다.

`extractor_version`, `detail_error`, `payload_hash`는 원본 관리 영역이고 가공은 `raw_payload_hash`, `field_status`, `provenance`를 가진다. 모든 관리 키가 모든 파일에 똑같이 존재한다고 가정하지 않는다.

## products.jsonl

### 식별과 관리

| 필드 | 형식 | 의미 |
|---|---|---|
| source | string | 현재 `danawa` |
| product_id | string | 다나와 pcode. 다른 출처의 상품번호와 동일 제품이라는 의미 없음 |
| job_id | string | 해당 수집 또는 재추출 작업 ID |
| schema_version | string | 현재 `1.2`. 이전 실행 파일은 `1.0`/`1.1`일 수 있음 |
| collected_at | UTC ISO 8601 string | 원본 상품 레코드가 만들어진 최초 목록 수집 시각. 상세 요청 시각은 원본에 별도 기록 |
| raw_payload_hash | string | 해당 실행의 원본 추출 payload 해시 |
| categories | array | 발견된 수집 카테고리. category_id, category_name, observed_category_name 포함 |
| detail_status | string | 상세 보강 처리 결과 |
| provenance | object | 정규화 필드별 추출 출처. 값이 없으면 null |
| field_status | object | 정규화 필드별 관측·미확보 사유 |

`category_name`은 설정 이름이고 `observed_category_name`은 응답 페이지 제목에서 확인한 이름이다. 설정 이름을 페이지에서 확인한 사실로 취급하지 않는다. 원본 상품의 재추출은 `collected_at`을 현재 시각으로 바꾸지 않는다.

### 상품 정보

| 필드 | 형식 | 의미와 제한 |
|---|---|---|
| name | string | 식별자가 확인된 상세 상품명 또는 목록 상품명 |
| product_url | string | 다나와 상품 상세 주소. 직접 판매 페이지 주소가 아님 |
| manufacturer, brand, model | string / null | 명시 제조사·브랜드·모델. 브랜드를 제조사로 대신 넣지 않음 |
| registered_month | string / null | `2018.02.` 같은 원문 등록월. 등록 일시로 확장하지 않음 |
| country_of_origin | string / null | 명시 원산지/제조국 |
| size_text, weight_text | string / null | 명시 크기·무게 표현. 단위 환산 안 함 |
| description | string / null | JSON-LD 설명 또는 설명 HTML 텍스트. 실제 검증 표본은 주로 규격 요약 |
| spec_text | string / null | 규격 요약 원문 텍스트 |
| specifications | array / null | 규격의 name/value, JSON-LD additionalProperty의 unit_text. 상품별 관측 항목이 다름 |
| keywords_text | string / null | JSON-LD/메타 태그의 키워드 표현 |
| images | array / null | 상세 상품 갤러리 URL·역할·순서·출처·관측 주소·해상도 관측 상태. 다운로드 후 파일명·경로·해시·상태 추가 |
| listing_image_url | string / null | 목록 이미지. 갤러리 배열과 분리 |
| listing_prices | array | 카테고리별 목록 가격 관측값 |
| offers | array / null | 집계 가격과 판매처별 제안을 kind로 구분 |

### 가격과 판매 제안

`listing_prices[]`에는 category_id, text, amount, currency, kind, provenance를 기록한다. kind는 comparison_lowest 또는 listing_display다. 단일 금액으로 읽을 수 없는 가격 표현은 text를 유지하고 amount는 null로 둔다.

`offers[]`의 kind별 필드는 다음과 같다.

| kind | 주요 필드 | 해석 |
|---|---|---|
| aggregate | low_price, high_price, currency, offer_count, provenance | JSON-LD의 비교 집계. offer_count는 판매 제안 수이며 재고가 아님 |
| seller_offer, JSON-LD 출처 | price, currency, seller_name, seller_id, url, availability, shipping, return_policy, provenance | Product offers에 명시된 제안. availability·배송·반품은 제공된 원문 표현/객체 |
| seller_offer, 상세 HTML 출처 | price, currency, seller_name, seller_id, url, shipping_fee, shipping_text, provenance | 다나와에 표시된 판매처별 가격·배송. seller_id는 cmpnyc 코드 |

판매처 제안은 추출 출처에 따라 하위 키가 다르므로 소비자는 키 존재와 null을 검사한다. 금액은 정수이면 int, 소수이면 정밀도 보존을 위한 string, 미확보이면 null이다. 같은 판매처의 JSON-LD 제안과 HTML 제안이 함께 존재하면 현재 구현은 별도로 보존하며 자동 병합하지 않는다.

`offers[].url`은 다나와 bridge 링크 또는 원문에 제공된 판매 연결 주소다. 링크를 따라 판매몰에 방문하거나 최종 주소·실제 재고를 검증하지 않았다. listing_prices와 aggregate.low_price가 다르더라도 자동으로 하나를 덮어쓰지 않는다. 목록의 다른 구성 pcode·가격은 원본 variant_prices에만 보존하고 별도 상품을 수집한 것으로 집계하지 않는다.

## product_raw.jsonl

최상위는 source, product_id, collected_at, job_id, schema_version, extractor_version, categories, payload_hash, detail_status, detail_error, payload로 구성된다. 재추출 결과에는 reprocessed_at이 추가된다.

| 원본 경로 | 보존 내용 |
|---|---|
| payload.listings[] | 발견 카테고리, 목록 URL·page, 해당 시각, 상품 ID·이름·이미지·가격·규격·등록월 원문. GET은 카드 html/variant_prices, POST는 format=action_product와 api_product 전체 객체 |
| payload.detail | 상세 URL·수집 시각, 제목, JSON-LD 원문·파싱 결과·오류, selected_product·matched_by, 규격·설명(description_html)·제조·등록월·판매 제안·메타데이터, gallery_images/gallery_status |

원본은 HTTP 통신 전체를 저장한 자료가 아니다. 목록은 관련 카드 HTML만 저장하며 응답 헤더·쿠키는 저장하지 않는다. 상세 미요청·예산 소진·요청 실패에서는 payload.detail이 null일 수 있다.

extractor_version 0.4부터 상세 응답 HTML 전체는 payload가 아니라 실행 디렉터리의 `details/<product_id>/<수집 시각>.json.gz`(gzip)에 둔다. 재추출은 payload의 `detail.collected_at`으로 같은 응답 파일을 찾는다. 보관 기간은 [README의 보관 정책](../README.md#상세-html-보관-정책)을 따른다. 규격·판매처·제조 정보 영역의 HTML 조각(specification_html, seller_offer_html, made_info_html)과 unrecognized_html도 저장하지 않는다. 인식 실패 페이지의 원문도 `details/`에 있다. 이 HTML은 재추출 근거이며 적재·파이프라인 입력이 아니다. 0.3 이하 실행은 payload의 html을 그대로 읽어 재추출하고, CLI 재추출(`--reprocess-from`)은 그 HTML을 새 결과의 `details/`에 옮겨 저장한다. 다른 위치로 내보낸 스냅샷과 재추출 결과의 `run_report.detail_evidence_dirs`는 `details/`가 있는 원래 실행 디렉터리를 가리킨다.

## 상태와 보고서 읽기

상세 상태는 not_requested, not_needed, ok, unrecognized, budget_exhausted, request_failed, disabled다. disabled는 --listing-only로 상세를 생략했다는 뜻이다. ok는 인식 가능한 상세 영역을 추출했다는 뜻이며 모든 필드를 확보했다는 뜻이 아니다.

정규화 값의 field_status는 다음과 같다.

| 값 | 의미 |
|---|---|
| observed | 정규화 필드가 null이 아님 |
| detail_not_requested | 상세를 아직 요청하지 못한 상태에서 필드 미확보 |
| detail_budget_exhausted | 요청 한도로 상세 보강하지 못해 필드 미확보 |
| detail_request_failed | 상세 요청 실패 상태에서 필드 미확보 |
| detail_disabled | 목록만 수집하는 설정으로 상세를 생략해 필드 미확보 |
| parser_unrecognized | 상세 응답을 받았으나 추출 영역을 인식하지 못함 |
| not_found_in_extracted_content | 추출한 영역에서 해당 필드를 찾지 못함. 페이지 전체 부재를 보장하지 않음 |

`run_report.field_coverage`는 최상위 정규화 필드가 null이 아닌 상품 수다. 예를 들어 listing_prices 배열은 있지만 내부 amount가 null이어도 해당 배열은 observed에 포함된다. 유효 금액·배송비 등 하위 값 확보율은 각 원소를 추가로 검사해야 한다. 이 집계를 전 필드 완전성이나 BRD 보존 확인율로 해석하지 않는다.

execution_mode가 live이면 request_count와 requests는 해당 실행의 HTTP 시도다. offline_reprocess이면 request_count는 0이며 original_run_id, original_request_count, original_request_log에 원래 실행의 요청 근거가 있다. 재추출 작업이 새 HTTP 검증을 수행한 것은 아니다.

## BRD 주요 정보의 위치와 부족한 부분

| BRD/PRD 주요 정보 | 이 수집기의 위치 | 현재 확보 범위 |
|---|---|---|
| 출처·원천 상품번호·상품명 | products.source/product_id/name | 다나와 관측 상품 기준 |
| 카테고리 | products.categories | 수집 코드·설정/관측 이름. 공통 분류 표준화 없음 |
| 이미지 참조 | products.images, listing_image_url | 갤러리 사진과 목록 사진을 분리. 기본은 URL이며 선택 다운로드 결과에는 파일명·경로·해시가 추가됨 |
| 가격·규격 | products.listing_prices/offers/spec_text/specifications/size_text/weight_text | 미제공·미추출 값은 null |
| 상품 주소·판매 연결 | products.product_url/offers[].url | 다나와 상세/판매 연결. 최종 판매몰 주소·실제 구매 가능 여부 검증은 별도 |
| 조회 시각 | products.collected_at, product_raw.payload의 단계별 collected_at | 목록/상세 관측 시점 대조 가능 |
| 이용 조건 | 대응 필드 없음 | 저장·분석·표시·이미지 사용권 조건 미확보 |
| 수집 성공·실패·생략 | run_report, product_raw.detail_status/detail_error | 수집 작업 상태이며 상품 대상 판정 상태가 아님 |
| 포함·제외·검토 대기, 추천 가능 여부 | 대응 필드 없음 | BRD 수집 대상 판정·추천 등록 처리는 미구현 |

현재 파일은 수집 단계의 계약이며 불변 MongoDB 원천·Delta Silver·서비스 Gold 완료 데이터가 아니다. 이용 조건·포함/제외 검수·추천 승인 상태는 현재 구현하지 않았다. 도매꾹 API 상품과의 동일 상품 연결도 생성하지 않는다. 현행 아키텍처와의 연결은 [연계 계획](pipeline_integration.md)을 따른다.

2026-10-14 실제 10개 표본을 수정된 추출기로 재처리한 결과, 주요 기본값·가격·제조사·브랜드·등록월·요약 설명·이미지·판매 제안·키워드는 10건, 크기는 8건, 모델명·원산지·무게는 0건 확보했다. 재고·최소 주문수량·주문 단위, 정확한 등록/판매 시작/종료 일시, 판매자 사업자 정보, 반품 주소·비용, 안전 인증·고시의 공통 구조도 확보 완료로 간주하지 않는다. 미확보 항목이 전체 다나와 사이트에서 제공되지 않는다는 결론은 아니다.

## 최근 실제 데이터 검토

2026-10-16의 300개 배치에서 가공 주요 필드 27개가 모두 존재했지만 모델 27개·원산지 9개·무게 12개·크기 186개만 값이 있었다. 이미지 URL 확보 678개와 파일 다운로드 성공 664개를 구분한다. API 350개와 NAS 복사본의 원문·가공·이미지 연결 검사는 [검증 기록](validation_results.md)에 대상 경로와 함께 남겼다. 위 2026-10-14의 10개 결과를 최신 배치 확보율로 사용하지 않는다.
