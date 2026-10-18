# Silver·Gold 제안서 반영 대조표

2026-10-16 설계 작성, 2026-10-18 Silver/Gold 및 MySQL 게시 연결 상태 갱신. 이 문서는 사용자 요청에 따라 `실버_골드_필드_제안.md`의 전체 항목을 구현 기준으로 구체화한다. 제안서의 권고를 팀 승인·실제 DDL·검증 성공으로 간주하지 않는다. Silver/Gold 필드 처리 위치를 추적하며 MySQL 게시와 업무 입력 인수를 구분한다.

원문: 작성일 2026-10-16, SHA-256 `2f2ba911d6b90496b9546f01fd06990aa281c5233c6fcc7f02ab02a0eb193dd9`. 공유 문서는 읽기만 했으며 수정하거나 저장소에 복제하지 않았다. 최초 대조 기준은 Bronze commit a20cc5b, dev e35b2a3다. 필드 계약은 [Silver 설계](silver_design_v1.md), [데이터 계약](data_contract_v1.md), [보강 계약](design_amendment_v1_1.md)을 따르며 현재 검증 범위는 [실서버 검증](service_validation_20261018.md)과 구분한다.

## 실제 입력 경로

Bronze `raw_envelope_json`을 계약 검증 후 해석한 객체를 `R`이라고 표기한다. `payload_json`은 `R.payload`와 내용/hash를 대조한다. 다음 별칭은 설계 표기이며 새 필드가 이미 구현됐다는 뜻이 아니다.

| 표기 | 실제 입력 | 의미 |
|---|---|---|
| `L[i]` | `R.payload.listings[i]` | 목록 관측. 여러 페이지/카테고리/시각을 하나의 최신값으로 합치지 않음 |
| `A[i]` | `L[i].api_product` | 제공처 API 원문. HTML 목록 관측에서는 없을 수 있음 |
| `D` | `R.payload.detail` | 상세 관측. 실패/미제공/다른 상품 선택을 구분 |
| `J` | `D.selected_product` | 선택 JSON-LD. ID·옵션 대응 확인 후만 필드 근거로 사용 |
| `M` | `R.collector_metadata` | 원래 wrapper의 분류·수집 상태·시각·버전 |
| `I[j]` | `R.image_refs[j]` | 접수 때 고정한 갤러리·파일 확보 근거. 예정 object key와 업로드 성공은 별개 |
| `V` | MongoDB `ingestion_observations.image_state_vector` 및 revision/fingerprint | 변경 가능한 업로드 상태. `R.observation_key`와 Raw ID 연결 대조 |

원래 `products.jsonl`의 `listing_prices`, `offers`, `images`, `size_text`, `provenance`, `field_status`는 현재 Bronze에 동일한 최상위 필드로 보존돼 있다고 가정하지 않는다. 새 Silver 어댑터는 위 원문에서 값을 재구성하고 자체 출처·상태를 남긴다. 원본 정규화 파일은 스냅샷의 감사/대조 자료이며 원문과 다른 값을 우선 신뢰하는 별도 입력 경로를 만들지 않는다.

## 실제 표본 확인 범위

기존 보관 run `2ab52dfe-92c3-4d0c-9ec3-879462a6c657`의 상품 `5097200`, `96056663`, `122661898` 세 건을 읽었다. 모두 `L.format=action_product`, `L.price_kind=comparison_lowest`, API name/category/price/dates/image, `D.specifications`, `D.seller_offers`, `I`가 있다. 갤러리 관계는 각각 4·4·1건이다. 이 결과는 전체 71카테고리 확보율이나 모든 입력 형식 지원을 입증하지 않는다.

`price.min`과 회원 계열 숫자가 같은 사례가 있어도 동일 의미로 병합할 수 없다. `dates.register=18.02.`와 `dates.make=2018.02.01`이 함께 있어도 make를 제조일/등록일 증명으로 쓰지 않는다. 상세 규격 `침대(가로x세로x높이)=110x200x36cm`도 옵션·측정 대상·세로 축 매핑을 별도 확인해야 한다.

product_images의 실제 갤러리 행은 I의 고정 연결에 대응한다. I에 대응이 확인되지 않은 API 목록 이미지 URL·version은 product_observations.evidence_json의 listing_image_candidates로 보존하고 새 갤러리/다운로드 성공 행으로 만들지 않는다. provider_image_version은 URL/원천 연결 대응이 확인된 대상에만 채운다.

## Silver 필드: 전체 26항목

반영 위치는 예정 Delta 테이블이다. 각 테이블의 행 기준·키·타입·상태는 [스키마](silver_design_v1.md#2-테이블과-행-기준)를 따른다. 근거가 없으면 NULL과 상태를 남기며 조용히 버리지 않는다.

| ID | 제안 항목 | 실제 근거 → 예정 출력 | 규칙·미확인 처리 |
|---|---|---|---|
| S01 | 출처·상품 식별 | `R.source_code/source_product_id`, `M.product_id`, `A.productCode` → product_observations의 `source_code/source_product_id/product_key` | 문자열 상품번호 대조, `danawa:<id>` 사용. 충돌은 품질 오류. 서비스 UUID와 구분 |
| S02 | 상품명 | `J.name`, `A.name.full/product`, `L.product_name` → `display_name/normalized_name` | 같은 상품·옵션의 유효 상세를 우선하고 원문·후보·선택 근거 보존. 서로 다른 의미의 이름을 공백 정리로 합치지 않음 |
| S03 | 제조사·브랜드·모델 | `A.name.maker/brand/model`, `L.manufacturer/brand/model`, 유효 `D.manufacturer/J.brand` → `manufacturer/brand/model` | JSON-LD brand 객체의 name과 문자열 구분. 빈 값 NULL, 제조사→브랜드 대입 금지. 별칭 사전 버전 미제공이면 원문 표기 유지 |
| S04 | 원천·서비스 분류 | `A.category.codes/names` → `source_category_path_json`; 외부 매핑 → `service_category_id/service_category_code/mapping_status/mapping_version` | codes/names 길이·순서 대조. 제안서 category_code는 서비스 코드로 명시하며 원천 코드와 혼용하지 않음. 미매핑/모호/제외 구분 |
| S05 | 수집 분류 | `M.categories`, `L.category` → `collection_category_context_json` | 수집 대상·페이지 맥락. API 분류와 다른 namespace이며 서비스 분류로 자동 승격하지 않음 |
| P01 | 목록 최저가 | `A.price.min/minFormatted`, `L.price_text/price_kind` → price_observations의 `amount/currency/price_kind/observed_at` | LISTING_LOWEST로 보존. 같은 API 원문에서 파생됐음이 입증된 표시값만 한 관측의 근거로 묶음. 단순 금액 일치로 dedup하지 않음 |
| P02 | 가격 표시 원문 | `A.price.*Formatted`, `L.price_text`, seller `price_text` → `price_text/evidence_json` | 숫자와 표시값 충돌은 PRICE_VALUE_CONFLICT, 정상 금액 선택 보류. 원문 보존 |
| P03 | 합계·회원 계열 | `A.price.total/member/memberTotal` → 종류별 price_observations | TOTAL_REPORTED/MEMBER_REPORTED/MEMBER_TOTAL_REPORTED로 구분. 배송·회원 할인·설치 포함 여부는 UNKNOWN, REGULAR로 표시하지 않음 |
| P04 | 의미 미확정 가격 | `A.price.initial/initialTypeName/promotionMinPriceQ` → price_observations의 원문·종류·상태 | 의미 미확정으로 대표가격 후보 제외. 0은 유효 판매가격이 아닌 원시값·INVALID_ZERO 상태로 보존 |
| P05 | 상세 Offer | `D.seller_offers`, 유효 `J.offers` → 가격 관측 및 검증된 `normalized_offers` | seller_id·source_offer_id·옵션 안정성 확인 시만 offer-v1 키 생성. bridge의 link_pcode는 검증 후보이며 곧바로 안정 ID로 확정하지 않음 |
| A01 | 축별 규격 | `D.specifications[name,value]`, `D.spec_text`, `L.spec_text`, `A.description`의 확인된 원문 → product_attributes의 치수 및 상품의 `width_mm/depth_mm/height_mm` 투영 | mm/cm/m의 명시 수치만 변환. 측정 대상·옵션·축이 확인된 단일값만 축별 투영 |
| A02 | 치수 근거·대상 | 위 규격의 이름/문맥/옵션 → attributes의 `raw_value_json/attribute_status/measurement_target/option_key` 및 상품 `dimension_status` | 제안 dimension_raw는 raw_value_json으로 보존. 포장·좌판·매트리스·프레임·전체 제품 구분. 범위는 min/max, 축 미확인 값 임의 배정 금지 |
| A03 | 색상 | 상세 명시 색상 근거 → `attribute_name=COLOR`, `raw_value_json/codes` | 제안 color_raw는 raw_value_json, color_codes는 codes에 대응. 복수 색상·부위·옵션 유지. 미제공/추정 구분 |
| A04 | 재질 | 상세 명시 재질 근거 → `attribute_name=MATERIAL`, `raw_value_json/codes` | 제안 material_raw/material_codes를 공통 속성 열에 대응. 복합 재질·부위·원문 보존, 임의 대표값 선택 금지 |
| A05 | 형태 | 상세 명시 형태 근거 → `attribute_name=SHAPE`, `raw_value_json/codes` | 제안 shape_raw는 원문 JSON, shape_code는 단일 확인 코드 codes[0]. 단일 대상에서 여러 코드 충돌이면 보류 |
| I01 | 이미지 관계 | `I`, `D.gallery_images`, `A.image.url`, `L.image_url` → product_images의 상품·URL·역할·순서·origin | gallery와 listing 후보를 구분. 기존 고정 image_refs를 갤러리 연결 기준으로 사용하며 다른 URL을 기존 바이트에 임의 결합하지 않음 |
| I02 | 실제 파일·저장소 | `I.acquisition_state/sha256/bytes/width/height/format/bucket/object_key` + `V` → `asset_ref_json/image_checksum/acquisition_status/availability/validation_status` | 로컬 검증과 원격 AVAILABLE 구분. 로컬 경로·다운로드 시각이 Raw에 없으면 미기록으로 표시. asset_ref는 서비스 asset UUID가 아님 |
| I03 | 제공처 이미지 버전 | `A.image.version` → `provider_image_version` | 제안 source_image_version은 이 별도 필드로 대응. 파이프라인 `I.source_version`과 checksum을 각각 보존. API 목록 버전을 모든 갤러리에 전파하지 않음 |
| I04 | 비교 URL | 유효 `D.url`, `L.product_url`, 확인한 `J.url` 후보 → link_observations의 `comparison_url/structure_status/link_status/checked_at` | pcode·HTTPS·허용 호스트를 검사. 현재 상태는 외부 검증 입력이 없으면 NOT_CHECKED, checked_at NULL |
| I05 | 판매 링크 | seller `url`, 유효 JSON-LD Offer URL → SELLER_LINK/DIRECT_PURCHASE 후보 | 다나와 bridge와 최종 구매 URL 구분. 리다이렉트 최종 상태·옵션·판매 조건 미확인은 purchase 준비 보류 |
| T01 | 등록월 | `A.dates.register`, `L.registered_month_text/registered_month`, `D.registered_month` → 등록 원문·`registered_year/month/status` | raw 2자리 연도를 임의 세기 확정하지 않음. 기존 추출값이 원문과 불일치하면 AMBIGUOUS/CONFLICT, 확인값으로 승격 금지 |
| T02 | make 날짜 | `A.dates.make` → `provider_make_raw/provider_make_status` | 날짜 문법 파싱과 업무 의미 확인을 분리. 날짜가 파싱돼도 제조일로 명명하지 않음 |
| T03 | 관측 시각 | `R.collected_at`, 각 `L.collected_at`, `D.collected_at` → 값별 `observed_at/time_status` | wrapper 최초 시각·목록 가격·상세 속성 시각 구분. 적재/재처리 시각으로 최신성 대체 금지 |
| T04 | 수집 실행 | `R.collection_run_id`, `M.job_id` → `collection_run_id` | 서로 대조. 정제 실행 UUID와 구분 |
| T05 | 원천·hash·상태 | R의 ID/hash/버전 및 추출 근거 → 공통 추적 필드·`field_status_json/evidence_json` | 실제 응답 hash가 NULL이면 미보존 상태. extracted payload hash를 응답 hash로 바꾸지 않음 |
| T06 | 정제 버전·실행 | 정규화 규칙·실행 입력 → `normalizer_version/quality_policy_version/processing_run_id/evaluation_key` | processing_run_id는 상위 pipeline_runs 참조 계약 유지. 임의 수집 job_id 대입 금지. 등록 미확인 실험과 운영 실행 구분 |

## Gold 필드: 전체 11항목

Gold 구현은 gold/build·source·storage·runner·export·cli에 있다. 아래는 Silver가 제공하는 근거와 추가 입력이며, 서비스 UUID·승인·벡터를 Silver가 임의 생성한다는 뜻이 아니다.

| ID | 제안 항목 | Silver에서 지원할 근거 / Gold 후속 처리 |
|---|---|---|
| G01 | 카탈로그 식별 | `product_key/source_code/source_product_id`, Raw·evaluation 참조 보존. 내부 product_id는 서비스 업무 키→UUID 매핑에서 조회·재사용 |
| G02 | 표시·분류 | 이름·제조사·브랜드·모델·service_category_id/code 및 필드 상태/출처 제공. category_code만으로 내부 FK 대체 금지 |
| G03 | 확인 속성 | 옵션·대상·단위·축·사전 버전을 보존한 attributes 제공. Gold는 다른 옵션의 색상·규격을 합치지 않음 |
| G04 | 대표가격 | price 종류·통화·조건·시각·유효성 제공. Gold가 호환 조건/옵션과 승인된 유효기간 정책에서 price_krw 선택. 단순 전체 min 금지 |
| G05 | 대표 이미지 | 상품별 이미지 연결·source_version·checksum·원격 상태·검수 참조 제공. Gold image_id/asset UUID는 서비스 관계에서 해결 |
| G06 | 품질·추적·현재값 | field_status·시각·원천/evaluation/매핑 버전 제공. Gold는 마지막 확인값 유지와 최신 실패를 함께 추적하며 명시 철회·과거값·시각 충돌을 구분 |
| G07 | 추천 이미지·벡터 | 승인된 상품–이미지–asset/checksum 관계를 임베딩 담당 결과의 embedding_id·벡터 참조·행·모델·차원·전처리·index_version과 대조. 벡터 생성·요청별 점수 저장은 이 ETL 범위 밖 |
| G08 | 비교 링크 | link_observations의 상품 대응·현재 검증·확인 시각 제공. Gold product_links로 버튼 준비 상태 구성 |
| G09 | 현재 Offer | 검증된 normalized_offers만 사용. 미확인 목록 가격을 가상의 판매처 Offer로 만들지 않음 |
| G10 | 가격/조건 이력 | Silver가 source evidence key·관측 시각·조건 fingerprint를 제공하고 offer_snapshot_key는 Gold 단계에서 생성한다. v1 원천 이력과 price-effect-v2의 집계/이행 선택은 Gold/MySQL 인수 전에 확정하며 기존 서비스 키 자동 변경 금지 |
| G11 | 일별 품질 집계 | raw/evaluation/상품 키·이미지 연결/파일 키·필드 상태·보류 사유 제공. 고유 상품·관측·평가·연결·고유 파일 수를 분리. 품목별 적용 가능한 속성으로 분모 정의 |

## 준비 상태·공통 규칙: 전체 9항목

| ID | 제안 항목 | 반영 규칙·후속 위치 |
|---|---|---|
| R01 | catalog_ready | Gold에서 식별·이름·유효 서비스 품목·상품 상태 및 사유로 판정. Silver mapping 미비는 보존 후 품질 대기 |
| R02 | recommendation_ready | catalog 준비 + 현재 유효 이미지 연결 + 목적별 검수 + 호환 벡터/인덱스 + 현재 링크를 기존 서비스 계약과 대조. 다운로드 성공만으로 true 금지 |
| R03 | comparison_link_ready | 상품과 대응하는 구조뿐 아니라 외부 현재 상태/유효기간 확인 필요. 미확인은 false + 사유 |
| R04 | purchase_link_ready | 안정 판매 항목·옵션·가격 조건·최종 직접 링크가 확인돼야 함. bridge 링크만으로 true 금지 |
| R05 | 요청별 MATCH/DIFFERENT/UNKNOWN | Silver/Gold의 결측과 신뢰 상태를 전달하고 요청별 판단은 추천 담당. 가격·치수 없음만으로 전체 추천 제외하지 않으며 추정값으로 필수 조건 통과 금지 |
| C01 | 행 기준·관계 | 상품/속성/가격/이미지/링크/문제를 별도 grain으로 저장. 이미지×가격 Cartesian join 금지. 계획된 6개 테이블 및 기존 normalized_offers·pipeline_evaluations 경로를 명시 |
| C02 | 안정 키·반복 관측 | Raw·관측·평가·자식 evidence 키 분리. 같은 평가 재실행은 재사용, 다른 시점 관측 보존. 이름/이미지 유사성으로 출처 상품 병합 금지 |
| C03 | 상태·NULL | CONFIRMED·NOT_PROVIDED·EXTRACTION_FAILED·CONFLICT·SEMANTICS_UNKNOWN 등을 구분. 값 확인/파일 상태/승인/서비스 준비 상태를 서로 대체하지 않음 |
| C04 | Gold→MySQL·운영 경계 | Gold Delta 버전 고정→staging→건수/키/FK/최신성 검증→기존 UUID 재사용. 사용자·추천·검수 원장 편집 및 단순 수집 누락에 따른 삭제 금지. 서버 가이드를 단계별 갱신 |

총 46개 추적 항목(S/P/A/I 각 5개, T 6개, G 11개, R 5개, C 4개)을 정의했다. 이는 전체 코드 구현률 100%가 아니라 **제안서의 설계 반영 위치를 모두 식별했다는 의미**다.

2026-10-17 구현 근거: S/P/A/I/T의 Silver 처리는 `silver/normalize.py`, exact 값 처리는 `values.py`, 담당 입력·유효성·vector는 `inputs.py`, 저장/receipt/재평가는 `delta.py`·`runner.py`·`checkpoint.py`에 구현했다. `test_silver*`와 `tests.smoke_silver_delta`가 근거다. 코드를 추가한 것이 값 확보율 100%나 실제 서버 인수 완료를 뜻하지 않는다. G/R의 Gold 후보 구성·준비 판정은 구현했다. C04의 고정 export→MySQL 게시는 문서 계약 기반 [게시기](mysql_publication.md)로 구현하고 격리 MySQL로 검증했다. 실제 서비스 DDL/guard와 담당 입력 인수는 남아 있다. Silver는 미확인 근거를 추정하지 않고 상태로 전달한다.

Gold의 실제 지원 한계·운영 명령은 [실행 문서](gold_execution.md), 전체 요구와 남은 개발은 [누락 대조](issue13_acceptance.md)를 따른다. 이 표의 설계 반영을 서비스 적재/승인 완료로 해석하지 않는다.
