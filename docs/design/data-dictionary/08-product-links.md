# 상품·판매 정보와 추천 연결 속성

2026-10-15: 원천·상품·링크의 공통 필드는 유지한다. 신규 추천 연결·크기 추정·오프라인 특징은 [직접 추천 속성](09-direct-recommendation.md)을 따른다. 생성/템플릿을 전제로 한 예시는 이전 이력이며 AI 추정으로 상품 실측·가격·링크를 덮어쓰지 않는다.

내부 상품·판매 조건과 추천 후 연결 정보를 구분하고 이미지의 출처를 추적한다. 필드와 예시는 프로젝트 설계이며 sample 값은 검수용 가상 값이다.

[상품 연결 계약](../09-product-links.md) · [템플릿 자료](../10-domeggook-reuse.md) · [모델](../03-data-model.md) · [사전 안내](index.md)

## products

### products source_code

이번 서비스 상품정보의 실제 원천 출처는 다나와다. 추후 다른 원천을 추가할 때도 실제 출처를 보존한다.

- 형식: 문자열
- 예시: `danawa`

### products source_product_id

다나와에서 확인한 상품번호. 내부 product UUID와 URL 자체를 구분한다.

- 형식: 문자열
- 예시: `19170035`

### products comparison_url

해당 다나와 상품번호와 대응을 확인한 개별 상품 확인·가격비교 URL. 상품번호와 URL은 별도 필드이며 미확정 시 NULL을 허용한다. 광고 서버가 추천 및 클릭 시 검증한다.

- 형식: HTTPS URL 또는 NULL
- 예시: `https://prod.danawa.com/info/?pcode=19170035`

## offers

### offers source_seller_id

확인된 안정 판매처 식별자. 예시는 가상 값이며 원천에서 검증한다.

- 형식: 문자열 또는 NULL
- 예시: `sample-mall-id`

### offers source_offer_id

쇼핑몰 판매 상품 ID. 안정성이 확인되지 않으면 임의 값을 발명하지 않는다.

- 형식: 문자열 또는 NULL
- 예시: `sample-mall-product-id`

### offers merchant_name

원천에 표시된 쇼핑몰 이름. 제조사·브랜드와 구분한다.

- 형식: 문자열
- 예시: `표본 쇼핑몰`

### offers price_basis

price_amount는 확인한 일반 판매가격. 조건부 혜택 가격은 별도 JSON에 보존한다.

- 형식: REGULAR / UNKNOWN
- 예시: `REGULAR`

### offers price_conditions_json

혜택 금액·카드/회원/쿠폰 조건과 추출 근거. 혜택 가격을 일반 가격으로 복제하지 않는다.

- 형식: JSON
- 예시: `{"benefit_prices":[],"observed_label":"일반가격"}`

### offers option_attributes_json

확인한 옵션과 대응 근거. 한 상품에 여러 옵션이 있으면 대표 사진만으로 옵션을 단정하지 않는다.

- 형식: JSON
- 예시: `{"color":"BEIGE","verified":true}`

### offers delivery_terms_json

무료/유료/지역 설치·배송 조건. 미제공 비용은 0이 아니다.

- 형식: JSON
- 예시: `{"fee":null,"region":"UNKNOWN","raw_text":"배송 조건 원문"}`

### offers url_kind

해당 주소가 실제 판매처 구매 이동인지 비교 페이지인지 구분한다.

- 형식: MERCHANT / COMPARISON
- 예시: `MERCHANT`

### offers url_status

검증 상태. VERIFIED 이전에는 구매 버튼으로 노출하지 않는다.

- 형식: PENDING / VERIFIED / EXPIRED / INVALID
- 예시: `PENDING`

### offers url_verified_at

도착 대상과 상품/옵션 관계를 확인한 시각. 200 응답만으로 확정하지 않는다.

- 형식: UTC datetime 또는 NULL
- 예시: `2026-10-14T01:00:00Z`

## offer_snapshots

### offer_snapshots pricing_terms_json

추천 당시 가격 조건·배송·옵션·구매 URL 검증 상태를 고정한다. 이후 갱신으로 덮어쓰지 않는다.

- 형식: JSON
- 예시: `{"price_basis":"REGULAR","benefit_prices":[],"delivery":{},"url_status":"VERIFIED","parser_version":"fixture-v1"}`

## assets

### assets usage_policy_json

파일 공통 이용 제한. 상품별 승인이나 다른 출처 승인으로 복사하지 않는다.

- 형식: JSON
- 예시: `{"generation":"DENIED"}`

## cross_source_matches (향후 확장)

이번 다나와 단일 원천 추천·상품 URL 연결에는 사용하지 않는다. 다른 판매 원천을 통합할 때 검수할 수 있는 설계 필드다.

### cross_source_matches id

불변 매칭 판정 버전 행의 PK.

- 형식: UUID4
- 예시: `sample UUID`

### cross_source_matches source_code

기존 자료의 실제 출처. 단순 이름 교체를 하지 않는다.

- 형식: 문자열
- 예시: `seller_b`

### cross_source_matches source_product_id

기존 원천 상품번호. 대상 내부 상품 UUID와 구분한다.

- 형식: 문자열
- 예시: `sample-source-id`

### cross_source_matches source_raw_document_id

검증에 사용한 특정 원천 이력. MongoDB와 MySQL 간 FK가 아니다.

- 형식: ObjectId 24자리 문자열
- 예시: `507f1f77bcf86cd799439011`

### cross_source_matches source_asset_id

비교에 사용한 이미지 assets.id. 저장·분석 이용 범위를 별도 확인.

- 형식: UUID FK 또는 NULL
- 예시: `sample UUID`

### cross_source_matches target_product_id

대상 내부 products.id. 원천과 대상 상품·옵션의 대응 근거를 확인.

- 형식: UUID FK
- 예시: `sample UUID`

### cross_source_matches match_type

동일 상품과 유사 추천을 구분. 벡터 유사도만으로 EXACT로 승격하지 않는다.

- 형식: EXACT / SIMILAR / PENDING / REJECTED
- 예시: `SIMILAR`

### cross_source_matches match_version

규칙·입력 변경마다 새 판정 버전. 이전 근거 유지.

- 형식: 문자열
- 예시: `match-v1`

### cross_source_matches match_score

검색 점수. 정확도·동일 상품 확률이 아니다.

- 형식: 실수 또는 NULL
- 예시: `0.8`

### cross_source_matches evidence_json

브랜드/모델/규격/옵션·이미지·원천 hash·방법과 검수 근거.

- 형식: JSON
- 예시: `{"method":"manual_review","model_check":"PENDING"}`

### cross_source_matches review_status

일치 방식과 별도인 검토 상태. 미확인 관계는 서비스 연결 근거에서 제외.

- 형식: PENDING / APPROVED / REJECTED
- 예시: `PENDING`

### cross_source_matches reviewer_id

검수자 users.id. 시스템 제안은 NULL.

- 형식: UUID FK 또는 NULL
- 예시: `sample UUID`

### cross_source_matches reviewed_at

검수 시각. 연결 대상 정보의 현재 유효성을 대신하지 않는다.

- 형식: UTC datetime 또는 NULL
- 예시: `2026-10-14T01:00:00Z`

## product_images

상품·원천 이미지 버전별 연결의 확장 속성이다. 원천·파일 참조는 불변이며 active와 현재 검토 상태의 변경은 감사 이력을 보존한다. 기본 UUID와 asset 참조는 [상품 사전](02-products.md#product_images)을 따른다.

### product_images source_code

원천 제공처 코드.

- 형식: string
- 예시: `danawa`
- 해석: 원천 출처를 대응한다. 상품별 이용 검토의 출처 범위다.

### product_images source_product_id

원천 상품번호.

- 형식: string
- 예시: `12345678`
- 해석: MySQL product_id UUID와 다르다. source_code와 함께 원천 상품을 찾는다.

### product_images image_variant

이미지 종류.

- 형식: string
- 예시: `original`
- 해석: 관측한 이미지 종류를 대응한다. SKU 옵션이 아니다.

### product_images source_url

해당 처리에서 사용한 원본 이미지 URL.

- 형식: text
- 예시: `https://example.invalid/image.jpg`
- 해석: 상품별 원천 참조이며 assets의 공유 객체 키와 구분한다.

### product_images source_version

다운로드 원천 정보의 비교 버전.

- 형식: string
- 예시: `source-v1:sha256-value`
- 해석: URL·실제 제공된 ETag/Last-Modified 등 validator와 계산 규칙·버전을 고정한다. 파일 바이트 checksum과 다르다.

### product_images raw_document_id

다운로드 기준이 된 불변 MongoDB 원천 ID.

- 형식: string
- 예시: `507f1f77bcf86cd799439011`
- 해석: raw_products._id의 24자리 문자열이며 DB 간 FK가 아니다. 원천 이력 조회를 검증한다.

### product_images raw_payload_hash

다운로드 기준 원천 본문의 해시.

- 형식: string
- 예시: `sha256-value`
- 해석: raw_document_id의 payload_hash를 고정한다. 이미지 파일 해시가 아니다.

### product_images raw_collected_at

다운로드 기준 상품 원천의 수집 시각.

- 형식: datetime
- 예시: `2026-10-14T00:00:00Z`
- 해석: 파일 저장 시각·상품 연결 시각과 구분하고 UTC로 보존한다.

### product_images linked_at

이 버전의 상품과 파일을 연결한 시각.

- 형식: datetime
- 예시: `2026-10-14T00:01:00Z`
- 해석: 파일 재사용에서도 새 연결의 시각을 기록한다. 기존 파일 downloaded_at을 복사하지 않는다.

### product_images active

현재 사용할 이미지 연결 여부.

- 형식: boolean
- 예시: `true`
- 해석: 상품·출처·원천 상품번호·이미지 종류 단위로 현재 연결 하나만 유지한다. 판매·이용 승인 완료를 뜻하지 않는다.

### product_images usage_review_json

상품·원천 이미지 버전별 이용 검토.

- 형식: json
- 예시: `{"storage":{"status":"APPROVED"},"display":{"status":"PENDING"},"embedding":{"status":"PENDING"},"generation":{"status":"DENIED"}}`
- 해석: 용도별 scope·검토자·시각·만료·근거·감사 event_id·source_version·permission_snapshot_hash를 포함한다. 상태 예시는 근거 없는 승인 기본값이 아니다. 상품 원본의 초기 생성 이용은 금지하며 검토 변경 이력을 보존한다.

## 실버·골드 가격·이미지·링크 준비

2026-10-16 입력 경로와 상세 규칙은 [실버·골드 필드 계약](01-raw-delta.md#실버골드-필드-계약-적용-범위)을 따른다. `price.min`·목록 가격은 목록 최저가 관측이며 `price.total`, `member`, `memberTotal`과 의미 미확인 `initial`·`initialTypeName`·`promotionMinPriceQ`를 같은 일반가로 합치지 않는다. formatted 문자열은 숫자 대조용 원문이며 충돌은 품질 문제로 남긴다.

`gold_product_links`의 `comparison_link_ready`는 비교 페이지 대응·현재 상태 검증, `gold_offers_current`의 `purchase_link_ready`는 판매처·옵션·조건·직접 주소 검증이다. `catalog_ready`나 다운로드 성공으로 링크를 승인하지 않는다. 미준비는 버튼을 비활성화하고 상태별 사유를 보존한다. TTL·대표가격 선택 정책·동일 시각 충돌 우선순위는 팀이 확정한다.

`image.version`은 `source_image_version`으로 보존하고 바이트 checksum과 분리한다. 로컬 파일 경로는 `asset_ref`의 배포 저장소 참조로 대응하되 URL/경로 존재만으로 다운로드 성공을 표시하지 않는다. 파일 디코딩·해시·크기 대조와 상품별 이용 범위·원천 버전은 별도로 확인한다.

목록 가격은 `price_kind`·조건·관측 시각을 명시해 표시할 수 있다. Offer가 없다는 이유로 확인된 목록 가격 관측까지 폐기하지 않으며, 이를 최종 결제금액이나 판매처 일반가로 표시하지 않는다. 준비 상태·추가 가격 필드의 실제 서비스 저장은 [원장 대응](../03-data-model.md#실버골드와-서비스-원장-대응)을 먼저 확정한다.
