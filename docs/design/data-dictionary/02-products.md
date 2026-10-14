# 상품과 판매 속성 사전

현재 논리 설계의 각 속성에 무엇을 저장하는지 설명한다. 형식·값·상태 코드는 후보이며, 예시는 테스트 설명용으로 실제 계정이나 상품을 뜻하지 않는다.

[사전 안내](index.md) · [데이터 모델](../03-data-model.md) · [전체 ERD](../03-erd-full.md)

## 엔터티 바로가기

- [categories](#categories): 서비스가 지원하는 표준 가구 카테고리
- [products](#products): 동일 모델과 옵션으로 통합한 상품
- [offers](#offers): 상품의 판매처별 현재 판매 정보
- [offer_snapshots](#offer_snapshots): 특정 수집 시점의 판매 정보
- [assets](#assets): Object Storage에 저장한 파일의 메타데이터
- [product_images](#product_images): 상품과 이미지 파일의 연결
- [category_mappings](#category_mappings): 제공처 카테고리와 표준 카테고리의 매핑
- [product_identity_reviews](#product_identity_reviews): 동일 상품 여부에 관한 검수 판단

## categories

이 엔터티는 서비스가 지원하는 표준 가구 카테고리에 해당한다. 같은 이름의 속성이라도 아래 업무 의미를 기준으로 해석한다.

### categories id

서비스가 지원하는 표준 가구 카테고리 한 건의 UUID 기본키.

- 형식 후보: UUID (Django UUIDField, MySQL CHAR(32))
- 예시: `ceb96b55-ee4d-46fa-9204-9db0c5f07a2d`
- 해석: 신규 행 생성 시 UUID4를 한 번 발급한다. API와 메시지에도 같은 id를 사용하며 재처리·이관에서는 보존한다. 제공처 ID와 이벤트 재전송 식별자와는 용도가 다르다. UUID만으로 접근 권한을 보장하지 않는다.

### categories code

표준 가구 카테고리의 고유 코드.

- 형식 후보: 문자열
- 예시: `CHAIR`
- 해석: AI 출력과 상품 분류에 함께 사용하며 변경 시 기존 매핑도 고려한다.

### categories name

화면에 표시할 카테고리 이름.

- 형식 후보: 문자열
- 예시: `의자`
- 해석: 상품 이름이나 제공처 카테고리 원문이 아니다.

### categories recognition_enabled

스케치 인식 지원 대상 여부.

- 형식 후보: 참 또는 거짓
- 예시: `true`
- 해석: false이면 상품 카테고리로 존재해도 현재 인식 후보 목록에 넣지 않는다.

### categories active

현재 서비스에서 사용할 카테고리 여부.

- 형식 후보: 참 또는 거짓
- 예시: `true`
- 해석: 인식 지원 여부와 별개다. 비활성화 시 조회·추천 정책도 맞춰야 한다.

### categories created_at

서비스가 지원하는 표준 가구 카테고리 행을 DB에 처음 등록한 시각.

- 형식 후보: UTC 날짜와 시각
- 예시: `2026-10-12T01:00:00Z`
- 해석: UTC로 저장한다. collected_at·occurred_at·started_at 등 업무 발생 시각과 다를 수 있다.

### categories updated_at

서비스가 지원하는 표준 가구 카테고리 행을 마지막으로 수정한 시각.

- 형식 후보: UTC 날짜와 시각
- 예시: `2026-10-12T01:01:00Z`
- 해석: UTC로 저장한다. 행의 최종 갱신 시각이며 원천 가격의 수집 시각이나 파일 내용 변경 시각을 대신하지 않는다.

## products

이 엔터티는 내부 서비스의 추천 상품에 해당한다. 출처별 안정 식별자를 유지하며 서로 다른 상품을 시각적 유사성으로 병합하지 않는다. 같은 이름의 속성이라도 아래 업무 의미를 기준으로 해석한다.

### products id

내부 상품 행의 UUID 기본키.

- 형식 후보: UUID4
- 예시: `e98225a5-5312-48ac-b2c4-d92556705f8a`
- 해석: 원천 상품번호 및 외부 연결 식별자와 구분한다. 새로운 행에만 발급하고 재적재·이관에서 보존한다.

### products product_key

출처와 원천 상품 식별자로 구성한 안정 키.

- 형식 후보: 문자열
- 예시: `danawa:19170035`
- 해석: 원천 상품번호 기준이며 색상/옵션 범위는 원천에서 확인한다. 재반영은 같은 키로 UUID를 보존한다.

### products name

서비스에서 보여줄 표준 상품명.

- 형식 후보: 문자열
- 예시: `베이지 원목 의자 A`
- 해석: 판매처마다 다른 offers.title과 구분한다.

### products category_id

상품의 표준 카테고리의 내부 ID.

- 형식 후보: UUID (Django UUIDField, MySQL CHAR(32))
- 예시: `ceb96b55-ee4d-46fa-9204-9db0c5f07a2d`
- 해석: [categories.id](02-products.md#categories-id)를 참조하는 MySQL FK다. 참조 대상 PK와 같은 UUID 타입을 사용한다.

### products brand

제공처 정보에서 검증한 브랜드.

- 형식 후보: 문자열
- 예시: `브랜드 A`
- 해석: 확보하지 못하면 NULL이다. 이미지의 추측만으로 사실값을 넣지 않는다.

### products model_name

상품 모델명 또는 제조사 모델 코드.

- 형식 후보: 문자열
- 예시: `CHAIR-A`
- 해석: 동일 상품 판단의 근거다. product_key 자체는 아니다.

### products color

해당 상품 옵션의 색상.

- 형식 후보: 문자열
- 예시: `베이지`
- 해석: 제공처에서 확보한 옵션을 기록한다. 스케치 색상 자동 추론 기능을 뜻하지 않는다.

### products size_label

제공처의 크기 옵션 이름.

- 형식 후보: 문자열
- 예시: `기본형`
- 해석: 가로·세로 실측 수치와 구분한다. 옵션이 없으면 NULL 가능하다.

### products width_mm

검증한 상품 가로 길이를 mm로 바꾼 값.

- 형식 후보: 소수 DECIMAL 후보
- 예시: `600`
- 해석: 규격 단위와 축 의미가 확인됐을 때만 저장한다. 미제공은 NULL이다.

### products depth_mm

검증한 상품 깊이를 mm로 바꾼 값.

- 형식 후보: 소수 DECIMAL 후보
- 예시: `550`
- 해석: 사진 속 크기나 스케치 상대 크기가 아니다. 미제공은 NULL이다.

### products height_mm

검증한 상품 높이를 mm로 바꾼 값.

- 형식 후보: 소수 DECIMAL 후보
- 예시: `800`
- 해석: 실제 공간 설치 가능성을 자동 보증하지 않는다. 미제공은 NULL이다.

### products dimensions_original

정규화 근거가 된 상품 규격 원문.

- 형식 후보: 긴 문자열
- 예시: `가로 60 × 깊이 55 × 높이 80cm`
- 해석: 판매처끼리 값이 충돌하면 검수하며 원천 ID로 근거를 추적한다.

### products status

서비스 상품의 활성 상태.

- 형식 후보: 문자열
- 예시: `ACTIVE`
- 해석: ACTIVE·INACTIVE 등은 후보 값이다. 판매처 재고 availability와 다르다.

### products dedup_status

동일 상품 통합 판단의 상태.

- 형식 후보: 문자열
- 예시: `REVIEW_REQUIRED`
- 해석: 통합 검증 여부나 검수 필요 여부를 나타낸다. 최종 값 집합은 구현 전 정한다.

### products created_at

동일 모델과 옵션으로 통합한 상품 행을 DB에 처음 등록한 시각.

- 형식 후보: UTC 날짜와 시각
- 예시: `2026-10-12T01:00:00Z`
- 해석: UTC로 저장한다. collected_at·occurred_at·started_at 등 업무 발생 시각과 다를 수 있다.

### products updated_at

동일 모델과 옵션으로 통합한 상품 행을 마지막으로 수정한 시각.

- 형식 후보: UTC 날짜와 시각
- 예시: `2026-10-12T01:01:00Z`
- 해석: UTC로 저장한다. 행의 최종 갱신 시각이며 원천 가격의 수집 시각이나 파일 내용 변경 시각을 대신하지 않는다.

## offers

이 엔터티는 상품의 판매처별 현재 판매 정보에 해당한다. 같은 이름의 속성이라도 아래 업무 의미를 기준으로 해석한다.

### offers id

상품의 판매처별 현재 판매 정보 한 건의 UUID 기본키.

- 형식 후보: UUID (Django UUIDField, MySQL CHAR(32))
- 예시: `5348ff9c-5755-4b96-a834-36e1ea766a16`
- 해석: 신규 행 생성 시 UUID4를 한 번 발급한다. API와 메시지에도 같은 id를 사용하며 재처리·이관에서는 보존한다. 제공처 ID와 이벤트 재전송 식별자와는 용도가 다르다. UUID만으로 접근 권한을 보장하지 않는다.

### offers offer_key

추천 상품의 판매처별 판매 항목 키.

- 형식 후보: 문자열
- 예시: `danawa:19170035:sample-mall:sample-offer:UNSPECIFIED`
- 해석: 판매처/판매 상품/옵션은 안정성을 확인한다. 예시는 가상 값이며 배열 위치·가격·일시적 클릭 URL을 키로 쓰지 않는다.

### offers product_id

이 판매 항목이 판매하는 통합 상품의 내부 ID.

- 형식 후보: UUID (Django UUIDField, MySQL CHAR(32))
- 예시: `e98225a5-5312-48ac-b2c4-d92556705f8a`
- 해석: [products.id](02-products.md#products-id)를 참조하는 MySQL FK다. 참조 대상 PK와 같은 UUID 타입을 사용한다.

### offers source_code

판매 관측의 정보 출처 코드.

- 형식 후보: 문자열
- 예시: `danawa`
- 해석: 이번 서비스 상품의 출처는 다나와로 보존한다. 실제 판매처는 source_seller_id·merchant_name에 별도로 기록한다.

### offers source_product_id

이 판매 정보의 다나와 원천 상품 식별자. 내부 상품 UUID와 연결 URL을 구분한다.

- 형식 후보: 문자열
- 예시: `19170035`
- 해석: 쇼핑몰 상품 ID는 source_offer_id로 따로 기록한다. 이 필드 하나가 전역 UUID가 아니다.

### offers source_variant_id

판매처가 구분한 옵션 식별자.

- 형식 후보: 문자열
- 예시: `beige-default`
- 해석: 제공하지 않는 경우 누락 표현을 통일해 offer_key를 생성한다.

### offers title

판매처에서 표시한 상품명.

- 형식 후보: 문자열
- 예시: `원목 식탁 의자 A 베이지`
- 해석: 통합 상품명 products.name과 구분하며 구매 페이지 확인에 사용한다.

### offers price_amount

확인한 일반 판매가격.

- 형식 후보: 정수 또는 NULL
- 예시: `340000`
- 해석: 일반가 미제공은 NULL. 카드/회원/쿠폰 가격은 price_conditions_json의 조건부 가격으로 구분한다.

### offers currency

현재 가격의 통화 코드.

- 형식 후보: 문자열
- 예시: `KRW`
- 해석: price_amount의 의미를 결정한다. 다른 통화 확대 시 소수 단위 정책을 별도 설계한다.

### offers purchase_url

선택한 판매 항목의 구매 페이지 주소.

- 형식 후보: 긴 문자열
- 예시: `https://example.com/buy/19170035`
- 해석: 실제 상품·옵션과 일치하는지 검증하며 AI가 생성한 주소를 사용하지 않는다.

### offers availability

최근 수집에서 확보한 판매·재고 상태.

- 형식 후보: 문자열
- 예시: `IN_STOCK`
- 해석: 정보가 없으면 UNKNOWN 등으로 구분하는 후보이다. 구매 시점 재고를 보증하지 않는다.

### offers collected_at

현재 가격·재고 정보를 수집한 시각.

- 형식 후보: UTC 날짜와 시각
- 예시: `2026-10-12T01:00:00Z`
- 해석: 갱신 완료 updated_at과 다르며 가격 최신성 안내에 사용한다.

### offers latest_raw_document_id

현재 판매 정보의 근거가 되는 MongoDB 원천 ID.

- 형식 후보: 문자열
- 예시: `507f1f77bcf86cd799439011`
- 해석: DB 간 추적용 문자열이며 MySQL 외래키가 아니다.

### offers eligible

현재 구매 연결에 사용할 수 있는 판매 항목 여부.

- 형식 후보: boolean
- 예시: `false`
- 해석: 옵션·가격 조건·현재 링크·상품 관계 검증을 확인한다. 비교 상품 자체의 탐색 가능 상태는 products.status와 별도다.

### offers created_at

상품의 판매처별 현재 판매 정보 행을 DB에 처음 등록한 시각.

- 형식 후보: UTC 날짜와 시각
- 예시: `2026-10-12T01:00:00Z`
- 해석: UTC로 저장한다. collected_at·occurred_at·started_at 등 업무 발생 시각과 다를 수 있다.

### offers updated_at

상품의 판매처별 현재 판매 정보 행을 마지막으로 수정한 시각.

- 형식 후보: UTC 날짜와 시각
- 예시: `2026-10-12T01:01:00Z`
- 해석: UTC로 저장한다. 행의 최종 갱신 시각이며 원천 가격의 수집 시각이나 파일 내용 변경 시각을 대신하지 않는다.

## offer_snapshots

이 엔터티는 특정 수집 시점의 판매 정보에 해당한다. 같은 이름의 속성이라도 아래 업무 의미를 기준으로 해석한다.

### offer_snapshots id

특정 수집 시점의 판매 정보 한 건의 UUID 기본키.

- 형식 후보: UUID (Django UUIDField, MySQL CHAR(32))
- 예시: `ae0abc7f-07a6-4d21-a20b-7b30f4907736`
- 해석: 신규 행 생성 시 UUID4를 한 번 발급한다. API와 메시지에도 같은 id를 사용하며 재처리·이관에서는 보존한다. 제공처 ID와 이벤트 재전송 식별자와는 용도가 다르다. UUID만으로 접근 권한을 보장하지 않는다.

### offer_snapshots snapshot_key

원천·판매 항목으로 특정 시점 판매 이력을 식별하는 키.

- 형식 후보: 문자열
- 예시: `danawa:19170035:beige:raw-001`
- 해석: 재처리에서도 같은 이력을 두 번 추가하지 않도록 생성 규칙을 고정한다.

### offer_snapshots offer_id

이 시점의 정보가 속한 판매 항목의 내부 ID.

- 형식 후보: UUID (Django UUIDField, MySQL CHAR(32))
- 예시: `5348ff9c-5755-4b96-a834-36e1ea766a16`
- 해석: [offers.id](02-products.md#offers-id)를 참조하는 MySQL FK다. 참조 대상 PK와 같은 UUID 타입을 사용한다.

### offer_snapshots raw_document_id

이 snapshot의 근거가 된 원천 문서 ID.

- 형식 후보: 문자열
- 예시: `507f1f77bcf86cd799439011`
- 해석: MongoDB 추적용이며 FK가 아니다. 현재 원천이 바뀌어도 이 참조는 과거 근거를 가리킨다.

### offer_snapshots price_amount

이 수집 시점의 판매 가격.

- 형식 후보: 정수 BIGINT 후보
- 예시: `89000`
- 해석: 현재 offers.price_amount가 바뀌어도 추천 당시 이력은 유지한다.

### offer_snapshots currency

snapshot 가격의 통화.

- 형식 후보: 문자열
- 예시: `KRW`
- 해석: 가격과 함께 고정해 과거 정보의 해석을 보존한다.

### offer_snapshots purchase_url

해당 수집 시점에 확인한 구매 주소.

- 형식 후보: 긴 문자열
- 예시: `https://example.com/buy/19170035`
- 해석: 추천·선택 당시 판매처와 상품의 연결을 추적한다.

### offer_snapshots availability

해당 수집 시점의 판매·재고 상태.

- 형식 후보: 문자열
- 예시: `IN_STOCK`
- 해석: 현재 availability와 다를 수 있으며 이력값을 갱신하지 않는다.

### offer_snapshots collected_at

이 가격 이력을 수집한 시각.

- 형식 후보: UTC 날짜와 시각
- 예시: `2026-10-12T01:00:00Z`
- 해석: snapshot 행 생성 시각과 구분한다.

### offer_snapshots created_at

특정 수집 시점의 판매 정보 행을 DB에 처음 등록한 시각.

- 형식 후보: UTC 날짜와 시각
- 예시: `2026-10-12T01:00:00Z`
- 해석: UTC로 저장한다. collected_at·occurred_at·started_at 등 업무 발생 시각과 다를 수 있다.

## assets

이 엔터티는 Object Storage에 저장한 파일의 메타데이터에 해당한다. 같은 이름의 속성이라도 아래 업무 의미를 기준으로 해석한다.

### assets id

Object Storage에 저장한 파일의 메타데이터 한 건의 UUID 기본키.

- 형식 후보: UUID (Django UUIDField, MySQL CHAR(32))
- 예시: `ee47322f-d90a-466a-830b-51d482224f9d`
- 해석: 신규 행 생성 시 UUID4를 한 번 발급한다. API와 메시지에도 같은 id를 사용하며 재처리·이관에서는 보존한다. 제공처 ID와 이벤트 재전송 식별자와는 용도가 다르다. UUID만으로 접근 권한을 보장하지 않는다.

### assets owner_id

사용자 파일의 소유 계정의 내부 ID.

- 형식 후보: UUID (Django UUIDField, MySQL CHAR(32))
- 예시: `1017d7a3-a0f0-436b-a113-e72fee3f4b42`
- 해석: [users.id](03-user-flow.md#users-id)를 참조하는 MySQL FK다. 참조 대상 PK와 같은 UUID 타입을 사용한다. 해당 단계에서 연결이 없으면 NULL을 허용하는 후보이다.

### assets kind

파일의 업무 용도.

- 형식 후보: 문자열
- 예시: `ROOM_PHOTO`
- 해석: ROOM_PHOTO·SKETCH·PRODUCT_IMAGE·PLACEMENT_RESULT·AI_RESULT 등은 후보 분류다.

### assets bucket

파일을 보관하는 Object Storage 버킷 이름.

- 형식 후보: 문자열
- 예시: `project-media`
- 해석: object_key와 합쳐 파일을 찾는다. 이 필드 하나로 고유하지 않다.

### assets object_key

버킷 안의 파일 경로에 해당하는 객체 키.

- 형식 후보: 문자열
- 예시: `products/19170035/front.png`
- 해석: 파일 내용이나 만료되는 서명 URL이 아니다. bucket과 복합 고유키 후보다.

### assets content_type

파일 형식을 나타내는 MIME 값.

- 형식 후보: 문자열
- 예시: `image/png`
- 해석: 업로드 검증값이며 확장자만 보고 확정하지 않는다.

### assets size_bytes

저장한 파일 크기.

- 형식 후보: 정수 BIGINT 후보
- 예시: `245760`
- 해석: 바이트 단위다. 이미지 픽셀 수와 다르다.

### assets width_px

이미지의 가로 픽셀 수.

- 형식 후보: 정수
- 예시: `1024`
- 해석: 실측 상품 규격이 아니다. 이미지가 아닌 파일에서는 NULL 가능하다.

### assets height_px

이미지의 세로 픽셀 수.

- 형식 후보: 정수
- 예시: `768`
- 해석: 실측 상품 규격이 아니다. 이미지가 아닌 파일에서는 NULL 가능하다.

### assets checksum

저장한 파일의 무결성 확인값.

- 형식 후보: 문자열
- 예시: `sha256:def456...`
- 해석: 파일 변경·이관 검증에 사용한다. 해시 알고리즘을 지정하며 ETag를 항상 동일한 해시로 취급하지 않는다.

### assets status

업로드와 파일 사용 가능 상태.

- 형식 후보: 문자열
- 예시: `READY`
- 해석: UPLOADING·READY·FAILED·DELETED 등은 후보다. 실제 객체 존재 확인과 연결한다.

### assets expires_at

보관 정책에 따른 파일 만료 또는 삭제 대상 시각.

- 형식 후보: UTC 날짜와 시각
- 예시: `2026-11-12T01:00:00Z`
- 해석: 서명 URL의 만료 시각이 아니다. 시각 도래만으로 파일이 자동 삭제되는 것은 아니다.

### assets created_at

Object Storage에 저장한 파일의 메타데이터 행을 DB에 처음 등록한 시각.

- 형식 후보: UTC 날짜와 시각
- 예시: `2026-10-12T01:00:00Z`
- 해석: UTC로 저장한다. collected_at·occurred_at·started_at 등 업무 발생 시각과 다를 수 있다.

### assets updated_at

Object Storage에 저장한 파일의 메타데이터 행을 마지막으로 수정한 시각.

- 형식 후보: UTC 날짜와 시각
- 예시: `2026-10-12T01:01:00Z`
- 해석: UTC로 저장한다. 행의 최종 갱신 시각이며 원천 가격의 수집 시각이나 파일 내용 변경 시각을 대신하지 않는다.

## product_images

이 엔터티는 상품·원천 이미지 버전과 공유 파일의 연결에 해당한다. MongoDB 동명 컬렉션의 다운로드 상태 기록과 구분한다. 원천 추적·현재 연결·상품별 이용 검토는 [확장 속성](08-product-links.md#product_images)을 따른다. 같은 이름의 속성이라도 아래 업무 의미를 기준으로 해석한다.

### product_images id

상품과 이미지 파일의 연결 한 건의 UUID 기본키.

- 형식 후보: UUID (Django UUIDField, MySQL CHAR(32))
- 예시: `e6d2d6f6-9d27-4142-824e-3019e18f8dfc`
- 해석: 신규 행 생성 시 UUID4를 한 번 발급한다. API와 메시지에도 같은 id를 사용하며 재처리·이관에서는 보존한다. 제공처 ID와 이벤트 재전송 식별자와는 용도가 다르다. UUID만으로 접근 권한을 보장하지 않는다.

### product_images product_id

이미지를 연결한 상품의 내부 ID.

- 형식 후보: UUID (Django UUIDField, MySQL CHAR(32))
- 예시: `e98225a5-5312-48ac-b2c4-d92556705f8a`
- 해석: [products.id](02-products.md#products-id)를 참조하는 MySQL FK다. 참조 대상 PK와 같은 UUID 타입을 사용한다.

### product_images asset_id

상품을 보여주는 이미지 파일의 내부 ID.

- 형식 후보: UUID (Django UUIDField, MySQL CHAR(32))
- 예시: `ee47322f-d90a-466a-830b-51d482224f9d`
- 해석: [assets.id](02-products.md#assets-id)를 참조하는 MySQL FK다. 참조 대상 PK와 같은 UUID 타입을 사용한다. 이미지 참조여도 파일 본문을 저장하지 않는다.

### product_images display_order

상품 상세에서 이미지를 보여줄 순서.

- 형식 후보: 정수
- 예시: `1`
- 해석: 숫자가 작을수록 먼저 보여주는 규칙 후보다. 배열 위치나 파일명이 아니다.

### product_images created_at

상품과 이미지 파일의 연결 행을 DB에 처음 등록한 시각.

- 형식 후보: UTC 날짜와 시각
- 예시: `2026-10-12T01:00:00Z`
- 해석: UTC로 저장한다. collected_at·occurred_at·started_at 등 업무 발생 시각과 다를 수 있다.

### product_images updated_at

상품과 이미지 파일의 연결 행을 마지막으로 수정한 시각.

- 형식 후보: UTC 날짜와 시각
- 예시: `2026-10-12T01:01:00Z`
- 해석: UTC로 저장한다. 행의 최종 갱신 시각이며 원천 가격의 수집 시각이나 파일 내용 변경 시각을 대신하지 않는다.

## category_mappings

이 엔터티는 제공처 카테고리와 표준 카테고리의 매핑에 해당한다. 같은 이름의 속성이라도 아래 업무 의미를 기준으로 해석한다.

### category_mappings id

제공처 카테고리와 표준 카테고리의 매핑 한 건의 UUID 기본키.

- 형식 후보: UUID (Django UUIDField, MySQL CHAR(32))
- 예시: `77d25b79-e190-4adc-af91-df391dc65147`
- 해석: 신규 행 생성 시 UUID4를 한 번 발급한다. API와 메시지에도 같은 id를 사용하며 재처리·이관에서는 보존한다. 제공처 ID와 이벤트 재전송 식별자와는 용도가 다르다. UUID만으로 접근 권한을 보장하지 않는다.

### category_mappings source_code

매핑 대상 제공처 코드.

- 형식 후보: 문자열
- 예시: `danawa`
- 해석: 제공처마다 같은 원문이 다르게 쓰일 수 있어 함께 식별한다.

### category_mappings source_category

제공처 카테고리 원문 또는 경로.

- 형식 후보: 문자열
- 예시: `홈/가구/식탁의자`
- 해석: 표준 카테고리 code로 바뀌기 전 표현이다.

### category_mappings category_id

원문 카테고리를 변환할 표준 카테고리의 내부 ID.

- 형식 후보: UUID (Django UUIDField, MySQL CHAR(32))
- 예시: `ceb96b55-ee4d-46fa-9204-9db0c5f07a2d`
- 해석: [categories.id](02-products.md#categories-id)를 참조하는 MySQL FK다. 참조 대상 PK와 같은 UUID 타입을 사용한다.

### category_mappings mapping_version

이 매핑 규칙의 버전.

- 형식 후보: 문자열
- 예시: `category-map-v1`
- 해석: Spark의 가공 결과가 어떤 분류 규칙을 적용했는지 추적한다.

### category_mappings active

현재 적용할 매핑인지.

- 형식 후보: 참 또는 거짓
- 예시: `true`
- 해석: 오래된 규칙을 보존하더라도 신규 처리에서 적용하지 않을 수 있다.

### category_mappings created_at

제공처 카테고리와 표준 카테고리의 매핑 행을 DB에 처음 등록한 시각.

- 형식 후보: UTC 날짜와 시각
- 예시: `2026-10-12T01:00:00Z`
- 해석: UTC로 저장한다. collected_at·occurred_at·started_at 등 업무 발생 시각과 다를 수 있다.

### category_mappings updated_at

제공처 카테고리와 표준 카테고리의 매핑 행을 마지막으로 수정한 시각.

- 형식 후보: UTC 날짜와 시각
- 예시: `2026-10-12T01:01:00Z`
- 해석: UTC로 저장한다. 행의 최종 갱신 시각이며 원천 가격의 수집 시각이나 파일 내용 변경 시각을 대신하지 않는다.

## product_identity_reviews

이 엔터티는 동일 상품 여부에 관한 검수 판단에 해당한다. 같은 이름의 속성이라도 아래 업무 의미를 기준으로 해석한다.

### product_identity_reviews id

동일 상품 여부에 관한 검수 판단 한 건의 UUID 기본키.

- 형식 후보: UUID (Django UUIDField, MySQL CHAR(32))
- 예시: `7fffe40a-cb53-4164-bfa2-ddea8ac2cd14`
- 해석: 신규 행 생성 시 UUID4를 한 번 발급한다. API와 메시지에도 같은 id를 사용하며 재처리·이관에서는 보존한다. 제공처 ID와 이벤트 재전송 식별자와는 용도가 다르다. UUID만으로 접근 권한을 보장하지 않는다.

### product_identity_reviews candidate_key

검수할 원천과 통합 상품 후보를 묶은 키.

- 형식 후보: 문자열
- 예시: `raw-001:product-chair-a-beige`
- 해석: 최종 확정된 상품 키와 구분한다. 반복 검수와 후보 연결의 생성 규칙을 정한다.

### product_identity_reviews raw_document_id

검수 근거가 되는 원천 ID.

- 형식 후보: 문자열
- 예시: `507f1f77bcf86cd799439011`
- 해석: MongoDB 추적용이다. 아직 통합되지 않은 상품도 검수할 수 있다.

### product_identity_reviews product_key

검수 대상 통합 상품 후보 또는 승인된 상품 키.

- 형식 후보: 문자열
- 예시: `product-chair-a-beige`
- 해석: 매칭 실패 시 NULL 가능하다. 현재 초안에서는 강제 FK로 표시하지 않는다.

### product_identity_reviews decision

검수자가 기록한 동일 상품 판단.

- 형식 후보: 문자열
- 예시: `SAME_PRODUCT`
- 해석: SAME_PRODUCT·DIFFERENT_PRODUCT·UNRESOLVED 등은 후보다. 추천 선택 이벤트와 다르다.

### product_identity_reviews reviewer_id

상품 동일성 검수를 수행한 운영자의 내부 ID.

- 형식 후보: UUID (Django UUIDField, MySQL CHAR(32))
- 예시: `1017d7a3-a0f0-436b-a113-e72fee3f4b42`
- 해석: [users.id](03-user-flow.md#users-id)를 참조하는 MySQL FK다. 참조 대상 PK와 같은 UUID 타입을 사용한다. 해당 단계에서 연결이 없으면 NULL을 허용하는 후보이다.

### product_identity_reviews review_version

검수 판단의 버전.

- 형식 후보: 문자열
- 예시: `review-v1`
- 해석: 판단 변경을 파이프라인 입력에 반영하고 재현하는 기준이다.

### product_identity_reviews reviewed_at

검수자가 판단을 확정한 시각.

- 형식 후보: UTC 날짜와 시각
- 예시: `2026-10-12T02:00:00Z`
- 해석: 검수 행 생성 시각과 다를 수 있다.

### product_identity_reviews created_at

동일 상품 여부에 관한 검수 판단 행을 DB에 처음 등록한 시각.

- 형식 후보: UTC 날짜와 시각
- 예시: `2026-10-12T01:00:00Z`
- 해석: UTC로 저장한다. collected_at·occurred_at·started_at 등 업무 발생 시각과 다를 수 있다.

### product_identity_reviews updated_at

동일 상품 여부에 관한 검수 판단 행을 마지막으로 수정한 시각.

- 형식 후보: UTC 날짜와 시각
- 예시: `2026-10-12T01:01:00Z`
- 해석: UTC로 저장한다. 행의 최종 갱신 시각이며 원천 가격의 수집 시각이나 파일 내용 변경 시각을 대신하지 않는다.
