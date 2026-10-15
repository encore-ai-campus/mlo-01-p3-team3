# 원천과 Delta 속성 사전

2026-10-15: 원천·상품·링크의 공통 필드는 유지한다. 신규 추천 연결·크기 추정·오프라인 특징은 [직접 추천 속성](09-direct-recommendation.md)을 따른다. 생성/템플릿을 전제로 한 예시는 이전 이력이며 AI 추정으로 상품 실측·가격·링크를 덮어쓰지 않는다.

현재 논리 설계의 각 속성에 무엇을 저장하는지 설명한다. 형식·값·상태 코드는 후보이며, 예시는 테스트 설명용으로 실제 계정이나 상품을 뜻하지 않는다.

[사전 안내](index.md) · [데이터 모델](../03-data-model.md) · [전체 ERD](../03-erd-full.md)

## 엔터티 바로가기

- [raw_products](#raw_products): 제공처에서 수집한 원천 문서

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

- 예시: `danawa:19170035:beige`
- 해석: MySQL offers.offer_key와 같은 규칙을 사용한다.

### silver normalizer_version

적용한 정규화 규칙의 버전을 저장한다.

- 예시: `normalizer-v1`
- 해석: 가격·단위·분류 변환 로직이 바뀌면 재처리 범위를 확인한다.

### silver processing_run_id

이 결과를 만든 가공 실행의 식별자을 저장한다.

- 예시: `e3835573-86c9-4138-b036-b95ce2bb3aa3`
- 해석: MySQL pipeline_runs.id의 UUID를 하이픈 포함 문자열로 보존한 논리 참조이며 Delta FK가 아니다. 재처리·이관에서도 원래 실행의 참조를 유지한다.

### gold product_key

통합한 상품과 옵션의 안정적인 키을 저장한다.

- 예시: `product-chair-a-beige`
- 해석: MySQL products.product_key와 일치시켜 반영한다.

### gold offer_key

통합 상품에 연결한 판매 항목 키을 저장한다.

- 예시: `danawa:19170035:beige`
- 해석: MySQL offers 반영과 동일 판매처 구분의 기준이다.

### gold eligible

서비스 추천 대상으로 사용할 수 있는지을 저장한다.

- 예시: `true`
- 해석: 필수 정보와 품질 조건을 반영한 값이며 모델 정확도 점수가 아니다.

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
