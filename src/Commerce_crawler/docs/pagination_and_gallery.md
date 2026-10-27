# POST 페이지 순회와 상품 갤러리

2026-10-16 구현 기준이다. 과거의 5개 카테고리 실험은 [원기록](five_category_pagination_validation.md), 이후 실제 데이터 검사는 [검증 기록](validation_results.md)을 따른다.

## 목록 요청을 확인한 과정

브라우저에서 페이지 버튼을 눌러도 주소는 바뀌지 않았다. Network에서 같은 `list/?cate=1523647`로 전송한 **POST의 전체 Request Payload**를 비교해 `page` 값을 확인했다. Response Headers의 `content-type`만으로는 요청을 재현할 수 없으며 Request Headers의 Next-Action과 성공한 본문이 필요했다.

다음은 당시 관측 구조다. 카테고리 값은 의자 예시이며 다른 카테고리에 그대로 복사하지 않는다.

- URL: `https://prod.danawa.com/list/?cate=1523647`
- 본문: 배열의 첫 객체에 `categoryCode="3647"`, `page`, `listCount=30`, 정렬·필터, `physicsCate1..4`, `group`, `depth` 등 포함.
- `"$undefined"`는 캡처된 문자열로 유지한다. null이나 누락으로 임의 변환하지 않는다.
- 같은 카테고리 안에서는 분류·정렬·필터 조건을 유지하고 page만 바꾼다.

공용 예제는 `config/danawa_pagination.example.json`, 5개 실제 분류 관측값은 `config/danawa_pagination_5.json`에 있다. 실제 헤더는 Git 제외된 `config/danawa_pagination.local.json`에서 관리한다. Next-Action은 사이트 배포 후 변경될 수 있으므로 실패 시 새 성공 요청과 대조한다. 요청 로그에는 Method·URL·본문을 남기고 헤더·쿠키·Authorization은 넣지 않는다.

## 71개 카테고리와 메타데이터

기본 CLI는 71개 전체 설정을 사용한다. 템플릿에 없는 카테고리는 자신의 목록 HTML의 `self.__next_f` 문자열 조각에서 viewModel을 복원하고 currentCategory, uiCategoryHierarchyCodes, physicsCategory, multiCategoryYN을 대조한다. 관측한 categoryCode·physicsCate·group·depth 등을 공통 요청 조건에 반영한다.

메타데이터가 부족하면 해당 카테고리를 실패로 기록한다. URL 접두 숫자를 잘라 categoryCode를 만들거나 의자 분류를 다른 URL에 재사용하지 않는다. 이 구현과 별개로 71개 모든 카테고리의 최종 페이지·품목 적합성 검증 완료를 주장하지 않는다.

## 응답 파싱과 순회

응답에는 `0:{...}` 메타 레코드와 `1:{"success":true,"data":{"products":[...]}}` 결과가 올 수 있다. 파서는 번호가 붙은 JSON 레코드 또는 단일 JSON에서 성공 상품 배열을 찾는다. 임의의 React Flight 바이너리·참조 스트림 전체를 해석하는 구현은 아니다.

`success=false`, 상품 배열이 아닌 보강 응답, 일반 HTML, 잘못된 products 타입은 정상적인 마지막 페이지로 취급하지 않는다. 반복·빈 페이지·한도·실패는 보고서에 이유를 남긴다.

- 카테고리 `start_page/max_pages/limit`과 전체 상품·요청 한도를 적용한다. 현재 기본 한도는 없음이며 CLI 한도의 0도 무제한이다.
- 서로 다른 카테고리는 `--workers`로 병렬화하고 같은 카테고리의 다음 페이지는 이전 결과 처리 후 요청한다.
- 겹치는 상품은 source + product_id로 병합하고 페이지·카테고리 관측값은 보존한다.
- 명시적인 빈 products, 새 상품 없는 반복 페이지에서 카테고리 순회를 끝낸다. 반복 응답 종료가 사이트 전체 확보 증거는 아니다.
- 요청·파싱 실패는 카테고리를 멈춘다. HTTP 403/429 후에는 새 목록·상세 요청을 중단한다.
- 저장 완료한 페이지는 재개 시 건너뛰고 실패·미처리 페이지를 다시 요청한다. 응답 후 저장 전 강제 종료된 페이지는 반복할 수 있다.

템플릿 없는 수동 어댑터의 첫 페이지 GET은 기존 HTML 카드 선택자를 유지한다. 전체 목록 HTTP 응답을 보존하는 방식은 아니며 GET은 카드 HTML, POST는 `payload.listings[].api_product` 전체 객체를 보존한다. [필드 구분](data_fields.md)

## 상세 상품 식별과 갤러리

상세 JSON-LD Product를 상품 ID·URL·sku 또는 유일한 상품명으로 식별한다. 다른 상품 ID를 명시한 후보는 제외한다. JSON-LD 오류는 원문과 함께 보존하고 HTML 추출을 계속한다.

1. `#basicThumbSlideArea img`, `#baseImage`에서 실제 갤러리를 관측한다.
2. data-original/data-src/data-lazy-src/src의 유효 URL을 읽고 상대 URL은 절대 URL로 정리한다.
3. 같은 사진의 썸네일·표시 이미지를 비교해 중복 제거한다. 크기·버전 파라미터 제거는 식별 비교에만 사용한다.
4. 식별된 Product.image에 같은 사진의 관측 URL이 있으면 그 rendition을 사용한다. 예를 들어 실제 관측한 500px 주소를 사용하되, URL 문자열만 고쳐 원본 해상도를 추정하지 않는다.
5. 상세설명 영역과 임의의 JSON-LD 이미지는 갤러리에 추가하지 않는다. 갤러리가 없으면 images는 null이며 목록 썸네일은 listing_image_url에만 남긴다.

이미지의 `url/role/order/origin/selector/observed_url/resolution_status`는 수집 시 기록한다. role의 representative/gallery는 촬영 방향 라벨이 아니다. 사진 수는 상한 없이 관측한 갤러리를 수집한다. `#expandImgArea`가 모든 사진을 직접 가진 컨테이너라고 가정하지 않는다.

원본 상세 HTML에는 상세설명 영역이 포함될 수 있지만 그 이미지가 다운로드 배열에 들어가는 것은 아니다. 기본 수집은 URL 확보이며, 실제 파일·해시·경로는 [이미지 단계](image_localization.md)에서 기록한다.

## 구현과 실험의 구분

- 2026-10-14 의자 8·9페이지: POST 2회, 각 30개, 총 60개. 상세·사진 다운로드 없음.
- 같은 날 5개 카테고리 1→2페이지: 목록 300개, POST 10회와 메타데이터 GET 5회. 별도 유한 실험이다.
- 이후 기본 실행을 71개 무제한으로 확장하고 자동 분류 준비·병렬·파일 복구를 추가했다.
- 2026-10-15~16 실제 배치의 상세·이미지·NAS 검사와 한계는 [검증 기록](validation_results.md)에 별도로 남긴다.

합성 응답 테스트 통과, 실제 목록 응답 관측, 저장 HTML의 오프라인 재추출, 사진 바이트 검사는 각각 다른 검증이다. 실제 사이트 전체 수집이나 모든 필드 확보로 합산하지 않는다.
