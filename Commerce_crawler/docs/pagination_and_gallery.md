# POST 페이지 순회와 상품 갤러리

## 확인한 요청과 제어 방식

확인한 목록 요청은 `POST https://prod.danawa.com/list/?cate=1523647`이다. 본문은 배열이며 첫 객체에 categoryCode="3647", page, listCount=30, sortMethod="BEST", viewMethod="LIST", physicsCate1="1826", physicsCate2="32303", physicsCate3="32346" 등이 있다. 사용자 제공 전체 본문은 `config/danawa_pagination.example.json`에 보존했다. "$undefined"는 캡처한 문자열 그대로 보내며 null이나 누락 필드로 바꾸지 않는다.

같은 의자 카테고리 안에서는 URL의 cate와 본문의 카테고리·정렬·필터 조건을 유지하고 page만 바꾼다. 기본 5개 카테고리의 categoryCode와 physicsCate* 대응은 각 목록 HTML과 실제 페이지 요청으로 검증했다. 나머지 카테고리는 별도 확인이 필요하며 URL 접두 숫자를 제거해 추측하거나 의자 본문을 재사용하지 않는다.

응답에는 `0:{...}` 메타 레코드와 `1:{"success":true,"data":{"products":[...]}}` 결과 레코드가 있다. 파서는 번호가 붙은 JSON 레코드 또는 단일 JSON에서 success=true/data.products 배열을 찾는다. success=false, productList 보강 응답, 일반 HTML, 잘못된 products 타입은 정상 빈 마지막 페이지로 처리하지 않는다. 임의의 React Flight 바이너리·참조 스트림 전체를 지원하는 파서는 아니며 형식이 바뀌면 파싱 실패로 남긴다.

Next-Action, Accept=text/x-component, Origin, Referer를 포함한 요청 헤더와 제공 본문으로 Python에서 실제 POST를 확인했다. Next-Action은 브라우저의 Request Headers에서 확보한 값이다. Response Headers의 text/x-component는 이 값의 대체가 아니다. 작업 ID는 사이트 배포에 따라 바뀔 수 있으므로 응답 형식이 달라지면 새 성공 요청과 대조한다. 쿠키·Authorization은 저장하지 않으며 실제 헤더는 Git 제외된 local.json에 관리한다. 요청 로그에는 Method와 본문을 기록하고 헤더는 기록하지 않는다.

## 구현 범위와 종료 조건

- 카테고리별 start_page, max_pages, limit과 전체 total-limit/max-requests를 함께 적용한다.
- 페이지마다 같은 URL로 POST하고 본문의 page만 바꾼다. 실패 POST도 예산에 포함하고 자동 재시도·리다이렉트 추적은 하지 않는다.
- 페이지 간 중복 상품은 source + product_id 기준으로 제외한다. 카테고리별 limit은 고유 선택 상품 수다.
- 명시적인 빈 products 배열, 새 상품 없는 반복 페이지, 페이지/상품/요청 한도에서 종료한다. 요청·파싱 실패 시 해당 카테고리를 멈추고 이미 선택한 상품을 유지한다.
- run_report.categories[].pages[]에 요청한 페이지, 선택 수, 응답 상품 수, 오류를 기록한다.
- --listing-only에서는 상세 요청을 하지 않고 detail_status=disabled로 표시한다. 일반 수집은 갤러리를 얻기 위해 상품당 상세 요청을 최대 한 번 수행한다.
- 템플릿이 없는 기본 첫 페이지 GET은 기존 HTML 선택자를 유지한다. 템플릿이 있는 카테고리는 시작 페이지부터 구조화된 POST 응답을 사용한다.
- 저장은 실행 종료 시점이다. 페이지별 체크포인트·프로세스 강제 종료 후 자동 재개·실행 간 upsert는 미구현이다. 대량 전체 순회 완료를 보장한 것은 아니다.

POST 상품의 productCode, name.full/maker/brand/model, description.pc.raw, price.min, image.url, dates.register는 실제 응답에서 확인한 경로만 매핑한다. 전체 상품 객체는 payload.listings[].api_product에 보존한다. 날짜 make 필드의 의미를 등록일로 단정하지 않으며 두 자리 등록 연도도 세기를 추측하지 않는다. API 목록 이미지는 갤러리 배열에 합치지 않는다.

## 상품 갤러리만 연결

schema_version=1.2에서는 products.images만을 상세 상품 갤러리 기준으로 구성한다. 기존 목록 이미지와 모든 JSON-LD 이미지를 합치는 방식에서 변경했다.

1. 저장된 실제 HTML에서 확인한 #basicThumbSlideArea img와 #baseImage를 읽는다.
2. data-original/data-src/data-lazy-src/src 중 유효한 HTTP(S) 주소를 읽고 상대 URL을 절대 주소로 만든다.
3. 같은 파일의 썸네일과 표시 이미지를 연결하고 중복 제거한다. 크기/버전 파라미터 제거는 식별 비교용이며 저장 URL을 임의로 바꾸지 않는다.
4. 식별자가 확인된 Product.image에 같은 사진의 관측 주소가 있으면 그 주소를 사용한다. 없으면 실제 표시/썸네일 URL을 그대로 사용한다. 원본 해상도라고 단정하지 않는다.
5. 상세 설명 영역이나 임의의 JSON-LD 이미지를 갤러리에 추가하지 않는다. 갤러리 HTML이 없으면 images는 null로 두고 목록 이미지는 listing_image_url에 별도 보존한다.

images[]에는 url, role, order, origin, selector, observed_url, resolution_status가 있다. #baseImage와 같은 사진은 representative, 그 외는 gallery다. role은 촬영 방향을 뜻하지 않는다. order는 관측 썸네일 순서다. 기본 수집은 URL만 저장하며 선택 다운로드 기능은 이미지 내재화 문서를 따른다.

상품 ID·name·product_url·images는 같은 상품 객체에 저장한다. 원본 detail.gallery_images에는 같은 추출값과 gallery_status를 남긴다. 원본 상세 HTML에는 설명 영역이 포함될 수 있으나 설명 이미지를 정규화 사진 배열에 등록하지 않는다. 사용자 예시의 #expandImgArea를 모든 사진의 컨테이너로 가정하지 않는다.

## 검증 근거

- 2026-10-14 실제 POST 2회로 의자 8·9페이지 각각 30개, 중복 없는 상품 총 60개를 수집했다. 두 요청 모두 HTTP 200, success=true와 products 배열을 반환했다. 상세·이미지 다운로드·다른 카테고리 요청은 하지 않았다.
- 8페이지 첫 상품은 productCode=16272926, 스틸케이스 제스쳐 헤드레스트 의자이며 사용자 브라우저 관측과 일치했다. 9페이지 첫 상품은 productCode=98771117, 제닉스 제로드 소프체어 프로 발받침 컴퓨터 게이밍의자였다.
- 최초 POST 실행 ID는 `42e31da9-0b41-4e70-add1-2dfb3f0092b8`이다. 이후 가격·목록 이미지 경로 보강은 저장된 API 객체를 네트워크 없이 재처리해 검증한다.
- 기존 실제 10개 상품의 저장 상세 HTML을 새 추출기로 네트워크 없이 재처리했다. 사진 수는 순서대로 4, 4, 3, 4, 4, 4, 1, 4, 1, 2장이었다. 상품 URL·ID·원본 연결 해시를 대조했다. 이 재추출 결과는 `cef1193b-8b79-407a-a0ae-2ef4879c0d9a`에 있다.
- 기존/신규 오프라인 테스트 총 36개 통과. page만 변경, 본문 보존, 겹치는 상품, 반복/빈 페이지, 시작/최대 페이지, 카테고리/요청 한도, HTTP 200 파싱 실패, 템플릿 누락, API 원본 재추출, 갤러리·설명 이미지 분리, 썸네일 URL 보존을 확인했다.
- 가격·목록 이미지 매핑 보강 후 같은 60개 API 객체를 추가 HTTP 요청 없이 재처리한 실행은 `20a2523f-0f0e-48a8-876e-15a5343b22e6`이다. 60건 모두 숫자 가격과 목록 이미지 참조를 확보했다. 목록만 수집했으므로 갤러리 images/image_urls는 null이고 listing_image_url만 있다.

이후 기본 5개 각각의 1→2페이지 실 검증을 보완했다. 메타데이터 확인 GET 5회+페이지 POST 10회로 총 300개 상품을 확보했고 실사이트 테스트 6개가 통과했다. 5개 실제 요청 메타데이터를 공용 템플릿과 작은 회귀 표본으로 보존했다. 상세 내용은 [5개 카테고리 검증](five_category_pagination_validation.md)을 따른다.

71개 후보는 config/danawa_categories_71.json으로 분리했다. 기본 5개 설정을 유지하고 71개 전체 실행은 수행하지 않았다. 상위/세부 분류 중복과 제외 대상 혼입은 별도 판정이 필요하다. 마지막 페이지까지 또는 71개 모두의 페이지 순회를 검증한 것은 아니다.
