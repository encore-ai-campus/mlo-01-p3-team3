# 5개 카테고리 페이지 전환 검증 — 2026-10-14 원기록

> 이 문서는 당시의 실험 범위·테스트 수를 보존한다. 현재 기본은 71개·무제한이며 최신 동작은 [수집 설계](danawa_collector.md), 이후 결과는 [검증 기록](validation_results.md)을 따른다. 아래 명령은 가상환경의 Python을 사용한다.

2026-10-14 기존 기본 카테고리 5개에서 각각 1→2페이지를 실제 검증했다. 의자 8·9페이지에만 한정됐던 이전 검증을 보완한 기록이다.

## 실제 결과

| 카테고리 | URL cate | 본문 categoryCode | physicsCate2 / physicsCate3 | 1페이지 | 2페이지 | 고유 선택 |
|---|---|---|---|---:|---:|---:|
| 침대 | 15241194 | 41194 | 11241 / 11242 | 30 | 30 | 60 |
| 책상 | 15240504 | 40504 | 11123 / 58840 | 30 | 30 | 60 |
| 학생/사무용의자 | 1523647 | 3647 | 32303 / 32346 | 30 | 30 | 60 |
| 식탁 | 15241240 | 41240 | 11287 / 59859 | 30 | 30 | 60 |
| 선반 | 15227279 | 27279 | 11182 / 11186 | 30 | 30 | 60 |

총 상품 300개, 페이지 POST 10회 모두 HTTP 200과 success=true/products 배열을 확인했다. 카테고리 확인용 GET 5회를 포함해 이번 보완에 사용한 실제 요청은 총 15회다. 상세 요청·이미지 다운로드는 하지 않았다.

POST 테스트에서는 이미 확보한 5개 HTML을 사용했다. 그러므로 해당 실행의 run_report.request_count는 POST 10회이고, 메타데이터 탐색 GET 5회는 별도 requests.json에 기록되어 있다. 기본 재실행은 GET 5회+POST 최대 10회를 한 RequestClient의 15회 예산 안에서 처리한다.

검사 항목:

- 각 카테고리에서 요청한 페이지가 정확히 1·2인지와 두 페이지 모두 상품 응답 30개인지.
- URL은 같은 카테고리로 유지하고 본문에서 page만 바뀌었는지.
- 2페이지에서 새 상품 ID가 추가되는지, 같은 상품을 중복 저장하지 않는지.
- 상품 응답 category.codes가 요청한 physicsCate1/2/3과 일치하는지. 의자 데이터를 다른 URL에서 잘못 받은 결과가 아닌지 확인.
- 5개 카테고리가 모두 처리됐는지, 요청 예산·상세 생략·원본/정규화 연결·출력 파일이 맞는지.

전체 마지막 페이지까지 확인한 것은 아니다. 71개 전체 카테고리로 결과를 일반화하지 않는다.

## 남긴 테스트와 설정

- `tests/test_live_pagination.py`: 실제 사이트를 대상으로 한 5개 카테고리별 테스트와 전체 연결/예산 테스트, 총 6개. 이번 실 실행에서 6개 모두 통과했다.
- `tests/test_five_category_pagination.py`: 저장한 실제 표본으로 5개를 순회하는 회귀 테스트, 메타데이터 일치·스크립트 분할·잘못된/누락된 메타데이터 검사, 총 4개.
- `tests/fixtures/five_category_pagination.json`: 실제 실행 ID·시각, 카테고리별 메타데이터·요청 본문, 각 페이지의 첫 2개 상품 표본과 선택된 ID 목록. 합성 응답과 실제 원문 표본을 구분한다.
- `config/danawa_pagination_5.json`: 5개 실제 메타데이터로 구성한 POST 본문 템플릿. 실제 Next-Action은 공용 파일에 넣지 않는다.
- `config/danawa_categories_5_pages.json`: 각 카테고리 limit=60/start_page=1/max_pages=2. 현재 기본 71개 무제한 설정과 분리했다.
- `config/danawa_pagination.local.json`: 확인된 헤더와 5개 템플릿을 로컬에 반영했다. Git 제외 대상이다.

요청용 카테고리 값은 URL 접두 숫자를 잘라 추측하지 않았다. `pagination_metadata()`가 실제 HTML의 self.__next_f JSON 문자열 조각에서 viewModel을 읽고 currentCategory, uiCategoryHierarchyCodes, physicsCategory, multiCategoryYN을 대조했다. 완전한 메타데이터를 찾지 못하면 실패로 처리한다. 당시에는 이 메타데이터 추출을 실사이트 테스트 준비 과정에서 사용했다. 현재 CLI에는 미등록 카테고리의 메타데이터를 실행 중 준비하는 경로가 추가되어 있으며, 이 문서의 실험 결과는 여전히 5개에 한정된다.

## 재실행

`Commerce_crawler`에서 실행한다. 기본 전체 테스트는 네트워크를 사용하지 않으며 실사이트 테스트 6개는 건너뛴다. 이번 기본 실행은 오프라인 40개 통과, 실사이트 6개 skip이었다.

```powershell
python -m unittest discover -s tests -p 'test_*.py' -v
```

실사이트 테스트는 명시적으로 켠다. 최신 Next-Action이 포함된 local.json이 필요하다. 보통 새 GET 5회+POST 10회가 발생하므로 기본 테스트에서 자동 실행하지 않는다.

```powershell
$env:DANAWA_LIVE_TEST = '1'
try {
    python -m unittest discover -s tests -p test_live_pagination.py -v
} finally {
    Remove-Item Env:DANAWA_LIVE_TEST -ErrorAction SilentlyContinue
}
```

저장한 HTML을 재사용하려면 DANAWA_DISCOVERY_DIR에 각 `<cate>.html`이 있는 디렉터리를 지정한다. 이 경우 GET은 없지만 POST 10회는 실제로 수행한다. 다른 헤더 파일은 DANAWA_TEMPLATE_FILE로 지정할 수 있다.

수집기 자체를 같은 범위로 실행하는 명령:

```powershell
python -m product_collector --categories config/danawa_categories_5_pages.json --pagination-template config/danawa_pagination.local.json --listing-only --total-limit 300 --max-requests 10
```

## 결과 위치

- 메타데이터 탐색 HTML·GET 로그: `data/danawa/pagination_discovery_2c83ac16-548b-4788-9466-b4edca2f2a38/`
- 실제 페이지 테스트 원본·정규화·보고서·확인된 HTML/템플릿: `data/danawa/96c884b5-9459-4f1c-898a-7afda7146eaa/`

실사이트 테스트는 실행마다 새 결과 디렉터리를 만들고 위치를 출력한다. 원본 실행 자료는 Git 제외된 data 안에 두고, 작은 회귀 표본과 테스트 코드는 tests에 남겼다. 소스·회귀 표본·테스트는 Git으로 관리하고 대량 원본 자료와 로컬 헤더는 제외한다.
