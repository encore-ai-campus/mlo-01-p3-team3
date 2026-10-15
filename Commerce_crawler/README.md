# Commerce_crawler

다나와 다중 카테고리 상품 수집기다. 기본 실행은 설정된 71개 카테고리에서 목록 마지막 페이지까지 순회하며, 상품·페이지·요청 횟수 상한을 두지 않는다. 상세 상품 갤러리 사진을 상품 ID·URL과 연결한다. 도매꾹 API 호출이나 도매꾹 상품과의 동일 상품 매칭은 수행하지 않는다.

## 실행 위치와 준비

명령은 이 `Commerce_crawler` 디렉터리에서 실행한다. 저장소 상위 루트에서 실행하면 `product_collector` 모듈을 찾지 못할 수 있다. Python 3.10 이상이 필요하며 `python`이 PATH에 없으면 설치된 Python 실행 파일의 전체 경로를 사용한다.

```powershell
Set-Location 'C:\Users\Playdata\Documents\encore_MLops\3st_project\mlo-01-p3-team3\Commerce_crawler'
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements-collector.txt
.\.venv\Scripts\python.exe -m unittest discover -s tests -p 'test_*.py' -v
```

테스트는 합성 HTML과 가짜 Session을 사용하며 HTTP 요청을 하지 않는다. 다음 명령은 실제 다나와에 요청한다.

```powershell
.\.venv\Scripts\python.exe -m product_collector
```

## 바로 실행

`run_collector.py`를 직접 실행할 수 있다. 파일을 실행하면 준비 안내, 71개 카테고리 설정, 요청 URL, 페이지별 상품 수, 상세 처리 순서와 저장 상태를 콘솔에 즉시 출력한다.

```powershell
python run_collector.py
```

목록 간격만 줄이고 상세·사진 다운로드까지 이어서 실행하려면:

```powershell
python run_collector.py --listing-interval 0 --interval 1 --download-images
```

--listing-interval은 목록 전용, --interval은 상세 요청·사진 다운로드 간격이다. 실행 중인 기존 프로세스에는 변경이 소급 적용되지 않는다.

실제 수집 없이 시작 경로만 점검하려면 `python run_collector.py --help`를 사용한다. `.cmd`도 같은 Python 파일을 호출한다. 이미 실행 중인 이전 프로세스에는 코드 변경이 소급 적용되지 않는다.

Windows에서 `run_collector.cmd`를 실행하면 현재 설정으로 수집이 시작된다. 실행 파일은 자체 폴더를 기준으로 작동하며 Python과 로컬 의존성 경로를 선택한다. 기본 71개 카테고리의 상품 수와 페이지·요청 상한은 없다. 기본 카테고리 파일은 config/danawa_categories_71.json이다.

```powershell
.\run_collector.cmd
```

기본은 목록·상세 상품 정보와 갤러리 URL까지 수집한다. 사진 파일 다운로드까지 이어서 실행하려면:

```powershell
.\run_collector.cmd --download-images
```

실제 수집 없이 실행 경로만 확인하려면 `.\run_collector.cmd --help`를 사용한다. 이번 설정 변경 검증에서는 도움말과 가짜 응답 테스트만 실행했으며 실제 수집은 시작하지 않았다.

## 실시간 저장과 중단 후 재개

시작 시 출력되는 data/danawa/<작업ID>/에 목록 페이지와 상세 상품을 받는 대로 상품별 JSON과 재개 이력 JSONL로 저장한다. checkpoint_state.json에서 저장 상품 수와 마지막 완료 페이지를 확인한다. 같은 설정으로 다시 실행하면 미완료 작업을 자동 재개한다.

수집을 멈추지 않고 현재 결과를 꺼내려면 다른 터미널에서 실행한다.

```powershell
python run_collector.py --export-from "data/danawa/<작업ID>"
```

출력된 exports 하위 경로에서 products.jsonl과 product_raw.jsonl을 읽을 수 있다. 외부 요청은 하지 않는다. 특정 작업은 --resume-from, 새 작업은 --new-run으로 선택한다. 이미 실행 중인 구버전 프로세스에는 소급 적용되지 않는다. 자세한 내용은 [중단 확인·실시간 내보내기·재개](docs/checkpoint.md)를 따른다.

## 설정과 출력

`config/danawa_categories_71.json`의 리스트를 수정한다. 현재 기본 설정은 이전에 추린 71개 전체이며 limit/max_pages가 null인 무제한 설정이다. 리스트 길이는 5개로 제한하지 않는다. ID는 숫자로만 이루어진 문자열이어야 하고 동일 카테고리를 중복 설정할 수 없다.

기본 전체 상품 수·목록/상세 요청 수·이미지 다운로드 요청 수의 상한은 없다. 빈 상품 페이지, 반복 상품 페이지, 오류에 따라 카테고리 순회가 끝나며 HTTP 403/429 후에는 추가 목록·상세 요청을 중단한다. 목록 요청 간격은 기본 0초이고, 상세 요청·사진 다운로드 간격은 기본 1초다. 타임아웃 20초는 유지한다. 아래 명령은 필요할 때 제한을 다시 지정하는 예다. CLI 한도 인자에 0을 주면 무제한이다.

```powershell
.\.venv\Scripts\python.exe -m product_collector --categories config/danawa_categories.json --total-limit 10 --max-requests 15 --interval 1 --timeout 20
```

기본 POST 템플릿은 Git 제외된 config/danawa_pagination.local.json을 자동 사용한다. 로컬 템플릿에 없는 카테고리는 실행 중 자신의 목록 HTML에서 categoryCode/physicsCate*/group/depth 등을 읽어 POST 본문을 구성한다. 기존 카테고리의 분류 값을 그대로 재사용하지 않는다. 공통 요청 헤더·본문을 가져올 로컬 템플릿이 없으면 실행 전에 오류로 알리고, 특정 카테고리의 메타데이터를 찾지 못하면 그 카테고리는 파싱 실패로 기록한다. 기존 Next-Action은 로컬에 보존하며 사이트 변경 시 갱신이 필요할 수 있다.

CLI 기본 카테고리 파일과 출력 위치는 패키지 위치를 기준으로 해석한다. 사용자가 입력한 상대 경로는 명령 실행 디렉터리를 기준으로 해석한다.

`config/danawa_categories.json`은 기존 5개 카테고리의 별도 설정으로 보존했다. 기본 실행은 71개 전체다. 71개 설정의 상품/페이지 한도는 모두 null이며, 이번 수정에서 실제 대량 수집을 실행하지는 않았다.

다중 페이지 실행은 카테고리별 POST 본문과 필요한 요청 헤더를 사용한다. `config/danawa_pagination.example.json`에는 사용자가 제공한 의자 본문을 보존했다. 로컬 설정의 headers에 브라우저에서 확인한 Next-Action 등을 입력한다. URL의 cate와 본문의 categoryCode/physicsCate*는 카테고리별로 연결하고 page만 변경한다. 다른 70개에 의자 본문을 재사용하지 않는다.

```powershell
# local.json이 이미 있다면 덮어쓰지 말고 기존 헤더를 사용한다.
# 처음 준비할 때만 예제 파일을 복사한 뒤 실제 Request Headers를 입력한다.
if (-not (Test-Path config/danawa_pagination.local.json)) {
    Copy-Item config/danawa_pagination.example.json config/danawa_pagination.local.json
}
.\.venv\Scripts\python.exe -m product_collector --categories config/danawa_categories_pagination_sample.json --pagination-template config/danawa_pagination.local.json --listing-only --total-limit 60 --max-requests 2
```

샘플 설정은 의자의 8·9페이지, 최대 60개·POST 2회다. start_page/max_pages/limit은 카테고리 JSON으로 관리하고 `--start-page`/`--max-pages`로 일괄 재정의할 수 있다. 전체 상품 상한이 작으면 첫 페이지에서 종료할 수 있다. 해당 카테고리의 템플릿 없이 다중 페이지를 설정하면 실제 요청 전에 입력 오류로 종료한다.

각 실행의 출력은 `data/danawa/<job_id>/`에 저장한다.

| 파일 | 용도 |
|---|---|
| `product_raw.jsonl` | 목록 카드·상세 HTML·JSON-LD와 추출값 보존, 재처리 근거 |
| `products.jsonl` | 활용할 공통 필드, 출처·누락 상태, 원본 연결 |
| `run_report.json` | 카테고리별 결과, 요청 로그, 제한·오류, 필드 확보 집계 |

파이프라인 정상 완료는 종료 코드 0, 부분 결과나 입력 오류는 2다. 상세 보강에 실패해도 확보한 목록 데이터는 저장한다. 정상 완료가 모든 필드 확보를 뜻하지 않는다.

재추출은 기존 실행 폴더를 지정한다. 아래 `<job_id>`는 실제 폴더 이름으로 바꾼다. HTTP 요청 없이 새 실행 폴더를 생성하고 원래 수집 시각을 유지한다.

```powershell
.\.venv\Scripts\python.exe -m product_collector --reprocess-from data/danawa/<job_id>
```

## 문서

- [수집 방식·설정·중복·페이지 전환·검증](docs/danawa_collector.md)
- [출력 필드와 BRD 대응·미확보 항목](docs/data_fields.md)
- [POST 페이지 순회와 상품 갤러리 사진](docs/pagination_and_gallery.md)
- [5개 카테고리 1→2페이지 실제 검증·테스트 재실행](docs/five_category_pagination_validation.md)
- [확보한 상품 이미지 다운로드·로컬 파일 연결·재개](docs/image_localization.md)
- [실시간 저장·중단 확인·자동 재개](docs/checkpoint.md)

JSONL은 테스트용 로컬 스냅샷이다. MongoDB Bronze, Spark/Parquet Silver, 서비스용 Gold 파이프라인까지 구현한 결과는 아니다. 데이터와 가상환경은 이 폴더의 `.gitignore`로 제외한다. Git 협업 절차는 팀 가이드를 따른다. 이번 변경은 사용자 승인 범위인 로컬 커밋까지만 기록하며 푸시는 하지 않는다.
