# 상품 갤러리 다운로드와 검증

2026-10-16 현재 구현 기준이다. 파일 전달과 NAS 차이는 [전달 문서](image_delivery.md), 실험 결과는 [검증 기록](validation_results.md)을 따른다.

## 대상과 실행 준비

다운로드 대상은 상세의 대표·추가 상품 갤러리 `images[]`다. 사진은 1~4장으로 제한하지 않는다. 상세설명 이미지와 목록 썸네일은 제외하고 `listing_image_url`은 참조로만 남긴다. 관측되지 않은 원본 해상도 URL을 만들지 않는다.

아래 명령은 Commerce_crawler 기준이며 해당 Python 환경에 requirements-collector.txt를 설치해야 한다. `run_collector.py`의 로컬 의존성 경로 보완은 `python -m product_collector.media`에 자동 적용되지 않는다. Pillow의 import 이름은 PIL이다.

```powershell
python -m pip install -r requirements-collector.txt
```

## 실행 모드

새 수집 → 상세 → 이미지 다운로드·검증:

```powershell
python run_collector.py --workers 3 --listing-interval 0 --interval 1 --download-images
```

확보한 갤러리 URL의 파일 다운로드만 실행(목록·상세 요청 없음):

```powershell
python -m product_collector.media --products "data/danawa/<job_id>/products.jsonl" --interval 1
```

기본 출력은 입력 products.jsonl의 상위 폴더/media다. 다른 출력은 --output, 공유 캐시는 --cache-dir로 명시한다. standalone media 명령은 자동 검증 보고서를 만들지 않는다. 필요하면 다음 명령으로 무결성·연결 보고서를 생성한다. 이 명령은 다운로드하지 않지만 media/verification_report.json을 쓴다.

```powershell
python -c "from pathlib import Path; from product_collector.validation import verify_assets; print(verify_assets(Path('data/danawa/<job_id>/media/products_with_local_images.jsonl'), Path('data/danawa/<job_id>/media')))"
```

저장된 목록 실행에서 부족한 상세까지 보강하고 사진을 내려받는 별도 도구:

```powershell
python -m product_collector.gallery_assets --run "data/danawa/<job_id>" --max-detail-requests 300 --max-image-requests 3000
```

이 도구의 기본 출력은 작업 폴더/gallery_assets, 기본 공유 캐시는 작업들의 상위 폴더/_asset_cache다. 메인 CLI와 출력·잠금 방식이 다르다. 위 300·3000은 유한 예제이며 현재 기본 한도는 없다.

저장 상세의 사진 연결을 외부 요청 없이 소량 확인:

```powershell
python -m product_collector.audit_assets --run "data/danawa/<job_id>" --limit 3
```

`<job_id>`는 실제 ID로 바꾼다. 이미 실행 중인 이미지 출력·캐시에 다른 다운로드 프로세스를 겹쳐 실행하지 않는다.

## manifest와 저장 경로

최종 상품·이미지 연결 manifest는 `media/products_with_local_images.jsonl`이다. 별도 manifest.jsonl 파일을 만들지 않는다. 기존 상품 필드를 유지하며 images[]에 다음 정보를 병합하고 상품 단위 image_download_status를 추가한다.

| 필드 | 의미 |
|---|---|
| url, role, order, origin, selector, observed_url, resolution_status | 갤러리 관측 주소·역할·순서·추출 근거 |
| source, product_id | 부모 상품과 일치하는 연결 키 |
| filename, local_path, category_paths | 파일명·media 기준 상대 경로·카테고리별 경로 |
| sha256, bytes, width, height, format, extension | 실제 받은 파일에서 측정한 값 |
| status | downloaded/failed/pending/budget_exhausted/stopped_after_block 등 |
| downloaded_at | 다운로드 성공 시각. 실패·대기에는 없을 수 있음 |
| error | 실패 사유. 성공에는 없을 수 있음 |

파일명은 `danawa_<상품ID>_<001부터 순번>_<URL해시12자리>.<확장자>`다. images[].order는 관측 순서이고 파일명 순번과 시작값이 다를 수 있다.

```text
media/
  images/<sha256>.<ext>
  categories/<category_id>/danawa/<product_id>/<filename>
  products_with_local_images.jsonl
  image_downloads.jsonl
  download_report.json
  verification_report.json  # 검증 함수를 실행한 경우
```

상품이 여러 카테고리에 속하면 각 경로를 만들고 category_paths에 기록한다. local_path는 그중 첫 경로다. 카테고리가 없는 호환 입력은 _uncategorized 경로를 쓴다. 새 출력은 images 하나만 사용하며 image_urls/local_images/storage_path를 중복 저장하지 않는다. 과거 형식 입력을 읽는 호환 처리는 유지한다.

## 중복 요청과 파일 재사용

- 동일 URL은 한 실행에서 한 번 요청한다.
- image_downloads.jsonl의 성공 이력과 실제 파일 해시가 유효하면 재실행에서도 요청을 생략한다.
- 서로 다른 URL의 동일 바이트는 다운로드 후 SHA256으로 원본 저장을 공유한다. URL이 다르면 받기 전 동일 바이트 여부를 보장할 수 없다.
- 캐시 파일에서 카테고리 경로로 하드링크를 시도하고 지원되지 않으면 복사한다. 둘은 같은 파일 내용이지만 물리 저장 방식이 다를 수 있다.
- 캐시는 자동 만료하지 않는다. URL 내용 최신성이나 변경 여부를 HTTP 조건부 요청으로 확인하는 기능은 없다.

기본 요청 시작 간격은 1초, 타임아웃 20초, 한 이미지 최대 20MiB다. 리다이렉트 추적·자동 재시도는 하지 않는다. HTTP 403/429 후 새 이미지 요청을 중단한다. 실패해도 URL·상품 연결과 사유를 남긴다.

## 진행 저장과 실패 재시도

URL 처리 이력은 이미지마다 append한다. 전체 상품 manifest와 다운로드 집계는 25개 URL마다, 정상 종료 및 처리 가능한 중단의 finally에서 갱신한다. 강제 종료 직후에는 이력과 manifest의 갱신 시점이 다를 수 있으며, 같은 입력·출력·캐시로 다시 실행해 복구한다.

다운로드된 파일이 전부 성공한 상품만 image_download_status=completed다. 하나라도 실패·대기이면 partial, 갤러리 자체가 없으면 gallery_missing이다. 실패 URL을 같은 media 명령으로 재실행하면 재시도하며 정상 파일은 재사용한다.

메인 CLI의 수집 OS 잠금은 이미지 단계까지 유지되는 잠금이 아니다. standalone media에도 프로세스 중복 실행을 막는 자체 잠금은 없다. gallery_assets는 자신의 output/pipeline.lock을 별도로 사용하며 강제 종료 시 남을 수 있다. 이 lock은 체크포인트의 OS 잠금과 다르므로 프로세스 종료를 확인한 뒤 대응한다. 공유 캐시도 한 작성자만 사용한다.

## 검증 범위

validation은 실제 바이트의 이미지 형식·해상도·용량·해시, 파일명과 상품 ID, manifest 경로와 카테고리 소속을 확인한다. 사진이 같은 제품을 올바르게 보여주는지의 시각적 검수나 사용권 검수까지 자동 승인하는 기능은 아니다.

2026-10-15의 3개 상품·10장 검증, 300개 배치의 664/678장 다운로드, 2026-10-16 NAS 검사는 [결과 문서](validation_results.md)에 시점별로 구분했다.
