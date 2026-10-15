# 상품 갤러리와 로컬 이미지

## 현재 계약: schema_version 1.2

상세 페이지의 대표 사진과 다른 각도의 상품 사진을 images[] 하나에 저장한다. 사진 수 상한은 두지 않는다. 상세 설명 이미지와 목록 썸네일은 다운로드 대상에서 제외한다. 목록 썸네일 주소는 listing_image_url로만 보존한다.

관측 위치는 #basicThumbSlideArea img와 #baseImage다. 동일 상품의 JSON-LD에 대응하는 이미지 URL이 실제로 명시되어 있으면 해당 해상도를 사용한다. 관측되지 않은 원본 URL이나 해상도를 추정하지 않는다.

images[]는 수집 단계에서 url/role/order/origin/selector/observed_url/resolution_status를 가지며, 다운로드 후 같은 객체에 다음 정보를 추가한다.

| 필드 | 의미 |
|---|---|
| source, product_id | 부모 상품과 일치하는 식별자 |
| filename, local_path | 상품별 파일명과 media/ 기준 상대 경로 |
| sha256, bytes, width, height, format, extension | 실제 받은 파일 정보 |
| status, downloaded_at, error | 다운로드 결과와 사유 |

파일명 예시: danawa_16272926_001_012345abcdef.jpg. 출처·상품 ID·순번·URL 해시 일부를 포함한다. 모든 갤러리 이미지가 성공해야 상품의 image_download_status가 completed다. 실패나 대기 항목도 원본 URL과 상품 연결을 보존한다.

새 출력에는 image_urls/local_images/storage_path를 중복 저장하지 않는다. URL 목록이 필요하면 images[].url에서 만든다. 기존 1.0/1.1 파일은 수정하지 않으며, 과거 image_urls 입력도 읽을 수 있다.

## 실행

Commerce_crawler에서 실행한다. 신규 수집과 다운로드를 연속 수행:

```powershell
.\.venv\Scripts\python.exe -m product_collector --download-images --max-image-requests 100
```

기존 상품을 상세 보강하고 다운로드·검증까지 이어서 수행:

```powershell
.\.venv\Scripts\python.exe -m product_collector.gallery_assets --run data/danawa/<job_id> --max-detail-requests 300 --max-image-requests 3000
```

이미 갤러리가 확보된 상품만 다운로드:

```powershell
.\.venv\Scripts\python.exe -m product_collector.media --products <products.jsonl> --max-requests 10 --cache-dir data/danawa/_asset_cache/media
```

외부 요청 없이 저장 상세를 소량 검사:

```powershell
.\.venv\Scripts\python.exe -m product_collector.audit_assets --run data/danawa/<job_id> --limit 3
```

검사는 기본적으로 audit/verification_report.json만 저장한다. 원문·상품 전체 복사와 이전 썸네일 전용 검사는 일반 경로에서 제거했다. 별도로 남긴 기존 검증 결과와 사진은 삭제하지 않았다.

## 저장과 재개

- 공통 JSON/JSONL 저장·이력 복구·파일 연결은 storage.py에서 처리한다.
- 상세 식별 검사는 다나와 추출기에 두고, 저장 상세의 재추출·보강·검사는 reprocess.apply_detail을 공유한다.
- 파일 무결성과 상품 연결 검사는 validation.py를 공유한다.
- 다운로드 이력은 이미지마다 기록한다. 전체 상품 결과는 25개 URL마다, 정상 종료·처리 가능한 중단 시 갱신한다. 강제 종료 후에는 이력으로 복구한다.
- 캐시를 지정하면 캐시에만 해시 기준 원본 파일을 저장하고, 출력에는 상품별 파일 링크를 만든다. 하드링크가 지원되지 않으면 복사한다.
- 같은 URL은 실행 중 한 번 요청하며, 재실행·공유 캐시 사용 시 정상 파일의 해시를 확인해 요청을 생략한다. 다른 URL의 같은 바이트는 다운로드 후 해시로 묶는다.
- 기본 요청 간격 1초, 타임아웃 20초, 이미지 최대 20MiB다. 재시도·리다이렉트 추적은 하지 않고 403/429 이후 네트워크 요청을 중단한다.
- 캐시는 자동 만료하지 않는다. 최신 내용이 필요하면 해당 캐시 또는 새 출력·캐시 경로를 관리한다. 출력과 공유 캐시는 단일 프로세스에서 사용한다.

최종 활용 파일은 media/products_with_local_images.jsonl이며 이름은 기존 사용 경로의 호환성을 위해 유지했다. 내부는 images[] 하나다. 실제 파일과 이름·경로·상품 식별자·해시·해상도가 일치하는지 검사해 media/verification_report.json에 기록한다.

## 소량 실검증: 2026-10-15

저장된 상세 상품 3개를 사용해 갤러리 10장(4장·5장·1장)을 다운로드했다. 새 목록·상세 요청은 0회, 이미지 요청은 10회였으며 전체 파일 연결·해시·이미지 정보 검증이 통과했다. 같은 입력의 재실행은 이미지 요청 0회였다.

결과: data/danawa/96c884b5-9459-4f1c-898a-7afda7146eaa/refactor_smoke_e897703a/smoke_report.json.

테스트 코드는 tests/test_media.py, tests/test_gallery_assets.py, tests/test_live_gallery_assets.py에 남긴다. 실제 이미지 테스트는 DANAWA_IMAGE_LIVE=1과 DANAWA_ASSET_RUN을 명시한 경우에만 실행하며 최대 10회 요청한다. 기본 테스트 실행은 외부 요청을 하지 않는다.

```powershell
.\.venv\Scripts\python.exe -m unittest discover -s tests -v
```

기존 300개 중 상세 203개·갤러리 연결 557개를 확보한 데이터는 보존했다. 이번에는 3개 상품만 검증했으며 전체 300개 내재화를 재개하지 않았다. 이 소량 검증 당시에는 Git 스테이징·커밋·푸시를 하지 않았다. 이후 사용자 승인에 따라 소스와 테스트를 로컬 커밋까지 기록하며 사진·캐시·로컬 설정은 제외한다.
