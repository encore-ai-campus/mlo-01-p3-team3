# 파일 저장과 중단 후 재개

SQLite를 사용하지 않는다. 시작할 때 출력되는 `data/danawa/<작업ID>/`에 JSON/JSONL 파일로 저장한다.

| 경로 | 용도 |
|---|---|
| product_raw/danawa_<상품ID>.json | 처리 직후 갱신하는 원본 상품 |
| products/danawa_<상품ID>.json | 같은 상품의 공통 정규화 데이터 |
| checkpoint_events.jsonl | 상품·카테고리 진행 위치·요청 이력을 함께 기록하는 재개용 이력 |
| run_config.json | 작업 ID와 수집 설정 |
| checkpoint_state.json | 상태, 상품 수, 카테고리별 마지막 완료 페이지 |
| product_raw.jsonl, products.jsonl, run_report.json | 종료·Ctrl+C 시 내보내는 통합 결과 |
| exports/<스냅샷ID>/ | 실행 중 요청한 시점의 통합 결과 |

목록 한 페이지 또는 상세 상품 한 건을 처리하면 체크섬을 포함한 JSONL 이력을 먼저 디스크에 반영한 뒤 상품별 JSON 파일을 교체한다. 강제 종료로 마지막 기록이 잘렸다면 완성된 기록까지만 복원한다. 개별 JSON 갱신 중 중단되면 다음 실행에서 이력으로 복구한다. 수집 중 원본과 정규화 파일을 각각 복사하면 갱신 시점이 다를 수 있으므로 전달용 자료는 아래 내보내기를 사용한다.

```powershell
python run_collector.py --export-from "data/danawa/<작업ID>"
```

외부 요청 없이 같은 완료 기록까지 읽어 exports 하위에 원본·정규화 JSONL과 보고서를 만든다. 사진 파일은 포함하지 않는다.

같은 설정으로 다시 실행하면 active_run.json이 가리키는 미완료 작업을 재개한다. 특정 작업은 다음처럼 선택한다.

```powershell
python run_collector.py --resume-from "data/danawa/<작업ID>"
```

카테고리·상품 한도 등 수집 설정은 기존 실행과 같아야 한다. 요청 간격과 작업자 수는 바꿀 수 있다. 완료한 페이지와 성공한 상세 상품은 건너뛰며 저장 직전 진행 중이던 요청은 다시 발생할 수 있다. 요청 예산은 실행마다 적용하고 보고서 이력은 누적한다. 새 작업은 --new-run을 사용한다.

checkpoint_state.json의 running/interrupted/failed/partial/completed와 마지막 완료 페이지를 확인한다. 강제 종료하면 running으로 남을 수 있다. OS 파일 잠금은 프로세스 종료 시 해제되므로 writer.lock을 수동 삭제할 필요는 없다.

기존 SQLite 파일은 삭제하거나 자동 변환하지 않는다. 그 형식의 작업이 자동 선택되면 파일 체크포인트가 없다는 안내가 나오며 --new-run으로 새 작업을 시작할 수 있다. 이미 실행 중인 프로세스에는 변경이 소급 적용되지 않는다.

## 병렬 수집과 이미지

--workers 2가 기본이며 서로 다른 카테고리의 목록 요청을 최대 두 개까지 동시에 처리한다. 같은 카테고리의 페이지는 순서대로 요청한다. 세션은 스레드별로 분리하고 상품 병합과 파일 저장은 메인 스레드에서 수행한다. 상품 ID가 같으면 카테고리 소속을 합친다. --workers 1은 순차 실행이다.

목록 요청 간격 --listing-interval은 모든 작업자가 공유한다. 상세·이미지 다운로드는 순차 처리하며 --interval을 따른다. 기본값은 각각 0초와 1초다. 중단 시 진행 중인 요청이 끝날 때까지 타임아웃 범위에서 기다릴 수 있다.

```powershell
python run_collector.py --workers 2 --listing-interval 0 --interval 1 --download-images
```

처리 순서는 목록 전체 → 상세 보강 → 선택적 이미지 다운로드다. 갤러리 URL은 상세를 완료한 상품부터 JSON에 추가된다. 사진 파일은 다운로드 단계에서 media/categories/<카테고리코드>/danawa/<상품ID>/에 저장한다. 여러 카테고리에 속하면 각 경로에 연결하고 images[].category_paths에 카테고리별 파일 경로를 남긴다. 동일 URL은 다운로드 이력을 재사용한다. 사진 단계에서 중단되면 --resume-from과 --download-images로 이어갈 수 있다.

회귀 테스트는 tests/test_checkpoint.py에서 합성 응답으로 강제 종료·재개·실행 중 내보내기·설정 불일치·중복 작성자 차단을 확인한다. 대량 실제 수집은 테스트에 포함하지 않는다.
