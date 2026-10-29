# 파일 저장·중단 확인·재개

2026-10-16 구현 기준이다. SQLite를 사용하지 않고 기존 jsonl-v1 형식을 유지한다. 기본 명령은 [README](../README.md), 사진 단계는 [다운로드 문서](image_localization.md)를 따른다.

## 실시간 저장과 통합 출력

시작 시 data/danawa/<job_id>/를 만든다. 목록 한 페이지 또는 상세 한 상품 처리 후 체크섬이 있는 checkpoint_events.jsonl 기록을 디스크에 반영하고, 상품별 원본·정규화 JSON을 교체한 뒤 checkpoint_state.json을 갱신한다.

| 경로 | 의미 |
|---|---|
| `run_config.json` | 작업 ID·jsonl-v1 형식·수집 설정 |
| `checkpoint_events.jsonl` | 원본/가공 결과·카테고리·요청 결과·보고서의 재개 이력 |
| `checkpoint_state.json` | 저장 완료 commit·상태·상품 수·카테고리별 마지막 완료 페이지 |
| `product_raw/danawa_<id>.json` | 원본 상품. 목록·상세 추가에 따라 갱신 |
| `products/danawa_<id>.json` | 같은 상품의 가공 필드·원본 연결 해시 |
| `details/<id>/<수집 시각>.json.gz` | 상세 응답 HTML 전체·URL·수집 시각(gzip). 상품 기록보다 먼저 저장하므로 재개 시 건너뛴 상세도 남아 있음. 재시도 응답은 새 파일이라 이전 스냅샷이 쓴 HTML을 바꾸지 않음 |
| `product_raw.jsonl`, `products.jsonl`, `run_report.json` | 정상 종료·수집 Ctrl+C·내보내기에서 만든 통합 결과 |
| `exports/<snapshot_id>/` | 실행 중 요청한 시점의 통합 내보내기 |

JSONL은 JSON 배열 하나가 아니라 한 줄에 상품 JSON 한 건을 기록한다. 강제 종료로 마지막 이력 줄이 잘리면 완성된 기록까지 복원한다. 상품 파일 교체가 덜 끝났으면 이력에서 해당 파일을 복구한다. 이력 순서·체크섬이 맞지 않으면 오류로 알린다.

수집 중 원본·정규화 폴더를 따로 복사하면 시점이 달라질 수 있다. 전달·적재에는 같은 완료 기록으로 생성한 스냅샷을 사용한다.

```powershell
python run_collector.py --export-from "data/danawa/<job_id>"
```

외부 요청 없이 exports 하위에 통합 원본·정규화·보고서를 생성한다. 이미지 파일과 이미지 manifest를 함께 복제하는 명령은 아니다. 수집 중 진행 상태는 checkpoint_state, 상세 실패는 원본의 detail_status/detail_error, 통합 오류 보고는 내보낸 run_report에서 확인한다.

## 재개와 실패 재시도

```powershell
python run_collector.py --resume-from "data/danawa/<job_id>" --workers 3 --listing-interval 0 --interval 1 --download-images
```

명령의 job_id는 실제 작업 ID로 바꾼다. 다음 조건을 지킨다.

- 재개 시 --new-run을 붙이지 않는다. 새 작업은 별도 ID로 처음부터 시작한다.
- 카테고리·순서·시작 페이지·상품/페이지 한도·목록 전용 옵션·저장된 POST 본문 설정이 같아야 한다. 다른 파일명을 쓰더라도 구성 값이 달라지면 거부한다.
- 작업자 수·요청 간격은 바꿀 수 있다. 요청 예산은 각 프로세스 실행에 적용하고 이력은 누적한다.
- 성공한 목록 페이지·완료 카테고리·정상 상세는 생략한다. 실패 페이지·상세와 미처리 부분은 다시 시도한다. 응답 성공 후 저장 전 종료된 요청은 반복할 수 있다.
- 이미지 단계는 다운로드 이력과 실제 캐시 파일 검증을 사용한다. 같은 작업의 photos까지 재개하려면 --download-images를 유지한다.

별도 --resume-from이 없으면 출력 루트의 active_run.json이 가리키는 미완료 작업을 선택한다. 정상 완료한 작업은 자동 선택 대상에서 해제되므로 완료 작업의 일부 이미지 실패 등은 명시적 작업 ID나 media 단독 명령으로 처리한다. 이미지 단계 시작 전 수집 잠금과 active 포인터 관리가 끝난다는 점을 구분한다.

## 중단과 잠금

수집 창에서 Ctrl+C를 한 번 누르고 [중단·저장 완료]와 프로세스 종료를 기다린다. 수집 단계의 종료 코드는 130이다. 준비 중 또는 강제 종료에서는 통합 내보내기 없이 끝날 수 있으며, 이미 완료한 이력으로 재개한다. 비정상 종료 시 status가 running으로 남을 수 있으므로 프로세스와 commit/updated_at 변화도 확인한다.

메인 수집기는 출력 루트/.collection.lock과 작업 폴더/writer.lock을 OS 파일 잠금으로 사용한다. 같은 출력 루트에 다른 수집을 실행하면 --new-run이 있어도 거부한다. 별도 실행은 --output을 다르게 지정한다.

```powershell
python run_collector.py --categories config/my_categories.json --output data/danawa_sample --download-images --new-run
```

config/my_categories.json은 실제로 준비한 파일 경로로 바꾼다. 정상·강제 프로세스 종료 시 OS 잠금은 해제되므로 lock 파일 자체를 지워 해결하지 않는다. 잠금 오류만으로 데이터 손상이라고 판단하지 않는다. 살아 있는 수집기의 잠금 보유 여부를 확인한다. --export-from은 읽기 전용 복원과 별도 스냅샷 출력이며 수집 OS 잠금을 획득하지 않는다.

이 OS 잠금은 standalone media의 동시 다운로드를 막지 않는다. gallery_assets의 pipeline.lock도 별도 단순 파일 잠금이므로 강제 종료 후 남을 수 있다. [이미지 도구별 제약](image_localization.md)

## 재개 준비 성능

준비 중 [이력 복원]에 읽은 MB·비율·상품 수·경과 시간을 약 1초 간격으로 출력한다. 기존 작성자도 상품 파일 갱신 후 state.commit을 저장했으므로 그 지점까지의 개별 파일은 재개 때 다시 열지 않는다. 이력만 기록되고 파일 갱신이 덜 끝난 상품만 검사·복구한다. 완료 지점이 없으면 전체 복구, 완료 지점이 이력보다 앞서면 오류로 처리한다.

## 메모리 사용

체크포인트는 상품 레코드를 메모리에 들고 있지 않는다. 상품마다 최신 이력 줄의 위치(바이트 오프셋·이벤트 안 순번·seq)와 detail_status·카테고리 ID만 색인으로 둔다. 레코드는 바꿀 때 그 줄에서 다시 읽고(직전 이벤트 하나만 캐시), 저장한 뒤 버린다. 위치로 읽은 레코드의 상품 키나 seq가 색인과 다르면 오류로 멈춘다.

- 목록 단계는 한 페이지의 상품만, 상세 단계는 처리 중인 상품 하나만 메모리에 있다. 이미 ok·not_needed인 상품은 색인만 보고 건너뛴다.
- 종료 집계(field_coverage, 상세 실패율, 상세 재사용 요약, 품질 관문)는 저장된 원본·정규화 레코드를 한 번 훑으며 계산한다. 품질 관문은 상품마다 필요한 값만 남긴 작은 투영으로 판정한다(계산식은 그대로).
- JSONL 내보내기, 이미지 단계의 products_with_local_images.jsonl 작성, verify_assets, --reprocess-from은 한 줄씩 처리한다. 이미지 단계는 products.jsonl을 다시 읽으며 manifest를 쓰고, 다시 읽은 바이트의 해시가 처음과 다르면 manifest를 교체하지 않고 멈춘다. 재추출 결과는 출력 루트의 `.staging-<job_id>/`에 쓴 뒤 실행 폴더로 옮겨 쓰고, 끝나면 지운다.
- 요청 기록은 끝난 요청의 연속 앞부분을 다음 저장부터 다시 인코딩하지 않는다. 요청이 모든 필드를 채운 뒤 완료 표시를 남기므로 건너뛴 요청은 최종 상태로 저장된 것이다.
- 남는 메모리는 색인(상품당 수백 B), 요청 기록, 이미지 URL별 상태다. 재개 시 이력 전체를 읽고 해시를 검증하는 시간은 그대로다.
- 산출물(checkpoint_events.jsonl, product_raw.jsonl, products.jsonl, 상품별 JSON, checkpoint_state.json, products_with_local_images.jsonl)의 바이트는 이전 구현과 같다(`tests/test_memory_equivalence.py`).

외부에서 오래된 상품 파일을 삭제·수정한 경우 기본 재개는 이미 완료된 파일을 전수 검사하지 않는다. 이때만 같은 명령에 --repair-files를 추가한다. 전체 상품 파일을 읽으므로 준비가 오래 걸릴 수 있다.

2026-10-16 상품 1,741개·이력 64개 표본에서 이전 커밋 45e7838과 비교했다. 상품 파일 읽기 3,482→0회, 준비 및 복사 87.56→0.40초였다. 원본을 수정하거나 HTTP 요청을 하지 않았으며 전체 65,498개 실행 시간을 보장하는 결과는 아니다. [검증 기록](validation_results.md)

## 이전 형식과 제한

SQLite 형식의 기존 작업은 자동 변환·재개하지 않는다. run_config.json이 없는 작업은 오류로 알리고 원본 자료를 보존한다. 파일 체크포인트의 run_config.json과 checkpoint_events.jsonl이 있는지 확인하며 checkpoint_state.json만으로 원본을 복구할 수는 없다.

개별 상품 JSON은 수집 중 갱신되므로 불변 MongoDB 원천과 같지 않다. DB·Kafka 적재의 체크포인트는 향후 별도로 관리한다. 자동 프로세스 재시작이나 예약 실행은 현재 구현하지 않았다. 테스트는 test_checkpoint.py, test_checkpoint_index.py, test_file_parallel.py, test_resume_startup.py, test_memory_equivalence.py에 있으며 [실행 방법](../README.md)을 따른다.
