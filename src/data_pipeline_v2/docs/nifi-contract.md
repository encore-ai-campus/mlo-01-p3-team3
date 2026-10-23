# NiFi 기동 계약

크롤 완료 유한 배치를 NiFi에서 기동할 때의 명령 경계다. **서버 NiFi FLOW에는 아직 연결하지 않았다.** 실제 processor 구성, 재기동 후 대기 batch 재조정, 경보 연결은 서버 이전 때 검증한다.

## 원칙

- NiFi는 크롤 주기 실행, 완료 manifest 확인, batch 제출·상태 조회, back pressure, 재시도·경보만 맡는다. 상품 데이터를 변환하거나 Bronze/Silver/Gold에 직접 쓰지 않는다.
- FlowFile에는 `crawl_batch_id`, manifest 위치, `job_id`만 싣는다. 원문·이미지 payload는 FlowFile에 복제하지 않는다.
- 긴 Spark 실행을 processor task에서 동기로 기다리지 않는다. `batch-work`는 별도 프로세스로 띄우고 `batch-status`로 상태를 확인한다.
- 모든 명령은 표준 출력의 마지막 줄에 JSON 한 줄을 쓴다. 정상 종료 코드는 `0`, 실패는 `2`(`{"status":"FAILED","error_code":...}`), 인터럽트는 `130`이다. 오류 코드에는 원문·URI·비밀값이 없다.
- 같은 요청이 다시 와도 결과가 같다. 제출 응답이 유실됐거나 NiFi가 재시작했다면 같은 명령을 다시 실행한다.

## 흐름

| 단계 | NiFi 동작 | 명령 | 다음 단계로 넘어갈 조건 |
|---|---|---|---|
| 1 | 크롤러 실행 후 완료 manifest와 입력 파일 위치를 받음 | `batch-prepare --manifest M --input F` | `status=SOURCE_COMPLETE` |
| 2 | 발행 | `batch-relay --batch B` | `status=COMPLETE`, `ranges` 존재(빈 크롤이면 비어 있음) |
| 3 | 백로그 확인 | `batch-backlog` | `saturated=false`일 때만 제출. 포화면 FlowFile을 대기 큐에 둔다 |
| 4 | 제출 | `batch-submit --batch B` | `job_id` 획득. `BATCH_BACKPRESSURE`면 지연 후 재시도 |
| 5 | 실행(비동기) | `batch-work --job J` 또는 `--phase process` 뒤 `--phase publish` | 별도 프로세스 종료 코드 |
| 6 | 상태 확인 | `batch-status --job J` | `status=COMPLETED` |

처리와 게시를 따로 띄우면 n번째 배치를 게시하는 동안 n+1번째 배치를 처리할 수 있다. 같은 단계의 두 번째 프로세스는 writer 잠금에서 `WRITER_BUSY`로 즉시 끝난다. 이것은 데이터 오류가 아니므로 나중에 다시 시도한다.

## 실패 처리

| 상태·오류 | NiFi 동작 |
|---|---|
| `CRAWL_NOT_COMPLETE`, `REQUIRED_SOURCE_INCOMPLETE` | 제출하지 않는다. 크롤 재실행이나 source별 부분 완료 계약을 확인한다 |
| `KAFKA_DELIVERY_UNCONFIRMED` | 발행 응답이 일부 확인되지 않았다. 확인된 ack는 저장돼 있으므로 같은 `batch-relay`를 다시 실행한다. 확인되지 않은 메시지만 다시 보낸다 |
| `BATCH_BACKPRESSURE`, `WRITER_BUSY` | 지연 후 같은 명령을 재시도한다 |
| job `RECOVERY_REQUIRED` | 경보를 낸다. 운영자가 프로세스·Spark 정지를 확인한 뒤 `batch-recover --job J --stop-evidence 근거` |
| `KAFKA_OFFSET_UNAVAILABLE` 또는 Spark `failOnDataLoss` | `batch-relay --batch B --generation N+1`로 다시 발행한 뒤 `batch-recover --generation N+1`. `latest`부터 읽어 우회하지 않는다 |
| `OUTBOX_SOURCE_MISSING_OR_CHANGED` | Outbox·원천에도 근거가 없다는 뜻이다. 누락을 경보하고 수동 조사한다 |

`batch-status`에 `metric_failures`가 0보다 크면 시간 기록 저장이 실패했다는 뜻이다. job 상태와 게시 결과는 그대로 유효하며, 상태 경로 용량과 권한을 점검하라는 경보로만 쓴다. 이 경보는 아직 NiFi FLOW에 연결하지 않았다.

## 운영 주기 작업

- `batch-retention`은 하루 한 번 실행한다. 격리 원문은 30일, 비식별 metadata는 90일이 지나면 지운다.
- `batch-audit --job J`은 정상 경로 밖에서 주기적으로 또는 표본으로 실행한다. 매 배치의 완료 조건으로 걸지 않는다.
- 원천 철회는 `batch-retract` 뒤 `batch-repair --products ...`로 영향 상품만 다시 계산한다.
