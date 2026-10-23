# 로컬 검증 기록

## 2026-10-23 부분 철회 최신성·Mongo 일괄 저장 중복·진단 필드 보완

외부 검토에서 확인된 결함 3건을 수정했다. 각 시험은 수정 전 소스(`HEAD`)로 따로 실행해 해당 증상으로 실패하는 것을 확인했다.

| 시험 | 수정 전 증상 |
|---|---|
| `test_partial_retraction_recompute_never_replaces_newer_published_evidence` | 부분 철회 재계산이 최신 게시본을 과거 결과로 되돌림(`PUBLISHED`가 나옴) |
| `test_batch_store.py` (가짜 컬렉션) | 복합 키 중복 시 `TypeError: unhashable type: 'list'`. 같은 입력을 두 번째 접수할 때 걸림 |
| `test_image_recheck_time_alone_is_the_same_event` | 확인 시각만 바뀐 재접수가 `IMMUTABLE_FILE_CONFLICT`로 실패 |

`MongoBatchStore.put_many`는 실제 Mongo가 아니라 `insert_many`/`BulkWriteError`를 흉내 낸 메모리 컬렉션으로 시험했다. 서버의 쓰기 동작과 오류 형식은 아직 확인하지 않았다.

## 2026-10-23 원격 호출 묶음·Delta 중복 검사·Silver 재사용 개선

전체 시험 **69개 실행, 69개 통과**(`PIPELINE_V2_TEST_SPARK=1`, 실제 로컬 Spark `local[2]`, `MemoryDelta`). **성능 개선 효과는 측정하지 않았다.** 아래는 호출 횟수와 읽기 범위를 줄이는 구조 변경이며, 시간 단축은 서버 계측으로 확인해야 한다. 이번에도 Mongo(`MongoBatchStore.put_many`), 실제 Kafka producer, 실제 Delta의 `append_history`는 실행하지 않았다.

| 변경 | 확인한 것 | 확인하지 못한 것 |
|---|---|---|
| Outbox `prepare`·`relay` 묶음 처리, `send_many` | 5건을 2건씩 묶으면 발행 대기가 3번(2·2·1)이고, 일부 미확인 시 확인된 ack는 보존되며 미확인 메시지만 재발행. 파일 store·`FileBroker` 기준 | Mongo `insert_many` 중복 키 처리, confluent/kafka-python 실제 발행과 `BufferError` 처리 |
| 게시 단계 `delivery_output`·`publication_receipt` 일괄 조회·저장 | 기존 게시·복구·감사 시험 통과 | Mongo 일괄 쓰기 |
| Delta 중복 검사 범위 축소 | join·충돌 판정·신규 행 필터(`join_existing`, `has_conflict`, `new_rows`)를 Spark DataFrame으로 시험 | 실제 Delta 테이블에서 `bucket` 조건이 파티션 가지치기로 이어지는지 |
| Silver `index`·`products` `persist()`, Bronze 검증 action 2개를 1개로 통합 | Spark 종단 시험 통과 | 실제 데이터량에서의 캐시 메모리 사용 |

묶음 처리로 시험 기대값이 바뀐 곳: ack 저장 전에 실패하면 이벤트 1건이 아니라 해당 묶음 전체가 다시 발행된다(`test_ack_loss_...`는 broker 레코드 6건이 아닌 10건). Outbox 저장 중 실패하면 원천 스냅샷은 묶음 전체가 이미 기록돼 있다(`test_partial_source_...`는 1건이 아닌 5건).

## 2026-10-23 정합성 3건·상품 이력 한도 보류 보완

전체 시험 **66개 실행, 66개 통과**(이전 60개에 새 시험 6개 추가). `PIPELINE_V2_TEST_SPARK=1`로 실제 로컬 Spark JVM(`local[2]`)과 `MemoryDelta`를 포함해 실행한 결과이며, 이 변수 없이 실행하면 Spark 시험 3개를 건너뛴다. Delta commit·`txnAppId`·time travel은 이번에도 검증하지 않았다. 각 결함은 수정 전에 재현 시험이 해당 증상으로 실패하는 것을 확인했다(상품 이력 한도 시험은 수정 전 실패를 따로 확인하지 않았다).

| 시험 | 확인한 계약 |
|---|---|
| `test_stale_withdrawal_keeps_...`, `test_mysql_stale_withdrawal_keeps_...` | 철회 결과 계산 뒤 새 관측이 게시되면 철회 게시가 `LAST_GOOD_RETAINED`가 되고 상품·이미지 연결이 활성으로 남는다(파일 게시기와 MySQL 대역) |
| `test_retry_after_lost_stage_manifest_reads_the_same_history` | Silver 저장 뒤 manifest 기록이 실패하고 다음 batch가 먼저 완료돼도 복구 재시도가 같은 입력을 읽는다 |
| `test_metric_storage_failure_does_not_undo_completed_publication` | 계측 저장이 모두 실패해도 job은 `COMPLETED`이고 `metric_failures`가 보인다 |
| `test_oversized_product_history_holds_only_that_product` (로컬) | 한도를 넘은 상품만 `QUALITY_HOLD`, 기존 게시본 유지, batch는 `COMPLETED` |
| `test_oversized_history_holds_only_that_product` (Spark) | 같은 계약을 Spark 처리기에서 확인 |

## 2026-10-23 크롤 완료 유한 배치 구현 반영

로컬 `develop`(`feature/#42`, HEAD `d234e42` + 미커밋 작업 트리) 기준이다. 실제 서버, Kafka broker, Garage, Delta JVM 쓰기, MySQL TLS 종단 시험과 실제 적재 속도 비교는 하지 않았다.

| 명령 | 결과 |
|---|---|
| `python -B -m unittest discover -s src/data_pipeline_v2/tests` | **60개 실행, 58개 통과, 2개 건너뜀**(77.7초). 건너뛴 2개는 아래 Spark 시험 |
| `PIPELINE_V2_TEST_SPARK=1`, `-p "test_spark*.py"` | **3개 실행, 3개 통과**(316.5초). native Gold, Spark 지표 보고서, 유한 배치 처리기 종단 흐름 |

새로 확인한 계약(`tests/test_batch_redesign.py`, `tests/test_spark_pipeline.py`):

- 행 단위 입력 판정: 식별자·해시 오류 행은 제한 접근 격리, 격리 비허용이면 payload 없이 metadata만 남긴다. allowlist 밖 필드는 어떤 상태·Lake·게시 파일에도 남지 않는다(검사용 문자열 전수 검색). 같은 행 재전송은 하나로 접고, 한 행 오류가 크롤 전체를 막지 않는다.
- 보존: 격리 원문은 30일, metadata는 90일 기준으로 지운다. 원문이 만료돼도 감사는 통과하고, 열람은 접근 기록을 남긴 뒤 파일로만 내보낸다.
- 단계 분리: 배치 n의 게시 잠금이 잡혀 있어도 배치 n+1 처리는 진행된다. 같은 단계의 두 번째 writer는 `WRITER_BUSY`로 멈춘다. 새 배치를 먼저 게시하면 오래된 배치는 `LAST_GOOD_RETAINED`가 된다. 파일 게시기와 MySQL 대역 모두에서 확인했다.
- 철회·재계산: 원천 버전 하나를 철회하면 해당 상품만 원천 본문 재조회 없이 다시 계산·게시하고, 모든 근거를 철회하면 `WITHDRAWN`이 된다. MySQL 대역에서는 `INACTIVE`/`SOURCE_RETRACTED`와 이미지 연결 비활성화를 확인했다.
- 용량·복구: 대기 시간 상한을 넘으면 새 예약을 거부하고, 색인은 job 원장으로 다시 만든다. 등록 충돌 시 예약을 해제한다. 전달 manifest가 바뀌면 job을 `RUNNING`으로 남기지 않고 `QUEUED`로 둔다.
- CLI: `--phase`, `batch-backlog`, `batch-retract`, `batch-repair`, `batch-retention`, `batch-quarantine-read`(격리 원문을 표준 출력에 쓰지 않음)
- 실제 Spark(local[2]) 처리기: 고정 offset 행 검증, Executor 원천 조회·분류, Silver 재사용, native Gold, 격리 metadata, 철회 후 `WITHDRAWN`, 보존 정리까지 한 흐름으로 돌렸다. **Delta 저장은 메모리 대역(`MemoryDelta`)으로 바꿨다.** Windows에 winutils가 없어 Delta jar를 붙인 Spark 세션이 시작되지 않기 때문이다. 따라서 Delta commit·time travel·`txnAppId`·`DeltaTable.delete`는 검증하지 않았다.
- 이 실행에서 미커밋 Spark 코드의 기존 결함 하나를 찾아 고쳤다. 중첩 컬럼 `e.event_id`로 `dropDuplicates`를 호출해 Bronze가 실패하던 문제다.

기존 실행기(`src/data_pipeline`) Python 구현은 이번 작업에서 바꾸지 않았다. 실데이터와 비용 큰 산출물(이미지·임베딩·인덱스)은 생성하지 않았다.

## 2026-10-23 `capture/tick` 실행기 첫 검증

새 테스트의 합성 원천과 FileLake/FilePublisher, 기존 MySQL 필수 컬럼 계약을 검사하는 MemoryDB를 사용한다. 실제 서버·Garage·Delta JVM·MySQL TLS 종단 시험과 실제 적재 속도 비교는 하지 않았다.

새 실행기 테스트 **31개 통과**(28.330초). 이 수치는 크롤 완료 유한 배치 코드가 들어오기 전 결과다. 추가한 상품 ID 제한은 Mongo run에 없는 ID를 거절하고 선택 상품의 전체 관측 이력만 탐색하는지 확인한다. 기존 실행기 전체 회귀는 동일 로컬 Python에서 633개 실행, 실패 5개·오류 8개·건너뜀 2개였다. `delta` 등 설치 의존성 누락, 기존 `test_driver_memory`의 SimpleNamespace 대역에 `_append_staged_chunk_inner`가 없는 오류, Kafka 사전 검사 오류 코드 불일치가 포함된다. 기존 실행기 Python 구현은 이번 작업에서 변경하지 않았으며 이 전체 회귀를 통과로 기록하지 않는다.

검증 명령:

```powershell
python -B -m unittest discover -s src/data_pipeline_v2/tests -v
```

검증 대상은 승인/활성화 없는 직접 게시, 각 단계별 영속 시간·scope 요약, 101관측의 전체 이력 Gold, 테이블별 묶음 commit, 무관한 정책의 작업 ID 유지, Gold 전용 정책 변경 시 Silver 재사용, 변경 없는 범위의 데이터 쓰기 0, 변경 상품만 정제, Gold→인계 사이 장애, 게시 응답 유실 후 멱등 재개, 정지 증거가 필요한 RUNNING, 오래된 입력 게시 차단, Gold 유실/변조 감지, 이력/용량 한도, 커서가 지난 차단 작업의 복구, SQL 필수 필드/이미지 연결/서비스 값 변조 감사, 스키마 검사 6조회·세션 내 재사용, CLI와 의존 방향, 소스 이전 묶음이다.

성능 관련 회귀는 **동일 합성 원천의 논리 작업량**을 확인한다. 10상품 1관측을 한 묶음으로 처리할 때 데이터 추가 commit이 18회 이하이고, 완료한 동일 입력을 새 scope로 다시 실행할 때 raw 본문 조회·정제·데이터 commit·파일 게시가 증가하지 않아야 한다. 이는 서버의 실제 파일 스캔량이나 초당 적재량 측정이 아니다. FileLake는 로컬 시험용이며 실제 Delta 버전 보존/분산 I/O와 같지 않다.

`status` 또는 `run`의 최종 JSON에서 `timing`을 보고, 개별 단계 증거는 상태 저장 경로의 `metrics/stage-times/<scope-hash>/*.json` 묶음에서 확인한다. 같은 scope를 여러 번 tick/run 하면 이벤트 묶음은 추가되고 요약은 재생성된다. 한 이벤트는 inclusive 경과 시간이므로 부모와 자식 시간을 더해 전체 시간으로 만들지 않는다.

## 남은 서버 인수

[이전 절차](server-migration.md)를 따른다. 특히 다음이 필요하다.

- Mongo 현재 스키마·조회 인덱스, 격리 컬렉션 접근 역할
- Kafka topic partition·retention, 실제 broker에서 ack 유실·offset 만료·새 generation 복구
- 실제 Delta 4.x의 snapshot 재시도·키 append·보존 delete와 물리 스캔량
- 한 번의 session에 유지한 Spark/lease 종료, 처리·게시 lease 분리
- MySQL 실제 트랜잭션과 제한 계정, 기존 상품 baseline·숫자/시간 변환, 철회 시 다중 행 UPDATE
- v3 출처 소비자 호환성
- NiFi FLOW 연결과 재기동 후 대기 batch 재조정
- 기존 실행기와 같은 입력으로 비교

MySQL 서비스 반영은 상품별 원자 관계와 안정 키별 SQL을 사용하므로 MySQL 전면 bulk-upsert 최적화가 끝났다고 주장하지 않는다. 처리와 게시를 겹치는 병행 실행의 성능 이득도 측정하지 않았다.
