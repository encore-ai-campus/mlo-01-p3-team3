# Kafka → 상품 Bronze Delta

현재 서버의 작은 window·세션 공유·게시 후 활성화 경로는 [대기 적재·연속 실행](pending_intake.md), 최신 실제 결과는 [검증 현황](service_validation_20261018.md)을 따른다. 아래 일반 CLI/초기 시험 계약과 배포된 서버 coordinator를 구분한다. Kafka 보관 만료 시 [복구 절차](kafka_retention_recovery.md)를 사용한다.

2026-10-16. 수집 종료 후 원천 적재/발행과 분리된 Spark 작업이다. 수집기·이미지 다운로드·MongoDB 원천·발행 기능은 기존 그대로 사용한다. 이번 범위는 상품 Bronze이며 Silver/Gold/MySQL·NiFi 설치·LIVE 인계는 포함하지 않는다.

## 데이터 경로

```text
전체 수집 JSON + 이미지 → prepare → ingest
  ├ 이미지용 저장소: 실제 이미지 바이트
  ├ MongoDB raw_products: 불변 원천과 상품별 갤러리 관계
  └ Kafka product.raw.collected: 원천 ID·hash 참조
       → Spark Kafka source → primary/majority 원천 조회·계약 검증
       → Bronze raw_products → source event receipt / poison 격리
       → foreachBatch 성공 → Spark checkpoint 완료
```

Bronze에는 이미지 바이너리를 복사하지 않는다. `raw_envelope_json`에 원천 전체와 이미지 URL·순서·역할·object key·checksum 관계를 보존하고 `payload_json`에는 원천 payload 내용을 보존한다. 원본 HTTP 응답 바이트와 canonical JSON은 다르며 없는 원본 응답을 새로 만들어 보존했다고 하지 않는다.

원천 ID·메타데이터·payload hash·document hash·관측/갤러리 hash와 Kafka key를 대조한다. `raw_document_id`는 Bronze 원천 키, `observation_key`는 관측 연결 키, `event_id`는 별도 전달 식별자다. 같은 상품의 다른 관측을 합치지 않는다. UTC 수집 시각은 Spark Timestamp로, 적재 시각은 별도로 저장한다.

## 초기 전체 적재와 이후 증분

1. 수집·다운로드가 끝난 전체 결과에 기존 `prepare`/`ingest`를 실행한다. 내부적으로 상품 단위로 보존하며 중단 후 같은 snapshot/state/config로 재개한다.
2. Bronze를 `--available-now`로 실행한다. 기존 checkpoint가 없으면 보존된 토픽의 earliest부터 읽고, 실행 시작 시점의 가용 데이터까지 작은 microbatch로 처리한 뒤 종료한다.
3. 원천/이벤트/격리 건수와 상품 ID·hash를 대조한다. Kafka 보관 기간 전에 전체 인계를 마쳐야 한다. 이미 만료된 이벤트는 기존 원천 적재 명령의 `--replay-events`로 동일 ID·본문을 재발행한다. checkpoint는 삭제하지 않는다.
4. 이후 같은 Bronze 설정/checkpoint에서 `--available-now` 없이 실행하면 계속 도착하는 이벤트를 처리한다. 반복적인 available-now 실행도 가능하다. 실행 중 수집 폴더는 [LIVE capture/drain](live_execution.md)으로 Kafka 앞단에 연결한다.

`available-now`는 전체 데이터를 메모리에 올리는 옵션이 아니다. 기본 한 배치 20메시지, Kafka 입력 8MiB, 조회 원천 32MiB 한도를 적용한다. 한도를 넘으면 배치를 성공시키지 않는다. 토픽 partition 수가 많으면 Spark의 maxOffsetsPerTrigger 배분이 설정 건수보다 커질 수 있으므로 한도와 partition을 맞추고 처리량을 측정해야 한다. 이 구현은 소규모 검증 기준이며 전체 수집량 처리시간은 아직 실측하지 않았다.

## 설치와 실행

선택 의존성은 Spark 4.0.1 / Delta 4.0.1이다. 실제 VM 버전은 인프라 담당과 맞춘 뒤 같은 조합으로 배포해야 한다. 기존 가벼운 원천 적재 의존성과 분리했다.

```bash
# Spark Driver용 Python 환경. 각 노드의 Python/Java/라이브러리 배포는 운영 구성으로 맞춘다.
python -m pip install -r requirements-bronze.txt
python -m pip install --no-deps -e .
```

로컬 검증의 전이 의존성까지 재현하려면 `requirements-bronze.lock.txt`를 사용한다. 이 파일은 검증 환경 버전 기록이며 다른 OS/서버에서의 설치·배포 성공을 보장한 결과가 아니다.

`config/bronze.example.json`을 Git 제외 `*.local.json`으로 복사한다. 환경·DB·실제 Kafka cluster ID·경로와 아래 lease 운영 구성을 확정한 뒤 `single_writer_confirmed=true`로 설정한다. 이 값은 운영 확인 표시이며, 실제 중복 실행은 Mongo lease가 차단한다. 테이블과 checkpoint는 서로 겹치면 안 된다.

배포 환경변수:

- `PIPELINE_MONGO_URI`: rs-products 접속 URI·TLS·인증. 원천 DB의 `raw_products`에 대한 읽기 전용 계정이다. Bronze 원천 조회는 primary/majority이며 원천·이미지 상태·원천 인덱스를 변경하지 않는다.
- `BRONZE_LEASE_MONGO_URI`: 별도 운영 계정의 rs-products 접속 URI·TLS·인증. 원천 계정과 구분하며 `pipeline_ops.bronze_writer_leases`에만 `find`, `insert`, `update`, `remove`, `listIndexes` 권한을 부여한다. 원천 DB 쓰기 권한이나 `createIndex`/`collMod`/`dropCollection` 권한은 필요하지 않다. URI 환경변수 이름은 `lease_uri_env`로 바꿀 수 있다.
- `BRONZE_KAFKA_OPTIONS`: Spark Kafka option JSON. 예: `{"kafka.bootstrap.servers":"<broker endpoints>","kafka.security.protocol":"<approved protocol>"}`. TLS/SASL 관련 Java Kafka 설정도 `kafka.` 접두어로 넣는다. 원천 발행기의 librdkafka 설정을 그대로 복사하지 않는다.
- `BRONZE_SPARK_OPTIONS`: Spark 설정 JSON. Driver/Worker의 S3A endpoint·인증·path-style·TLS·자원 설정을 주입한다. 비밀은 환경/역할/비밀 저장소를 사용하고 셸 이력·Git에 넣지 않는다. 로컬 작업은 `spark.master=local[2]` 등으로 지정할 수 있다.

다음은 **버전/경로를 검토한 뒤 사용하는 제출 예시**다. 실제 주소·계정이 포함된 완성 명령은 아니다.

```bash
spark-submit \
  --master spark://<spark-master>:7077 \
  --deploy-mode client \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1,io.delta:delta-spark_2.13:4.0.1 \
  run_bronze.py --config config/bronze.local.json --available-now

# 계속 수신하려면 같은 명령에서 --available-now만 제거한다.
```

S3A의 `hadoop-aws`와 AWS SDK는 Spark에 포함된 Hadoop과 맞춰 배포한다. 위 명령에 무작정 다른 버전 JAR를 더하지 않는다. 이미지 저장소와 lake 저장소 endpoint를 혼동하지 않는다. 단일 Driver S3 Delta 쓰기 계약과 해당 S3 호환 서버의 Delta 저장소 호환 시험이 선행 조건이다.

## Driver lease 준비와 실행 순서

운영자가 시작 전에 `lease_database`/`lease_collection`의 컬렉션을 미리 만든다. 기본값은 `pipeline_ops.bronze_writer_leases`이며 `lease_database`는 원천 `mongo_database`와 달라야 한다. 컬렉션은 **unsharded**, 기본 unique `_id` 인덱스, **TTL 인덱스 없음**으로 관리한다. 실행 계정은 이 구성을 변경할 권한을 받지 않는다. 시작 시 인덱스 조회가 불가능하면 `BRONZE_LEASE_COLLECTION_UNCONFIRMED`, TTL 인덱스가 있으면 `BRONZE_LEASE_TTL_FORBIDDEN`으로 실패한다. TTL로 오래된 소유권을 삭제하면 아래 안전 전제가 깨진다.

같은 lake를 쓰는 모든 Driver/호스트는 **같은 Mongo replica set, 같은 운영 DB·컬렉션, 같은 canonical 대상 URI**를 사용해야 한다. URI 별칭·다른 운영 namespace로 동일 저장소를 우회하지 않도록 배포 설정에서 고정한다. 다른 Mongo 클러스터의 잠금을 서로 확인할 수는 없다. 설정 예시의 `lease_seconds=120`, `lease_heartbeat_seconds=10`을 기본으로 사용한다. 허용 범위는 lease 60~3600초, heartbeat 1초 이상 및 lease의 1/6 이하이다. 시간 판단은 Driver 벽시계 대신 Mongo 서버의 `$$NOW`를 사용한다.

실행 순서는 다음과 같다.

1. 설정·환경변수를 검증하고 각 테이블/체크포인트 URI에 대해 운영 컬렉션의 unique `_id`를 원자적으로 claim한다. `_id`는 `bronze-path:H(canonical URI)`이며 문서에는 `token`, `target_uri`, `target_fingerprint`, 서버 `heartbeat_at`/`expires_at`을 기록한다. 환경명이나 fingerprint만 바꾸어 같은 경로를 재사용해도 차단된다. 네 경로 중 일부만 공유하는 실행도 차단된다. 일부 claim 후 충돌하면 이번 실행이 확보한 문서만 token 조건으로 해제하며 대상 저장소는 아직 변경하지 않는다. claim 결과가 통신 오류로 불확실하면 자동 해제하지 않는다.
2. 모든 claim을 확보한 뒤 Spark를 만들고 heartbeat를 시작한다. Kafka cluster 확인, checkpoint binding, Delta 생성, query 시작 전에 소유권과 대상 fingerprint를 확인한다. claim 충돌은 `BRONZE_WRITER_ALREADY_CLAIMED`이며 두 번째 Driver는 Spark/query/대상 저장소 변경 전에 실패한다.
3. heartbeat와 각 배치는 token 및 미만료 조건으로 소유권을 갱신한다. 각 Delta 생성/merge 직전·직후와 배치 성공 반환 전에도 확인한다. 원천 commit 후 lease를 잃으면 뒤의 receipt/격리 쓰기와 배치 성공을 중단한다. 여러 Delta commit이 한 트랜잭션으로 바뀌는 것은 아니다.
4. 만료·token 변경·Mongo 통신 장애를 확인하면 `BRONZE_WRITER_LEASE_LOST`로 영구 실패 처리한다. 이후 통신이 돌아와도 같은 Driver는 다시 활성화되지 않는다. heartbeat는 Spark 작업 취소와 query 중단을 요청한다. 콜백이 손실을 확인하면 예외를 발생시켜 해당 배치 checkpoint 완료를 방지한다.
5. CLI의 정상 완료·오류·Ctrl+C·SIGTERM은 finally에서 query, 원천 연결, Spark 중단을 각각 시도한다. 모두 중단 확인된 건강한 소유권만 token 조건으로 삭제한다. 종료를 확인하지 못하면 `BRONZE_SHUTDOWN_UNCONFIRMED`로 실패하고 claim을 남긴다. SIGKILL·호스트 장애처럼 finally를 실행할 수 없는 경우도 claim이 남는다.

**만료된 claim은 자동 takeover하지 않는다.** Mongo token은 Delta/S3 commit 안에서 검증되는 fence가 아니며, 이미 진행 중인 객체 쓰기·Delta commit·Spark checkpoint I/O를 소급 취소하지 못한다. 마지막 확인 직후 정지/장애가 발생하면 진행 중인 쓰기가 끝날 수 있다. 따라서 이 구현을 엄격한 분산 저장소 fence나 즉각적인 쓰기 권한 회수로 설명하지 않는다. 오래된 Driver가 실제로 멈췄다는 확인 전까지 다음 Driver의 시작을 막는 fail-closed 운영 잠금이다.

### 남은 claim의 수동 복구

1. 자동 재시작/스케줄러 제출을 중지하고 해당 `target_uri`/`target_fingerprint`와 이전 `token`의 문서를 확인한다. heartbeat 만료만으로 Driver 종료를 판단하지 않는다.
2. 이전 Driver 프로세스와 연결된 Spark query/실행 작업이 종료되었고 추가 저장소 쓰기가 없음을 운영 환경에서 확인한다. 확인할 수 없으면 claim을 유지하고 새 실행을 제출하지 않는다.
3. 운영 권한으로 **확인한 이전 token 및 대상 URI가 일치하는 claim만** 삭제한다. 해당 실행이 남긴 여러 경로 문서도 모두 확인한다. 새 실행의 다른 token이나 다른 작업 문서를 함께 지우지 않는다. 이 단계는 자동 TTL 삭제나 실행 CLI의 강제 takeover 옵션으로 대체하지 않는다.
4. 기존 Delta 데이터와 checkpoint를 보존하고 동일 설정으로 한 Driver만 재제출한다. 부분 commit은 재처리 시 내용 대조와 insert-only 동작으로 복구한다. checkpoint 삭제는 복구 절차가 아니다.

## 저장·중복·복구

| 저장 영역 | 키와 내용 |
|---|---|
| `bronze/raw_products` | raw_document_id, source/collection/observation/hash, payload_json, raw_envelope_json, collected_at, ingested_at |
| `bronze/product_raw_events` | event_id·불변 본문/hash, raw ID/hash, 최초 Kafka 위치 |
| `quarantine/product_raw_collected` | H(topic,partition,offset,value_sha256), 원문 key/value base64·hash·오류·시각 |
| `project-checkpoints/raw-ingestion` | Spark offset/commit 및 `_pipeline_target` 작업 대상 바인딩 |

Delta 테이블은 insert-only MERGE하고 기존 행을 UPDATE하지 않는다. 동일 배치 내부도 먼저 중복을 정리한다. 같은 raw ID의 다른 원천, 같은 event ID의 다른 본문은 DATA_CONFLICT로 격리한다. Delta에 자동 UNIQUE 제약이 있다고 가정하지 않는다. 각 테이블은 한 Driver만 쓰며 위 운영 lease와 공유 namespace 규칙으로 중복 제출을 차단한다. `single_writer_confirmed`만으로 보호된다고 보지 않는다.

원천 commit → event receipt → 격리 기록은 별도 Delta commit이다. 중간 실패는 다음 실행에서 내용 대조 후 이어서 반영한다. 이 전체가 한 트랜잭션인 것은 아니다. 모든 처리/격리 저장과 대조가 끝나기 전에 foreachBatch를 성공 반환하지 않는다. 수동 Kafka offset commit과 Spark checkpoint를 혼합하지 않는다.

원천 미확인·MongoDB 장애·저장소 오류는 재시도 대상이며 작업을 실패시켜 checkpoint 진전을 막는다. 지원하지 않는 schema·변조·깨진 본문·확인된 Mongo 원천의 구조 오류는 영속 격리한다. 격리 저장 실패도 작업 실패다. 격리는 정상 데이터로 바뀐 것이 아니므로 자동 재주입하지 않고 운영 검토 후 원래 이벤트 ID/본문으로 지정 범위를 재처리한다.

checkpoint와 각 테이블을 대상 fingerprint로 묶고 실행 전에 실제 Kafka cluster ID를 조회·대조한다. 다른 DB·토픽·클러스터·경로로 설정을 바꿔 기존 checkpoint를 재사용하면 차단한다. 바인딩 없는 기존 checkpoint도 자동 수용하지 않는다. 재개 시 동일 설정·동일 checkpoint로 다시 제출한다. 이전 checkpoint 삭제를 복구 방법으로 사용하지 않는다.

CLI 종료는 완료=0, 실패=2, 사용자 중단=130이다. 배치별 received/accepted/quarantined, execution_id와 duration_ms를 출력한다. accepted는 처리 성공한 메시지 수이며 신규 Bronze 행 수와 다르다. 원천·이벤트 수·이미지 관계 수·고유 파일 수도 각각 대조해야 한다.

## 검증과 남은 인수

단위 테스트는 MongoDB/Kafka/S3 없이 기존 회귀 테스트와 함께 실행한다.

```powershell
.\.venv\Scripts\python.exe -m unittest discover -s tests -v
# 로컬 실제 Spark/Delta. 저장된 리허설 3상품을 입력으로 사용, 새 출력 폴더 필요.
.\.venv\Scripts\python.exe -m tests.smoke_bronze_delta --source-rehearsal .runtime/actual-smoke-resumed-v1 --output .runtime/bronze-delta-new
```

로컬 Spark 실행에는 Java와 Hadoop 파일시스템 환경이 필요하다. Windows는 별도 Hadoop native 도구가 필요할 수 있으며 VM/Linux 실행 환경과 구분한다. `smoke_bronze_delta`는 새 로컬 `file://` 출력 경로와 테스트용 `DummyLease`를 사용한다. 보고서의 `lease_backend=localtest-double`은 실제 Mongo lease를 검증하지 않았다는 뜻이다. 중복 claim·만료·heartbeat 장애·잘못된 대상·부분 claim 응답 손실·TTL 거부·안전 종료 순서는 `test_bronze_lease`의 외부 서비스 대역으로 검증하며 실제 서버 장애 전환 시험과 구분한다.

2026-10-16 검증 결과:

| 검증 | 결과 | 증거 |
|---|---|---|
| 최초 Bronze 자동 테스트 | 당시 69개 통과, 29.594초. 현재 보완 시험은 이슈 충족표 참조 | `.runtime/unit-tests-bronze-final.txt` |
| 실제 로컬 Delta | Spark 4.0.1 / Delta 4.0.1, 원천 3건·이벤트 receipt 3건 | `.runtime/bronze-delta-v3/report.json` |
| 원천 보존 | 보관 상품 3개 전체 envelope/payload·UTC 수집 시각 일치 | 같은 보고서의 `payload_envelope_timestamp_match=true` |
| 중복/재개 | 동일 이벤트 재처리 및 새 DeltaSink 인스턴스에서도 원천/이벤트 각각 3건 유지 | 같은 보고서의 counts 및 테스트 코드 |
| 격리 | 깨진 JSON 1건과 동일 event ID의 다른 본문 1건, 재전달 후 총 2건 유지 | 같은 보고서의 quarantine=2 |
| 외부 서버 | MongoDB/Kafka/S3 연결 0 | Source만 보관 파일 대역, Delta는 실제 로컬 저장 |

이슈 보완 후에도 실제 로컬 Spark/Delta 시험을 재실행했다. `.runtime/bronze-gap-v1/report.json`: passed=true, 원천 3건·event receipt 3건·격리 2건, 원문/UTC 시각 일치, 새 Sink·반복 처리 후 동일 건수. 이 실행의 lease_backend는 `localtest-double`이며 실제 Mongo lease 인수와 구분한다.

Windows 검증에서 초기 경로 표현 차이와 Python worker 종료를 재현해 수정했다. 최종 성공 실행에서도 JVM 종료 시 잠긴 임시 JAR 삭제 경고가 남았으나 프로세스 종료 코드 0, Delta commit 및 내용 대조는 성공했다. VM/Linux의 실제 클러스터 시험과 이 결과를 구분한다.

아직 남은 시험: 실제 Kafka를 통한 available-now/연속 수신·checkpoint 재시작, MongoDB 장애 전환과 서로 다른 호스트의 중복 lease 획득 차단·heartbeat 장애·강제 종료 후 수동 복구, 실제 lake의 Delta commit/재부팅 보존·S3A 인증, 클러스터 네트워크/Worker 배포, 전체 초기 데이터 건수/처리량 대조. 로컬 성공을 VM 서버 연동 완료로 표현하지 않는다.

공식 기준: [Spark Kafka source](https://spark.apache.org/docs/4.0.1/streaming/structured-streaming-kafka-integration.html), [Structured Streaming foreachBatch](https://spark.apache.org/docs/4.0.0/streaming/apis-on-dataframes-and-datasets.html), [Delta MERGE](https://docs.delta.io/delta-update/), [Spark/Delta 호환표](https://docs.delta.io/releases/), [Delta 저장소](https://docs.delta.io/delta-storage/).


## 로컬 호환성 기록

초기 Delta CREATE의 file:///와 Hadoop file:/ 표현 차이는 Hadoop 경로로 통일했다. Windows Python worker 종료 문제 때문에 Driver의 JSON을 Spark SQL from_json으로 전달하며, Executor Python worker로 원문을 다시 직렬화하지 않는다. 초기 구현 계획은 완료되어 이 실행 가이드로 통합했다.
