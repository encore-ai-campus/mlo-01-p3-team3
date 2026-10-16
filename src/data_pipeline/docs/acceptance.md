# 실제 서비스 인수 검증

`tests.smoke_live_services`는 실제 MongoDB → S3 이미지 → Kafka → Spark Kafka source → Bronze Delta 경로를 검증한다. 모의 구현으로 대체하거나 연결 실패를 건너뛰지 않는다. 현재 로컬 리허설 결과만으로 이 검증을 PASS로 기록하면 안 된다.

## 실행 전 준비

- `config/environment.example.json`, `config/bronze.example.json`을 각각 무시되는 `*.local.json`으로 복사하고 실제 승인된 대상 이름으로 채운다. environment/database/replica set/topic/Mongo URI 환경변수 이름은 두 설정에서 일치해야 한다. Bronze의 실제 Kafka cluster ID와 `single_writer_confirmed: true`가 필요하다.
- 이미지가 검증된 1~10개 상품의 봉인된 snapshot을 준비한다. 원본 export를 변경하지 않는 `tests.smoke_saved_export`의 `snapshot` 출력도 사용할 수 있다. 해당 observation key는 대상 MongoDB에 아직 없어야 한다.
- MongoDB는 데이터 노드 3개 replica set, primary/majority read 및 `w=majority,j=true` 쓰기를 지원해야 한다. 계정에 raw/운영 컬렉션과 인덱스 생성 및 읽기/쓰기 권한이 필요하다.
- Bronze raw 조회 계정과 별도로 `BRONZE_LEASE_MONGO_URI`에 운영 lease 계정을 설정한다. 기본 `pipeline_ops.bronze_writer_leases`에 majority/journal 쓰기, 조회 및 자신이 소유한 lease 해제 권한이 필요하다. 모든 writer가 같은 운영 namespace를 사용해야 한다. 이 검증은 두 stream 실행 동안 한 lease를 유지하며, Spark 종료 확인 뒤 자신의 운영 claim만 해제한다. lease를 잃거나 종료가 불확실하면 claim을 남긴다.
- 기존 S3 버킷 및 HTTPS endpoint(또는 `aws`)와 bucket head/object read/conditional put 권한이 필요하다. AWS SDK 자격 증명 체인을 사용한다.
- Kafka 토픽은 미리 생성되어 있어야 한다. 각 partition replication factor=3, 현재 ISR>=2, 유효 topic `min.insync.replicas`>=2가 필요하다. producer metadata 조회, topic DescribeConfigs, 쓰기 및 Spark 읽기 권한이 필요하다. 이 검증 중 다른 상품 이벤트가 들어오지 않는 격리된 토픽을 사용한다.
- JVM, PySpark, Delta Python 패키지와 호환되는 Delta/Spark Kafka JAR, S3A 사용 시 Hadoop AWS JAR/자격 증명을 미리 배치한다. 검증 명령은 설치/다운로드를 수행하지 않으며 `spark.jars.packages`/`spark.jars.repositories` 설정을 거부한다. `spark.jars` 또는 사전 구성된 classpath를 사용한다.
- Bronze의 raw/events/quarantine/checkpoint 경로 4개는 모두 존재하지 않는 전용 경로여야 한다. 실행은 파일/테이블/체크포인트/이벤트를 남기며 삭제하지 않는다. 새로운 로컬 output도 필요하다.

기본 환경변수는 `PIPELINE_MONGO_URI`, `PIPELINE_S3_ENDPOINT`, `PIPELINE_KAFKA_CONFIG`(librdkafka JSON), `BRONZE_KAFKA_OPTIONS`(Spark의 `kafka.*` 문자열 옵션 JSON), `BRONZE_SPARK_OPTIONS`(`spark.*` 문자열 옵션 JSON), `BRONZE_LEASE_MONGO_URI`이다. Kafka bootstrap 서버 목록은 producer와 Spark 설정에서 같아야 한다. 비밀 값은 명령 인수나 버전 관리 파일에 넣지 않는다.

## 실행

`src/data_pipeline`에서 기존 Python 환경으로 실행한다:

```powershell
python -m tests.smoke_live_services `
  --snapshot .runtime/selected-export/snapshot `
  --config config/acceptance.local.json `
  --bronze-config config/bronze-acceptance.local.json `
  --output .runtime/live-acceptance-new `
  --allow-live-writes --query-timeout 300
```

검증은 첫 ingest의 raw 생성 및 broker delivery 확인, Mongo 전체 문서 재조회, S3 실제 바이트 checksum 재조회, 같은 snapshot 재실행 시 raw/object/event 추가 0을 확인한다. 실제 Kafka를 Spark `availableNow`로 읽고 Delta를 다시 읽어 ID/원문/event 본문/개수를 대조한다. 이어서 동일 ID의 이벤트를 실제 Kafka에 재발행하고 같은 checkpoint로 다시 소비하여 Delta raw/event 개수가 증가하지 않는지 확인한다. quarantine이 1건이라도 생기거나 다른 이벤트가 섞이면 실패한다. 장애 주입/노드 failover 시나리오는 이 명령의 범위에 포함되지 않는다.

## 결과 해석

- exit 0, `status: PASS`: 모든 실제 검증을 통과했다.
- exit 2, `status: FAIL`: 잘못된 설정 또는 연결/쓰기/재조회/중복 방지 검증이 실패했다. 실패 단계와 안전한 error code를 확인한다. 부분 적재가 남을 수 있다.
- exit 3, `status: NOT_RUN`: 명령 인수, 명시적 쓰기 허용 또는 필수 환경변수가 없다. PASS가 아니다. 네트워크 연결을 시작하지 않는다.

stdout JSON과 새 output의 `report.json`에 전체/단계별 duration 및 결과를 기록한다. ingest 감사 기록은 `audit/`, Bronze 배치 결과는 `batches/`에 남는다. `.runtime/` 아래 결과는 Git에서 무시된다. 자격 증명/SDK 예외 원문은 보고서에 기록하지 않는다. 실패 뒤 서버 데이터를 자동 삭제하거나 fresh 검사를 우회하지 말고, 감사 기록을 검토한 후 별도 대상/새 observation으로 재검증한다.
