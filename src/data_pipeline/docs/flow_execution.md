# 전체 단계 실행·대조·복구

2026-10-18. `product_pipeline/operations/flow*.py`는 기존 각 단계 구현을 연결하는 단일 호스트 실행기다. 서버 설치·크롤러 실행·승인 생성·서비스 실행 등록 원장 편집을 수행하지 않는다. 실제 운영 연결 완료가 아니라 실행 코드와 로컬 시험을 제공한다.

## 입력·책임

`config/flow.example.json`을 Git 밖의 `flow.local.json`으로 복사한다. 봉인된 완료 snapshot/LIVE microbatch, 각 단계 설정, 실제 담당 Silver/Gold 입력·REGISTERED 등록 export, 게시 guard hash 파일을 제공한다. 상대 파일 경로는 flow 설정 파일의 폴더 기준이다. 설정·입력마다 hash를 고정하며 변경되면 기존 실행을 조용히 이어가지 않고 새 실행을 준비한다.

환경 ID·Mongo DB/Replica Set·Kafka 토픽, Bronze→Silver→Gold lake 경로 대응과 등록 export를 준비 단계에서 검증한다. fixture 등록을 live에서 허용하지 않는다. 미제공 업무 snapshot은 기존 Silver/Gold 계약대로 NOT_PROVIDED 상태가 가능하지만 파일/등록 계약 자체는 필요하다. guard hash 파일은 담당이 제공하고 DB의 현재 guard와 게시 시 다시 대조한다. 실행기가 승인을 만들지 않는다. `as_of`는 고정 업무 기준 시각이며 실제 게시 시점의 가격/링크/guard 만료 재검사를 생략하지 않는다.

## 실행

```bash
python run_operations.py flow-prepare \
  --config /etc/product-pipeline/flow.local.json \
  --output /srv/pipeline/flows/batch-001
python run_operations.py flow-run --state /srv/pipeline/flows/batch-001
# 일반 실패 후 같은 명령을 다시 실행한다.
python run_operations.py flow-run --state /srv/pipeline/flows/batch-001
```

전체 순서:

| 단계 | 실행·확정하는 근거 |
|---|---|
| INGEST | 기존 이미지/Mongo/Kafka 적재기와 원장·고정 ID 재사용 |
| BRONZE | 기존 CLI의 available-now로 현재 backlog 처리 후 raw/events/quarantine Delta 버전 고정 |
| SILVER | 앞 단계의 고정 Bronze 버전·담당 입력으로 전체 처리, 완료 후 8개 Silver 버전 고정 |
| GOLD | 고정 Silver 버전·담당 입력·고정 as_of, 전체 완료 GOLD_EXPORT |
| PREPARE | 완료 export의 고정 Gold·현재 제공 guard hash로 게시 계획 ready-last 생성 |
| PUBLISH | 기존 원자 게시기·SQL receipt 재사용. 품질/승인 보류 상품은 쓰지 않음 |
| RECONCILE | 요청 snapshot·Mongo/이미지 객체·Bronze 이벤트 receipt·Silver/Gold 근거·계획·SQL 연결 대조 |

Bronze/Silver/Gold는 각자 별도 checkpoint·Driver lease를 사용한다. 실행기는 고정 Python 모듈을 자식 프로세스로 호출하며 임의 shell/command/executable을 설정에서 받지 않는다. 지정 실행 호스트에는 Java·Spark/Delta·고정 Python 의존성과 모든 대상의 접근 권한이 필요하다. Spark Master 주소는 각 SPARK_OPTIONS의 `spark.master`로 정한다. 이 코드는 SSH 원격 제출·NiFi/VM 프로그램 설치를 대신하지 않는다.

`flow.json`은 입력 identity, `receipts/STAGE.json`은 이전 receipt hash·출력·파일 hash, `attempts/STAGE.json`은 시도 상태/token/duration이다. 이전 단계 증거가 없거나 변조되면 후속 재사용을 차단한다. receipt 없이 작업이 끝난 경우 같은 단계가 다시 실행될 수 있으므로 기존 단계의 멱등 원장/Delta/SQL receipt가 업무 중복을 방지한다. 개별 CLI의 실패가 성공 receipt로 바뀌지 않는다.

모든 단계 완료 시 `result.json`의 COMPLETED 또는 COMPLETED_WITH_QUALITY_WAIT를 남긴다. 후자는 확정된 보류 상품이 있다는 의미이며 전체 상품 게시·추천 승인이 아니다. PREPARE ready의 blocked 사유를 확인한다. Gold/관측이 없는 빈 범위는 자동 성공/허위 export를 만들지 않고 차단한다.

## NiFi·예약 연결

`operations.local.json`의 `flow_directories`에 준비 완료 flow 상태 경로를 넣는다. `logical_target`은 flow의 `target`과 같아야 한다. 기존 tick이 FLOW 작업을 고정 ID로 등록하고 순차 실행한다. 기존 RECEIVE/INGEST/PUBLISH/RECONCILE도 유지한다. flow별 PUBLISH를 중복 예약할 필요는 없다.

queue worker.lock은 전체 FLOW를 실행하는 동안 유지되므로 같은 호스트의 다른 tick은 동시에 실행하지 않는다. 실제 NiFi ExecuteProcess의 실행 제한/종료 정책은 긴 단발 flow 소요시간에 맞춰 인수해야 한다. 현재 NiFi 도구의 processor 생성·수동 복구 확인·버전 대조 절차는 [운영 가이드](operations_execution.md)를 따른다. 실제 NiFi 적용은 아직 미실행이다.

## 강제 종료·정지 불명확

작업 중 인터럽트, worker 응답 미확인 또는 JVM 종료 미확인은 RUNNING을 보존한다. `workers/STAGE/<id>/child.json`의 PID와 실행 로그를 참고해 Python 및 Spark/JVM/Executor가 실제 정지·격리됐는지 확인한다. 시간 경과나 PID 파일만으로 자동 해제하지 않는다.

```bash
python run_operations.py flow-recover --state /srv/pipeline/flows/batch-001 \
  --stage GOLD --expected-token '<attempts/GOLD.json token>' \
  --stop-evidence '<담당자가 확인한 Driver/JVM 정지 증거>'
```

queue로 실행 중이었다면 기존 queue task도 RUNNING일 수 있다. flow 및 기존 단계의 공용 lease 정지/복구를 확인한 뒤 queue의 `recover --expected-token ... --stop-evidence ...`를 수행하고 tick을 재개한다. 잠금·checkpoint 삭제나 실행 중인 자식의 무조건 종료 명령을 제공하지 않는다.

## 대조 범위와 재점검

대조는 건수 일치만으로 통과하지 않는다. snapshot의 observation/raw/event ID·document/payload/event hash를 Mongo와 고정 Bronze에서 확인하고, Silver receipt의 정확한 출력 hash, Gold source refs, 계획의 planned/blocked 상품 집합, SQL publication/staging·상품 identity·이미지/asset·Offer/가격 이력 연결을 검사한다. 품질 보류는 SQL 누락으로 오판하지 않는다. 현재 서비스 필드나 담당 active/승인은 이후 변경될 수 있어 과거 계획을 현재 값에 강제 대입하지 않는다.

이미 AVAILABLE로 확인한 이미지 객체는 실제 바이트/hash를 다시 읽는다. 큰 image 재점검은 I/O 비용이 있으므로 소량부터 인수한다. Bronze 이벤트 receipt는 해당 이벤트를 처리한 증거이며 **Kafka가 현재 메시지를 보존한다는 전수 검사와 다르다**. 별도 Kafka 보관/ACL/failover 인수가 필요하다. 고정 lake 범위에 이전 상품이 있으면 Gold scope에 포함될 수 있으며 요청 snapshot의 각 관측 포함 여부와 export 전체 planned/blocked 분류를 별도로 확인한다.

완료 flow의 반복 실행은 외부 저장소를 매번 읽지 않고 확인된 단계 receipt를 재사용한다. 외부 삭제·변조를 새로 검사하려면:

```bash
python run_operations.py flow-audit --state /srv/pipeline/flows/batch-001
```

별도 `audits/<id>/`에 새 대조 결과를 남기고 기존 성공 receipt·서비스 데이터를 수정하지 않는다. 오류는 자동 삭제·수정하지 않고 실패 코드로 보고한다. 등록/승인 변경은 새 입력 계약과 새 flow로 반영한다.

## 용량·로그

```bash
python run_operations.py storage-report --root /srv/pipeline \
  --max-bytes 107374182400 --min-free-bytes 10737418240 --log-retention-days 30
```

숫자는 예시이며 실제 디스크·원천/이미지/Delta/backup 예산을 측정해 확정한다. 선택적 `operations.local.json.storage_policy`에 root/max_bytes/min_free_bytes/log_retention_days를 넣으면 tick 진입 시 용량/여유 부족을 차단한다. 진행 로그는 worker 시도당 허용 JSON 필드만 최대 1MiB 저장하며 raw SDK/JVM 메시지를 저장하지 않는다. 오래된 progress 로그는 정리 검토 경보만 내고 원천·checkpoint·queue·복구 증거를 자동 삭제하지 않는다. 중앙 로그 전송·운영 경보 채널과 보관 삭제 승인은 배포 담당 인수 범위다.

## 검증 경계

`tests/test_flow.py`는 순서·재개·증거/hash·동시 실행·자식 오류·65,498건 합성 stream 대응을 검증한다. `tests/smoke_flow_delta.py`는 실제 로컬 Delta와 파일 대역으로 입력→Bronze→Silver→Gold→품질 보류를 연결하고 Gold 저장 후 실패·재개를 시험한다. 이는 명시적 시험 adapter이며 실제 Kafka/Mongo/S3/NiFi/VM CLI 종단 시험이 아니다. 실제 MySQL의 게시 연결 대조는 별도 격리 smoke로 검증한다. 합성 stream benchmark는 비교 함수의 시간/메모리만 측정하며 JSON 파싱·정렬·Spark·업로드·전체 ETL 용량 보장이 아니다. 실제 서비스 DDL/등록/guard, Linux/VM 종단·장애·전체 수집량 성능 인수는 남아 있다.

최종 로컬 전체 회귀는 **218개/104.135초/OK**였다. 실제 Delta 연결·운영 대조·격리 MySQL 및 합성 비교 결과는 [검증 기록](validation_20261018.md)의 후속 FLOW 절을 따른다.
