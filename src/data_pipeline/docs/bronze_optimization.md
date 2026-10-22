# Bronze 묶음 I/O·세션 재사용 개선

2026-10-21. 기존 원천·이벤트 ID, Delta 경로와 스키마, 논리 대상 fingerprint, Kafka offset·체크포인트 세대는 바꾸지 않는다. 실제 운영 연속 처리 재개와 격리 비교 시험을 구분한다.

## 변경한 여섯 항목

| 항목 | 적용 방식 | 유지하는 검증 |
|---|---|---|
| 중복 조회 | 배치의 raw ID·관측 키·이벤트 ID를 한 번에 대조. raw ID와 관측 키는 raw 테이블 한 번의 필터로 읽고 event와 union해 collect | 중복 키·같은 관측의 다른 원천·본문 충돌 차단 |
| MERGE 제거 | 단일 작성자 lease 아래 신규 행만 append. 사전 대조는 배치 단위, 저장 확인은 테이블 단위 | 기존 행 불변, 저장 응답 유실 시 실제 저장분 재조회·멱등 재개 |
| MongoDB | 최대 100개 고유 ID를 `$in` 조회. cursor는 최대 20행 단위로 받고 전체 canonical 원문 바이트 상한을 적용 | 기존 PRIMARY·majority·Replica Set 확인·문서 hash 검증·미확인 원천 재시도 |
| 초기화 | 기존 Delta는 생성 명령을 생략하고 스키마를 확인. 세 테이블의 속성을 union해 조회 | fingerprint·appendOnly·스키마 대조; 신규 대상만 명시적 생성 |
| 실행 비용 | 서버 coordinator가 순차 단계에서 하나의 `SessionScope`를 소유. Bronze query 종료·작업 소멸 확인 후 후속 단계와 다음 epoch가 JVM·연결 풀을 재사용 | 단계별 별도 writer lease, 설정 identity 변경 차단, 정지·실패 시 최종 Spark 종료 |
| 계측 | `bronze-window.json.runtime_metrics`와 `bronze-result.json.bronze_runtime_metrics`에 원인별 시간·호출 수 기록 | 실패한 호출도 계측하며 원문·암호는 기록하지 않음 |

2MiB·10행의 JSON 쓰기 조각 상한은 유지한다. 큰 Catalyst 리터럴을 피하기 위한 메모리 경계이며 요청 배치를 10개로 줄인 것이 아니다. 작은 조각별 MERGE·사전 조회·저장 확인을 없앴지만 모든 조각을 무제한 단일 리터럴로 합치지는 않는다. 조각마다 lease를 확인하고 모든 신규 행을 다시 읽어 대조한 뒤에만 foreachBatch가 성공한다. raw→event→quarantine은 독립 Delta 커밋이며 분산 원자 트랜잭션으로 표시하지 않는다.

준비된 조회 결과는 해당 배치·테이블의 한 번의 쓰기에만 사용하고 쓰기 전에 소비한다. 실패 후 부재 캐시를 재사용하지 않는다. 다음 callback은 사전 대조부터 다시 시작한다. Mongo의 손상 행은 해당 이벤트의 poison으로 구분하고, DB 장애·원천 누락·용량 초과는 배치 실패로 처리해 offset 완료를 막는다.

## 계측 해석

- `spark_connect`, `mongo_connect`, `kafka_connect_and_frontiers`, `checkpoint_binding`, `delta_target_open`: 연결·시작·대상 확인.
- `mongo_batch_read`, `mongo_documents`, `mongo_raw_bytes`: 묶음 읽기·고유 문서 수·바이트. 한 find와 cursor getMore는 네트워크 요청 한 번을 뜻하지 않는다.
- `delta_batch_lookup`, `delta_lookup`, `delta_append`, `delta_storage_verification`: 사전 조회·보조/확인 조회·조각 쓰기·저장 대조.
- `lease_acquire`, `lease_renew`, `lease_close`: MongoDB majority 잠금 왕복 비용. 잠금 유효성 검사를 생략하지 않는다.
- `query_run`, `foreach_batch`, `query_stop`, `mongo_close`, `spark_release`: 실행·종료 경계.

`delta_stage_<table>_chunk`는 SDK 종료까지 포함한 표별 전체 청크 구간이며 `input_rows`·`input_bytes`는 청크 행 수와 직렬화한 입력 바이트다. `local_serialize`, `local_checksum`, `client_open`, `put`, `get`, `readback_verify`, `spark_json_plan`, `delete`, `client_close`로 임시 입력 경로의 단계를 나눠 기록한다. `_get`은 응답 객체 수신까지, `_readback_verify`는 응답 본문 읽기와 SHA-256 확인까지 측정하며 `_readback_bytes`는 읽은 바이트 수다. `_filesystem_copy`는 Garage S3 프로필이 아닌 경로의 파일 복사 시간이다. `delta_append_<table>`과 `delta_storage_verification_<table>`은 표별 저장 시간이다. `lease_renew_mongo_update`는 lease 검사 중 MongoDB의 각 `find_one_and_update` 호출이다.

접미사 `_calls`는 코드 호출 수, `_ms`는 해당 구간 wall time, 선택적으로 기록되는 `_max_ms`는 단일 호출 중 최댓값이다. 평균 호출 시간은 `_ms / _calls`로 계산한다. Spark JSON DataFrame 생성은 지연 실행이므로 `delta_stage_<table>_spark_json_plan_ms`는 계획 생성 시간만 포함하며 실제 파일 읽기는 `delta_append_ms` 안에서 일어난다. 청크 전체·세부 단계·Delta append·lease heartbeat 측정은 서로 중첩될 수 있고 heartbeat는 다른 스레드에서 실행되므로 값을 더해 전체 소요 시간으로 해석하지 않는다. 원문·객체 키·hash·암호는 기록하지 않는다. Spark callback·heartbeat는 동일 invocation의 thread-safe profile에 연결한다. `spark_session_reused`는 그 invocation이 기존 scope의 Spark를 빌렸는지 나타낸다. 실제 최종 JVM 종료 시간은 `continuous-runtime.json.metrics.spark_shutdown_ms`에 남는다.

## 배포·중지·재개

원래 작업 루트는 `/var/lib/pj3/product-pipeline/spark/production-pending-20261019`다. `product_pipeline/bronze/metrics.py`를 포함한 Bronze 소스와 `spark_session.py`, `production_work.py`·`continuous_work.py`·`production_window_work.py`를 함께 배포한다. Bastion의 기존 `production_manage` Ansible 모듈도 소스의 `production_pending_manage.py`와 대조한다. 새 실행기는 `PIPELINE_SHARED_BRONZE_SESSION=1`을 주입한다. 명시적 0에서는 기존 단계별 자식 프로세스 방식으로 돌아가므로 새 coordinator만 복사하고 제어 모듈을 빠뜨리지 않는다.

재사용 대상은 Spark/JVM과 그 안의 Hadoop·저장소 클라이언트 환경이다. Kafka cluster/topic/offset 범위 확인은 각 Bronze 시작에서 계속 수행한다. Python Mongo 원천·lease 클라이언트는 각 실행에서 열고 닫으며 서로 다른 인증 역할의 연결을 합치지 않는다. 이 비용도 계측하지만 이번 구현을 모든 서비스 재연결 제거로 설명하지 않는다.

### 기존 테이블 준비 추가 개선

위 초기화 행은 1차 구현이다. 이후 기존 테이블의 `isDeltaTable`·DataFrame schema·`DESCRIBE DETAIL` 경로를 `bronze/metadata.py`의 최신 Delta snapshot 조회로 대체했다. 파일 수·용량 계산용 데이터 작업을 실행하지 않고 스키마·table ID·version·fingerprint·appendOnly를 확인한다. 각 writer invocation에서 `DeltaLog.update(false, None, None)`으로 최신 로그를 확인하므로 과거의 속성 검증을 무기한 캐시하지 않는다. 실제 설치된 Delta 4.4.1 JAR의 메서드 서명을 대조했다. 호환되지 않는 런타임이나 메타데이터 조회 실패는 `BRONZE_DELTA_METADATA_UNCONFIRMED`로 차단하며, 조회 실패를 새 테이블로 간주하지 않는다. version -1인 신규 대상만 기존 생성 규칙을 적용한다.

추가 개선 후 로컬 작업 파일 기준 전체 494개 / 118.021초, 서버 Bronze 관련 69개 / 1.480초가 통과했다. 2026-10-21 14:42:50 KST 기존 운영 epoch 6을 재개한 실제 시험에서는 100이벤트 Bronze가 181,445ms에 커밋됐고, 테이블 준비 1,696ms·snapshot 조회 합계 1,314ms였다. Delta 신규 append는 총 110행으로, 이전 중단 때 저장된 원천을 재사용한 배치다. 이를 순수 신규 100건이나 격리 신규 테이블의 48.4초와 직접 비교하지 않는다.

이 실제 시험에는 앞선 묶음 조회·append·Mongo 묶음 읽기·계측·Spark 공유 실행기가 모두 적용됐다. 한 번의 후속 window를 게시·활성화까지 완료한 뒤 정지하도록 기존 pause 요청을 유지한다. 시험 결과는 서버 `perf-20261020/existing-target-batch-result.json`, 배포·회귀 근거는 `existing-target-metadata-deployment.json`에 기록한다. 후속 단계의 완료 전에는 이 Bronze 커밋을 전체 업무 DB 반영 완료로 표시하지 않는다.

공유 Driver를 사용할 때 기존 signal 방식 `stop`은 거부한다. 작업 루트의 `pause-after-window.json`으로 게시·활성화 완료 경계에서 멈춘다. 종료·writer 소유권이 불확실하면 기존 잠금 복구 절차를 사용하고 TTL 삭제·자동 인수·체크포인트 초기화로 우회하지 않는다. 독립 Bronze CLI는 자체 Spark를 종료하며, 공유 모드에서도 활성 query·job·stage가 남으면 lease를 성공 해제하지 않는다.

이번 변경 전 기존 coordinator와 Bronze worker는 기존 정상 SIGTERM 경로로 중지했다. 원천·체크포인트·부분 성공 Delta는 보존했고, Spark 활성 작업 0·Worker 3대 idle·Bronze claim 0을 조회했다. 중지된 6번째 epoch는 완료 처리하지 않았다. 그 시점 업무 DB는 상품 448 / ACTIVE 406 / PENDING_REVIEW 42다. SIGTERM 종료로 예전 `flow-progress.status`가 RUNNING으로 남을 수 있으므로 PID·Spark 앱·종료 결과·정지 증거와 함께 해석한다.

운영 재개는 이 개선 시험에 포함하지 않는다. 정지 요청을 해제한 뒤 기존 `run_server_pipeline.py flow`로 미완료 epoch를 이어갈 수 있다. 작업 파일·체크포인트·등록·성공 영수증을 삭제하지 않는다.

## 검증

로컬 전체 회귀 **492개 / 122.781초 / OK**, 배포 서버 관련 회귀 **68개 / 1.393초 / OK**. 로컬 작업 파일의 기존 게시 테스트 삭제 변경은 이번 개선에서 수정하지 않았으며, 492개는 새 Git 커밋 트리를 검증한 수치가 아니다. 테스트 수를 이전 커밋의 488개나 부분 시험과 합산하지 않는다.

`tests/test_bronze_optimization.py`는 조각 수와 무관한 사전 조회·저장 확인 횟수, 응답 유실 후 추가 쓰기 없는 재개, 배치 끝의 충돌에서 첫 쓰기 차단, 저장 확인 실패, `$in` 누락·poison·용량·cursor 닫기, 메타데이터 일괄 조회·기존 대상 무생성, 스레드별 계측, 공유 세션·실패 시 root/환경 복원을 검사한다. 기존 Bronze 잠금·offset·중단 복구 회귀도 통과했다.

실제 비교 실행기는 [verify_bronze_optimization.py](server_setup/verify_bronze_optimization.py)다. 기존 정상 실행 환경과 보존한 변경 전 `bronze/delta.py` 경로가 필요하다. 승인된 최대 20개 원천의 별도 검증 prefix에서만 비교하며 운영 Kafka에 발행하거나 운영 checkpoint·업무 DB를 변경하지 않는다. 이번에는 실제 원천 12개로 기존/개선 테이블을 각각 만들고 동일 원천·이벤트 내용, 재실행 시 Delta 버전 불변, 실제 append 후 응답 유실의 재사용, 동일 application ID를 대조한다.

비교는 기존 방식→개선 방식 순서의 동일 JVM 소량 시험이다. 워밍업 영향과 작은 신규 테이블이라는 한계가 있으며 전체 100관측 배치의 속도 향상률·장기 파일 수·65,498상품 완주 시간을 보장하지 않는다. 서버 시험 결과는 `perf-20261020/bronze-optimization-trial.json`, 배포·소스 hash·회귀·중지 증거는 같은 폴더의 `bronze-optimization-deployment.json`, `bronze-stop-20261021.json`에 보존한다.

### 실제 격리 결과

12건 비교는 **VERIFIED**다. 신규 대상 초기화·실제 원문 읽기·저장·확인을 포함한 최초 처리는 기존 MERGE **82,569ms → 개선 append 48,432ms**, 같은 입력 재실행은 **22,567ms → 4,674ms**였다. 양쪽 모두 12건 수용·격리 0이며 원천·이벤트 본문이 동일하고 재실행 시 Delta 버전이 증가하지 않았다. 실제 append 성공 직후 응답 유실을 주입한 뒤 재시도에서 추가 저장 없이 기존 행을 확인했다.

두 방식은 동일 application `app-20261021142430-0011`을 사용했다. Spark 시작 1회·재사용 1회, 최종 종료 423ms를 확인했다. 이것은 동일 Driver 재사용과 별도 Delta 저장 경로의 시험이며 **운영 Kafka→Silver→Gold 전체 공유 세션 연속 실행을 인수한 결과는 아니다.** 업무 파이프라인은 중지 상태로 유지한다. [공개 결과](verification/bronze_optimization_20261021.json)를 보존한다.

[배포·테스트·최종 정지 조회](verification/bronze_optimization_deployment_20261021.json)에는 소스 SHA256, 서버 회귀 결과, 실제 MySQL 건수와 active application·writer claim 0개를 기록했다. 이번 개선 변경은 아직 새 Git 커밋·푸시를 수행하지 않았다.
# 2026-10-22 추가 개선 — 배치 경계 배포

Worker 01에 누락된 Kafka 전용 keystore/truststore를 기존 Master의 파일 쌍과 동일하게 원자 배포했다. Master·Worker 3대에서 `pj3-spark` 읽기 권한을 확인했다. `run_server_pipeline.py`의 수동 시작·preflight는 이제 네 노드가 모두 확인되지 않으면 차단한다. NiFi의 기존 고정 SSH 실행 경로가 이 PC용 검사기를 매번 호출하는 것은 아니며, 두 경로를 구분한다.

Bronze의 저장 전 전체 충돌 검사는 유지한다. 저장 후 검증은 고정된 전후 Delta 버전 사이의 append 로그에서 새 데이터 파일을 확인한 뒤 그 파일의 행 수·중복·원문을 검증한다. 예상하지 못한 로그 작업이나 버전 범위는 기존 전체 조회로 돌아간다. 대상 테이블 ID·스키마·fingerprint·appendOnly 속성도 대조한다.

대상 선정에서 전체 과거 snapshot과 반복 anti-join하던 경로는 고정 버전 구간의 신규 append 파일로 한정한다. 로그 누락/읽기 오류는 건너뛰지 않고 실패한다. 스키마 변경·overwrite·dataChange=true 삭제·파티션 테이블·column mapping·64버전 초과 구간은 원래 경로를 사용한다. OPTIMIZE의 dataChange=false 파일 교체는 신규 관측으로 간주하지 않는다.

작은 파일 정리는 완료 배치의 정지 표식, Driver 종료, 모든 Worker 유휴, 기존 Mongo 단일 작성자 잠금 아래에서만 실행한다. 네이티브 Delta OPTIMIZE를 사용하고, 전후 행 수와 SHA-256 조각 합계 지문·테이블 ID·스키마·계약 속성을 대조한다. 이는 물리 저장 재배치이며 원천·상품·승인·UUID를 수정하지 않는다. **VACUUM이나 과거 파일 삭제를 하지 않아 기존 고정 버전 참조가 유지된다.** 따라서 객체 저장소의 전체 파일 수가 곧바로 줄어드는 것과 현재 snapshot의 활성 파일 수 감소는 다르다.

사용자의 후속 지시에 따라 이번 배포·정리 후에는 `pause-after-window.json`을 유지한다. NiFi 타이머가 있어도 자동 재개하지 않는다. 처리 대상 200건·메모리 묶음 50~100건은 이번 변경에 포함하지 않았으며 기존 100건 계약을 유지한다. 제어 도구는 [배치 경계 유지보수](server_setup/pipeline_maintenance_watch.py), [정리와 내용 검증](server_setup/compact_pipeline_tables.py)다. 실제 완료 결과는 서버 `perf-20261020/range-maintenance-state.json`과 `range-compaction-20261022.json`으로 확인한다.
