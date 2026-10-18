# 예약 실행·재시도·NiFi 연계

## 서로 다른 NiFi/Driver VM의 고정 tick
2026-10-18: `nifi` CLI는 기존 `--python` 대신 `--remote-ssh-config /etc/pj3-clients/pipeline-trigger/ssh.conf --remote-target pj3-pipeline-tick`을 받을 수 있다. 두 방식은 상호 배타적이다. SSH config는 서비스 전용 키·검증한 known_hosts를 고정하고 서버는 해당 키에 tick 강제 명령을 적용한다. 임의 원격 명령·포워딩을 허용하지 않는다. 로컬 config는 실제 원격 config/수동 복구 receipt와 일치해야 한다. config/launcher/queue를 바꿀 때 수동 검증을 다시 수행한다.

검증에서는 NiFi 2.12.0 timer→제한 SSH→Master 큐를 실제 확인하고 프로세서를 STOPPED로 남겼다. 실제 처리한 수동 복구 작업은 INGEST이고 타이머의 후속 tick은 빈 큐다. 전체 7단계 FLOW 예약 성공으로 보지 않는다. [정확한 경로·범위·결과](service_validation_20261018.md)를 따른다.

2026-10-18. 기존 capture/send/receive/ingest 및 새 MySQL 게시기를 명시적인 작업으로 연결한다. 크롤러 네트워크/파서를 변경하거나 현재 수집 프로세스를 시작/중단하지 않는다.

## 실행 책임

파일 기반 영속 queue를 **지정된 실행 서버 한 대**에서 소유한다. 기본 상태는 READY → RUNNING → SUCCEEDED, 일시 오류는 RETRY_WAIT/next_retry_at, 한도 소진은 RETRY_EXHAUSTED, 입력/승인/본문 충돌은 QUARANTINED다. 파일 교체는 fsync+rename이며 같은 서버의 worker.lock과 attempt token으로 중복·오래된 완료 응답을 막는다. RUNNING을 시간이 지났다는 이유만으로 자동 탈취하지 않는다.

이는 여러 서버가 각자 로컬 queue를 두는 분산 scheduler가 아니다. NiFi 클러스터 Primary가 바뀌는 환경에서는 queue 소유권·저장소 잠금/이관을 별도 인수해야 하며 그대로 활성화하지 않는다. 현재 프로젝트의 단일 NiFi/Python 실행 호스트를 대상으로 한다. 서버 이전은 정지 후 동일 queue를 보존해 옮기는 방식이다.

스케줄러는 상품별 DB/Kafka 상태를 별도 구현하지 않는다. INGEST는 기존 원장·이미지 상태·이벤트 ID를 재사용하고 PUBLISH는 기존 SQL receipt를 재사용한다. RECEIVE는 완료 ready manifest만 검증하고 INGEST를 등록한다. RECONCILE은 고정 스냅샷을 Mongo majority 원천·접수/발행 원장과 대조하여 필요한 경우 기존 ID를 유지하는 repair 작업을 등록한다.

## 설정과 수동 실행

```bash
python -m pip install -r requirements.lock.txt -r requirements-publication.lock.txt
python -m pip install --no-deps -e .
python run_operations.py tick --config /etc/product-pipeline/operations.local.json
```

`operations.example.json`에서 논리 target, queue/audit/ingest-state/receive 경로와 설정 파일 위치를 확정한다. live 모드에는 `single_owner_confirmed=true`가 필요하다. 예시 target은 실행을 차단한다. 비밀은 환경변수/별도 보호 파일로 전달하며 queue spec에 넣지 않는다. Windows `.venv`는 VM에 복사하지 않는다.

작업은 명령 문자열이 아닌 허용된 kind와 불변 참조로 등록한다.

```bash
python run_operations.py enqueue --config /etc/product-pipeline/operations.local.json \
  --kind INGEST --spec /srv/pipeline/tasks/first-ingest.json --max-attempts 5
python run_operations.py tick --config /etc/product-pipeline/operations.local.json
```

INGEST spec 필드: `snapshot`, `snapshot_id`, `state`, `config`, `config_sha256`, `target_fingerprint`. 마지막 fingerprint는 Settings의 논리 환경/DB/bucket/토픽 계약이다. rehearsal에서는 외부 설정 없이 FileLedger/FileObjects/FilePublisher만 사용한다. PUBLISH는 `plan`, `plan_sha256`, `plan_id`, `config`, `config_sha256`이다. 계획이나 설정 내용이 변경되면 작업을 조용히 재사용하지 않는다.

## 발견과 주기 대조

- `discover_remote=true`: 전용 handoff bucket의 `handoff/ready/*.ready.json`만 발견한다. 페이지의 모든 task를 영속화한 뒤 continuation token을 저장한다. 전송 도중 chunk나 임시 파일은 적재하지 않는다. 전체 목록을 다시 훑어도 task ID로 중복 실행을 막는다.
- `publication_ready_directories`: ready.json에 등록된 hash와 개별 게시 계획을 검증하고 PUBLISH를 등록한다. 단순 파일 생성 알림만으로 게시하지 않는다.
- `reconciliation_snapshots`: 각 고정 snapshot별 UTC 일자에 RECONCILE을 등록한다. raw 유실/접수 누락/미발행은 repair, 원문 충돌은 격리다. snapshot을 훑지 않고 최신 DB 건수만 비교하는 작업이 아니다.
- 소스 전체 범위는 운영자가 확정한 스냅샷 목록이다. 이 목록에 없는 원천을 대조했다고 주장하지 않는다. Bronze/Gold 전체 대조와 모니터링 알림은 별도 인수하며 Mongo와 비교했다고 Kafka/Delta의 실제 보존을 대신하지 않는다.

큐의 delay는 지수 대기이고 HTTP 429/503 등 Retry-After가 있으면 그 최소 대기시간을 지킨다. 한 tick은 `max_tasks_per_tick`만 처리한다. 일시 실패가 남으면 다음 tick에서 기한 도래 작업을 처리한다. QUARANTINED/RETRY_EXHAUSTED는 운영 확인 대상으로 종료 코드 2를 남긴다. 원천 1건의 오류로 성공한 상품을 새 ID로 다시 만들지 않는다.

## 강제 종료·한도 소진 복구

```bash
# 이전 worker/하위 프로세스가 종료·격리된 증거를 먼저 확인한다.
python run_operations.py recover --config /etc/product-pipeline/operations.local.json \
  --task-id TASK_ID --expected-token ATTEMPT_TOKEN --stop-evidence operator://incident/worker-stopped

python run_operations.py retry --config /etc/product-pipeline/operations.local.json \
  --task-id TASK_ID --additional-attempts 1 --evidence operator://incident/service-restored
```

recover는 RUNNING의 정확한 token과 작업자 정지 근거가 필요하다. 실행 중 worker.lock을 획득할 수 없으면 거절한다. retry는 RETRY_EXHAUSTED만 제한적으로 재개한다. 입력 충돌을 override하지 않는다. 수정된 승인/입력은 새 계획으로 처리한다. attempt 번호와 과거 시도 파일은 지우지 않는다.

## NiFi

`operations/nifi.py`는 REST로 ExecuteProcess 프로세서를 계획/생성하고 명시적으로 시작/정지한다. HTTP·인증서 검증 해제를 허용하지 않는다. 설치 버전은 제공되지 않았으므로 배포 때 `--version`을 필수로 받고 서버 버전·해당 processor bundle을 대조한다. 공식 근거: [ExecuteProcess](https://nifi.apache.org/components/org.apache.nifi.processors.standard.ExecuteProcess/), [NiFi REST](https://nifi.apache.org/docs/nifi-docs/rest-api/index.html).

```bash
# 먼저 같은 실제 설정으로 수동 tick 및 실패→동일 task 재시도 성공을 확인한다.
# audit_root/manual-smoke.json에 config hash와 recovery_verified가 기록된다.
python run_operations.py nifi \
  --url https://nifi.internal:8443/nifi-api --group PROCESS_GROUP_UUID --version CONFIRMED_NIFI_VERSION \
  --python /opt/product-pipeline/.venv/bin/python --working-directory /opt/mlo-01-p3-team3/src/data_pipeline \
  --config /etc/product-pipeline/operations.local.json

# 동일 명령에 --apply를 붙이면 STOPPED 상태로 생성한다.
# 반환된 processor UUID로 --apply --processor-id UUID --start를 명시해 활성화한다.
# 정지는 --apply --processor-id UUID --stop이다.
```

기본 30초, 동시 실행 1, PRIMARY 실행 노드다. 명령은 shell이 아닌 Python 모듈이며 queue의 불변 spec에서 실행파일/쉘을 받지 않는다. 다른 설정의 기존 동명 프로세서를 덮어쓰지 않는다. 실제 restricted ExecuteProcess 권한과 Python 실행 계정·환경변수·인증서 접근을 담당이 준비한다. NiFi stdout FlowFile은 success를 자동 종료하며 영속 작업 결과는 Python audit/attempts에 있다. 외부 경보 연동·NiFi 실제 import/기동은 서버 인수 대상이다.

## 시험 근거

`test_operations.py`는 실제 파일로 기한 대기, Retry-After, 중단→수동 복구, token 충돌, 한도 소진→제한 재시도, 중복 실행, 실제 sealed snapshot→file rehearsal 연결, 이관 hash/ID 보존을 검증한다. NiFi는 REST 대역으로 계획/버전/구성 충돌을 검증했다. 실제 NiFi 서버를 설치·변경했다고 표시하지 않는다. SQL 테스트는 별도 실제 로컬 MySQL 인스턴스이며 VM 적재가 아니다.

## 전체 FLOW와 용량 정책 추가

기존 네 작업에 `FLOW`를 추가했다. `flow-prepare`로 입력/hash·담당 계약·단계 대상 일치를 봉인하고 `flow_directories`의 준비된 경로를 tick이 등록한다. 각 단계는 자식 Driver/기존 CLI와 별도 checkpoint를 유지한다. 순서·실패 단계 재개·품질 보류·`flow-audit`·정지 불명확 복구는 [전체 단계 실행](flow_execution.md)을 따른다. INGEST만 연결한 것을 전체 Spark 자동화로 취급하던 이전 지원 경계는 이 후속 구현으로 대체됐다. 실제 서버 전체 worker CLI/NiFi 실행은 미인수다.

선택적 `storage_policy`는 root/max_bytes/min_free_bytes/log_retention_days로 tick 진입 시 용량 backpressure와 오래된 진행 로그 경보를 제공한다. worker 진행 로그는 시도당 허용 JSON만 최대 1MiB이며 원천/증거 자동 삭제는 없다. 중앙 로그 전송·운영 경보 채널은 배포 담당이 인수한다.
