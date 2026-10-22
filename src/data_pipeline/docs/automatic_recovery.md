# 제한된 자동 장애 복구

2026-10-21. 사용자 승인에 따라 기존 만료 잠금 복구를 NiFi 운영 제어에 연결했다. 만료만으로 잠금을 가져오거나 실패 상태를 성공으로 바꾸지 않는다.

## 이번 장애와 확인 범위

epoch 9의 Gold에서 MongoDB 소유권 갱신이 약 8.5초 후 `_OperationCancelled`로 취소됐다. 20초 socket timeout을 넘긴 사례가 아니다. 기존 정책은 갱신 결과가 불명확하면 `BRONZE_WRITER_LEASE_LOST`·`RECOVERY_REQUIRED`로 정지한다. 이름과 달리 이 잠금은 Silver·Gold도 사용한다.

확인 당시 Spark Driver·실행 앱은 없고 Worker 세 대가 정상 유휴 상태였다. Mongo02의 SSH는 응답하지 않았지만 이것만으로 당시 오류 원인을 확정하지 않았다. Mongo01·03의 해당 구간 journal 조회에서는 Primary 전환 기록을 찾지 못했다. 이 조회가 Mongo 파일 로그·네트워크·Replica Set 전체 장애 이력을 모두 검증한 것은 아니다. 자동 복구는 일시 장애의 재개 수단이며 DB·네트워크 장애 자체를 해결했다는 의미가 아니다.

## 자동 복구 조건

다음 조건을 모두 확인한다.

1. 운영 root의 `auto-recovery-policy.json`이 명시적으로 활성화돼 있다.
2. 오류가 `BRONZE_WRITER_LEASE_LOST`, 갱신 단계의 `DATABASE_OPERATION_FAILED`이며 허용된 연결 예외다. 취소·재연결·네트워크/Primary 변경 계열만 포함한다. 소유권 불일치·입력 충돌·Kafka offset 유실은 자동 처리하지 않는다.
3. 해당 실행이 잠금 획득 직후 저장한 비공개 `lease-owners/<execution_id>.json`이 존재한다. 파일은 0600이며 token·대상 fingerprint·잠금 키·PID·Linux 시작 시각을 보존한다. 공개 오류 메시지에는 token을 넣지 않는다.
4. 원래 PID의 동일 프로세스가 종료됐고 Master의 Driver·Spark 앱·Worker 실행이 모두 종료됐다. Worker 누락·DEAD·확인 불가는 거절한다.
5. Mongo 서버 시각으로 모든 잠금이 만료됐고 현재 token·대상·키가 비공개 소유권 증거와 정확히 일치한다.
6. 기존 복구 트랜잭션 안에서 소유권과 만료를 다시 확인하고 정확히 일치하는 잠금만 해제한다. 준비·응답 유실·실제 해제 결과를 비공개 감사 파일로 보존한다. 새로운 소유자가 나타나면 자동 시작하지 않는다.
7. 기존 launch와 controller.lock으로 같은 실행을 재개한다. 원래 확정 입력·성공 영수증·원천/이벤트 ID·체크포인트는 유지하며 미완료 단계부터 진행한다.

NiFi 운영 타이머는 5분마다 이 제어를 호출한다. 한 업무 DB 완료 지점이 진전되지 않으면 최대 3회, 최소 대기 60초다. 장애마다 incident가 달라져도 예산을 초기화하지 않는다. 정지 요청은 우선하며 예산 소진·증거 부족·DB 불통·클러스터 사용 중에는 자동 진행하지 않는다. 개별 체크 실패를 강제로 무시하는 경로는 없다.

## 운영 파일

Master root는 `/var/lib/pj3/product-pipeline/spark/production-pending-20261019`다.

- `auto-recovery-policy.json`: `contract_version=1`, `enabled=true`, `max_attempts=3`, `cooldown_seconds=60`.
- `auto-recovery/state.json`: 최근 업무 DB 완료 지점·시도 수·다음 허용 시각.
- `auto-recovery/<incident>-*.json`: 계획·준비·불명확·완료 감사. token이 포함될 수 있으므로 외부 공유·Git 등록하지 않는다.
- 각 단계 상태의 `lease-owners/`: 기존 소유권 증거. 새 환경 이관 때 해당 상태와 함께 보호해 전달한다.
- `nifi-last-tick.json`: 일반 시작/중복 실행 확인 또는 복구 차단 코드. 상세 정책은 [NiFi 운영](nifi_production.md)을 따른다.

이번 기존 장애 실행은 증거 파일을 만들기 전 버전이었다. 사용자 승인 아래 전역 유휴·정확한 만료 잠금을 기존 수동 절차로 재확인하고 Gold 잠금 2개를 해제했다. 이 과거 실행에 사후 소유권 증거를 만들어 자동 복구 성공으로 표시하지 않았다. 20:23:53 KST 새 Driver로 Gold부터 연속 처리 재개 후 각 단계가 증거를 생성한다.

## 검증

- 대역: 살아 있는 PID, 다른 token·대상·부분 소유자, 사용 중 클러스터, 응답 유실 후 재확인, cooldown·3회 제한, 기존 cursor 보존을 확인했다.
- 실제 서버: 고유 verification 경로에만 시험 잠금 4개를 만들었다. 별도 소유 프로세스를 종료한 뒤 인위적으로 만료한 시험 잠금을 실제 MongoDB 트랜잭션으로 자동 해제했다. 실제 Spark 유휴 확인과 운영 잠금 불변을 대조했다. 합성 오류이며 실제 운영 장애 재현으로 표시하지 않는다.
- 실제 운영: 기존 epoch 9 장애의 감사 복구와 성공 단계 재사용을 확인했다. 새 운영 장애가 발생하지 않은 상태에서 운영 자동 재기동을 실제로 시험했다고 주장하지 않는다.

## 연결 코드

[판단·복구](../product_pipeline/operations/automatic_recovery.py), [공통 시작 제어](../product_pipeline/operations/server_flow.py), [잠금 소유권 증거](../product_pipeline/bronze/lease.py), [기존 트랜잭션 복구](../product_pipeline/lease_recovery.py), [대역 시험](../tests/test_automatic_recovery.py).

### 실제 gateway 비정상 종료

2026-10-22 보완부터 CLI가 소유한 로컬 gateway 프로세스의 `poll()`에서 0이 아닌 종료 코드를 직접 확인하면 `SPARK_DRIVER_JVM_EXITED`와 `phase=jvm/reason=PROCESS_EXITED`를 기록한다. 이 오류는 일반 재시도를 하지 않고 복구 대기로 전달한다. 자동 해제는 기존 비공개 소유권·PID 종료·Spark 전체 유휴·서버 시각 만료·정확한 token 대조를 그대로 요구한다. Driver의 `ExitOnOutOfMemoryError`는 손상된 JVM에서 무한 대기를 피하기 위한 것이며 원문 저장·checkpoint 성공을 대신하지 않는다. 단순 Py4J 오류나 RPC 지연은 이 근거를 충족하지 않는다.

## epoch 10 majority 타임아웃 후속 보완 — 2026-10-21

20:58:29 KST 활성화 Gold의 갱신에서 `WTimeoutError`(서버 코드 64, 약 15초)가 발생했다. Spark 취소가 `Py4JJavaError`로 먼저 반환되어 조정기가 일반 재시도를 했고, 20:59:06 재시도는 이전 실행의 잠금에 막혔다. 해당 잠금 2개의 소유자는 최초 실패 실행이며 PID는 종료된 것으로 확인했다. 이는 다른 실행이 활동 중이라는 증거가 아니다. majority 응답 지연의 인프라 원인은 아직 확정하지 않았다.

- `checked_stage`는 갱신 실패 진단이 있으면 Spark의 표면 오류보다 잠금 상실을 우선 전달한다. 종료 미확인 오류는 계속 우선 차단한다.
- 자동 복구는 `WTimeoutError` 중 서버 코드 64인 갱신 실패만 추가 허용한다. acquire 충돌 자체를 자동 해제 사유로 사용하지 않는다. 기존 정확한 소유권·만료·PID·전체 Spark 유휴·트랜잭션·횟수 제한은 유지한다.
- 기존 epoch 10은 원본 실행 감사 기록과 충돌 대상 키를 대조하고, 조정기 잠금 아래 기존 실패 상태를 백업한 뒤 최초 원인을 복원했다. 성공·cursor를 조작하지 않았다. 이후 동일 자동 복구 경로가 잠금 2개를 해제하고 21:14:11 KST에 새 PID로 연속 처리를 재개했다. 이 기존 상태 보정은 명시적인 장애 복구 작업이며, 무인 최초 원인 추론을 구현한 것으로 표현하지 않는다.
- 로컬 관련 시험 12개, 서버 복구·소유권·단계 시험 48개 통과. 원천·업무 DB 적재·활성화 Silver 완료 결과를 보존하고 활성화 Gold부터 재개했다. 서버 증거는 `perf-20261020/e10-recovery-tests.json`, `e10-cause-restoration.json`, `nifi-last-tick.json`에 남긴다. 비공개 소유권/감사 파일에는 token이 포함될 수 있으므로 공유하지 않는다.
