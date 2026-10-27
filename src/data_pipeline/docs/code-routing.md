# 파이프라인 책임별 코드·문서 라우팅

로컬 `develop/src/data_pipeline`의 코드 변경을 위한 진입점이다. 운영 명령은 [문서 안내](README.md)의 실행·복구 가이드, 현재 서버 상태는 [실서버 검증](service_validation_20261018.md)을 따른다. 다른 작업 폴더나 협업용 문서 사본을 구현 기준으로 사용하지 않는다.

2026-10-22 계층 분리는 로컬 코드 변경이다. 아래 구조의 존재를 서버 배포·전체 상품 처리 완료로 해석하지 않는다. 계층 분리 자체는 기존 계약을 유지한다. 후속 [상품 증분 처리](incremental_processing.md)는 명시적인 Gold v2 평가 식별자와 검수 읽기 집합을 추가하며 v1 호환 경로를 보존한다.

## 읽는 순서와 기준

1. [develop 문서 안내](../../../docs/README.md) → [설계 라우팅](../../../docs/design/index.md)에서 변경 범위를 선택한다.
2. [통합 설계](integration_design.md), [데이터·복구 계약](data_contract_v1.md), [보강 계약](design_amendment_v1_1.md)에서 해당 단계의 불변 조건을 읽는다. 세부 보강 규칙은 보강 계약을 우선하며 상위 설계·실제 서비스 DDL의 소유권을 바꾸지 않는다.
3. 아래 표에서 담당 코드·추가 계약·회귀 테스트를 선택한다. 범위가 넓어지면 해당 행의 문서를 추가로 읽는다.
4. 코드 책임이나 호출 경계를 바꾸면 이 문서와 관련 실행 가이드를 함께 확인한다. 서버 상태 기록은 실제 서버 검증 후 갱신한다.

## 변경 목적별 라우팅

코드 링크는 `product_pipeline/`, 테스트 링크는 `tests/` 기준의 실제 파일로 연결한다.

| 변경 목적 | 먼저 읽을 문서 | 담당 코드 | 함께 확인할 문서·테스트 |
|---|---|---|---|
| 상품 변경·만료·야간 대조·증분 실행 | [증분 처리·인수](incremental_processing.md) | `incremental/`, `infrastructure/incremental_state.py`, `operations/service.py` | `test_incremental.py`, `test_incremental_integration.py`, `test_gold_incremental.py` |
| CLI 인자·JSON 출력·종료 코드 | 해당 단계 실행 가이드 | [입력 CLI](../product_pipeline/cli.py), [Bronze CLI](../product_pipeline/bronze/cli.py), [Silver CLI](../product_pipeline/silver/cli.py), [Gold CLI](../product_pipeline/gold/cli.py), [출력](../product_pipeline/console.py) | [입력 실행](execution.md), [CLI 회귀](../tests/test_cli.py), [계층 경계](../tests/test_layer_boundaries.py) |
| 환경변수·연결 생성·자원 종료 등록 | [배포](deployment.md) | [연결 구성](../product_pipeline/bootstrap.py), [저장소 인터페이스](../product_pipeline/ports.py) | [Garage 프로필](garage_v241_profile.md), [어댑터 회귀](../tests/test_adapters.py), [SDK 계약](../tests/test_sdk_contract.py) |
| 스냅샷·LIVE 인계 | [입력 실행](execution.md), [LIVE 계약](live_handoff_v1.md) | [스냅샷](../product_pipeline/snapshot.py), [캡처](../product_pipeline/live.py), [인계](../product_pipeline/handoff.py), [전송](../product_pipeline/transfer.py) | [LIVE 실행](live_execution.md), [스냅샷 회귀](../tests/test_snapshot.py), [LIVE 회귀](../tests/test_live.py), [전송 회귀](../tests/test_transfer.py) |
| 관측 적재·중복 재개·이미지/원천/이벤트 순서 | [데이터·복구 계약](data_contract_v1.md), [보강 계약](design_amendment_v1_1.md) | [배치 실행](../product_pipeline/runner.py), [단일 관측](../product_pipeline/ingestion/observation.py) | [적재 회귀](../tests/test_runner.py), [완료 관측 재사용](../tests/test_observation_reuse.py) |
| 재시도 분류·시도 기록·복구 증거 | [데이터·복구 계약](data_contract_v1.md), [자동 복구](automatic_recovery.md) | [재시도 정책](../product_pipeline/retry.py), [제한 시도](../product_pipeline/ingestion/attempts.py), [파일 감사 기록](../product_pipeline/infrastructure/ingestion_audit.py) | [적재 장애 회귀](../tests/test_runner.py), [자동 복구 회귀](../tests/test_automatic_recovery.py) |
| Bronze 실행·Kafka 범위·lease | [Bronze 실행](bronze_execution.md) | [실행 서비스](../product_pipeline/bronze/service.py), [스트림](../product_pipeline/bronze/stream.py), [lease](../product_pipeline/bronze/lease.py), [원천 저장](../product_pipeline/bronze/delta.py) | [Kafka 복구](kafka_retention_recovery.md), [lease 회귀](../tests/test_bronze_lease.py), [배치 경계](../tests/test_bronze_window.py) |
| Silver 실행·정규화·품질 판정 | [Silver 설계](silver_design_v1.md), [필드 대응](silver_gold_field_mapping.md) | [실행 서비스](../product_pipeline/silver/service.py), [정규화](../product_pipeline/silver/normalize.py), [평가 저장 순서](../product_pipeline/silver/runner.py), [일괄 평가](../product_pipeline/silver/batch.py) | [Silver 실행](silver_execution.md), [규칙 회귀](../tests/test_silver.py), [CLI 회귀](../tests/test_silver_cli.py), [완료 증거](../tests/test_silver_batch_proofs.py) |
| Gold 최신값·이력·준비 상태·export | [Gold 실행](gold_execution.md), [필드 대응](silver_gold_field_mapping.md) | [실행 서비스](../product_pipeline/gold/service.py), [정책](../product_pipeline/gold/build.py), [평가 저장 순서](../product_pipeline/gold/runner.py), [export](../product_pipeline/gold/export.py) | [Gold 회귀](../tests/test_gold.py), [선택 범위](../tests/test_gold_selection.py), [완료 증거](../tests/test_gold_batch_proofs.py) |
| Silver/Gold 공통 Delta 저장·checkpoint | [Bronze 개선](bronze_optimization.md), [후속 단계 개선](downstream_optimization.md) | [공통 저장](../product_pipeline/infrastructure/delta.py), [입력 보관·checkpoint](../product_pipeline/infrastructure/checkpoint.py), [Silver 스키마/추가 검사](../product_pipeline/silver/delta.py), [Gold 스키마](../product_pipeline/gold/storage.py) | [append 회귀](../tests/test_delta_append.py), [저장 증거](../tests/test_delta_proofs.py), [키 인덱스](../tests/test_delta_key_index.py), [입력 묶음](../tests/test_downstream_optimization.py) |
| Gold→게시 계획 생성 | [MySQL 게시](mysql_publication.md), [게시 필드 계약](../../../docs/design/13-gold-mysql-publication.md) | [계획 준비](../product_pipeline/publication/preparation.py), [계획 계약](../product_pipeline/publication/plan.py) | [계획 준비 회귀](../tests/test_gold_preparation.py), [게시 회귀](../tests/test_publication.py) |
| 실서버 배치 계측·성능 병목 확인 | [실서버 반복 계측](performance_measurement.md), [Bronze 개선](bronze_optimization.md), [Silver·Gold 개선](downstream_optimization.md) | [Bronze 지표](../product_pipeline/bronze/metrics.py), [Silver 처리](../product_pipeline/silver/batch.py), [Gold 처리](../product_pipeline/gold/batch.py), [공통 Delta 지표](../product_pipeline/infrastructure/delta.py) | [세부 계측 회귀](../tests/test_downstream_metrics.py), [실행 단계](flow_execution.md), [현재 실서버 근거](service_validation_20261018.md) |
| 게시 최신값·NULL 보호·속성/출처 유지 | [게시 필드 계약](../../../docs/design/13-gold-mysql-publication.md), [보강 계약](design_amendment_v1_1.md) | [순수 게시 정책](../product_pipeline/publication/policy.py), [경계 값 변환](../product_pipeline/publication/values.py) | [게시 회귀](../tests/test_publication.py), [속성 재평가](../tests/test_attribute_replay.py) |
| 게시 트랜잭션·상품/이미지/판매 저장 | [MySQL 게시](mysql_publication.md) | [게시 유스케이스](../product_pipeline/publication/runner.py), [게시 저장 처리](../product_pipeline/publication/repository.py), [MySQL 어댑터](../product_pipeline/publication/mysql.py), [대기 적재](../product_pipeline/publication/pending.py) | [스키마 반영](mysql_schema_rollout.md), [대기 적재](pending_intake.md), [게시 회귀](../tests/test_publication.py), [대기 회귀](../tests/test_publication_pending.py) |
| 게시 결과·과거 출처의 읽기 전용 감사 | [MySQL 게시](mysql_publication.md), [전체 FLOW](flow_execution.md) | [게시 감사](../product_pipeline/publication/audit.py), [단계 간 대조](../product_pipeline/operations/flow_audit.py) | [게시 회귀](../tests/test_publication.py), [FLOW 회귀](../tests/test_flow.py), [이전 출처 선택](../tests/test_gold_selection.py) |
| 검수 원장·승인·활성화 | [검수 절차](review_workflow.md) | [검수](../product_pipeline/review.py), [순수 검수 정책](../product_pipeline/review_policy.py), [검수 SQL](../product_pipeline/review_repository.py), [승인 후 게시](../product_pipeline/publication/reviewed.py) | [검수 회귀](../tests/test_review.py), [불변 감사](../tests/test_review_append_only.py), [검수 FLOW](../tests/test_reviewed_flow.py) |
| FLOW 순서·프로세스 실행·재개 | [전체 FLOW](flow_execution.md), [운영 실행](operations_execution.md) | [단계 상태·영수증](../product_pipeline/operations/flow.py), [단계 연결·프로세스](../product_pipeline/operations/flow_runtime.py), [worker](../product_pipeline/operations/flow_worker.py), [실행 결과](../product_pipeline/execution.py) | [FLOW 회귀](../tests/test_flow.py), [서버 제어](../tests/test_server_flow.py), [계층 경계](../tests/test_layer_boundaries.py) |

## 계층과 호출 방향

| 책임 | 현재 위치 | 의존 규칙 |
|---|---|---|
| 명령 인터페이스 | `cli.py`, 각 단계 `cli.py`, `console.py` | 인자·결과 표현을 맡고 실행 서비스를 호출한다. 내부 라이브러리는 CLI를 가져오지 않는다. |
| 실행 유스케이스 | 각 단계 `service.py`, `runner.py`, `ingestion/observation.py`, `operations/flow*.py`, `publication/preparation.py` | 실행 순서·자원 수명·트랜잭션·완료 증거를 관리한다. 출력 재해석 대신 `Request`와 `StageResult`를 사용한다. |
| 업무 규칙 | `silver/normalize.py`, `gold/build.py`, `publication/policy.py`, `retry.py` | 입력값으로 판단하며 CLI·네트워크 연결·DB 트랜잭션을 만들지 않는다. |
| 저장소 인터페이스 | `ports.py` | 기존 파일 대역과 실제 어댑터가 같은 호출 계약을 구현한다. 실행 시 상속을 강제하지 않는 `Protocol`이다. |
| 저장·외부 연결 구현 | `infrastructure/`, `mongo.py`, `objects.py`, `publisher.py`, `publication/mysql.py`, `publication/repository.py` | 파일·SDK·SQL·Delta의 구체 처리를 소유한다. 공통 Delta 기반은 Silver/Gold 패키지에 의존하지 않는다. |
| 연결 구성 | `bootstrap.py` | 환경변수 확인 후 실제 어댑터를 생성하고 자원 종료를 등록한다. CLI와 인계/운영 실행이 함께 사용한다. |

Bronze·Silver·Gold는 데이터 처리 단계이며, 위 계층은 코드의 책임이다. 단계별 패키지는 유지한다. 공통 코드를 옮기기 위해 서로 다른 단계의 품질 규칙·해시·영수증을 하나의 규칙으로 합치지 않는다. Silver 이미지 revision 검사는 Silver에 남기고 공통 저장 기반에는 넣지 않는다.

FLOW worker는 각 단계의 `service.run(Request(...))`를 호출한다. CLI는 같은 서비스를 호출한 뒤 결과를 JSON으로 출력한다. `StageResult.verified()`는 실패 코드와 worker 오류 분류를 유지하며, 자원 종료 미확인 오류가 완료로 바뀌지 않게 한다. 기존 worker 프로세스 경계·RUNNING 보존·정지 증거 요구는 유지한다.

## 호환성과 변경 경계

- 기존 `run_*.py`와 명령 인자·종료 코드를 유지한다. 과거 도구가 가져오는 `bronze.cli.await_window`, `publication.cli.prepare_from_source`, `publication.runner`의 정책/감사 함수는 호환용 export를 유지한다. 신규 내부 코드는 실제 담당 모듈을 가져온다.
- 게시의 staging 트랜잭션과 서비스 반영 트랜잭션은 구분한다. guard 잠금, 기존 UUID, 검수 소유권, 최신값·NULL 보호, 게시 완료 후 재사용 판정을 유지한다.
- 공통 Delta 기반은 각 단계의 실제 settings와 lease, 명시적인 저장 루트를 받는다. Gold가 Silver 설정인 것처럼 만든 proxy를 사용하지 않는다. 기존 marker·단계 이름·오류 코드·출력 해시·경로는 그대로 유지한다.
- `docs/server_setup/`의 도구는 [배포](deployment.md)와 [운영 인계](operator_handoff.md)의 지정 절차로 선택한다. 운영 배포·복구·과거 시험 재현 도구를 디렉터리 단위로 일괄 실행하지 않는다. 기존 서버 경로를 보존하기 위해 이번 계층 분리에서 이 도구들을 이동하지 않는다.
- 이 변경은 상품 재적재·임베딩 재생성·스키마 변경·서버 자동 재개를 요구하지 않는다. 서버 반영은 검토한 커밋과 배포 절차에 따라 별도로 검증한다.

## 검증과 문서 갱신

파이프라인 디렉터리에서 역할별 잠금 의존성이 준비된 Python으로 실행한다. 테스트들이 `helpers`, `test_silver` 등 같은 디렉터리의 대역을 가져오므로 다음 discovery 진입점을 사용한다.

```powershell
python -B -m unittest discover -s tests
```

전체 회귀와 함께 `tests/test_layer_boundaries.py`가 CLI 역방향 import, 공통 저장소의 단계 의존, 순수 게시 정책의 DB 의존, 직접 실행 결과·FLOW 호출을 검사한다. 실제 Spark/Delta·MySQL 검증은 해당 `smoke_*.py`와 실행 가이드의 지정 시험 자원에서 별도로 수행한다. 로컬 대역 회귀 통과를 운영 서버 배포·실제 종단 검증 완료로 기록하지 않는다.

책임/파일 이동은 이 표와 진입점, 계약 변경은 해당 계약과 실행 가이드, 로컬 변경 검증은 [구현 이력](implementation_history.md), 실제 서버 검증은 [실서버 검증](service_validation_20261018.md)에 기록한다. 완료 처리 순서·재시도·lease·고정 버전 대조를 바꾸면 장애/재개 테스트를 함께 확인한다.
