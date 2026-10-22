# 상품 변경분 처리와 서버 적용 기준

2026-10-22 로컬 구현이다. 서버 배포·실제 Spark/DB 종단 검증·처리 시간 단축은 아직 확인하지 않았다. 현재 운영 사실은 [실서버 검증](service_validation_20261018.md)을 따른다. 상위 [파이프라인 설계](../../../docs/design/02-data-pipeline.md)와 [게시 계약](../../../docs/design/13-gold-mysql-publication.md)을 우선한다.

## 처리 정책과 계층

- 최초에는 전체 원천 메타데이터를 대조하고, 이후 변경·만료 대상과 날짜가 바뀐 뒤 첫 안전 경계의 전체 메타데이터 대조를 수행한다. 65,498건 모두를 매번 Silver/Gold 재처리하지 않는다.
- 정상 재처리 중에는 기존 정상 게시본을 유지한다. 담당자의 명시적 거절·철회와 만료는 별도 reviewer 경로에서 서비스 사용을 제한한다. ETL publisher가 승인 원장을 소유하지 않는다.
- 새 상품·이미지·분류 변경은 필요한 Silver부터, 상품 승인·게시 정책 등 Gold 입력만 바뀌면 검증된 Silver를 재사용한다. 임베딩 생성과 검색 인덱스 구축은 이 작업에 포함하지 않는다.

| 계층 | 변경 위치 | 책임 |
|---|---|---|
| 순수 정책 | `incremental/policy.py`, `gold/identity.py`, `review_policy.py` | 상품 입력 해시·재시작 단계·묶음 크기·Gold 평가 식별자·검수 값 검증 |
| 유스케이스 | `incremental/service.py`, `review.py`, `publication/reviewed.py`, `publication/reuse.py` | 발견→영속 큐→범위 고정→실행→감사→정확한 세대 완료, 승인·게시 재사용 |
| 저장·외부 연결 | `infrastructure/incremental_state.py`, `incremental/production.py`, `review_repository.py`, `mongo.py` | 세대·역방향 의존성·만료 색인, Mongo/SQL/실제 운영 단계 연결 |
| 연결 구성·운영 | `bootstrap.py`, `operations/service.py`, `incremental/runtime.py`, `operations/server_flow.py` | 보호 설정·실제 어댑터·기존 Master coordinator 안전 경계·완료 후 유지 처리 |

## 변경 발견과 완료의 의미

원천의 `incremental_change` 표시는 기존 관측/이미지 revision 변경과 함께 기록한다. 발견 시점의 revision·fingerprint만 조건부 확인 처리하므로 새 변경을 이전 작업이 지울 수 없다. 변경 표시는 **완료**가 아니라 **영속 작업 인계** 뒤 해제된다. 성공은 별도의 단계 영수증과 게시 감사로 확인한다.

상품별 generation과 불변 작업 파일을 먼저 저장하고, 기존 운영 큐에 `REEVALUATE`를 등록한 다음 원천 표시를 해제한다. 등록 실패 시 해제하지 않는다. 실행 중 새 입력이 생기면 이전 세대가 새 세대를 완료 처리할 수 없으며, 게시 직전 입력을 재검사한다. A→B→A 복귀도 대기 중인 B 세대를 무효화한다. 성공한 단계 파일은 재시작 때 재검증해 사용한다.

Bronze에 아직 없는 원천은 deferred로 남기며 완료·확인 처리하지 않는다. 기존 수집→Kafka→Bronze 경로와 checkpoint를 유지한다. 새 checkpoint로 처음부터 재전송하지 않는다. 묶음은 최대 100상품이면서 100관측이다. 한 상품이 100관측을 초과하면 명시적으로 실패하며 일부 관측을 버리지 않는다.

검수 원장은 최신 subject별 메타데이터를 discovery마다 읽어 변화를 비교한다. UUID나 작성 시각을 단순 증가 cursor로 사용하지 않는다. 상품·이미지·매핑(미존재 포함)·분류·정책·공유 자산 의존성을 기록한다. 이 원장 조회 비용은 남아 있으며 실서버 계측 대상이다.

야간 대조는 원천 메타데이터·게시 참조와 보존된 로컬 산출물 해시를 확인하고 파생 조회 색인을 복구한다. 모든 이미지 바이너리·Delta 물리 파일을 매일 전수 검사한다는 뜻은 아니다. 실제 처리 대상은 고정 Delta 영수증과 MySQL 게시 결과를 대조한다. `QUALITY_WAIT`도 정상 평가 완료로 기록하고 의존성 변경·만료·손상 대조가 있을 때 다시 평가한다.

## Gold·검수·게시의 호환성

`PRODUCT_INPUTS_V2`는 상품에 영향을 주는 Silver 영수증·검수 읽기 집합·정책·시간상 유효 상태로 평가를 식별한다. 다른 상품의 Delta 버전 증가나 날짜 변경만으로 새 평가를 만들지 않는다. 원래 물리 버전과 입력 묶음은 영수증에 보존한다. 재사용할 때 현재 선택 범위와 원래 보관 입력의 binding을 모두 검증한다. v1은 기존 식별 규칙을 유지하며 v1→v2 첫 평가는 정상 증분 비용과 구분해 측정한다.

v2의 상품별 `quality_daily` 기여 행은 날짜를 비워 매일 같은 결과를 복제하지 않는다. 최종 일별 export 집계는 기준일을 유지한다. 상위 설계의 `gold_catalog_quality_daily` 일별·품목별 집계를 폐기하지 않는다. 기존 Gold 실행 문서의 날짜·전체 버전 기반 키 설명은 v1 기준이다.

신규 guard는 관련 검수 읽기 집합만 대조한다. 관련 없는 승인 변경은 다른 상품을 무효화하지 않는다. 과거 guard는 안전을 위해 기존 전역 검수 집합 대조를 유지한다. reviewer의 named lock과 트랜잭션으로 승인·guard 생성의 부재 행 경합을 직렬화한다. 기존 UUID·NULL 보호·staging과 서비스 게시의 트랜잭션 구분을 보존한다.

동일 v2 평가가 이미 ACTIVE로 게시됐다면 원래 게시 계획을 복원해 전체 게시 감사를 통과한 뒤 `REUSED`로 반환한다. 이 경우 MySQL에 재게시하지 않는다. guard 변경·만료는 재평가 대상이며, 데이터 손상은 재사용으로 숨기지 않는다.

## 실제 서버 연결과 적용 순서

[인프라 설계](../../../docs/design/01-infrastructure.md)의 역할을 유지한다. NiFi의 제한 SSH tick이 Spark Master의 단일 coordinator를 호출하고, Master는 실제 Spark Workers·Garage Delta·MongoDB 3노드 replica set·MySQL 현재 Primary와 통신한다. 상태·큐·검수 보호 파일은 Master의 영속 디스크에 둔다. 각 서버에 독립 큐나 Delta writer를 만들지 않는다.

기존 배치의 안전 경계에서 증분 tick을 호출한다. 성공한 tick 사이는 최소 300초이며 현재 배치가 길면 발견도 늦어진다. 5분 반영 SLA를 보장하지 않는다. 전체 최초 처리가 완료되면 같은 coordinator의 maintenance 경로로 진입하며 원천 생산자를 재시작하지 않는다. pause·manual-stop·미확인 RUNNING 보존·명시적 복구 증거 요구를 유지한다.

1. 변경 검토 및 사용자 커밋 승인 후 기존 배치를 안전하게 중지하고 상태·보호 설정·코드 해시를 보존한다. 운영 정지 표식을 임의로 지우지 않는다.
2. [배포 가이드](deployment.md)에 따라 `product_pipeline/` 전체와 동반 모듈을 함께 배포한다. `production_pending_work.py`→`production_work.py`, `production_continuous_work.py`→`continuous_work.py`, `production_activation.py`→동일 파일명 대응을 유지한다. `production_incremental_prepare.py`도 별도 관리 도구로 배치한다.
3. 기존 Spark/Delta/Java·Python 잠금 의존성, Master/Workers TLS, Garage·Mongo·MySQL 권한을 확인한다. Mongo는 3노드 replica set과 Primary/majority+j 조건을 검사한다. publisher와 reviewer의 별도 보호 설정을 사용한다.
4. Master에서 `production_incremental_prepare.py --production-root <실제 작업 루트> --mongo-database <실제 DB>`로 기본 조회만 실행한다. `pipeline_incremental_pending_v1` sparse 색인이 없으면 유지보수 변경으로 `--apply`를 명시한다. 잘못된 기존 색인은 자동 교체하지 않는다. 런타임은 색인이 없으면 거부하며 DDL을 수행하지 않는다.
5. [설정 예시](../config/incremental.operations.example.json)를 실제 작업 루트의 `incremental-operations.json`으로 준비한다. 대상·DB·replica set·상태/큐 경로와 **실제 배포된 `production_work.py` SHA-256**을 넣는다. 예시 placeholder는 실행 불가다. `max_tasks_per_tick=1`부터 시작한다.
6. 격리한 서버 시험 자원에서 아래 항목을 검증하고 제한된 실제 상품 범위를 대조한다. legacy→v2 전환과 최초 원천 대조 비용을 따로 측정한다. 중단 표식 해제와 연속 운영 재개는 검증 결과를 검토한 후 명시적으로 수행한다.

## 인수 기준

- 변경 없음: 초기 대조 후 같은 입력에서 신규 Silver/Gold 평가·MySQL 서비스 쓰기 0. 조회·감사·스케줄러 상태 기록은 발생할 수 있다.
- 단일 상품 변경: 해당 상품과 실제 공유 의존 대상만 처리. 관련 없는 상품의 guard와 게시본 유지. Gold-only 변경은 Silver 결과 재사용.
- 거절·철회·만료: 사용 제한과 다음 평가에 반영. 정상 재처리 실패 시 기존 정상 게시본 유지. 명시적 삭제는 원천 행 부재로 추정하지 말고 담당 원장의 철회/삭제 정책으로 전달.
- 장애: capture/큐/원천 확인/단계 저장/게시/완료 사이마다 중단·재시작해 손실·중복 서비스 반영·오래된 세대 완료가 없는지 대조. 미확인 RUNNING은 자동 탈취 금지.
- 전체 범위: 누락·오래된 결과만 보충하고, 고정 입력·기준 시각에서 전체 평가와 증분 결과의 상품/관계/상태가 일치. 산출물·색인 유실 시 복구 또는 명시적 오류.
- 성능: 같은 입력 크기에서 단계별 경과 시간, 실제 읽은 파일/행/바이트, Delta 커밋 수, MySQL 반영 행 수를 전후 비교. 로컬 테스트 통과만으로 처리량 개선을 확정하지 않는다.

로컬 회귀는 `python -B -m unittest discover -s tests`로 실행한다. 신규 회귀는 `test_incremental.py`, `test_gold_incremental.py`, `test_incremental_integration.py`이며 실제 서버 인수와 구분한다.

## 이번 변경의 커밋 검토 대상

아래 목록은 이번 로컬 변경 범위이며 서버 배포 완료 목록이 아니다.

- `config/incremental.operations.example.json`
- `docs/code-routing.md`
- `docs/deployment.md`
- `docs/gold_execution.md`
- `docs/implementation_history.md`
- `docs/incremental_processing.md`
- `docs/operations_execution.md`
- `docs/README.md`
- `docs/review_workflow.md`
- `docs/server_setup/production_activation.py`
- `docs/server_setup/production_continuous_work.py`
- `docs/server_setup/production_incremental_prepare.py`
- `docs/server_setup/production_pending_work.py`
- `product_pipeline/bootstrap.py`
- `product_pipeline/execution.py`
- `product_pipeline/gold/batch.py`
- `product_pipeline/gold/build.py`
- `product_pipeline/gold/contracts.py`
- `product_pipeline/gold/identity.py`
- `product_pipeline/gold/runner.py`
- `product_pipeline/gold/service.py`
- `product_pipeline/incremental/__init__.py`
- `product_pipeline/incremental/policy.py`
- `product_pipeline/incremental/production.py`
- `product_pipeline/incremental/runtime.py`
- `product_pipeline/incremental/service.py`
- `product_pipeline/infrastructure/incremental_state.py`
- `product_pipeline/mongo.py`
- `product_pipeline/operations/cli.py`
- `product_pipeline/operations/dispatch.py`
- `product_pipeline/operations/flow_runtime.py`
- `product_pipeline/operations/flow_worker.py`
- `product_pipeline/operations/queue.py`
- `product_pipeline/operations/server_flow.py`
- `product_pipeline/operations/service.py`
- `product_pipeline/ports.py`
- `product_pipeline/publication/mysql.py`
- `product_pipeline/publication/policy.py`
- `product_pipeline/publication/repository.py`
- `product_pipeline/publication/reuse.py`
- `product_pipeline/publication/reviewed.py`
- `product_pipeline/publication/source.py`
- `product_pipeline/review.py`
- `product_pipeline/review_policy.py`
- `product_pipeline/review_repository.py`
- `README.md`
- `tests/publication_fixture.py`
- `tests/test_gold_incremental.py`
- `tests/test_incremental.py`
- `tests/test_incremental_integration.py`
- `tests/test_layer_boundaries.py`
- `tests/test_offer_recovery.py`
- `tests/test_review.py`
