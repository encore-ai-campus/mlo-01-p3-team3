# 책임과 처리 계약

기준은 사용자 지정 `pipeline-redesign-review/파이프라인_개선_인계.md`와 동반 병목 검토다. 인계 문서에 남은 담당 승인·활성화 항목은 후속 사용자 결정에 따라 새 실행기에서 제거했다. 기존 구현은 이력·비교 대상으로 보존한다.

## 계층별 변경 위치

| 책임 | 위치 | 지켜야 할 경계 |
|---|---|---|
| 명령·표현 | `interfaces/cli.py` | 인자와 JSON 출력만 담당한다. 내부 계층은 CLI를 import하지 않는다. |
| 연결 구성 | `bootstrap.py` | 설정 검증, 경로 분리, 환경변수, 자원 수명을 조립한다. |
| 범위 고정 | `application/capture.py` | 관측 참조·정책을 불변 scope로 봉인한 후 상품의 현재 작업을 지정한다. |
| 정제 실행 | `application/processing.py` | 원천 페이지, 테이블별 묶음 저장, 영수증 마지막 확정, Silver/Gold 재사용을 조정한다. |
| 진행·인계·복구 | `application/coordinator.py` | 단일 writer, 게시 재시도, 용량 제한, 진행 커서, 불명확한 RUNNING 복구를 담당한다. |
| 단계별 계측 | `adapters/timing.py` | 단조 시계로 시작·종료·경과 ms와 안전한 행/SQL 카운터를 기록한다. |
| 업무 규칙 | `domain/transform.py` | 네트워크 없이 원천 사실을 정제하고 상품 이력을 현재 값으로 줄인다. |
| 해시·상한 계약 | `domain/contracts.py` | canonical JSON, 콘텐츠 해시, 관측 페이지와 이력 한도를 정의한다. |
| 외부 포트 | `ports.py` | Source/Lake/Publisher의 최소 호출 계약이다. |
| 구체 저장 | `adapters/` | 파일 원자 저장, State, Mongo, Delta, MySQL과 읽기 감사를 구현한다. |

## 실행과 저장 단위

1. `capture`가 수집 run에 등장한 상품을 찾고 그 상품의 필요한 전체 원천 이력 메타데이터를 고정한다. 전체 과거 본문을 driver로 가져오는 방식이 아니다. 관측 참조는 최대 100개 단위, 상품 인덱스는 128개 단위로 저장한다. 실제 본문 읽기는 선택한 관측 ID에 한정한다.
2. 작업 ID는 상품·원천 문서 해시·이미지 revision/fingerprint·관련 분류/규칙·Gold 정책·변환 버전으로 결정한다. 변환 의미를 바꾸면 `domain.transform.VERSION`도 올린다. 무관한 품목 매핑 변경은 해당 상품 작업을 바꾸지 않는다.
3. 한 묶음의 Bronze/Silver/Gold를 테이블별로 저장한다. `record_hash`가 같은 안정 키의 결과를 재사용하고 다른 내용으로 덮어쓰지 않는다. Silver 입력 receipt는 문서·이미지 상태·해당 정제 정책으로 결정한다. Gold 전용 정책 변경은 Silver 파싱을 재사용한다.
4. 101관측인 한 상품도 100+1 페이지로 정제하고, 부모 상품의 전체 이력으로 Gold를 한 번 만든다. 설정된 이력 행/바이트 한도를 넘으면 `BLOCKED_HISTORY_LIMIT`이다. 임의로 앞 100개만 게시하거나 품질 보류 완료로 바꾸지 않는다. 다른 부모 상품은 계속 처리할 수 있다.
5. Gold 필수 출력의 키·해시를 확인한 뒤에만 Gold receipt를 쓴다. 이어서 불변 delivery manifest와 active 인덱스를 저장한 뒤 진행 커서를 전진한다. 인계 직전 장애는 기존 Gold를 대조해 재사용한다.
6. 게시가 다음 tick에서 active 작업을 처리한다. 원천 revision과 현재 상품 작업이 바뀌면 이전 인계를 `SUPERSEDED`로 남기고 게시하지 않는다. 새 입력은 새 scope로 잡아야 하며 이전 scope를 완료로 위장하지 않는다.
7. MySQL은 불변 staging과 서비스 반영을 별도 트랜잭션으로 처리한다. 서비스 결과와 완료 증거는 같은 트랜잭션이다. 응답이 유실되면 같은 plan ID의 저장 결과를 확인한다. 스키마 사전 검사는 필요한 10테이블을 총 6조회로 확인하고 세션당 최대 100상품/60초 범위에서 재사용한다. 검수 guard·임베딩·검색 활성화 테이블은 이 검사의 필수 입력이 아니다.

게시 재시도 대기 중에도 다음 정제 묶음을 허용하되, 기본 대기 high=3/low=1, 보존 바이트·여유 디스크 상한을 적용한다. 대기 큐가 무한히 커지지 않는다. MySQL 네트워크/선별된 일시 오류는 최대 3회 재시도하고, 증거 변조·영구 오류는 차단한다. 무결성 오류는 계속 쓰지 않고 실패를 드러낸다.

## 필드와 게시 계약

이름·품목이 있으면 기술적으로 카탈로그 게시 가능하다. 선택 설정으로 이미지/비교 링크를 필수화할 수 있다. 필수 부족은 `QUALITY_WAIT`, 이미 게시된 상품이 있으면 `LAST_GOOD_RETAINED`로 보존한다. SQL 상태값 `ACTIVE`는 직접 게시된 현재 행이라는 저장 표현이며 사용자 활성화 절차가 아니다.

관측별 규격·제조사·출처·옵션·가격 종류·이미지 연결을 보존한다. 같은 시각의 상충 관측은 임의 순서로 선택하지 않는다. 최신 실패/NULL이 이전 확인 값을 지우지 않으며 기존 상품 UUID를 재사용한다. 같은 원천의 정제 정책 변경은 새 정책 해시를 남겨 값 정정을 허용한다. 별개의 동시각 원천 값 충돌은 계속 차단한다.

원천에 있는 비교 URL의 구조와 상품 ID를 검사한다. 실제 URL 접속 성공까지 검증했다고 표시하지 않는다. 다운로드 상태·메타데이터가 확인된 이미지를 저장 관계로 연결하며 담당 승인·리뷰 원장을 만들거나 갱신하지 않는다. 기존 모델 결과를 생성·덮어쓰지 않고, 호환 벡터 검증을 구현하기 전까지 `recommendation_ready=False`다. 구매 옵션·조건 검증이 없는 가격은 대표가격·구매 가능 상태로 승격하지 않는다.

새 Gold의 물리 테이블은 `gold_catalog/images/offers/attributes/links/prices/quality`, Silver는 기존 파서의 관측 테이블을 `silver_` 접두사로 저장한다. 각 저장 행은 `row_key`, `product_key`, `payload_json`, `record_hash`이며 기존 v1 테이블 스키마와 다르다. Delta proof는 테이블·고정 버전·키/해시·대상을 묶는다. 세션 검증 캐시는 4,096 proof로 제한한다. 물리 파일 스캔량은 Spark 실행 계획과 파일 계측으로 별도 확인해야 한다.

MySQL 출처 JSON `schema_version=3`은 `plan_id`, `product_key`, `source_refs`, 정책 해시, 보존 출처 목록을 가진다. `pipeline_publications.gold_versions_json`은 고정 Gold proof, 결과, 서비스 행 읽기 대조 해시를 가진다. 현재 상품의 baseline이 해당 게시일 때 실제 상품·이미지·asset·판매·가격 이력·링크 값을 다시 읽어 대조한다. 이후 게시로 대체된 과거 완료는 불변 staging/proof를 검사하며 그 과거 값이 지금도 현재 값이라고 주장하지 않는다.

## 운영 불변 조건

로컬 파일 lock과 서버의 명시적 Mongo writer lease를 함께 사용한다. lease 만료만으로 자동 소유권 인수를 하지 않는다. Spark가 종료되지 않으면 lease 해제를 완료 처리하지 않는다. SQL commit 직전에도 writer 상태를 검사한다. 소스에는 쓰지 않고 임의 런타임 DDL도 수행하지 않는다.

scope 완료는 봉인된 **해당 상품 전체**의 Gold·게시 증거로 계산한다. 기존 65,498 같은 전역 상품 수를 완료 기준으로 사용하지 않는다. `BLOCKED/FAILED/SUPERSEDED/RUNNING`을 품질 보류와 구분한다. 실행 상태와 영수증에 비밀값·접속 URI를 넣지 않으며, 상태 내부 경로는 상대 POSIX 표기로 저장한다.

모든 실행 단계는 상태 경로 `metrics/stage-times/<scope-sha256>/`의 해시가 붙은 JSON 묶음으로 남긴다. 각 묶음은 개별 이벤트의 run UUID, 단계 이름, UTC 시작/종료, `perf_counter_ns` 기반 `elapsed_ms`, 성공/실패 범주, job hash, 관측/저장/SQL 호출 건수를 보존한다. 실행 중 쓰기 지연을 줄이려고 최대 32개 이벤트를 묶되 명령 경계·오류에서 즉시 저장한다. scope·job 원문과 예외 문구·원천값·URI는 측정 로그에 복사하지 않는다. `summary.json`은 단계별 이벤트 수·전체 경과·최댓값·실패 수·누적 카운터를 요약한다. `run.total`, `tick.total`, 개별 DB/Delta 작업 시간은 포함 범위가 겹치므로 단순 합산하지 않는다. 유한 배치 경로에서는 계측 저장 실패가 업무 상태를 바꾸지 않는다. 저장에 실패한 이벤트는 최대 4,096개까지 메모리에 두고 다음 flush에서 다시 저장하며, 실패 횟수는 `batch-status`의 `metric_failures`로 보인다. SQL 수는 연결 후 감지한 문장 호출 수이며 MySQL 서버 처리량이나 Spark의 실제 디스크 scan bytes를 뜻하지 않는다.

`recover`는 정지 근거와 이전 상태를 기록하고 차단된 작업이 뒤처져 있어도 해당 scope 커서를 되돌린다. 완료된 다른 작업은 결과를 다시 검증한 뒤 건너뛴다. 승인·활성화 기능을 복구 명령에 추가하지 않는다.

## 크롤 완료 유한 배치의 책임과 계약

재설계 목표 경로다. 위 `capture/tick` 실행기와 같은 업무 규칙(`domain/transform.py`)과 게시기를 쓰지만, 입력 고정·실행·저장 구조는 별도다.

| 책임 | 위치 | 지켜야 할 경계 |
|---|---|---|
| 배치·이벤트·처분 계약 | `domain/batch.py` | 크롤·전달 manifest, event ID, 거부 이벤트, 보존 기간, 철회·재계산 job ID를 네트워크 없이 정의한다. |
| 크롤 대조·Kafka 전달 | `application/outbox.py` | 행별 처분, 원천→Outbox 저장 후 읽기 대조, ack 기반 offset 범위, delivery generation |
| 제출·처리·게시·복구 | `application/batch.py` | job 원장 CAS, 용량 예약·해제, 처리와 게시를 별도 잠금으로 실행, 정지 근거 기반 복구, 상세 감사, 보존 정리 |
| 로컬 참조 처리 | `adapters/batch_local.py` | 파일 broker·FileLake로 계약과 복구를 검증한다. Spark를 대신하지 않는다. |
| 분산 처리 | `adapters/spark_batch.py`, `adapters/spark_gold.py`, `adapters/batch_delta.py`, `adapters/kafka_batch.py` | 고정 offset Kafka batch 읽기, Executor 원천 조회·분류·정제, 상품 단위 Silver·Gold 패스(`adapters/spark_product.py`), Delta snapshot/history |
| 제어 원장 | `adapters/batch_store.py` | 파일·Mongo 저장. `quarantine_raw`만 보존 기한 삭제가 허용되고 나머지는 append-only다. |
| 명령 | `interfaces/batch_cli.py` | NiFi가 호출하는 유한 명령과 JSON 출력 |

### Spark 저장 구조와 상주 처리기 (2026-10-25, `finite-v3`)

| 테이블 | 내용 | 쓰는 시점 |
|---|---|---|
| `input_index` | 관측별 이벤트·의존 정보·revision (원문 없음), bucket 16 분할 | Bronze, 원문보다 먼저 |
| `input_raw` | 승인된 원문, 받은 job별 파티션 | 게시 뒤 `finalize()`. job은 이 커밋 뒤에 `COMPLETED` |
| `normalized_history` | 정규화 결과(정책 identity 키) | 상품 패스 뒤, Gold와 동시 |
| `runs/<job>/gold`, `silver_holds` | 배치 출력 | 상품 패스 뒤 |

- Silver·Gold는 `product_key` 셔플 한 번 뒤 Executor의 상품 함수(`adapters/spark_product.py`)가 계산한다. native Gold(`spark_gold.py`)와 payload가 같음을 `test_spark_batch.py`가 확인한다.
- 이력이 있는 상품 키는 별도 테이블 없이 `input_index`에서 얻는다. 단일 writer가 시작할 때 상품 키 열을 한 번 읽어 메모리에 두고, 배치의 이력 커밋이 끝난 뒤에만 그 상품을 더한다. 이력이 없던 상품은 이력 테이블을 읽지 않고 이번 job의 캐시된 색인 행만 쓴다. 정책이 바뀌어 정규화를 다시 할 관측은 상품 패스가 원문 부족을 알리고, 그 원문의 job 파티션만 읽어 해당 상품을 다시 계산한다.
- 원문이 덜 커밋된 채 재시도되면 고정 Kafka 범위를 다시 읽어 채운다(처리 중이면 상품 패스 전, 게시 뒤면 `finalize()`).
- `batch-serve`는 Spark 세션·원장·Kafka·lease를 한 번 열고 spool 요청을 차례로 실행한다. 시작 때 Python 작업자를 미리 띄우고 상품 키 집합을 읽는다. 큐가 10초 이상 비면 이력 테이블을 OPTIMIZE하고, 요청이 오면 취소한다. 상품 색인은 이 단일 writer의 메모리에 둔다.
- `batch-abandon`은 멈추거나 시작 전인 job을 정지 증거·사유와 함께 `ABANDONED`로 닫고 대기열 예약을 푼다.

### 상태 축

| 축 | 값 | 저장 위치 |
|---|---|---|
| 입력 처분 | `ACCEPTED`, `INPUT_QUARANTINE`, `PAYLOAD_OMITTED`, `SOURCE_METADATA_ONLY` | 크롤 원장 `intake`, Bronze stage `counts`, 격리 metadata |
| Silver 품질 | `VALID`, `QUALITY_HOLD` | `normalized_history`, `silver_holds`/`silver_quality_holds` |
| job 진행 | `QUEUED` → `RUNNING` → `GOLD_COMMITTED` → `PUBLISHING` → `COMPLETED` | job 원장 |
| 실행 복구 | `RECOVERY_REQUIRED`(실패 단계 `failed_phase`) | job 원장 |
| 상품 결과 | `OUTPUT_COMMITTED`, `QUALITY_WAIT`, `SOURCE_RETRACTED` / 게시 `PUBLISHED`, `LAST_GOOD_RETAINED`, `QUALITY_WAIT`, `WITHDRAWN` | Gold `outcome`, publication receipt |

`PENDING_REVIEW`, `ACTIVATE_REVIEWED`, 승인 대기 상태는 만들지 않는다. 일시 장애는 품질 보류나 격리로 바꾸지 않고 `RECOVERY_REQUIRED`로 멈춘다.

### 상품 이력 한도

Silver는 상품별 관측 수와 바이트가 `history_observations`·`history_bytes`를 넘으면 그 상품만 `QUALITY_HOLD`(사유 `PRODUCT_HISTORY_LIMIT`)로 `silver_holds`에 기록하고 Silver·Gold·게시에서 제외한다. 배치와 다른 상품은 계속 처리한다. 이미 게시된 이전 버전은 그대로 남고 숨기지 않는다. 단계 `counts.history_limit`이 보류한 상품 수다. 같은 상품은 이후 batch에서도 한도를 넘는 동안 매번 다시 보류된다. 한도 조정이나 입력 정리는 운영자가 판단하며 자동으로 풀리지 않는다. 이 항목은 새 유한 배치 경로에만 적용한다. 위 `capture/tick` 실행기는 `BLOCKED_HISTORY_LIMIT`으로 그 작업만 멈춘다.

### manifest 연결

- 크롤 원장: 크롤 manifest, 고유 event inventory와 집합 hash, 입력 행 수·중복 수·처분별 수, 입력 바이트
- 전달 manifest: generation, partition별 `start_offset`(포함)·`end_offset`(제외)·물리 레코드 수, 크롤 원장 hash
- 단계 manifest(`bronze`/`silver`/`gold`/`publication`): `parent`(앞 단계 content hash), 입력(`delivery_hash`, `event_set_hash`, generation), 출력 proof(Delta 경로·버전·행 수), 단계별 `counts`, `attempts`, 시작·완료 시각, code version. Spark 실행이면 `spark_application_id`도 남긴다. Bronze에는 `source_version_watermark`, Gold에는 `gold_version`·`input_hash`·`rule_version`, 게시에는 receipt 목록을 남긴다.
- 출력이 있어도 manifest가 없으면 완료가 아니다. 같은 job을 다시 실행하면 Delta snapshot은 `txnAppId`와 내용 대조로, history 테이블은 키·hash로, 게시는 product job ID와 MySQL plan ID로 멱등 처리한다.

### 묶음 단위 원격 호출

Outbox 대조와 Kafka 발행은 이벤트마다 원격 호출하지 않고 `outbox_chunk`(기본 200건, 최대 1,000건)와 `outbox_chunk_bytes`(기본 32MiB) 중 먼저 닿는 한도로 묶는다. `prepare`는 묶음마다 원천 스냅샷을 일괄 저장하고 일괄 읽어 대조한 뒤에 Outbox 레코드를 같은 방식으로 저장한다. 원천이 Outbox보다 먼저라는 순서는 유지한다. `relay`는 묶음의 Outbox·원천·기존 ack를 키 일괄 조회로 확인하고, ack가 없는 메시지만 `send_many`로 함께 발행해 응답을 한 번 기다린 뒤 ack를 일괄 저장한다. 일부만 확인되면 확인된 ack는 저장하고 `KAFKA_DELIVERY_UNCONFIRMED`로 멈추며, 재실행은 확인되지 않은 메시지만 다시 보낸다. ack 저장 전에 실패하면 그 묶음 전체가 다시 발행될 수 있고, 같은 event ID의 중복은 Spark 입력 검사에서 하나로 접힌다. 게시 단계의 `delivery_output`·`publication_receipt`도 청크 단위 일괄 조회·저장이다.

### 진단 필드와 이벤트 식별

`image_state_checked_at`은 이미지 확인 시각이라는 진단 값이다. 원천 설계는 이미지 상태 지문에서 이 값을 제외하며, 이벤트 ID도 이 값을 쓰지 않는다. 그래서 Outbox는 정상 접수하는 항목에서 이 필드를 제거한 형태로 payload hash와 원천 스냅샷을 만든다. 확인 시각만 바뀐 재접수는 같은 이벤트로 접히고 새로 처리할 것이 없다. 접수가 거부돼 제한 접근 격리로 가는 원문은 받은 모양 그대로 보관한다. 원천 Mongo에는 확인 시각이 그대로 남는다.

### 입력 고정과 재시도

처리 시작 시 job별 `input_pin` 기록을 한 번 저장한다. 이 기록은 Silver가 참조할 앞선 완료 Bronze job 목록과 그 시점의 철회 목록을 고정한다. 재시도와 복구는 항상 이 기록을 읽는다. Silver 저장 뒤 단계 manifest 기록이 실패하고, 그 사이 다른 batch가 Bronze를 완료하거나 새 철회가 등록돼도 재시도 입력은 같다. 그래서 같은 job의 Silver 출력이 달라져 `LAKE_IMMUTABLE_CONFLICT`가 나지 않는다. `input_index`는 job ID별 append-only 행이므로 job 목록이 같으면 읽는 행도 같다. 고정 이후에 등록된 철회는 `batch-repair`로 반영한다. 이 고정은 로컬 파일 시험과 Spark 시험(`MemoryDelta`)으로만 확인했다.

### 순서와 버전 보호

같은 상품의 이벤트는 같은 Kafka partition에 들어가지만 Spark 이후의 순서는 가정하지 않는다. Silver는 Bronze가 완료된 모든 job의 이력을 묶어 정제하고, Gold는 관측 시각과 필드 규칙으로 현재값을 고른다. 게시 순서가 뒤집혀 오래된 batch가 나중에 게시되면 결과는 `LAST_GOOD_RETAINED`가 되고 최신 행을 덮지 않는다. 처리 잠금은 하나뿐이므로 Delta 대상 writer도 하나다.

### 원천 철회와 재계산

`batch-retract`는 원천 버전 하나를 철회 원장에 남긴다. 이후 모든 Silver는 그 버전을 제외한다. `batch-repair`는 지정 상품의 보존된 이력만으로 Silver·Gold를 다시 만든다. 정규화 결과는 재사용하므로 원천 본문을 다시 읽지 않는다. 남은 근거가 있으면 다시 계산한 값을 게시한다. 이때 MySQL에서는 철회된 버전의 필드 근거·출처·속성·이미지 연결을 비활성화하고, 버전 fence는 철회로 낮아진 최신 시각을 허용한다. 남은 근거가 없으면 상품을 `INACTIVE`/`SOURCE_RETRACTED`로 숨긴다. 철회 결과를 계산한 뒤 새 관측이 먼저 게시됐을 수 있으므로, 게시 시점에 현재 버전의 출처에 철회 대상이 아닌 근거가 남아 있으면 숨기지 않고 `LAST_GOOD_RETAINED`로 둔다. 일부만 철회한 재계산도 같은 원칙이다. 철회는 제거하는 근거만큼만 최신성 기준을 낮추므로, 입력의 최신 관측 시각이 현재 게시본의 **철회되지 않은 근거** 중 최신 시각보다 오래되면 `LAST_GOOD_RETAINED`로 두고 최신 게시본을 되돌리지 않는다. 행과 이력은 지우지 않는다. 가격 이력(`offer_snapshots`)과 과거 링크 관측 행은 철회 표시 열이 없어 그대로 남는다. 개인정보 삭제 요청은 이 철회와 다른 절차다.

### 격리와 보존

허용된 격리 원문은 일반 원천과 분리된 `quarantine_raw` 원장(서버에서는 Mongo `pipeline_v2_quarantine_raw` 컬렉션)에 둔다. Bronze 격리·metadata 테이블(`bronze_quarantine`, `bronze_metadata`)에는 event ID, 처분, 사유 코드, 수신 위치, 만료 시각만 남긴다. 이 테이블은 불변 stage snapshot이 아니어서 보존 정리로 지울 수 있다. 감사는 만료 전 metadata만 대조한다. 접근 권한 분리는 서버 DB 역할로 적용해야 한다. 코드는 읽기마다 `quarantine_access` 기록을 남긴다.
