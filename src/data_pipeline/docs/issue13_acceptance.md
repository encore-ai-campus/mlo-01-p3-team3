# 이슈 #13 구현·인수 대조

2026-10-16 최초 작성, 2026-10-18 MySQL 게시·운영·이관 및 최신 dev 병합 후 상태 갱신. `feature/#13`. 설계 문서의 존재와 구현 완료를 구분한다. `src/data_pipeline`만 보완했으며 크롤러/인프라/서비스 코드는 변경하지 않았다. SSH hostname 조회와 구분해 실제 데이터 서비스 서버 시험은 수행하지 않았다.

상태의 **로컬 확인**은 파일 대역·SDK 계약·실제 로컬 Delta 또는 저장된 실제 수집물에 대한 확인이다. 실제 Mongo/S3/Kafka/클러스터 인수와 동등하지 않다.

## 1. 입력 검증 및 스냅샷

| 요구 | 구현·검증 | 남은 인수 |
|---|---|---|
| raw/products/media의 상품·실행·관측 대응 | snapshot._validate, gallery 의미·파일 대조; fixture 및 저장 실데이터 시험 | 실제 전체 수집량 부하 |
| 원본 보존·불변 snapshot | 별도 경로, fsync·완료 manifest·변경 hash 검증 | 대상 VM/S3 파일시스템 내구성 |
| 파일·이미지 checksum·안전 경로 | SHA256·Pillow 실제 포맷/크기/해상도, 루트 탈출 거부 | 실제 객체 저장소 비교 |
| 대량 JSONL 스트리밍·분할 | FULL_EXPORT 외부 partition/sort, LIVE 최대 변경/제어 이벤트 수·바이트 목표·유한 이력 행 | 65,498건 성능/용량 벤치마크 |
| 외부 쓰기 없는 검증 | plan, network/state 생성 금지 회귀 시험 | 없음(현재 지원 스키마 범위) |

## 2. 원천·이미지 적재

| 요구 | 구현·검증 | 남은 인수 |
|---|---|---|
| Mongo 접수/처리 원장 | observations/snapshots/attempts, fenced lease·이미지 CAS | 실제 3멤버 Mongo |
| 상품 키/관측 키/원천 ID 분리 | 계약 v1 키 및 최초 ObjectId·event UUID 보존 | 실제 DB 재실행 |
| 불변 raw·멱등 재적재 | INSERT-only·내용 충돌 거부·새 관측 별도 보존 회귀 시험 | 실제 서버 |
| majority/journal·응답 미확인 | WriteConcern/ReadConcern 명시, 미확인 insert 다수결 readback | 실제 노드 장애/응답 유실 |
| 객체 저장·동일 바이트 재사용 | SHA256 key, 실제 바이트 확인, 조건부 PUT | 실제 S3 호환성/장애 |
| 이미지 순서/출처/원천 버전/checksum | image_refs·source_version·원본 URL·slot·checksum 및 확보 상태 분리 | 실제 업로드 대조 |
| 부분 실패/크기 초과/충돌 | 이미지 실패와 raw 보존 분리, BSON/접수 문서 상한·본문 충돌 격리 | 실제 한도/권한 장애 |

크기 초과 처리는 **봉인 snapshot의 원문을 보존하고 격리하는 정책**이다. Mongo 상한을 넘는 본문을 잘라 저장하거나 무조건 성공 처리하지 않는다. 외부 본문 참조로 자동 전환하는 구현은 없으며 이번 요구의 ‘크기 초과 처리’를 그 기능이 반드시 필요하다는 뜻으로 바꾸지 않는다.

## 3. Kafka 발행 및 복구

| 요구 | 구현·검증 | 남은 인수 |
|---|---|---|
| product.raw.collected 계약 | 고정 키/필드/ID·시각·본문 검증 | 실제 producer/consumer |
| 원천 확인 후 발행 | RAW_CONFIRMED 이후 전송 | 실제 쓰기 순서 증거 |
| 고정 이벤트 재발행 | 명시 replay, 미확인 전송 재개, 동일 본문 보존 | 실제 broker 응답 유실 |
| 전달 확인/미확인 구분 | delivery callback, EVENT_UNKNOWN; RF3/ISR>=2/minISR>=2 preflight | 실제 ACL·broker failover |
| 중간 종료/중복 복구 | 외부 경계 중단 회귀 시험·등록 증거 복원 | 실제 시스템 장애 주입 |

at-least-once 전달이므로 응답 미확인은 동일 이벤트를 재전송할 수 있다. 임의의 새 이벤트 ID를 발급해 중복을 감추지 않는다.

## 4. Spark·MySQL

| 요구 | 현재 상태 |
|---|---|
| 원천 ID Bronze 멱등 | 구현. 로컬 실제 Delta 및 대역 시험; 실제 Kafka/Mongo/lake 인수 대기 |
| 작업별 checkpoint/대상 정체성 | Bronze 별도 경로·실제 Kafka cluster ID 바인딩·Mongo ops 단일 Driver 잠금 구현 |
| Silver 분류/규격/가격/이미지/검수·품질 상태 | 구현·로컬 검증. 실제 담당 입력/서버 인수 대기 |
| Gold 안정 키/관측 시각·판매 정보 | 구현·로컬 검증. 실제 담당 입력/서버 인수 대기 |
| 고정 Delta/staging/MySQL 검증·UUID/검수 보존·과거 값 차단 | 문서 계약으로 구현. 고정 계획·guard·원자 게시·UUID 유지·응답 유실/과거 값 차단을 실제 격리 MySQL로 검증. 실제 DDL·기존 baseline·담당 guard 인수 대기 |

Bronze는 만료된 Driver의 저장소 쓰기를 소급 취소할 수 없으므로 자동 takeover하지 않는다. 이전 Driver의 실제 종료가 확인되기 전 새 실행을 막는다. [운영 잠금·수동 복구](bronze_execution.md)를 따른다.

## 5. 수집 중 증분 인계

| 요구 | 구현·검증 | 남은 인수 |
|---|---|---|
| commit 이하 raw/normalized 쌍 | journal 읽기 전용, tail 제외, C 고정 | 실제 동시 수집 프로세스 |
| seq/이력 hash/source 정체성 | collector 직렬화 hash, 연속 seq, run/config/prefix, 잘림·교체 거부 | 실제 공유 파일시스템 |
| capture/접수/발행 진도 분리 | cursor vs 불변 배치 vs DB/event 원장 및 drain receipt | 실제 DB 장애 중 spool 운영 |
| 불변 microbatch/원격 ready | seal-last, cursor 후행, 8 MiB chunk/ready-last, 수신 전체 hash 검증 | 실제 PC↔VM S3 전송 |
| 이미지 확보 버전 후속 | frozen media 대응·모호성 대기·source 종료 재분류·부분 확보 새 관측 | 실제 이미지 단계와 동시 인계 |
| 일괄/증분 동일 관측 중복 방지 | FULL/LIVE 동일 key 시험, 기존 registry ID 재사용 | 실제 Mongo 고유 키 인수 |
| 적체/spool backpressure | 변경/제어 이벤트·큰 envelope 격리·임시 작업 quota·최소 여유·cursor 정지 | 장기 backlog 부하 |

LIVE 실행 중 검증 캐시는 append-only source와 파이프라인 소유 불변 spool을 전제로 한다. 재시작 때 prefix/chain 전체를 검증한다. media 생성 기준 seq가 없는 기존 입력을 EXACT_COMMIT으로 해석하지 않는다. 실제 source 보관/rotation 정책은 [실행 계약](live_execution.md)대로 운영 담당이 보장해야 한다.

## 6. 실행·운영

| 요구 | 구현·검증 | 남은 인수 |
|---|---|---|
| 경로/주소/인증/저장소 외부화 | 환경변수·설정, 비밀 출력 금지, 전용 handoff bucket | 실제 환경별 설정 |
| 의존성·런타임 기록 | base/Bronze lock, Python/Java/Spark/Delta 버전 문서 | Linux/VM/AWS 조합 시험 |
| preflight/누락 차단 | ingest 읽기 전용 Mongo/S3/Kafka 검사; Bronze 설정/cluster/ops claim 확인 | 실제 권한/토폴로지 |
| 제한 재시도/lease/중복 실행 | 기존 잠금 및 파일 queue의 due/Retry-After·한도·격리·token 복구 구현 | 지정 실행 호스트 한 대 운영, 실제 다중 호스트/노드 장애 |
| 로그/건수/실패/duration | ingest audit, capture 결과, drain_report/receipts, Bronze 배치·인수 보고서 | 서버 로그 보관/알림 |
| 수동 복구 후 NiFi 예약 | ready 발견·허용 작업 queue·수동 복구 확인, REST 버전/bundle·STOPPED 생성·명시 start/stop 구현·대역 시험 | 실제 NiFi 버전/권한·일정 활성화, 전체 Spark 단계 자동 연결 |
| 주기 대조·상태 이관 | snapshot↔Mongo Raw/접수/발행 대조·repair, 파일 상태 hash/ID 보존 pack/restore·코드 release 구현 | 전체 Delta/서비스 대조·외부 저장소 실제 백업/복원·Linux/AWS 재개 |
| 실행/재개/재처리/배포 문서 | README, execution, live_execution, bronze_execution, deployment, acceptance | 담당자 운영 인수 |

정상 재개는 확인된 외부 단계에 다시 요청하지 않는다. 원장 성공 상태를 신뢰하므로 외부에서 삭제된 raw는 `--repair-raw` 명시 실행으로 원래 ID 아래 확인/복구한다. 이벤트 replay와 분리했다.

## 검증 기록과 완료 판단

- 최신 dev 병합 후 전체 자동 테스트: **192개 통과, 74.540초**, `.runtime/post-dev-merge-regression-20261018.txt`.
- 실제 격리 MySQL 8.0.46의 UUID/롤백/guard/응답 유실/시각 역전 및 실제 로컬 Delta 고정 게시 계획 시험 통과. NiFi는 REST 대역, 이관은 로컬 파일/원장 시험이다. [최신 검증 기록](validation_20261018.md).
- 과거 이슈 보완 시험: **122개/64.058초**, `.runtime/issue13-gap-tests.txt`; Gold 구현 당시 **162개/67.157초**. 최신 결과와 구분한다.
- 저장된 실제 journal: 확정 관측 **5건**, 첫 drain 1배치, 재실행 신규 적재 **0배치**, source config/state/journal hash 유지. `.runtime/actual-live-gap-v2/report.json`.
- 변경 후 실제 로컬 Spark 4.0.1/Delta 재시험: 원천 **3건**, event receipt **3건**, 격리 **2건**, 원문·UTC 시각 일치 및 재실행 유지. `.runtime/bronze-gap-v1/report.json`. 운영 lease는 이 시험에서 대역이다.
- 이미지 연결·부분 실패·media 페이지 재시도·소스 종료·중복 pointer·cursor 중단·원격 chunk 손상은 fixture 기반 회귀 시험이다. 실제 실행 중 수집기 또는 원격 전송 시험으로 표시하지 않는다.
- 원천/이미지 성공 단계 생략, 새 관측 불변 저장, 응답 유실은 파일/SDK 대역 시험이다.
- 로컬 경로 이전 후 원장/ID/receipt 보존을 시험했다. VM/AWS의 실제 DB·객체·Delta·Kafka 이전을 검증한 것은 아니다.
- `tests.smoke_live_services`는 실제 연결 인수 코드다. 설정이 없으면 **NOT_RUN**, 실제 전부 통과해야 PASS다. 노드 failover 시험을 자동 수행하지 않는다.

이슈는 아직 닫을 수 없다. MySQL 게시기·예약 재시도·NiFi 제어·파일 상태 이관은 구현했다. 남은 것은 **전체 단계 자동 연결·등록/게시 입력 계약**, **전체 종단 대조**, **실제 서버 종단·장애 인수**, **환경 이전·NiFi 운영 인수**, **대량 부하 시험**이다. Silver/Gold와 무관한 LIVE·복구·중복 실행 제어를 단순히 후속 설계로 미루지 않고 코드와 로컬 검증까지 보완했다. 테스트 코드가 있다는 이유로 실제 환경 인수 조건을 완료 체크하지 않는다.

현재 지원 경계와 남은 서버 외 연결(전체 단계 자동화·등록 입력 방식·실제 DDL/guard·종단 대조)은 [최신 누락 대조](requirements_gap_review.md)가 기준이다. 이슈 체크는 담당자가 수행한다.
