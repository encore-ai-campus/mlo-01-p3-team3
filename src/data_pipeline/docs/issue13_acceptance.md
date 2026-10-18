# 이슈 #13 구현·인수 대조

2026-10-18 기준. 체크 여부는 담당자가 판단한다. 아래는 구현, 소량 실제 검증, 남은 인수를 구분한 근거다. 현재 검증 상태의 기준은 [실서버 검증](service_validation_20261018.md), 실행은 [문서 안내](README.md)를 따른다. 문서가 있다는 이유로 운영 완료로 판정하지 않는다.

## 1. 입력 검증 및 스냅샷

| 요구 | 구현·검증 | 남은 인수 |
|---|---|---|
| raw/products/media의 상품·실행·관측 대응 | snapshot 검증, fixture와 실제 3상품/9이미지 관계 | 전체 수집량 부하 |
| 원본 보존·불변 snapshot | 별도 경로·fsync·완료 manifest·변경 hash 차단 | 대상 파일시스템 장애 내구성 |
| 파일·이미지 checksum·경로 검증 | SHA256·Pillow 포맷/크기/해상도·경로 이탈 차단, 실제 객체 바이트 대조 | 대량 전수 검사 |
| 대량 JSONL 스트리밍·분할 | FULL_EXPORT 외부 partition/sort, LIVE 변경/제어 이벤트·바이트·이력 상한 | 65,498건 처리량·용량 |
| 외부 쓰기 없는 검증 | plan의 네트워크·state 생성 금지 회귀 시험 | 현재 스키마 범위에서 구현 |

## 2. 원천·이미지 적재

| 요구 | 구현·검증 | 남은 인수 |
|---|---|---|
| Mongo 접수/처리 원장 | observations/snapshots/attempts·fenced lease·이미지 CAS, 실제 서버 적재 | 운영 보관/부하 |
| 상품/관측/원천 키 분리·불변 raw·멱등성 | INSERT-only·충돌 차단·새 관측 별도 보존, 실제 3건 반복 시 추가 0·최초 ID 복구 | 운영 데이터 전수 |
| majority/journal·미확인 복구 | 실제 ACK와 3멤버 majority 읽기, 응답 미확인 readback 회귀 | 실제 노드 중지·선거·응답 유실 |
| 이미지 객체 재사용·연결 | URL·순서·source_version·slot·checksum, 실제 9관계 및 Garage 내용 주소 재사용 | 저장소 노드 장애·내구성 |
| 부분 실패·원문 크기 초과·본문 충돌 | 이미지 실패와 raw 보존 분리·BSON 상한 격리·충돌 거부 회귀 | 실제 한도/권한 장애 |

크기 초과 원문은 봉인 snapshot에 보존하고 격리한다. 외부 본문 envelope로 자동 전환하지 않는다. Garage 2.4.1은 조건부 PUT 대신 [내용 주소 프로필](garage_v241_profile.md)을 사용하며 외부 작성자의 직접 덮어쓰기까지 막는 WORM이 아니다.

## 3. Kafka 발행 및 복구

| 요구 | 구현·검증 | 남은 인수 |
|---|---|---|
| product.raw.collected 계약·원천 확인 후 발행 | 고정 key/필드/ID/시각/본문, RAW_CONFIRMED 이후 전송, 실제 격리 토픽에서 3건 | 운영 토픽/ACL·보관 정책 |
| 고정 재발행·중복 전달 | 같은 이벤트 replay·동일 checkpoint 재소비 후 raw/event 3/3 유지 | 실제 broker 장애·ACK 유실 |
| 전달 확인/미확인 구분 | callback·EVENT_UNKNOWN·RF3/minISR2 확인 | 실제 failover |
| 저장 후 발행 전 종료·등록 복구 | 대역 경계 중단 회귀, 실제 등록 실패 3건을 기존 ID로 재개 | 모든 외부 경계의 실제 장애 주입 |

at-least-once 전달이다. 응답 미확인은 같은 이벤트를 재전송할 수 있으며 새 ID로 중복을 감추지 않는다.

## 4. Spark·MySQL

| 요구 | 구현·검증 | 남은 인수 |
|---|---|---|
| 원천 ID Bronze 멱등 | 실제 Kafka/Mongo/Garage 3원천·3receipt·격리0, 반복/재전달 | 전체 데이터·장애 |
| 작업별 checkpoint·단일 작성자 | cluster ID/fingerprint 바인딩·MongoLease, 실제 잠금 중복 차단·Worker 3대 Delta | 다른 Driver로 장애 후 재개 |
| Silver 분류/규격/가격/이미지/검수·품질 | 8개 테이블·고정 입력·재평가·receipt 구현, 실제 원천 1상품/4이미지 관계 엔진·QUALITY_WAIT | 담당 NORMALIZATION/매핑/검수 export와 운영 CLI |
| Gold 안정 키/관측 시각·현재 판매/이력 | 8개 테이블·고정 Silver/export·반복 재사용, 실제 저장소 엔진 시험 | 담당 GOLD_BUILD/정책 입력·확보율 |
| 고정 Delta→staging→MySQL·UUID/검수 보존·역전 방지 | 실제 MySQL01 8.4.11 별도 문서 스키마에서 9개 게시/복구 사례 | 실제 서비스 DDL·기존 UUID/관측 baseline·guard·접속 TLS |

실제 Silver/Gold는 담당 입력 NOT_PROVIDED, Gold 정책 fixture의 기술 시험이다. 준비 완료/추천 승인이나 운영 CLI 등록 인수가 아니다. 새 이미지 연결은 inactive로 게시하며 담당 원장을 수정하지 않는다. 미확인 집계 가격으로 가상 Offer를 만들지 않는다. 집계 가격/판매 항목, 최초 시각/값별 관측 시각을 구분한다.

## 5. 수집 중 증분 인계

| 요구 | 구현·검증 | 남은 인수 |
|---|---|---|
| commit 이하 raw/normalized 확정 쌍 | journal 읽기 전용·tail 제외·C 고정, 실제 저장 journal 5관측/반복 신규0 | 실제 동시 수집 프로세스 |
| seq/hash/source 정체성 | 연속 seq·run/config/prefix·잘림/교체 거부 | 실제 공유 파일시스템·rotation 운영 |
| cursor/DB 접수/발행 분리 | 불변 배치·원장·drain receipt 분리 | DB 장애 중 장기 spool |
| 불변 microbatch·ready-last 원격 인계 | seal-last·8MiB chunk·수신 hash, 실제 Master→Garage→NiFi 3상품/9관계·반복 추가0 | 실제 PC 동시 수집 인계 |
| 이미지 후속 인계·일괄/증분 중복 방지 | frozen media·모호성 대기·동일 관측 key·registry ID 재사용 회귀 | 실제 이미지 단계와 동시 인계 |
| backpressure | 변경/제어 이벤트·임시 quota·최소 여유·cursor 정지 회귀 | 장기 backlog/디스크 부하 |

LIVE 캐시는 append-only source와 파이프라인 소유 불변 spool을 전제로 하며 재시작 시 prefix/chain을 검증한다. media 기준 seq가 없는 입력은 EXACT_COMMIT으로 해석하지 않는다. 상품 상세 직후 다운로드하도록 크롤러를 바꾸는 작업은 포함하지 않는다.

## 6. 실행·운영

| 요구 | 구현·검증 | 남은 인수 |
|---|---|---|
| 경로/인증/저장소 외부화·의존성 기록·preflight | 역할별 설정·비밀 출력 금지·누락 차단, 실제 Linux venv/CLI/pip check·서비스 접속 | AWS/다른 환경 조합 |
| 제한 재시도·lease·중복 실행 | 단일 호스트 파일 queue·Retry-After·한도·격리·token 복구, 실제 exit130 후 INGEST 성공 | 공유 노드 장애·운영 정책 |
| 로그·건수·실패·duration_ms | ingest audit·capture/drain/receipt·배치 결과, 로그 크기/보관 경보·여유 공간 차단 | 중앙 보관/알림·quota 기준 |
| 수동 복구 후 NiFi | 실제 제한 SSH·NiFi 2.12.0 timer/STOPPED·activeThread0 | 승인 입력의 전체 7단계 FLOW·운영 일정 |
| 주기 대조·상태 이관 | 고정 Delta/SQL 포함 flow-audit·repair/replay, 파일 hash/ID/receipt 보존 pack/restore·release | 외부 DB/객체/Kafka/Delta 백업·복원 및 목적지 재개 |
| 실행/재개/재처리/배포 문서 | 단계별 실행·배포·이관 가이드 | 담당자 운영 인수 |

## 완료 판단과 지원 경계

- 최종 자동 회귀 **252개/116.362초 통과**. Linux 운영/LIVE/전송 파일·대역 **43개/1.056초 통과**. 실제 시험과 대역을 합산하여 통합 완료로 판정하지 않는다. 로컬 근거는 [로컬 검증 기록](validation_20261018.md), 공개 서버 결과는 실서버 검증 문서에 있다.
- 운영 전에 실제 DDL/guard·매핑/검수·NORMALIZATION/GOLD_BUILD 입력 계약을 인수하고 전체 FLOW·운영 일정·백업/복원·부하를 확인한다. 등록 export가 공식 계약이면 서비스 pipeline_runs 직접 생성 어댑터가 반드시 필요한 것은 아니다.
- queue는 지정 호스트 한 대 소유다. 만료된 Driver lease 자동 takeover·TTL 삭제, Windows Spark checkpoint URI 변환, 다른 Kafka cluster offset 자동 이행은 지원하지 않는다. 이전 Driver 종료 증명 후 명시 복구한다.
- Silver 상품별 이력 기본 상한은 20,000행/32MiB다. 65,498건 합성 stream 비교는 전체 JSON/ETL 성능 보장이 아니다. 과거 미수집 응답 본문/hash·판매 조건/시각은 생성하지 않고 NULL/UNKNOWN/LEGACY_EXTRACTED_ONLY를 유지한다.
- 정상 재개는 원장에 확인된 성공 단계를 재사용한다. 외부 유실 raw의 명시 복구는 --repair-raw, 고정 이벤트 재발행은 --replay-events로 구분한다. 검증 원장의 운영 fingerprint 충돌 처리도 실서버 인계 지침을 따른다.
- 크롤러/인프라 설치, 매핑·승인 원장 편집, 외부 임베딩·추천·UI·서비스 작업 원장 연결은 다른 담당자 책임이다.
