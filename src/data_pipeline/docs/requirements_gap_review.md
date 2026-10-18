# 구현 요구사항 대조와 남은 작업

2026-10-18 갱신. Issue #13, 최신 `origin/dev` 병합 후 `feature/#13` 기준이다. **구현·로컬 검증·외부 계약·실제 서버 인수**를 구분한다. 주요 단계와 FLOW 자동 연결·고정 범위 종단 대조를 구현하고 로컬 시험했지만 실제 전체 worker/서버 종단 운영 완료는 아니다. [현재 현황](status.md), [이슈 대조표](issue13_acceptance.md), [검증 기록](validation_20261018.md)을 함께 따른다.

## 현재 요구사항 대조

| 요구 | 현재 구현·근거 | 남은 확인 |
|---|---|---|
| 입력 대응·원본 보존·checksum·스트리밍·검증 전용 | snapshot/files, FULL_EXPORT/LIVE 검증·plan | 전체 수집량 성능/용량 |
| 접수·불변 Raw·관측/상품 키·majority/journal·미확인 복구 | mongo/runner/contracts, SDK/파일 장애 시험 | 실제 Replica Set 장애/권한 |
| 이미지 공유·순서·URL·원천 버전·checksum·부분 실패 | objects·이미지 vector/성공 슬롯 재사용 | 실제 S3 호환성/내구성 |
| 원천 크기 초과·본문 충돌 | 격리·snapshot 원문 보존·같은 키 다른 본문 차단 | 외부 본문 envelope 자동 변환은 지원하지 않음 |
| 저장 확인 후 Kafka·고정 이벤트 재발행 | publisher/runner, ID/시각/본문 replay·preflight | 실제 broker 장애·중복/응답 유실 |
| Bronze 멱등·checkpoint·단일 작성자 | Delta/receipt/격리·공용 Mongo lease | 실제 다중 호스트·Kafka/Mongo/lake |
| Silver 분류/규격/가격/이미지/검수·품질 | 8 Delta·고정 입력·재평가·receipt·CLI | 실제 업무 snapshot/등록 입력 |
| Gold 안정 키·현재값/이력·집계·고정 export | 고정 Silver→8 Delta·완료 manifest | 실제 업무 계약/필드 확보율 |
| 고정 Gold→MySQL·UUID/검수 보호·역전 방지 | publication의 문서 계약 preflight·staging·guard·원자 게시·응답 유실 복구, 실제 격리 MySQL 시험 | 실제 DDL·기존 상품 baseline·guard 공급·TLS/권한/Primary 인수 |
| LIVE 확정 쌍·seq/hash·진도 분리 | journal/live/handoff | 실제 동시 수집/파일시스템 |
| 불변 배치·원격 ready-last·미디어 후속·중복/backpressure | live/transfer·quota/free-space/cursor 정지 | 실제 원격 전송·장기 적체 |
| 내구성 예약 재시도 | operations/queue·단일 소유 파일 원장, Retry-After·한도·token 복구 | 실제 지정 호스트 운영 |
| 새 입력 발견·주기 대조 | 기존 ready/Raw 대조 및 FLOW의 고정 Delta/SQL·품질 보류·이미지 바이트 대조, fresh audit | 실제 전체 worker·서비스 종단/장애 인수 |
| NiFi 연계 | REST 제어·대역 시험 및 tick→FLOW 7단계 순차 실행·완료 receipt/실패 재개 | 실제 NiFi 버전·권한·수동 복구 후 활성화 |
| 상태 보존 이관·서버 전달 | pack/verify/restore·정지/외부 증거·경로 별칭·source release | 외부 DB/객체/Kafka/Delta 실제 백업·복원·목적지 재개 |
| 환경/로그/실행 문서 | 외부 설정·버전 lock·preflight·건수/오류/duration·배포 가이드 | Linux/VM/AWS 조합·보관/중앙 로그/알림 인수 |

## 현재 연결·계약과 남은 인수

1. **전체 단계 자동 연결 구현:** FLOW가 고정 입력·버전·완료 receipt로 7단계를 연결하고 실패 단계부터 재개한다. queue/NiFi tick의 FLOW 등록과 지정 실행 호스트 설정을 실제 서버에서 인수해야 한다. 로컬 시험 adapter의 Delta 연결 통과를 실제 worker CLI/데이터 서비스 종단 통과로 확대하지 않는다.
2. **등록 입력 운영 방식:** Silver NORMALIZATION/Gold GOLD_BUILD 담당 export 검증은 구현했다. 서비스 `pipeline_runs` 직접 생성/조회/갱신은 없다. export 제공을 공식 인수하면 직접 어댑터가 반드시 필요한 것은 아니며, 직접 연계 요구라면 추가 개발한다.
3. **실제 게시 계약:** 참조 DDL과 새 운영 테이블은 문서 계약이다. 실제 서비스 ORM/DDL, 필드 확장, 기존 UUID/관측 baseline, 담당 guard 공급·만료/변경 규칙을 인수하고 차이가 있으면 어댑터를 보완한다. 게시기가 승인 원장을 대신 생성하지 않는다.
4. **고정 범위 종단 대조 구현:** FLOW RECONCILE/flow-audit가 snapshot identity·Mongo/객체·고정 Bronze 이벤트 receipt·Silver/Gold 근거·계획/품질 보류·SQL identity/이미지/가격 이력을 비교한다. 불일치는 쓰기 없이 실패하며 기존 명시 repair/replay와 분리한다. Kafka의 현재 보존 전수 확인과 실제 서버 장애 인수는 별도다.
5. **운영 인수:** 시도별 진행 로그 1MiB 상한·보관 기한 경보·저장소 size/free-space 진입 차단을 추가했다. 실제 quota/보관 기준·중앙 수집·경보 채널은 배포 환경에서 인수한다. source release와 상태 이관 도구가 실제 서비스 백업 작업을 대신하지 않는다.

## 지원 한계와 범위 밖

- 파일 queue는 지정 실행 호스트 한 대 소유다. 다중 호스트 queue/NiFi Primary 자동 인계는 현 지원 범위가 아니다.
- 이관 도구는 파일 상태와 ID/hash를 보존하고 외부 백업/복원 증거를 검증한다. 외부 저장소 백업 실행, Windows Spark checkpoint URI 변환, 다른 Kafka cluster offset 자동 이행은 제공하지 않는다.
- Silver의 상품별 이력 한도(기본 20,000행/32MiB) 초과는 명시 실패다. 전체 65,498상품·장기 이력 성능을 보장한 상태가 아니다.
- 큰 원문은 봉인 snapshot에 보존하고 격리한다. BSON 초과 외부 본문 참조 자동 변환은 별도 계약이다.
- 매핑/이용 승인 원장 편집·외부 임베딩 호출·모델 학습/추천·서비스 API·인프라 설치는 파이프라인 범위 밖이다. 확정 입력을 받는 책임과 그 판단을 직접 수행하는 책임을 혼동하지 않는다.
- 과거 추출의 HTTP 응답 원문/response_body_hash, 당시 미수집 판매 조건·날짜·축은 복원하지 않는다. LEGACY_EXTRACTED_ONLY/NULL/UNKNOWN을 유지한다.

## 필드 제안과 게시 경계

[46항목 대조표](silver_gold_field_mapping.md)의 G/R 후보·준비 상태는 Gold에 구현했다. C04는 이제 고정 export→문서 기반 MySQL 게시기로 연결된다. 기존 product/asset/image/offer UUID를 재사용하며 새 이미지 연결은 inactive다. 서비스 검수/이용 승인·활성화는 담당 원장을 따른다. 미확인 목록 가격으로 가상 Offer를 만들지 않는다. 필드별 관측 시각·가격 조건·최초 시각과 후속 관측을 분리한다.

## 검증·다음 순서

FLOW 후속 구현 최종 전체 회귀 **218개 / 104.135초 / OK** (`.runtime/flow-regression-final.txt`). 이전 dev 병합 직후 192개/74.540초는 당시 결과다. 실제 격리 MySQL 8.0.46과 로컬 Spark 4.0.1/Delta 게시 계획 시험을 별도로 통과했다. NiFi는 REST 대역, 이관은 로컬 파일/원장 시험이다. 세부 보고서 경로는 [검증 기록](validation_20261018.md)에 있다.

이전 Gold 단계의 162개/67.157초, 3상품·9이미지 관계·27링크·3평가, 반복 3건 재사용은 당시 증적이다. 실제 표본은 QUALITY_WAIT/현재 Offer 0이며 확인 가격·완전 준비 상태는 fixture 검증이다. 이전 'MySQL/예약/NiFi/이관 미구현' 판정은 후속 구현으로 대체됐다.

전체 연결·대조의 현재 코드/로컬 시험은 [FLOW 실행 가이드](flow_execution.md)를 따른다. 후속 순서는 등록/게시 입력 계약 확정 → 실제 서버의 전체 worker 소량·중복·중단/장애 시험 → 전체 실데이터 성능 → NiFi 활성화·복원 인수다. 65,498건 합성 stream 비교 benchmark는 전체 JSON/정렬/ETL 처리량을 검증하지 않는다. 이슈 완료 체크와 업무 승인은 담당자가 수행한다.
