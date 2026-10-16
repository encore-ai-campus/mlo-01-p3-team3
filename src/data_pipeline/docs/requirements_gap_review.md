# Gold 구현 후 요구사항 대조와 남은 작업

2026-10-17. Issue #13의 사용자 요구, `docs/design/01-infrastructure.md`, `02-data-pipeline.md`, `03-data-model.md`, `05-kafka-jobs.md`, `12-direct-recommendation.md`, 파이프라인 통합/데이터/LIVE/보강 계약, Silver·Gold 필드 제안 대조표를 기준으로 검토했다. **로컬 구현, 외부 담당 입력, 미구현, 실제 환경 미검증을 구분한다.** 이 문서는 이슈 완료 체크나 팀 승인을 대신하지 않는다.

작업 브랜치는 feature/#13, 전용 작업 폴더는 mlo-01-p3-team3-pipeline-13이다. 기존 LIVE/복구/Silver 작업을 `98ae0ae`으로 먼저 커밋했다. Gold와 이 대조 문서는 그 이후 작업이다. 원본 작업 폴더의 크롤러·다른 팀원 스테이징·상위 설계 문서는 변경하지 않았다. 푸시는 하지 않았다.

## 이슈 구현 범위 대조

| 요구 | 현재 근거·판정 | 서버 시험과 별도로 남은 것 |
|---|---|---|
| 입력 대응·원본 보존·checksum·스트리밍·검증 전용 | snapshot/files, FULL_EXPORT/LIVE 검증, plan 구현 | 전체 수집량 성능 한계 측정 |
| Mongo 접수·불변 Raw·관측 키·majority/journal·응답 유실 복구 | mongo/runner/contracts 및 계약·장애 테스트 구현 | 실제 서버 인수 |
| 이미지 객체 공유·순서·URL·원천 버전·checksum·부분 실패 | objects/image vector·성공 슬롯 재사용 구현 | 실제 S3 호환 구현/권한/내구성 인수 |
| 원천 크기 초과·본문 충돌 | RAW_TOO_LARGE 격리·원본 snapshot 보존, 같은 키 다른 본문 차단 | 큰 원문 객체 참조 envelope v2는 별도 계약, 현재 자동 변환 없음 |
| Kafka 원천 저장 후 고정 이벤트 발행·전달 미확인 복구 | publisher/runner, 동일 ID/시각/본문 replay·Broker 조건 preflight 구현 | 실제 Broker 전달/장애 인수 |
| Bronze 원천 ID 멱등성·checkpoint·단일 작성자 | bronze Delta/receipt/격리, 공용 Mongo lease 구현 | 실제 다중 호스트 인수 |
| Silver 분류·규격·가격·이미지·검수와 품질 상태 | silver 8 Delta·고정 의존 입력·재평가·receipt·CLI 구현 | 실제 업무 snapshot 제공, 등록 export 인수 |
| Gold 안정 상품/판매 키·관측 시각·현재값/이력 | gold/build·source·storage·runner·export, 고정 Silver→8 Delta+완료 manifest 구현 | 시각/조건/순서 미확정 입력은 보류. 실제 업무 계약 확인 |
| 고정 Gold→staging→MySQL 게시·UUID/검수 보존·역전 방지 | Gold 고정 버전 export와 게시 차단 사유까지 구현 | **MySQL DDL preflight·staging·UUID 조회/재사용·publishing guard·트랜잭션 게시 미구현** |
| LIVE 확정 쌍·seq/hash·cursor/접수/발행 분리 | journal/live/handoff 구현 | 실제 수집기 동시 실행 및 원격 인수 |
| 불변 마이크로배치·원격 ready-last·미디어 후속 | live/transfer, 봉인·대응 모호성·chunk/ready/receive 구현 | remote ready 발견·호출 일정의 실제 구성 |
| 일괄/증분 중복·spool backpressure | 동일 관측 수렴·quota/free-space/cursor 중단 구현 | 실제 NAS/객체 저장소 부하·용량 시험 |
| 환경 설정/의존성/실행 차단·제한 재시도·lease | 외부 설정·lock·수동 복구·명령 구현 | 아래 재시도/이관/로그의 독립 누락 |
| 구조화 건수·실패·duration_ms·운영 문서 | CLI/audit/Delta receipt 및 단계별 실행·배포 문서 구현 | 로그 순환/보관 한도와 중앙 수집의 실제 구성 |
| 수동 복구 후 NiFi 예약 실행 | 단발/일부 watch 명령과 종료 코드·운영 가이드 제공 | **NiFi flow 정의·수입/활성화·기한 도래 작업 발견 미구현** |

## 필드 제안서의 Gold·준비 상태 대조

| 제안 | 구현 위치·범위 | 보류/미구현 |
|---|---|---|
| G01 식별 | product_key/source ID·선택 Raw/evaluation·hash 보존 | 서비스 UUID는 MySQL 게시 때 기존 매핑 조회, 현재 임의 발급 안 함 |
| G02 표시/분류 | 확인 이름·제조사/브랜드/모델·서비스 category 및 매핑 상태 대응 | 담당의 유효 매핑·상품 상태 없으면 catalog_ready=false |
| G03 속성 | 원문/단위/옵션/대상 속성을 분리 보존, 확인된 제품 전체 축만 투영 | 부속품/범위/미확인 축을 전체 치수로 채우지 않음 |
| G04 대표가격 | 안정 Offer·KRW·일반가·확인 조건·옵션·시각/유효기간 검증 | 현재 원문에 없는 가격 조건은 담당 확인 필요. 미확인 목록 최저가로 대체하지 않음 |
| G05 대표 이미지 | 명시 선택/단일 bootstrap, 순서·URL·원천 버전·checksum·객체 키·추천 검수 대응 | 실제 active 연결 전환·asset/image UUID 결정은 MySQL 게시 후속 |
| G06 품질/추적/현재값 | 필드별 근거·시각, 최신 실패 기록과 이전 확인값, 동시각 충돌, 최초 시각 보존 | Gold의 최초 시각은 고정 입력 범위 기준. 서비스 전체 최초 시각 보존은 MySQL에서 대조 필요 |
| G07 임베딩 연결 | 정확한 상품 이미지 버전/checksum·벡터/모델/차원/전처리/인덱스·승인·Silver 확인 export 대조 | 벡터 생성·실물 파일 검사·검색 인덱스 활성화는 담당 범위 밖 |
| G08 비교 링크 | 구조·상품 대응·담당 확인/만료와 비교 준비 분리 | 현재 링크 검사 실행 자체는 링크 검증 담당 입력 |
| G09 현재 Offer | 안정 식별·조건·옵션·가격 종류별 현재 후보, 최종 링크 확인 | 식별 불명/조건 미확인은 생성 보류 |
| G10 가격 이력 | snapshot-v1+price-effect-v2와 원천/가격 시각·조건 근거 기록 | MySQL key 정책은 실제 서비스 합의 전 자동 이행 금지 |
| G11 일별 품질 | 품목/GLOBAL 별 상품·관측·평가·이미지 관계·distinct 파일 집계, 적용 속성 분모와 미제공 분리 | 전체 71개 분류 적용 규칙과 운영 전체 범위 성능 확인 필요 |
| R01~R04 준비 플래그 | catalog/recommendation/comparison/purchase를 독립 산출 | 후보 상태이며 서비스의 현재 권한·상태 검사를 대체하지 않음 |
| R05 MATCH/DIFFERENT/UNKNOWN | 결측/충돌·출처·옵션을 인계 | 요청별 추천 판정은 추천 담당 책임 |
| C01~C03 행 기준/키/상태 | 상품/속성/가격/이미지/링크/원천/평가 grain 분리, NULL·충돌·보류 보존 | 원천에 없는 값을 생성해 채우는 것이 충족 조건이 아님 |
| C04 Gold→MySQL | 고정 버전·선택 manifest·게시 차단 계약 제공 | **실제 게시기 개발이 남음** |

## 서버 검증을 제외해도 남은 구현

1. **MySQL 게시기:** 실제 ORM/DDL 확인, 허용 테이블/컬럼·UUID/FK/unique/nullability preflight, staging 검증, 상품별 publication guard/잠금·관련 검수/매핑 재확인, 기존 UUID 재사용, 고정 plan 재게시·응답 유실 복구가 필요하다. Gold를 구현했다고 이 요구를 완료로 표시하지 않는다. 승인 원장 편집이나 서비스 DB 자동 생성은 하지 않는다.
2. **실행 등록 어댑터:** Silver NORMALIZATION/Gold GOLD_BUILD는 담당자 등록 export를 검증한다. MySQL에 직접 실행을 생성·조회·상태 갱신하는 어댑터는 없다. export 제공 방식의 인수 또는 실제 DDL/API 어댑터 구현이 필요하다.
3. **내구성 있는 재시도 예약:** ingest는 제한된 즉시 재시도와 실패 기록·수동 재실행을 제공한다. Mongo에 next_retry_at 인덱스가 있다는 사실만으로 기한 도래 작업 발견/재발행기가 구현된 것은 아니다. Retry-After 기반 장기 대기·RETRY_WAIT/next_retry_at·RETRY_EXHAUSTED의 통합 상태 전환·서비스 job 투영은 추가 구현/인수가 필요하다. Silver/Gold 실패 로그도 그 자체가 예약 재시도 원장이 아니다.
4. **주기 운영 대조와 NiFi:** 캡처/watch/drain·명시 replay·고정 범위 재실행은 가능하지만 NiFi의 실제 flow, 기한 도래/새 입력/원장 대조의 예약 연결, 일별 전체 snapshot→접수→Raw→Bronze 대조 job은 아직 없다. 설치 후 단순히 명령 한 번 실행한 것을 운영 자동화 완료로 표시하지 않는다.
5. **환경 이관 패키지:** endpoint·인증·경로를 외부화했지만 ID·event·Mongo 운영 상태·공용 lease·checkpoint·Gold 고정 manifest를 새 환경으로 옮기고 목적지 fingerprint를 검증·전환하는 전용 이관 도구는 없다. VM/AWS에서 import가 된다는 것과 상태 보존 이관은 다르다. 기존 환경 정지/증거 보존/대조/전환 절차와 실제 시험이 필요하다.
6. **운영 보관/로그:** 실행별 JSON/오류·duration은 남지만 중앙 로그 수집·순환/보관량 제어·알림 규칙을 모두 제공한 상태는 아니다. 원본·객체·checkpoint 자동 삭제는 비활성 유지다.
7. **규모 확장:** 한 상품의 Silver 이력이 한도(기본 20,000행/32MiB)를 넘으면 명시 실패한다. 65,498개 전체/수년 이력의 분산 reduction·성능 보장은 아직 없다. 이는 데이터 유실 없이 중단하는 현재 지원 한계다.

## 담당 입력과 원본 한계

카테고리 매핑/제외·상품 active/supported·속성 적용 분모·가격 조건/안정 판매 ID 규칙·검수/권리·링크·임베딩 결과는 담당이 확정해 제공해야 한다. 미제공 시 품질 대기 또는 실행 차단한다. 이를 임의 생성하는 것은 누락 보완이 아니다.

기존 추출 자료의 HTTP 응답 원문 바이트/response_body_hash는 복원할 수 없다. LEGACY_EXTRACTED_ONLY와 NULL을 유지한다. 당시 응답 보존을 완료했다고 주장하지 않으며 크롤러 변경은 이번 범위 밖이다. 일부 날짜/축/옵션/판매 조건도 원문에 없으면 UNKNOWN이 정상이다.

dev 기준 상위 인프라 문서의 19대와 이전 작업 문서/실제 SSH의 20대 차이는 [현황](status.md)에 기록했다. 현재 코드에 VM 역할별 IP를 하드코딩하지 않았다. SSH 20대 hostname 조회 성공은 프로그램 설치/클러스터/파이프라인 인수를 의미하지 않는다.

## 검증 근거와 후속 순서

Gold 단위/계약/복구 테스트는 `tests/test_gold.py`, `test_gold_cli.py`, 실제 로컬 Spark/Delta는 `tests/smoke_gold_delta.py`로 남긴다. 저장 실제 Silver 3상품·9이미지 관계를 입력으로 사용하며 담당 입력이 없으면 QUALITY_WAIT/현재 Offer 0이 정상이다. 완전 준비 상태·확인 가격 경로는 명시한 fixture로 검증하며 실제 원문 확보율로 주장하지 않는다. 실행 기록은 Git 제외 `.runtime`에, 코드/재현 절차는 저장소에 보존한다.

서버 종단·장애 인수는 이번 요청에 따라 제외한다. 골드 실행/일별 집계까지 로컬 검증한 뒤에는 실제 DDL·업무 입력 계약을 먼저 확인하고 MySQL 게시기 → 내구성 재시도/전체 대조 → NiFi flow → 상태 보존 이관/운영 인수 순으로 진행한다. 이 누락을 단순히 ‘서버만 검증하면 완료’라고 정리하지 않는다.

최종 실행 결과: 전체 자동 회귀 **162개 통과 / 67.157초**(`.runtime/gold-unit-final.txt`). 실제 로컬 Delta(`.runtime/gold-delta-v2/report.json`)는 passed=true, 상품 3·이미지 관계 9·링크 27·평가 3, 반복 평가 3건 재사용이었다. 실제 CLI(`.runtime/gold-cli-v3.log`, cli-logs/runs/857b2bb8-48b1-428b-a949-16bfd3f97af9.json)는 COMPLETED, processed=3/reused=3/source_drained=true, 47,782ms이며 완료 export와 일별 품목/GLOBAL 집계를 검증했다. 집계는 상품 3/원천 관측 3/이미지 관계 9/distinct 파일 9/추천 준비 0을 구분했다. 집계일은 UTC as-of의 2026-10-16이며 문서 갱신일은 KST 2026-10-17이다. 외부 데이터 서비스 연결 0, lease는 로컬 대역이다. Windows JVM 종료의 임시 JAR 잠금 경고는 보존했으며 종료 코드와 데이터/receipt 검증은 통과했다.
