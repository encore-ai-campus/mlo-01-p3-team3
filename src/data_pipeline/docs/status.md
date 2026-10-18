# 파이프라인 제작 현황

2026-10-18 갱신. 기존 feature/#13 전용 작업 폴더에서 문서 계약 기반 MySQL 게시·운영 기능을 추가했다. 실제 서버 설치/적재나 Git 커밋·푸시는 이번 작업에서 하지 않았다.

## 2026-10-18 추가 구현

| 영역 | 구현 | 실제 서버 인수 |
|---|---|---|
| MySQL 게시 | 고정 Gold 계획·문서 스키마 preflight·staging·UUID/FK·시각 역전 방지·트랜잭션·응답 유실 복구 | 실제 서비스 DDL·guard·권한/TLS·VM Primary 확인 필요 |
| 예약 재시도 | 단일 소유 파일 queue, due 조회·Retry-After·한도/격리·token 복구·수동 한도 재개 | 지정 호스트/디스크/로그 운영 인수 필요 |
| 자동 발견/대조 | 원격 ready 페이지·게시 ready→queue, 고정 snapshot과 Mongo Raw/접수/발행 대조 | 실제 S3/Mongo 연결·Bronze 전체 대조는 별도 |
| NiFi | REST 버전/bundle 대조·계획/STOPPED 생성·명시 시작/정지 | 실제 버전 미제공. 대역 검증이며 실제 NiFi에 적용하지 않음 |
| 상태 이관 | 정지/외부 백업 증거·파일 hash/ID 보존 pack/verify/restore·task 경로 별칭 | DB/객체/Kafka/Delta 실제 백업·복원과 목적지 preflight는 별도 |
| 서버 전달 | 코드/문서/예시/고정 의존성만 포함한 source release와 hash 검증 | 대상 OS에서 새 venv·설정·권한 준비 필요 |

실행 기준은 [MySQL 게시](mysql_publication.md), [운영 자동화](operations_execution.md), [상태 이관](state_migration.md)이다. 구현을 실제 DDL 승인/서비스 추천 승인/서버 종단 인수로 간주하지 않는다. 새 이미지 연결은 inactive로 게시하며 승인·활성화는 담당 원장을 따른다.

## 2026-10-17 구현 당시 기록

2026-10-17 갱신. Issue #13 / feature/#13. 분리 기준은 origin/dev의 e35b2a3이다. 기존 feature/#6의 커밋이나 스테이징은 포함하지 않았다.

## 구현·검증 상태

| 영역 | 현재 상태 |
|---|---|
| FULL_EXPORT | 원천/정규화/media JSONL·실제 이미지 검증, 불변 스냅샷 봉인 구현 |
| 이미지·원천 적재 | S3 조건부 내용 주소 업로드, MongoDB 접수·불변 원천·lease·복구 구현 |
| Kafka 발행 | Mongo 저장 확인 후 고정 event ID/본문 발행·재시도·명시 replay 구현 |
| Bronze | Spark Kafka source → primary/majority 원천 검증 → Delta insert-only MERGE, receipt·격리·checkpoint 구현 |
| 초기 전체 적재 | prepare/ingest 이후 Bronze available-now로 적체 처리. 이후 같은 checkpoint로 증분 처리 |
| 테스트 | 최종 전체 162개 통과(67.157초): 기존 145개 + Gold 17개. `.runtime/gold-unit-final.txt`. 저장 실제 journal 5건·반복 drain, Bronze 로컬 Delta 기록과 Silver 실제 표본/CLI 결과를 각각 보존 |
| LIVE | commit 이하 journal 쌍, seq/hash/정체성·cursor/chain·media 후속/모호성·quota·drain receipt·S3 chunk/ready-last 코드 및 로컬 시험 구현 |
| 복구 보완 | 확인된 이미지 슬롯·원천·이벤트 요청 생략, 명시 --repair-raw, 새 관측 불변 저장 회귀 시험 |
| 중복 실행 | Bronze Mongo ops claim·heartbeat·실패 시 자동 takeover 금지 구현; 실제 다중 호스트 인수 미수행 |
| Silver | 상품·속성·가격·이미지·링크·품질·확인된 Offer·평가 receipt의 8개 Delta, 고정 입력/품질/재평가·복구·공유 잠금·CLI 구현. 저장 표본 3상품·9이미지 관계의 실제 로컬 Delta 및 반복 평가 확인 |
| Gold | 고정 Silver receipt/버전 → 상품·현재 Offer·이력·이미지·링크·품질·일별 기여·평가의 8 Delta, 고정 export manifest·품목/GLOBAL 집계·CLI 구현. MySQL 게시·승인·임베딩 생성은 수행하지 않음 |
| 자동화·서비스 | capture/drain 단발·watch 및 운영 문서 구현. 실제 NiFi flow/일정 활성화·MySQL 게시 미구현 |
| 실제 서버 | Mongo/Kafka/S3/Spark 클러스터 종단 인수 미수행 |

진행 중 산출물: [제안서 46항목 대조표](silver_gold_field_mapping.md), [Silver 설계 v1](silver_design_v1.md), [서버 배포·인계 가이드](deployment.md). 설계 문서와 실행 가능 코드를 구분하며, 기능이 추가될 때 README·배포 가이드·이 현황을 함께 갱신한다.

Silver 실행은 [실행·배포 가이드](silver_execution.md)를 따른다. 운영은 담당자의 실제 NORMALIZATION 등록 export를 입력으로 요구한다. 현재 등록 어댑터는 export 검증/보관이며 MySQL에 직접 생성·실시간 확인하는 기능은 아니다. 실제 Mongo/Spark VM/S3A 연계, 등록 담당 인수, 대규모 부하는 아직 미수행이다.

현재 [이슈 충족표](issue13_acceptance.md), [LIVE 실행·복구](live_execution.md), [실제 서비스 인수 시험](acceptance.md)을 함께 확인한다. SSH 20대 hostname 조회는 성공했지만 데이터 서비스 종단 시험은 NOT_RUN이며 로컬 테스트 결과로 대체하지 않았다.

테스트 코드는 함께 배포하지만 로컬 보관 데이터·이미지·실행 보고서·가상환경·비밀 설정은 Git에 올리지 않는다. 실제 보관 데이터 smoke는 해당 입력을 제공해야 재현 가능하다. 기본 단위 테스트는 저장소에 포함된 fixture로 실행한다. 서버 배포·초기 적재·재개 절차는 [README](../README.md#서버-배포-순서), Bronze 세부는 [실행 문서](bronze_execution.md)를 따른다.

## 기준 문서의 차이

분리된 dev의 상위 인프라 문서는 19대/역할별 논리 서버명이며, 이전 작업 폴더에서 검토한 상위 설계안은 모니터링을 포함한 20대/pj3-* 구성이다. 기존 pipeline 설계 문서의 20대·상위 대조·해시 근거는 **그 당시 검토한 작업 폴더 설계안**의 기록이다. dev의 현재 상위 문서가 이미 같은 내용으로 갱신됐다는 의미가 아니다.

이번 커밋은 파이프라인 폴더만 추가하고 상위 설계·데모·크롤러는 변경하지 않는다. 추천 알고리즘·서비스 스키마 차이는 이번 원천/Bronze 구현 범위에 끌어오지 않는다. 실제 배포 전 서버 역할·버전·endpoint를 확정하고 실제 인프라 인수를 수행해야 한다. Silver·Gold 개발 때 최신 dev의 서비스/필드 계약과 제안서의 스키마·상태·업무 키를 다시 대조한다.

현재 코드에는 VM IP/호스트 역할을 하드코딩하지 않았다. Mongo rs-products 3멤버, Kafka 토픽·실제 cluster ID, 이미지/lake endpoint와 권한은 배포 설정으로 연결한다.

크롤러는 dev에 아직 없으므로 문서의 크롤러 근거 링크는 최초 구현을 검증한 commit 67e837db4114ef8955a12c0b466f6f4aba50bd8f에 고정했다. 크롤러를 import하는 런타임 의존성은 없고 schema 1.2 완료 export를 읽는다.

Gold 실행은 [가이드](gold_execution.md), 실제 요구사항 대비 남은 개발은 [누락 대조](requirements_gap_review.md)를 따른다. 서버 검증 외에도 MySQL 게시기·실행 등록 어댑터·내구성 재시도 예약·전체 대조 job·NiFi flow·이관/운영 인수가 남아 있다. 기존 LIVE/Silver 작업은 98ae0ae으로 커밋했고 이후 Gold 작업은 별도 변경이다.

최종 Gold 로컬 증적: `.runtime/gold-delta-v2/report.json` passed=true, Spark 4.0.1, 3상품·9이미지 관계·27링크·3평가, 재실행 3건 재사용, 외부 데이터 서비스 연결 0. `.runtime/gold-cli-v3.log` 및 같은 출력의 cli-logs/runs에서 COMPLETED·source_drained=true·고정 export/품목/GLOBAL 집계를 확인했다. 3건 모두 QUALITY_WAIT, current_offers/offer_history는 0으로 실제 안정 판매 근거 부족을 유지했다.
