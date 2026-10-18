# 2026-10-18 게시·자동화·이관 검증 기록

요청 범위는 Gold→MySQL 게시, NiFi 연계, 내구성 예약 재시도, 환경 상태 보존 이관이다. 실제 서비스 DDL과 NiFi 버전은 제공되지 않아 사용자의 지시에 따라 문서 기반 계약으로 구현했다. 실행 가능한 코드/검토용 스키마/로컬 실물 시험과 실제 서버 인수를 구분한다.

## 구현 산출물

| 영역 | 코드 | 문서 |
|---|---|---|
| 고정 Gold·MySQL | `product_pipeline/publication/`, `run_publication.py` | [게시 가이드](mysql_publication.md) |
| 일정·재시도·누락 대조 | `operations/queue.py`, `dispatch.py`, `cli.py` | [운영 가이드](operations_execution.md) |
| NiFi | `operations/nifi.py` | REST 버전/bundle 대조·STOPPED 생성·명시 기동 |
| 상태 이관 | `operations/migration.py` | [상태 이관](state_migration.md) |
| 코드 전달 | `operations/release.py` | [배포 순서](deployment.md) |

Silver 버전을 1.0.3으로 올려 이미지의 실제 payload hash와 원천 수집 시각을 evidence JSON에 포함했다. 기존 Delta를 수정하거나 source wrapper hash를 payload hash로 바꾸지 않는다. 추적 근거 없는 과거 Gold 게시는 차단한다. 크롤러·원천 수집 데이터·현재 수집 프로세스·MongoDB 설치 스크립트는 이번 작업에서 변경하지 않았다.

## 실제 로컬 시험

전체 자동 회귀 시험은 **192개, 74.816초, OK**였다. 로그는 Git 제외 `.runtime/pipeline-publication-operations-regression-release.txt`에 보존했다. 아래의 실제 MySQL/Delta 시험은 이 자동 시험 수와 별도로 실행했다. NiFi 검증은 실제 서버 연결이 아닌 REST 대역이다.

전달용 코드 묶음은 `run_operations.py release`로 만들고 `release-verify`로 파일 체크섬을 검증한다. 데이터·가상환경·로컬 설정·비밀정보는 포함하지 않는다. 서버에서는 가상환경을 새로 만들고 [배포 가이드](deployment.md)의 설정·preflight·소량 인수 순서를 따른다.

### MySQL 8.0.46

`tests/smoke_publication_mysql.py`가 별도 data directory와 loopback 임의 포트의 시험 mysqld를 시작했다. 기존 Windows MySQL80 서비스는 그대로 Running이며 그 데이터에 접속하지 않았다. 시험 child만 정상 종료했다. 설정/상품은 명시적인 fixture이며 실제 서비스 승인이 아니다.

최종 보고서: Git 제외 `.runtime/mysql-publication-final/report.json`.

- 참조 스키마의 UUID CHAR(32), 업무 UNIQUE, FK, 타입·NULL·InnoDB·Primary 상태 검사 통과.
- 서비스 쓰기 중 실패 주입 시 전체 롤백, 먼저 저장한 staging 유지.
- 같은 계획 재실행 시 product/asset/image/offer/publication UUID 유지.
- 계획 이후 담당 guard 변경 시 서비스 commit 차단.
- 실제 COMMIT 후 응답 유실을 주입한 뒤 동일 receipt로 재실행·재사용.
- 과거 이름·가격이 최신 확인값을 덮어쓰지 않음.
- 확인된 현재 Offer 1건과 조건 포함 가격 이력 2건 보존.
- 상품 1, asset 1, image 연결 2, Offer 1, 가격 이력 2, publication/staging 각각 4건.
- 새 이미지 연결 inactive 유지. 검수/활성화 자동 결정 없음.

### Spark 4.0.1·Delta

`tests/smoke_publication_delta.py`로 실제 로컬 Gold 테이블·완료 receipt·export를 만들고 게시 계획 1건을 생성했다. 나중에 추가한 Delta 행은 고정 버전 입력에 섞이지 않았다. 최종 계약으로 재생성한 보고서는 `.runtime/publication-delta-final/report.json`이다. 외부 데이터 서비스 연결 0, 최종 reader는 읽기 전용이고 생성 단계 lease만 로컬 대역이다.

MySQL과 Delta의 최종 계약 hash는 `5d7758a814f5711fc0910c997c7e4f5ddbe2a23a91d583dd79b5b59f9771d027`로 일치한다. 실행 후 Windows JVM에서 임시 JAR 삭제 잠금 경고가 있었으나 데이터/계획 검증과 프로세스 종료 코드는 통과했다. 로그는 보존했다.

### 예약·NiFi·이관

- 실제 파일 queue로 중복 등록/완료 재사용, 기한 대기, 429 Retry-After, 한도 소진·제한 재시도, 강제 종료·정지 증거/token 복구, 실행 중 중복 worker 차단을 시험했다.
- 저장된 sealed snapshot을 예약 INGEST로 연결한 file rehearsal을 시험했다.
- 완료된 ingest 파일 원장을 포장/다른 폴더 복원한 뒤 raw ID·event·객체를 새로 생성하지 않고 재개했다.
- 활성 task/실제 lock·잘못된 외부 복원 증거·파일 hash 변경·비밀 파일의 일반 bundle 포함을 차단했다. 과거 등록 archive에 보존된 lease 사본은 현재 lease로 잘못 판정하지 않고 그대로 보존했다.
- NiFi는 REST 대역으로 버전/processor bundle·기존 구성 충돌·정지 생성·검증 상태/revision에 따른 명시 기동을 시험했다. 실제 NiFi는 설치·변경하지 않았다.
- source release에서 데이터/가상환경/로컬 설정을 제외하고 파일 변조를 탐지하는 시험을 수행했다.

## 미실행·인수 조건

실제 MySQL DDL/권한/TLS/Primary 전환, 담당 guard 공급/기존 상품 baseline, 이미지 검수·활성화, 실제 NiFi 버전과 restricted processor 권한·예약 실행, VM↔AWS 외부 DB/객체/Kafka/Delta 백업·복원, 전체 65,498상품 성능/용량 시험은 미실행이다.

파일 queue는 지정 실행 호스트 한 대의 소유다. 다중 호스트 분산 scheduler나 NiFi Primary 변경 때의 무조건적인 로컬 queue 자동 인계는 제공하지 않는다. task 경로 별칭은 지원하지만 Windows 로컬 Spark checkpoint 내부 URI를 Linux 운영 checkpoint로 자동 변환하지 않는다. 서비스의 pipeline_runs 직접 등록 어댑터와 전체 Bronze/서비스 원장 대조는 기존 별도 후속 범위로 남는다.

따라서 코드 전달 묶음은 서버 인수 준비 산출물이다. 실제 서비스 운영 완료나 상위 설계의 모든 인수 조건 완료를 의미하지 않는다. 구현 시점에는 Git 작업을 수행하지 않았고, 이후 구현을 `460f6df`로 커밋했다. 최신 `origin/dev`(`22168b8`) 병합 커밋 `f85e59e`을 `feature/#13`에 푸시하고 원격 hash 일치를 확인했다. 병합 후 회귀 시험도 **192개/74.540초/OK**였으며 `.runtime/post-dev-merge-regression-20261018.txt`에 보존했다. 이 문서 후속 갱신 자체의 커밋·푸시를 뜻하지 않는다.

## 후속 FLOW 자동 연결·종단 대조 시험

2026-10-18, 문서 갱신 커밋 cdc4447 이후 후속 구현이다. 기존 CLI를 7단계 FLOW로 연결하고 고정 입력/hash·버전·receipt chain·실패 재개·정지 불명확 latch·fresh audit·저장소 backpressure를 추가했다. Silver discovery는 입력/normalizer/flow scope로 분리했다. 이번 후속 코드의 커밋·푸시는 아직 수행하지 않았다.

- 실제 로컬 Spark 4.0.1/Delta: `.runtime/flow-delta-v1/report.json`, passed=true. 합성 원천 1건·이미지 관계 2건을 FileLedger/Objects/Publisher→Bronze→Silver→Gold→품질 보류로 연결했다. Gold commit 후 flow receipt 전 실패를 주입하고 앞 3단계 재사용·실패 단계 재개·최종 7단계 재사용을 확인했다. MySQL 쓰기 0은 미제공 업무 승인에 대한 정상 보류다. 명시 시험 adapter/로컬 lease 대역이며 실제 Kafka/Mongo/S3 또는 production worker 전체 CLI 시험이 아니다.
- 운영 `live_audit` 함수 자체: `.runtime/flow-audit-v1/report.json`, passed=true. 실제 저장 Delta와 명시 Mongo/S3 파일 대역으로 원천/이벤트/Silver/Gold/보류 대응·이미지 객체 유실 탐지를 확인했다. 실제 외부 서비스 접속 0.
- 격리 실제 MySQL 8.0.46: `.runtime/flow-mysql-final/report.json`, passed=true. 기존 게시 장애 시험에 더해 건수 유지 상태의 잘못된 이미지 URL·가격 이력 조건 변조·Offer 부모 관계 대조를 확인했다. 기존 Windows MySQL 서비스·VM 데이터 미접속.
- 65,498건 합성 정렬 스트림: `.runtime/flow-stream-benchmark.json`, 0.795초·Python traced peak 5,553bytes. 비교 함수만의 측정이며 원본 JSON 파싱·정렬·Spark/MySQL·전체 ETL 처리량/필요 용량을 측정한 결과가 아니다.
- Windows JVM 종료 시 기존 임시 JAR 삭제 잠금 경고가 남았지만 Delta 검증·프로세스 종료 코드 0 및 passed=true 보고서를 확인했다.

실제 서비스 DDL/등록/guard, 실제 worker 전체 CLI/NiFi·Linux/VM·DB/객체/Kafka/Delta 종단·장애·전체 실데이터 규모 인수는 남아 있다. [FLOW 실행 가이드](flow_execution.md)의 지원 범위와 경로 이관 한계를 따른다.

후속 최종 전체 회귀 시험: **218개 / 104.135초 / OK** (`.runtime/flow-regression-final.txt`). 새 FLOW 관련 26개를 포함한다. 문서/실행 명령의 설정 예시·참조 링크와 변경 범위를 별도로 점검했다.
