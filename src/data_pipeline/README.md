# 데이터 파이프라인

분리 브랜치: `feature/#13`, 최초 기준 `dev`의 `e35b2a3`(2026-10-16). 2026-10-18 최신 `origin/dev`의 `22168b8`을 병합해 `f85e59e`까지 푸시했다. [현재 제작 상태·기준 문서 차이](docs/status.md)를 먼저 확인한다. 크롤러 코드는 이 브랜치에 포함하지 않으며 완료된 export 파일로 인계받는다.

다나와 수집 결과를 인계받는 독립 Python 파이프라인이다. **FULL_EXPORT/LIVE_DELTA → 이미지·MongoDB → Kafka → Bronze/Silver/Gold → 고정 계획의 MySQL 게시** 실행 코드를 제공한다. 실제 서버에 대한 쓰기/장애 인수는 아직 수행하지 않았다.

범위는 저장된 수집 결과의 스냅샷·이미지 적재·MongoDB 원천·Kafka 전달·Spark Bronze/Silver/Gold·상품 영역 MySQL 게시·실패 복구까지다. 크롤링 실행·파서 변경, 인프라 구축, 서비스 API/운영 화면, 이용 승인·매핑 원장 편집, 임베딩 호출·추천·학습은 포함하지 않는다. 해당 담당의 버전이 고정된 결과를 입력으로 받아 검증한다.

- [통합 설계안](docs/integration_design.md): 현재 상태, 필드 매핑, 이미지 연결, 멱등 적재, 발행·복구, 자동화, 인수 검증, 개발 전 결정 사항.
- [보강 계약 v1.1](docs/design_amendment_v1_1.md): 현재 20대 인프라 기준, 상태 대조 재평가, LIVE 연결/순서, 게시 경쟁 보호, 가격 이력·검수 정책과 인수 우선순위. 새 세부 계약의 기준 문서.
- [데이터·복구 계약 v1](docs/data_contract_v1.md): 실제 raw/products/media에 맞춘 관측 키, Mongo 운영 원장, 이미지·서비스 ID, 시각·가격·결측 처리와 실행 조건.
- [상세 설계 대조표](docs/architecture_alignment.md): 03 데이터 모델·05 Kafka·01 인프라·12 직접 추천의 항목별 대응, 확장과 범위 구분.
- [수집 중 증분 인계 계약](docs/live_handoff_v1.md): commit 상한·마이크로배치·이미지 후속 인계·cursor/접수/발행 분리·재개와 책임 경계.
- [LIVE 실행·복구·NiFi 인수](docs/live_execution.md): capture/drain, S3 ready-last 전송, 용량 제한과 운영 경계.
- [이슈 #13 충족표](docs/issue13_acceptance.md): 코드·대역/로컬 검증·실제 서버 인수를 구분한 항목별 근거.
- [실제 서비스 인수 시험](docs/acceptance.md): 명시적 실행으로 Mongo/S3/Kafka/Delta를 대조하는 시험. 아직 미실행.
- [상위 파이프라인 설계](../../docs/design/02-data-pipeline.md): 프로젝트 공통 계약.
- [수집기](https://github.com/encore-ai-campus/mlo-01-p3-team3/blob/67e837db4114ef8955a12c0b466f6f4aba50bd8f/Commerce_crawler/README.md): 입력 파일을 생성하는 기존 시스템.

관측 버전·운영 기록·이미지 연결은 계약 v1에 구체화했다. 수집기의 요청·저장·재개 기능과 파이프라인의 적재·발행·복구 책임을 분리한다. **상품 Bronze/Silver/Gold Delta 저장·중복·품질·복구와 LIVE 인계의 로컬 검증을 수행했다.** 실제 Kafka/MongoDB/lake를 통한 종단 인수는 남아 있다. 2026-10-18 MySQL 게시기, 파일 기반 예약 재시도, Raw/원장 주기 대조, NiFi REST 생성·기동 제어, 상태 포장·복원 코드를 추가했다. 실제 서비스 DDL·담당 guard 인수, 실제 NiFi 배포·외부 DB/저장소 복원과 전체 종단 인수는 남아 있다. 전체 충족 판정은 아래 요구사항 대조를 따른다.

- [Bronze 실행·초기 전체 적재](docs/bronze_execution.md): 전체 JSON/이미지 인계 이후 available-now 처리, 지속 수신, checkpoint·Delta 복구.
- [Bronze 구현 계획·검증 기록](docs/bronze_implementation_plan.md).
- [Silver·Gold 제안서 전체 대조표](docs/silver_gold_field_mapping.md): 실제 입력과 46개 요구 항목의 반영 위치. 코드 구현률과 구분한다.
- [Silver 설계 v1](docs/silver_design_v1.md): 테이블·키·타입·품질·멱등 저장·재평가 기준안.
- [Silver 실행·서버 배포](docs/silver_execution.md): 고정 Bronze, 담당자 입력/실행 등록, 정규화·Delta·재평가·복구 명령과 현재 인수 경계.
- [Gold 실행·복구·배포](docs/gold_execution.md): 고정 Silver, 담당 입력·준비 상태·현재 Offer/이력, Delta 및 고정 export·일별 집계.
- [Gold 이후 요구사항·누락 대조](docs/requirements_gap_review.md): 구현/외부 입력/미구현/서버 미검증을 구분한 후속 목록.
- [서버 배포·인계 가이드](docs/deployment.md): 기능 추가와 함께 갱신하는 실행 위치·권한·초기 적재·재개·백업 절차.

전체 자동화는 아직 인수 전이다. 현재 queue는 RECEIVE/INGEST/PUBLISH/RECONCILE만 지원하며 Bronze/Silver/Gold 실행과 게시 계획 생성까지의 자동 의존성 연결·종단 대조는 남아 있다. 전체 회귀 192개와 로컬 MySQL/Delta 통과를 실제 서버 운영 완료로 해석하지 않는다.

## 2026-10-18 게시·운영·서버 인계 추가

- [MySQL 게시 실행·인수](docs/mysql_publication.md): 문서 기반 스키마 계약, 고정 Gold/staging, 기존 UUID·검수 보존, 트랜잭션·응답 유실 복구.
- [예약 재시도·NiFi](docs/operations_execution.md): 영속 파일 queue, 제한 재시도/Retry-After, 수동 복구, ready 발견·Raw 대조, NiFi REST 계획·생성·명시 기동.
- [상태 보존 이관](docs/state_migration.md): 정지/백업 증거, 파일 hash·ID 보존·복원, 경로 매핑과 실제 외부 저장소 인수 경계.
- [2026-10-18 검증 기록](docs/validation_20261018.md): 실제 로컬 MySQL·Delta와 NiFi 대역·이관 시험 및 미실행 서버 인수 구분.
- [문서 기반 참조 DDL](sql/publication_reference_v1.sql): 검토/격리 테스트용. 실제 서비스 DB에 자동 적용하지 않음.

진입점은 `run_publication.py`, `run_operations.py`다. 게시 의존성은 `requirements-publication.lock.txt`로 별도 설치한다. 원래 크롤러와 Mongo 설치 도구는 변경하지 않았다. 코드 릴리스는 `python run_operations.py release --source . --output .runtime/release-001`로 생성하고 목적지에서 `release-verify`로 hash를 확인한다. 로컬 수집 데이터·가상환경·비밀 설정은 포함하지 않는다.

## 설치와 실행

PowerShell에서 크롤러와 별도 가상환경으로 설치한다. 패키지는 Python 3.11~3.14를 선언하며 실제 검증 환경은 Windows/Python 3.13.14다. VM/Linux/EC2에서는 가상환경을 새로 생성한다. Windows `.venv`를 복사하지 않는다.

```powershell
cd 'C:\Users\Playdata\Documents\encore_MLops\3st_project\mlo-01-p3-team3\src\data_pipeline'
py -3.13 -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.lock.txt
.\.venv\Scripts\python.exe -m pip install --no-deps -e .
.\.venv\Scripts\python.exe run_pipeline.py --help
```

정지/완료된 수집기 export를 새 스냅샷 폴더로 복사·검증하고 로컬 대역으로 처리한다. 원본 사이트 요청이나 이미지 URL 재다운로드는 하지 않는다.

```powershell
$export = 'C:\path\to\completed-export'
$snapshot = 'C:\path\to\pipeline-snapshots\batch-001'
$state = 'C:\path\to\pipeline-state\batch-001-rehearsal'
$python = '.\.venv\Scripts\python.exe'

& $python run_pipeline.py prepare --export-dir $export --media-manifest "$export\media\products_with_local_images.jsonl" --output $snapshot
& $python run_pipeline.py plan --snapshot $snapshot
& $python run_pipeline.py rehearse --snapshot $snapshot --state-dir $state
# 같은 명령으로 재개/중복 실행 검증
& $python run_pipeline.py rehearse --snapshot $snapshot --state-dir $state
```

다운로드 결과가 없다면 `--media-manifest`를 생략할 수 있다. 이미지 결측을 보존하며 partial 판정이 될 수 있다. `plan`은 검증만 하고 네트워크·파일 쓰기를 하지 않는다. `rehearse`는 파일 기반 대역만 사용한다. 자세한 출력·복구·인수 기록은 [실행 문서](docs/execution.md)를 따른다.

## 실제 환경

`config/environment.example.json`을 별도 `*.local.json`으로 복사하고 환경 ID·DB명·이미지 버킷을 확정한다. 예시 값으로는 실제 연결을 차단한다. 비밀정보는 Git/설정 JSON에 넣지 않고 배포 환경변수로 주입한다.

- `PIPELINE_MONGO_URI`: `rs-products` Replica Set 접속 URI와 실제 TLS/인증 설정.
- `PIPELINE_S3_ENDPOINT`: 승인된 HTTPS S3 호환 endpoint. AWS S3 기본 endpoint는 문자열 `aws`.
- `PIPELINE_KAFKA_CONFIG`: `bootstrap.servers` 및 TLS/SASL 설정을 포함하는 JSON 문자열. `acks=all`, idempotence를 강제한다.
- 객체 저장 인증/리전: boto3 표준 credential/region chain. EC2 역할을 사용할 수 있다.

```powershell
# 환경변수가 주입된 터미널에서 실행
& $python run_pipeline.py preflight --config config/environment.local.json
& $python run_pipeline.py ingest --snapshot $snapshot --state-dir 'C:\path\to\pipeline-state\batch-001-live' --config config/environment.local.json
# 중단·실패 뒤 동일 snapshot/state/config로 같은 ingest 명령 재실행
```

`preflight`는 읽기 전용이다. 실제 쓰기·장애 인수를 대체하지 않는다. `ingest`는 인덱스·접수 상태·원천·객체·이벤트를 실제 생성한다. rehearsal 상태를 live로 재사용하거나 같은 상태 폴더를 다른 대상에 연결할 수 없다. 실제 이미지 서버 역할은 인프라 담당이 확정하며 코드에서 임의 지정하지 않는다.

정상 재개는 원장의 성공 이미지 슬롯·원천·이벤트를 재사용한다. 외부에서 유실된 원천까지 다시 대조/복원할 때는 같은 ingest 명령에 `--repair-raw`를 추가한다. `--replay-events`는 확인된 이벤트를 최초 ID·시각·본문 그대로 다시 발행하는 별도 기능이다. 이미지 객체를 매번 다시 읽어 존재를 확인하는 전수 무결성 검사와 정상 재개를 혼동하지 않는다.

## 서버 배포 순서

상세 절차와 단계별 실행 가능 여부는 [서버 배포·인계 가이드](docs/deployment.md)를 따른다. 현재 원천 적재·Bronze·Silver·Gold 실행 코드가 있다. 새 단계가 구현되면 실제 명령·권한·검증·재개 항목을 이 절과 가이드에 함께 반영한다.

인프라 프로그램 설치는 서버 담당자가 수행하고, 아래 절차는 준비된 서버에 파이프라인을 배치하는 절차다. 실제 접속 주소·비밀정보는 Git 밖에서 관리한다.

| 작업 | 초기 실행 위치 | 서버에서 필요한 접근 |
|---|---|---|
| 수집기 | 기존 수집 PC. 이후 NiFi 실행 서버로 이전 가능 | 다나와, 로컬 수집 디스크 |
| prepare/ingest | 완료 export와 이미지에 접근할 수 있는 PC 또는 Python 실행 서버 | MongoDB 3멤버·이미지 저장소·Kafka |
| Bronze Driver | Spark Master/Driver 서버에서 client 모드 제출 | MongoDB 읽기·Kafka 읽기·lake 읽기/쓰기·Spark 클러스터 |
| Spark Executor | Spark Workers | Kafka·lake·Driver 통신 |
| Silver Driver | Spark Driver 서버. Bronze와 별도 작업/잠금/checkpoint | 고정 Bronze·lake·Mongo 이미지 상태 읽기·공용 ops lease |

1. 이 브랜치 코드를 배포하고 해당 OS의 가상환경을 새로 만든다. PC의 `.venv`를 복사하지 않는다. 원천 적재는 `requirements.lock.txt`, Bronze는 `requirements-bronze.lock.txt`를 설치한 뒤 `python -m pip install --no-deps -e .`를 실행한다. 서버 Python·Java·Spark·Delta·Hadoop 조합을 인프라 담당과 맞춘다.
2. 원천 적재 서버에 `environment.local.json`과 배포 비밀을 구성한다. 수집 PC에 데이터가 있다면 그 PC에서 우선 실행할 수 있다. 데이터를 옮길 때는 완료 export의 JSON과 media 폴더 상대 경로를 함께 보존한다.
3. `preflight` 후 소량 `prepare → ingest`를 실행한다. 이미지 바이트/hash·Mongo 원천·Kafka 메시지를 대조한다. 실제 서버 검증을 통과한 뒤 전체 export를 적재한다.
4. Bronze Driver에 `bronze.local.json`, 실제 Kafka cluster ID, Mongo 읽기 계정, Kafka Java client 설정, lake S3A 설정을 주입한다. 이미지 버킷과 Delta 버킷을 구분한다.
5. [Bronze 제출 예시](docs/bronze_execution.md#설치와-실행)에 따라 Driver 서버에서 `spark-submit --deploy-mode client`로 실행한다. 초기에는 `--available-now`, 이후 지속 수신은 같은 checkpoint에서 해당 옵션만 제거한다. 동시에 두 Driver를 시작하지 않는다.
6. 원천·이벤트·격리·이미지 관계 건수와 hash를 대조한다. 실패 시 원천 적재는 동일 snapshot/state/config, Bronze는 동일 config/checkpoint로 재개한다. checkpoint를 삭제하거나 같은 상태를 다른 환경에 연결하지 않는다.
7. [LIVE 실행 문서](docs/live_execution.md)의 capture/drain으로 수집 중 인계를 시험한다. 실제 종단 인수 후 NiFi가 검증된 단발 명령을 호출하도록 일정·로그 수집을 연결한다. 이 저장소가 NiFi 실행 작업을 자동 설치한 상태는 아니다.
8. [Silver 실행 문서](docs/silver_execution.md)에 따라 `config/silver.local.json`, 담당자 snapshot, NORMALIZATION 등록 증거, `SILVER_SPARK_OPTIONS`/`SILVER_LEASE_MONGO_URI`를 준비한다. `run_silver.py --bronze-version N --inputs ... --registration ... --state-dir ... --max-observations 3`으로 인수한다. 매핑/검수 미제공은 품질 대기이며 Gold/MySQL로 자동 게시하지 않는다.

원천 적재 감사/등록 사본, MongoDB, 이미지·Delta 저장소와 checkpoint를 각 보관 정책으로 백업한다. 현재 로컬 테스트 결과는 실제 서버 구축·연동 완료를 의미하지 않는다.
