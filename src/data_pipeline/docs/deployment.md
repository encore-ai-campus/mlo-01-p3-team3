# 서버 배포·인계 가이드

상품 파이프라인을 준비된 서버에 배치하는 절차다. 인프라 설치·서비스 승인·추천 연동은 다른 담당자 책임이다. 현재 확인된 결과와 최종 release/venv/SSH 경로는 [실서버 검증](service_validation_20261018.md)을 따른다. 운영 입력이 없는 기술 시험을 전체 운영 인수로 취급하지 않는다.

## 1. 현재 업무 실행과 인계

2026-10-21 한 배치가 실제 업무 DB 게시·활성화까지 완료했다. 최신 수치·남은 인수는 [실서버 검증](service_validation_20261018.md)에만 관리한다. 전체 65,498상품 완료·NiFi 운영 일정 인수·환경 이전 완료와 구분한다.

현재 작업 루트는 Spark Master의 `/var/lib/pj3/product-pipeline/spark/production-pending-20261019`이며, 공용 설정/venv 기준은 상위 `/var/lib/pj3/product-pipeline/spark`다. `run_server_pipeline.py`는 기존 Bastion Ansible 제어 모듈을 호출한다. 다른 PC에서도 Bastion alias·키·권한과 동일 저장소가 준비되면 실행할 수 있다. SSH 비밀번호만으로 제어 모듈·서버 보호 환경이 자동 생성되는 것은 아니다.

배포 시 `product_pipeline/`을 작업 루트 `source/product_pipeline/`에 반영하고, `docs/server_setup/production_pending_work.py`를 작업 루트의 `production_work.py`, `production_pending_manage.py`를 기존 `production_manage.py`로 배치한다. `production_continuous_work.py`는 `continuous_work.py`, `production_downstream_work.py`는 원래 파일명 그대로 배포한다. `production_window_work.py`, `production_gold_inputs.py`, `production_activation.py`, `recover_kafka_retention.py` 등 이 실행기들이 import/호출하는 동반 모듈도 같은 작업 루트에 배포한다. Bastion의 기존 Ansible `production_manage` 모듈도 관리 소스와 대조한다. 예전 release 하나만 복사하고 작업 루트의 신규 모듈을 빠뜨리지 않는다.

새 배포는 실행 중 Driver/writer가 없는 단계 경계에서 기존 파일을 백업하고 SHA256·배포 시각·테스트 결과를 기록한다. 보호 설정·원천·Delta·checkpoint·window selection·receipt·등록 ID를 덮어쓰지 않는다. 별도 PC의 `.runtime/` 도구는 인계 계약이 아니며 Git의 재사용 도구와 기존 서버 실행 환경을 사용한다. 계정 권한·TLS CA·런타임 프로필·원격 제어 설치는 아래 절을 따른다.

```powershell
python run_server_pipeline.py status
python run_server_pipeline.py preflight
python run_server_pipeline.py downstream
python run_server_pipeline.py flow
```

두 시작 명령을 동시에 실행하지 않는다. `pause-after-window.json`이 남아 있으면 한 배치 뒤 정지한다. Kafka offset 범위 오류는 [고정 이벤트 재발행·체크포인트 세대 복구](kafka_retention_recovery.md), 공용 JAR·작업 디렉터리 정리는 [Garage 실행 프로필](garage_v241_profile.md), 내부 검수 정책은 [검수 절차](review_workflow.md)를 따른다. 새 설정으로 기존 커서를 지우거나 성공 상태를 직접 편집하지 않는다.

## 현재 VM의 보호 설정

원천 DB는 승인된 `pj3_products_raw`, 공용 잠금 DB는 `pipeline_ops`다. `pipeline_ingest`는 원천 DB readWrite와 admin clusterMonitor, `pipeline_spark`는 원천 DB read와 ops DB readWrite 및 clusterMonitor다. 현재 DB 단위 역할이며 컬렉션별 최소 권한 커스텀 역할은 아니다. `bronze_writer_leases`/`pipeline_writer_leases`에 TTL을 두지 않는다.

Mongo01의 계정 원본은 `/etc/pj3-secrets/product-pipeline/mongodb-accounts.json`(root 0600)이다. 서비스별 설정은 `/var/lib/pj3/product-pipeline/{nifi,spark}/config/`(0700), 파일은 서비스 소유 0600이다.

- `mongo-ca.crt`: TLS CA. 내부 Replica Set hostname을 각 환경 DNS/hosts로 해석할 수 있어야 한다.
- `mongo-accounts.json`: NiFi는 ingest, Master는 ingest와 spark. 관리자 자격을 애플리케이션에 전달하지 않는다.
- `kafka-python.json`: 역할별 P12/CA 참조. Java Spark Kafka 옵션과 형식이 다르다.

설정 파일 존재만으로 환경변수나 원격 실행이 자동 주입되지는 않는다. 운영 계정으로 읽기 전용 preflight를 통과한 뒤 명시적으로 소량 쓰기를 인수한다. 보호 파일의 내용을 출력하거나 Git에 복사하지 않는다. S3의 계정/CA/JAR 조건은 [Garage 프로필](garage_v241_profile.md)을 따른다.

## 2. 실행 위치와 데이터

현재 VM에서는 서비스 계정별 불변 release와 전용 venv를 사용한다. 새 환경은 아래 설치 절차로 준비하고 Garage 설정은 전용 프로필을 따른다. 기존 검증 큐/launcher를 운영 것으로 전환하지 않는다.

| 역할 | 실행 위치 | 보존할 로컬 상태 | 접속 대상 |
|---|---|---|---|
| 크롤러 | 기존 PC, 이후 승인된 수집/Python 서버 | 수집 checkpoint·JSON·media | 다나와 |
| 원천 적재 | 완료 export를 읽을 수 있는 PC/Python 서버 | sealed snapshot·audit·최초 등록 사본·run report | MongoDB·이미지 저장소·Kafka |
| Bronze Driver | Spark Driver 서버에서 client 모드 제출 | 로그·한도 있는 scratch | Kafka·MongoDB·lake·Master/Workers |
| Spark Executor | Spark Workers | shuffle/scratch·worker 로그 | Kafka·lake·Driver |
| Silver Driver | 기존 Spark 역할의 별도 Silver 작업 | 로그·한도 있는 batch buffer | Bronze·Mongo 운영 상태·고정 의존 입력·Silver |
| NiFi | 일정 관리 서버 | 실행 이력/제어 상태 | 검증된 원천 적재·Spark 제출 명령 |

Bronze·Silver는 서로 다른 테이블/작업/checkpoint를 사용한다. **한 Silver 테이블의 신규 입력과 재평가는 하나의 Silver Driver가 소유**한다. Driver 메모리와 Master 데몬·다른 작업의 자원 경쟁은 배포 전에 측정한다.

Worker가 Driver의 C: 경로를 읽을 수 있다고 가정하지 않는다. 실제 공용 테이블·checkpoint는 lake S3A 경로로 제공하고 Driver/Worker 모두 접근한다. 수집 PC에서 적재하면 원본의 VM 선복사가 필요 없다. 완료 export는 JSON과 media 상대 경로를 함께 보존하고 prepare로 재검증한다. 실행 중 파일은 [LIVE capture/ready 인계](live_execution.md)를 사용하며, 변경 중인 FULL_EXPORT를 직접 복사해 적재하지 않는다.

## 3. 설치·권한·설정

1. 배포 커밋을 고정하고 코드 hash·의존성·설정 버전을 기록한다. 미커밋 설계를 서버의 기능으로 취급하지 않는다.
2. 대상 OS에서 가상환경을 새로 만든다. 원천 적재는 requirements.lock.txt, 현재 VM Spark는 requirements-spark-server.txt를 설치한 뒤 해당 디렉터리에서 `python -m pip install --no-deps -e .`를 실행한다. 게시 역할은 requirements-publication.lock.txt를 추가한다. 로컬 Spark 4.0.1 전용 lock/extras를 현재 VM에 적용하지 않는다.
3. Python·Java·Spark·Delta·Scala·Hadoop·hadoop-aws/AWS SDK를 호환 조합으로 배포한다. 로컬 시험은 Python 3.13.14 / Java 21 / Spark 4.0.1 / Delta 4.0.1, 실제 VM 시험은 Python 3.14.4 / Java 21 / Spark 4.1.3 / Delta 4.4.1이다. VM의 고정 JAR과 Delta 전이 의존성은 Garage 프로필을 따른다. JAR·Python 환경을 Driver/Worker 필요 범위에 일관 배포한다.
4. 비밀은 제한 권한 파일/환경변수/역할로 주입한다. 프로세스 관리기의 작업 디렉터리·환경·계정·로그 위치를 명시하고 Git 예시에 계정·개인키를 넣지 않는다.

| 작업 | 권한·환경 |
|---|---|
| 원천 적재 | PIPELINE_MONGO_URI: 접수/원천/감사 쓰기·인덱스·primary/majority 조회. PIPELINE_S3_ENDPOINT·이미지 버킷: 조회·검증·프로필별 불변 쓰기(현재 Garage는 내용 주소). PIPELINE_KAFKA_CONFIG: metadata·발행 |
| Bronze | PIPELINE_MONGO_URI: raw_products 읽기. BRONZE_KAFKA_OPTIONS: 클러스터/토픽 metadata·메시지 읽기. BRONZE_SPARK_OPTIONS: lake/checkpoint 읽기·쓰기·S3A/TLS·자원 |
| Silver | Bronze 읽기, ingestion_observations의 연결·image vector/revision 읽기, 고정 매핑/검수/링크/사전 export 읽기, Silver·input snapshot·checkpoint 쓰기, 등록된 NORMALIZATION run 연동 |

Silver는 원천·업로드 vector·서비스 승인 원장을 수정하지 않는다. 실제 pipeline_runs 등록 인터페이스/DDL이 없으면 로컬 fixture 시험과 운영 실행을 구분하고 등록 연동을 먼저 인수한다.

Kafka advertised 주소·Mongo Replica Set hostname·S3 endpoint/인증서 이름은 PC/Driver/Worker에서 접근 가능해야 한다. Worker↔Driver 통신도 필요하다. 정확한 포트는 설치 담당의 설정을 확인해 반영한다. SSH 접속 성공만으로 서비스 통신이 검증된 것은 아니다.

## 4. 현재 실행 가능한 초기 적재

Linux 배포 예시다. `/srv/...`와 local 설정은 실제 경로로 바꾼다. 이 문서가 서버 경로를 생성하지 않으며, 아래 네트워크 명령은 설정/권한 인수 후 실행한다.

```bash
cd /opt/mlo-01-p3-team3/src/data_pipeline
python run_pipeline.py preflight --config config/environment.local.json
python run_pipeline.py prepare \
  --export-dir /srv/collector/completed-run \
  --media-manifest /srv/collector/completed-run/media/products_with_local_images.jsonl \
  --output /srv/pipeline/snapshots/batch-001
python run_pipeline.py plan --snapshot /srv/pipeline/snapshots/batch-001
python run_pipeline.py ingest \
  --snapshot /srv/pipeline/snapshots/batch-001 \
  --state-dir /srv/pipeline/state/batch-001 \
  --config config/environment.local.json
```

환경·버킷·DB를 바꿔 기존 state를 재사용하지 않는다. snapshot 출력은 새 경로여야 한다. preflight는 실제 쓰기/장애 인수를 대신하지 않는다. 소량 이미지·원천·이벤트 대조 후 전체 작업을 실행한다.

Bronze는 [제출 형식](bronze_execution.md#설치와-실행)에 따라 Driver 서버에서 실행한다. 초기 `--available-now`, 이후 같은 checkpoint로 지속 수신한다. 실제 cluster ID·필수 JAR·인증·단일 작성자를 먼저 구성한다.

## 5. Silver 배포 절차

실행 코드는 구현되어 있으며 [Silver 실행 가이드](silver_execution.md)의 실제 명령·입력 형식·등록 증거·잠금·복구 절차를 따른다. 아래는 실제 서버에서 아직 수행하지 않은 **인수 순서**다. Silver는 Bronze와 같은 Spark/Delta 의존성을 사용하고 `requirements-bronze.lock.txt`로 설치할 수 있다.

1. [Silver 스키마](silver_design_v1.md)의 8개 경로, input snapshot 보존 경로, 분리 checkpoint를 확정한다. 생성 권한과 단일 작성자를 확인한다.
2. Bronze version·Raw 범위를 고정하고 NORMALIZATION run의 담당자 등록 export를 받는다. 일반 CLI는 등록 export를 검증·보관하며, 현재 서버 coordinator는 registration 도구와 보호 계정으로 NORMALIZATION/GOLD_BUILD를 등록한 뒤 그 결과를 고정 입력으로 사용한다. 최초 전체 처리와 상태 변경 재평가를 구분한다.
3. 매핑/검수/링크/사전·이미지 상태를 동결한다. 미제공은 NOT_PROVIDED, 연결·권한·조회 실패는 실행 실패다. 빈 입력을 승인된 설정으로 만들지 않는다.
4. 소량 실데이터로 상품 식별·이미지 관계·가격 종류·등록월·값별 시각·결측 사유·원격 업로드 상태를 원천과 대조한다. 기존 세 상품의 이미지 관계는 4/4/1이다.
5. 같은 입력 재실행, 일부 출력 후 중단·재개, 이미지 상태/매핑 변경 재평가를 시험한다. 동일 평가 중복 방지와 변경된 평가 이력을 확인한다.
6. 실제 Delta commit·출력 키/hash·version과 완료 receipt를 대조한다. receipt 없는 부분 출력은 Gold 입력으로 선택하지 않는다.
7. 전체 범위의 메모리·I/O·지연을 측정한다. 5분 순회 기회는 전체 데이터 5분 완료 보장이 아니다. 인수 후 NiFi 일정에 연결한다.

`config/silver.local.json`, 4종 owner snapshot, NORMALIZATION 등록 export와 `PIPELINE_MONGO_URI`/`SILVER_LEASE_MONGO_URI`/`SILVER_SPARK_OPTIONS`를 주입한다. Mongo 입력 원장은 읽기 전용, 별도 ops 잠금 컬렉션만 작성한다. `run_silver.py --bronze-version N --inputs ... --registration ... --state-dir ... --max-observations 3`으로 첫 소량 시험을 하고, 대조 후 소량 상한을 제거한다. 새 Bronze version은 새 범위로 시작하며 같은 version의 다음 순환에서는 상태/규칙 변경을 재평가한다. 상세 spark-submit 명령과 종료 코드는 실행 가이드에 있다.

실제 시험 전에 Silver가 서버에 배포/검증됐다고 기록하지 않는다. Gold·서비스 게시·임베딩 호출을 자동 후속 단계로 붙이지 않는다.

## 6. 중단·로그·백업·코드 교체

LIVE 인계가 추가되었다. 수집 PC에서 source를 읽기 전용으로 접근하는 `capture`와 DB/Kafka 실행 서버의 `drain`을 분리한다. [LIVE 실행 가이드](live_execution.md)의 quota·서로 다른 spool/state·전용 handoff bucket·재시작 검증·NiFi 단발 명령을 따른다. 실제 서비스는 [인수 시험](acceptance.md)을 먼저 수행하며, 현재 실제 서버 인수는 미수행이다.

Bronze는 `BRONZE_LEASE_MONGO_URI`의 별도 ops 계정을 추가한다. 인프라 담당이 공용 ops namespace의 unsharded·TTL 없는 lease 컬렉션을 사전 준비하고 listIndexes/find/insert/update/remove 권한을 부여해야 한다. source raw 조회 계정은 읽기 전용으로 유지한다. [Bronze lease 인수·수동 복구](bronze_execution.md)를 확인한다. 만료 claim을 자동 삭제하거나 새 Driver가 강제로 빼앗지 않는다.

- 원천 적재: 동일 snapshot/state/config로 재개하고 감사/등록 사본과 Mongo를 함께 보존한다.
- 정상 재개는 성공 단계의 외부 요청을 생략한다. 확인된 raw 유실의 대조/복구는 `--repair-raw`, 고정 이벤트 재발행은 `--replay-events`를 명시한다. 전체 무결성 검사와 정상 재개를 구분한다.
- Bronze: 동일 config/checkpoint로 재제출한다. checkpoint 삭제를 재개 방법으로 사용하지 않는다.
- Silver: 같은 설정/고정 Bronze version/checkpoint로 재개한다. 동일 평가의 고정 출력 version/hash·receipt를 대조하고, 입력·규칙 변경은 새 fingerprint로 보존하며 과거 출력을 덮어쓰지 않는다. 기존 규칙 버전명으로 다른 내용을 배포하지 않는다.
- 로그: 원천 audit/runs·failures·quarantine, Bronze batch status/execution_id/duration_ms, Silver state-dir/runs 및 SILVER_ATTEMPT·평가/품질·오류·duration_ms, Driver/Worker 로그를 수집한다. 비밀을 포함할 수 있는 SDK 오류·원문을 무제한 출력하지 않는다.
- 백업: export/snapshot, MongoDB, 이미지 저장소, Delta 데이터와 `_delta_log`, checkpoint, side input snapshot, 최초 등록 증거를 보존한다. Delta 데이터 파일만 복사해 복구 가능하다고 하지 않는다.
- 코드 교체: 정상 중지 후 완료 범위/부분 출력·checkpoint를 기록하고 호환 확인한 고정 커밋을 배포한다. 다른 schema/의미로 같은 경로를 재사용하거나 상태·Delta 파일을 임의 삭제하지 않는다. 이전 코드의 새 protocol/schema 읽기는 별도 검증한다.

## 7. 기능 변경 시 문서 동시 갱신

매 단계에 README·이 가이드·status의 ①실행 서버 ②코드/의존성 ③입출력·상태 위치 ④권한·네트워크 ⑤실제 명령·종료 코드 ⑥초기/증분/재개 ⑦건수/hash·실패 확인 ⑧백업·호환성 ⑨검증 완료/미검증을 갱신한다. 설계와 실제 실행 상태를 구분한다.

## 8. Gold 배포·인계

2026-10-17 추가. Spark 실행 서버에서 `run_gold.py`를 실행하며 Silver 고정 버전을 읽기 전용으로 사용한다. Gold root와 독립 checkpoint에만 쓰고 GOLD_LEASE_MONGO_URI의 공용 ops claim을 사용한다. Python/Java/Spark/Delta와 저장소 접속 권한은 Silver와 같은 검증 버전을 사용하되 GOLD_SPARK_OPTIONS로 실행 master/저장소 설정을 분리한다. 현재 로컬 검증 버전은 Spark/Delta 4.0.1이다.

[Gold 실행 가이드](gold_execution.md)의 명령과 config/gold.* 예시를 따른다. 8개 Silver version·Gold 입력·실제 GOLD_BUILD 등록 export·--as-of를 고정한다. 소량 시험은 --max-products 3, 재개는 같은 입력으로 수행한다. 성공 출력·receipt를 대조해 재사용하며 성공 기록이 있어도 실제 출력이 없으면 중단한다. 작업이 완료되면 stdout/state-dir/runs의 export_manifest_ref·quality_summary_ref와 고정 Gold versions를 확인한다. SOURCE_DRAINED와 부분 시험을 구분한다.

Gold 결과에는 원천/이미지 URL/객체 키/checksum/선택 근거와 확인 상태를 보존한다. 백업에는 Gold 데이터·_delta_log뿐 아니라 선택 페이지·입력/등록 export·완료 manifest와 공유 checkpoint를 포함한다. 새 스키마를 기존 root에 덮어쓰거나 Delta 로그를 삭제하지 않는다. 실제 인수 전 NiFi 일정이나 MySQL 게시에 자동 연결하지 않는다.

MySQL 문서 계약 기반 DDL preflight/staging/게시 guard/기존 UUID·검수 보존의 게시기는 구현했고 실제 격리 MySQL로 검증했다. 현재 pj3_products의 v2 스키마·TLS·업무 게시와 guard·활성화 연결을 확인했다. 기존 외부 서비스 baseline과 전체 범위 인수는 별도다. 운영 게시 명령은 서비스 DDL을 생성하거나 검수 원장을 수정하지 않는다. [게시 실행](mysql_publication.md)을 따른다. [요구사항 누락 대조](issue13_acceptance.md)에 서버 시험 외의 남은 구현을 기록했다. SSH hostname 조회 성공은 이 단계의 실제 클러스터 인수와 별개다.


## 게시·운영 배포 순서

1. 검토한 source release를 새 목적지에 복사하고 `python run_operations.py release-verify --bundle RELEASE_DIR`로 hash를 확인한다. 이 묶음은 src/data_pipeline 기준이며 상위 docs/design·인프라 설치·수집 데이터·비밀·가상환경을 포함하지 않는다. 상위 문서는 저장소의 검토한 버전을 함께 참고한다.
2. 대상 OS에서 새 venv를 만든다. 현재 VM은 위 3절의 서버 조합과 `requirements-spark-server.txt`를 사용한다. 원천 역할은 `requirements.lock.txt`, 게시 역할은 `requirements-publication.lock.txt`를 설치한다. Linux/VM에서 import/CLI 도움말·자동 테스트를 다시 확인한다.
3. MySQL 담당이 document-publication-v2 계약과 실제 DDL을 대조한다. `sql/publication_reference_v2.sql`을 서비스 DB에 무조건 실행하지 않는다. 승인된 DDL, 최소 권한 계정·현재 Primary·TLS/CA·논리 target과 담당 guard를 준비한다.
4. `run_publication.py preflight` → 고정 Gold 계획 prepare → 소량 `publish --apply` → 동일 계획 재실행·장애 복구를 검증한다. 새 image active/서비스 검수는 담당이 관리한다. 자세한 명령은 [MySQL 게시](mysql_publication.md)를 따른다.
5. `operations.local.json`에 단일 queue 소유 서버·영속 디스크·감사 경로·전용 handoff bucket을 설정한다. 초기에는 remote discovery를 끄고 명시 등록한 작은 작업으로 tick·실패 재시도를 검증한다.
6. 같은 설정의 수동 실패→재시도 성공 receipt를 확인한 뒤 [NiFi REST 절차](operations_execution.md)로 STOPPED 프로세서를 생성한다. 실제 버전/bundle/실행 권한을 확인하고 명시적으로 기동한다. 현재 검증용 NiFi의 timer/정지는 실제 확인했다. 승인 입력의 전체 FLOW 및 운영 일정 활성화는 별도 인수다.
7. VM→AWS 이전은 [이관 절차](state_migration.md)의 정지·외부 백업/복원·논리 ID/hash 대조·파일 state 복원을 먼저 수행한다. fingerprint를 임의로 바꾸거나 RUNNING/공용 lease/stream checkpoint를 지워서 재개하지 않는다.

기존 queue에 FLOW를 추가했다. [전체 단계 실행](flow_execution.md)의 flow-prepare→flow-run 또는 flow_directories→tick으로 Bronze/Silver/Gold·게시 계획·SQL·고정 범위 대조를 연결한다. 실제 Driver 위치/환경·권한·NiFi 종료 정책과 전체 worker CLI의 소량/장애 인수는 필요하다. 기존 서버 coordinator의 Silver/Gold 등록 경로는 연결됐으며, 외부 서비스의 별도 등록 인터페이스로 이전할 때는 담당 계약을 대조한다.

실제 소량 기술 시험과 전체 운영 인수의 구분은 실서버 검증 문서를 따른다. 검증용 launcher·queue·시험 원장을 운영 것으로 자동 전환하지 않는다.
