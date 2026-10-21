# 실제 상품의 검수 대기 적재

## 2026-10-19 세션·I/O 재사용 보완

사용자 요청에 따라 현재 window 000000만 처리하고 다음 window·연속 FLOW 진입을 중단한다. 서버 `pause-after-window.json`은 완료된 window의 게시·추가 활성화까지 끝낸 뒤 `PAUSED_AFTER_BATCH`로 반환하게 한다. 정지 요청 자체를 전체 범위 완료로 표시하지 않는다. 재개 전에는 운영자가 이 요청 파일을 확인하고 해제해야 하며, 완료 receipt와 커서는 삭제하지 않는다.

`production_window_work.py`는 단계별 Python 자식 프로세스를 만드는 대신 `production_work.run_at`을 순차 호출한다. `SessionScope`가 하나의 Spark 세션을 소유하며 Silver/Gold/게시/추가 활성화가 빌려 쓴다. 외부 설정의 master·JAR·Driver/Executor 자원·TLS JVM 설정이 달라지면 공유를 거부한다. 단계 종료 시 활성 Job/Stage/Streaming Query가 없는지 최대 5초 대조하고, 확인되지 않으면 writer lease를 성공 해제하지 않는다. 최종 scope 종료에서 Spark를 한 번 종료한다. 독립 CLI는 기존처럼 자체 세션을 종료한다.

Driver의 고정 Silver 그룹 캐시는 전체 32MiB LRU이며 root·테이블별 고정 버전·상품 키·행/바이트 한도를 키로 사용한다. 같은 고정 범위를 승인 입력 생성과 Gold 계산에서 재사용하고 반환값은 복사해 호출자 변경을 격리한다. 재시작 시 캐시는 사라지며 완료의 영속 증거로 사용하지 않는다. DeltaSink는 독점 lease 안에서 테이블 버전을 기억하고 MERGE 직전에 무효화한다. 고정/현재 버전이 같으면 같은 snapshot을 한 번 검증하고, 버전이 다르면 양쪽을 검증한다. 검증된 키·해시는 sink 수명에서만 재사용하고 10,000개 상한을 둔다.

기존 내용 주소 archive는 실제 내용을 한 번 읽어 비교한 결과를 바로 사용한다. 신규/복구 쓰기는 계속 다시 읽어 확인한다. Silver discovery cursor는 출력과 receipt가 모두 확정된 페이지마다 한 번 저장한다. 100개 관측을 가진 한 페이지에서 100번 cursor를 덮어쓰던 동작을 없앴다. 개별 원천/이미지 상태 archive를 생략하거나 무결성 비교를 제거한 것은 아니다.

Silver는 선택된 window 전체의 raw ID·관측 키·완료 evaluation key를 `silver-state/evaluation-selection.json`으로 봉인한다. 작은 window 재시도는 receipt를 검증하며 전체 선택 목록을 복구한다. Gold는 해당 명시적 완료 평가를 사용한다. 동일 원천의 여러 평가에서 시각·해시 순서로 최신을 추정하지 않는다. `repair_current_batch.py`는 이전 버전에서 이 목록을 남기지 못해 실패한 window 000000만 대상으로 성공 Silver 로그·원천을 대조하고 별도 `activation-repair-v1` 입력/등록으로 Gold·게시를 복구한다. 기존 실패 입력·등록·로그를 덮어쓰지 않으며 다음 window를 실행하지 않는다.

성능 계측은 각 단계 `runtime_metrics`와 `batch-runtime.json`에 session 생성/재사용, 버전 조회, 논리 테이블 읽기, 검증 읽기/재사용, archive 읽기/쓰기 횟수를 남긴다. 이 값은 SDK/네트워크 패킷의 실측 횟수가 아니라 코드 경로의 계측값이다. 중첩 activation의 시간·계측은 내부 단계 값을 포함하므로 합산하지 않는다. 격리 읽기 시험은 `/var/lib/pj3/product-pipeline/spark/session-reuse-verification-20261019/result.json`을 확인한다. 전체 데이터의 완료시간이나 처리량을 이 소량 시험으로 보장하지 않는다.

격리 시험은 VERIFIED다. 같은 application ID에서 session 생성 1회·재사용 2회, Worker 3대의 Executor 등록을 확인했다. 2개 상품의 고정 Silver 8개 테이블 읽기는 최초 25,342ms, 재사용 1,862ms였고 재조회 때 논리 테이블 읽기가 증가하지 않았다. 업무 DB 쓰기와 Kafka offset 변경은 없었다. 이 결과는 고정 입력 재사용의 측정이며 새로운 100상품의 전체 ETL 속도 비교가 아니다.

로컬 전체 단위 회귀 342개가 통과했고, 이후 추가한 legacy 완료 선택 복구는 관련 13개 테스트로 검증했다. 배포 서버에서는 세션/검증 재사용 6·배치/선택 13·검수 7·Silver 무결성 7·단계 배치 3, 총 36개를 통과했다. 배포 SHA256은 `6e9e14be9a0a6ae7e476b64395d06091d439697d52127e0ad247a591614fc3c1`이다. 현재 배치 복구 결과와 정지 상태는 root `pause-after-batch.json`, window의 `activation-repair-v1/*-result.json`, `activation-result.json`, `windows-progress.json`으로 대조한다.

현재 window 000000 복구는 완료됐고 `PAUSED_AFTER_BATCH`를 확인했다. 대상 100상품 중 활성화 게시 91·품질 대기 9이며 기존 활성 상품 재게시가 포함된 숫자다. 업무 DB 전체는 117상품 중 ACTIVE 91·PENDING_REVIEW 26·이미지 관계 46이다(ACTIVE 84→91). 다음 window 000001 디렉터리는 없고 처리 프로세스 종료를 확인했다. Gold 실행 235,754ms·게시 74,485ms였으며 Gold/게시 합계 세션 생성 1·재사용 2, 논리 Silver 테이블 읽기 8·그룹 캐시 적중 1, Delta 버전 조회 8·검증 읽기 6·검증 재사용 2였다. 이 작업은 이미 완료한 Silver 이후 복구 구간이므로 신규 상품 전체 ETL과 직접 속도 비교하지 않는다. 전체 연속 처리는 자동 재개하지 않는다.

## 2026-10-19 클러스터·작은 배치 실행 보완

운영 실행기는 `config/spark-runtime.json`의 명시적인 Standalone 클러스터 프로필을 요구한다. 현재 서버 파일은 `/var/lib/pj3/product-pipeline/spark/config/spark-runtime.json`이다. `spark://192.168.0.121:7077`, Driver 주소 `192.168.0.121`, Executor당 1코어·768MiB, 전체 3코어, shuffle 3을 사용한다. 로컬 모드로 자동 대체하지 않는다. 기존 Spark 인증·TLS와 Kafka TLS 설정은 유지한다. 단일 작성자는 Driver 수의 제한이며 Worker를 하나만 사용한다는 뜻이 아니다.

실서버 조회에서 Spark Worker `192.168.0.122~124` 세 대가 ALIVE이고 각각 1코어·1024MiB를 제공했다. Kafka Broker 1·2·3과 상품 토픽의 replicas/ISR 3개를 확인했다. `product.raw.collected`는 현재 파티션 1개이며 이번 개선에서 파티션 수나 기존 소비 checkpoint를 변경하지 않는다. 복제 수를 소비 병렬도와 혼동하지 않는다.

`flow`는 Kafka의 완료된 Bronze 범위마다 `production_window_work.py`를 호출한다. 범위의 Bronze 버전과 이전 게시 완료 Bronze 버전을 먼저 동결한다. 최초 기존 저장분은 전체 범위, 이후 정상 epoch는 이전 버전과 raw ID를 비교한 신규 원천 범위를 사용한다. 원장·이미지 상태 변경의 전체 재평가는 별도 `downstream`으로 수행할 수 있으며 신규 원천 차분 처리만으로 검수 변경 재평가까지 끝났다고 간주하지 않는다.

각 window는 **최대 100개 원천 관측·원문 합계 32MiB**다. 상품 수는 관측 수와 다를 수 있다. `selection.json`은 Bronze 버전·기준 버전·관측 키·상품 키·해시를 고정한다. 해당 범위만 Silver에서 처리하고, Gold는 그 상품들의 고정 Silver 이력을 평가한다. 완료된 작은 범위의 Gold export를 MySQL에 게시한 뒤 다음 window를 시작한다. Gold의 전체 범위 재순회를 매 window에서 수행하지 않는다.

신규 상품은 최초 대기 적재 전에는 업무 상품 원장이 없으므로, 최초 게시 후 같은 window 상품을 대상으로 자동 검수 정책을 다시 적용한다. 새 결정이 생긴 경우에만 `activation/` 아래 별도 고정 입력·등록으로 Silver→Gold→게시를 한 번 재평가한다. 이 재평가까지 끝나야 다음 window로 진행한다. 분류 충돌·출처 불일치 등은 승인으로 덮지 않고 대기·오류를 유지한다. `ACTIVE`와 추천 준비 상태는 별개다.

Gold 승인 입력은 상품별 Spark 조회 대신 테이블별 배치 읽기로 생성한다. 크기 집계와 collect는 DISK_ONLY 캐시를 공유하고 finally에서 해제한다. 최대 32MiB의 고정 Gold 읽기는 대기 적재·활성화 사이에 재사용하며 현재 guard·DB 검수는 다시 조회한다. Delta 저장 함수와 호출자가 같은 결과를 두 번 검증하던 부분은 제거했다. 호출자는 고정/현재 출력과 receipt를 계속 검증한다.

실행 예시(SSH 접속이 설정된 PC의 저장소 `src/data_pipeline`):

```powershell
python run_server_pipeline.py status
python run_server_pipeline.py downstream  # 기존 Bronze부터 처리하고 성공 후 flow로 연결
python run_server_pipeline.py flow        # 신규 원천/이미지 인계와 Kafka부터 연속 처리
```

중복 실행은 거부된다. 기존 `all`은 이전 고정 범위의 단계 실행 명령이므로 새 운영 재개에는 `downstream` 또는 `flow`를 사용한다. 서버 `windows-progress.json`의 stage/window/after와 각 `windows/<번호>/`의 plan·selection·stage result·activation 결과가 배치 진행 근거다. 커서는 게시·활성화 단계 완료 후에만 전진한다. 중단되면 같은 선택·등록·성공 결과를 재사용한다. 원천·Kafka·Delta checkpoint와 기존 UUID는 삭제하지 않는다. 이전 비분할 실행 이력은 history로 보존한다.

배포 후 서버 Python에서 핵심 배치·검수·등록·Silver 무결성 테스트 **40개**를 실행해 통과했다(배치 10·검수 5·등록 15·단계 배치 3·Silver 무결성 7). 실제 Worker 실행 및 Delta 읽기의 격리 결과는 `/var/lib/pj3/product-pipeline/spark/cluster-window-verification-20261019-v2/result.json`에서 확인한다. 단위 테스트 통과는 전체 65,498상품 처리 완료나 ACTIVE 전환 성공을 뜻하지 않는다.

실제 격리 결과는 `VERIFIED`였다. Worker01·02·03 모두에서 실행된 partition 3개를 확인했고 고정 Silver의 2개 상품·3개 관측 평가 그룹을 배치로 읽었다. 테이블 읽기는 27,023ms, 시험 전체는 68,936ms였다. 운영 DB 쓰기와 Kafka offset 변경은 없었다. 첫 시험은 Executor 전체 등록 전에 짧은 작업이 끝나 3대 실행을 입증하지 못해 실패로 남겼고, 등록 대기 후 재시험 결과와 구분한다. 이는 전체 파이프라인 처리량 수치가 아니다.

최종 배포 소스 SHA256은 `6f2f86f7672e403cba7f31b00798524b389c4b5fdcb1b4662bf652c5eeafeebf`이다. 기존 소스는 서버 `releases/` 아래 보존했다. 초기 window 상태 기록의 중복 updated_at 오류는 데이터 처리 전 실패했으며 서버 회귀 테스트로 재현·수정한 뒤 같은 범위에서 재개했다. 각 배치의 실제 MySQL 게시·ACTIVE 건수는 해당 receipt와 업무 DB 조회로 별도 확인한다. Git 커밋·푸시는 이번 작업에서 수행하지 않았다.

아래는 초기 대기 적재 방식과 이전 실행 이력에 대한 설명이다.

운영자가 승인한 초기 적재 정책은 **검수 대기·비활성 저장**이다. 기존 승인 완료 publication 경로와 별개이며, 카테고리 매핑·이미지 권리·서비스 노출·추천 승인을 생성하지 않는다.

## 실행 위치

원래 프로젝트 `mlo-01-p3-team3/src/data_pipeline/run_server_pipeline.py`에서도 실행할 수 있다. 같은 진입 파일의 유지보수 원본은 `feature/#13` 작업 경로에 있다. 다른 브랜치의 소스 전체를 덮어쓰거나 실행 중 브랜치를 변경하지 않는다.

```powershell
cd C:\Users\Playdata\Documents\encore_MLops\3st_project\mlo-01-p3-team3\src\data_pipeline
.\.venv\Scripts\python.exe run_server_pipeline.py status
.\.venv\Scripts\python.exe run_server_pipeline.py preflight
# 전체 원천과 확보된 이미지 범위를 반복 epoch로 처리. 이미 작업 중이면 거부한다.
.\.venv\Scripts\python.exe run_server_pipeline.py flow
# 기존 고정 입력 범위의 단계별 실행(최초 10건 연결 확인 등에 사용).
.\.venv\Scripts\python.exe run_server_pipeline.py all
```

실제 Spark와 DB 접속은 Spark Master에서 실행한다. 로컬 진입점은 Bastion을 통해 기존 Ansible·보호 설정을 사용하며 로컬에 DB 비밀번호를 복사하지 않는다. 터미널 종료로 서버 작업이 중지되지는 않는다. 전체 수집 범위는 `flow`로 시작·재개한다. 같은 실행의 확정 결과와 체크포인트를 재사용하고 실패한 epoch부터 이어간다. 단계별 `all`과 `bronze/silver/gold/publish`는 별도 고정 범위 명령이며 전체 처리를 대체하지 않는다.

현재 배포 제어기는 `production_manage` Ansible 모듈이며, 서버 실행 경로는 `/var/lib/pj3/product-pipeline/spark/production-pending-20261019`이다. 실제 상품 10개·이미지 관계 31개로 최초 전체 연결을 확인했다. `flow`는 원천 65,498건의 저장된 이벤트 등록을 전달하고, 이미 확보된 상품 16,384개의 이미지 인계를 별도로 처리한다. 이미지 다운로드를 재시작하지 않는다. 전체 원천은 이미지 확보 전 metadata 관측과 확보 후 관측을 별도로 보존하므로 원천 행 수와 업무 상품 수가 같지 않을 수 있다.

`flow-progress.json`이 전체 처리 상태의 기준이다. `epochs/<번호>/`의 단계별 result/failure가 해당 실행 증거이며, 루트의 최초 10건 성공 기록만으로 전체 성공을 판단하지 않는다. `raw-relay-progress.json`은 원천 이벤트 전달 상태이고, 전달 건수가 증가해도 Silver/Gold/MySQL 완료를 뜻하지 않는다.

2026-10-19 보완: Bronze Delta 저장은 한 번에 최대 10행·권장 2MiB로 나눈다. 단일 원천 행은 분해하지 않으며 기존 원천 크기 제한을 따른다. 같은 행의 저장을 이미 대조한 경우 다시 MERGE하지 않는다. 카프카 배치 및 체크포인트 정체성은 변경하지 않는다.

연속 실행은 `--available-now --max-batches 1`로 Spark가 진행 보고를 완료한 배치부터 Silver/Gold/게시로 넘긴다. foreachBatch 내부 로그만으로 완료를 판단하지 않는다. `source_drained=false`면 현재 적체 전체를 완료한 것으로 기록하지 않고 다음 epoch에서 남은 Kafka 데이터를 계속 처리한다. 원천 전달 작업이 먼저 끝나더라도 Kafka 적체를 건너뛰지 않는다. `--max-batches` 생략 또는 0은 기존 전체 적체 처리 방식이다.

`production_readback.py`는 별도 Spark Driver 없이 Delta commit 메타데이터와 업무 DB 건수를 읽는다. commit 파일 수·추가 파일의 행 수는 전체 유효 상품 수와 구분한다. 최종 판정은 고정 Gold export와 publication receipt·staging 및 SQL readback audit로 한다.

## 매핑과 이미지

### MongoDB 정규화 입력과 서버 승인 게시

`pj3_products_raw.danawa_products`는 수집기의 정규화 결과이며 `raw_products`의 대체 원천이나 승인 원장이 아니다. 상위 `docs/design/02-data-pipeline.md`의 원천 불변 보존 및 검수 경계, `03-data-model.md`의 상품/이미지 연결, `data-dictionary/08-product-links.md`의 이용 범위별 승인 계약을 따른다.

신규 서버 epoch의 Silver는 `--collector-normalized`로 동일 source/product_id/job_id/collected_at/raw_payload_hash의 문서 하나를 읽는다. 원천과 카테고리·갤러리 순서/URL을 대조하고 정규화 본문·문서 hash·원천 ID·관측 키를 공유 checkpoint의 불변 입력 archive에 남긴다. 이전 epoch 재개는 기존 실행 등록을 보존하여 새 입력 계약을 소급하지 않는다. 입력이 없거나 중복·불일치이면 해당 실행을 성공으로 기록하지 않는다. `VERIFIED`는 기술 연결 검사 결과이며 서비스 승인 `APPROVED`와 다르다.

연속 게시에서는 먼저 기존 비활성 적재를 수행하고, 확정 검수 원장이 존재하면 별도 reviewer 계정으로 `publish_reviewed`를 수행한다. 고정 Gold·동일 검수 버전·유효한 PRODUCT/IMAGE/POLICY·카테고리·checksum/이용 근거를 대조하여 guard를 등록하고 기존 UUID를 유지한 활성화 계획을 게시·audit한다. 계획·완료 receipt와 보류 사유는 epoch의 `reviewed-plans`에 저장한다. 재시도는 같은 계획을 사용한다. 검수 미제공·만료·품질 대기는 `WAITING_REVIEW`로 기록하고, 입력 충돌은 오류로 남긴다. 전체에 일괄 승인값을 쓰지 않는다.

실제 확정 매핑·이용 승인·링크 판정·가격 정책을 입력하는 업무와 등록/검증 도구를 구현한 사실은 구분한다. 승인 원장 0건이면 `WAITING_OWNER_INPUTS`이며 임의로 승인 근거를 만들지 않는다. 임베딩·인덱스 준비는 이 경로에서 실행하지 않는다.

확정 매핑이 없으면 Silver 입력은 `NOT_PROVIDED`, Gold는 품질 대기를 유지한다. MySQL FK를 위해 비활성 `PIPELINE_UNMAPPED` 범주를 사용한다. 이 범주는 실제 서비스 카테고리 매핑이 아니다. 원천 카테고리와 품질 사유는 고정 Gold·staging·source refs에서 추적한다.

단일 확정 갤러리의 원천 순서만 선택할 수 있으며 서로 다른 갤러리가 있으면 임의 병합하지 않는다. 이미지를 저장한 사실은 이용 승인과 다르다. 상품은 `PENDING_REVIEW`, 네 가지 readiness는 모두 false, 이미지 연결은 inactive/NOT_REVIEWED다. 대표 이미지·서비스 대표 가격·검증 링크·임베딩·판매 게시를 생성하지 않는다. 원문 가격과 URL의 증거는 Gold와 불변 staging에 남는다.

`prepare --pending-category-id <UUID>`로 만든 명시적 publication plan만 이 경로를 사용한다. 기존 plan의 기본 승인 요구사항은 그대로 적용한다. ACTIVE 상품이나 소유자가 변경한 상태·검수는 대기 적재가 덮어쓸 수 없다. 서비스 사용으로 전환하려면 담당자의 실제 승인·활성화가 필요하다.

검토된 상품·이미지는 대량 초기 적재에서 `SKIPPED_OWNER_MANAGED`로 따로 기록하고 계속 진행한다. 데이터 충돌·원천 변조 등 다른 오류를 이 상태로 감추지 않는다. 실제 매핑·이용 승인·게시 승인과 활성화는 [검수 절차 실행](review_workflow.md)을 따른다.

## 권한과 TLS

MongoDB·Kafka·S3·MySQL 인증서 검증을 유지한다. MySQL 공개 CA는 Spark 서비스 계정이 읽는 보호 경로에서 사용하며 인증서·호스트 이름 검증을 끄지 않는다.

### Spark Executor의 Kafka TLS 설정

노드별 `/etc/pj3-secrets/spark/keystore.p12`는 Spark 통신용이며 노드마다 비밀번호가 다를 수 있다. Driver의 비밀번호를 Kafka 옵션으로 전달하면서 각 Worker의 이 파일을 읽으면 `keystore password was incorrect`로 실패한다. Kafka용 공통 클라이언트 묶음은 별도 `/etc/pj3-clients/kafka/pipeline/{keystore.p12,truststore.p12}`에 배포한다. Driver와 참여하는 모든 Worker에서 같은 경로·같은 파일 해시·서비스 계정 읽기 권한을 확인한다. 기존 노드별 Spark 통신 인증서를 덮어쓰지 않는다. 현재 묶음은 기존 Kafka 접속에 사용하던 Driver 클라이언트 인증서를 분리 배포한 것이며 신규 CA를 발급한 것은 아니다.

운영 실행기는 `/var/lib/pj3/product-pipeline/spark/config/kafka-spark.json`을 읽는다. 비밀값이 없는 형식 예시는 `config/kafka_spark.example.json`이다. 실제 설정은 서버의 보호 파일(0600)이며 Git에 넣지 않는다. keystore·truststore·개인키 비밀번호를 각각 명시하고, 노드별 Spark 설정이나 Python Producer의 비밀번호로 암묵 대체하지 않는다. `product_pipeline.kafka_tls.spark_options`가 설정 누락·비보안 프로토콜·호스트 이름 검증 해제를 차단한다.

2026-10-19 점검 시 Worker 01(`192.168.0.122`)과 Kafka 02(`192.168.0.127`)가 응답하지 않았다. 접근 가능한 Worker 02·03에 Kafka 묶음을 배포했고, Kafka 토픽은 Broker 03 리더·ISR [3,1] 상태였다. 중단 노드가 복구되면 동일 인증서 배포와 연결 점검을 마친 뒤 참여시킨다. 이번 Bronze 재개 검증은 별도 `tls-recovery-20261019/runtime-two-workers.json`의 2코어 프로필을 사용하며 기존 기본 3코어 프로필은 변경하지 않는다.

만료 잠금은 자동 인수하지 않는다. 이번 복구에서는 로컬 Driver 종료·Spark Master 활성 Application 0건·MongoDB 서버 시각 기준 만료를 확인하고, 이전 잠금 4개를 보호 파일에 보존한 뒤 token·heartbeat·만료 시각 조건이 일치하는 잠금만 해제했다. 원천·Delta·Kafka 체크포인트는 삭제하지 않았다. 근거는 서버 작업 경로 `tls-recovery-20261019/lease-recovery.json`과 접근 제한된 잠금 원문이다.

실제 검증은 2026-10-19 19:47 KST에 완료했다. 기존 체크포인트 ID `93c7ebcf-b37e-41c5-9cf4-30fc46dc21c6`을 유지해 배치 2에서 Kafka 이벤트 100건을 수신·수용했고 격리 0건으로 저장했다. 체크포인트 커밋은 [0,1]에서 [0,1,2]로 증가했으며 Spark 종료 후 대상 Bronze 잠금은 0개였다. 로컬/서버 관련 테스트는 각각 19개 통과했다. 증거는 서버 작업 경로 `tls-recovery-20261019/verification.json`과 상태 조회용 `bronze-tls-recovery.json`이다. 한 배치 정상 커밋 후 종료한 검증이며 전체 적체 처리나 Gold의 이력 용량 제한 해결을 의미하지 않는다. 과거 `flow-progress.json` 실패 기록과 새 복구 증거의 시각·scope를 구분한다.

`docs/server_setup/pipeline_privilege_audit.py`는 해당 환경의 publisher 계정 권한을 검사한다. pipeline_runs와 파이프라인 쓰기 테이블에 SELECT/INSERT/UPDATE, categories·guard·임베딩/인덱스 원장에는 SELECT만 요구한다. 보완은 Primary에서 명시적으로 적용하며 다른 두 노드에는 조회로 반영을 확인한다. GRANT OPTION이나 승인 원장 편집 권한을 추가하지 않는다.

등록의 SELECT FOR UPDATE에는 UPDATE 권한이 필요하다. 실행 중 확인된 1142 오류는 이 범위를 보완한 뒤 같은 등록 ID로 재개한다. 기존 입력 hash·등록·원문·성공 이력을 삭제하지 않는다.

서버 준비 도구 `pending_mysql_prepare.py`는 신규 대기 범주와 누락된 실행 등록 테이블만 다룬다. 기존 테이블을 재생성하거나 변형하지 않는다. 런타임 publisher는 DDL을 수행하지 않는다.

## 검증

### 2026-10-20 Gold 이력 선택 및 읽기 비용 보완

`epochs/000001/windows/000015`의 중단 원인은 `GOLD_EVALUATION_SELECTION_REQUIRED`였다. 현재 배치 10개 원천의 완료 평가는 있었지만 같은 상품의 과거 원천 하나에 평가가 둘 이상 있었고 선택 근거가 빠졌다. 현재 배치 완료 영수증, 실제 게시 완료 staging을 대조한 과거 선택, 단일 유효 영수증 순서로 전체 이력을 연결한다. 시간·해시 순서로 최신 평가를 추정하지 않는다. 근거가 없는 과거 원천은 같은 고정 Bronze와 Silver 입력으로 `history-silver/<입력 hash>/`에 별도 등록하여 보충 정규화한다. 고정 Bronze에서 해당 원천을 확인할 수 없으면 차단한다.

기존 등록된 Gold 입력은 수정하지 않는다. 실패한 이전 계약은 `gold-recovery-v2/`의 새 등록으로 이어받고, 부모 Gold 결과의 `gold_state_dir`를 따라 게시한다. 이전 15개 성공 배치·서비스 UUID·원천·Kafka 체크포인트를 초기화하지 않는다. 신규 배치 준비는 `docs/server_setup/production_gold_inputs.py`를 서버 `production_work.py`와 같은 디렉터리에 배포해야 한다.

Gold의 Silver 입력은 테이블별 집계·수집 대신 고정 버전의 테이블을 묶어 용량을 확인한 후 읽는다. 동일 세션의 동일 경로·버전·상품·제한 설정은 재사용한다. 메모리 캐시를 넘는 Silver 그룹은 임시 디스크 캐시(기본 총 256MiB)에 체크섬과 함께 저장하며 세션 종료 시 제거한다. 검수 DB 트랜잭션 전에 읽기를 완료하는 별도 review spool도 기본 총 256MiB/그룹 32MiB와 디스크 여유 상한을 적용한다. 임시 파일은 완료 증거나 체크포인트가 아니다. 개별 Spark 입력 읽기의 기존 행·바이트 제한은 유지한다.

실제 서버의 동일 고정 Silver 버전·상품 10개·평가 19개에서 기존 읽기 50,398ms, 개선 읽기 19,613ms와 반복 읽기 19,166ms를 확인했다. 영수증 및 모든 출력 행 hash 구성이 일치했고 행 상한 초과도 수집 전에 차단됐다. 이는 **입력 읽기 구간의 비교**이며 전체 파이프라인이나 전체 65,498상품 처리량으로 환산하지 않는다. 근거는 서버 작업 경로 `perf-20261020/benchmark.json`이다. 운영 배포 파일·SHA256·단위 테스트는 `perf-20261020/deployment.json`에 기록한다.

배치 크기는 `PIPELINE_WINDOW_RECORDS`로 새 selection 생성 시에만 적용한다. 이미 봉인된 배치의 구성은 변경하지 않는다. 일시 정지 결과 `PAUSED_AFTER_BATCH`는 epoch 완료가 아니며 coordinator는 `COMPLETED` 결과만 건너뛴다. `windows-progress.json`의 `last_batch`에 관측 수·상품 수·단계별 소요 시간을 기록하고, 실패 시에도 상태 callback을 갱신한다. 크기를 확대하기 전 실패 배치의 실제 MySQL 게시와 새 배치 소요 시간을 확인한다.

기본 실행 설정은 서버 작업 경로의 `runtime-overrides.json`에 `window_records`(1~100), `spark_runtime_file`(서버 BASE 내부의 절대 JSON 경로)로 남긴다. 제어기가 이를 읽어 시작하므로 다른 PC의 환경 변수에 의존하지 않는다. 이번 검증에서는 두 Worker 프로필과 25건 시험 배치를 선택했다. 기존 selection을 덮어쓰지 않는다. 제어기는 `production_window_work.py`도 실행 중으로 감지한다. CLI가 남긴 실제 실패 코드를 상태에 전달하고 평가 선택·이력 상한 같은 확정 입력 오류는 동일 계약으로 세 번 반복하지 않는다.

Silver/Gold 불변 결과 저장은 단일 작성자 lease 아래 기존 키와 본문 hash를 먼저 대조하고 없는 행만 append한다. 기존 행을 다시 추가하거나 값을 갱신하지 않는다. 커밋 응답 유실 후에는 저장된 키·hash를 다시 읽어 성공분을 재사용한다. 모든 출력의 저장 대조를 마친 뒤에만 완료 영수증을 쓰는 순서는 유지한다. 중복 입력 키나 본문 충돌은 차단한다. 이 변경은 Bronze 저장 방식이나 서비스 MySQL upsert 정책을 바꾸지 않는다. 실제 Mongo lease를 획득한 별도 Delta 경로에서 2행 저장·혼합 재입력·재실행 시 버전 불변·본문 충돌 거부를 확인했다. 두 번의 쓰기는 기존 MERGE 22,258ms, 신규 행 추가 6,347ms였으며 이는 소량 격리 시험 수치다(`perf-20261020/append-trial.json`).

`tests/test_publication_pending.py`는 미매핑/미검수 입력, 비활성 상태, 재실행 ID 유지, 부분 실패 롤백, ACTIVE 상품 보호, 원문·이미지 연결 대조를 검사한다. 실제 서버 결과는 해당 작업 경로의 단계별 result/failure와 publication receipt로 확인한다. 서버 연결 성공·일부 실제 상품 적재·전체 65,498건 적재 완료는 구분한다.

### 2026-10-20 활성화 재평가 범위 및 메타데이터 개선

중단됐던 `000015` 배치의 Gold·MySQL 게시·활성화를 복구했고, 다음 `000016` 배치의 실제 25개 상품도 활성화 평가까지 완료했다. 완료 배치는 17개이며 전체 65,498건 완료를 의미하지 않는다. 이 25건의 실제 처리 시간은 입력 준비 35,935ms, Silver 229,169ms, Gold 259,791ms, 게시 62,696ms, 활성화 412,773ms였다. 전체 1,000,388ms로, 개별 읽기·쓰기 시험의 개선 비율을 전체 처리량으로 해석할 수 없다.

활성화 412,773ms의 하위 완료 기록(`000016/activation/*-result.json`)을 추가 대조한 결과는 다음과 같다.

| 구간 | 실제 시간 | 확인한 처리 |
|---|---:|---|
| Silver 재조회·검증 | 101,216ms | 25건 처리, 기존 평가 25건 재사용, 신규 Delta 행 0건. archive 읽기 55회·쓰기 4회, 출력 proof 읽기 7회 |
| Gold 재평가·저장 | 235,605ms | 25개 상품 모두 신규 평가, 기존 평가 재사용 0건. Delta append 8회·785행, proof 읽기 8회, 버전 조회 8회 |
| 게시·활성화 | 73,592ms | 고정 Gold 읽기와 MySQL 게시·guard·audit를 포함. 최종 활성화 확인 25개 |
| 검수 등록·준비 등 잔여 | 2,360ms | 전체에서 위 세 구간을 차감한 값. 세부 독립 타이머 수치가 아님 |

이 배치의 새 검수 결정은 PRODUCT 17건뿐이며 이미지·매핑·가격 정책 결정은 추가되지 않았다. 첫 게시에서 이미 8개가 활성화되고 17개만 대기했지만, 이전 활성화 경로는 전체 25개를 다시 Silver→Gold→게시로 보냈다. Silver는 결과를 다시 쓰지 않았으나 저장된 결과와 입력을 전부 재검증했고, Gold는 승인 입력이 바뀌어 25개 모두 새 평가·출력을 생성했다. 활성화 중 Spark 신규 세션은 0회이고 세션 재사용은 5회였다. 따라서 이 배치의 활성화 지연을 Spark 세션 재시작이나 단순 ACTIVE SQL 갱신 지연으로 설명하지 않는다.

Gold의 고정 Silver 그룹은 이때 이미 캐시에서 두 번 재사용됐고 새 union 읽기는 0회였다. Gold 내부 235,605ms를 대상 초기화·저장·proof·export 각각으로 나눌 과거 시간 기록은 없으므로 객체 저장소나 DB 한 구간의 지연으로 단정하지 않는다. 이번에 추가한 하위 타이머로 다음 실행에서 분리한다. 대기 상품 범위 축소와 변경 없는 Silver의 실행 전 재사용은 위에서 확인한 불필요한 전체 재처리를 줄이는 조치다.

추가 개선은 새 배치를 실행하기 전에 배포한다. `pause-after-window.json`을 유지하고 `PAUSED_AFTER_BATCH` 상태를 확인한다. 배치 크기는 기존 시험값 25를 유지하며 50/100건 확대나 연속 처리는 별도 재개 후 판단한다.

- Delta 대상이 이미 존재하면 생성 명령을 반복하지 않고 스키마를 대조한다. 모든 대상의 속성 조회와 버전 조회를 각각 묶는다. fingerprint·appendOnly·정확한 테이블 구성·writer lease 검사는 유지한다. 서버 읽기 전용 시험에서 스키마와 버전이 변하지 않았으며, 버전 조회는 15,972→2,747ms, 대상 초기화는 22,505→2,736ms였다. 같은 세션에서 순서대로 측정한 소량 시험으로 워밍업 영향을 포함한다(`perf-20261020/metadata-trial.json`).
- 상품 승인·가격 정책 변경만으로 Silver가 다시 정규화되지 않도록, 신규 Silver 입력의 `review-ledger-v2` hash는 실제 매핑·이미지 검수 행에 연결한다. 각 종류의 행을 정렬한 후 hash를 계산한다. Gold의 승인·정책 계약은 별도로 갱신한다. 이미 동결된 기존 입력·등록은 수정하지 않는다.
- `publish_reviewed` 완료 결과에 `waiting_product_keys`를 남긴다. 신규 `production_activation.py`는 이 목록과 건수·원래 selection의 포함 관계를 검증한 뒤 대기 상품의 관측만 `activation-v2/`에서 재평가한다. 이미 활성화된 상품은 다시 정규화하지 않는다. 목록이 없는 과거 결과와 기존 `activation/` 재개는 이전 전체 범위를 유지한다.
- 활성화 시 동결한 Silver 입력이 부모의 입력과 정확히 같고 부모 Silver가 완료됐으면 완료 선택을 재사용한다. 이전 Silver 실행 시작부터 현재까지 valid_from·expires_at·checked_at 경계를 지나지 않았는지도 확인하며, 시각 증거가 없으면 실제 Silver를 실행한다. `silver-reuse.json`에 부모 selection·입력 hash·재사용 범위를 기록한다. 이미지 검수나 매핑이 달라졌으면 실제 Silver를 다시 실행한다. Gold는 고정 버전의 실제 Silver 영수증과 출력 hash를 계속 검증하며, 변경된 승인 입력은 새 Gold 평가로 처리한다.
- MySQL 게시용 Gold 읽기도 고정 버전의 테이블을 묶어 행 수·UTF-8 바이트 수를 확인한 후 수집한다. 상한을 넘으면 상품 페이지를 줄이고 단일 상품도 초과하면 차단한다. 서버의 기존 25개 상품에서 기존 방식 84,453ms, 개선 방식 31,467ms였으며 영수증·모든 출력 행을 포함한 그룹 hash가 같았다. 행 제한 거부도 확인했고 업무 데이터는 쓰지 않았다(`perf-20261020/publication-read-trial.json`).
- content-addressed 입력 archive는 같은 Driver에서 저장 후 대조한 객체만 재사용한다. 재시작 시에는 저장소를 다시 확인한다. 메모리 캐시는 바이트 상한과 1,000개 항목 상한을 함께 적용한다.

단계별 `runtime_metrics`에 `delta_target_open_ms`, `delta_output_read_ms`, `delta_version_read_ms`, `delta_append_commit_ms`, `delta_output_verify_ms`, `archive_ms`, `gold_input_preparation_ms`, `publication_gold_read_ms`, `publication_pending_mysql_ms`, `publication_activation_mysql_ms`를 기록한다. 활성화에는 검수 등록·Silver 재사용 판정·Silver/Gold/게시별 시간과 재평가/건너뛴 상품 수를 남긴다. 실패한 시도도 duration과 당시 metrics를 남긴다. 상위 시간은 하위 시간을 포함하므로 서로 합산하지 않는다.

운영 배포에는 `production_work.py`, `production_gold_inputs.py`, `production_activation.py`와 대응하는 패키지 코드가 모두 필요하다. 서버의 이전 STAGE 아카이브를 사용하는 `refresh`로 직접 배포된 개선을 덮어쓰지 않는다. 해당 묶음이 현행 코드로 갱신되기 전에는 일반 `flow` 시작·재개 제어기를 사용한다. 실제 배포 파일 hash와 서버 테스트 결과는 `perf-20261020/deployment.json`을 확인한다. 1차 보완 시점의 로컬 관련 회귀 테스트는 128개 통과했다. 이후 보완과 실제 새 배치의 측정 결과는 아래에 기록한다.

### Gold 최종 집계 및 출력 검증 묶음 처리

Gold 최종 export는 selection·고정 catalog·quality contribution·image 관계를 실행 내 DISK_ONLY 캐시로 재사용하고 종료/실패 시 해제한다. selection의 누락·중복·건수는 한 집계로 검사하고 catalog·receipt 건수도 묶어 확인한다. 카테고리 및 GLOBAL 품질 집계는 rollup으로 계산하며 GLOBAL 파일 수는 원본 checksum의 distinct 집계다. 카테고리별 파일 수를 더해서 공유 파일을 중복 계산하지 않는다.

Gold 배치 저장은 각 테이블의 불변 신규 행을 저장한 뒤 버전을 한 번에 조회하고 출력 proof를 묶어 대조한다. 현재 버전과 영수증의 과거 버전이 다르면 둘 다 확인한다. 키·정규화된 전체 행 hash·누락·중복을 검사하며, 하나라도 실패하면 새 완료 영수증을 생성하지 않는다. 읽기는 요청당 5,000키·수집당 32MiB 이하로 나누고 초과 시 더 작은 요청으로 분할한다. 새 공통 구현은 `product_pipeline/delta_proofs.py`이며 Silver의 기존 호출 방식은 바꾸지 않았다.

실서버 읽기 전용 시험에서 기존 25개 상품의 최종 export는 75,229→25,630ms였고 summary·manifest hash가 같았다. 785개 출력 행의 묶음 proof 검증과 의도적으로 잘못된 hash의 차단을 확인했다. 업무 데이터는 쓰지 않았다(`perf-20261020/gold-batch-trial.json`). 관련 로컬 테스트 135개와 서버 테스트 82개가 통과했다. 실패 후 부분 출력 재사용과 완료 영수증 저장 순서도 테스트했다.

실제 업무 배치는 기존과 같은 25건으로 한 번만 실행한다. 시작은 완료 배치 17개 지점이며, `pause-after-window.json`을 유지해 18개 완료 지점에서 정지한다. 시험 요청·패치 hash는 서버 `perf-20261020/single-batch-request.json`에 기록하고 다음 배치의 selection이 생성되지 않았는지 확인한다. 처리 시간은 `epochs/000001/windows-progress.json`의 `last_batch` 및 `windows/000017/*-result.json`을 기준으로 비교한다. 이전 배치와 상품·이력·이미지 구성이 다르므로 동일 입력 벤치마크와 구분한다.

2026-10-20 실제 1회 실행 결과: 25개 관측·25개 상품을 처리하고 완료 배치 18개 지점에서 `PAUSED_AFTER_BATCH`로 정지했다. 다음 `000018/selection.json`은 없으며 실행 중인 파이프라인 프로세스도 0개다. 증거는 `perf-20261020/single-batch-result.json`이다.

| 단계 | 이전 25건 배치 | 개선 후 새 25건 배치 |
|---|---:|---:|
| 입력 준비 | 35,935ms | 29,932ms |
| Silver | 229,169ms | 209,054ms |
| Gold | 259,791ms | 206,915ms |
| 최초 MySQL 게시 | 62,696ms | 43,012ms |
| 활성화 재평가·반영 | 412,773ms | 450,044ms |
| 전체 배치 경과 시간 | 1,000,388ms | 938,981ms |

전체는 약 15분 39초로 이전보다 약 61초 짧았지만, 동일 입력 비교가 아니므로 전체 성능 개선율로 일반화하지 않는다. Gold 본처리의 최종 집계는 25,503ms, 묶음 proof 검증은 34,219ms, 묶음 버전 조회는 2,894ms였다. 해당 Gold 출력은 854행으로 이전 802행과도 다르다.

첫 게시에서 20개 활성화를 확인하고 대기 5개만 후속 재평가했다. 후속 검수에서 PRODUCT 4건·IMAGE 10건이 새로 등록돼 Silver의 reviews 입력이 실제로 달라졌다. 이때는 Silver를 재사용하지 않는 것이 맞다. 후속 Silver 214,399ms, Gold 201,836ms, 게시 32,551ms가 소요돼 활성화 총 시간은 오히려 증가했다. 후속 Silver는 5개 중 1개 평가를 재사용하고 4개를 새로 정규화했으며, 출력 조회 158,475ms와 검증 106,146ms가 기록됐다. 이 둘은 중첩되므로 더하지 않는다. 적은 상품 수에서도 반복 출력 조회·과거/현재 proof 검증의 고정 비용이 크다는 점이 남은 병목이다. 이번 묶음 proof 호출은 Gold에 적용했고 Silver의 기존 호출 방식은 유지했다.

이번 범위의 활성화 확인은 최초 20개·후속 4개이며 나머지 1개(`danawa:124237427`)는 `GOLD_CATALOG_QUALITY_WAIT`다. 저장된 Gold 계획에는 상품 상태 미확인·비교 링크 준비 미완료·대표 가격 미확정·동일 시각 값 충돌 등이 남아 있다. 이 상품의 값을 임의 수정하지 않았다. MySQL 전체 readback은 상품 209개, ACTIVE 195개, PENDING_REVIEW 14개, offers와 offer_snapshots 각각 1,368개다. 기존 활성 상품을 포함한 배치 활성화 확인 건수와 신규 전역 활성 증가 4개를 구분한다.

### Silver·Gold 저장 전 조회 및 Silver proof 보완

다음 보완은 `product_pipeline/delta_batch.py`의 `insert_many`로 Silver·Gold 신규 출력의 기존 행을 같은 writer lease 안에서 묶어 읽는다. 모든 입력 키·본문 hash와 조회한 기존 행의 전체 hash를 대조한 뒤 첫 테이블 저장을 시작한다. 필요한 신규 행만 append하며 조회 당시 없었던 키는 호출 외부에 캐시하지 않는다. 일부 테이블 저장 후 응답 유실이나 프로세스 종료가 발생하면 재시도에서 저장소를 다시 대조한다. 테이블·스키마·ID·완료 영수증 계약은 바꾸지 않는다.

Silver 배치도 과거/현재 출력 proof와 저장 후 버전 조회를 묶는다. 모든 출력이 검증된 뒤 이미지 상태 revision·fingerprint를 다시 확인하고 마지막으로 완료 영수증을 저장한다. 저장 중 이미지 상태가 달라지면 성공하지 않는다. 신규·기존 평가가 섞인 재시도에서도 같은 규칙을 적용한다. 활성화에서 Silver를 실제로 재평가하는 경우에도 이 공통 경로가 적용된다. 승인 변경의 영향 범위를 관측 단위로 더 좁히는 별도 설계는 이번 보완에 포함하지 않는다.

로컬 관련 회귀 테스트는 142개 통과했다. 기존 행 충돌 시 첫 저장 전 차단, 커밋 응답 유실 후 중복 없는 재시도, writer lease 상실, 이미지 상태 변경, proof 실패 시 완료 영수증 미생성을 확인했다. 다음 1회 시험은 완료 배치 18개→19개를 목표로 요청했으나, 아래 writer lease 장애로 신규 Silver·Gold 배치에 도달하지 못했다. 따라서 이 보완의 실제 전체 배치 시간은 아직 측정하지 못했다. `single-batch2-request.json`은 시험 요청이며 성공 완료 증거로 해석하지 않는다.

### Writer lease 장애 진단·명시적 복구 (2026-10-20)

2026-10-20 23:08 KST 재실행에서 epoch 1의 기존 범위가 소진되어 epoch 2 Bronze로 이동했다. 최초 시도는 약 16초 후 `BRONZE_WRITER_LEASE_LOST`, 이후 두 시도는 `BRONZE_WRITER_ALREADY_CLAIMED`로 종료됐다. 최초 Mongo 갱신 오류의 세부 원인은 당시 코드가 보존하지 않아 확정할 수 없다. 단순 만료나 TLS 오류라고 단정하지 않는다. 운영 잠금 4개는 만료 상태로 남아 있으며 이번 도구 검증에서 변경하지 않았다. 파이프라인 자동 재개도 하지 않았다.

`bronze/lease.py`는 최초 실패의 단계·대상 키·예외 종류·정수 Mongo 오류 코드·소요 시간을 `lease_failure`로 남긴다. 비밀번호·접속 URI·SDK 오류 원문·잠금 토큰은 일반 로그에 쓰지 않는다. Bronze/Silver/Gold 결과와 운영 실패 파일에 전달한다. 연속 처리기는 lease 상실·기존 작성자·저장 확인 불가·Spark 종료 확인 불가를 만나면 `RECOVERY_REQUIRED`를 기록하고 즉시 종료한다. 같은 오류를 세 번 반복하거나 만료를 이유로 자동 인수하지 않는다. 이 변경 전 실패한 실행의 `FAILED` 기록은 소급 변경하지 않는다.

복구 구현은 `product_pipeline/lease_recovery.py`, 서버 실행 도구는 `docs/server_setup/recover_writer_lease.py`다. Bronze·Silver·Gold의 설정에 지정된 정확한 출력·checkpoint 경로만 대상으로 한다. `inspect`는 프로세스·인증된 Spark Master·예상 Worker 목록·자원 사용 0·Mongo 서버 시각 기준 만료·TTL 인덱스 부재를 확인하고 변경 없는 계획을 생성한다. `apply`는 이 증거를 다시 확인하고 token·heartbeat·만료·경로·fingerprint가 같은 잠금만 snapshot/majority+j 트랜잭션으로 함께 해제한다. 중간 실패는 전부 취소하며 응답 유실은 `UNCONFIRMED`로 기록해 재확인한다. DB·Delta 데이터, Kafka offset, checkpoint는 삭제하지 않는다. 복구 도구는 수집기나 파이프라인을 시작하지 않는다.

서버 사용 예시는 다음과 같다. 배포 root·Python·인증서·현재 Worker 목록은 실제 환경에 맞춰 지정한다. 현재 명령은 Spark Master에서 `pj3-spark` 계정으로 실행하며, `COMMON`은 Bash 배열이다.

```bash
ROOT=/var/lib/pj3/product-pipeline/spark/production-pending-20261019
PY=/var/lib/pj3/product-pipeline/spark/venvs/128dbd37d879b397-spark/bin/python
COMMON=(--stage bronze --config "$ROOT/epochs/000002/bronze.json"
  --deployment-root "$ROOT" --state-dir "$ROOT/lease-recovery-20261020"
  --master-url http://192.168.0.121:8080/json/
  --worker 192.168.0.123 --worker 192.168.0.124
  --ca /etc/pj3-secrets/spark/ca.pem
  --client-cert /etc/pj3-secrets/spark/cert.pem
  --client-key /etc/pj3-secrets/spark/key.pem)
"$PY" "$ROOT/recover_writer_lease.py" inspect "${COMMON[@]}"
# inspect 출력의 plan_path를 정확히 지정한 뒤 명시적으로 실행한다.
"$PY" "$ROOT/recover_writer_lease.py" apply "${COMMON[@]}" --plan /absolute/path/to/plan.json
```

Master HTTP 주소는 현재 HTTPS로 리다이렉트되는 진입점이다. 최종 응답이 HTTPS가 아니면 차단한다. Worker가 복구되어 구성원이 늘어나면 실제 구성과 `--worker`를 함께 갱신해야 한다. 보호 state 디렉터리는 0700, 계획·감사 파일은 0600이다. 계획에는 조건부 해제에 필요한 기존 토큰이 있어 일반 공유 문서에 첨부하지 않는다. `apply` 성공 후에도 원래 실패 단계의 설정·증거를 확인하고 기존 실행 관리 도구로 별도 재개해야 한다.

관련 로컬 회귀 테스트는 175개 통과했다. 소유자 변경·heartbeat 갱신·미만료·실행 중 Driver·Worker 구성 변경·부분 삭제 실패·커밋 응답 유실·재실행을 포함한다. 실제 Mongo Replica Set 시험에서는 별도 verification 경로의 시험 잠금 4개를 획득·만료시킨 뒤 트랜잭션으로 해제했고, 반복 실행은 `ALREADY_CLEAR`였다. 운영 잠금 4개의 전후 본문은 동일했다. 증거는 서버 `perf-20261020/recovery-verification.json`과 보호된 `lease-recovery-20261020/isolated-*.json`이다.

재현용 실서버 시험 코드 `tests/smoke_lease_recovery.py`도 보존했다. `--deployment-root`, `--config`, `--state-dir`, `--master-url`, 반복 `--worker`, `--ca`, `--client-cert`, `--client-key`, 명시적 `--apply-isolated`를 받는다. 실제 운영 설정의 접속을 쓰되 무작위 verification 경로의 시험 잠금만 수정하고 운영 잠금 전후 일치를 확인한다. 원천/Delta/체크포인트 저장은 수행하지 않으며 전체 장애 복구나 다음 업무 배치의 성공을 뜻하지 않는다.

### 활성화 관측별 재평가 및 연속 처리 재개

2026-10-20 추가 보완은 `activation-v3`에서 대기 상품의 이전 완료 Silver 평가 후보를 관측별로 봉인한다. 입력 snapshot 전체 hash가 달라졌다는 이유만으로 모든 관측을 다시 정규화하지 않는다. 공통 `silver.normalize.evaluation_input`이 현재 원천 문서 hash·관측별 관련 매핑/승인/링크/규칙의 유효 상태·이미지 revision/fingerprint·정규화 버전으로 기존과 같은 평가 키를 계산한다. 후보와 같으면 정규화를 생략하고 원래 평가 ID를 유지하며, 다르면 해당 관측만 정규화한다. 관련 없는 승인 변경은 해당 관측의 키를 바꾸지 않으며, 유효일·만료일 경계를 넘으면 같은 입력 snapshot이어도 재평가한다.

재사용 후보 파일 자체는 완료 증거가 아니다. Silver writer lease 안에서 원천·현재 이미지 상태를 확인하고 실제 저장된 완료 영수증의 관측 연결·전체 hash·입력 fingerprint 및 테이블별 출력 proof를 대조한다. 기존 결과가 없거나 훼손됐으면 차단하고, 출력 검증 후 이미지 상태 재확인과 새 완료 영수증의 마지막 저장 순서를 유지한다. Gold는 재사용·신규 평가가 섞인 완전한 선택 목록으로 기존 이력을 포함해 검증한다. 전역 승인, 이미지 이용 정책, MySQL 활성화 조건은 바꾸지 않는다. 이전 `activation`/`activation-v2` 실행은 원래 입력을 보존해 재개하며 새 실행부터 v3를 사용한다.

CLI `--reuse-selection`은 봉인된 동일 window의 관측 후보 파일을 받으며 `--selection --batch-writes`가 함께 필요하다. 후보 hash는 NORMALIZATION 등록 범위와 discovery scan에 포함하고 입력 파일도 archive한다. `normalization_skipped` 및 runtime의 `silver_normalization_skipped`/`silver_normalizations`가 관측 수를 기록한다. 무결성 검증은 생략하지 않으므로 정규화 생략 건수와 전체 속도 개선율은 다르다.

로컬 관련 회귀 183개, 서버 132개가 통과했다. 같은 상품의 두 관측 중 한쪽 이미지 revision 변경 시 해당 관측만 정규화하는지, 관련 없는 승인 변경·관련 승인·만료일 변경·결과 누락·실행 중 이미지 변경·잘못된 관측 연결을 검증했다. 이전 완료 원문·평가 ID가 유지되는 것도 확인했다.

운영 연속 처리 재개는 기존 Driver/Spark 작업 종료·만료·잠금 본문 일치를 새로 확인한 뒤 Bronze 잠금 4개를 트랜잭션으로 해제했다(`lease-recovery-20261020/*-prepared.json`, `*-released.json`). 2026-10-20 23:43 KST에 기존 체크포인트로 연속 처리를 시작했다. `pause-after-window.json` 및 `pause-after-batch.json`은 `perf-20261020/`에 보존한 뒤 정지 설정을 해제했으며 25건 window 설정은 유지했다. 백그라운드 프로세스는 Spark Master에서 실행되므로 조작 PC의 세션 종료에 의존하지 않는다. 이후 새 장애가 생기면 복구 대기로 중단하며 성공을 가정하지 않는다.

단계별 조회 도구는 서버 root의 `pipeline_progress.py`다. 다른 PC에서는 Spark Master에 접속한 뒤 아래 명령을 실행한다. 상태와 원천 결과 파일은 다른 관측 시점일 수 있으므로 출력의 시각을 함께 확인한다. 새 시도 완료 전에는 Bronze 결과 파일에 이전 실패 기록이 남아 있을 수 있다.

```bash
"$PY" "$ROOT/pipeline_progress.py" --root "$ROOT" \
  --measurement "$ROOT/perf-20261020/continuous-observation-request.json"
# 실제 업무 DB 건수도 필요할 때만 --mysql 추가
```

배치별 단계 시간은 `epochs/<epoch>/windows/<window>/*-result.json`, 최신 완료 window 요약은 `windows-progress.json`의 `last_batch`다. 조회 도구는 이번 측정 시작 이후 첫 활성화 완료 배치의 관측/상품 수·단계별 소요 시간·정규화 생략 수를 `perf-20261020/first-measured-batch.json`에 한 번 보존한다. 합계는 단계 실행 시간의 합이며 재시작 대기 등을 포함한 전체 경과 시간과 구분한다. 첫 측정을 위해 다음 배치를 정지시키지 않는다.

#### 이번 실실행에서 추가로 발견한 Bronze 전환 오류

2026-10-21 추가 조사에서 Worker02/03의 작업별 AWS SDK JAR 복사본이 디스크를 소진한 것도 확인했다. 원본 체크섬과 실행 중인 앱 부재를 확인한 뒤 중복을 정리했고, 이후 [공용 JAR 직접 사용 및 Worker 자동 정리](garage_v241_profile.md#worker-jar-누적-방지-2026-10-21-적용)를 3개 Worker에 적용했다. 별도 읽기 전용 앱 두 개는 통과했다. 이 작업으로 업무 연속 처리를 재개하거나 기존 실패를 완료 처리하지는 않았다.

첫 재개에서는 Kafka batch 3의 100건이 저장됐고 격리 0건, callback 처리 시간은 1,044,250ms였다. 하지만 `await_window`가 목표 배치의 Spark progress를 확인해 정지할 때 다음 batch 4가 시작되어 취소됐다. 이 취소가 `StreamingQueryException`으로 전달돼 정상 인계 대신 Bronze 재시도가 발생했다. 이 17분 24초는 Bronze 저장 시간이며 Silver·Gold 개선 후 전체 시간으로 해석하지 않는다.

수정한 경계 제어는 목표 수의 비어 있지 않은 callback 처리가 끝난 뒤 다음 callback의 I/O를 첫 줄에서 차단한다. 다음 callback을 정상 반환하면 미처리 offset이 commit될 수 있으므로 `BRONZE_WINDOW_BOUNDARY`로 종료한다. Driver는 Spark의 실제 `lastProgress`에서 목표 수의 비어 있지 않은 완료 배치와 바로 다음 boundary ID, query 종료 상태가 모두 확인되는 경우만 인계한다. callback 성공 로그만으로 인계하지 않으며, 원래 발생한 데이터/저장/종료 오류나 확인되지 않은 경계는 숨기지 않는다. 다음 배치의 offset은 체크포인트에 미완료로 남아 다음 epoch에서 재개한다.

중단 이후 또 다른 시도에서는 잠금 갱신 중 `NetworkTimeout`이 6,496ms 후 발생했다. 클라이언트 socket 제한과 서버 `majority+j` writeConcern 제한이 모두 5,000ms여서 응답을 받을 여유가 부족한 설정을 확인했다. 클라이언트 socket 제한만 15,000ms로 보완했고, 저장 확인 제한 5,000ms·lease TTL 120초·heartbeat 10초·서버 시각 만료 판정·자동 인수 금지·retryWrites=False는 유지했다. 서버의 정확한 지연 원인까지 확정한 것은 아니며 장시간 서버/네트워크 장애는 계속 안전 중단한다.

운영 완료 상태의 수동 복원은 자동 승인 검토에서 차단되어 실행하지 않았다. 준비하던 수동 복원 스크립트도 제거했다. 원래 Kafka 체크포인트·실패 이력·불변 저장 결과는 유지하고 기존 실행 경로로 재개했다. 최종 재개 시각은 2026-10-21 00:16 KST이며 25건 후속 window와 연속 처리 설정을 유지했다. 로컬 관련 테스트 221개 및 서버 156개를 통과했고 timeout 설정 관련 36개도 추가 통과했다. 별도 Spark 생성 데이터 시험은 업무 실행 자원 우선 배정을 위해 중단했으며 실제 통합 검증 통과로 계산하지 않는다. 이후 실제 업무 배치의 단계별 결과로 경계 인계 및 후속 게시를 확인한다.
