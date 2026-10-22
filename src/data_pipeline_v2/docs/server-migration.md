# 로컬에서 서버로 이전

현재 단계는 새 실행기의 로컬 개발이다. 이 문서는 다음 서버 이전 작업의 절차이며 **서버 적용 기록이 아니다**. 접속 정보는 저장소 바깥의 `LOCAL_INFRA_ACCESS.md`와 기존 운영 문서에서 확인하고 배포 묶음에 포함하지 않는다.

## 소스 이전 묶음

`develop`에서 실행한다. 출력 경로는 기존 파일이 없어야 한다.

```powershell
python -B src/data_pipeline_v2/tools/release.py build C:/Temp/product-pipeline-v2.zip
python -B src/data_pipeline_v2/tools/release.py verify C:/Temp/product-pipeline-v2.zip
```

묶음에는 `v2/`의 코드·문서·설정 예제와 `legacy/`의 사실 파서/연결 기반 소스·기존 의존성 잠금 3종을 넣는다. 의존 모듈의 전이 import가 있으므로 legacy Python 패키지 전체를 동봉하지만 새 실행기는 허용한 파서/연결 모듈만 호출한다. 기존 운영 CLI를 새 실행기로 실행하는 방식은 아니다. 가상환경, 실제 설정, 데이터, 캐시, DB dump, 이미지, 모델, 임베딩은 제외한다.

`manifest.json`은 각 소스 바이트의 SHA-256과 기준 Git HEAD를 기록한다. 작업 트리의 내용이 기준 커밋과 같다고 가정하지 않는다. 서버에서 ZIP을 풀기 전에 전송한 파일의 SHA-256을 별도 대조하고 `release.py verify`로 파일 구성과 해시를 확인한다. manifest 자체의 인증 서명은 아니므로 신뢰한 전송 경로와 별도 전달한 해시가 필요하다.

## 서버 사전 준비

1. 검토·커밋한 소스를 새 버전 디렉터리로 옮긴다. 기존 서비스 경로와 v1의 Delta/checkpoint/state를 보존한다. 서버 venv를 복사하지 않고 서버의 검증된 Python/잠금 의존성을 사용한다. v2는 Python 3.11~3.14, 기존 파서 의존성과 PySpark/Delta 4.0.1, PyMySQL 1.1.2를 기준으로 한다. 서버 Java·JAR·CA·Garage 연결은 기존 환경의 실제 설치 버전과 먼저 대조한다.
2. 서버 `PYTHONPATH`에 해시를 검증한 `v2`와 `legacy`를 지정한다. 기존 설치 패키지가 의도치 않게 우선하지 않는지 `pipeline_v2.__file__`, `product_pipeline.__file__`을 출력해 확인한다. 패키지 설치 방식이면 두 로컬 소스를 같은 검증된 venv에 설치하고 dependency 해석으로 다른 0.1.0 배포본이 섞이지 않게 한다.
3. `server.example.json`을 서버의 보호된 설정 경로로 복사하고 모든 `replace-with-*`를 실제 논리 target, 원천 run, DB 정보로 바꾼다. 첫 canary는 `mongo.source_product_ids`에 원천에 실제 존재하는 상품 ID 1개를 지정한다. 해당 상품의 고정 run 내 진입 여부를 확인하며, 정제 시에는 그 상품의 전체 관측 이력을 읽는다. 전체 범위 실행에서만 이 값을 `null`로 둔다. 상태 디렉터리는 지속 로컬 디스크에 둔다. `lake_root`는 새 `/direct-v2` 경로로 제한한다. 서버 설정에 Windows 경로를 넣지 않는다.
4. URI·MySQL 비밀번호·CA 경로는 예제의 `*_env`가 지정한 환경변수에 둔다. Spark 옵션은 `PIPELINE_V2_SPARK_OPTIONS` JSON, Garage는 기존 `PIPELINE_SPARK_S3_PROFILE=garage_v241`, HTTPS endpoint·공통 JAR·CA truststore·credentials 파일을 사용한다. 키를 Spark 옵션이나 명령 인자로 직접 넣지 않는다. lease 컬렉션에 TTL 인덱스를 만들지 않는다.
5. MySQL 기존 참조 스키마·카테고리 FK·안정 키를 읽기 검사하고, 기존 상품마다 `pipeline_product_versions` baseline이 있는지 확인한다. 누락되면 임의 baseline을 만들지 않고 이전 게시 근거로 별도 정합성을 확인한다. 새 실행기는 DDL이나 검수 원장 쓰기를 하지 않는다.

## 전환과 인수

1. 기존 운영 스케줄/timer를 중단하고 기존 coordinator·자식 프로세스·Spark 작업이 끝난 근거를 확보한다. 구버전과 신버전의 서비스 DB 동시 쓰기는 금지한다. 새 namespace의 lease만으로 구버전의 서비스 DB 쓰기까지 막힌다고 가정하지 않는다.
2. 새 DB 스냅샷/복구 지점과 기존 서비스 행·게시 baseline을 기록한다. 기존 결과의 입력 ID·checksum·버전·완료 manifest를 먼저 확인하고 재사용/재생성 범위를 결정한다. 임베딩/상품 카탈로그/검색 인덱스의 중복 생성은 별도 판단 전 실행하지 않는다. 새 실행기는 이 산출물을 자동 생성하지 않는다.
3. 제한한 실제 수집 run으로 `capture --scope server-canary-001`을 실행한다. Mongo를 읽고 로컬 상태를 봉인하며 Spark나 MySQL 쓰기를 시작하지 않는다. 해당 상품의 전체 이력 수가 예상과 맞는지 확인한다.
4. `tick --scope server-canary-001 --apply`로 실행한다. Gold와 게시를 각각 확인하고 `status`로 proof를 검증한다. 이력 101건·결측·기존 상품 갱신·응답 유실·재개를 제한 범위에서 점검한다. 정상 처리 상태와 차단 사유를 구분한다.
5. 기존 추천 카탈로그 소비자의 schema v2 Gold 참조·승인 이미지 전제를 새 `schema_version=3` 직접 게시 계약에 맞춰 전환/검증한다. `ACTIVE`만 보고 검증되지 않은 모델 결과를 추천 가능하다고 취급하지 않는다. 이 어댑터 변경은 본 실행기 구현에 포함되지 않았다.
6. 동일 입력·동일 서버 자원으로 기존/새 경로를 분리해 비교한다. 전체 처리 시간, raw 본문 조회 수/바이트, Spark 실제 스캔 파일/바이트·commit 수, schema 검사 SQL 수, 서비스 저장 SQL 수, Gold 지연·게시 지연·대기 바이트를 기록한다. 새 namespace 최초 생성 비용과 재실행 비용을 구분한다.
7. 서버 동작·소비자 인수가 끝난 뒤 새 명령을 기존 실행 제어의 단일 진입점으로 등록한다. 새 실행기는 별도 daemon이나 timer를 자동 설치하지 않는다.

서버 명령은 다음 형식이다. 정해진 scope 하나를 끝까지 실행하고 다음 수집 run에는 새 scope를 만든다.

```bash
python -B /opt/pj3/pipeline-v2/releases/RELEASE/v2/run_pipeline.py --config /etc/pj3/pipeline-v2.json capture --scope server-canary-001
python -B /opt/pj3/pipeline-v2/releases/RELEASE/v2/run_pipeline.py --config /etc/pj3/pipeline-v2.json run --scope server-canary-001 --apply --max-ticks 100
python -B /opt/pj3/pipeline-v2/releases/RELEASE/v2/run_pipeline.py --config /etc/pj3/pipeline-v2.json status --scope server-canary-001
```

## 중단과 복구

다음 tick 호출을 중단한 뒤 현재 프로세스/Spark 종료를 확인한다. `RUNNING`과 lease 불명확 상태는 정지 증거 없이 초기화하지 않는다. `recover --job HASH --stop-evidence 실제근거`는 기록을 남기고 안전한 재검증·인계를 다시 시도한다. lease 복구는 기존 lease 운영 절차로 별도 처리하며 state recover만으로 다른 서버의 lease를 지우지 않는다.

롤백은 실행기 경로만 되돌리는 것으로 끝나지 않는다. v3 출처가 들어간 서비스 DB를 구버전 감사·추천 어댑터가 해석할 수 있는지 확인하고, 필요하면 보존한 DB 복구 지점/게시 근거와 함께 되돌린다. 새 상태와 v2 Delta는 장애 증거로 보존하고 기존 v1 checkpoint를 새 실행기에 연결하지 않는다.
