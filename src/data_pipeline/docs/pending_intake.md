# 실제 상품의 검수 대기 적재

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

확정 매핑이 없으면 Silver 입력은 `NOT_PROVIDED`, Gold는 품질 대기를 유지한다. MySQL FK를 위해 비활성 `PIPELINE_UNMAPPED` 범주를 사용한다. 이 범주는 실제 서비스 카테고리 매핑이 아니다. 원천 카테고리와 품질 사유는 고정 Gold·staging·source refs에서 추적한다.

단일 확정 갤러리의 원천 순서만 선택할 수 있으며 서로 다른 갤러리가 있으면 임의 병합하지 않는다. 이미지를 저장한 사실은 이용 승인과 다르다. 상품은 `PENDING_REVIEW`, 네 가지 readiness는 모두 false, 이미지 연결은 inactive/NOT_REVIEWED다. 대표 이미지·서비스 대표 가격·검증 링크·임베딩·판매 게시를 생성하지 않는다. 원문 가격과 URL의 증거는 Gold와 불변 staging에 남는다.

`prepare --pending-category-id <UUID>`로 만든 명시적 publication plan만 이 경로를 사용한다. 기존 plan의 기본 승인 요구사항은 그대로 적용한다. ACTIVE 상품이나 소유자가 변경한 상태·검수는 대기 적재가 덮어쓸 수 없다. 서비스 사용으로 전환하려면 담당자의 실제 승인·활성화가 필요하다.

검토된 상품·이미지는 대량 초기 적재에서 `SKIPPED_OWNER_MANAGED`로 따로 기록하고 계속 진행한다. 데이터 충돌·원천 변조 등 다른 오류를 이 상태로 감추지 않는다. 실제 매핑·이용 승인·게시 승인과 활성화는 [검수 절차 실행](review_workflow.md)을 따른다.

## 권한과 TLS

MongoDB·Kafka·S3·MySQL 인증서 검증을 유지한다. MySQL 공개 CA는 Spark 서비스 계정이 읽는 보호 경로에서 사용하며 인증서·호스트 이름 검증을 끄지 않는다.

`docs/server_setup/pipeline_privilege_audit.py`는 해당 환경의 publisher 계정 권한을 검사한다. pipeline_runs와 파이프라인 쓰기 테이블에 SELECT/INSERT/UPDATE, categories·guard·임베딩/인덱스 원장에는 SELECT만 요구한다. 보완은 Primary에서 명시적으로 적용하며 다른 두 노드에는 조회로 반영을 확인한다. GRANT OPTION이나 승인 원장 편집 권한을 추가하지 않는다.

등록의 SELECT FOR UPDATE에는 UPDATE 권한이 필요하다. 실행 중 확인된 1142 오류는 이 범위를 보완한 뒤 같은 등록 ID로 재개한다. 기존 입력 hash·등록·원문·성공 이력을 삭제하지 않는다.

서버 준비 도구 `pending_mysql_prepare.py`는 신규 대기 범주와 누락된 실행 등록 테이블만 다룬다. 기존 테이블을 재생성하거나 변형하지 않는다. 런타임 publisher는 DDL을 수행하지 않는다.

## 검증

`tests/test_publication_pending.py`는 미매핑/미검수 입력, 비활성 상태, 재실행 ID 유지, 부분 실패 롤백, ACTIVE 상품 보호, 원문·이미지 연결 대조를 검사한다. 실제 서버 결과는 해당 작업 경로의 단계별 result/failure와 publication receipt로 확인한다. 서버 연결 성공·일부 실제 상품 적재·전체 65,498건 적재 완료는 구분한다.
