# 데이터 파이프라인

다나와 수집 결과를 받아 이미지·MongoDB → Kafka → Spark Bronze/Silver/Gold → MySQL 상품 게시로 연결한다. 완료 export와 수집 중 확정된 증분 파일을 입력으로 사용하며, 크롤러 실행·파서·이미지 다운로드는 수집기 책임이다.

2026-10-21 최종 확인에서는 실제 업무 DB의 **상품 234건 / ACTIVE 213건 / PENDING_REVIEW 21건**을 확인했다. Kafka 보관 만료 복구 후 한 배치는 Bronze 100이벤트와 후속 25관측을 처리하고 MySQL 게시·활성화 평가까지 31분 17초에 완료했다. 한 배치 종료 후 `PAUSED_AFTER_BATCH` 상태이며, **전체 65,498개 상품 처리 완료나 현재 연속 실행 중이라는 의미는 아니다.** 이는 확인 시점의 기록이며 현재 상태는 서버에서 조회한다.

운영 경로는 작은 확정 배치마다 Bronze → Silver → Gold → MySQL 게시 → 활성화 평가를 마친 뒤 다음 범위로 진행한다. Spark 클러스터와 단계 간 세션·고정 테이블 읽기를 재사용한다. Kafka 보관 만료는 원장의 고정 이벤트 재발행과 별도 체크포인트 세대로 복구하며, 기존 ID·체크포인트·성공 영수증은 보존한다. 처리 속도, 전체 범위 완주, NiFi 운영 일정과 환경 이전 인수는 남아 있다. [현재 상태와 검증 근거](docs/service_validation_20261018.md), [변경 이력](docs/implementation_history.md)을 따른다.

## 문서 읽는 순서

1. [문서 안내](docs/README.md): 목적별 필요한 문서와 기준 우선순위.
2. [서버 배포·인계](docs/deployment.md): 실행 위치, 현재 보호 설정 경로, 설치·권한·배포·재개.
3. [전체 FLOW 실행](docs/flow_execution.md): INGEST → BRONZE → SILVER → GOLD → PREPARE → PUBLISH → RECONCILE.
4. [이슈 #13 요구사항 대조](docs/issue13_acceptance.md): 구현·실제 검증·남은 인수의 구분.

데이터·복구 계약과 단계별 명령은 문서 안내에서 찾는다. 과거 계획/접속 실패를 현재 상태와 나란히 나열하지 않으며, 현재 검증 상태는 실서버 검증 문서 한 곳에서 관리한다.

## 배포된 서버 확인·재개

기존 Bastion SSH alias·서버 권한이 준비된 PC에서 파이프라인 디렉터리 기준으로 실행한다. 서버의 보호 설정·작업 상태를 사용하므로 수집 PC의 상주 프로세스에 의존하지 않는다.

```powershell
python run_server_pipeline.py status
python run_server_pipeline.py preflight
python run_server_pipeline.py downstream  # 저장된 Bronze부터 처리
python run_server_pipeline.py flow        # 원천·이미지 인계와 Kafka부터 연결
```

재개 전에 서버의 `pause-after-window.json`과 현재 실행 여부를 확인한다. 정지 요청이 있으면 한 배치 뒤 다시 정지한다. 체크포인트나 영수증을 삭제해 재개하지 않는다. 서버 코드·보호 환경·제어 모듈 배포 절차는 [배포 가이드](docs/deployment.md), offset 범위 오류는 [Kafka 복구](docs/kafka_retention_recovery.md)를 따른다. 위 명령을 실행해야 새 작업이 시작되며 이 문서 업데이트 자체가 서버를 재가동하지는 않는다.

## 로컬 설치와 첫 확인

파이프라인 디렉터리에서 별도 가상환경을 만든다. 아래는 로컬 Windows 기준이며, VM의 Spark 환경은 [Garage 실행 프로필](docs/garage_v241_profile.md)의 서버 요구사항을 사용한다. 다른 OS로 Windows 가상환경을 복사하지 않는다.

```powershell
py -3.13 -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.lock.txt
.\.venv\Scripts\python.exe -m pip install --no-deps -e .
.\.venv\Scripts\python.exe run_pipeline.py --help
```

완료 export는 `prepare`로 별도 불변 snapshot을 만들고 `plan`으로 외부 쓰기 없이 검증한다. 파일 대역 시험은 `rehearse`, 실제 접속 점검은 `preflight`, 실제 적재는 `ingest`다. 정확한 인자와 재개 절차는 [입력·적재 실행](docs/execution.md)을 따른다. 수집 중 입력은 [LIVE 실행](docs/live_execution.md)을 사용한다.

설정 예시는 `config/`, 의존성은 역할별 requirements 파일에 있다. 실제 비밀번호·키·접속 URI는 Git 밖의 보호 설정/환경변수로 주입한다. 시험 원장을 운영 환경으로 조용히 전환하거나 checkpoint·ID를 삭제하여 충돌을 우회하지 않는다.

## 책임 범위

저장된 상품 입력·이미지 객체·불변 원천·이벤트·Delta·상품 MySQL 게시·활성화 평가·복구까지 담당한다. 운영자가 지정한 내부 프로젝트 정책의 매핑·검수 등록은 별도 reviewer 계정과 불변 감사 이력으로 수행한다. 일반 publisher의 승인 원장 수정 권한을 확대하지 않으며 분류 충돌·원천 불일치·이미지 미확보는 대기를 유지한다. ACTIVE와 임베딩·인덱스 기반 추천 준비는 별도다.

크롤러의 다나와 요청·이미지 다운로드, 인프라 운영, 사용자 입력·해석·임베딩·추천·UI·서비스 작업 원장 연동은 이 실행기의 책임 밖이다. 파이프라인은 확보된 이미지 바이트를 검증해 객체 저장소에 적재하며 다나와에서 이미지를 대신 다운로드하지 않는다. MongoDB VM별 설치 도구는 별도 [인프라 도구](../../ops/mongodb/README.md)로 보존한다.

상위 설계는 [프로젝트 파이프라인 설계](../../docs/design/02-data-pipeline.md), 세부 정합성은 [아키텍처 대조](docs/architecture_alignment.md)를 따른다.
