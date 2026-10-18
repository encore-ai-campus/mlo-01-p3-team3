# 데이터 파이프라인

다나와 수집 결과를 받아 이미지·MongoDB → Kafka → Spark Bronze/Silver/Gold → MySQL 상품 게시로 연결한다. 완료 export와 수집 중 확정된 증분 파일을 입력으로 사용하며, 크롤러 실행·파서·이미지 다운로드는 수집기 책임이다.

현재 소량 실서버 시험은 3상품·9이미지 관계의 적재/재개, Worker 3대, Silver/Gold 품질 대기·재사용, MySQL 문서 계약, 제한 SSH와 NiFi timer까지 확인했다. **실제 업무 DDL·등록·검수 입력을 사용하는 전체 FLOW와 운영 일정 인수는 남아 있다.** 자동 테스트는 최종 252개 통과이며, NiFi 검증 프로세서는 STOPPED다. 상세 범위와 근거는 [실서버 검증·남은 인수](docs/service_validation_20261018.md)를 따른다.

## 문서 읽는 순서

1. [문서 안내](docs/README.md): 목적별 필요한 문서와 기준 우선순위.
2. [서버 배포·인계](docs/deployment.md): 실행 위치, 현재 보호 설정 경로, 설치·권한·배포·재개.
3. [전체 FLOW 실행](docs/flow_execution.md): INGEST → BRONZE → SILVER → GOLD → PREPARE → PUBLISH → RECONCILE.
4. [이슈 #13 요구사항 대조](docs/issue13_acceptance.md): 구현·실제 검증·남은 인수의 구분.

데이터·복구 계약과 단계별 명령은 문서 안내에서 찾는다. 과거 계획/접속 실패를 현재 상태와 나란히 나열하지 않으며, 현재 검증 상태는 실서버 검증 문서 한 곳에서 관리한다.

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

저장된 상품 입력·이미지 객체·불변 원천·이벤트·Delta·상품 MySQL 게시·복구까지 담당한다. 인프라 설치, 승인/매핑 원장 편집, 사용자 입력·해석·임베딩·추천·UI·서비스 작업 원장 연동은 다른 담당자의 범위다. 확정 입력이 없으면 품질 대기 또는 실행 차단하며 승인을 대신 생성하지 않는다.

상위 설계는 [프로젝트 파이프라인 설계](../../docs/design/02-data-pipeline.md), 세부 정합성은 [아키텍처 대조](docs/architecture_alignment.md)를 따른다.
