# Kafka → 상품 Bronze 구현 계획

2026-10-16. 사용자가 승인한 1번 작업의 실행 계획이다. 기존 data_contract_v1 216절과 design_amendment_v1_1 4절, 상위 02/05 및 원천 사전을 따른다. 새로운 인프라 배정·수집기 변경·Silver 처리는 하지 않는다.

목표: Spark Structured Streaming이 product.raw.collected를 수신하고 primary/majority Mongo 원천을 검증한 뒤 Bronze Delta에 insert-only MERGE한다. 완료된 Delta 원천·이벤트 receipt·격리 기록만 배치 성공으로 반환한다. 수동 Kafka offset commit은 사용하지 않는다.

구조: bronze/contracts.py(검증), batch.py(배치 처리), delta.py(Delta 영속성), mongo.py(읽기 전용 원천), config.py(경로/환경), stream.py(Spark Kafka 실행), cli.py/진입 스크립트. 핵심 로직은 Spark 없는 대역 테스트로도 검증한다.

제약: 원천/이벤트 해시 계약 불변, 실제 서버 미접속, 단일 작성자 Driver 운영, 한 배치 입력·Mongo 원천 크기 상한, 오류 발생 시 checkpoint 삭제 금지, 비밀정보 로그 금지, 기존 Git index/크롤러/상위 설계 보존. Spark/Delta는 별도 선택 의존성으로 유지한다. 서버 버전 확정은 인프라 담당과 맞추며 로컬 검증 조합을 서버 설치 완료로 표현하지 않는다.

- [x] 1. 원천·이벤트 검증 및 배치의 실패 테스트 작성/실패 확인. 정상 보존, 변조/키 불일치, poison, 원천 미확인·인프라 실패 구분.
- [x] 2. 최소 배치 구현. 원천/이벤트 중복·충돌, 격리 실패·저장 후 중단 재실행 검증.
- [x] 3. Mongo 읽기·Delta insert-only 및 Kafka foreachBatch 실행 구현. 원천→receipt→격리의 부분 commit은 재실행으로 수렴. 승인 없는 원천 UPDATE 없음.
- [x] 4. 선택 Spark/Delta 의존성·환경설정·실행 문서, 전체 회귀 테스트, 가능한 로컬 Delta 실제 동작 시험. 실제 Kafka/Mongo/S3 통합 인수는 별도 표시.
- [x] 5. 최종 코드 검토 및 구현 상태 갱신. 커밋·푸시 없음.

검토 초점: 같은 event ID 다른 본문, 같은 raw ID 다른 원천, 원천 미확인을 poison으로 버리지 않기, 격리 저장 실패 후 배치 성공 금지, Delta 원천 commit 후 receipt 전 종료 시 중복 생성 금지.

실행 기록: 기존 설계 승인과 이번 개발 지시에 따라 inline 진행한다. 재승인·worktree 생성·커밋을 추가하지 않는다. 한 Driver가 각 테이블을 소유하는 운영 전제는 유지한다.

판정 기록:

- 초기 전체 적재는 기존 prepare/ingest 이후 available-now로 적체 이벤트를 처리한다. 이후 같은 checkpoint로 계속 수신한다. 원천 이중 입력 경로·별도 수동 offset은 도입하지 않는다.
- 검토에서 Kafka 클러스터 변경에 따른 checkpoint 오사용을 발견해 실제 cluster ID 선확인·대상 fingerprint를 보강했다. 확인된 원천의 구조 오류는 poison이고 원천 미확인/DB 장애는 재시도임을 실패 재현 테스트로 구분했다.
- 로컬 Delta CREATE에서 file:///와 Hadoop file:/ 표현 차이를 재현해 Hadoop 경로로 통일했다. Windows Python worker가 종료되는 문제를 확인해 이미 Driver에 있는 JSON을 Spark SQL from_json으로 전달한다. Spark Executor Python worker에 원문을 다시 직렬화하지 않는다.
- 전체 테스트 69개 통과(29.594초), 실제 Spark/Delta 원천 3건·이벤트 3건·격리 2건 보존 및 반복/새 Sink 재개 검증. 상세 증거는 bronze_execution.md 참조. 실제 서비스 미접속.
