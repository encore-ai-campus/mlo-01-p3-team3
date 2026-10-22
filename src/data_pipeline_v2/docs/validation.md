# 로컬 검증 기록

2026-10-23, 로컬 `develop` 기준. 새 테스트의 합성 원천과 FileLake/FilePublisher, 기존 MySQL 필수 컬럼 계약을 검사하는 MemoryDB를 사용한다. 실제 서버·Garage·Delta JVM·MySQL TLS 종단 시험과 실제 적재 속도 비교는 하지 않았다.

새 실행기 테스트 **31개 통과**(28.330초). 추가한 상품 ID 제한은 Mongo run에 없는 ID를 거절하고 선택 상품의 전체 관측 이력만 탐색하는지 확인한다. 기존 실행기 전체 회귀는 동일 로컬 Python에서 633개 실행, 실패 5개·오류 8개·건너뜀 2개였다. `delta` 등 설치 의존성 누락, 기존 `test_driver_memory`의 SimpleNamespace 대역에 `_append_staged_chunk_inner`가 없는 오류, Kafka 사전 검사 오류 코드 불일치가 포함된다. 기존 실행기 Python 구현은 이번 작업에서 변경하지 않았으며 이 전체 회귀를 통과로 기록하지 않는다.

검증 명령:

```powershell
python -B -m unittest discover -s src/data_pipeline_v2/tests -v
```

검증 대상은 승인/활성화 없는 직접 게시, 각 단계별 영속 시간·scope 요약, 101관측의 전체 이력 Gold, 테이블별 묶음 commit, 무관한 정책의 작업 ID 유지, Gold 전용 정책 변경 시 Silver 재사용, 변경 없는 범위의 데이터 쓰기 0, 변경 상품만 정제, Gold→인계 사이 장애, 게시 응답 유실 후 멱등 재개, 정지 증거가 필요한 RUNNING, 오래된 입력 게시 차단, Gold 유실/변조 감지, 이력/용량 한도, 커서가 지난 차단 작업의 복구, SQL 필수 필드/이미지 연결/서비스 값 변조 감사, 스키마 검사 6조회·세션 내 재사용, CLI와 의존 방향, 소스 이전 묶음이다.

성능 관련 회귀는 **동일 합성 원천의 논리 작업량**을 확인한다. 10상품 1관측을 한 묶음으로 처리할 때 데이터 추가 commit이 18회 이하이고, 완료한 동일 입력을 새 scope로 다시 실행할 때 raw 본문 조회·정제·데이터 commit·파일 게시가 증가하지 않아야 한다. 이는 서버의 실제 파일 스캔량이나 초당 적재량 측정이 아니다. FileLake는 로컬 시험용이며 실제 Delta 버전 보존/분산 I/O와 같지 않다.

`status` 또는 `run`의 최종 JSON에서 `timing`을 보고, 개별 단계 증거는 상태 저장 경로의 `metrics/stage-times/<scope-hash>/*.json` 묶음에서 확인한다. 같은 scope를 여러 번 tick/run 하면 이벤트 묶음은 추가되고 요약은 재생성된다. 한 이벤트는 inclusive 경과 시간이므로 부모와 자식 시간을 더해 전체 시간으로 만들지 않는다.

남은 서버 인수는 [이전 절차](server-migration.md)를 따른다. 특히 Mongo 현재 스키마·조회 인덱스, Delta 실행 계획과 물리 스캔량, 한 번의 session에 유지한 Spark/lease의 종료, MySQL 실제 트랜잭션과 제한 계정, 기존 상품 baseline·숫자/시간 변환, v3 출처 소비자 호환성, 기존 실행기와 동일 입력 비교가 필요하다. MySQL 서비스 반영은 상품별 트랜잭션과 안정 키별 SQL을 사용하므로 MySQL 자체의 전면 bulk-upsert 최적화가 끝났다고 주장하지 않는다.

실데이터나 비용 큰 산출물은 이 테스트를 위해 재생성하지 않았다. 기존 사용자의 `src/data_pipeline` 미커밋 문서·검증 JSON도 본 작업에서 변경하지 않았다.
