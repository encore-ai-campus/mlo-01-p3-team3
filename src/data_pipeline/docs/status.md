# 파이프라인 제작 현황

2026-10-16. Issue #13 / feature/#13. 분리 기준은 origin/dev의 e35b2a3이다. 기존 feature/#6의 커밋이나 스테이징은 포함하지 않았다.

## 구현·검증 상태

| 영역 | 현재 상태 |
|---|---|
| FULL_EXPORT | 원천/정규화/media JSONL·실제 이미지 검증, 불변 스냅샷 봉인 구현 |
| 이미지·원천 적재 | S3 조건부 내용 주소 업로드, MongoDB 접수·불변 원천·lease·복구 구현 |
| Kafka 발행 | Mongo 저장 확인 후 고정 event ID/본문 발행·재시도·명시 replay 구현 |
| Bronze | Spark Kafka source → primary/majority 원천 검증 → Delta insert-only MERGE, receipt·격리·checkpoint 구현 |
| 초기 전체 적재 | prepare/ingest 이후 Bronze available-now로 적체 처리. 이후 같은 checkpoint로 증분 처리 |
| 테스트 | 기존 작업 폴더에서 전체 69개 통과, 로컬 실제 Spark 4.0.1/Delta 4.0.1로 3상품·3receipt·2격리 및 재실행 검증 |
| Silver·Gold | 필드 제안서 검토 단계. 정규화·재평가·게시 코드 미구현 |
| 자동화·서비스 | 수집기 자동 인계, NiFi 일정, LIVE, MySQL 게시 미구현 |
| 실제 서버 | Mongo/Kafka/S3/Spark 클러스터 종단 인수 미수행 |

테스트 코드는 함께 배포하지만 로컬 보관 데이터·이미지·실행 보고서·가상환경·비밀 설정은 Git에 올리지 않는다. 실제 보관 데이터 smoke는 해당 입력을 제공해야 재현 가능하다. 기본 단위 테스트는 저장소에 포함된 fixture로 실행한다. 서버 배포·초기 적재·재개 절차는 [README](../README.md#서버-배포-순서), Bronze 세부는 [실행 문서](bronze_execution.md)를 따른다.

## 기준 문서의 차이

분리된 dev의 상위 인프라 문서는 19대/역할별 논리 서버명이며, 이전 작업 폴더에서 검토한 상위 설계안은 모니터링을 포함한 20대/pj3-* 구성이다. 기존 pipeline 설계 문서의 20대·상위 대조·해시 근거는 **그 당시 검토한 작업 폴더 설계안**의 기록이다. dev의 현재 상위 문서가 이미 같은 내용으로 갱신됐다는 의미가 아니다.

이번 커밋은 파이프라인 폴더만 추가하고 상위 설계·데모·크롤러는 변경하지 않는다. 추천 알고리즘·서비스 스키마 차이는 이번 원천/Bronze 구현 범위에 끌어오지 않는다. 실제 배포 전 서버 역할·버전·endpoint를 확정하고 실제 인프라 인수를 수행해야 한다. Silver·Gold 개발 때 최신 dev의 서비스/필드 계약과 제안서의 스키마·상태·업무 키를 다시 대조한다.

현재 코드에는 VM IP/호스트 역할을 하드코딩하지 않았다. Mongo rs-products 3멤버, Kafka 토픽·실제 cluster ID, 이미지/lake endpoint와 권한은 배포 설정으로 연결한다.

크롤러는 dev에 아직 없으므로 문서의 크롤러 근거 링크는 최초 구현을 검증한 commit 67e837db4114ef8955a12c0b466f6f4aba50bd8f에 고정했다. 크롤러를 import하는 런타임 의존성은 없고 schema 1.2 완료 export를 읽는다.
