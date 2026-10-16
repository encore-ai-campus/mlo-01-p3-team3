# interior_recommender 역할별 라우팅

2026-10-16 코드 기준. 패키지 이름은 `interior_recommender`다. 이 문서는 실제 로컬 호출과 담당 코드를 찾는 진입점이며, 서비스 설계 기준은 [직접 추천 계약](../design/12-direct-recommendation.md)이다.

## 문서 계층과 읽는 순서

`프로젝트 README → docs/README → 이 라우팅 → 호출 구조 → 담당 역할 → 상세 설계/코드/검증` 순서로 읽는다. 요구사항은 [BRD](../BRD_3차.md)·[PRD](../PRD_3차_초안.md), 서비스 전체 설계는 [설계 라우팅](../design/index.md), 실행 명령은 [패키지 안내](../../src/interior_recommender/README.md)에 둔다.

| 담당·작업 | 먼저 읽을 문서 | 함께 확인할 문서 | 구현 위치 |
|---|---|---|---|
| 전체 호출·영향 분석 | [호출 구조](call-structure.md) | [전체 아키텍처](../design/00-architecture.md) | 아래 전체 계층 |
| 프런트엔드·HTTP·Canvas 연동 | [웹 진입과 연동](roles/web.md) | [Canvas 연결 계약](../../src/interior_recommender/web/INTEGRATION.md) | `web/` |
| 추천 실행·상태·재시도 | [실행 조정](roles/application.md) | [Application 설계](../design/04-application-ai.md), [복구](roles/operations.md) | `application/`, 공통 `pipeline.py` |
| AI 응답·조건·추천 순위·상품 링크 | [추천 규칙](roles/domain.md) | [직접 추천 계약](../design/12-direct-recommendation.md), [크기 정책](../design/11-scale-policy.md), [상품 연결](../design/09-product-links.md) | `domain/`, `adapters/` |
| 수집 원장·규격 정제·사전 벡터 | [카탈로그 준비](roles/catalog.md) | [데이터 파이프라인](../design/02-data-pipeline.md), [추천 서버](../design/07-ad-server.md) | `catalog/` |
| 설정·외부 API·보존·회귀 검증 | [운영과 검증](roles/operations.md) | [운영 설계](../design/06-operations-migration.md), [Kafka 설계](../design/05-kafka-jobs.md) | `config.py`, `assets.py`, `pipeline.py`, `adapters/`, `tests/` |
| 기존 생성 CLI·웹 유지 | [기존 생성 경로](roles/legacy.md) | [전환 경계](../design/08-template-generation.md) | `cli.py`, `web/workflow.py`, `pipeline.py` |

## 갱신 규칙

- 호출 순서·실행 모드·모듈 위치를 변경하면 [호출 구조](call-structure.md), 해당 역할 문서와 이 표를 함께 갱신한다.
- API·입출력·상태·복구 변경은 웹/실행/운영 담당 문서를 함께 확인한다. 스키마와 서비스 계약 변경은 기존 설계 라우팅으로 돌아가 관련 설계·속성 사전도 갱신한다.
- 구현의 기준 경로는 새 역할 패키지다. 기존 모듈은 외부 import와 명령 호환용으로 유지하고 신규 내부 호출은 새 경로를 사용한다.
- 기존 상세 설계 파일명과 앵커는 유지한다. 이 폴더에는 코드 책임·호출·검증 위치를 두고 서비스 정책 전문을 복제하지 않는다.
- 로컬 파일/스레드풀 구현과 서비스 DB·Kafka·Bronze·FAISS 운영 배포를 구분한다. 코드 이동으로 API 횟수나 처리량이 개선되었다고 표현하지 않는다.

이번 계층화는 Python 모듈 배치와 호출 경로를 정리한다. `Pipeline`·`Studio`에 남아 있는 공통 기반과 생성 전용 기능의 결합은 [호출 구조의 공통 경계](call-structure.md#공통-경계와-남은-결합)에 명시한다.
