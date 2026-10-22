# 설계 문서 안내

2026-10-15 현행 흐름은 direct-recommendation-v3이다. 먼저 [BRD](../BRD_3차.md)·[PRD](../PRD_3차_초안.md)와 [담당별 추천 다이어그램](12-direct-recommendation.md#담당별-추천-흐름)을 읽는다. 문서 변경과 실제 구현 완료를 구분한다.

| 작업 | 먼저 읽을 문서 | 함께 확인할 문서 |
|---|---|---|
| 새 상품 실행기·승인/활성화 없는 직접 게시 | [v2 실행기](../../src/data_pipeline_v2/README.md) | [책임·처리 계약](../../src/data_pipeline_v2/docs/architecture.md), [서버 이전](../../src/data_pipeline_v2/docs/server-migration.md), [게시 계약](13-gold-mysql-publication.md) |
| 추천 이후 선택 상품 이미지 생성 | [상품 배치 미리보기](../interior-recommender/product-preview.md) | [Application](04-application-ai.md), [직접 추천](12-direct-recommendation.md) |
| SketchOn SQLite 워커·동기/비동기·복구 | [실행·복구](../interior-recommender/execution.md) | [구현 라우팅](../interior-recommender/README.md), [Application](04-application-ai.md), [작업](05-kafka-jobs.md) |
| interior_recommender 구현·호출 계층 | [역할별 구현 라우팅](../interior-recommender/README.md) | [호출 구조](../interior-recommender/call-structure.md), 아래 담당 설계 |
| 전체 경계·변경 범위 | [직접 추천 계약](12-direct-recommendation.md) | [아키텍처](00-architecture.md) |
| 상품 파이프라인 구현·책임 계층·호출 변경 | [책임별 코드·문서 라우팅](../../src/data_pipeline/docs/code-routing.md) | [파이프라인](02-data-pipeline.md), [보강 계약](../../src/data_pipeline/docs/design_amendment_v1_1.md), [데이터·복구 계약](../../src/data_pipeline/docs/data_contract_v1.md) |
| AI 입력 해석·코드 검증 | [Application](04-application-ai.md) | [크기 근거](11-scale-policy.md), [작업](05-kafka-jobs.md) |
| 상품 검색·콘텐츠 추천·후속 평가 | [광고 서버](07-ad-server.md) | [파이프라인](02-data-pipeline.md), [평가 계약](12-direct-recommendation.md#이번-scikit-learn과-후속-내재화) |
| 데이터·ORM/DDL 계획 | [직접 추천 속성](data-dictionary/09-direct-recommendation.md) | [데이터 모델](03-data-model.md), [전체 ERD](03-erd-full.md), [간략 ERD](03-erd-summary.md) |
| Gold→MySQL 필수 컬럼·게시·추천 입력 연결 | [게시 필드 계약](13-gold-mysql-publication.md) | [상품 속성 사전](data-dictionary/02-products.md#gold-게시-필수-확장-컬럼), [데이터 모델](03-data-model.md), [게시 실행](../../src/data_pipeline/docs/mysql_publication.md) |
| Bronze·메시지·실패 복구 | [Kafka](05-kafka-jobs.md) | [운영](06-operations-migration.md) |
| 실버·골드 필드·테이블·준비 상태 | [Delta 필드 계약](data-dictionary/01-raw-delta.md#실버골드-필드-계약-적용-범위) | [파이프라인](02-data-pipeline.md#실버골드-필드-적용), [원장 대응](03-data-model.md#실버골드와-서비스-원장-대응), [검수](06-operations-migration.md#실버골드-필드-검수) |
| 상품 출처·규격·가격·URL | [상품 연결](09-product-links.md) | [속성 사전](data-dictionary/index.md) |
| 인프라·DB 복제·이전 | [인프라](01-infrastructure.md) | [DB 복구](06-operations-migration.md#db-복제와-장애-복구) |
| 생성 경로 폐기·기존 데이터 | [전환 경계](08-template-generation.md) | [이전 도매꾹 자료](10-domeggook-reuse.md), [운영](06-operations-migration.md) |

## 설계 갱신 규칙

작업 범위가 바뀌면 해당 라우팅을 다시 확인한다. 요구사항은 BRD·PRD, 새 흐름의 통합 계약은 12-direct-recommendation.md, 상세 역할/스키마는 각 문서에 함께 반영한다. 변경한 스키마·API·메시지를 이미 배포한 것으로 표현하지 않는다. 사용자 절차를 추가하지 않고 요청 조건과 공간 관찰·실측과 추정·운영과 오프라인 실험을 구분한다.

이전 접힘 영역의 생성·템플릿·scene_object 필수 연결과 SVG/PNG를 현행 흐름의 기준으로 사용하지 않는다. 기존 링크/DB/원천 보존 공통 계약은 충돌하지 않는 범위에서 유지한다. 파일명/기존 앵커는 이력 링크를 위해 보존한다.

## 후속 내재화

초기 추천은 라벨 없이 scikit-learn `cosine_similarity`와 수동 초기 가중치로 수행한다. `weighted-content-v1`은 선호 색상이 있으면 `0.85 × 유사도 + 0.15 × 색상 일치`, 없으면 유사도만 사용한다. 필수 조건 불일치는 제외하고 결측·미지원 필수 조건은 UNKNOWN 목록으로 분리한다. 가중치는 학습값이 아니며 점수는 확률이 아니다. 사람의 6만 이미지 전수 적합/부적합 라벨링·LogisticRegression 학습·직렬화 모델 파일은 초기 실행 조건이 아니다. 반응 자료를 충분히 확보한 뒤 지도학습을 별도 평가한다.
