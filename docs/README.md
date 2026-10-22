# 3차 프로젝트 문서

2026-10-21: #34의 로컬 시연 MVP를 `src/local_demo/`로 분리하고 기존 `src/demo_sketchon/`은 PR 대상 dev 소스로 보존한다. **MVP 구현 당시 데이터 파이프라인 구축·연동 준비가 완료되지 않아 실제 파이프라인과 연동하지 못했고 고정 파일 카탈로그로 시연했다.** [분리 배경·실행·DB 연결 보존 기준](interior-recommender/local-demo.md)을 따른다.

2026-10-18 범위 확장: 추천 후 상품을 선택하면 외부 API로 [상품 배치 미리보기](interior-recommender/product-preview.md)를 생성한다. 아래 2026-10-15 이미지 생성 제외 결정은 선택형 미리보기 추가 이전 기준이다.

2026-10-18 추천 구현 갱신: 신규 로컬 요청은 [방 특징 추천](interior-recommender/interior-ranking.md)의 `interior-content-v3`를 기본으로 사용한다. 아래 v1/v2 수동 가중치 설명은 당시 정책이며 기존 저장 실행은 유지한다. MySQL 연결·운영 배포는 후속 작업이다.

2026-10-15 현행은 **이미지 생성 없는 공간 맞춤 상품 추천**, 설계 direct-recommendation-v3이다. scikit-learn CPU 콘텐츠 유사도와 수동 초기 가중합을 사용하며 전수 사람 라벨링이나 학습 모델이 초기 실행 조건은 아니다. 로컬 웹 구현과 전체 서비스 통합 상태를 구분한다.

| 목적 | 먼저 읽을 문서 | 함께 확인할 문서 |
|---|---|---|
| dev 원본 보존·로컬 시연 MVP 분리 | [시연 분리·미연동 배경](interior-recommender/local-demo.md) | [시연 실행](../src/local_demo/README.md), [서버 카탈로그](interior-recommender/server-catalog.md) |
| 추천 카탈로그의 MySQL·Garage·Delta 연결 | [서버 카탈로그 연결](interior-recommender/server-catalog.md) | [카탈로그 담당](interior-recommender/roles/catalog.md), [게시 계약](design/13-gold-mysql-publication.md) |
| 추천 이후 상품 배치 이미지 생성 | [상품 배치 미리보기](interior-recommender/product-preview.md) | [실행·복구](interior-recommender/execution.md), [직접 추천](design/12-direct-recommendation.md) |
| SketchOn SQLite 실행·복구·비동기/동기 경계 | [작업 실행](interior-recommender/execution.md) | [역할별 라우팅](interior-recommender/README.md), [호출 구조](interior-recommender/call-structure.md) |
| interior_recommender 코드·호출·담당별 변경 | [역할별 코드·문서 라우팅](interior-recommender/README.md) | [현재 호출 구조](interior-recommender/call-structure.md), [설계 라우팅](design/index.md) |
| 사업·사용자 범위 | [BRD](BRD_3차.md) | [PRD](PRD_3차_초안.md) |
| 전체 흐름·담당·내재화 단계 | [직접 추천 계약·다이어그램](design/12-direct-recommendation.md) | [아키텍처](design/00-architecture.md) |
| 역할별 기술 작업 | [설계 라우팅](design/index.md) | 담당 상세 문서 |
| AI와 코드 검증 | [Application](design/04-application-ai.md) | [크기 정책](design/11-scale-policy.md) |
| 검색·콘텐츠 추천·후속 평가 | [추천 서버](design/07-ad-server.md) | [파이프라인](design/02-data-pipeline.md), [운영](design/06-operations-migration.md) |
| 실버·골드 정제 필드·준비 상태 | [Delta 필드 계약](design/data-dictionary/01-raw-delta.md#실버골드-필드-계약-적용-범위) | [파이프라인](design/02-data-pipeline.md#실버골드-필드-적용), [원장 대응](design/03-data-model.md#실버골드와-서비스-원장-대응) |
| 데이터 변경 | [직접 추천 속성](design/data-dictionary/09-direct-recommendation.md) | [모델](design/03-data-model.md), [ERD](design/03-erd-full.md) |
| Gold→MySQL 테이블 생성·필수 컬럼·게시 연결 | [게시 필드 계약](design/13-gold-mysql-publication.md) | [상품 속성](design/data-dictionary/02-products.md#gold-게시-필수-확장-컬럼), [게시 실행](../src/data_pipeline/docs/mysql_publication.md) |
| 상품 파이프라인 실행·배포·복구·실서버 계측 | [구현 README](../src/data_pipeline/README.md) | [반복 계측 절차](../src/data_pipeline/docs/performance_measurement.md), [배치 시간 비교](../src/data_pipeline/docs/verification/batch_timing_comparison_20261022.md), [현재 검증·남은 인수](../src/data_pipeline/docs/service_validation_20261018.md), [작업 이력](../src/data_pipeline/docs/implementation_history.md) |
| 상품 파이프라인 코드·책임 계층·리팩토링·계측 변경 | [책임별 코드·문서 라우팅](../src/data_pipeline/docs/code-routing.md) | [반복 계측 절차](../src/data_pipeline/docs/performance_measurement.md), [설계 라우팅](design/index.md), [데이터·복구 계약](../src/data_pipeline/docs/data_contract_v1.md), [보강 계약](../src/data_pipeline/docs/design_amendment_v1_1.md) |
| 상품·링크 | [상품 연결](design/09-product-links.md) | [광고 서버](design/07-ad-server.md) |
| 인프라·보존·복구 | [인프라](design/01-infrastructure.md) | [Kafka](design/05-kafka-jobs.md), [운영](design/06-operations-migration.md) |

## 현행 설계와 구현 상태

기준 요구사항은 이 폴더의 BRD·PRD이며 후속 기술 계약은 direct-recommendation-v3이다. 기존 문서의 ‘이전 설계’ 접힘 영역과 SVG/PNG는 생성 중심 이력이다. 상품/링크/DB/보존/이벤트 공통 규칙은 새 흐름과 충돌하지 않는 범위에서 유지한다. 명시적으로 역사 자료로 표시한 생성 스키마를 신규 필수 계약으로 구현하지 않는다.

크기 근거 부족·상품 규격 누락은 미확인으로 남기고 추가 실측을 강제하지 않는다. 코드가 AI 해석의 사실 여부를 자동 확인한다고 설명하지 않는다. 응답·처리량·모델 품질은 별도 검증 대상이다.

[MVP 실행·구현 범위](../src/interior_recommender/README.md)를 확인한다. 직접 추천 로컬 웹은 구현했으며 서비스 DB/Kafka/Bronze·FAISS·구매 링크 검증·신규 FK/API/메시지의 전체 배포 완료를 뜻하지 않는다. 다른 협업용 사본과 별도 원본 BRD는 갱신하지 않는다.
