# 3차 프로젝트 문서

2026-10-15부터 **이미지 생성 없이 사진·텍스트·스케치의 공간 맥락·크기 추정으로 실제 상품을 직접 추천**한다. 이번 scikit-learn은 오프라인 실험이며 운영 순위는 코드 규칙이다. 모델 내재화는 후속 프로젝트 범위다. 기존 생성 MVP 코드·실행은 이번 문서 작업에서 바꾸지 않았다.

| 목적 | 먼저 읽을 문서 | 함께 확인할 문서 |
|---|---|---|
| 사업·사용자 범위 | [BRD](BRD_3차.md) | [PRD](PRD_3차_초안.md) |
| 전체 흐름·담당·내재화 단계 | [직접 추천 계약·다이어그램](design/12-direct-recommendation.md) | [아키텍처](design/00-architecture.md) |
| 역할별 기술 작업 | [설계 라우팅](design/index.md) | 담당 상세 문서 |
| AI와 코드 검증 | [Application](design/04-application-ai.md) | [크기 정책](design/11-scale-policy.md) |
| 검색·규칙 순위·scikit-learn 실험 | [추천 서버](design/07-ad-server.md) | [파이프라인](design/02-data-pipeline.md), [운영](design/06-operations-migration.md) |
| 데이터 변경 | [직접 추천 속성](design/data-dictionary/09-direct-recommendation.md) | [모델](design/03-data-model.md), [ERD](design/03-erd-full.md) |
| 상품·링크 | [상품 연결](design/09-product-links.md) | [광고 서버](design/07-ad-server.md) |
| 인프라·보존·복구 | [인프라](design/01-infrastructure.md) | [Kafka](design/05-kafka-jobs.md), [운영](design/06-operations-migration.md) |

## 현행 설계와 구현 상태

기준 요구사항은 이 폴더의 BRD·PRD이며 후속 기술 계약은 direct-recommendation-v1이다. 기존 문서의 ‘이전 설계’ 접힘 영역과 SVG/PNG는 생성 중심 이력이다. 상품/링크/DB/보존/이벤트 공통 규칙은 새 흐름과 충돌하지 않는 범위에서 유지한다. 명시적으로 역사 자료로 표시한 생성 스키마를 신규 필수 계약으로 구현하지 않는다.

크기 근거 부족·상품 규격 누락은 미확인으로 남기고 추가 실측을 강제하지 않는다. 코드가 AI 해석의 사실 여부를 자동 확인한다고 설명하지 않는다. 응답·처리량·모델 품질은 별도 검증 대상이다.

[기존 생성 MVP 안내](../../ai_mvp/README.md)는 현재 코드 실행 문서다. 직접 추천·scikit-learn·신규 FK·v2 API/메시지의 구현 완료를 뜻하지 않는다. 다른 협업용 사본과 별도 원본 BRD는 이번 갱신 대상이 아니다.
