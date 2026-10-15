# 설계 문서 안내

[BRD](../BRD_3차.md)와 [PRD](../PRD_3차_초안.md)를 먼저 읽고 담당 영역의 설계를 확인한다. 모든 문서는 구현 완료 기록과 구분한다.

| 작업 | 문서 |
|---|---|
| 전체 경계와 요구사항 | [아키텍처](00-architecture.md) |
| DB 3+3대 복제·접속·장애 전환 | [인프라 DB 계약](01-infrastructure.md#db-다중화와-접속-계약), [운영 복구](06-operations-migration.md#db-복제와-장애-복구) |
| VMware·서비스 배치·저장소·AWS 이전 | [인프라](01-infrastructure.md) |
| 상품 자료 준비·Spark·Gold·반응 집계 | [파이프라인](02-data-pipeline.md) |
| 상품·판매처·원천·입력·이미지 연결 키 | [데이터 모델](03-data-model.md), [속성 사전](data-dictionary/index.md) |
| 업무별 관계·전체 필드·핵심 흐름 | [전체 ERD](03-erd-full.md), [간략 ERD](03-erd-summary.md) |
| Django·Cloudflare 생성·Gemini 해석/임베딩·비교/구매 API | [Application과 AI](04-application-ai.md) |
| 직접 이벤트 발행·중복 방지·단계별 복구 | [Kafka](05-kafka-jobs.md) |
| 인수 조건·원문/파일 보존·미구현 경계 | [운영](06-operations-migration.md) |
| 실제 상품 추천·인덱스·비교/구매 클릭·광고 효과 | [추천/광고 서버](07-ad-server.md) |
| 생성 API·출력 해상도·무료 한도 | [생성 계약](08-template-generation.md#생성-api와-해상도-계약) |
| 템플릿·입력 버전·생성·좌표·재시도 | [템플릿](08-template-generation.md) |
| 상품 출처·이미지·다나와 연결·링크 정책 | [상품 연결](09-product-links.md) |
| 기존 도매꾹 이미지의 템플릿 제작 자료 활용 | [템플릿 자료](10-domeggook-reuse.md) |

그림으로 먼저 보려면 [전체 아키텍처 이미지](assets/architecture-overview.svg), [데이터 파이프라인 이미지](assets/data-pipeline-overview.svg), [핵심 데이터 관계 이미지](assets/erd-core-overview.svg)를 확인한다. 상세 계약과 구현 기준은 위 문서에 둔다.

이번 개정 기준 문서는 docs/BRD_3차.md·docs/PRD_3차_초안.md다. 기존 파일명으로 내용을 갱신하고 다른 설계 문서는 docs/design/에서 확인한다.


## 실행 방식 설계 읽는 순서

접수된 작업의 [비동기 I/O·스레드풀·동기 트랜잭션 분류](04-application-ai.md#비동기-io스레드풀동기-트랜잭션-처리-경계) → [작업 동시 실행과 offset 관리](05-kafka-jobs.md#작업-내부-동시-실행과-offset-관리) → [광고 검색 실행 경계](07-ad-server.md#비동기-호출과-동기-검색-처리) → [성능·복구 검증](06-operations-migration.md#작업-내부-동시-처리-검증) 순서로 읽는다. HTTP 트래픽 분산·웹 워커 증설·서버 추가는 이번 작업 내부 실행 설계의 범위가 아니다.

## 읽는 순서와 기준

1. BRD·PRD에서 목표·완료 기준을 확인한다.
2. 아키텍처에서 서비스 책임, 인프라에서 현재 배치 후보를 확인한다.
3. 파이프라인에서 상품 준비 경로, 광고 서버에서 추천·클릭 호출 순서를 확인한다.
4. 업무별 ERD → 데이터 모델 → 필드 사전 순서로 구현 데이터를 확인한다.

| 계약 | 기준 문서 |
|---|---|
| 입력 Bronze 확인·AI 시작·메시지 중복 | Kafka |
| 템플릿 선택·생성·좌표 | 템플릿 |
| 추천·노출·클릭·검색 인덱스 | 광고 서버 |
| 상품 출처·연결 URL·판매 조건 | 상품 연결 |
| DB 물리 복제·접속 / 장애 전환·복원 | 인프라 / 운영 |
| 컬럼·관계·키 | 데이터 모델·업무별 ERD·속성 사전 |

그림은 한 장에 한 질문만 설명한다. 이용 순서는 흐름도, 서버 간 호출은 시퀀스, 데이터 연결은 ERD, 상태·필드·운영 조건은 표로 구분한다. 상세 필드·토픽·예외를 개요 그림에 모두 표시하지 않는다.


## 공간 대비 크기 작업 읽는 순서

BRD·PRD → [공간 크기 정책](11-scale-policy.md) → [템플릿·좌표](08-template-generation.md) → [Application](04-application-ai.md) → [복구](05-kafka-jobs.md) → [운영 검증](06-operations-migration.md) 순서로 읽는다. 크기 계획은 원본 주변 물체의 통상 치수 범위와 원근에 근거하며 과대·과소·판단 불가를 구분한다.
