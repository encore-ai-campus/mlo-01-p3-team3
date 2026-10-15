# 핵심 데이터 관계: 직접 추천

2026-10-15 신규 관계는 아래와 같다. 생성 중심 기존 그림/필드는 접힘 영역에 보존하며 공통 상품/이벤트 관계 외 신규 필수 계약으로 사용하지 않는다. [현행 속성](data-dictionary/09-direct-recommendation.md)과 [논리 변경안](12-direct-recommendation.md#논리-데이터와-erd-변경안)을 함께 읽는다. 실제 ORM/DDL 변경은 별도다.

```mermaid
erDiagram
    projects ||--o{ input_revisions : owns
    input_revisions ||--o{ recommendation_contexts : interpreted
    input_revisions ||--o{ recommendation_runs : requested
    recommendation_contexts |o--o{ recommendation_runs : searched_with
    recommendation_runs ||--o{ recommendation_items : returns
    products ||--o{ recommendation_items : matched
    recommendation_items ||--o{ recommendation_impressions : shown
    recommendation_impressions ||--o{ user_events : responded
```

접수 중에는 context_id가 NULL일 수 있고 검색 전에는 확정 맥락이 필요하다. 후보 전체/특징/버전은 run의 manifest로, 반환 항목 설명은 item 스냅샷으로 연결하며 반응 라벨은 실제 impression에 연결한다. 원천/벡터/작업의 나머지 공통 관계는 기존 목록을 유지한다.

<details>
<summary>이전 상세 설계 — 공통 계약은 유지, 생성 중심 단계/관계는 신규 사용 안 함</summary>

# 핵심 데이터 관계

입력에서 생성 결과와 상품 추천까지 이어지는 핵심 관계만 표시한다. 전체 필드와 나머지 관계는 [업무별 ERD](03-erd-full.md), 용어는 [데이터 모델](03-data-model.md)을 따른다.

## 핵심 관계

공간 생성, 물체별 추천, 상품 판매 이력, 실제 노출·행동을 줄마다 분리했다. 반복 표시한 테이블명은 동일한 테이블이다.

![공간 생성·상품 추천·판매 이력·실제 반응을 네 줄로 나눈 핵심 데이터 관계](assets/erd-core-overview.svg)

[PNG로 보기](assets/erd-core-overview.png). 화살표는 관계를 읽는 방향이며 서버 호출이나 처리 순서가 아니다. `1 : 0..N`은 왼쪽 한 건에 오른쪽 0건 이상이 연결된다는 뜻이다. `N : 1`은 추천 항목 여러 건이 하나의 상품을 참조한다는 뜻이다. 생략한 관계와 정확한 키는 [업무별 ERD](03-erd-full.md#전체-관계-목록)에서 확인한다.

사용자와 프로젝트의 소유 관계는 전체 관계 목록에서 확인한다.

## 업무별로 더 보기

| 알고 싶은 내용 | 상세 위치 |
|---|---|
| 스케치·사진·텍스트·사용 템플릿 | [입력·공간 생성](03-erd-full.md#입력공간-생성) |
| 같은 이미지의 여러 상품 연결 | [상품·이미지](03-erd-full.md#상품이미지) |
| 판매 정보와 당시 가격 | [판매 정보](03-erd-full.md#판매-정보) |
| 상품 검색과 추천 이력 | [추천·검색](03-erd-full.md#추천검색) |
| 실제 노출과 사용자 반응 | [사용자·반응](03-erd-full.md#사용자반응) |
| 재시도·외부 호출 | [비동기 작업](03-erd-full.md#비동기-작업) |

그림은 DB 관계이며 서버 호출 순서가 아니다. 비로그인 프로젝트는 user_id가 없을 수 있다. 사진은 필수이고 스케치 또는 텍스트를 입력 버전에 연결한다. 생성 후 물체별로 추천을 요청하며 재추천은 완료된 공간 이미지를 유지한다.

파일·템플릿 연결, 판매 snapshot, 노출, 작업·검수·공통 FK와 상세 속성은 생략했다. 관계를 생략한 것이므로 이 그림만으로 DDL을 작성하지 않는다. 신규 흐름에서 사용하지 않는 placements는 전체 문서의 이전 배치안에서만 확인한다.

</details>
