# 사용자와 추천 속성 사전

현재 논리 설계의 각 속성에 무엇을 저장하는지 설명한다. 형식·값·상태 코드는 후보이며, 예시는 테스트 설명용으로 실제 계정이나 상품을 뜻하지 않는다.

[사전 안내](index.md) · [데이터 모델](../03-data-model.md) · [전체 ERD](../03-erd-full.md)

## 엔터티 바로가기

- [users](#users): Django 사용자 계정
- [projects](#projects): 공간 사진을 기준으로 한 사용자 탐색
- [sketch_revisions](#sketch_revisions): 저장한 스케치의 입력 버전
- [recommendation_runs](#recommendation_runs): 스케치에 대한 한 번의 추천 실행
- [recommendation_items](#recommendation_items): 추천 목록의 개별 상품
- [placements](#placements): 선택 상품의 기본 배치
- [user_interests](#user_interests): 사용자와 관심 카테고리의 연결

## users

이 엔터티는 Django 사용자 계정에 해당한다. 같은 이름의 속성이라도 아래 업무 의미를 기준으로 해석한다.

### users id

Django 사용자 계정 한 건의 UUID 기본키.

- 형식 후보: UUID (Django UUIDField, MySQL CHAR(32))
- 예시: `1017d7a3-a0f0-436b-a113-e72fee3f4b42`
- 해석: 신규 행 생성 시 UUID4를 한 번 발급한다. API와 메시지에도 같은 id를 사용하며 재처리·이관에서는 보존한다. 제공처 ID와 이벤트 재전송 식별자와는 용도가 다르다. UUID만으로 접근 권한을 보장하지 않는다.

### users username

계정 로그인 이름 또는 사용자 모델의 이름 필드.

- 형식 후보: 문자열
- 예시: `user_a`
- 해석: Django 기본 모델의 대표 필드다. 이메일 로그인 등으로 바꾸면 사용자 모델에 맞춰 갱신한다.

### users email

계정에 등록한 이메일 주소.

- 형식 후보: 문자열
- 예시: `user@example.com`
- 해석: 개인정보로 관리한다. 인증 방식 확정 전 고유성과 필수 여부를 단정하지 않는다.

### users password

Django가 관리하는 비밀번호 해시 표현.

- 형식 후보: 문자열
- 예시: `해시된 비밀번호 값`
- 해석: 평문 비밀번호를 저장하지 않는다. 이 사전에 실제 해시나 비밀값을 기록하지 않는다.

### users is_active

Django 계정의 활성 여부.

- 형식 후보: 참 또는 거짓
- 예시: `true`
- 해석: 관심 카테고리나 로그인 세션 존재 여부와 다르다.

### users date_joined

계정 가입 시각.

- 형식 후보: UTC 날짜와 시각
- 예시: `2026-10-12T01:00:00Z`
- 해석: Django 기본 사용자 모델의 대표 필드이며 UTC로 관리한다.

## projects

이 엔터티는 공간 사진을 기준으로 한 사용자 탐색에 해당한다. 같은 이름의 속성이라도 아래 업무 의미를 기준으로 해석한다.

### projects id

공간 사진을 기준으로 한 사용자 탐색 한 건의 UUID 기본키.

- 형식 후보: UUID (Django UUIDField, MySQL CHAR(32))
- 예시: `d16156e5-0634-4463-a39a-37b61109aabf`
- 해석: 신규 행 생성 시 UUID4를 한 번 발급한다. API와 메시지에도 같은 id를 사용하며 재처리·이관에서는 보존한다. 제공처 ID와 이벤트 재전송 식별자와는 용도가 다르다. UUID만으로 접근 권한을 보장하지 않는다.

### projects user_id

이 공간 탐색의 사용자 계정의 내부 ID.

- 형식 후보: UUID (Django UUIDField, MySQL CHAR(32))
- 예시: `1017d7a3-a0f0-436b-a113-e72fee3f4b42`
- 해석: [users.id](03-user-flow.md#users-id)를 참조하는 MySQL FK다. 참조 대상 PK와 같은 UUID 타입을 사용한다. 해당 단계에서 연결이 없으면 NULL을 허용하는 후보이다.

### projects session_id

비로그인 탐색을 연결할 세션 식별자.

- 형식 후보: 문자열
- 예시: `session-001`
- 해석: 로그인 user_id를 대신할 수 있다. 인증 토큰·쿠키 비밀값 자체를 저장하지 않는다.

### projects room_asset_id

프로젝트의 기준 공간 사진의 내부 ID.

- 형식 후보: UUID (Django UUIDField, MySQL CHAR(32))
- 예시: `ee47322f-d90a-466a-830b-51d482224f9d`
- 해석: [assets.id](02-products.md#assets-id)를 참조하는 MySQL FK다. 참조 대상 PK와 같은 UUID 타입을 사용한다. 이미지 참조여도 파일 본문을 저장하지 않는다.

### projects title

사용자가 공간 탐색에 붙인 이름.

- 형식 후보: 문자열
- 예시: `거실 가구 탐색`
- 해석: 상품명과 구분한다.

### projects status

공간 탐색 프로젝트의 상태.

- 형식 후보: 문자열
- 예시: `ACTIVE`
- 해석: 보관·삭제 상태 등의 후보 집합을 정한다. AI 작업의 실행 상태는 jobs에서 관리한다.

### projects created_at

공간 사진을 기준으로 한 사용자 탐색 행을 DB에 처음 등록한 시각.

- 형식 후보: UTC 날짜와 시각
- 예시: `2026-10-12T01:00:00Z`
- 해석: UTC로 저장한다. collected_at·occurred_at·started_at 등 업무 발생 시각과 다를 수 있다.

### projects updated_at

공간 사진을 기준으로 한 사용자 탐색 행을 마지막으로 수정한 시각.

- 형식 후보: UTC 날짜와 시각
- 예시: `2026-10-12T01:01:00Z`
- 해석: UTC로 저장한다. 행의 최종 갱신 시각이며 원천 가격의 수집 시각이나 파일 내용 변경 시각을 대신하지 않는다.

## sketch_revisions

이 엔터티는 저장한 스케치의 입력 버전에 해당한다. 같은 이름의 속성이라도 아래 업무 의미를 기준으로 해석한다.

### sketch_revisions id

저장한 스케치의 입력 버전 한 건의 UUID 기본키.

- 형식 후보: UUID (Django UUIDField, MySQL CHAR(32))
- 예시: `5eb7bf86-3512-4344-8f24-a354f9183ce7`
- 해석: 신규 행 생성 시 UUID4를 한 번 발급한다. API와 메시지에도 같은 id를 사용하며 재처리·이관에서는 보존한다. 제공처 ID와 이벤트 재전송 식별자와는 용도가 다르다. UUID만으로 접근 권한을 보장하지 않는다.

### sketch_revisions project_id

스케치를 그린 공간 탐색의 내부 ID.

- 형식 후보: UUID (Django UUIDField, MySQL CHAR(32))
- 예시: `d16156e5-0634-4463-a39a-37b61109aabf`
- 해석: [projects.id](03-user-flow.md#projects-id)를 참조하는 MySQL FK다. 참조 대상 PK와 같은 UUID 타입을 사용한다.

### sketch_revisions revision_no

프로젝트 안의 스케치 버전 번호.

- 형식 후보: 정수
- 예시: `2`
- 해석: 같은 project_id 내 고유하다. 다시 그릴 때 증가하고 이전 입력을 보존한다.

### sketch_revisions sketch_asset_id

저장한 스케치 레이어 이미지의 내부 ID.

- 형식 후보: UUID (Django UUIDField, MySQL CHAR(32))
- 예시: `ee47322f-d90a-466a-830b-51d482224f9d`
- 해석: [assets.id](02-products.md#assets-id)를 참조하는 MySQL FK다. 참조 대상 PK와 같은 UUID 타입을 사용한다. 이미지 참조여도 파일 본문을 저장하지 않는다. 해당 단계에서 연결이 없으면 NULL을 허용하는 후보이다.

### sketch_revisions strokes_json

사용자가 그린 선과 좌표의 구조화 데이터.

- 형식 후보: JSON
- 예시: `[{"points":[[0.25,0.40],[0.30,0.45]],"color":"#000000","width":3}]`
- 해석: 점 좌표는 사진 기준 0~1이다. 선 굵기 단위와 좌표 계약을 정한다. 큰 데이터는 파일 참조로 전환할 수 있다.

### sketch_revisions bounding_box_json

스케치 전체를 감싸는 사각 영역.

- 형식 후보: JSON
- 예시: `{"x":0.25,"y":0.40,"width":0.30,"height":0.35}`
- 해석: 좌표와 크기는 사진 기준 0~1 값이다. 가구의 실측 규격이 아니다.

### sketch_revisions canvas_width

입력을 그린 Canvas의 가로 픽셀 수.

- 형식 후보: 정수
- 예시: `1024`
- 해석: 정규화와 재표시에 참고한다. 상품 width_mm와 다르다.

### sketch_revisions canvas_height

입력을 그린 Canvas의 세로 픽셀 수.

- 형식 후보: 정수
- 예시: `768`
- 해석: 원본 사진 크기와 표시 화면 크기는 다를 수 있어 입력 기준을 고정한다.

### sketch_revisions created_at

저장한 스케치의 입력 버전 행을 DB에 처음 등록한 시각.

- 형식 후보: UTC 날짜와 시각
- 예시: `2026-10-12T01:00:00Z`
- 해석: UTC로 저장한다. collected_at·occurred_at·started_at 등 업무 발생 시각과 다를 수 있다.

### sketch_revisions updated_at

저장한 스케치의 입력 버전 행을 마지막으로 수정한 시각.

- 형식 후보: UTC 날짜와 시각
- 예시: `2026-10-12T01:01:00Z`
- 해석: UTC로 저장한다. 행의 최종 갱신 시각이며 원천 가격의 수집 시각이나 파일 내용 변경 시각을 대신하지 않는다.

## recommendation_runs

이 엔터티는 스케치에 대한 한 번의 추천 실행에 해당한다. 같은 이름의 속성이라도 아래 업무 의미를 기준으로 해석한다.

### recommendation_runs id

스케치에 대한 한 번의 추천 실행 한 건의 UUID 기본키.

- 형식 후보: UUID (Django UUIDField, MySQL CHAR(32))
- 예시: `1aef5145-108f-468b-b1ab-15239ff6601e`
- 해석: 신규 행 생성 시 UUID4를 한 번 발급한다. API와 메시지에도 같은 id를 사용하며 재처리·이관에서는 보존한다. 제공처 ID와 이벤트 재전송 식별자와는 용도가 다르다. UUID만으로 접근 권한을 보장하지 않는다.

### recommendation_runs project_id

추천을 요청한 공간 탐색의 내부 ID.

- 형식 후보: UUID (Django UUIDField, MySQL CHAR(32))
- 예시: `d16156e5-0634-4463-a39a-37b61109aabf`
- 해석: [projects.id](03-user-flow.md#projects-id)를 참조하는 MySQL FK다. 참조 대상 PK와 같은 UUID 타입을 사용한다.

### recommendation_runs sketch_revision_id

추천 입력으로 이어진 스케치 버전의 선택 참조. 텍스트만 사용한 공간 구성에서는 NULL이다.

- 형식 후보: UUID (Django UUIDField, MySQL CHAR(32))
- 예시: `5eb7bf86-3512-4344-8f24-a354f9183ce7`
- 해석: [sketch_revisions.id](03-user-flow.md#sketch_revisions-id)를 참조하는 MySQL FK다. 참조 대상 PK와 같은 UUID 타입을 사용한다.

### recommendation_runs job_id

광고 서버의 추천 검색을 실행하는 비동기 작업의 내부 ID.

- 형식 후보: UUID (Django UUIDField, MySQL CHAR(32))
- 예시: `898b82f3-8bae-4f73-8413-321d84c297f0`
- 해석: [jobs.id](04-ai-jobs.md#jobs-id)를 참조하는 MySQL FK다. 참조 대상 PK와 같은 UUID 타입을 사용한다.

### recommendation_runs category_id

선택 물체에서 확정한 가구 카테고리의 내부 ID.

- 형식 후보: UUID (Django UUIDField, MySQL CHAR(32))
- 예시: `ceb96b55-ee4d-46fa-9204-9db0c5f07a2d`
- 해석: [categories.id](02-products.md#categories-id)를 참조하는 MySQL FK다. 참조 대상 PK와 같은 UUID 타입을 사용한다. 해당 단계에서 연결이 없으면 NULL을 허용하는 후보이다. UNKNOWN이면 임의 카테고리를 넣지 않는다.

### recommendation_runs recognition_status

선택 물체 품목 판단의 상태.

- 형식 후보: 문자열
- 예시: `ACCEPTED`
- 해석: UNKNOWN 등과 구분한다. 추천 작업 전체 status와 별개다.

### recommendation_runs model_version

인식에 사용한 실제 모델 식별자와 버전.

- 형식 후보: 문자열
- 예시: `gemini-3.5-flash-lite`
- 해석: 후보 모델의 예시이며 채택을 뜻하지 않는다. 가능한 경우 고정 버전을 기록한다.

### recommendation_runs ranking_version

후보 상품 정렬 규칙의 버전.

- 형식 후보: 문자열
- 예시: `ranking-v1`
- 해석: 모델 버전과 다르다. 가격·최신성·검수 등 어떤 규칙으로 순위를 만들었는지 설명한다.

### recommendation_runs status

추천 실행의 진행 또는 완료 상태.

- 형식 후보: 문자열
- 예시: `SUCCEEDED`
- 해석: 작업 상태와 일치하도록 갱신하되 인식 불가·후보 없음 등 업무 결과도 구분한다.

### recommendation_runs failure_reason

추천을 완료하지 못한 사용자·운영자 설명.

- 형식 후보: 긴 문자열
- 예시: `지원 카테고리를 인식하지 못함`
- 해석: 성공 시 NULL 가능하다. 민감한 모델 응답이나 스택 전체를 화면에 노출하지 않는다.

### recommendation_runs created_at

스케치에 대한 한 번의 추천 실행 행을 DB에 처음 등록한 시각.

- 형식 후보: UTC 날짜와 시각
- 예시: `2026-10-12T01:00:00Z`
- 해석: UTC로 저장한다. collected_at·occurred_at·started_at 등 업무 발생 시각과 다를 수 있다.

### recommendation_runs updated_at

스케치에 대한 한 번의 추천 실행 행을 마지막으로 수정한 시각.

- 형식 후보: UTC 날짜와 시각
- 예시: `2026-10-12T01:01:00Z`
- 해석: UTC로 저장한다. 행의 최종 갱신 시각이며 원천 가격의 수집 시각이나 파일 내용 변경 시각을 대신하지 않는다.

## recommendation_items

이 엔터티는 추천 목록의 개별 상품에 해당한다. 같은 이름의 속성이라도 아래 업무 의미를 기준으로 해석한다.

### recommendation_items id

추천 목록의 개별 상품 한 건의 UUID 기본키.

- 형식 후보: UUID (Django UUIDField, MySQL CHAR(32))
- 예시: `f8c188ca-66c0-4450-af4c-16d084d848ce`
- 해석: 신규 행 생성 시 UUID4를 한 번 발급한다. API와 메시지에도 같은 id를 사용하며 재처리·이관에서는 보존한다. 제공처 ID와 이벤트 재전송 식별자와는 용도가 다르다. UUID만으로 접근 권한을 보장하지 않는다.

### recommendation_items run_id

이 추천 항목을 생성한 추천 실행의 내부 ID.

- 형식 후보: UUID (Django UUIDField, MySQL CHAR(32))
- 예시: `1aef5145-108f-468b-b1ab-15239ff6601e`
- 해석: [recommendation_runs.id](03-user-flow.md#recommendation_runs-id)를 참조하는 MySQL FK다. 참조 대상 PK와 같은 UUID 타입을 사용한다.

### recommendation_items product_id

이 순위에서 추천한 상품의 내부 ID.

- 형식 후보: UUID (Django UUIDField, MySQL CHAR(32))
- 예시: `e98225a5-5312-48ac-b2c4-d92556705f8a`
- 해석: [products.id](02-products.md#products-id)를 참조하는 MySQL FK다. 참조 대상 PK와 같은 UUID 타입을 사용한다.

### recommendation_items offer_snapshot_id

추천 시점의 가격과 판매 주소 근거의 내부 ID. 확인된 판매 항목이 없는 비교 상품 추천은 NULL이며 products.comparison_url로 탐색한다.

- 형식 후보: UUID (Django UUIDField, MySQL CHAR(32))
- 예시: `ae0abc7f-07a6-4d21-a20b-7b30f4907736`
- 해석: [offer_snapshots.id](02-products.md#offer_snapshots-id)를 참조하는 MySQL FK다. 참조 대상 PK와 같은 UUID 타입을 사용한다.

### recommendation_items link_snapshot_json

광고 서버가 추천 시점에 확정한 외부 연결 정보를 보존한다. 원천 상품 식별자와 별개이며 수정 시 기존 추천을 덮어쓰지 않고 새 추천 항목을 만든다.

- 형식 후보: JSON
- 구조: schema_version, destination=danawa, link_kind=COMPARISON/MERCHANT, url, status, verified_at, product_id, 선택적 offer_id, evidence_ref.
- 미확정 URL은 NULL·PENDING으로 기록하고 버튼을 활성화하지 않는다. 클릭 시 현재 연결 가능 상태를 다시 확인한다.
- 해석: Application이 광고 서버의 응답을 검증해 한 번 저장한다. 직접 판매 당시 가격·옵션은 offer_snapshot_id가 가리키는 이력에 따로 보존한다.

### recommendation_items rank

이번 추천 실행 안에서의 표시 순위.

- 형식 후보: 정수
- 예시: `1`
- 해석: run_id와 복합 고유키 후보다. 낮은 숫자를 먼저 표시한다.

### recommendation_items score

추천 정렬 계산에 사용한 값.

- 형식 후보: 소수 DECIMAL 후보
- 예시: `0.82`
- 해석: ranking_version에 따른 값이며 AI 카테고리 확률이나 사용자 만족도가 아니다.

### recommendation_items created_at

추천 목록의 개별 상품 행을 DB에 처음 등록한 시각.

- 형식 후보: UTC 날짜와 시각
- 예시: `2026-10-12T01:00:00Z`
- 해석: UTC로 저장한다. collected_at·occurred_at·started_at 등 업무 발생 시각과 다를 수 있다.

### recommendation_items updated_at

추천 목록의 개별 상품 행을 마지막으로 수정한 시각.

- 형식 후보: UTC 날짜와 시각
- 예시: `2026-10-12T01:01:00Z`
- 해석: UTC로 저장한다. 행의 최종 갱신 시각이며 원천 가격의 수집 시각이나 파일 내용 변경 시각을 대신하지 않는다.

## placements

이전 실제 상품 배치안의 참조용 스키마이며 신규 3차에서는 생성하지 않는다. 템플릿 구성 결과는 assets와 scene_objects로 연결하고 실제 상품은 추천·행동 이력으로 추적한다.

이 엔터티는 선택 상품의 기본 배치에 해당한다. 같은 이름의 속성이라도 아래 업무 의미를 기준으로 해석한다.

### placements id

선택 상품의 기본 배치 한 건의 UUID 기본키.

- 형식 후보: UUID (Django UUIDField, MySQL CHAR(32))
- 예시: `f54ef9eb-1369-4c12-8946-e04e033cd129`
- 해석: 신규 행 생성 시 UUID4를 한 번 발급한다. API와 메시지에도 같은 id를 사용하며 재처리·이관에서는 보존한다. 제공처 ID와 이벤트 재전송 식별자와는 용도가 다르다. UUID만으로 접근 권한을 보장하지 않는다.

### placements project_id

상품을 배치할 공간 탐색의 내부 ID.

- 형식 후보: UUID (Django UUIDField, MySQL CHAR(32))
- 예시: `d16156e5-0634-4463-a39a-37b61109aabf`
- 해석: [projects.id](03-user-flow.md#projects-id)를 참조하는 MySQL FK다. 참조 대상 PK와 같은 UUID 타입을 사용한다.

### placements recommendation_item_id

사용자가 선택한 추천 항목의 내부 ID.

- 형식 후보: UUID (Django UUIDField, MySQL CHAR(32))
- 예시: `f8c188ca-66c0-4450-af4c-16d084d848ce`
- 해석: [recommendation_items.id](03-user-flow.md#recommendation_items-id)를 참조하는 MySQL FK다. 참조 대상 PK와 같은 UUID 타입을 사용한다.

### placements selected_offer_snapshot_id

사용자가 확정한 판매처와 가격 정보의 내부 ID.

- 형식 후보: UUID (Django UUIDField, MySQL CHAR(32))
- 예시: `ae0abc7f-07a6-4d21-a20b-7b30f4907736`
- 해석: [offer_snapshots.id](02-products.md#offer_snapshots-id)를 참조하는 MySQL FK다. 참조 대상 PK와 같은 UUID 타입을 사용한다.

### placements product_asset_id

배치에 사용한 실제 상품 이미지의 내부 ID.

- 형식 후보: UUID (Django UUIDField, MySQL CHAR(32))
- 예시: `ee47322f-d90a-466a-830b-51d482224f9d`
- 해석: [assets.id](02-products.md#assets-id)를 참조하는 MySQL FK다. 참조 대상 PK와 같은 UUID 타입을 사용한다. 이미지 참조여도 파일 본문을 저장하지 않는다.

### placements bounding_box_json

선택 상품을 기본 배치할 사진 내 영역.

- 형식 후보: JSON
- 예시: `{"x":0.25,"y":0.40,"width":0.30,"height":0.35}`
- 해석: 사진 기준 정규화 값이다. 실제 설치 가능성을 보증하지 않는다.

### placements result_asset_id

기본 배치 결과를 저장한 이미지 파일의 내부 ID.

- 형식 후보: UUID (Django UUIDField, MySQL CHAR(32))
- 예시: `ee47322f-d90a-466a-830b-51d482224f9d`
- 해석: [assets.id](02-products.md#assets-id)를 참조하는 MySQL FK다. 참조 대상 PK와 같은 UUID 타입을 사용한다. 이미지 참조여도 파일 본문을 저장하지 않는다. 해당 단계에서 연결이 없으면 NULL을 허용하는 후보이다.

### placements status

기본 배치 결과 생성 상태.

- 형식 후보: 문자열
- 예시: `SUCCEEDED`
- 해석: 배치 이미지 생성 실패나 저장 완료를 구분한다. 판매 재고 상태가 아니다.

### placements created_at

선택 상품의 기본 배치 행을 DB에 처음 등록한 시각.

- 형식 후보: UTC 날짜와 시각
- 예시: `2026-10-12T01:00:00Z`
- 해석: UTC로 저장한다. collected_at·occurred_at·started_at 등 업무 발생 시각과 다를 수 있다.

### placements updated_at

선택 상품의 기본 배치 행을 마지막으로 수정한 시각.

- 형식 후보: UTC 날짜와 시각
- 예시: `2026-10-12T01:01:00Z`
- 해석: UTC로 저장한다. 행의 최종 갱신 시각이며 원천 가격의 수집 시각이나 파일 내용 변경 시각을 대신하지 않는다.

## user_interests

이 엔터티는 사용자와 관심 카테고리의 연결에 해당한다. 같은 이름의 속성이라도 아래 업무 의미를 기준으로 해석한다.

### user_interests id

사용자와 관심 카테고리의 연결 한 건의 UUID 기본키.

- 형식 후보: UUID (Django UUIDField, MySQL CHAR(32))
- 예시: `a7c95fbb-acee-4de2-abc0-520a1b711dd0`
- 해석: 신규 행 생성 시 UUID4를 한 번 발급한다. API와 메시지에도 같은 id를 사용하며 재처리·이관에서는 보존한다. 제공처 ID와 이벤트 재전송 식별자와는 용도가 다르다. UUID만으로 접근 권한을 보장하지 않는다.

### user_interests user_id

관심 카테고리를 기록할 사용자의 내부 ID.

- 형식 후보: UUID (Django UUIDField, MySQL CHAR(32))
- 예시: `1017d7a3-a0f0-436b-a113-e72fee3f4b42`
- 해석: [users.id](03-user-flow.md#users-id)를 참조하는 MySQL FK다. 참조 대상 PK와 같은 UUID 타입을 사용한다.

### user_interests category_id

사용자가 선택한 상품의 카테고리의 내부 ID.

- 형식 후보: UUID (Django UUIDField, MySQL CHAR(32))
- 예시: `ceb96b55-ee4d-46fa-9204-9db0c5f07a2d`
- 해석: [categories.id](02-products.md#categories-id)를 참조하는 MySQL FK다. 참조 대상 PK와 같은 UUID 타입을 사용한다.

### user_interests selection_count

사용자가 해당 카테고리 상품을 선택한 누적 횟수.

- 형식 후보: 정수
- 예시: `3`
- 해석: 조회나 추천 노출 횟수가 아니다. 중복 이벤트 반영을 막고 삭제·재계산 정책을 정한다.

### user_interests last_selected_at

해당 카테고리 상품을 마지막으로 선택한 시각.

- 형식 후보: UTC 날짜와 시각
- 예시: `2026-10-12T03:00:00Z`
- 해석: 행 생성·갱신 시점과 구분하며 선택 이벤트의 시각을 사용한다.

### user_interests created_at

사용자와 관심 카테고리의 연결 행을 DB에 처음 등록한 시각.

- 형식 후보: UTC 날짜와 시각
- 예시: `2026-10-12T01:00:00Z`
- 해석: UTC로 저장한다. collected_at·occurred_at·started_at 등 업무 발생 시각과 다를 수 있다.

### user_interests updated_at

사용자와 관심 카테고리의 연결 행을 마지막으로 수정한 시각.

- 형식 후보: UTC 날짜와 시각
- 예시: `2026-10-12T01:01:00Z`
- 해석: UTC로 저장한다. 행의 최종 갱신 시각이며 원천 가격의 수집 시각이나 파일 내용 변경 시각을 대신하지 않는다.
