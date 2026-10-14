# 행동과 발행 및 이관 속성 사전

현재 논리 설계의 각 속성에 무엇을 저장하는지 설명한다. 형식·값·상태 코드는 후보이며, 예시는 테스트 설명용으로 실제 계정이나 상품을 뜻하지 않는다.

[사전 안내](index.md) · [데이터 모델](../03-data-model.md) · [전체 ERD](../03-erd-full.md)

## 엔터티 바로가기

- [user_events](#user_events): 사용자가 수행한 개별 행동
- [archive_manifests](#archive_manifests): 운영 기록 이관 파일의 검증 내역

## user_events

이 엔터티는 사용자가 수행한 개별 행동에 해당한다. 같은 이름의 속성이라도 아래 업무 의미를 기준으로 해석한다.

### user_events id

사용자가 수행한 개별 행동 한 건의 UUID 기본키.

- 형식 후보: UUID (Django UUIDField, MySQL CHAR(32))
- 예시: `6203a5c8-0d4d-49e6-97eb-87cfb63498e1`
- 해석: 신규 행 생성 시 UUID4를 한 번 발급한다. API와 메시지에도 같은 id를 사용하며 재처리·이관에서는 보존한다. 제공처 ID와 이벤트 재전송 식별자와는 용도가 다르다. UUID만으로 접근 권한을 보장하지 않는다.

### user_events event_id

사용자 행동 한 건의 중복 방지 ID.

- 형식 후보: 문자열
- 예시: `event-uuid-001`
- 해석: 웹 재전송과 Kafka 재발행에도 같은 행동은 같은 ID를 사용한다.

### user_events user_id

이 행동을 수행한 사용자 계정의 내부 ID.

- 형식 후보: UUID (Django UUIDField, MySQL CHAR(32))
- 예시: `1017d7a3-a0f0-436b-a113-e72fee3f4b42`
- 해석: [users.id](03-user-flow.md#users-id)를 참조하는 MySQL FK다. 참조 대상 PK와 같은 UUID 타입을 사용한다. 해당 단계에서 연결이 없으면 NULL을 허용하는 후보이다.

### user_events session_id

행동이 발생한 비로그인 또는 탐색 세션 식별자.

- 형식 후보: 문자열
- 예시: `session-001`
- 해석: 사용자 ID와 별개며 인증 비밀값을 저장하지 않는다.

### user_events event_type

발생한 사용자 행동의 종류.

- 형식 후보: 문자열
- 예시: `PRODUCT_SELECTED`
- 해석: 업로드·스케치·텍스트·구성·추천·실제 노출·선택·명시적 거절·재추천·판매 링크 클릭을 구분한다. 자유 문장 대신 코드 집합을 사용한다.

### user_events project_id

행동이 발생한 공간 탐색의 내부 ID.

- 형식 후보: UUID (Django UUIDField, MySQL CHAR(32))
- 예시: `d16156e5-0634-4463-a39a-37b61109aabf`
- 해석: [projects.id](03-user-flow.md#projects-id)를 참조하는 MySQL FK다. 참조 대상 PK와 같은 UUID 타입을 사용한다. 해당 단계에서 연결이 없으면 NULL을 허용하는 후보이다.

### user_events sketch_revision_id

행동과 연결된 스케치 버전의 내부 ID.

- 형식 후보: UUID (Django UUIDField, MySQL CHAR(32))
- 예시: `5eb7bf86-3512-4344-8f24-a354f9183ce7`
- 해석: [sketch_revisions.id](03-user-flow.md#sketch_revisions-id)를 참조하는 MySQL FK다. 참조 대상 PK와 같은 UUID 타입을 사용한다. 해당 단계에서 연결이 없으면 NULL을 허용하는 후보이다.

### user_events recommendation_run_id

선택·거절·재추천 행동의 추천 실행의 내부 ID.

- 형식 후보: UUID (Django UUIDField, MySQL CHAR(32))
- 예시: `1aef5145-108f-468b-b1ab-15239ff6601e`
- 해석: [recommendation_runs.id](03-user-flow.md#recommendation_runs-id)를 참조하는 MySQL FK다. 참조 대상 PK와 같은 UUID 타입을 사용한다. 해당 단계에서 연결이 없으면 NULL을 허용하는 후보이다.

### user_events product_id

선택·거절·조회 등 행동의 대상 상품의 내부 ID.

- 형식 후보: UUID (Django UUIDField, MySQL CHAR(32))
- 예시: `e98225a5-5312-48ac-b2c4-d92556705f8a`
- 해석: [products.id](02-products.md#products-id)를 참조하는 MySQL FK다. 참조 대상 PK와 같은 UUID 타입을 사용한다. 해당 단계에서 연결이 없으면 NULL을 허용하는 후보이다.

### user_events offer_id

구매 클릭 등 행동의 대상 판매 항목의 내부 ID.

- 형식 후보: UUID (Django UUIDField, MySQL CHAR(32))
- 예시: `5348ff9c-5755-4b96-a834-36e1ea766a16`
- 해석: [offers.id](02-products.md#offers-id)를 참조하는 MySQL FK다. 참조 대상 PK와 같은 UUID 타입을 사용한다. 해당 단계에서 연결이 없으면 NULL을 허용하는 후보이다.

### user_events occurred_at

사용자 행동이 실제 발생한 시각.

- 형식 후보: UTC 날짜와 시각
- 예시: `2026-10-12T03:00:00Z`
- 해석: 클라이언트 시간이면 시계 오차를 고려한다. 서버 수신 시각과 분리한다.

### user_events received_at

서버가 행동 이벤트를 받은 시각.

- 형식 후보: UTC 날짜와 시각
- 예시: `2026-10-12T03:00:01Z`
- 해석: 네트워크 지연과 클라이언트 시각 차이를 확인하는 기준이다.

### user_events metadata_json

행동 종류별 추가 맥락.

- 형식 후보: JSON
- 예시: `{"reason_code":"NOT_PREFERRED"}`
- 해석: 스케치 이동 묶음·거절 사유 코드 등을 허용된 계약으로 저장한다. 비밀값과 무제한 원문을 넣지 않는다.

### user_events created_at

사용자가 수행한 개별 행동 행을 DB에 처음 등록한 시각.

- 형식 후보: UTC 날짜와 시각
- 예시: `2026-10-12T01:00:00Z`
- 해석: UTC로 저장한다. collected_at·occurred_at·started_at 등 업무 발생 시각과 다를 수 있다.

## archive_manifests

이 엔터티는 운영 기록 이관 파일의 검증 내역에 해당한다. 같은 이름의 속성이라도 아래 업무 의미를 기준으로 해석한다.

### archive_manifests id

운영 기록 이관 파일의 검증 내역 한 건의 UUID 기본키.

- 형식 후보: UUID (Django UUIDField, MySQL CHAR(32))
- 예시: `f7a08dac-e447-4264-b7df-e49c6a6394ed`
- 해석: 신규 행 생성 시 UUID4를 한 번 발급한다. API와 메시지에도 같은 id를 사용하며 재처리·이관에서는 보존한다. 제공처 ID와 이벤트 재전송 식별자와는 용도가 다르다. UUID만으로 접근 권한을 보장하지 않는다.

### archive_manifests archive_type

내보낸 운영 기록의 종류.

- 형식 후보: 문자열
- 예시: `JOB_HISTORY`
- 해석: 원천 데이터나 DB 전체 백업과 구분한다. 브라우저가 다운로드한 파일 종류가 아니다.

### archive_manifests period_start

이관 대상 기록 구간의 시작.

- 형식 후보: UTC 날짜와 시각
- 예시: `2026-10-01T00:00:00Z`
- 해석: 파일 생성 시각이 아니다. 시작 포함·끝 제외 등의 구간 규칙을 고정한다.

### archive_manifests period_end

이관 대상 기록 구간의 끝.

- 형식 후보: UTC 날짜와 시각
- 예시: `2026-10-12T00:00:00Z`
- 해석: period_start와 함께 중복·누락을 대조한다.

### archive_manifests object_keys_json

이관한 파일들의 저장 위치 목록.

- 형식 후보: JSON
- 예시: `[{"bucket":"archive-target","object_key":"jobs/2026-10.jsonl"}]`
- 해석: 특정 archive 버킷을 필수로 뜻하지 않는다. 저장소 URI 등 실제 보관 방식과 계약을 맞춘다.

### archive_manifests record_count

이관 파일에 포함된 기록 수.

- 형식 후보: 정수
- 예시: `2500`
- 해석: 원본 대조와 읽기 검증에 사용한다. 파일 개수와 다르다.

### archive_manifests checksum

파일 또는 파일 묶음의 무결성 확인값.

- 형식 후보: 문자열
- 예시: `sha256:archive123...`
- 해석: 복수 파일이면 개별 checksum과 묶음 검증 규칙을 정한다.

### archive_manifests status

이관과 검증의 진행 상태.

- 형식 후보: 문자열
- 예시: `VERIFIED`
- 해석: EXPORTED·VERIFIED·FAILED 등은 후보다. 내보내기만으로 완료라고 판정하지 않는다.

### archive_manifests verified_at

이관 파일의 읽기·건수·무결성 검증 완료 시각.

- 형식 후보: UTC 날짜와 시각
- 예시: `2026-10-12T04:00:00Z`
- 해석: 파일 업로드나 이관 작업 시작 시각과 구분한다.

### archive_manifests created_at

운영 기록 이관 파일의 검증 내역 행을 DB에 처음 등록한 시각.

- 형식 후보: UTC 날짜와 시각
- 예시: `2026-10-12T01:00:00Z`
- 해석: UTC로 저장한다. collected_at·occurred_at·started_at 등 업무 발생 시각과 다를 수 있다.

### archive_manifests updated_at

운영 기록 이관 파일의 검증 내역 행을 마지막으로 수정한 시각.

- 형식 후보: UTC 날짜와 시각
- 예시: `2026-10-12T01:01:00Z`
- 해석: UTC로 저장한다. 행의 최종 갱신 시각이며 원천 가격의 수집 시각이나 파일 내용 변경 시각을 대신하지 않는다.
