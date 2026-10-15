# 직접 추천 v1 데이터 변경안

2026-10-15 논리 설계. 신규 ORM/DDL·마이그레이션·이벤트 배포 완료를 뜻하지 않는다. [통합 계약](../12-direct-recommendation.md) · [사전 안내](index.md) · [모델](../03-data-model.md).

## 키와 관계

기존 UUID PK·UTC·파일 checksum·입력 버전·상품 식별 규칙을 유지한다. 신규 run은 생성물 선택을 요구하지 않는다. `input_revision_id`가 입력의 기준이며 `context_id`는 확정 해석/정책 버전이다. 기존 scene_object/generation 연결은 이력 조회에만 유지하고 새 직접 추천의 필수 FK로 사용하지 않는다.

| 엔터티·필드 후보 | 형식/관계 | 의미 |
|---|---|---|
| recommendation_contexts.id | UUID PK | 입력 해석·코드 검증의 불변 맥락 버전 |
| recommendation_contexts.input_revision_id | UUID FK, NOT NULL | 같은 소유 프로젝트의 입력 버전 |
| recommendation_contexts.analysis_refs_json | JSON, NOT NULL | 외부 ai_analysis_runs·모델/프롬프트/스키마·원응답/출력 참조 및 checksum |
| recommendation_contexts.requirements_json | JSON, NOT NULL | 품목·명시 속성·부정 조건·예산/치수·원문/영역 출처. 미지정 null |
| recommendation_contexts.room_context_json | JSON, NOT NULL | 방 색상·재질·스타일·근거/불확실성. 사용자 필수조건과 분리 |
| recommendation_contexts.placement_json | JSON, NOT NULL | 원본 기준 영역/null·배치 문장·스케치 역할·근거 |
| recommendation_contexts.size_estimate_json | JSON, NOT NULL | basis·근거 참조·축/단위별 범위/null·원근/깊이 가정·오차/미확인 |
| recommendation_contexts.validation_json | JSON, NOT NULL | 형식/참조 검사·조건별 오류/미지원·축별 크기 사용 정책. 의미 정답 판정 아님 |
| recommendation_contexts.policy_version | 문자열, NOT NULL | direct-recommendation-v1 등 해석 후 처리 정책 |
| recommendation_runs.input_revision_id | UUID FK, v2 NOT NULL | 추천 접수 시 고정한 입력 |
| recommendation_runs.context_id | UUID FK, 접수 시 NULL 허용 | SEARCHING 전에는 검증된 동일 입력 맥락 필수 |
| recommendation_runs.schema_version | 정수, NOT NULL | 기존 생성물 추천 계약과 신규 직접 추천 계약 구분 |
| recommendation_runs.ranking_version | 문자열, NOT NULL | 운영 규칙·가중치·동점 순서. 실험 모델 버전과 별도 |
| recommendation_runs.feature_schema_version | 문자열 | 특징 이름·단위·결측 표현의 버전 |
| recommendation_runs.index_version_id | 기존 검색 버전 FK | 모델·차원·전처리·상품 매핑 고정 |
| recommendation_runs.candidates_ref_json | JSON/NULL | 평가 후보 전체·특징·시점/상품 버전 manifest 참조·checksum. 후보 준비 전 NULL |
| recommendation_items.feature_snapshot_json | JSON | 요청 당시 유사도·조건/스타일 특징·크기 비교·결측. 행동 라벨 미포함 |
| recommendation_items.match_explanation_json | JSON | 조건별 MATCH/DIFFERENT/UNKNOWN·크기 근거·코드 추천 이유 |
| projects.latest_recommendation_run_id | UUID FK/NULL | 최신 입력 순번·완료 상태를 확인해 조건부 갱신 |

같은 입력으로 해석을 다시 수행하면 새 맥락을 만들고 이전 추천의 맥락은 바꾸지 않는다. 성공 API 출력 재사용으로 저장만 복구하는 경우 같은 맥락/시도 ID로 멱등 처리한다. 실제 고유 제약·상태 전이·DDL 세부는 마이그레이션 작업에서 검증한다. 새로운 컬럼이 현재 DB에 있다고 가정하지 않는다.

## 크기·속성 의미

basis는 VERIFIED_DIMENSION/TYPICAL_OBJECT_SIZE/RELATIVE_ONLY/NONE이다. 검증된 실측 참조는 코드가 실제 저장 자료와 대조한다. 축별 실제 mm와 사진상 비율을 구분하며 근거 없는 값은 null이다. AI 추정으로 products의 실제 width_mm/depth_mm/height_mm를 덮어쓰지 않는다. 규격은 옵션·축·측정 대상·단위를 정제한 원장 값을 사용한다.

requirements는 명시 조건, room_context는 관찰이다. 부정 조건·미지원 조건·결측은 구분한다. 상품 규격이나 속성 누락은 UNKNOWN이며 자동 불일치/충족으로 변환하지 않는다. 임베딩 유사도는 사용자 만족도/정확도 백분율이 아니다.

## 작업·이벤트·실험

ai_analysis_runs는 INPUT_CONTEXT·QUERY_EMBEDDING 등 실제 외부 호출의 모델/시간/원응답을 기록한다. 코드 검증/규칙 정렬은 별도 로컬 시간/정책 메타데이터로 남기며 과금 호출로 만들지 않는다. jobs/job_attempts의 입력 참조는 recommendation_run_id/input_revision_id와 단계 버전이며 생성 실행 FK가 신규 필수는 아니다.

user_events는 추천 run·항목·실제 impression과 연결한다. 생성 요청 이벤트는 신규 흐름에서 발생하지 않는다. 선택·거절·클릭은 이후 관측 라벨이며 요청 시점 특징에 넣지 않는다. 미노출·미클릭은 부적합/거절과 다르다.

오프라인 실험은 승인된 Delta export/manifest에 데이터/특징/라벨/분할/모델/코드 버전·seed·NDCG/조건 위반·CPU 지연/메모리를 남긴다. 노출 위치·반응 지연·동일 입력/상품 변형의 누수를 고려한다. 실험 결과는 서비스 recommendation_items/ranking_version을 덮어쓰지 않는다. 권한·보관/삭제 범위는 원천 승인과 별도로 확인한다.
