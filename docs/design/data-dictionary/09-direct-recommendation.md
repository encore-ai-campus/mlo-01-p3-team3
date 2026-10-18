# 직접 추천 v3 데이터 변경안

2026-10-18 SQLite 로컬 실행 원장은 `recommendations.{RecommendationJob,RecommendationAttempt,QueueControl,WorkerPresence,ProviderGate,ProviderPermit}`다. UUID/고유 요청 키·lease/token·available_at·시도/대기 시간은 [로컬 작업 속성](../../interior-recommender/execution.md#상태와-로컬-원장)을 따른다. 운영 MySQL 엔터티와 별도이며 기존 API receipt/상품 벡터를 이관·재생성하지 않는다.

[통합 계약](../12-direct-recommendation.md) · [사전](index.md) · [데이터 모델](../03-data-model.md)

2026-10-15 논리 설계다. 로컬 파일 구현과 신규 ORM/DDL·이벤트/서비스 배포 완료를 구분한다. 기존 UUID·UTC·입력/상품/벡터 checksum·소유권·불변 이력 계약을 유지한다.

## 맥락·입력

| 필드 후보 | 의미 |
|---|---|
| recommendation_contexts.id / input_revision_id | 입력 버전에 연결한 불변 맥락 UUID/FK |
| requirements_json | 품목·명시 필수 EQ/NE 속성/LE 가격·치수·원문 인용. 누락/미지원 별도 |
| room_context_json / placement_json | 방 관찰·배치 의도/좌표·스케치 역할. 사용자 필수조건으로 승격하지 않음 |
| size_estimate_json / validation_json | 근거 유형·축/단위·범위·참조/검사 결과·오류/미확인. 실제 상품 규격과 별도 |
| analysis_refs_json / policy_version | 해석/질의 임베딩 원응답·모델/스키마·checksum·코드 정책 |

로컬 RecommendationContext는 required/preferred_color/room_context/placement_region/size_estimate 등으로 구현한다. required의 source_quote 존재·숫자/단위 환산은 검사하지만 AI 해석의 사실 여부는 보증하지 않는다. 실측 참조 저장소가 없어 로컬은 VERIFIED_DIMENSION을 받지 않으며 추정치로 하드 필터/초기 점수를 계산하지 않는다. 실제 공간 적합성은 UNKNOWN이다.

## 추천 버전·점수

| 필드 후보 | 의미 |
|---|---|
| recommendation_runs.input_revision_id / context_id | 입력/맥락 FK. context는 접수 중 NULL 가능, 검색 전 확정 필요 |
| schema_version / ranking_version | 서비스 계약 버전 / weighted-content-v1 순위·동점 정책 |
| ranking_policy_json | similarity_weight .85 / color_weight .15, 미선호 시 similarity 1.0, 결측은 가산 없음, MANUAL_INITIAL, 라이브러리 버전 |
| index_version_id / embedding_ref_json | 상품/이미지/벡터 행 대응·checksum·모델·차원·전처리 고정 |
| candidates_ref_json | 승인 저장소의 요청 당시 카탈로그/평가 후보 재현 참조. 로컬은 고정 인덱스 + 반환 항목만 저장 |
| ranking_model_ref_json | 후속 지도학습 도입 시 선택 사항. 초기 운영의 필수 필드가 아님 |
| recommendation_items.ranking_score | 콘텐츠 수동 가중합 점수. 학습 예측/확률/정확도 아님 |
| score_components_json / feature_snapshot_json | similarity 및 preferred_color 기여분·원 유사도·당시 메타데이터/결측 |
| condition_status / match_explanation_json | MATCH 또는 UNKNOWN·조건별 상태. DIFFERENT 필수 후보는 제외 |
| projects.latest_recommendation_run_id | 최신 입력 순번/완료를 대조해 조건부 갱신 |

필수 조건 미확인 후보는 충족 목록에 섞지 않는다. 점수→유사도 내림차순→product_id/image_id 오름차순이며 동일 상품은 가장 높은 이미지 점수 한 개다. 상품·가격·규격은 원장 출처/시점에 연결하고 구매 URL은 임의 생성하지 않는다.

로컬 manifest.direct_contract는 context_version/ranking_policy/index_version을 input_hash에 포함한다. recommendations.json은 policy·weight_source·sklearn_version·index_version·score_components·items/unverified_items·시간을 저장한다. 학습용 model_score를 초기 콘텐츠 점수로 혼용하지 않는다. 실제 DDL 전환은 별도 구현한다.

## 원천·메타데이터·실험

상품 카탈로그 행은 image_id/product_id/name/image/image_checksum와 원장 확인 category/color/material/shape/price_krw/width_mm/depth_mm/height_mm·metadata_source/observed_at를 가진다. 메타데이터가 있으면 출처가 필수다. vectors.npy는 동일 행 순서 float32이며 checksum을 기록한다. 이미지-only 인덱스는 품목/상품 대응 UNKNOWN으로 남긴다.

초기는 라벨 없이 실행한다. 실제 노출/반응은 별도 연결하며 미노출·미클릭을 부적합으로 만들지 않는다. 후속 학습은 승인·보관/삭제·시간/그룹 분리·위치 편향·미래 누수 검증 후 비교한다. 초기에는 직렬화 모델 파일을 요구하지 않는다.

코드/CPU 계산은 과금 외부 AI 호출로 기록하지 않는다. INDEX_NOT_READY/RANKING_FAILED/NO_RESULTS를 구분하고 로컬 저장/계산 실패에서 성공 원응답·질의 벡터를 재사용한다. 서비스 DB/Kafka/Bronze·링크 통합은 미완료다.

## 상품 골드와 추천 결과의 경계

상품 입력은 [실버·골드 필드 계약](01-raw-delta.md#실버골드-필드-계약-적용-범위)에 따른 확인값·출처·옵션·준비 상태다. `recommendation_ready`는 상품/이미지/벡터 준비이며 요청의 필수조건 충족을 뜻하지 않는다. 요청별 MATCH/DIFFERENT/UNKNOWN·유사도·색상 기여분·최종 점수는 run/item에 고정하고 Gold 상품 속성에 저장하지 않는다.

초기는 `weighted-content-v1` 수동 가중치와 `MANUAL_INITIAL`을 유지한다. 선호 색상이 없으면 유사도 1.0, 있으면 유사도 0.85와 색상 일치 0.15를 사용한다. 미제공 색상에만 가중치를 재분배하지 않는다. 후속 라벨/학습은 현재 실행·카탈로그 준비의 필수조건이 아니다. 미확인 가격·치수·재질/형태를 추정값으로 채워 필수조건 MATCH로 만들지 않는다.
