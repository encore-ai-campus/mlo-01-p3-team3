# 방 특징을 반영하는 콘텐츠 추천

2026-10-18 구현. 새 요청의 기본은 `direct-context-v3`와 `interior-content-v3`다. MySQL 연결·적재·배포는 이번 구현에 포함하지 않는다. 기존 파일 상품 카탈로그와 사전 임베딩으로 실행한다. 실제 API 해석 품질·추천 품질·실서버 부하는 아직 검증하지 않았다.

## 실행 흐름과 책임

1. 기존 멀티모달 해석 1회에서 `room_profile`의 palette/materials/shapes/styles, 관찰 evidence, uncertainties를 받는다. 기존 자유문장 `room_context`도 유지한다. 스케치 선 색상·배치 사각형·요청 가구 특징을 방의 특징으로 혼용하지 않는다.
2. `domain/interior.py`가 지원 코드/별칭을 정규화한다. 근거 없는 특징은 입력 오류이며 지원하지 않는 코드는 가점에 사용하지 않는다. 관찰 근거 문자열이 실제 사실을 검증했다는 뜻은 아니다.
3. `application/direct.py`가 방 특징을 기존 검색 문장에 추가하고 질의 임베딩 1회를 요청한다. 상품 벡터를 재생성하지 않는다.
4. `ProductIndex.rank()`는 기존 필수 조건 판정과 scikit-learn 코사인 유사도 계산 후 방 특징 유사성 가점을 더한다. 방 관찰 자체는 필수 제외 조건이 아니다. 기존 v2와 같이 재질 조건은 검색 참고이며 강제 일치를 보장하지 않는다.
5. 반환 항목에 `interior_match`와 `match_explanation`을 기록하고 독립 웹·SketchOn 상품 카드에 근거를 표시한다. 결과/receipt는 불변 재사용한다. 추정 치수로 설치 가능성을 판정하지 않는다.

## 점수와 결측

기존 base는 사용자 색상 선호가 있으면 `0.85×cosine_similarity + 0.15×색상 일치`, 없으면 유사도다. 지원되는 방 특징이 있으면 `최종 점수 = 0.8×base + 0.2×방 특징 점수`이며 `room_weight`의 초기값은 0.2다. 방 특징이 없으면 base를 그대로 사용한다. 정책과 가중치는 run에 고정한다.

방 특징 내부 초기 비중은 색조 0.5, 재질 0.25, 형태 0.15, 스타일 0.1이다. 요청에서 관찰된 지원 특징만으로 이 비중을 정규화한다. 후보 상품의 결측에 따라 비중을 다시 나누지 않는다. 해당 상품 특징이 없으면 UNKNOWN·가점 0이며 제외하지 않는다.

각 특징은 관찰 코드와 상품 코드의 최대 유사성을 사용한다. 동일 코드 1.0, 수동 지정한 유사 색조군은 0.7, 다른 확인값은 0이다. 색조군은 흰색/아이보리/베이지, 베이지/갈색, 검정/회색, 파랑/네이비다. 복수 재질·스타일의 미적 조화를 보장하거나 보색 이론을 학습한 모델이 아니다. 모든 비중과 색조 유사성은 수동 초기 정책이며 품질 평가로 조정해야 한다. 점수는 확률이 아니다.

정책의 `room_feature_weights`, `palette_family_affinity`, `interior_version`도 요청에 고정한다. `score_components`는 실제 가중 기여분을 기록하고 합계가 최종 점수다. `interior_match`는 규칙 버전·방/상품 특징·근거·불확실성·특징별 상태/유사성/비중·가점·추천 이유를 보존한다. 원래 질의 유사도와 규칙 가점을 구분한다. 특징 snapshot의 근거는 최대 12개이며 전체 원천 속성과 출처는 반환 상품 메타데이터에 유지한다.

## 상품 입력과 향후 MySQL 경계

기존 `Product.color/material/shape`와 출처가 있는 파일 상품은 그대로 사용한다. 새 선택 필드는 `interior_profile`과 Gold 형식 `attributes_json` 배열이다. 파일에 이 필드가 없어도 동작하며 카탈로그·임베딩의 재생성은 필수가 아니다. 특징 정규화는 인덱스 로딩 시 캐시한다.

`interior_profile`은 palette/materials/shapes/styles의 문자열 배열과 evidence/uncertainties 배열이다. 특징을 넣으면 evidence와 상품 `metadata_source`가 필요하다. 상품 스타일을 원목이라는 이유만으로 natural로 만들어내지 않는다. 원천/별도 특징 추출 결과가 없으면 스타일은 UNKNOWN이고 방 스타일은 질의 의미 검색에만 기여한다.

`attributes_json`의 COLOR/MATERIAL/SHAPE/STYLE 중 `attribute_status=CONFIRMED`, `measurement_target=PRODUCT`, `option_status=NO_OPTION_CONFIRMED`, 미지정 option_key/component와 `source_evidence_key`가 있는 행만 전체 상품 특징으로 해석한다. 같은 종류의 미확인·충돌·복수 코드가 있으면 전체 상품 특징으로 확정하지 않는다. 상세 상태가 있는 종류는 기존 scalar 값으로 이 검사를 우회하지 않는다. 옵션·부위·원천 상태를 생략한 단순 code 배열은 가점 근거가 아니다. 현재 Silver에서 코드가 매핑돼도 SEMANTICS_UNKNOWN인 속성은 가점에 사용하지 않는다.

향후 MySQL 어댑터는 [게시 계약](../design/13-gold-mysql-publication.md)의 상품 속성·상태·원천을 이 입력으로 변환해야 한다. 현재 구현은 DB에 접속하거나 DDL/적재 쿼리를 실행하지 않는다. 필수조건 대조용 scalar 값과 이미지·벡터·활성 인덱스 연결 및 현재 검수 대조는 어댑터 인수 범위로 남긴다.

## 호환·검증·실행

v1/v2 정책은 방 특징 가점을 적용하지 않는다. 기존 `direct-context-v2` 실행은 당시 prompt 경로와 정책을 사용하며 저장 성공 단계/결과를 변경하지 않는다. 신규 정책으로 추천하려면 새 요청을 제출한다. 기본 정책/맥락 버전도 SketchOn 실행 설정 hash에 포함하므로 웹과 워커 코드를 함께 갱신하고 기존 워커를 종료한 뒤 다시 시작한다. 운영 계정·API 환경·배포는 별도다.

검증은 `test_interior_ranking.py`의 실제 순서 변경·필수조건 우선·결측/옵션/부위/충돌·스타일 미추측·질의 조립·재시도 무추가 호출과 기존 추천/제공처/워커 회귀로 수행한다. 합성 fixture만 사용하며 유료 API·실제 DB를 호출하지 않는다.

저장소 루트에서 설치된 Python으로 실행한다. PowerShell 명령은 한 줄이다.

```powershell
..\.venv\Scripts\python.exe -m pytest -q -c src/interior_recommender/pyproject.toml src/interior_recommender/tests -p no:cacheprovider
```

새 로직은 기존 SketchOn 웹/워커 실행 경로에 자동 적용된다. 카탈로그 경로를 바꾸거나 기존 벡터를 재생성할 필요는 없다. 명시적인 v1/v2 정책 파일을 설정한 경우 그 정책을 유지하므로 새 정책 사용 여부를 실행 결과의 `ranking_version`으로 확인한다.
