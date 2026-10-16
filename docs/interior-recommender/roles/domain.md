# AI 응답 검증과 추천 규칙

[역할별 라우팅](../README.md) · [직접 추천 계약](../../design/12-direct-recommendation.md) · [크기 정책](../../design/11-scale-policy.md) · [상품 연결](../../design/09-product-links.md)

`domain/recommendation.py`는 `RecommendationContext`, `Condition`, `SizeEvidence`, `Product`, `RankingPolicy`, `ProductIndex`와 맥락 prompt/파싱/검증을 담당한다. `domain/product_links.py`는 `comparison_url()`을 담당한다. 외부 AI 실행은 이 모듈에서 하지 않고 실행 계층이 `Pipeline → adapters`로 호출한다.

멀티모달 API는 의미·관찰·크기 근거를 해석한다. `parse_context()`·`validate_context()`는 응답·품목·원문 인용·단위/좌표의 일관성을 확인한다. 이 검증은 해석의 사실 여부를 보장하지 않는다. 현재 실측 근거 저장소는 없으며 추정 크기는 표시용이고 초기 순위에 쓰지 않는다. 설치 적합성은 UNKNOWN이다.

`ProductIndex.load()`는 이미지/벡터 경로·checksum·행 대응을 확인하고, `validate_settings()`는 임베딩 모델·차원·전처리의 호환을 확인한다. `rank()`의 순서는 다음과 같다.

1. 품목·필수 조건 대조: DIFFERENT는 제외, 결측/미지원은 UNKNOWN으로 분리한다.
2. 남은 후보를 2,048행 청크로 `cosine_similarity` 계산한다.
3. 색상 선호가 있으면 기본 `0.85 × 유사도 + 0.15 × 색상 일치`, 없으면 유사도만 사용한다.
4. 각 결과 그룹에서 같은 product_id의 최고 이미지 하나를 남긴다. 점수→유사도 내림차순, product_id→image_id 오름차순으로 정렬한다.
5. `items`와 `unverified_items`를 기본 각 6개 반환하고 점수 기여분·조건 상태·버전을 기록한다.

수동 초기 가중치는 학습값이 아니며 점수는 확률이 아니다. 상품 가격·규격·URL은 원장에서 가져온다. `comparison_url()`은 `https://prod.danawa.com/info/`와 `pcode`의 정확한 상품번호 대응을 검사한다. 링크 조회 시 실행 계층이 출처·저장 추천 포함 여부·소유권을 추가 확인한다. 실시간 판매 여부 확인은 별도다.

검증 위치는 `test_recommendation.py`, `test_adapter.py`, `test_product_links.py`다. 규격 정제 변경은 [카탈로그 담당](catalog.md)과 함께 확인하고 응답 스키마 변경은 `services.py::SCHEMAS`와 `adapters/gemini.py`의 wire schema도 확인한다.
