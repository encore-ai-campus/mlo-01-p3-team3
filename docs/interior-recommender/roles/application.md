# 추천 실행 조정

2026-10-18 SketchOn 실행: 웹은 `submissions → queue`로 원본·고정 계약과 영속 작업을 접수한다. 별도 `recommendations.worker`가 선점한 뒤 `DirectStudio.validate_run/prepare`를 재사용한다. `application.deferred.DeferredPipeline`이 API 단계에서 실행을 양보하고 워커가 native async 응답/receipt를 보존한다. 성공 단계는 재사용하고 재시도 시각은 SQLite에 보존해 슬롯을 반납한다. [실행 상세](../execution.md). 아래 `launch()` 설명은 독립 추천 웹의 기존 실행 방식이다.

[역할별 라우팅](../README.md) · [호출 구조](../call-structure.md) · [Application 설계](../../design/04-application-ai.md)

실제 구현은 `application/direct.py::DirectStudio`다. `submit()`이 UUID·입력 조합·이미지 크기를 확인하고 원본/사본·manifest·event 대응을 저장한다. 같은 owner/event_id의 같은 입력은 기존 run을 반환하고 입력이 다르면 거절한다. run 접수 시 해석 버전·전체 순위 정책·인덱스 버전을 고정한다.

SketchOn의 선택적 스타일은 `external_project.style_policy=color-first-v2`와 `style_preferences`로 별도 고정한다. 크림 패브릭→아이보리, 오크 우드→밝은 갈색·베이지, 세이지 그린→차분한 초록으로 질의 검색어를 조립한다. 재질 단어 대신 색상·톤을 임베딩에 넣고 기존 `preferred_color` 가중치에 연결한다. 사용자 원문의 명시 색상 EQ 조건, 원문에서 해석한 색상 선호, 선택 스타일 순으로 우선한다. 원본 `context.json`/receipt를 바꾸지 않고 순위용 사본에만 반영하며 결과의 `color_preference`로 사용 색상과 출처를 표시한다. 색상 결측·불일치만으로 제외하지 않으며 미확정 옵션을 전체 상품 색상으로 확정하지 않는다. 과거 `search-only-v1` run은 기존 스타일 원문 검색 방식을 유지한다.

`launch()`는 직접 추천에 `prepare`만 허용하고 상위 `Studio.launch()`가 제한된 스레드풀에 `_work()`를 제출한다. `_work()`는 run lock·입력 hash·모드·맥락 버전·인덱스 호환을 확인하고 `_prepare()`를 실행한다. 사용한 adapter는 finally에서 닫고 작업 결과는 `job.json`에 쓴다.

| 단계 | 호출 | 보존 결과 |
|---|---|---|
| 맥락 해석 | `Pipeline.stage(context, direct_context, ...)` | `receipts/context.json`, `context.json` |
| 코드 검증·질의 | `parse_context`, `validate_context`, category/search_terms/room_context 조립 | 해석 결과와 원문 검증, 질의 prompt |
| 질의 임베딩 | `Pipeline.stage(query, embed_text, ...)` | `receipts/query.json`, `query.json` |
| 추천·보존 | `ProductIndex.rank`, 반환 이미지 checksum 검사/사본 | `recommendations.json`, `products/`, manifest의 ranking stage |

기본 순위 정책 `weighted-content-v2`는 재질 조건의 원문 인용을 질의 임베딩에 추가한다. 재질 조건은 `preference_checks`로 대조만 기록하고 필수 필터에서 제외한다. 색상 가중치와 다른 필수 조건은 유지한다. 기존 v1 run은 새 질의나 새 정책으로 바꾸지 않는다.

해석 결과가 ACCEPTED가 아니면 해당 decision 상태로 종료한다. 추천 결과는 충족 후보가 있으면 `COMPLETED`, 미확인 후보만 있으면 `UNVERIFIED_ONLY`, 둘 다 없으면 `NO_RESULTS`다. 인덱스가 없거나 고정 버전이 다르면 `INDEX_NOT_READY`, 순위/추천 저장 오류는 `RANKING_FAILED`다. API/로컬 변환 실패는 공통 Pipeline stage와 job을 함께 확인한다.

성공 ranking은 저장 checksum 확인 후 재사용한다. 순위만 실패하면 성공 해석/임베딩을 재사용한다. 입력/정책을 바꾸려면 새 요청을 제출한다. 구체적인 receipt 복구는 [운영](operations.md), 조건 대조는 [추천 규칙](domain.md)을 따른다. 핵심 회귀 검증은 `test_recommendation.py`의 정책 고정·API 재사용·순위 실패 복구다.
