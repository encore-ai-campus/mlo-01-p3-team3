# 추천 실행 조정

[역할별 라우팅](../README.md) · [호출 구조](../call-structure.md) · [Application 설계](../../design/04-application-ai.md)

실제 구현은 `application/direct.py::DirectStudio`다. `submit()`이 UUID·입력 조합·이미지 크기를 확인하고 원본/사본·manifest·event 대응을 저장한다. 같은 owner/event_id의 같은 입력은 기존 run을 반환하고 입력이 다르면 거절한다. run 접수 시 해석 버전·전체 순위 정책·인덱스 버전을 고정한다.

`launch()`는 직접 추천에 `prepare`만 허용하고 상위 `Studio.launch()`가 제한된 스레드풀에 `_work()`를 제출한다. `_work()`는 run lock·입력 hash·모드·맥락 버전·인덱스 호환을 확인하고 `_prepare()`를 실행한다. 사용한 adapter는 finally에서 닫고 작업 결과는 `job.json`에 쓴다.

| 단계 | 호출 | 보존 결과 |
|---|---|---|
| 맥락 해석 | `Pipeline.stage(context, direct_context, ...)` | `receipts/context.json`, `context.json` |
| 코드 검증·질의 | `parse_context`, `validate_context`, category/search_terms/room_context 조립 | 해석 결과와 원문 검증, 질의 prompt |
| 질의 임베딩 | `Pipeline.stage(query, embed_text, ...)` | `receipts/query.json`, `query.json` |
| 추천·보존 | `ProductIndex.rank`, 반환 이미지 checksum 검사/사본 | `recommendations.json`, `products/`, manifest의 ranking stage |

해석 결과가 ACCEPTED가 아니면 해당 decision 상태로 종료한다. 추천 결과는 충족 후보가 있으면 `COMPLETED`, 미확인 후보만 있으면 `UNVERIFIED_ONLY`, 둘 다 없으면 `NO_RESULTS`다. 인덱스가 없거나 고정 버전이 다르면 `INDEX_NOT_READY`, 순위/추천 저장 오류는 `RANKING_FAILED`다. API/로컬 변환 실패는 공통 Pipeline stage와 job을 함께 확인한다.

성공 ranking은 저장 checksum 확인 후 재사용한다. 순위만 실패하면 성공 해석/임베딩을 재사용한다. 입력/정책을 바꾸려면 새 요청을 제출한다. 구체적인 receipt 복구는 [운영](operations.md), 조건 대조는 [추천 규칙](domain.md)을 따른다. 핵심 회귀 검증은 `test_recommendation.py`의 정책 고정·API 재사용·순위 실패 복구다.
