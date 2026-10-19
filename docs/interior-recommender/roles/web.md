# 웹 진입과 Canvas 연동

2026-10-19 입력 삭제 보완: SketchOn 추천 접수의 `text`는 설명 입력창의 현재 값만 저장한다. 캔버스 텍스트 객체의 원문은 별도 텍스트에 합치지 않고 현재 렌더링된 스케치 이미지에서만 해석한다. 지우개 `cuts` 이후에도 편집·실행 취소용 객체 원문이 남을 수 있으므로 이를 다시 필수 조건의 인용 근거로 사용하지 않는다. 기존 저장 추천은 유지하고 수정한 입력은 새 추천으로 접수한다.

2026-10-18: 두 추천 화면의 카드 버튼과 공유 `web/ui/product-preview.mjs`에서 [배치 미리보기](../product-preview.md)를 제공한다. `POST api/runs/{parent}/previews`는 추천 image_id만 받고 기존 소유권·CSRF·상태/asset API를 사용한다. SketchOn은 `ui/product-preview.mjs`로 공통 스크립트를 제공한다. 계정 변경·입력 교체 시 이전 미리보기와 늦은 응답을 비운다.

2026-10-16 SketchOn 통합: `src/demo_sketchon/recommendations/`가 같은 Django 프로세스의 `/interior-recommand/`를 담당한다. `submissions → queue`로 접수하고 별도 SQLite 작업 워커가 `application.direct.DirectStudio.prepare`를 호출한다. 프로젝트/계정 권한·CSRF·multipart 검증은 웹 앱이 처리한다. 상태/링크는 카탈로그 없는 `DirectRunReader`로 읽는다. QUEUED/RETRY_WAIT도 폴링하며 준비 불가·포화는 503/Retry-After다. [책임·실행·복구](../execution.md)를 따른다. 브리지와 독립 서버 설정은 사용하지 않는다. [설치·API·권한·검증](../../../src/demo_sketchon/docs/ver2/직접추천_통합_실행.md)을 따른다. 아래 표는 독립 서버의 기존 진입점이다.

[역할별 라우팅](../README.md) · [호출 구조](../call-structure.md) · [전송·설치 계약](../../../src/interior_recommender/web/INTEGRATION.md)

`web/__main__.py`는 Django 설정, `web/views.py::studio()`는 구현 선택을 담당한다. 기본 화면은 `direct.html`·`direct.js`, 기존 생성 화면은 `studio.html`·`studio.js`다. RoomPick 요청은 `roompick_bridge.bridge()`가 AI 서버로 중계한다.

| 요청 | 핸들러 → 호출 | 담당 경계 |
|---|---|---|
| `POST /api/runs` | `views.submit → DirectStudio.submit → launch` | 사진/스케치·document·text·request_id 검증, 202 응답 |
| `GET /api/runs/{run_id}` | `views.state → DirectStudio.state` | 소유 run의 상태·결과 조회 |
| `POST /api/runs/{run_id}/actions/prepare` | `views.action → DirectStudio.launch` | `retry`를 포함한 준비 재시도 |
| `GET /assets/{run_id}/{name}` | `views.asset → Studio.directory` | 소유권과 등록 asset 확인 |
| `GET /api/runs/{run_id}/product-link?image_id=...` | `views.product_link → DirectStudio.product_link` | 저장 결과의 상품만 302 연결, 부적격은 404 |

외부 브라우저는 브리지의 `/ai-mvp/` 접두 경로를 사용할 수 있다. 세션 소유권과 CSRF는 웹 책임이며 인증된 브리지 요청은 `BridgeAuthentication`의 owner 처리를 따른다. 사진 바이트와 Canvas 원문을 전송하고 서버가 `project.photo_url`을 가져오는 방식으로 바꾸지 않는다.

추천 정책과 AI 호출은 [실행 담당](application.md)·[규칙 담당](domain.md)에 둔다. HTTP 변경 시 `test_recommendation.py`, `test_web_workflow.py`, `test_product_links.py`로 기본 모드·소유권·브리지·리다이렉트를 확인한다. 기존 `templates`/`selection` URL이 존재해도 직접 추천의 템플릿 선택 절차로 안내하지 않는다.
