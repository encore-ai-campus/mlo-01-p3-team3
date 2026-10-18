# SketchOn Membership Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 자체 회원가입·로그인, 비회원 IP별 하루 3회 제한, 프로젝트 버전 저장·수정·비교, 회원 찜 목록과 작은 우측 광고 자리를 기존 데모에 확장한다.

**Architecture:** Django 기본 인증과 Project.account를 사용하고 기존 owner·Raw 파일은 유지한다. 생성 제한은 DB 트랜잭션 안에서 결과 저장과 함께 차감하며, 저장 프로젝트/버전과 찜은 기존 Composition·추천 실행을 참조하는 별도 모델로 보관한다. 인증·생성 제한·라이브러리 기능은 공통 접근 계약에 의존하므로 한 통합 계획 안에서 의존 순서대로 구현한다.

**Tech Stack:** 기존 Python/Django/Pillow/SQLite, Django auth/forms, HTML templates/CSS/ES modules, 기존 Node·Edge/Playwright 검사. 새 외부 서비스·UI 라이브러리는 추가하지 않는다.

**Spec:** [승인된 설계](../specs/2026-10-16-sketchon-membership-design.md)

## Global Constraints

- 대상 루트는 `C:\mlo-01-p3-team3\src\demo_sketchon`이다. 아래 상대 경로는 모두 이 루트 기준이며 기존 `C:\CHAPTER3\roompick`에 앱 변경을 적용하지 않는다.
- 설계는 2026-10-16 사용자의 진행 요청으로 확정됐다. 이 계획을 검토한 뒤 실행 방식을 선택해야 구현을 시작한다.
- 비회원: 서버가 확인한 IP별 한국 시간 하루 3회, 성공한 새 공간 생성만 차감. 다운로드·동일 요청 재전송·실패·배치 조절·추천은 비차감, 회원은 무제한.
- 아이디·비밀번호·닉네임 가입/로그인. 비밀번호는 Django 해시·검증만 사용하고 이벤트 payload에 평문·해시·CSRF·세션 토큰을 남기지 않는다.
- 현재 브라우저의 아직 account가 없는 작업만 로그인 계정에 연결한다. owner·writer_id·Raw 스키마·파일 내용을 바꾸지 않는다.
- 비회원 저장/찜 버튼 미노출과 서버 API 거절을 함께 적용한다. 다른 회원·로그아웃한 회원 기록을 owner 문자열만으로 열 수 없어야 한다.
- 프로젝트 이전 버전의 이미지·입력·배치는 불변이다. 같은 프로젝트 수정은 다음 버전으로 저장하고 두 개의 본인 저장 이미지를 비교한다.
- 가구 사양 비교·실제 광고·개인화·실제 AI·판매 API는 다음 단계다. 우측 광고는 약 80~110px 높이의 placeholder이며 모바일에서는 아래로 배치한다.
- 기존 그림판·사진·결과·Raw·이벤트 기능·검정 Sketch · On 로고를 유지한다. docs/ver1 원본은 보존하며 현재판 docs/ver2와 루트 README를 갱신한다.
- 실행 전 DB 온라인 백업·미디어/Raw 해시 목록·수정 파일 보관. 테스트는 별도 DB/미디어/Raw를 사용한다. 소스·테스트 완료 전에 실제 DB migration이나 서버 재시작을 하지 않는다.
- 대상 경로는 기본 쓰기 허용 범위 밖이다. 필요한 쓰기 권한을 요청하며 권한 경계를 다른 도구로 우회하지 않는다.
- 변경은 작업별로 검토 가능한 단위로 유지한다. Git 상태·사용자 변경을 확인하고 기존 파일을 임의 reset/stash하지 않는다. 커밋이 필요한 경우 이번 작업 파일만 선택한다.

## Review Focus

1. 같은 브라우저에서 A 로그아웃 후 B 로그인: A 사진·Raw·추천·저장 이미지·찜과 늦은 응답이 B 화면에 보이지 않음 — Task 1·6·9.
2. KST 자정 직전 요청, 동일 IP 동시 생성·첫 카운터 행 생성 경합: 성공은 날짜별 3회 이내이고 충돌/파일 실패는 횟수를 남기지 않음 — Task 3·9.
3. 로그인 상태에서 다른 기기의 기존 작업 편집: 해당 Project.owner 기반 Raw.writer_id 유지, 새 브라우저 식별자와 혼동하지 않음 — Task 1·6·9.
4. 저장과 배치 이동 경합·중복 저장 요청: 저장 PNG와 snapshot의 version 일치, 이전 버전은 불변 — Task 4·9.
5. 요청마다 새 ID인 모의 추천 항목과 사라진 임시 이미지: 같은 품목/variant 찜은 중복되지 않고 소유권이 있는 안정된 상품 snapshot으로 표시 — Task 5·7·9.

## 파일과 책임

| 생성·수정 | 파일 | 책임 |
| --- | --- | --- |
| 생성 | sketch/access.py | 익명 식별·회원 연결·프로젝트/결과/추천 접근 공통 조건 |
| 생성 | sketch/accounts.py | Django Form·회원 status DTO·가입/로그인/로그아웃·회원정보 |
| 생성 | sketch/quota.py | 서버 IP·KST 날짜·일별 잔여/차감·제한 예외 |
| 생성 | sketch/library.py | 저장 버전·이미지·복원·비교·찜 API와 마이페이지 views |
| 수정 | sketch/models.py | Project.account/saved_project, 사용량·저장 프로젝트/버전·찜 모델 |
| 생성 | sketch/migrations/0007_membership_library_quota.py | 기존 데이터 기본값을 보존하는 확장 migration |
| 수정 | config/settings.py, sketch/urls.py, sketch/views.py, sketch/platform_views.py | 인증 연결·권한·생성 제한·프로젝트 계보·새 URL |
| 수정 | sketch/platform.py | 안정된 mock 상품 키·찜용 서버 DTO |
| 생성 | sketch/templates/sketch/account_navigation.html, ad_placeholder.html, my_base.html | 공통 계정 헤더·광고 자리·회원 좌측 메뉴 |
| 생성 | sketch/templates/sketch/login.html, signup.html, profile.html, wishlist.html, saved_projects.html, saved_project_detail.html, saved_compare.html | 인증·회원 화면 |
| 수정 | sketch/templates/sketch/home.html, editor.html, result.html | 공통 계정·광고·저장 버튼과 데이터 연결 |
| 생성 | sketch/static/sketch/account-state.mjs, library.mjs | 인증 재확인·안전한 복귀·저장/찜/비교 UI |
| 수정 | sketch/static/sketch/editor.mjs, result.mjs, events.mjs, roompick-ui.css | 입력 보존·권한 상태·버튼·반응형 |
| 생성 | sketch/test_accounts.py, test_quota.py, test_library.py, tests/membership-browser.cjs | 회원·quota·버전/찜·브라우저 회귀 |
| 수정 | 기존 서버/브라우저 테스트, README.md, docs/ver2/*.md | 신규 인증과 호환, 현재 설명 갱신 |

## Task 1: 모델·공통 접근·회원 귀속 계약

**Files:** models.py, 새 access.py·migration·test_accounts.py, 기존 views.py/platform_views.py.

**Interfaces:** `visitor_id(request)->str`, `project_scope(request)->Q`, `owned_project(request,id)->Project`, `owned_result(request,id)->Composition`, `claim_guest_projects(request,user)->int`. 공통 필터는 인증 시 account_id=user.id, 비회원 시 account IS NULL AND owner=visitor_id 조건이다. 추천 실행은 composition.project에 이 조건을 적용한다.

- [ ] 기존 변경 상태와 실제 실행 프로세스 경로를 확인하고 수정 대상 파일을 보관한다. SQLite backup API로 DB를 복사하며 미디어·Raw의 상대 경로/SHA-256·기존 모델 PK 목록을 남긴다. 백업 경로는 `C:\mlo-01-p3-team3\src\demo_sketchon-backups\2026-10-16-membership`로 한다.
- [ ] `test_guest_claim_preserves_owner_and_raw`, `test_logout_cannot_read_claimed_project`, `test_other_account_cannot_read_same_owner`, `test_account_reads_own_project_from_other_session`을 먼저 작성해 현재 권한 계약과 다른 지점을 확인한다.

```python
assert project.account_id == user.id
assert project.owner == previous_owner
assert second_account_client.get(project_photo_url).status_code == 404
assert raw_file.read_bytes() == raw_before
```

- [ ] 기본 User를 위한 auth/middleware/context processor·비밀번호 검증 설정을 추가하고 Project.account nullable FK를 만든다. 나머지 신규 모델은 아래 Task 3~5 필드 계약대로 같은 migration에 포함하되 기존 행을 계정으로 추정해 자동 귀속하지 않는다.
- [ ] owner 함수를 access.visitor_id로 옮기고 views.owner는 필요 시 호환 import로 둔다. 모든 기존 endpoint의 직접 owner 필터를 공통 scope로 교체한다. account가 있는 Project는 owner만 일치한다고 접근을 허용하지 않는다.
- [ ] 새 Project/crop/redraw에는 request.user 또는 원본 프로젝트의 계정과 saved_project 연결을 명시적으로 전파한다. DTO에 `writer_id=Project.owner`를 추가하고 브라우저 세션 식별자와 구분한다.
- [ ] 격리 환경에서 `python -B manage.py test sketch.test_accounts --noinput`, `makemigrations --check --dry-run`으로 새 계약을 확인한다. 테스트·변경 파일을 검토 가능한 단위로 보관한다.

## Task 2: 자체 인증·회원정보와 상태 API

**Files:** 새 accounts.py·test_accounts.py 확장, settings.py, urls.py.

**Interfaces:** `account_status(request)->JsonResponse`, `login_page/signup_page/logout_action/profile_page`; status DTO는 `authenticated, actor_key, nickname, csrf_token, quota, project_access`다. actor_key는 guest:<visitor> 또는 user:<pk>이며 계정 전환 검사에 사용한다. quota는 Task 3에서 연결한다.

- [ ] 가입 중복·비밀번호 확인·닉네임 변경·현재 비밀번호 확인·POST logout·CSRF·외부 next 거절·현재 비회원 작업만 귀속하는 테스트를 먼저 작성하고 실패를 확인한다.
- [ ] Django AuthenticationForm/UserCreationForm/PasswordChangeForm을 확장한다. 아이디는 Django 기준, 닉네임은 공백 제외 1~30자(first_name), 이메일은 요구하지 않는다. Django login 세션 키 순환을 이용하며 visitor_id는 귀속 전후 유지한다.
- [ ] 가입/로그인 성공 후 같은 서비스 next로 복귀한다. 회원정보의 비밀번호 변경은 update_session_auth_hash로 현재 로그인 상태를 유지한다. 실패 응답에 비밀번호·CSRF를 넣지 않는다.
- [ ] 로그아웃은 Django logout 후 비회원 연속성을 유지하더라도 Task 1의 account=null 필터를 적용한다. 회원 정보/상태/비공개 HTML에는 private,no-store를 적용한다.
- [ ] `/accounts/login/`, `/accounts/signup/`, `/accounts/logout/`, `/api/account/status/`, `/my/profile/`를 등록한다. status는 get_token의 새 CSRF와 선택적 project_id 접근 결과를 반환하며 다른 사용자 자료는 노출하지 않는다.
- [ ] `python -B manage.py test sketch.test_accounts --noinput` 통과, 모든 신규 변경 POST에서 CSRF가 작동하고 GET logout은 405인지 확인한다.

## Task 3: IP별 일일 생성 제한·원자적 차감

**Files:** 새 quota.py·test_quota.py, views.py.search, settings.py·models.py/migration.

**Interfaces:** `client_ip(request)->str`, `quota_status(request,at=None)->dict`, `charge_guest_generation(request,at=None)->None`. `GuestLimitReached`는 JSON 429로 변환한다. 이 함수는 외부 트랜잭션 내부에서만 차감하며 인증 회원은 건너뛴다. 날짜는 ZoneInfo('Asia/Seoul') 기준, IP는 REMOTE_ADDR를 ipaddress로 정규화한다.

- [ ] `test_three_successes_fourth_429`, `test_kst_day_rollover`, `test_same_request_replay_after_limit`, `test_forwarded_header_does_not_change_ip`, `test_failure_rolls_back_count_and_project_lock`을 먼저 작성한다.

```python
assert statuses == [201, 201, 201, 429]
assert usage.successful_count == 3
assert failed_project.locked is False
assert replay_response.status_code == 200
```

- [ ] GuestGenerationUsage(client_ip,usage_date,successful_count,updated_at), unique(client_ip,usage_date)를 사용한다. 반환 DTO에는 used/limit=3/remaining/reset_at을 제공하며 회원은 unlimited=true다.
- [ ] 기존 성공 request_id 재사용 검사를 제한 검사보다 먼저 둔다. 실패·동일 재전송을 차감하지 않는다. 합성 계산 전 빠른 제한 확인과 DB commit 직전 차감을 모두 수행한다.
- [ ] 카운터 조건부 갱신·Project 조건부 잠금·Composition 생성·성공 이벤트를 transaction.atomic에 묶는다. 차감 후 revision 충돌도 예외로 전체 rollback한다. 파일 저장 실패/거절 시 생성된 미확정 파일만 정리한다.
- [ ] 같은 request_id의 동시 경합은 원래 입력과 권한을 다시 대조해 기존 성공을 반환하고 카운터를 추가 차감하지 않는다. 첫 usage 행 생성 충돌은 유일 제약과 트랜잭션으로 처리한다.
- [ ] `python -B manage.py test sketch.test_quota --noinput` 통과. Task 9에서는 실제 별도 SQLite 서버에서 1회 남은 두 요청 경합을 검증한다.

## Task 4: 불변 프로젝트 버전 저장·복원·비교 API

**Files:** 새 library.py·test_library.py, models.py/migration, views.py 직렬화·crop/redraw.

**Interfaces:** `save_result(request,result_id)`, `saved_version_image(request,version_id)`, `edit_saved_version(request,version_id)`, `project_list/detail/compare` views. 저장 body는 `request_id, result_version, title`이며 서버가 Project.saved_project를 기준으로 기존 프로젝트 다음 버전을 선택한다.

- [ ] `test_save_v1_edit_save_v2_keeps_v1_bytes`, `test_save_replay_does_not_duplicate_version`, `test_save_stale_result_rejected`, `test_compare_other_user_denied`, `test_restore_keeps_document_context_photo`를 작성해 실패를 확인한다.
- [ ] SavedProject(id,user,title,created_at,updated_at), SavedProjectVersion(id,saved_project,version_number,composition,source_result_version,request_id,snapshot,image,created_at)를 추가한다. 버전 번호·저장 요청의 유일 제약, FK 보호를 적용한다.
- [ ] 저장은 본인 결과·현재 result_version을 검사하고 잠금 상태에서 PNG를 별도 `saved_projects/` 파일로 복사한다. snapshot에는 document/context/placements/initial_placements/photo 참조/width/height를 넣는다. 결과 배치와 동시에 변경되어 snapshot과 PNG가 다르면 commit하지 않는다.
- [ ] 첫 저장 시 SavedProject를 만들고 원본 Project.saved_project를 연결한다. 새 프로젝트 생성과 버전 할당은 같은 트랜잭션이다. 같은 source composition+version 중복은 기존 저장본으로 안내하고 새로운 명시적 저장 request의 무의미한 중복 버전도 만들지 않는다.
- [ ] 수정 endpoint는 저장 snapshot으로 unlocked 새 Project를 만들고 같은 account/saved_project를 연결한다. 옛 버전은 변경하지 않는다. 비교는 본인 SavedProjectVersion UUID 두 개만 허용하고 이미지 endpoint도 같은 권한을 검사한다.
- [ ] 저장 실패의 임시 파일 정리·DB rollback과 버전 증가 경쟁을 검사하고 `python -B manage.py test sketch.test_library --noinput`을 통과시킨다.

## Task 5: 추천 가구 찜 저장·해제·안정된 상품 키

**Files:** library.py·test_library.py, platform.py·platform_views.py, models.py/migration.

**Interfaces:** `wishlist_list_add(request)`, `wishlist_remove(request,entry_id)`, `catalog_item_key(item,category_id)->str`. 찜 POST body는 recommendation_run_id와 item_id뿐이며 DTO는 wishlist_id/item snapshot/created_at을 반환한다.

- [ ] `test_mock_item_key_stable_across_runs`, `test_like_is_not_wishlist`, `test_only_owned_run_item_can_be_saved`, `test_remove_is_user_scoped`와 중복 POST의 단일 행 보관을 먼저 검사한다.
- [ ] mock 제공자에 category+variant를 포함한 안정된 catalog_key를 추가하고 normalize_recommendations에서 보존한다. live는 실제 product_id를 사용한다. 구버전 모의 실행은 서버 item image_url의 검증 가능한 category/variant와 run 정보로 키를 복원하며 추정 불가능하면 잘못 합치지 않는다.
- [ ] WishlistEntry(id,user,stable_item_key,item_snapshot,recommendation_run,item_id,created_at), unique(user,stable_item_key)를 추가한다. item_snapshot은 소유한 run에서 읽으며 클라이언트가 임의 상품명·URL을 삽입할 수 없게 한다.
- [ ] 이미지 표시에는 보존된 원본 결과/추천 참조의 권한을 사용한다. 다른 세션에서도 본인 계정으로 같은 이미지를 열고 임시 요청의 새 UUID 때문에 찜이 중복되지 않아야 한다.
- [ ] 저장/해제 서버 이벤트는 wishlist.saved/wishlist.removed로 남긴다. PRODUCT_SELECTED/REJECTED와 별개이며 비회원 endpoint는 401이다. 사양·실제 판매 가격을 새로 만들지 않는다.
- [ ] `python -B manage.py test sketch.test_library sketch.test_platform --noinput` 통과와 기존 모의 응답 호환성을 확인한다.

## Task 6: 공통 인증 헤더·뒤로가기·입력 보존

**Files:** 새 account_navigation.html/account-state.mjs·로그인/가입 템플릿, home/editor/result 템플릿, editor/result/events JS·CSS, 회원 브라우저 검사.

**Interfaces:** `initAccountState({projectId,onChange,beforeLogin})->{refresh,destroy}`; `beforeLogin(nextUrl)`은 편집기 문서·설명 확정 및 서버 저장 후 안전한 로그인 경로로 이동한다. `onChange(status)`는 생성 중 요청·옛 추천을 무효화하고 권한에 맞는 DOM을 갱신한다.

- [ ] 브라우저 검사에 guest→login→back→forward, 다른 탭 logout, A→B 계정 전환, 미저장 스케치 로그인 후 복귀, CSRF 갱신 검사를 추가하고 현재 실패를 확인한다.
- [ ] 모든 페이지 우측 상단 공통 계정 영역을 연결한다. guest는 로그인, member는 닉네임·마이페이지·POST 로그아웃이다. 로그인/가입 폼은 기존 흰 배경·녹색 버튼·검정 영문 로고를 유지한다.
- [ ] pageshow/visible 복귀에서 status를 조회하되 무조건 reload하지 않는다. 계정·접근 불일치 시 기존 비공개 내용을 즉시 감추고 현재 계정으로 재조회/안내한다. pending old requests는 AbortController·sequence로 무효화한다.
- [ ] editor 로그인 링크는 commitText/finishRequirements 후 document+request_context+revision을 서버에 저장하고 history revision을 맞춘다. 실패하면 이동하지 않는다. Raw 구간·미전송 이력 보관은 현재 계약대로 처리한다.
- [ ] 브라우저 세션 visitor와 Project.writer_id를 구분해 collector writer를 해당 Project.owner로 초기화한다. 계정 전환 시 sessionStorage 편집 복원도 actor scope를 검사한다. 옛 계정의 큐를 새 계정 기록으로 재귀속하지 않는다.
- [ ] status의 새 csrf_token을 API 공통 전송에 반영하고 인증 후 GET 화면도 현재 계정·quota를 표시한다. 기존 action-finally의 버튼 일괄 활성화가 비회원 저장/찜 권한을 뒤집지 않는지 확인한다.

## Task 7: 프로젝트·찜·회원정보 화면과 오른쪽 광고 자리

**Files:** 새 my_base/profile/wishlist/saved_projects/saved_project_detail/saved_compare/ad_placeholder 템플릿, library.mjs, result.mjs·CSS.

**Interfaces:** result DTO의 saved_project 정보·status 권한, Task 4/5의 본인 자료 endpoint. 비교 선택은 version UUID 두 개다. 로그인하지 않은 DOM에는 저장/찜 버튼을 만들지 않는다.

- [ ] 결과→저장 v1→내역→수정→생성→v2→두 버전 비교, 찜/해제/새로고침·빈 목록을 membership-browser 검사에 추가한다.
- [ ] 결과 기존 4개 동작 옆에 회원 전용 프로젝트 저장 버튼과 제목 입력 UI를 추가한다. saving 중·배치 저장 중 비활성화하고 완료 후 버전·내역 링크를 제공한다. 현재 추천 카드에 별도 찜 토글을 추가한다.
- [ ] 마이페이지 왼쪽 메뉴 3개를 공통화하고 회원정보·찜 목록·프로젝트 카드·버전 목록·본인 이미지 두 장 비교를 구현한다. 모바일은 메뉴/비교를 세로로 바꾸며 키보드·라벨·상태 안내를 제공한다.
- [ ] ad_placeholder는 약 80~110px 보조 카드로 기존 우측 패널 아래에 배치한다. 별도 세 번째 큰 열·sticky·popup을 만들지 않는다. Home/회원 화면도 본문을 압도하지 않는 오른쪽 보조 위치, 모바일은 아래 배치다.
- [ ] 광고 자리 렌더링은 광고/상품 노출 이벤트를 만들지 않는다. 사양 비교 버튼을 동작하는 기능처럼 표시하지 않는다. 결과 제목·사진 표시폭/비율·그룹/부분 지우기 UI를 보존한다.
- [ ] 320·390·768·1366·1700px에서 header/광고/비교/기존 Canvas 넘침을 확인하고 화면 캡처를 `test-results/membership/`에 저장한다.

## Task 8: 새 사건 수집과 전체 문서 업데이트

**Files:** accounts/quota/library 서버 기록, 루트 README·docs/ver2 기존 11개, 새 docs/ver2/문서_업데이트_요약.md.

**Interfaces:** 회원 이벤트는 소유한 Project 연결을 사용한다. auth.login/auth.logout, guest.generation_limited, project.saved/restored/compared, wishlist.saved/removed로 새 기능을 구분한다. 성공 로그와 UI 의도 이벤트가 같은 의미가 아니게 설명한다.

- [ ] 계정·quota·저장·찜 테스트에 사건이 남고 비밀번호/토큰이 payload에 없는지 검사한다. 실패한 로그인은 사용자 정보 대신 일반 실패 응답/서버 로그로 처리하고 임의의 피해자 Project에 연결하지 않는다.
- [ ] README에 가입·회원 이용·IP별 3회/NAT 공유·프로젝트 버전·찜·새 migration·실행/검증 방법을 쓴다. 다음 단계인 광고/사양 비교는 미구현으로 남긴다.
- [ ] docs/ver2의 기능 목록/Flow/Raw/로그/수집기준/조회 명령/파일구조/변경이력/인수인계/계획현황/안내를 실제 완성 코드와 맞춘다. 변경이력은 8차에 이번 선택 정책·구현·검증 결과를 정리한다.
- [ ] 문서_업데이트_요약.md에 파일별 변경 내용·새 데이터·검증 범위를 표로 적고 최종 답변의 요약 링크로 사용한다. 기능 테스트는 실제 실행 결과만 기록한다.
- [ ] 기존 docs/ver1·Raw 불변 파일을 바꾸지 않았는지 해시로 확인한다. 내부 상대 링크와 PowerShell 예제는 ParseInput으로 문법 검사한다. ID·source·Raw 계약을 서비스 표시명 때문에 바꿨다고 쓰지 않는다.

## Task 9: 통합·경합·회귀 검증 및 실제 적용

**Files:** membership-browser.cjs, 기존 tests/*.cjs/서버 fixtures, 필요한 회귀 수정·검증 기록.

- [ ] 기존 기능 테스트는 생성 3회 정책 때문에 우연히 실패하지 않도록 일반 편집/결과 회귀 context를 테스트 회원으로 준비한다. guest/limit 전용 검사는 계속 실제 3회와 고정 IP를 사용한다. 운영 기본 제한을 높여 테스트를 통과시키지 않는다.
- [ ] 기존 intercepted result fixture는 auth status·공통 헤더·새 DTO를 실제 계약과 맞추고 기존 CAS·늦은 응답·추천 반응·배치 이력 검사를 보존한다.
- [ ] 별도 DB/미디어/Raw를 지정해 migration·Django 전체 검사·Node 전체 검사·기존 브라우저·새 회원 브라우저·실제 SQLite 동시성 검사를 실행한다. 마지막 1회 두 생성 중 한 건만 성공하고 동일 request 동시 요청은 결과/횟수가 한 번인지 검증한다.
- [ ] 강제 파일 저장 오류·DB rollback·저장/배치 경합·IP 날짜 경계·옛 A 계정 pending 응답→B 계정 화면을 반복 가능한 테스트로 고정한다. member 다른 브라우저의 기존 작업 Raw.writer_id와 최종 파일 내용도 검사한다.
- [ ] 통과 후 실제 DB 확장 migration을 적용하고 기존 PK·미디어/Raw 해시 보존을 확인한다. 이번 작업과 관계없는 자료를 삭제하거나 DB를 초기화하지 않는다.
- [ ] 현재 수신 프로세스 명령을 확인하고 새 프로젝트 경로의 공유 서버만 재시작한다. LAN 8003의 Home·로그인·정적 파일 및 소유권 endpoint를 확인한다. 실제 팀원 실기기 접속·공개 운영 부하는 확인하지 않았다면 미검증으로 쓴다.
- [ ] 변경 전체를 검토하고 문서별 업데이트 요약·테스트 결과·주요 제한을 사용자에게 전달한다.

## 실행 명령과 격리 경계

PowerShell은 먼저 `Set-Location C:\mlo-01-p3-team3\src\demo_sketchon`을 실행한다. 아래 python은 `.\.venv\Scripts\python.exe`를 뜻하며 PATH Python에 의존하지 않는다.

```powershell
$env:ROOMPICK_DB_PATH = 'C:\mlo-01-p3-team3\src\demo_sketchon\test-results\membership\db.sqlite3'
$env:ROOMPICK_MEDIA_ROOT = 'C:\mlo-01-p3-team3\src\demo_sketchon\test-results\membership\media'
$env:ROOMPICK_RAW_ROOT = 'C:\mlo-01-p3-team3\src\demo_sketchon\test-results\membership\raw'
New-Item -ItemType Directory -Path .\test-results\membership -Force | Out-Null
.\.venv\Scripts\python.exe -B manage.py migrate
.\.venv\Scripts\python.exe -B manage.py test --noinput
.\.venv\Scripts\python.exe -B manage.py check
.\.venv\Scripts\python.exe -B manage.py makemigrations --check --dry-run
node --test tests/*.test.mjs
.\.venv\Scripts\python.exe -B manage.py runserver 127.0.0.1:8015 --noreload
```

브라우저 검사는 별도 PowerShell에서 같은 프로젝트로 이동하고 `$env:ROOMPICK_TEST_URL='http://127.0.0.1:8015'`를 명시한다. 이미 설치된 Playwright/Edge 환경을 사용한다. 실제 서비스 실행은 테스트 저장소 환경 변수가 없는 새 터미널에서 한다.

## 자체 검토 결과와 실행 방법

설계의 인증/뒤로가기/회원 연결은 Task 1·2·6, IP quota는 Task 3·9, 버전 보존·비교는 Task 4·7·9, 찜은 Task 5·7·9, 작은 우측 광고는 Task 7, 문서·변경 요약은 Task 8에 연결했다. 앞선 task의 함수·필드·상태 DTO를 뒤 task가 동일한 이름으로 소비한다. 기존 데이터 보존·계정 전환·동시 요청·오류 복원은 별도 테스트에 포함했다.

실행 방식은 이 대화에서 직접 task 순서대로 구현하는 **Native**를 권한다. 기존 owner/접근 필터·새 모델·브라우저 인증 상태가 서로 맞물려 있고 한 작업자가 계약을 일관되게 적용하는 데 유리하다. 대안은 task별 구현자·검토자를 분리하는 Subagent-driven이다. 계획 검토와 실행 방식 선택 전에는 이 문서를 구현 완료로 표시하지 않는다.
