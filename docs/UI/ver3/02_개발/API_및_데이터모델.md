# API·화면 경로 및 데이터모델

기준일 2026-10-18 · 실제 urls.py와 models.py에서 목록을 대조했다. 등록 URL 57개에는 HTML·미디어·API·같은 기능의 별칭이 함께 들어 있다. URL 개수를 페이지 수나 완성률로 사용하지 않는다.

## 1. 공통 계약

- 변경 요청은 Django CSRF 검사를 거친다. 계정/소유권은 브라우저 신고가 아닌 서버 세션으로 판단한다.
- `Project.revision`은 작업 문서, `Composition.version`은 생성 결과의 배치 버전이다. 물체·추천도 기준 버전을 확인한다.
- 생성·추천·저장은 request_id로 재전송을 구분한다. 이벤트는 event_id로 중복을 막는다.
- API별 400/401/403/404/409/413/422/429/503 등의 처리 범위는 해당 뷰가 기준이다. 모든 API가 동일 오류 상태를 반환한다고 가정하지 않는다.
- 공개된 media 디렉터리 URL로 개인 사진을 제공하지 않는다. 회원 scope 또는 운영 민감 권한이 있는 보호 경로를 사용한다.
- `/scene-generations/`는 현재 legacy mock 생성 뷰의 별칭이다. `/api/jobs/<id>/`는 추천 실행 상태 조회다. 외부 비동기 생성 작업 시스템이 구현됐다는 뜻이 아니다.

## 2. 등록된 경로

근거: [sketch/urls.py](../../../sketch/urls.py), [operations/urls.py](../../../operations/urls.py). 아래 서비스 scope/입력 검사는 ‘무인증 공개 데이터’라는 뜻이 아니다. 구체적인 본인 소유 검사와 반환은 각 뷰에서 수행한다.

| 경로 | 메서드 | 핸들러 | 접근/검사 |
| --- | --- | --- | --- |
| `/api/results/<uuid:result_id>/save/` | POST | `library.save_result` | 로그인 |
| `/api/saved-versions/<uuid:version_id>/image/` | GET | `library.saved_version_image` | 로그인 |
| `/api/saved-versions/<uuid:version_id>/edit/` | POST | `library.edit_saved_version` | 로그인 |
| `/api/wishlist/` | GET/POST | `library.wishlist_list_add` | 로그인 |
| `/api/wishlist/<uuid:entry_id>/remove/` | POST | `library.wishlist_remove` | 로그인 |
| `/my/projects/` | GET | `library.saved_projects` | 로그인 |
| `/my/projects/<uuid:project_id>/` | GET | `library.saved_project_detail` | 로그인 |
| `/my/compare/` | GET | `library.saved_compare` | 로그인 |
| `/my/wishlist/` | GET | `library.wishlist_page` | 로그인 |
| `/accounts/login/` | GET/POST | `accounts.login_page` | 서비스 scope/입력 검사 |
| `/accounts/signup/` | GET/POST | `accounts.signup_page` | 서비스 scope/입력 검사 |
| `/accounts/logout/` | POST | `accounts.logout_action` | 서비스 scope/입력 검사 |
| `/api/account/status/` | GET | `accounts.account_status` | 서비스 scope/입력 검사 |
| `/my/profile/` | GET/POST | `accounts.profile_page` | 로그인 |
| `/api/scene-objects/<uuid:object_id>/revisions/` | POST | `platform_views.revise_object` | 서비스 scope/입력 검사 |
| `/api/results/<uuid:result_id>/samples/<str:placement_id>/<str:variant>/` | GET | `views.result_sample` | 서비스 scope/입력 검사 |
| `/api/projects/<uuid:project_id>/scene-generations/` | POST | `views.search` | 서비스 scope/입력 검사 |
| `/api/scene-generations/<uuid:scene_id>/` | GET | `platform_views.scene` | 서비스 scope/입력 검사 |
| `/api/scene-objects/<uuid:object_id>/recommendations/` | POST | `platform_views.recommend` | 서비스 scope/입력 검사 |
| `/api/jobs/<uuid:job_id>/` | GET | `platform_views.job` | 서비스 scope/입력 검사 |
| `/api/recommendation-items/<uuid:item_id>/events/` | POST | `platform_views.item_event` | 서비스 scope/입력 검사 |
| `/api/recommendation-items/<uuid:item_id>/link/` | POST | `platform_views.link` | 서비스 scope/입력 검사 |
| `/` | GET | `views.home` | 서비스 scope/입력 검사 |
| `/editor/` | GET | `views.editor` | 서비스 scope/입력 검사 |
| `/editor/<uuid:project_id>/` | GET | `views.editor` | 서비스 scope/입력 검사 |
| `/results/<uuid:result_id>/` | GET | `views.result_page` | 서비스 scope/입력 검사 |
| `/api/projects/` | GET/POST | `views.projects` | 서비스 scope/입력 검사 |
| `/api/projects/<uuid:project_id>/` | GET/POST | `views.project_detail` | 서비스 scope/입력 검사 |
| `/api/projects/<uuid:project_id>/photo/` | GET | `views.photo` | 서비스 scope/입력 검사 |
| `/api/projects/<uuid:project_id>/events/` | POST | `views.collect_events` | 서비스 scope/입력 검사 |
| `/api/projects/<uuid:project_id>/samples/` | POST | `views.collect_sample` | 서비스 scope/입력 검사 |
| `/api/projects/<uuid:project_id>/search/` | POST | `views.search` | 서비스 scope/입력 검사 |
| `/api/projects/<uuid:project_id>/crop/` | POST | `views.crop_photo` | 서비스 scope/입력 검사 |
| `/api/results/<uuid:result_id>/image/` | GET | `views.result_image` | 서비스 scope/입력 검사 |
| `/api/results/<uuid:result_id>/data/` | GET | `views.result_data` | 서비스 scope/입력 검사 |
| `/api/results/<uuid:result_id>/sprites/<str:placement_id>/` | GET | `views.result_sprite` | 서비스 scope/입력 검사 |
| `/api/results/<uuid:result_id>/placements/` | POST | `views.update_placements` | 서비스 scope/입력 검사 |
| `/api/results/<uuid:result_id>/redraw/` | POST | `views.redraw` | 서비스 scope/입력 검사 |
| `/ops/` | GET | `views.dashboard` | view_console |
| `/ops/people/` | GET | `views.people` | view_people |
| `/ops/projects/` | GET | `views.projects` | view_projects |
| `/ops/projects/<uuid:project_id>/` | GET | `views.project_detail` | view_projects |
| `/ops/events/` | GET | `views.events` | view_behavior |
| `/ops/collection/` | GET | `views.collection` | view_collection |
| `/ops/collection/index/` | POST | `views.index_collection` | review_raw |
| `/ops/collection/<uuid:receipt_id>/review/` | POST | `views.review_raw` | review_raw |
| `/ops/media/projects/<uuid:project_id>/photo/` | GET | `views.project_photo` | view_sensitive |
| `/ops/media/results/<uuid:result_id>/image/` | GET | `views.result_image` | view_sensitive |
| `/ops/media/raw/<uuid:receipt_id>/` | GET | `views.raw_file` | view_sensitive |
| `/ops/ads/` | GET | `views.ads` | manage_ads |
| `/ops/ads/new/` | GET/POST | `views.campaign_edit` | manage_ads |
| `/ops/ads/<uuid:campaign_id>/edit/` | GET/POST | `views.campaign_edit` | manage_ads |
| `/ops/ads/<uuid:campaign_id>/review/` | POST | `views.campaign_review` | review_ads |
| `/ops/ads/<uuid:campaign_id>/toggle/` | POST | `views.campaign_toggle` | manage_ads |
| `/ops/ads/<uuid:campaign_id>/image/` | GET | `views.campaign_image` | manage_ads |
| `/ops/ad-reports/` | GET | `views.ad_reports` | view_ad_reports |
| `/ops/audit/` | GET | `views.audit_log` | view_ops_audit |

## 3. 업무 모델 13개

Django auth/session/contenttypes 기본 테이블은 아래 수에 포함하지 않았다. 필드는 현재 소스 기준이며 migration 적용 상태는 [검증 현황](../04_운영/검증_및_운영준비현황.md)에 있다.

| 모델 | 주요 필드(소스 전체 선언) | 근거 |
| --- | --- | --- |
| Project | `id`, `owner`, `flow_id`, `account`, `saved_project`, `title`, `photo`, `original_photo`, `original_checksum`, `request_context`, `width`, `height`, `document`, `revision`, `locked`, `updated_at` | [sketch/models.py](../../../sketch/models.py) |
| SketchEvent | `event_id`, `project`, `event_type`, `event_time`, `payload`, `actor_kind`, `actor_account`, `visitor_id`, `flow_id`, `reported_actor_kind`, `published_at` | [sketch/models.py](../../../sketch/models.py) |
| Composition | `id`, `project`, `image`, `input_document`, `placements`, `revision`, `version`, `generation_context`, `initial_placements`, `created_at` | [sketch/models.py](../../../sketch/models.py) |
| RecommendationRun | `id`, `composition`, `scene_object_id`, `input_version`, `input_hash`, `lease_token`, `lease_expires_at`, `attempt_count`, `status`, `mode`, `items`, `metadata`, `created_at` | [sketch/models.py](../../../sketch/models.py) |
| SceneObjectRevision | `id`, `composition`, `placement_id`, `base_version`, `previous_object_id`, `category_id`, `bbox`, `request_id`, `input_hash`, `created_at` | [sketch/models.py](../../../sketch/models.py) |
| GuestGenerationUsage | `client_ip`, `usage_date`, `successful_count`, `updated_at` | [sketch/models.py](../../../sketch/models.py) |
| SavedProject | `id`, `user`, `title`, `created_at`, `updated_at` | [sketch/models.py](../../../sketch/models.py) |
| SavedProjectVersion | `id`, `saved_project`, `version_number`, `composition`, `source_result_version`, `request_id`, `snapshot`, `image`, `created_at` | [sketch/models.py](../../../sketch/models.py) |
| WishlistEntry | `id`, `user`, `stable_item_key`, `item_snapshot`, `recommendation_run`, `item_id`, `created_at` | [sketch/models.py](../../../sketch/models.py) |
| GenerationAttempt | `request_id`, `project`, `input_hash`, `actor_kind`, `actor_account`, `status`, `started_at`, `finished_at`, `duration_ms`, `error_code`, `result` | [operations/models.py](../../../operations/models.py) |
| RawReceipt | `id`, `sample_id`, `project`, `checksum`, `byte_count`, `stroke_count`, `point_count`, `capture_status`, `validation_status`, `warnings`, `has_file`, `first_received_at`, `last_received_at`, `retry_count`, `review_status`, `review_reason`, `reviewer`, `reviewed_at` | [operations/models.py](../../../operations/models.py) |
| AdCampaign | `id`, `campaign_id`, `title`, `description`, `advertiser`, `creative`, `landing_url`, `category`, `slot_id`, `bid_units`, `starts_at`, `ends_at`, `active`, `review_status`, `review_reason`, `reviewed_by`, `reviewed_at`, `content_version`, `reviewed_version`, `revision`, `policy_version`, `created_at`, `updated_at` | [operations/models.py](../../../operations/models.py) |
| OperatorAudit | `actor`, `action`, `object_type`, `object_id`, `before`, `after`, `reason`, `created_at` | [operations/models.py](../../../operations/models.py) |

## 4. 관계와 중복 방지

Project는 owner(브라우저 방문 ID)와 nullable account를 가진다. 로그인 후에도 owner를 바꾸지 않고 account만 연결한다. SketchEvent·Composition·RawReceipt·GenerationAttempt는 Project에 연결된다. 생성 당시 문서/요청과 결과 배치는 Composition에 남는다.

SavedProject는 회원 개인 프로젝트이며 SavedProjectVersion이 독립 PNG와 snapshot을 가진다. 같은 결과/버전은 중복 저장하지 않고 새 결과는 같은 프로젝트의 다음 버전에 추가한다. WishlistEntry는 회원별 stable_item_key를 유일하게 유지한다.

GuestGenerationUsage는 IP/날짜 유일 제약으로 한도를 공유한다. RecommendationRun과 SceneObjectRevision은 요청 ID·입력 해시·기준 버전을 사용한다. RawReceipt는 샘플/체크섬/검증 상태 조합과 샘플 ID 없는 경우의 조건부 유일 제약을 사용한다. AdCampaign.revision은 운영자의 동시 수정을 막고 content_version/reviewed_version은 승인 무효화를 구분한다.

업무 FK는 대부분 PROTECT이며 계정/프로젝트 삭제 기능을 추가할 때 이를 자동 삭제 정책으로 오해하지 않는다. 이벤트 published_at 필드는 존재하지만 현재 export 명령은 Kafka에 보내거나 필드를 변경하지 않는다.
