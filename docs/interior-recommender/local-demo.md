# dev 원본과 로컬 시연 MVP 분리

2026-10-21 · `feature/#34` → `dev` PR 기준.

## 분리 배경

**MVP 구현 당시 데이터 파이프라인 구축과 서비스 연동 준비가 완료되지 않아 실제 데이터 파이프라인과 연동하지 못했다.** 시연에서는 서비스 MySQL의 상품 게시·이미지 이용 승인·임베딩 참조·활성 인덱스 연결을 대신해, 검증한 로컬 배치와 고정 Mongo·Garage·Delta 추출 자료를 파일 카탈로그로 결합했다. Delta 임베딩이 일부 존재한다는 사실과 서비스 전체의 연동 완료를 구분한다.

이 사유는 당시 로컬 시연 구성을 선택한 배경이다. 현재 파이프라인 구축·게시·연동 상태는 [서버 카탈로그](server-catalog.md)와 담당자의 최신 검증 기록으로 확인한다.

## 폴더와 PR 경계

| 경로 | 역할 | 이번 분리 |
|---|---|---|
| `src/demo_sketchon/` | dev의 기존 Django 앱·연동 경계 | 최신 `origin/dev`의 `22926f29051bd0373d19b1a3f312114532579812` 추적 파일과 동일하게 보존 |
| `src/local_demo/` | #34 시연용 Django 앱 | 시연 코드·화면·테스트를 분리하고 독립 진입점 제공 |
| `src/interior_recommender/` | 공유 추천·생성 엔진, 운영 카탈로그 어댑터 | MySQL·Garage·Delta 연결 코드 보존 |
| `src/data_pipeline/`, `src/ad_server/` | 팀 파이프라인·광고 서버 | 시연 분리를 이유로 원복하거나 교체하지 않음 |
| `scripts/Start-LocalDemo.ps1` | 시연 전용 실행 | `src/local_demo/manage.py`만 실행, 기본 포트 8004 |

원격 dev는 `git fetch origin dev` 후 확인했다. 오래된 로컬 `dev`는 복원 기준으로 사용하지 않았다. 이후 dev가 변경되면 PR 차이를 다시 확인한다. 두 앱은 각각 별도 프로세스로 실행하며 같은 이름의 Django 앱을 한 프로세스에 동시에 로딩하지 않는다.

공유 엔진의 범용 생성·순위 정책 변경은 두 앱에 영향을 줄 수 있으므로 별도 리뷰한다. 폴더 분리가 엔진 버전 고정까지 제공하는 것은 아니다. 운영 연결 경로 `catalog.server`, `catalog.server_embeddings`, `LiveGuard`와 게시/이용 승인 검사를 시연 경로로 대체하지 않는다.

## 기존 데이터 보존

기존 `data/local-demo/snapshot-v43/`와 `outputs/local-demo-v43/`를 그대로 가리킨다. 상품·벡터·이미지·manifest·기존 실행 결과를 새로 만들거나 덮어쓰지 않는다. 카탈로그는 `test_only=true`, `bronze_verified=false`를 유지하며 운영 MySQL 게시나 승인 완료로 표시하지 않는다. 구성과 과거 검증은 [시연 스냅샷](server-catalog.md#파이프라인-완료-전-로컬-시연)을 따른다.

시연 기본 경로는 SQLite `outputs/local-demo-v43/db.sqlite3`, 미디어 `media/`, Raw `raw/`, 실제 API 추천 `recommendations/`, 고정 응답 테스트 `recommendations-offline/`이다. 환경 변수가 우선하므로 테스트에 운영 DB·원본 앱 경로를 넣지 않는다. 이번 분리는 기존 DB를 수정하거나 자동 migration하지 않는다.

시연 쿠키는 `local_demo_sessionid`, `local_demo_csrftoken`이고 브라우저 저장소는 `local_demo.*`, Raw IndexedDB는 `local-demo-sketch-v1`이다. 이전 dev 쿠키·브라우저 이력과 분리하며 시연 DB의 기존 계정·프로젝트·결과 파일은 보존한다.

## 실행과 검증

[시연 README](../../src/local_demo/README.md)의 한 줄 명령을 사용한다. 기존 스크립트의 Init·Check·Web·Worker 인터페이스는 유지하며 기본 포트만 8004로 분리한다. Check는 기존 스냅샷 checksum·행 대응·Django 설정만 검사한다. Init는 시연 DB 변경 명령이며 승인한 시연 경로에서만 실행한다. Web·Worker는 같은 카탈로그·SQLite·출력을 사용해야 한다.

검증은 별도 임시 SQLite·미디어·Raw·추천 출력에서 원본 앱과 시연 앱을 각자 실행한다. 원본은 `src/demo_sketchon/manage.py`, 시연은 `src/local_demo/manage.py`의 Django check·테스트와 Node/브라우저 회귀를 사용한다. 실제 API는 호출하지 않으며 테스트 종료 시 임시 파일·프로세스를 정리한다. `git diff origin/dev -- src/demo_sketchon`에 소스 변경이 없어야 한다.

## 2026-10-21 로컬 검증 결과

- dev 원본 추적 소스는 위 기준 커밋과 Git diff가 없다. 원본 Django 159개, 시연 Django 163개, 원본 Node 30개, 시연 Node 32개가 통과했다.
- 별도 임시 서버의 실제 Edge에서 두 앱의 본문·상단·모바일 추천 접수와 회원 운영 권한 거절을 확인했다. 시연은 1440·390·320px 스티커 확대/축소·취소/복원·새로고침, 배치 영역 전달·사진 업로드·캐시 호환을 추가 확인했다.
- 기존 원본 브라우저 스크립트의 초기화 전 클릭 경쟁은 검증 하네스에서 준비 완료를 기다려 처리했다. 원본 테스트 파일을 바꾸지 않았으며 시연 테스트에는 준비 대기를 반영했다.
- 기존 스냅샷의 상품 1,421개·상품-이미지 관계 1,457개·고유 이미지 1,278개·768차원 무결성 검사와 시연 Django check가 통과했다. 상품 임베딩·생성 API는 호출하지 않았고 업무 DB는 변경하지 않았다.
- 검증용 DB·미디어·Raw·로그·서버 프로세스는 정리했다. 기존 사용자 자료와 `docs/presentations/` 미추적 문서는 이 변경에 포함하지 않는다.

## 서버 반영 순서

로컬 변경 검토 → 명시적 커밋 승인 → 커밋 → VMware 웹서버 반영 → 서버 동작 확인 순서를 따른다. 원본 앱과 시연 앱을 각각 같은 소스 경로로 배치하고 시연 웹·워커가 `src/local_demo`를 실행하도록 한다. 기존 DB·미디어·상품 스냅샷·비밀 설정을 소스 배포로 덮어쓰지 않는다. PNG 4종 등 Git 제외 필수 파일은 기존 checksum으로 대조해 전달한다. 서버 배포가 끝나기 전에는 로컬 분리를 운영 반영 완료로 기록하지 않는다.
