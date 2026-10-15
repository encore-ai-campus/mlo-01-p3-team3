# Team 3 Git/GitHub 협업 가이드

## 1. 목적

이번 프로젝트에서는 Git과 GitHub를 활용하여 소스코드와 문서를 관리합니다.

단순히 결과물을 제출하는 것이 아니라, 실제 개발 현업에서 사용하는 **Issue → Branch → Commit → Push → Pull Request → Code Review → Merge** 협업 절차를 팀원 모두 경험하는 것을 목표로 합니다.

Git/GitHub 사용이 익숙하지 않더라도 함께 배우면서 진행합니다.

## 2. Git과 GitHub의 차이

- **Git:** 파일의 변경 이력을 기록하고 버전을 관리하는 도구
- **GitHub:** Git 저장소를 공유하고 협업할 수 있도록 지원하는 플랫폼
- **Repository:** 프로젝트의 파일과 변경 이력을 보관하는 저장소
- **Branch:** 기존 작업과 분리하여 독립적으로 개발하는 작업 공간
- **Commit:** 변경 사항을 로컬 Git 이력으로 기록
- **Push:** 로컬의 커밋을 GitHub 원격 저장소로 전송
- **Pull:** GitHub의 변경 사항을 로컬로 가져와 통합
- **Pull Request(PR):** 작업한 변경 사항을 검토하고 병합하도록 요청
- **Merge:** 서로 다른 브랜치의 변경 내용을 통합

## 3. 프로젝트 저장소

- Organization: `encore-ai-campus`
- Repository: `mlo-01-p3-team3`
- 기본 브랜치: `main`
- 협업 방식: 기능별 브랜치 생성 후 Pull Request

## 4. GitHub 기능별 역할

| 기능 | 사용 목적 |
|---|---|
| Code | 소스코드와 프로젝트 문서 관리 |
| Issues | 업무 등록, 담당자 지정, 작업 추적 |
| Branches | 팀원별 독립 작업 |
| Pull Requests | 코드와 문서 변경 사항 검토 |
| Projects | 프로젝트 전체 진행 현황 관리 |
| Discussions | 필요 시 기술 토론 및 질의응답 |

**주의:** Issues에 파일을 첨부하는 것과 Git 저장소에 파일을 업로드하는 것은 다릅니다. 프로젝트 소스코드와 공식 문서는 Code 영역에서 Git으로 관리합니다.

## 5. 기본 협업 규칙

1. 새로운 작업은 GitHub Issue로 등록합니다.
2. Issue에는 작업 목적, 담당자, 완료 조건을 작성합니다.
3. 작업할 때는 `main`이 아닌 별도의 브랜치를 생성합니다.
4. 작업 단위별로 Commit을 작성합니다.
5. Push 후 Pull Request를 생성합니다.
6. 다른 팀원이 변경 사항을 검토합니다.
7. 검토가 완료되면 `main` 브랜치에 병합합니다.
8. 병합 후 작업 브랜치를 삭제합니다.
9. 작업 완료 시 연결된 Issue를 종료합니다.

관리자 보호 규칙이 설정되기 전까지는 팀원 모두 `main` 직접 Push 금지 규칙을 준수합니다.

## 6. 브랜치 이름 규칙

| 작업 유형 | 브랜치 예시 |
|---|---|
| 기능 개발 | `feature/12-data-collector` |
| 오류 수정 | `fix/15-api-error` |
| 문서 작업 | `docs/18-update-brd` |
| 환경 설정 | `chore/20-project-setup` |

숫자는 해당 GitHub Issue 번호입니다.

## 7. Commit 메시지 규칙

| 접두어 | 의미 |
|---|---|
| `feat:` | 신규 기능 추가 |
| `fix:` | 오류 수정 |
| `docs:` | 문서 변경 |
| `refactor:` | 기능 변경 없는 코드 개선 |
| `test:` | 테스트 코드 |
| `chore:` | 설정 및 유지보수 |

예시:

- `feat: add data collector`
- `fix: handle API timeout`
- `docs: update BRD`
- `chore: configure gitignore`

## 8. 실제 작업 예시

**작업: 데이터 수집 모듈 개발**

1. Issue 생성: `#12 데이터 수집 모듈 개발`
2. 작업 브랜치 생성: `feature/12-data-collector`
3. Python 코드 작성 및 테스트
4. Commit: `feat: implement data collector`
5. GitHub에 Push
6. Pull Request 생성
7. 다른 팀원에게 리뷰 요청
8. 리뷰 결과 반영 및 Merge
9. Issue #12 종료

PR 본문에 `Closes #12`를 작성하면 PR을 기본 브랜치에 병합할 때 해당 Issue를 자동 종료할 수 있습니다.

## 9. 파일 및 보안 관리

GitHub에 등록하지 않는 파일:

- `.env`, 비밀번호, API Key, SSH 개인키
- Python 가상환경 `.venv/`
- `__pycache__/`, `*.pyc`
- 로그 파일 `*.log`
- 불필요한 로컬 데이터베이스 파일
- 대용량 원본 데이터 및 임시 결과물

필요한 파일 제외 규칙은 `.gitignore`로 관리합니다.

대용량 데이터와 공용 원본 자료는 NAS를 활용하며, 개발 소스코드는 각 팀원 PC의 개별 Git 작업 디렉터리에서 관리합니다.

## 10. 팀 공통 원칙

- 팀원 모두 GitHub 사용 방법을 함께 학습합니다.
- 모르는 기능은 질문하고 공유합니다.
- 다른 팀원의 작업을 임의로 덮어쓰지 않습니다.
- PR에는 변경 목적과 테스트 결과를 작성합니다.
- 실수한 경우 변경 이력을 확인하고 함께 해결합니다.
- GitHub를 단순 저장소가 아닌 협업 도구로 활용합니다.

**최종 목표: 프로젝트 완성 + 실무 Git/GitHub 협업 경험 확보**
