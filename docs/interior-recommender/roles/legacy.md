# 기존 생성 CLI와 웹

[역할별 라우팅](../README.md) · [전환 경계](../../design/08-template-generation.md) · [기존 실행 이력](../../../src/interior_recommender/README.md)

`python -m interior_recommender`와 설치 스크립트 `interior-recommender`는 `cli.main()`으로 진입한다. `embed-directory`는 새 `catalog.batch_embedding`으로, 기존 start/resume/select/embed 등 생성 명령은 `create_run()`·`Pipeline.execute()`로 연결된다. 이 CLI를 직접 추천 웹 실행 명령으로 설명하지 않는다.

`python -m interior_recommender.web --legacy-generation`은 `web.views.studio()`에서 `web.workflow.Studio`를 선택한다. `Studio._prepare/_generate/_compare`, 생성 프롬프트와 `local_processing.py`·`scale.py`는 기존 생성용이다. 기본 직접 추천은 `application.DirectStudio`의 `_work/_prepare`를 사용한다.

공통 `Pipeline.stage/response`, 설정, 파일 보존과 `Studio`의 풀/소유권/조회는 직접 추천에서도 재사용한다. 따라서 기존 생성 관련 파일을 폴더명만 보고 삭제하지 않는다. 생성물 검출·템플릿 선택·시각 비교·생성 크기 검사를 직접 추천의 필수 단계로 추가하지 않는다.

기존 경로 변경 시 `test_cli.py`, `test_pipeline.py`, `test_web_workflow.py`, `test_reference_preservation.py`, `test_scale_policy.py`, `test_annotation_prevention.py`를 확인하고 직접 추천의 기본 모드도 함께 검증한다.
