# 공통 실행 기반과 검증

[역할별 라우팅](../README.md) · [운영 설계](../../design/06-operations-migration.md) · [Kafka 설계](../../design/05-kafka-jobs.md)

| 책임 | 구현 | 확인할 사항 |
|---|---|---|
| 설정 | `config.Settings`, `load_environment` | 키를 출력/산출물에 노출하지 않고 run에 고정된 모델·차원 사용 |
| 공통 stage/응답 | `pipeline.Pipeline.stage/response` | 성공 output checksum, 먼저 저장한 receipt 재사용, 제한된 재시도 |
| 파일 보존 | `assets.py`, `Pipeline.persist_asset` | 원자적 저장·checksum·입력 원본과 API 사본 분리 |
| 외부 API | `adapters.router.ModelAdapter → GeminiAdapter/CloudflareAdapter` | 제공처 지연 생성·오류 정제·close |
| 로컬 작업 실행 | `web.workflow.Studio.launch`, `pipeline.run_lock` | 풀·대기 상한·소유권·run 단위 중복 실행 방지 |

`Pipeline.response()`는 제공처 원응답을 receipt에 먼저 기록한다. 이후 로컬 검증/저장 실패 시 같은 receipt를 읽고 성공한 과금 호출을 반복하지 않는다. `stage()`는 성공 출력 checksum을 확인하고 재사용한다. `manifest.json`의 stage/attempt/status와 `job.json`을 함께 읽는다. 로컬 lock은 외부 API 호출 중에도 유지되는 현재 구현이며, 서비스 설계의 짧은 DB 트랜잭션과 혼동하지 않는다.

직접 추천의 API 종류는 `direct_context`·`embed_text`이며 `ModelAdapter`는 두 종류를 Gemini로 보낸다. Cloudflare는 `generate` 요청에서 선택할 수 있는 생성 전용 제공처다. `--demo`는 외부 제공처를 호출하지 않는다.

저장소 루트(`develop/`)에서 설치된 환경의 Python으로 회귀 검증한다.

```powershell
python -m pytest -q -p no:cacheprovider src/interior_recommender/tests
```

설치가 안 된 환경은 [패키지 안내](../../../src/interior_recommender/README.md)의 설치 절차를 먼저 따른다. `test_entrypoints.py`는 이동 후 import 객체·기존/신규 CLI의 호환을 확인한다. 추천 정책/HTTP/재시도·카탈로그·기존 생성 회귀는 전체 패키지 테스트로 확인한다. 테스트 통과는 실제 API 품질·운영 부하·DB/Kafka/Bronze/FAISS 통합 검증을 뜻하지 않는다.
