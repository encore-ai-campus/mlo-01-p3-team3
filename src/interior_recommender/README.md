# scikit-learn 콘텐츠 추천과 선택 상품 배치 미리보기 MVP

2026-10-18: 추천 카드의 **이 상품을 방에 배치해 보기**로 외부 이미지 생성 API를 호출하는 선택 기능을 추가했다. 방·선택 상품·선택 스케치를 사용하며 추천/상품 벡터는 재생성하지 않는다. [설정·구동·선택/중복/복구 계약](../../docs/interior-recommender/product-preview.md)을 따른다. 아래 생성 제외 설명은 추천 선행 생성과 과거 템플릿 흐름에 관한 내용이다.

2026-10-18 새 기본 추천은 `interior-content-v3`다. [방 특징 추천·점수·입력·호환](../../docs/interior-recommender/interior-ranking.md)을 따른다. 기존 파일 상품/벡터에 방 색조·재질·형태·스타일 맥락을 연결하며 결측은 UNKNOWN이다. API 호출 종류는 기존 해석/질의 임베딩이고 MySQL 연결은 후속 작업이다. 아래 v1/v2 정책 설명은 구버전 동작이다.

2026-10-18 SketchOn 통합 실행은 웹 접수/조회와 별도 SQLite 워커로 나뉜다. 기존 `DirectStudio.prepare`/domain 규칙과 stage/receipt를 재사용하고 Gemini는 native async 호출, 이미지/파일/계산은 제한된 공유 풀에서 실행한다. [SQLite 실행·복구·후속 파이프라인 연결](../../docs/interior-recommender/execution.md)을 따른다. 아래 독립 `python -m interior_recommender.web`은 기존 제한된 스레드풀 방식이다.

[역할별 코드·문서 라우팅](../../docs/interior-recommender/README.md) · [호출 구조와 모듈 이전표](../../docs/interior-recommender/call-structure.md)

2026-10-16 역할별 코드 계층: `web/`(HTTP·연동) → `application/`(직접 추천 실행) → `domain/`(조건·순위·링크 규칙), `catalog/`(사전 상품/벡터 준비), `adapters/`(외부 모델 연결). 공통 Pipeline·설정·파일·스키마와 기존 생성 기반은 유지한다. 이전 모듈 import·CLI 명령은 호환 진입점으로 유지한다.

2026-10-15: 웹 기본 흐름은 직접 추천이다. 기존 생성 CLI·일괄 임베딩은 유지하고 생성 웹은 `--legacy-generation`으로 실행한다. 아래 접힘 영역은 이전 생성 MVP 이력이다.

## SketchOn 단일 서버 통합

`demo_sketchon`의 `/interior-recommand/`에서는 기존 로컬 295개 상품과 Gemini를 재사용한다. [설치·환경 변수·API·권한·검증](../demo_sketchon/docs/ver2/직접추천_통합_실행.md)을 따른다. 이 구성은 아래 독립 추천 웹 서버를 실행하지 않는다.

## 실행

이 문서는 저장소 루트에서 실행하는 명령을 안내한다. 패키지와 pyproject.toml은 `src/interior_recommender/`에 있으며 먼저 `python -m pip install --no-deps -e .\src\interior_recommender`로 설치한다. 테스트·데모·일괄 임베딩의 모듈 이름은 모두 `interior_recommender`다.

```powershell
.\.venv\Scripts\python.exe -m pip install -r .\src\interior_recommender\requirements-mvp.lock.txt
.\.venv\Scripts\python.exe -m pip install --no-deps -e .\src\interior_recommender
.\.venv\Scripts\python.exe -m interior_recommender.web --demo --output-root outputs/direct-demo --port 8765
```

`http://127.0.0.1:8765/`에서 방 사진+텍스트/스케치를 보내면 가상 상품 추천을 확인한다. 데모 해석·임베딩·상품은 합성 자료이며 정확도/비용/실제 API 검증이 아니다. 실제 모드는 기존 `.env`의 GEMINI_API_KEY를 사용하며 Cloudflare 키는 직접 추천에 필요하지 않다.

```powershell
.\.venv\Scripts\python.exe -m interior_recommender.web --products outputs/product-catalog-v1/products.json --output-root outputs/direct-web --port 8765
```

## 구현 흐름과 가중치

사진/텍스트/스케치→멀티모달 direct_context API→코드 스키마·원문/단위 검사·질의 조립→질의 embed_text API→필수 조건 대조·불일치 제외/결측 분리→scikit-learn cosine_similarity→수동 가중합·상품 중복 제거→직접 상품 표시다. 정상 요청은 해석 1회+질의 임베딩 1회이며 재시도/제공처 오류가 있으면 달라진다. 상품별 온라인 임베딩·이미지 생성·참조 구조/생성 후 검사는 호출하지 않는다.

초기 추천은 라벨 없이 scikit-learn `cosine_similarity`와 수동 초기 가중치로 수행한다. 기본 `weighted-content-v2`는 선호 색상이 있으면 `0.85 × 유사도 + 0.15 × 색상 일치`, 없으면 유사도만 사용한다. 재질은 원문을 질의 임베딩에 넣는 검색 참고이며 결측·불일치로 제외하거나 미확인 후보로 분리하지 않는다. 나머지 필수 조건 불일치는 제외하고 결측·미지원 필수 조건은 UNKNOWN 목록으로 분리한다. 가중치는 학습값이 아니며 점수는 확률이 아니다. 사람의 6만 이미지 전수 적합/부적합 라벨링·LogisticRegression 학습·직렬화 모델 파일은 초기 실행 조건이 아니다. 반응 자료를 충분히 확보한 뒤 지도학습을 별도 평가한다.

`--ranking-policy policy.json`으로 {"version":"weighted-content-v2","similarity_weight":0.85,"color_weight":0.15}를 지정할 수 있다. 유한한 0~1 가중치의 합이 1이어야 하며 run 접수 때 전체 값을 고정한다. 변경 정책은 새 요청에만 적용한다. `weighted-content-v1`은 과거 재질 필수 판정과 저장 run 재시도 호환용으로 지원한다. 동점은 유사도 내림차순→product_id→image_id 오름차순이다. 선호 색상 미지정 때는 유사도만 사용하며 누락 상품만 가중치를 재정규화하지 않는다.

required는 사용자 텍스트에 인용 근거가 있는 색상/재질/형태 EQ/NE, 가격·폭/깊이/높이 LE 상한을 지원한다. v2에서 재질 대조는 `preference_checks`에만 기록하며 일치·부정 재질 제외를 보장하지 않는다. 나머지 실제 원장 결측은 UNKNOWN 목록, 불일치는 제외다. 미지원 조건은 unapplied_conditions에 보존하고 충족 목록으로 안내하지 않는다. 원문 인용이 AI 해석의 정답을 보증하지 않는다. 색상/재질 값은 원장과 같은 표준 어휘를 사용해야 하며 동의어 정제는 상품 준비 범위다.

현재 검증된 실측 참조 저장소가 없어 공간 크기는 TYPICAL_OBJECT_SIZE/RELATIVE_ONLY/NONE만 받는다. 추정폭은 저장/표시만 하고 필수 제외나 초기 점수에는 사용하지 않는다. 사용자 명시 상한은 확인된 상품 규격과 비교하지만 실제 방 설치 가능성은 항상 UNKNOWN이다.

## 기존 다나와 벡터 재사용과 상품정보 연결

이미지 벡터만 있으면 `--danawa-index outputs/danawa_embeddings/index.json`으로 미확인 이미지 후보를 시험할 수 있다. 파일명으로 품목/가격/상품 대응을 추측하지 않는다. 확인 상품 추천은 수집·정제 원장과 연결한 메타데이터가 필요하며 사용자에게 별도 라벨링/품목 선택을 요구하지 않는다.

`metadata.json`의 예시(실제 원장 값으로 준비):

```json
{"products":[{"image_id":"danawa-원본이미지SHA256","product_id":"확인된상품ID","name":"확인된 상품명","category_code":"CHAIR","color":"brown","material":"wood","price_krw":90000,"width_mm":550,"metadata_source":"수집원장/관측ID","observed_at":"2026-10-15T00:00:00Z"}]}
```

```powershell
.\.venv\Scripts\python.exe -m interior_recommender.product_catalog --danawa-index outputs/danawa_embeddings/index.json --metadata metadata.json --output outputs/product-catalog-v1
```

export는 외부 API 없이 기존 벡터를 재사용해 이미지·products.json·vectors.npy를 새 디렉터리에 만든다. 같은 image_id 중복/알 수 없는 ID·동일 product_id의 상충 메타데이터·출처 누락을 거절하고 기존 디렉터리를 덮어쓰지 않는다. 원장에 없는 행은 미확인으로 남긴다. 생성된 JSON의 embedding_model/dimension/preprocessing_version/products/vectors_file/vectors_checksum과 이미지 checksum을 검증한다.

작은 자체 카탈로그는 동일 JSON에 vectors_file 대신 products와 행 순서가 같은 2차원 `vectors` 배열을 넣을 수도 있다. 모든 image 경로와 vectors_file은 manifest 디렉터리 안에 있어야 한다. 임베딩 모델/차원/전처리가 설정과 다르면 실행하지 않는다. 상품 벡터가 없는 새 이미지는 기존 일괄 임베딩 경로로 사전 준비한다.

## 저장·재시도·규모

run은 inputs/에 원본 바이트와 사진/스케치 사본만 보존하고 direct_contract에 해석 버전·가중치·인덱스를 고정해 input_hash로 확인한다. context.json/query.json·receipts에 성공 외부 응답을 보존한다. recommendations.json에는 점수/기여분·MATCH/UNKNOWN·상품 스냅샷·정책/라이브러리/인덱스·로컬 계산 시간을 기록하고 반환 이미지만 products/에 복사한다. API 응답에 내부 이미지 절대 경로는 반환하지 않는다.

로컬 계산/저장 실패의 재시도는 성공 API 결과를 재사용한다. 성공 추천도 checksum 검사 후 재사용한다. 응답 내용 자체를 다시 해석하려면 새 요청을 제출한다. 같은 event_id+입력은 같은 run, 다른 입력은 거절한다. 고정 인덱스가 없거나 바뀌면 INDEX_NOT_READY, 추천 오류는 RANKING_FAILED다. 재시작 후 이전 인덱스 복구가 없으면 새 카탈로그로 이전 run을 조용히 재계산하지 않는다.

float32 NPY mmap은 시작할 때 검증 후 재사용하며 2,048행씩 질의 하나와 비교한다. 전체 NxN 유사도 행렬과 요청별 전체 이미지/벡터 복사를 만들지 않는다. 6만×768 원시 float32 벡터는 약 176MiB이나 전체 RAM/시작 검증은 더 크다. 기존 Danawa JSON 로더/export의 메모리도 별도다. 2작업/대기 상한 8은 로컬 보호 설정이며 수백 명 처리량 보장이 아니다. 운영에서는 BLAS 내부 스레드/프로세스 수를 제한하고 실제 호스트에서 전체 API·대기 포함 p50/p95를 측정한다.

서비스 MySQL·Kafka·Delta Bronze·광고 서버 HTTP·FAISS·구매 링크 검증은 미연결이다. 실제 다나와 6만 이미지 원장 연결/운영 배포·품질/부하 검증도 미완료다. 로컬은 LOCAL_DIRECT_SAMPLE/bronze_verified=false를 유지한다. 구매 URL을 임의 생성하지 않고 연결 통합 전 구매 버튼을 제공하지 않는다.

## 검증

```powershell
.\.venv\Scripts\python.exe -m pytest -q -c .\src\pyproject.toml
.\.venv\Scripts\python.exe -m pip check
```

필터·결측·가중치·중복/동점·checksum·API 재사용·권한·생성 호출 차단을 회귀 검증한다. 테스트 통과를 실제 외부 API 응답 품질이나 서비스 부하 검증으로 해석하지 않는다.

<details>
<summary>이전 생성 MVP 실행·구현 이력 — 웹 실행에는 --legacy-generation 필요</summary>

# Gemini 해석·임베딩과 Cloudflare 이미지 생성 MVP

## 해석 이후 계산·검색·검증의 코드 처리 (2026-10-15)

새 웹 요청은 `scene-context-local-v5`, 로컬 후처리는 `semantic-results-v1`을 사용한다. 사용자 입력·선택 절차는 동일하다. AI는 의미 해석, 임베딩 생성, 참조 구조·크기 계획 및 시각 비교를 계속 담당한다. 해석 후 계산·조립·판정은 `local_processing.py`가 담당하며 모델 내재화나 임베딩 API 제거를 뜻하지 않는다.

| 작업 | 처리와 기록 |
|---|---|
| 검색 문장 | AI는 품목·색상·재질·형태와 추가 의미 조건 `search_terms`만 반환한다. 코드는 공백·중복을 정리해 `plan.json.search_text`를 조립한다. 팔걸이 없음 같은 부정 조건은 의미 구절로 보존하고 배치 좌표·주석·원문 전체를 검색어에 추가하지 않는다. |
| 후보 검색·정렬 | 검증·정규화된 벡터의 내적을 코드로 계산한다. 유사도 내림차순·ID 오름차순이며 다나와 상위 6개는 전체 정렬 대신 heap top-k로 선택한다. 속성 미제공은 `UNKNOWN`, 다른 값은 `DIFFERENT` 진단이며 점수 가중치나 필수 필터로 쓰지 않는다. |
| 좌표·프롬프트 | 원본 배치 영역의 검증·여백 좌표 변환과 고정 프롬프트 조립은 코드가 수행한다. 이 부분은 기존에도 코드였으며 이번에 공통 좌표 함수로 분리했다. |
| 비교 결과 처리 | AI가 관찰한 물체·영역·구조·주석·원근 판정을 받아 수치 크기 비교와 선택 가능 여부·대표 검토 상태를 코드로 결정한다. 누락·불확실·낮은 신뢰도를 통과로 승격하지 않는다. |

`template_candidates.json`에는 `query_text`, `local_processing_version`, `ranking`, `ranking_duration_ms`, `attribute_policy`를 기록한다. 정렬 시간은 후보 임베딩·캐시 읽기·외부 API·대기 시간을 포함하지 않는다. `generation_request.json`에도 로컬 처리 버전을 고정한다. `compare.json.local_processing`은 원래 시각 크기 판정 `visual_scale_status`, 검사 통과 여부 `gates`, 대표 `status`, 로컬 계산 `duration_ms`를 기록한다. AI 원응답은 기존 `receipts/`에 보존하며 가공 결과와 구분한다.

검색·좌표 계산·프롬프트·수치 검증·선택 판정에는 별도 AI 호출이 없다. 기존 의미 해석 호출을 삭제한 변경은 아니므로 API 호출 수·비용 감소나 수백 명 처리량, 전체 p95 15초를 보장하지 않는다. scikit-learn·로컬 모델·GPU·신규 의존성·서비스 DB/Kafka 통합은 추가하지 않았다. 다나와 품목 미검증 상태와 템플릿/상품 분리 경계는 유지한다.

업데이트 후 서버를 재시작하고 새 요청으로 준비·생성한다. 이전 버전의 새 준비·생성은 차단하고 원본·완료 출력·receipt를 재작성하지 않는다. v4의 명시적 비교 재시도는 기존 고정 크기 계획을 사용하고 새 로컬 판정 메타데이터를 기록한다. 계획이 없는 이전 결과는 크기 판단을 보류한다. CLI·일괄 임베딩의 입력/프롬프트·저장 벡터는 변경하지 않는다.

회귀 검증은 검색 구절의 부정 조건·배치 분리, top-k 동점 순서, 미제공 속성, 좌표·원응답 보존, 불확실 판정, 완료 준비 단계의 API 미호출 및 v4 비교 재시도의 생성 미호출을 포함한다. 실제 검색 품질·응답 속도는 별도 실측 대상이다.

## Canvas 연결 MVP와 참조 구조 보존 (2026-10-15)

기존 RoomPick Canvas와 연결하는 로컬 웹 API/화면은 [연결 안내](web/INTEGRATION.md)를 따른다. CLI 흐름과 별도로 `python -m interior_recommender.web`로 실행하며 서비스용 Kafka·DB·Bronze 확인은 연결하지 않는다.

```powershell
.\.venv\Scripts\python.exe -m interior_recommender.web --legacy-generation --danawa-index .\outputs\danawa_embeddings\index.json --port 8765
```

서버 재시작 후 `http://127.0.0.1:8765/`에서 사진·스케치로 **새 요청**을 제출한다. 이전 웹 실행으로 새 생성/준비를 요청하면 프롬프트 버전 차이를 안내하며 기존 결과를 보존한다. CLI와 일괄 임베딩의 기존 버전/저장 벡터는 변경하지 않는다. 파일·화면 연결만 확인하려면 `--demo --output-root .\outputs\web-demo`를 추가한다. 데모의 구조 분석·일치 판정은 고정 응답이며 품질 검증이 아니다.

웹 처리 순서는 입력 해석 → 저장 벡터 후보 검색 → 사용자 선택 → **선택 이미지의 구조 분석·공간 크기 계획** → 보존 지시를 포함한 생성 요청 미리보기 → 명시적 생성 → **원본·결과·참조의 3장 비교**다. 다나와 저장 인덱스를 사용하는 동작은 사용자 요청에 따른 로컬 MVP 시험 범위이며 서비스 문서의 템플릿/상품 분리 설계와 구분한다.

### 배치용 스케치의 생성 입력 제외와 주석 검사

현재 웹 프롬프트 버전은 `scene-context-local-v5`다. v4의 구조·주석·크기 정책을 유지하고 해석 후 코드 처리 경계를 분리했다. 업데이트 후 서버를 재시작하고 **새 입력 요청**으로 테스트한다. 기존 실행의 프롬프트·전송 이미지를 재작성하지 않으며 기존 결과와 원천을 보존한다.

방 사진과 스케치는 입력 해석 Gemini에 함께 보낸다. 이때 품목·색상·형상 조건과 배치 영역 `plan.placement_region`을 추출한다. 영역은 **원본 방 사진 기준 0~1의 `{x_min,y_min,x_max,y_max}`**이며 글자·화살표를 포함한 스케치 전체의 외곽이 아니라 가구가 놓일 영역이다. 확실하지 않으면 `null`로 두고 배치 문장을 사용한다. 잘못된 순서나 범위의 좌표는 조용히 보정하지 않고 응답 검증에서 거절한다.

생성 입력 정책:

| 해석된 스케치 역할 | 생성 API 입력 |
|---|---|
| `PLACEMENT_ONLY` | 방 사진 + 선택 참조, 스케치는 제외하고 해석한 좌표·조건·배치 문장 전달 |
| `FURNITURE_SHAPE` | 방 사진 + 상세 가구 스케치 + 선택 참조 |
| `NONE` / `UNCERTAIN` | 스케치 이미지를 제외하고 확인된 조건·배치 문장 전달 |

스케치 라벨의 `검은색 의자`는 색상·품목 조건으로 사용하고 원문 글자를 화면에 그리라는 지시로 사용하지 않는다. 원본 PNG·Canvas 문서·입력 스냅샷은 삭제하지 않는다. 미리보기에는 실제 전송할 이미지와 제외 안내가 표시된다.

배치 영역은 명시한 `request.requested_region`을 우선하고, 없으면 `plan.placement_region`을 사용한다. 원본 좌표와 생성 API의 정사각 여백 사본 좌표를 구분해 변환하며 입력 원문은 변경하지 않는다. `generation_request.json`의 `original_placement_region`, `api_requested_region`, `placement_region_source`에서 원본/변환 좌표와 출처를, `sketch_policy`에서 전송 여부를 확인한다. 프롬프트의 참조 이미지 번호도 실제 2장/3장 순서에 맞춘다.

생성 프롬프트는 자연스러운 실내 사진을 요청하고 좌표가 시각적 그래픽이 아닌 지시임을 밝힌다. 치수선·문자·네모·화살표·도식 표시를 제외하도록 지시한다. 생성 후 기존 3장 비교 호출에서 `annotation_check={status,findings,reason}`도 함께 받는다. `CLEAR`는 새 주석 없음으로 판정, `DETECTED`는 주석 발견, `UNCERTAIN`은 확인 불가다. 원본에 있던 글자, 창틀, 가구 부품이나 그림자를 새 주석으로 단정하지 않도록 비교한다.

`DETECTED` 또는 `UNCERTAIN`이면 `NEEDS_ANNOTATION_REVIEW`로 표시하고 추천 선택을 보류한다. 판정 필드가 누락되어도 `UNCERTAIN`으로 처리한다. 주석 검토가 구조 검토보다 우선하지만 화면에는 두 검사 결과가 모두 표시된다. 구조가 `MATCH`이고 주석이 `CLEAR`이며 크기가 `APPROPRIATE`일 때만 추가/교체 물체의 기본 선택을 허용한다. 결과는 그대로 보존하며 자동 재생성하지 않는다. 비교만 다시 실행하거나 같은 참조의 새 요청을 준비한 뒤 명시적으로 생성한다.

배치 스케치를 보내지 않아 주석이 복사되는 경로를 줄이는 변경이며, 참조 이미지나 상세 스케치에서 새 문자가 생길 가능성까지 완전히 차단하는 기능은 아니다. 주석 검사는 AI 판정이며 제거 보장이 아니다. 기존 Gemini 해석·비교 호출의 응답 필드를 확장하며 별도 주석 검사 API 호출을 추가하지 않는다. CLI/일괄 임베딩의 버전·입력 정책은 변경하지 않는다.

- 단순 네모·화살표·외곽선은 `PLACEMENT_ONLY`로 해석한다. 가구 형상은 명시 텍스트(`TEXT`) 또는 상세 가구 스케치(`SKETCH_DETAIL`)에 근거가 있을 때만 사용한다. 배치용 네모를 `shape=square`로 전달하지 않는다. `검은색 의자`라는 텍스트는 색상 조건이지만 검은 펜 선만으로 색상을 확정하지 않는다.
- 선택 후 Gemini가 부품·지지 방식·부품 연결·보존할 형태·불확실한 부분을 `reference.json`으로 기록한다. 가려지거나 작은 이미지에서 정확한 다리/바퀴 수를 추측하지 않으며 확인할 수 없는 수는 `null`이다. 이 프로필은 사용자가 지정한 조건과 구분한다.
- 생성 프롬프트는 실제 전송 순서에 맞춰 1부터 참조 번호를 부여한다. 방은 공간, 스케치는 배치 위치/대략적 점유 영역, 선택 참조는 가구 구조의 근거로 지정한다. 부품 연결·중앙 지지대·받침 등의 관찰된 구조를 보존하고 위치·크기·원근·조명·접촉 그림자를 공간에 맞추도록 지시한다. 미리보기의 `참조 이미지의 보존할 구조`에서 분석 내용을 확인한다.
- 구조 분석 캐시는 `reference-structures/`에 보존한다. 이미지 checksum·분석 모델·분석 프롬프트 버전·응답 스키마·데모 여부가 모두 같을 때 재사용한다. 각 실행은 `reference.json`과 단계 출력 checksum을 보존한다. 첫 선택에는 구조 분석 Gemini 호출이 추가되며 재사용 시 그 호출은 생략한다. 생성 후 비교 입력에는 참조 이미지가 추가되므로 기존 2장 비교보다 입력 토큰이 늘 수 있다.
- 비교 결과의 `reference_fidelity`는 `MATCH`, `MISMATCH`, `UNCERTAIN`, 차이 목록, 근거를 반환한다. 원근/가림만으로 구조 변경을 단정하지 않는다. 추가 지지대·받침 변경·부품 연결 차이 또는 판단 불가 시 `NEEDS_STRUCTURE_REVIEW`로 표시하고 기본 추천 선택을 보류한다. `MATCH`도 AI 판정이며 구조 일치를 보증하지 않는다.
- 구조 차이가 있어도 생성 이미지를 보존하고 자동 재생성하지 않는다. `비교 분석 재시도`는 동일 이미지로 Gemini 비교만 다시 호출한다. `같은 참조로 새 요청 준비`는 새 입력 버전/미리보기를 만들며, 그 화면의 생성 버튼을 눌러야 Cloudflare를 다시 호출한다. 참조 분석·비교 실패에서도 생성 완료 이미지는 유지한다.

회귀 테스트는 단순 네모의 형상 오인 방지, 참조 분석 캐시와 모델별 분리, 3장 비교 전달, 구조 불일치/불확실 결과의 선택 보류, 비교 재시도 중 생성 미호출을 검증한다. 실제 모델의 추가 다리 발생률이나 시각적 일치율은 별도 실험으로 평가해야 한다. 프롬프트와 AI 비교만으로 변형을 완전히 차단하지는 못한다.

샘플 입력으로 해석 → 템플릿 선택 → 공간 생성 → 물체 분석 → 선택 물체 임베딩을 실행하는 Python CLI다. 서비스용 Django·Kafka·MySQL·Object Storage·FAISS와 Bronze 확인은 아직 연결하지 않는다. 실행 manifest의 `mode=LOCAL_SAMPLE`, `bronze_verified=false`는 이 경계를 나타낸다. 서비스 통합에서는 Bronze 보존 확인 후 해석 작업을 등록해야 한다.

생성 모델 변경 기준은 2026년 10월 14일 갱신된 [AI 연결](../../docs/design/04-application-ai.md), [생성·해상도 계약](../../docs/design/08-template-generation.md), [작업 복구](../../docs/design/05-kafka-jobs.md)다. 공식 사양: [Python SDK](https://googleapis.github.io/python-genai/), [Cloudflare FLUX API](https://developers.cloudflare.com/changelog/post/2026-01-15-flux-2-klein-4b-workers-ai/), [임베딩](https://ai.google.dev/gemini-api/docs/embeddings).

## 공간 대비 크기 적정성: 과대·과소 생성 검사 (2026-10-15)

웹 MVP는 선택 참조의 구조 분석 후 `scale` 단계에서 **원본 방 사진 + 선택 참조**로 Gemini를 한 번 추가 호출한다. 주변 물체의 통상 치수 범위와 깊이 관계, 바닥/벽면 접점·원근, 대상 가구의 세부 유형을 함께 고려해 사진상 폭·높이의 하한·상한을 추정한다. 실측 엔진·전용 깊이 모델은 아니며 통상 치수는 불확실한 가정이다. 서비스 정책은 [공간 대비 크기 정책](../../docs/design/11-scale-policy.md)을 따른다.

- 명시 치수·미니어처·대형 요구를 우선한다. 스케치 크기는 대략적 배치 의도이며 실제 치수나 반드시 채울 크기가 아니다. 입력 해석의 `explicit_size_intent`에 명시 크기 요구를 보존한다.
- `scale.json`에 `basis`, `confidence`, 원본 기준 `anchors`(영역·치수 축·통상 mm 범위·깊이 관계·불확실성), 가구 크기 가정·접점·원근 근거, `width_ratio`·`height_ratio` 범위를 저장한다. `CONTEXT`는 기준 물체가 필요하고, 품목 지식만 있는 `CATEGORY_ONLY`·`UNKNOWN`은 LOW 신뢰도로 처리한다. 근거가 없는 범위는 null이다.
- `generation_request.json`에 원본 `scale_plan`과 여백 변환한 `api_scale_bounds`를 고정한다. 생성 프롬프트는 너무 크거나 작은 가구를 모두 피하도록 지시한다. 미리보기의 구조·크기 계획에서 확인할 수 있다.
- 기존 3장 비교 호출에 `scale_check`(크기 상태·원본 배경 유지 여부·발견 사항·근거)를 추가한다. 원본 카메라·배경이 유지되고 충분한 근거와 단일 추가/교체 대상 대응이 있을 때, 결과 물체의 가로·높이 영역을 고정 하한·상한과 코드로 대조한다. 수치가 범위 안이어도 AI 원근 검사가 부적정이면 통과시키지 않는다.
- `TOO_SMALL`은 과소, `TOO_LARGE`는 과대, `OUT_OF_RANGE`는 폭/높이 혼합 범위 이탈, `UNCERTAIN`은 판단 보류다. LOW 계획·계획/검사 누락·배경 변화·대상 불명도 보류한다. `APPROPRIATE`만 크기 검사 통과다. 검토 상태는 `NEEDS_SCALE_REVIEW`이며 추천 선택을 보류한다. 주석 → 구조 → 크기 순으로 대표 상태를 정하지만 화면은 모든 검사 결과를 표시한다.
- 결과를 보존하고 자동 재생성은 하지 않는다. 비교 재시도는 같은 결과·계획으로 비교만 호출한다. 같은 참조로 새 요청 준비 후 생성 버튼을 눌러야 다시 생성한다. 크기 분석 실패 시 준비 재시도로 해당 단계만 복구한다. 추정값은 상품 실제 규격이나 설치 가능 판단으로 사용하지 않는다.

새 정책은 서버 재시작 후 **새 입력 요청**부터 적용한다. 이전 실행은 보존하며, 고정 크기 계획 없는 이전 결과의 비교 재시도는 크기 판단을 보류한다. CLI/일괄 임베딩에는 이 웹 정책을 적용하지 않는다. 데모 크기 계획·판정은 고정 응답으로 실제 시각 품질 검증이 아니다. 회귀 테스트는 과대·과소·혼합 이탈·경계값, 여백 좌표, 불확실/배경 변화/대상 불명, 검사 누락, 크기 분석 복구·비교 재시도의 생성 미호출을 확인한다. 실제 사진에서의 오탐·미탐·판단 보류율은 별도 평가가 필요하다.

## 설치

프로젝트 루트에서 PowerShell로 실행한다. Python 3.11 이상이 필요하다.

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r .\src\interior_recommender\requirements-mvp.lock.txt
.\.venv\Scripts\python.exe -m pip install -e .\src\interior_recommender --no-deps
# src/interior_recommender/.env가 아직 없다면 예제 파일을 복사한다.
# Copy-Item .\src\.env.example .\src\interior_recommender\.env
```

`src/interior_recommender/.env`에 API 키와 설정을 입력한다. CLI의 신규 실행·재개 및 어댑터 직접 사용 시 `load_dotenv`가 모듈 위치 기준의 이 파일을 자동으로 읽으므로 쉘에서 환경변수를 설정할 필요가 없다. 기존 쉘 환경변수가 있으면 우선하며 상위 디렉터리의 다른 `.env`는 탐색하지 않는다. UTF-8 BOM을 지원하고 변수 치환은 하지 않는다. 모델별 운영 설정은 새 실행 생성 시 고정되므로 재개 중 `.env`의 모델·차원 설정 변경은 적용되지 않으며 API 키는 현재 값을 읽는다. API 키는 manifest에 저장하지 않는다. `.env`는 Git에서 제외한다. 실제 실행은 입력 이미지를 Google에 전송하고 API 비용이 발생할 수 있다.

이미지 생성은 `@cf/black-forest-labs/flux-2-klein-4b`를 Cloudflare REST API로 호출하고, 해석·물체 분석과 임베딩은 Gemini를 유지한다. 생성에는 별도의 계정 ID와 Workers AI 실행 권한이 있는 API 토큰이 필요하다. 생성 참조 이미지는 Cloudflare로 전송한다.

```dotenv
GEMINI_API_KEY=기존_Gemini_키
AI_IMAGE_PROVIDER=cloudflare
AI_IMAGE_MODEL=@cf/black-forest-labs/flux-2-klein-4b
AI_OUTPUT_WIDTH=512
AI_OUTPUT_HEIGHT=512
CLOUDFLARE_ACCOUNT_ID=Cloudflare_계정_ID
CLOUDFLARE_API_TOKEN=Cloudflare_API_토큰
```

1024×1024 출력은 두 출력 설정을 1024로 변경한 뒤 새 `start`로 실행한다. 기존 실행은 저장된 제공처·모델·해상도를 유지하며 Gemini 이미지 생성으로 자동 전환하지 않는다. 변경 전에 만든 Gemini 실행도 그대로 유지하므로 Cloudflare로 전환하려면 새 실행을 만든다.

## 입력과 검수된 템플릿 준비

예시 `examples/request.json`을 복사해 파일 경로와 요구사항을 수정한다. 입력 이미지 경로는 **요청 JSON이 있는 폴더 기준**이다. 이미지 없는 텍스트 입력도 지원한다. PNG/JPEG, 파일당 최대 15 MiB를 허용하며 EXIF 방향을 보정하고 RGB PNG로 저장한다. 최초 범위는 요청 가구 한 개다.

```json
{
  "room_image": "room.jpg",
  "sketch_image": null,
  "text": "나무 의자 하나를 배치해주세요.",
  "requested_region": {"x_min": 0.1, "y_min": 0.3, "x_max": 0.45, "y_max": 0.9},
  "categories": ["CHAIR", "SOFA", "TABLE"],
  "template_id": "chair-01"
}
```

배치 영역은 방향 보정한 원본 공간 사진 기준 0~1 좌표다. 텍스트 전용 입력에서는 `room_image`와 `requested_region`을 생략하거나 null로 둔다. 스케치는 원본 사진과 분리해서 전달한다.

다음 템플릿 manifest를 준비한다. 이미지 경로는 **템플릿 manifest가 있는 폴더 기준**이다. 실제 검수를 완료한 자료만 APPROVED로 등록한다. 기본 `templates/manifest.json`은 빈 목록이며 예제 이미지나 검수 완료 자료를 제공하지 않는다.

```json
{
  "templates": [
    {
      "id": "chair-01",
      "version": 1,
      "category_code": "CHAIR",
      "image": "chair-template.png",
      "review_status": "APPROVED",
      "active": true,
      "provenance": "별도로 제작하고 검수한 템플릿; 제작 출처 및 이용 조건"
    }
  ]
}
```

품목이 일치하고 검수·활성 조건을 만족하는 템플릿만 선택한다. `template_id`를 생략하면 해당 품목에 후보가 정확히 하나일 때만 자동 선택한다. 품목 불명·템플릿 복수 후보·부재에서는 생성하지 않고 선택 필요 상태를 반환한다. 선택 또는 요구사항 수정은 요청 JSON을 수정한 뒤 **새 start 실행**으로 접수한다. 기존 실행의 manifest는 수정하지 않는다. 원본 상품 이미지를 공간 생성 템플릿으로 등록하지 않는다.

## 실행

```powershell
.\.venv\Scripts\python.exe -m interior_recommender start --request .\samples\request.json --templates .\samples\templates.json
```

출력된 `run_dir` 아래 `generated.png`와 `analyze.json`을 확인한다. 분석 결과의 영역은 생성 이미지 기준이며 원본 사진의 배치 영역과 다르다. 선택할 물체의 `id`를 지정하면 크롭과 이미지 단독 임베딩을 수행한다.

```powershell
.\.venv\Scripts\python.exe -m interior_recommender embed --run-dir .\outputs\실행UUID --object-id 물체UUID
```

최초 전체 실행은 물체 선택에서 멈추며 임의 물체를 자동 확정하지 않는다. 임베딩 결과는 `embed-물체UUID.json`이다. 모델은 `gemini-embedding-2`, 차원은 기본 설정 768을 API에 명시한다. 크롭에 EXIF/RGB 처리와 최대 1024px 비율 유지 축소를 적용하고 벡터를 L2 정규화한다. 상품 벡터를 준비할 때도 동일 전처리·차원·모델을 적용해야 한다.

개별 단계는 전제 단계의 완료 결과를 재사용한다.

```powershell
# 새 실행에서 해석만 확인
.\.venv\Scripts\python.exe -m interior_recommender start --request .\samples\request.json --templates .\samples\templates.json --until interpret
# 같은 실행을 생성까지만 진행
.\.venv\Scripts\python.exe -m interior_recommender generate --run-dir .\outputs\실행UUID
# 생성 이미지로 물체 분석
.\.venv\Scripts\python.exe -m interior_recommender analyze --run-dir .\outputs\실행UUID
# 실패한 단계에서 재개하고 분석까지 진행
.\.venv\Scripts\python.exe -m interior_recommender resume --run-dir .\outputs\실행UUID --until analyze
```

## 결과와 복구

### 폴더의 원본 이미지 일괄 임베딩

`embed-directory`는 생성·물체 분석 없이 지정 폴더의 PNG/JPEG 파일을 재귀적으로 임베딩한다. `src/interior_recommender/.env`의 Gemini 키를 사용하며 Cloudflare 생성은 호출하지 않는다. 이미지마다 EXIF 방향/RGB 보정·최대 1024px 비율 유지 축소를 적용하고 기본 768차원 벡터를 L2 정규화한다. 생성 물체 크롭과 같은 임베딩 전처리 규칙이다.

```powershell
# API 호출·출력 저장 없이 실제 파일 검증
.\.venv\Scripts\python.exe -m interior_recommender embed-directory --input-dir .\data\danawa_images --output-dir .\outputs\danawa_embeddings --dry-run
# 처음 3개 파일로 확인
.\.venv\Scripts\python.exe -m interior_recommender embed-directory --input-dir .\data\danawa_images --output-dir .\outputs\danawa_embeddings --limit 3
# 전체 처리 또는 중단한 작업 재개
.\.venv\Scripts\python.exe -m interior_recommender embed-directory --input-dir .\data\danawa_images --output-dir .\outputs\danawa_embeddings
```

출력 폴더는 입력 폴더 밖에 둔다. 처음 생성할 때 파일 목록·원본 checksum·API용 이미지 사본을 고정한다. 이후 원본 폴더가 바뀌어도 같은 출력 폴더는 최초 스냅샷을 이어서 처리한다. 새 파일/변경 파일을 포함하려면 새 출력 폴더를 지정한다. 모델·차원·전처리가 달라져도 새 출력 폴더가 필요하다.

`index.json`은 원본 상대 경로 → 입력 자산 → 임베딩 파일·상태를 연결한다. 각 `embed-체크섬.json`에는 모델·차원·이미지 checksum·전처리 버전·벡터를 보존한다. 동일 바이트의 중복 파일은 같은 벡터를 참조한다. `manifest.json`은 호출 시도·오류·사용량을 기록하며 `receipts/`는 파일 저장 실패 복구에 사용한다. 완료된 결과는 재호출하지 않는다.

기본 호출 간격은 분당 10회이며 `--requests-per-minute 1~60`으로 조절한다. 이는 로컬 호출 간격 설정으로 제공처 할당량 보장이 아니다. 일시 오류의 내부 재시도는 기존 작업 정책을 사용하므로 실제 요청 수가 이 간격과 다를 수 있다. API 오류가 발생하면 추가 파일 호출을 중단하고 같은 명령으로 재개한다. 손상 파일은 `input_errors`에 기록하고 정상 파일은 처리한다. 이미지 목록 전체를 모델 입력 하나로 합치지 않고 한 이미지씩 임베딩한다. Gemini API 호출 요금과 한도가 적용된다. 벡터 생성은 FAISS 상품 검색 구축과 구분한다.

- `manifest.json`: 입력·설정·템플릿 스냅샷, 파일 checksum·자산 UUID, 단계 상태, 외부 호출 시도·모델·사용량·지연·오류 코드. 요금표를 연결하지 않았으므로 예상 비용은 null이다.
- `inputs/`: 방향 보정한 입력 이미지와 검수된 템플릿의 불변 사본.
- `interpret.json`, `generate.json`, `generated.png`, `analyze.json`: 단계별 검증 결과.
- `crops/`, `embed-물체UUID.json`: 선택 물체 이미지와 벡터·입력 자산·전처리 정보.
- `receipts/`: 외부 호출 응답. 이미지 응답도 보존하여 파일 저장 실패에서 재호출 없이 복구한다.
- `generation_refs/`, `manifest.generation_input_refs`: 생성 API용 최대 480px의 정사각 여백 사본, 원본·사본 자산/체크섬·크기·참조 순서·scale·offset. 원본을 자르거나 늘리지 않으며 배치 좌표는 사본 기준으로 변환한다. 원천 `requested_region`은 유지한다.

Cloudflare 생성은 multipart의 `prompt`, `width`, `height`, `input_image_0`~`input_image_3`를 사용한다. 출력 Base64·실제 PNG/JPEG 형식·요청 해상도 일치를 검사한다. 불일치 출력을 리사이즈해 성공으로 처리하지 않는다. 참조 입력은 최대 4장이며 초과 시 조용히 누락하지 않는다. 생성 호출 이력에는 해상도·참조 크기·예상 Neurons·요금 기준일을 기록하고 미확인 USD 비용은 null을 유지한다.

SDK 자동 재시도는 1회 시도로 설정하고 runner가 429·일시적 서버 오류·네트워크 오류만 재시도한다. 한 명령 실행의 각 단계에서 최초 호출 포함 최대 3회다. 사용자가 다시 resume하면 추가 시도를 허용하며 모든 시도 이력을 유지한다. Retry-After를 우선하고 30초보다 긴 대기는 시각을 기록한 뒤 종료한다. 시각이 지난 후 resume한다. 사용자 명령 재개까지 포함한 예산 상한은 아직 구현하지 않았다.

Cloudflare 오류 코드 `3036`은 일일 무료 할당량 소진으로 구분하여 `BUDGET_WAIT`로 저장하고 짧은 재시도를 하지 않는다. UTC 다음 00:00(한국 09:00) 이후 같은 실행을 resume한다. 다른 모델이나 유료 플랜으로 자동 전환하지 않는다. 로컬 MVP는 계정의 다른 Workers AI 사용량을 조회하거나 여러 서버의 예약을 조정하지 않으며, 계정 전체 예산 예약·실측 대조는 서비스 통합 시 연결한다. 계산상 Neurons는 측정 사용량이나 실제 청구액이 아니다.

JSON·품목·좌표·차원이 잘못된 응답은 자동 재호출하지 않는다. 기본 resume은 저장된 응답으로 로컬 검증·저장을 재시도한다. 외부 응답 자체가 잘못됐다면 아래처럼 명시적으로 새 호출을 요청한다. 실패 단계의 기존 응답은 이력으로 남는다.

```powershell
.\.venv\Scripts\python.exe -m interior_recommender analyze --run-dir .\outputs\실행UUID --retry-rejected
```

입력·설정·완료 출력의 checksum 불일치는 중단한다. 동일 실행을 동시 실행하면 `.lock`으로 막는다. 프로세스 강제 종료 후에는 다른 runner가 없는 것을 확인하고 해당 실행의 `.lock` 파일만 제거한 뒤 재개한다. 프로세스가 API 응답을 받은 직후 receipt 저장 전에 중단되면 재호출 비용이 발생할 수 있다. 로컬 MVP는 외부 API 비용의 exactly-once를 보장하지 않는다.

새 start는 동일 내용이어도 새 UUID 실행과 호출을 생성한다. 동일 실행의 완료 임베딩은 재사용하지만 실행 간 벡터 캐시·사용자 event_id 중복 제거는 서비스 통합 단계에서 구현한다. 결과 보관·삭제는 로컬 실행자 책임이며 `outputs/`, 비밀 설정, 가상환경은 Git에서 제외한다.

## 검증과 다음 연결

```powershell
.\.venv\Scripts\python.exe -m pytest -q -c .\src\pyproject.toml
```

모의 응답으로 SDK 요청 계약·완료 단계 재사용·생성 저장 실패 복구·물체 분석 실패 재개·선택 대기·좌표/벡터 검증·재시도/오류 기록을 확인한다. 이 테스트는 실제 모델 접근 권한이나 생성 품질 검증을 대신하지 않는다.

검증 환경은 Python 3.14.6, google-genai 2.30.1이다. 실제 Google API 호출은 키 설정 후 별도로 검증해야 한다. 이후 `GeminiAdapter`와 services 계약을 재사용하면서 파일 저장을 Object Storage로, 실행 이력을 MySQL로 옮기고 Django 관리 명령·Bronze 확인·Outbox/Kafka·광고 검색을 연결한다.

Cloudflare는 HTTP 모의 응답으로 multipart·인증 전달·입출력 제한·해상도 검사·무료 한도 대기를 검증한다. 실제 계정 권한과 생성 결과는 Cloudflare 자격 증명 설정 후 확인해야 한다. `ModelAdapter`가 작업에 따라 제공처를 선택하며 필요한 제공처만 초기화한다.

</details>
