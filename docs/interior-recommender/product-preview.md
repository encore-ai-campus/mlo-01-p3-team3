# 추천 상품 배치 미리보기

2026-10-19 크기 초과 보완: 신규 정책은 `product-bounded-placement-v4`이다. 좌표가 정상 전달된 v3 실행에서도 생성 모델이 방 구도를 바꾸고 상품을 확대하는 사례가 있어, 정사각 방 참조 이미지에 자홍색 최대 배치 사각형을 그린 시각 가이드를 마지막 참조로 추가한다(스케치 포함 최대 4장). 상품의 머리부터 다리까지 전체가 사각형 안에 들어가도록 가로·세로를 각각 상한으로 명시하고, 원래 카메라·방 프레이밍·흰 여백을 유지하도록 최우선 지시한다. 가이드 선은 최종 이미지에서 제거하도록 지시한다. 별도 AI 호출은 추가하지 않으며 코드로 생성물의 실제 물체 경계를 검증하거나 크기 일치를 보장하는 방식은 아니다. v1/v2/v3 실행은 기존 프롬프트·참조·receipt로 복구하고, 기존 결과는 유지한다. 새 정책으로 보려면 상품 배치 버튼을 다시 눌러 새 미리보기를 접수해야 한다.

2026-10-19 배치 영역 보완: 신규 정책은 `product-geometry-placement-v3`이다. 편집기는 텍스트 주석을 제외하고 지우개·투명도를 반영한 최대 512px 미리 렌더의 보이는 영역을 구해 `requested_region`으로 보낸다. 사용자 영역이 있으면 AI 추정 영역보다 우선한다. 생성 요청에는 원본 사진 기준 영역, 정사각 참조 여백을 보정한 영역·픽셀 좌표·중심·하단 기준점·가로/세로 비율을 전달하며 Cloudflare의 고정 출력 크기에는 출력 픽셀 좌표도 넣는다. 해당 영역 안에서 상품을 최대한 크게 균일 배율로 맞추고 중심·하단 위치를 유지하도록 지시한다. 실제 mm 실측이나 생성 결과의 픽셀 단위 일치 보장은 아니다. 글자만 있거나 모두 지운 스케치는 코드 영역을 강제하지 않는다. 기존 v1/v2 미리보기는 원래 정책으로 읽기·복구하며 이전 추천에 새 스케치 영역을 소급해 넣지 않는다.

2026-10-19 로컬 시연 보완: 신규 미리보기는 `preserve-product-geometry-v2` 프롬프트 정책을 고정한다. 상품 참조 이미지의 실루엣·비율·부품 구성·재질·색상을 공통 보존 기준으로 삼고 의자의 등받이/좌판/팔걸이/하부 구조, 책상·테이블의 상판/다리/서랍, 침대의 헤드보드/프레임/지지 구조를 구체적으로 지시한다. 스케치는 위치 참고이며 상품 모양의 기준이 아니다. 배치 영역에 맞추려고 상품을 늘이거나 부품을 바꾸지 않고 균일 축소·위치 조정을 우선한다. 프롬프트가 실제 생성물의 동일성을 보장하지는 않는다.

프롬프트 정책은 신규 실행의 식별 hash와 manifest, 웹·워커 설정 계약에 포함된다. 이전 완료 이미지·receipt를 덮어쓰지 않으며 기존 v1 실행은 기존 프롬프트로 읽기·복구한다. 정책 변경 후 같은 상품을 선택하면 새 정책의 별도 실행을 접수하고 이후 같은 입력의 반복 선택은 그 실행을 재사용한다. 변경만으로 기존 결과를 재생성하거나 API를 호출하지 않는다. 미리보기 표시 위치·버튼·진행 표시 방식은 유지하고, 두 추천 화면에서 수집 규격 원문 펼침 표시만 제거했다. 원천 속성 데이터와 정제된 규격 표시는 유지한다.

2026-10-19 서버 카탈로그 연결: MySQL에서 준비한 상품은 `server_binding`으로 현재 상품·이미지·인덱스를 재검증하고, 선택/생성 준비 때 별도 `GENERATION_REFERENCE` 이용 범위를 요구한다. 추천 노출 승인으로 외부 생성 이용을 대신하지 않는다. 기존 파일/데모 상품 동작은 유지하며 [서버 연결 경계](server-catalog.md)를 따른다.

2026-10-18. 직접 추천 후 사용자가 선택한 상품 한 개를 방에 배치하는 외부 API 기능을 추가한다. 계약은 `product-preview-v1`이다. 추천 점수·상품 링크·상품 임베딩은 기존 결과를 사용하며 생성 결과로 상품을 다시 검색하지 않는다. 서버 GPU나 로컬 생성 모델은 필요하지 않다. 실제 생성 품질·지연·계정 호출 가능 여부는 API 검증 전이다.

## 흐름과 책임

```mermaid
flowchart TD
    Input[사진·텍스트·스케치] --> Interpret[멀티모달 API: 공간·품목·조건 해석]
    Interpret --> Embed[임베딩 API: 질의 벡터]
    Embed --> Rank[scikit-learn + 코드: 유사도·조건·방 특징 점수]
    Rank --> Cards[실제 상품 추천·원래 상품 링크]
    Cards --> Select[사용자: 상품 카드에서 방에 배치해 보기]
    Select --> Freeze[코드: 권한·추천 포함 여부·checksum 검사, 참조 고정]
    Freeze --> Queue[기존 SQLite 큐·별도 워커]
    Queue --> Generate[Cloudflare 또는 Gemini 이미지 생성 API]
    Generate --> Receipt[코드: receipt 보존·이미지 디코딩·크기/checksum 검사]
    Receipt --> Preview[AI 배치 미리보기 표시]
```

`application/preview.py`가 선택·스냅샷·프롬프트·출력 검증을 담당한다. 웹은 임의 URL을 가져오지 않고 완료 추천의 `image_id`만 받는다. 저장 결과와 등록 이미지의 checksum을 확인한 뒤 방 사진·상품 이미지·선택 스케치를 별도 run의 `inputs/`에 고정한다. 부모 추천과 링크는 보존한다. 독립 웹의 미확인 후보를 선택하더라도 조건 충족으로 승격하지 않는다. SketchOn은 기존처럼 확인 상품만 표시한다.

참조 순서는 방·상품·선택 스케치다. 비율 유지·최대 480px·흰 정사각 여백으로 변환하고 원본 배치 좌표를 여백 기준으로 수치 변환한다. 선택 상품 한 개의 외형을 유지하고 상품 사진의 배경·다른 가구나 스케치 선/글자를 출력하지 않도록 지시한다. 추가 해석/임베딩/생성물 분석 API는 없다. 프롬프트 지시가 상품 동일성·방 구조·실제 크기 보장을 뜻하지 않으며 화면에 AI 시안임을 명시한다.

## HTTP·저장·복구

SketchOn 접두사는 `/interior-recommand/`, 독립 웹은 서버 루트다.

| 요청 | 계약 |
|---|---|
| `POST api/runs/{parent}/previews` | JSON `{"image_id":"추천 결과 ID"}`. 소유권·CSRF 검사 후 자식 `run_id`/job/state_path와 202 반환 |
| `GET api/runs/{preview}` | `flow=product-preview`, 부모 ID·선택 상품·상태·주의 문구·완료 시 result_image |
| `POST api/runs/{preview}/actions/prepare` | JSON `{"retry":true}`. 실패 작업의 고정 입력과 성공 receipt를 유지해 재시도 |
| `GET assets/{preview}/generated.png` | 소유권·등록 이미지 검사 후 반환 |

SketchOn은 기존 `RecommendationJob` 테이블에 자식 작업을 저장한다. 새 migration이나 실제 MySQL 변경은 없다. 워커는 `preview_contract`에 따라 `preview.validate/prepare`를 실행하고 기존 lease·heartbeat·시도 상한·receipt 복구를 따른다. 독립 웹은 기존 제한된 스레드풀에서 같은 application 함수를 실행한다. 추천 접수/조회는 생성을 호출하지 않는다.

동일 부모·상품·입력 checksum·모델/설정·계약 버전은 같은 미리보기로 연결한다. 버튼을 다시 누르면 기존 작업/완료 결과를 반환한다. 제공처 응답을 검증/최종 저장 전에 `receipts/preview.json`에 보존하고 로컬 저장 실패 시 재사용한다. 응답 유실·receipt 기록 전 종료 시 중복 과금 가능성은 남는다. 완료 결과의 의도적 재생성 버튼은 이번 범위에 없다.

## 설정과 구동

`src/interior_recommender/.env`의 기존 설정을 사용한다. 기본은 `AI_IMAGE_PROVIDER=cloudflare`, `AI_IMAGE_MODEL=@cf/black-forest-labs/flux-2-klein-4b`, `AI_OUTPUT_WIDTH=512`, `AI_OUTPUT_HEIGHT=512`다. `CLOUDFLARE_ACCOUNT_ID`와 `CLOUDFLARE_API_TOKEN`을 설정한다. 키는 브라우저/manifest에 기록하지 않는다. Gemini 전환 시 `AI_IMAGE_PROVIDER=gemini`, 계정에서 가능한 이미지 모델 ID, `GEMINI_API_KEY`를 설정한다. 자동 제공처 전환은 없다. [Cloudflare 공식 multipart·참조 이미지 계약](https://developers.cloudflare.com/workers-ai/models/flux-2-klein-4b/).

SketchOn 기본 생성 제한은 `RECOMMENDATION_GENERATION_CONCURRENCY=1`, `RECOMMENDATION_GENERATION_RPM=2`다. 계정/제공처/모델별 gate를 쓰며 전체 접수/실행 상한은 추천과 공유한다. 실제 계정 할당량을 확인한 보장값이 아니다. Cloudflare 3036 오류는 다음 UTC 자정까지 RETRY_WAIT로 보존한다. 웹·워커를 같은 설정으로 재시작한다. 성공 추천·상품 벡터의 재생성은 필요 없다.

저장소 `develop`에서 기존 환경으로 각각 별도 터미널에서 실행한다. 각 명령은 한 줄이다.

```powershell
& '..\.venv\Scripts\python.exe' '.\src\demo_sketchon\manage.py' run_recommendation_worker
```

```powershell
& '..\.venv\Scripts\python.exe' '.\src\demo_sketchon\manage.py' runserver 127.0.0.1:8003
```

기존 추천 화면에서 **이 상품을 방에 배치해 보기**를 누른다. 이때 API 비용이 발생할 수 있다. `INTERIOR_RECOMMENDER_DEMO=1`은 비과금 데모 이미지를 만들고 표시한다. 기존 공간 만들기 크레딧/결과 모델과 별도로 처리한다. 운영 과금 정책·MySQL/Kafka/Bronze 이관은 후속 작업이다.

## 검증

2026-10-18 실행 결과: 엔진 pytest 221개, Django 추천/실행 31개와 신규 제공처 gate 1개, Node 30개 통과. Django check와 수정 Markdown 19개의 로컬 링크도 확인했다. Playwright 미설치로 브라우저 시각 점검은 수행하지 않았고 실제 유료 생성 API·운영 DB 변경은 하지 않았다.

권한·CSRF·추천 포함 여부·checksum·자동 생성 없음·중복 클릭·선택 상품 대응·생성 실패 시 추천 보존·실패 재시도·receipt 이후 저장 실패 복구를 비과금 응답으로 검사한다. 제공처 async 전송/gate도 가상 통신으로 검사한다. 실제 생성 품질·지연·부하·시각적 동일성·운영 배포 완료를 의미하지 않는다.
