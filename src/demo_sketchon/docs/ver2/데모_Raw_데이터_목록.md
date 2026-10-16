# 스케치·온 SketchOn — 데모 Raw 데이터 목록 ver2

기준일: 2026-10-16 · 프로젝트: `C:\mlo-01-p3-team3\src\demo_sketchon`

이 문서는 **무엇을 수집하고 어디에 저장하는지**를 정리한다. 화면을 이용하는 순서는 [Flow](데모_Flow_이용및처리흐름.md), 기록을 관찰하는 기준은 [로그](데모_로그_종류와확인.md), 세부 Raw 검증 계약은 [수집기준](행동데이터_수집기준.md)을 함께 본다.

## 1. Raw라는 말의 범위

현재 코드에서 좁은 의미의 Raw는 `data/sketch_raw/<sample_id>.json`에 저장하는 **펜·지우개 좌표와 편집 과정 원본**이다. 분석 원천 전체를 말할 때는 업로드 사진 원본, 요청 설명, 행동 이벤트와 추천 반응도 필요하므로 아래에서는 이들을 함께 목록화한다. 모든 항목이 불변 Raw 파일이거나 Bronze 테이블로 구현된 것은 아니다.

**원본·파생 이미지·현재 상태·발생 이력·모의 결과는 다른 데이터다.** 결과 PNG가 있다는 이유로 스케치 Raw·실제 AI 판정·학습 정답이 있다는 뜻이 되지 않는다.

## 2. 전체 데이터 목록

| 번호 | 종류 | 실제 저장 위치 | 생성 시점·주요 내용 | 성격 |
| --- | --- | --- | --- | --- |
| R01 | 업로드 사진 원본 | `media/originals/<project_id>.bin`, Project.original_photo/original_checksum | 신규 사진 업로드의 파일 바이트와 SHA-256 | 입력 원본; 확장자가 .bin이어도 내용은 업로드 파일 |
| R02 | 편집용 공간 사진 | `media/rooms/*.jpg`, Project.photo/width/height | EXIF 회전·흰 배경·최대 변 2,048px JPEG, 또는 해당 사진의 crop | 파생 이미지; 원본 화소·메타데이터와 동일하지 않음 |
| R03 | 작업 문서 | Project.document | 서버에 저장된 strokes/items, 선택 요소와 부분 지우기·그룹 관계 | 갱신 가능한 작업 상태 |
| R04 | 요청 설명·스타일 | Project.request_context | text_requirements, style_preferences | 작업 입력; 그림판 안 텍스트와 별도 |
| R05 | 스케치 과정 Raw | `data/sketch_raw/<sample_id>.json` | 펜·지우개 점별 시간·입력 상태·편집 이력·시작/최종 문서 | sample_id 기준 불변 JSON |
| R06 | 일반 행동·서버 완료 이벤트 | SketchEvent | 클릭·도구·획·편집·방문·업로드·생성·배치 저장 | 사건 기록; 상세 원본의 대체물이 아님 |
| R07 | 생성 입력 스냅샷 | Composition.input_document/generation_context/revision | 생성 당시 문서·별도 설명·스타일·작업 버전 | 결과에 귀속된 입력 스냅샷 |
| R08 | 생성 결과와 배치 | Composition.image/initial_placements/placements/version | 최초 배치·현재 배치·모의 품목·좌표·합성 PNG | 파생 결과; 현재 배치는 갱신됨 |
| R09 | 추천 요청·결과 | RecommendationRun | request ID·입력 해시·물체·버전·임대·상태·items·metadata | 모의 상품 추천 실행 기록 |
| R10 | 품목 검색 조건 수정 | SceneObjectRevision | 이전 물체 ID·품목·bbox·기준 버전·요청 ID·해시 | 명시적 수정 이력; 가구의 정답 라벨이 아님 |
| R11 | 상품 반응 | SketchEvent의 PRODUCT_* | 노출·선택·비선호, 추천 실행/항목/노출 ID | 추천 반응 사건; 현재 product_id는 모의이므로 null |
| R12 | 익명 방문 연결 | Django 세션 + Project.owner | 쿠키와 서버 세션의 sketch_owner | 연결용 식별자; 계정·실명·기기 간 동일인 식별 아님 |
| R13 | 브라우저 임시 보관 | localStorage/sessionStorage/IndexedDB | 전송 대기·거절·원본 초안·편집 이력 | 서버 저장 전 상태도 포함; 브라우저별로 분리 |
| R14 | 서버 실행·오류 기록 | 실행 터미널, 파일 리다이렉션 시 data/runtime/server.*.log | HTTP 메서드·경로·상태·오류 traceback | 운영 진단 로그; 학습용 좌표 원본 아님 |

`/api/results/<id>/data/`의 JSON은 Composition·Project 정보를 조합한 **조회 응답**이다. 별도 작업 JSON 파일을 media 폴더에 계속 저장하는 구조가 아니다. 추천 상품의 samples 이미지도 요청 시 예시를 렌더링하는 응답이며 실제 판매 상품의 원본 사진 수집을 뜻하지 않는다.

## 3. 사진 원본과 파생 사진

업로드 API는 파일 크기 10MiB와 2,500만 픽셀을 검사하고 JPEG/MPO/PNG/WebP 내용을 확인한다. MPO는 첫 이미지를 사용한다. 원본은 업로드 바이트 그대로 `.bin`에 보관하고 체크섬을 Project에 기록한다. 화면에는 EXIF 방향 적용 후 최대 변 2,048px JPEG를 사용한다.

자르기는 **편집용 사진**을 잘라 새 Project를 만들며 원본 파일 참조와 체크섬은 이어받는다. 재시작에서도 같은 사진 참조를 재사용할 수 있다. 따라서 원본 참조가 있는 Project 수와 원본 파일 수는 같지 않을 수 있다. 예시 공간 체험도 파일 업로드 경로를 거치므로 원본 보관 개수에는 체험·검증용 데이터가 섞일 수 있다.

원본 보관 기능을 넣기 전에 업로드했던 사진의 과거 원본 바이트는 복원하지 않았다. 기존 JPEG에서 원본 EXIF나 원래 해상도를 역으로 재구성할 수 있다고 설명하지 않는다. 원본 파일은 현재 UI에 직접 내려받는 버튼을 제공하지 않는다.

## 4. 작업 문서와 설명 데이터

| 구성 | 주요 필드 | 해석 |
| --- | --- | --- |
| 문서 strokes[] | id, tool, color, width, pointer_type, points | 표시용 펜/지우개 경로 |
| 표시 점 | x, y, pressure | 0~1 사진 좌표; Raw 점의 t·출처와 다름 |
| 삽입 요소 items[] | id, kind, x, y, width, height, color | text/rectangle/ellipse/line/arrow/fill |
| 텍스트 | text, font_size | 사진 위에 실제로 표시한 글자 |
| 도형 | stroke_width, flip_x, flip_y | 선 두께와 방향 |
| 채우기 | parent_id, mask_width, mask_height, runs | 연결 윤곽과 픽셀 구간 마스크 |
| 연결·부분 지우기 | group_id, cuts | 그룹 관계, 요소에 붙는 지우개 경로 |
| 요청 입력 | text_requirements, style_preferences | 별도 설명 최대 1,000자, 스타일 최대 1개 |

문서는 최대 1,000획·100개 삽입 요소, 전체 점·부분 지우기 점 100,000점 등의 검증을 받는다. 사진 위 텍스트는 최대 200자이고, 별도 설명과 수집 위치가 다르다. 상세 제한은 `validation.py`가 기준이다.

편집 중 상태는 메모리·같은 탭 sessionStorage에 있을 수 있다. Project.document를 읽었다고 브라우저의 미제출 편집까지 모두 저장됐다고 단정하지 않는다. 생성 성공 시 Composition에는 확정 입력 문서와 요청 입력을 함께 저장한다. 실행 취소·복사·그룹에 대한 전체 과정을 확인하려면 최종 상태만 보지 말고 Raw operations도 확인한다.

## 5. 스케치 Raw JSON에 들어 있는 것

### 샘플 단위 필드

| 필드 | 저장 의미 |
| --- | --- |
| schema_version | `roompick-sketch-1.0.0`; 문서 ver2·서비스 표시명 변경과 무관하게 유지 |
| sample_id | 편집 수집 구간 UUID; 재전송 시 동일 ID·내용 유지 |
| project_id / writer_id | 해당 작업 UUID / Project.owner와 일치하는 익명 방문 ID |
| created_at | 시간대가 있는 샘플 생성 시각 |
| label_id / label_status | `null` / `unverified` |
| quality_status | 원본에 보관하는 `pending` |
| coordinate_system | `normalized_xy_top_left_down` |
| time_origin | `first_pointerdown_ms` |
| canvas_width / canvas_height / device_pixel_ratio | 수집 기준 CSS 캔버스 크기와 DPR |
| project_width / project_height | 편집용 사진 크기; 업로드 원본 화소 크기가 아님 |
| strokes | 삭제·취소된 경로도 보존하는 펜·지우개 원본 경로 |
| operations | 편집·도구·버튼 동작의 종류·대상·변경분·기존 이벤트 연결 |
| initial_document / final_document | 구간 시작·마지막 기록 시점의 표시용 문서 |
| final_active_stroke_ids | 최종 문서에 남은 펜 획 ID; 보이는 픽셀 전체와 동의어 아님 |
| duration_ms / undo_count | 마지막 기록까지 상대 시간 / Raw 이력의 undo 횟수 |
| submission_reason / result_id | 구간 종료 이유 / 연결된 생성 결과 UUID 또는 null |
| capture_status | `complete` / 한도 종료인 `limited` |

### 획과 점 필드

획에는 stroke_id, stroke_order, tool, ink_hex, stroke_width_css_px, canvas_width/height, device_pixel_ratio, pointer_type, end_reason, points가 있다. 도구는 pen/eraser이고 입력 장치는 mouse/touch/pen/unknown이다.

점의 필수 필드는 **x, y, t, event_type, source, in_bounds**다. 제공되는 경우 pressure, tilt_x, tilt_y를 포함하고 획 중 캔버스 크기가 달라지면 canvas_width/height를 추가한다. event_type은 down/move/up/cancel, source는 native/coalesced/synthetic이다. Raw x/y는 화면 밖 값도 보존하며 표시용 좌표처럼 0~1로 잘라 넣지 않는다. 필압 0도 원본 값으로 보관한다.

실제 장치가 제공하지 않은 기울기·종료 위치를 관측된 실제 값으로 만들어 넣지 않는다. 마지막 위치를 합성해 종료할 때는 synthetic 표시를 남긴다. point.t는 시간 원점 이후 밀리초이며 서버 수신 시각과 같지 않다.

### 편집 작업 필드

Operation은 op_id, type, t, client_time, target_stroke_ids, target_item_ids를 가지며 필요하면 document_patch, event_id, source_stroke_id를 가진다. patch는 strokes/items별 upsert·remove·order로 표현한다. 서버가 initial_document에 이를 순서대로 재생해 final_document와 일치하는지 확인한다.

펜·지우개의 전체 경로는 strokes에 있고, 도형 삽입·텍스트 편집·요소 이동은 최종 상태 변경분 중심이다. **모든 드래그 동작의 연속 경로나 키 입력 한 글자마다의 이력을 수집하는 것은 아니다.** 별도 설명·스타일은 이벤트 payload와 request_context/generation_context로 확인한다. Raw 최상위에 독립 설명 전문 필드가 있는 것으로 해석하지 않는다.

## 6. 원본이 제출되는 시점

| 상황 | submission_reason | 주의점 |
| --- | --- | --- |
| 공간 생성 성공 | search | result_id 연결; 생성 실패만으로는 이 성공 경로 제출이 일어나지 않음 |
| 사진 교체·자르기·프로젝트 전환 | project_changed | 이전 구간을 종료하고 새 작업 구간 시작 |
| 페이지 이동·새로고침·종료 | page_exit | 보관·전송 시도; 서버 도착 보장 아님 |
| 종료된 작성자의 초안 복구 | recovered | localhost/HTTPS의 Web Locks 조건 적용 |
| 수집 한도 도달 | capture_limit | 이후 편집 전체를 포함한 최종 검색 문서와 다를 수 있음 |

하나의 Project에 여러 Raw 구간이 존재할 수 있고, 한 구간에 여러 획·이벤트가 담긴다. 기록한 획·operation이 전혀 없으면 파일이 생기지 않을 수 있다. 그림 없이도 도구·버튼·설명 변경 등의 operation 때문에 Raw가 만들어질 수 있다.

서버 한도는 4MiB·1,000 Raw 획·100,000 Raw 점·1,000 operation이다. 브라우저는 약 3MiB·900획·90,000점·950 operation에서 수집을 먼저 종료한다. 원본 파일 pending은 실패를 뜻하지 않는다. API의 pass/review는 구조·기본 품질 경고 판정이며 원본을 덮어쓰지 않고, 잘못된 구조는 quarantine 응답으로 거절해 서버 파일을 남기지 않는다.

## 7. 생성·추천 기록은 무엇이 다른가

Composition에는 생성 당시 input_document와 generation_context, 최초 initial_placements와 갱신 가능한 placements/version이 있다. 배치를 바꿔도 입력 스냅샷은 원래 입력이다. 현재 합성 PNG는 배치 저장 때 갱신되며 모든 중간 PNG를 학습 원본처럼 보관하는 구조가 아니다.

추천 요청은 RecommendationRun.id를 request_id로 사용한다. composition·scene_object_id·input_version·input_hash로 요청 대상을 고정하고 lease_token/lease_expires_at·attempt_count·status로 실행·재시도를 관리한다. items에는 추천 항목 ID·표시명·브랜드·이미지 URL·가격·연결 가능 여부 등이 있고 metadata에는 안내·오류·index_version_id 등이 들어간다. 모의 추천의 실제 product_id·price·index_version_id는 보통 null이며 실판매 가격이나 FAISS 인덱스 증거가 아니다.

품목 변경은 SceneObjectRevision에 별도 남는다. 배치 크기(%)를 바꾸는 것은 실제 Composition/PNG 변경이고, 검색 품목 변경은 생성 이미지를 다른 가구로 재생성하는 동작이 아니다.

## 8. ID로 연결할 때

| ID | 연결과 주의 |
| --- | --- |
| visitor / Project.owner / Raw.writer_id | 같은 익명 방문자를 연결; 화면에는 앞 8자리만 보일 수 있음 |
| project_id | 사진·편집·일반 이벤트·Raw가 귀속되는 작업 |
| sample_id | Raw 수집 구간; Project·획 ID와 다름 |
| event_id | 개별 행동 사건; Raw operation.event_id와 연결 가능 |
| stroke_id / item id | Raw 경로와 표시용 요소 식별자; source_stroke_id·대상 ID로 연결 |
| result_id / scene_generation_id | Composition UUID; 현재 데모 DTO에서 같은 결과를 가리킴 |
| placement_id | 결과 안 가구 배치 식별자; 다른 결과에도 같은 문자열이 생길 수 있어 result_id와 함께 사용 |
| scene_object_id | 현재 버전·품목 수정에 따른 물체 UUID; 영구 고정 ID가 아님 |
| recommendation_run_id / request_id | 추천 실행 UUID; 같은 요청 재시도와 다른 입력을 구분 |
| recommendation_item_id / impression_id | 특정 추천 실행의 항목과 노출 연결 |

## 9. 실제 저장 현황 스냅샷

**조회 시각: 2026-10-16 09:39:53 KST.** 기본 `db.sqlite3`를 SQLite 읽기 전용으로 조회하고 기본 폴더의 파일 수를 집계했다. 이 표는 누적 개수이며 실행 중 서버가 다른 ROOMPICK_* 경로를 사용한다면 그 서버의 현재 저장소와 다를 수 있다. 테스트·예시 체험·사람의 실제 이용 기록을 구분한 통계가 아니다.

| 대상 | 건수 |
| --- | ---: |
| Project | 291 |
| SketchEvent | 3,309 |
| Composition | 71 |
| RecommendationRun | 7 |
| SceneObjectRevision | 9 |
| 업로드 바이트 원본 파일 media/originals | 11 |
| 원본 파일 참조가 있는 Project | 12 |
| 편집·자르기 사진 media/rooms | 91 |
| 합성 PNG media/results | 71 |
| 스케치 Raw JSON data/sketch_raw | 29 |

Raw 29개 구간 안의 원본 획은 총 16개, operation은 281개였다. 제출 이유는 project_changed 17개·search 9개·page_exit 3개이며 capture_status는 모두 complete였다. **complete는 이 구간이 수집 한도로 종료되지 않았다는 뜻이며, 모든 행동이 빠짐없이 전달됐거나 라벨이 검수됐다는 증명이 아니다.** 원본 참조 12건과 파일 11개 차이는 재시작·자르기의 파일 참조 재사용과 구분해 본다.

## 10. 아직 수집·구현하지 않은 것

- 검수된 정답 클래스, 학습 동의·철회·보관 정책의 확정, Curated/Features 데이터셋·학습 PNG·모델 학습 자동 실행.
- 실제 상품 검색 결과·실판매 가격·실제 판매사이트 방문 성공·구매 전환·광고 노출 수익.
- 사진 속 기존 물체에 대한 실제 AI 분류와 신뢰도. 현재 ADDED·DEMO_ASSUMPTION은 모의 가정이다.
- 결과 가구 드래그의 전체 점 시퀀스, 텍스트 한 글자마다의 타이핑 이력, 두 손가락 확대·이동.
- 로그인·계정별 이력·다른 기기에서 동일인을 연결하는 식별 기능.
- Kafka/Spark/S3로의 실제 적재, 운영 규모 부하 성능 보장. 로컬 저장을 AWS 이관 완료라고 부르지 않는다.

근거: [모델](../../sketch/models.py), [사진·수집·생성 API](../../sketch/views.py), [Raw 검증](../../sketch/raw_samples.py), [브라우저 수집기](../../sketch/static/sketch/collector.mjs), [추천 API](../../sketch/platform_views.py), [플랫폼 DTO](../../sketch/platform.py). 조회 명령은 [명령어 문서](행동데이터_및_로그_확인_명령어.md)에 있다.
