# Raw·파생·업무 데이터 목록

기준일 2026-10-18 · DB는 mode=ro/query_only로 읽고 개인정보 원문/사용자 ID/IP는 문서에 옮기지 않았다. 아래 집계는 스냅샷이며 사용 중 증가할 수 있다.

## 1. 데이터 분류

| 데이터 | 저장 위치/모델 | 의미와 경계 |
| --- | --- | --- |
| 업로드 원본 | media/originals/*.bin · Project.original_photo/checksum | 업로드 바이트·SHA256, 원본 도입 전 사진은 복원 안 됨 |
| 편집용 사진 | media/rooms/*.jpg · Project.photo/width/height | EXIF 보정·최대 변 2,048px·crop 파생 사진 |
| 작업 문서/설명 | Project.document/request_context | 최종 스케치 상태·별도 설명·스타일, 갱신 가능 |
| Raw JSON | data/sketch_raw/<sample_id>.json | 펜/지우개 점별 시간·operations·초기/최종 문서, 불변 |
| 행동 이벤트 | SketchEvent | 사용자/서버 완료 사건·수신 인증·방문/flow 연결 |
| 생성 입력/결과 | Composition · media/results/*.png | 문서·요청 snapshot, 최초/현재 배치·결과 버전 |
| 추천 실행 | RecommendationRun | 입력 해시·물체/버전·lease·상태·예시 상품/metadata |
| 검색 조건 수정 | SceneObjectRevision | 사용자가 수정한 품목/bbox/기준 버전, 정답 라벨 아님 |
| 인증/방문 | Django auth/session · Project.owner/account | 자체 계정·브라우저 ID·계정 연결, 익명 동일인 추정 아님 |
| 비회원 제한 | GuestGenerationUsage | 서버 IP/KST 날짜별 성공 횟수, 광고 실적 아님 |
| 개인 프로젝트 | SavedProject / SavedProjectVersion · media/saved_projects/ | 회원별 버전·독립 PNG·입력/사진 참조/배치 snapshot |
| 찜 | WishlistEntry | 회원별 안정 상품 키·추천 출처·예시 상품 snapshot |
| 생성 시도 | GenerationAttempt | 새 추적 기능 이후 request_id별 상태/시간/오류/결과 |
| Raw 수신·검수 | RawReceipt | 실제 저장 바이트 체크섬·수신/재전송·경고·사람 검수 |
| 광고 초안 | AdCampaign · media/ops/ad_creatives/ | 로컬 소재·HTTPS URL·품목·기간·심사/활성·버전, 실제 송출 없음 |
| 운영 감사 | OperatorAudit | 행위자·변경/열람·대상·전후 값·사유·시각 |
| 브라우저 임시 데이터 | localStorage / sessionStorage / IndexedDB | 미전송 이벤트·Raw·편집 이력, 서버 저장과 다름 |
| 서버 실행 로그 | 터미널 또는 data/runtime/*.log | HTTP 상태·traceback, 점별 Raw나 제품 분석 집계 아님 |

원본 사진, 편집용 사진, 결과 PNG, 사용자 저장 PNG는 서로 다른 산출물이다. 사진 참조를 여러 Project가 공유할 수 있어 Project 수와 파일 수를 일대일로 비교하지 않는다. sample 이미지와 상품 응답은 요청 시 렌더링되는 예시이며 실판매 상품의 원본 사진 수집이 아니다.

## 2. 실제 저장 현황 (읽기 전용)

| 테이블 | 건수 |
| --- | --- |
| `auth_user` | 1 |
| `sketch_project` | 355 |
| `sketch_composition` | 78 |
| `sketch_sketchevent` | 3568 |
| `sketch_recommendationrun` | 10 |
| `sketch_sceneobjectrevision` | 9 |
| `sketch_savedproject` | 1 |
| `sketch_savedprojectversion` | 1 |
| `sketch_wishlistentry` | 2 |
| `sketch_guestgenerationusage` | 2 |
| `operations_generationattempt` | 1 |
| `operations_rawreceipt` | 44 |
| `operations_adcampaign` | 0 |
| `operations_operatoraudit` | 6 |

Raw JSON 파일은 **44개**다. 광고 초안 건수가 0이어도 등록/심사 기능이 없는 것은 아니다. 반대로 Raw 44개가 학습용 정답 44개 또는 실제 사용자 44명을 뜻하지 않는다. 체험·개발 검증 기록이 섞일 수 있고 운영 통계로 사용하려면 범위를 정해야 한다.

| Raw 자동 검증 | 사람 검수 | 메타데이터 건수 |
| --- | --- | --- |
| PASS | PENDING | 16 |
| REVIEW | PENDING | 28 |

## 3. 문서·점별 Raw·검수의 차이

표시 문서의 points는 x/y/pressure를 가진 상대좌표다. 점별 t·입력 출처·바깥 좌표는 Raw points에서 확인한다. final_document만으로 삭제/Undo 전의 손 움직임을 복구할 수 없다. 별도 설명 text_requirements는 그림판 안 text item과 다른 입력이다.

Raw의 label_status/quality_status는 수집 계약 값이고 사람의 검수는 RawReceipt에 기록한다. 사용자가 바꾼 검색 품목, 모의 분류 결과, 운영 검수 승인을 서로 같은 정답으로 합치지 않는다. 회원 연결은 Project.account에 반영하며 이미 보존한 writer_id/원문을 바꾸지 않는다.

## 4. 존재하지 않는 저장 산출물

- `/api/results/<id>/data/`는 업무 모델에서 조합한 내부 조회 응답이며 매번 별도 JSON 파일이 쌓이는 구조가 아니다.
- 실제 광고 decision/impression/click, CTR·매출·정산 기록은 없다.
- Kafka 토픽·Spark/Delta·Bronze/Silver/Gold·모델 학습 데이터셋은 데모에 연결되지 않았다.
- 외부 AI가 생성한 실제 방 사진/인식 신뢰도·실제 판매 가격/구매 이력은 없다.

수집 계약은 [행동데이터 수집기준](행동데이터_수집기준.md), DB 구조는 [API 및 모델](../02_개발/API_및_데이터모델.md), 조회 명령은 [로그 확인](로그_종류와_확인명령.md)을 참조한다.
