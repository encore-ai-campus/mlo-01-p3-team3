# 다나와 파이프라인 데이터·복구 계약 v1

Garage 2.4.1의 객체 쓰기는 [내용 주소 프로필](garage_v241_profile.md)의 추가 계약을 따른다. 모든 참여 writer의 키-바이트 일치로 애플리케이션 불변성을 유지하며 저장소 WORM/원자적 조건부 PUT 지원으로 표현하지 않는다. Delta 쓰기와 checkpoint는 별도의 단일 Driver/자동 인계 금지 잠금을 요구한다.

구현 상태(2026-10-16): FULL_EXPORT 첫 단계 코드는 작성했고 로컬/대역 검증했다. 아래 전체 계약이 배포·인수 완료됐다는 뜻은 아니다. 구체적인 구현/미구현 구분은 [실행 기록](execution.md)을 따른다.

2026-10-16. [통합 설계](integration_design.md)의 미확정 데이터 계약을 구체화한 **구현 기준안**이다. 문서로 정한 동작이며 실행 코드·DB 컬렉션·DDL·시험 인프라가 만들어졌다는 뜻은 아니다. 상위 설계는 [데이터 모델](../../../docs/design/03-data-model.md), [Kafka](../../../docs/design/05-kafka-jobs.md), [인프라](../../../docs/design/01-infrastructure.md), [상품 연결](../../../docs/design/09-product-links.md)을 따른다. 이 문서는 기존 공통 컬렉션에 추가할 필드와 보조 컬렉션을 정의하며, 실제 DB 변경은 별도 구현·마이그레이션 작업이다.

[보강 계약 v1.1](design_amendment_v1_1.md)이 접수/이벤트·상태 재평가·이미지 순서·서비스 게시의 세부 보완 기준이다. observation_key와 Raw/event ID·원문 해시 규칙은 유지한다. 가격 효과 키 변경안은 실제 DDL/서비스 합의 후에만 활성화한다.

## 0. 책임과 쓰기 경계

범위는 수집된 파일을 입력으로 하는 상품 데이터 파이프라인이다. 아래 표의 타 담당 기능은 연동 계약만 다루며 이번 개발에 구현하지 않는다.

| 담당 | 읽는 입력 | 쓰는 결과·책임 | 이번 범위 |
|---|---|---|---|
| commerce_crawler | 다나와 목록/상세·이미지 응답 | raw/products/media·수집 checkpoint·관측 시각·파서 provenance | 기존 기능 유지. 요청·재수집·파서 변경 제외 |
| 파이프라인 스냅샷/적재 | 확정 export·이미지 manifest/파일 | snapshot·Mongo raw/보조 운영 원장·이미지 객체·Kafka 원천 이벤트 | 포함 |
| Spark 상품 ETL | 원천 ID·고정 raw·업로드 확인·버전별 매핑/검수 결과 | Delta Bronze/Silver/Gold·quality_issues·자체 checkpoint | 포함 |
| 파이프라인 게시 | 고정 Gold 버전·검수 입력·실제 DDL | 상품·자산·이미지 연결·판매·가격 이력의 허용 영역, pipeline/publishing 실행 이력 | 포함 |
| 서비스/운영 담당 | 원천·품목·이용 근거 | categories/category_mappings·usage_review_json·감사 원장·운영 UI | 원장 편집·승인 판단·화면 구현 제외 |
| 광고 서버 | 게시 카탈로그·승인 이미지·고정 벡터/인덱스 | 외부 임베딩 요청·검색·인덱스·실제 링크 판단·scikit-learn 순위 | 제외. 준비 결과/버전 인계만 |
| Application | 사용자 입력·광고 서버 결과·행동 | 사용자·context·추천 run·jobs·노출/행동 API | 제외. 상품 게시가 덮어쓰지 않음 |
| 인프라/배포 담당 | 환경 manifest·패키지/실행 계약 | DB·Kafka·S3·Spark·NiFi 설치, 계정·TLS·백업·서버 배치 | 구축 제외. preflight·연결·실행 인터페이스 제공 |

파이프라인은 storage/display/embedding 등 이용 검토 값을 새로 승인하지 않는다. 전달받은 승인 근거·버전·만료·대상 연결을 검증하고 누락은 PENDING으로 유지한다. 카테고리 매핑도 읽기 전용 입력이다. 게시 때 검수 원장의 동시 변경을 버전 비교로 확인하고 스냅샷과 다르면 게시를 보류해 새 버전으로 재평가한다.

서비스 MySQL에 대한 쓰기 허용 목록은 products, offers, offer_snapshots, 상품 영역 assets, product_images의 비검수 연결 필드와 pipeline_runs/publishing_runs 관련 실행 메타데이터다. 기존 테이블·컬럼의 실제 존재와 담당 계정 권한을 preflight로 확인한다. categories/category_mappings·usage_review_json·usage_policy_json·사용자 assets·추천·사용자 jobs·학습 라벨은 UPDATE하지 않는다. 신규 상품 이미지 연결의 검수값은 미검토 기본 상태를 기록할 뿐 승인 이력을 생성하지 않는다. 승인/감사 이벤트 생성은 담당 서비스가 수행한다.

파이프라인의 URL 검사는 저장된 주소의 문법·HTTPS·허용 호스트·pcode 대응과 근거의 버전을 검사한다. 임의로 상품 상세·판매처를 재요청하지 않는다. 현재 접근 가능성·클릭 시 유효성 확인은 수집/링크 담당 결과를 입력으로 받고 실제 사용자 이동은 광고 서버가 판단한다. 검증 시점·만료 근거가 없으면 UNVERIFIED다.

새 수집기의 원본 응답 보존 변경은 이 문서의 연동 요구사항이며 이번 파이프라인 구현에 포함하지 않는다. 적재기는 저장된 원천을 검증하고 사이트별 HTML 파싱·재수집을 수행하지 않는다. 기존 payload_hash 대조는 버전별 순수 검증기로 구현하고 원래 core.payload_hash의 테스트 벡터와 동일성을 확인해 수집 CLI·세션·파서에 의존하지 않게 한다.

## 1. 현재 입력 구조와 지원 범위

코드 근거: [수집·해시](https://github.com/encore-ai-campus/mlo-01-p3-team3/blob/67e837db4114ef8955a12c0b466f6f4aba50bd8f/Commerce_crawler/product_collector/core.py), [정규화](https://github.com/encore-ai-campus/mlo-01-p3-team3/blob/67e837db4114ef8955a12c0b466f6f4aba50bd8f/Commerce_crawler/product_collector/danawa.py), [오프라인 재처리](https://github.com/encore-ai-campus/mlo-01-p3-team3/blob/67e837db4114ef8955a12c0b466f6f4aba50bd8f/Commerce_crawler/product_collector/reprocess.py), [갤러리 보강](https://github.com/encore-ai-campus/mlo-01-p3-team3/blob/67e837db4114ef8955a12c0b466f6f4aba50bd8f/Commerce_crawler/product_collector/gallery_assets.py), [이미지 저장](https://github.com/encore-ai-campus/mlo-01-p3-team3/blob/67e837db4114ef8955a12c0b466f6f4aba50bd8f/Commerce_crawler/product_collector/media.py).

추가 확인 표본은 로컬 수집 실행 `2ab52dfe-92c3-4d0c-9ec3-879462a6c657`의 raw/products/media 각 첫 3개 상품이다. ID는 5097200·96056663·122661898이고, 원천 최초 시각은 2026-10-15 16:55:33+09:00, 상세 시각은 각각 16:55:59·16:56:00·16:56:01+09:00이다. 가공·media 갤러리는 4·4·1개이고 첫 가격 항목은 aggregate였다. 필드·시각 구조를 확인한 표본이며 전체 데이터나 실제 이미지 파일을 다시 전수 검증한 결과는 아니다.

| 입력 | 기존 필드·해석 | v1 사용 |
|---|---|---|
| product_raw.jsonl | source, product_id, job_id, collected_at, schema_version, extractor_version, categories, payload, payload_hash, detail_status/error | 불변 원천 관측. 원래 wrapper도 보존 |
| payload.listings[] | category, url, page, collected_at, product_url, 가격·원문 HTML/API 등 | 목록별 출처·시각. 배열을 첫 항목으로 축약하지 않음 |
| payload.detail | url, collected_at, 추출 규격·판매 항목·gallery_images·HTML 등 또는 null | 상세별 출처·시각. 없는 상세를 생성하지 않음 |
| products.jsonl | source/product_id/job_id/collected_at/categories/raw_payload_hash, fields, provenance, field_status | 파생 입력 검증. 그 자체를 Silver로 게시하지 않음 |
| media/products_with_local_images.jsonl | products 내용 + images[].status/local_path/category_paths/sha256/bytes/width/height/format/extension 등 | 이미지 확보와 상품 연결 대조 |
| run_report 또는 gallery_report | 실행 종류·원래 실행·입력 경로·오류 등 | 실행 계보. 기존 보고서 종류에 맞춰 읽음 |

첫 구현은 source=danawa, 확인한 숫자 product_id, PRODUCT wrapper만 지원한다. seller_offers는 payload 안에 그대로 남긴다. 별도 OFFER 원천을 임의 생성하지 않는다. media 없이 원문만 보존하는 모드도 명시적으로 허용하되 이미지 미확보를 기록하고 추천·게시 성공으로 계산하지 않는다. LIVE의 metadata와 후속 이미지 확보도 동일 관측 키 계산을 적용하며 source seq/전송 snapshot ID를 관측 키에 추가하지 않는다.

## 2. 스냅샷과 접수 단위

FULL_EXPORT 입력 선택은 export_dir, 선택적 media_manifest, 실행 보고서다. 이 모드는 수집·갤러리 보강·다운로드가 멈춘 확정 묶음을 사용한다. LIVE_DELTA는 별도 [증분 인계 계약](live_handoff_v1.md)으로 확정 commit의 raw/normalized pair를 동결한 묶음이다. 활성 개별 JSON을 직접 조합하지 않으며 크롤러를 자동 중단하거나 잠금을 강제 해제하지 않는다.

스냅샷에는 raw/products/선택한 media/보고서의 원문 파일, 상대 경로·파일 SHA256·바이트 수·행 수, 실행 계보, 검증한 이미지 파일 목록을 보존한다. snapshot_id는 manifest의 안정 부분을 아래 H 규칙으로 계산한다. 생성 시각·로컬 절대 경로는 ID 계산에서 제외한다. 이미지 경로는 로컬 드라이브 기준이 아닌 snapshot 내부 상대 경로로 관리한다. 생성 전후 해시·상품 집합·job_id·raw_payload_hash·갤러리 순서와 URL을 대조한다. 완료 manifest를 마지막에 확정하고, 깨진 JSONL은 건너뛰지 않고 스냅샷을 실패시킨다.

FULL_EXPORT에서 제품을 묶는 키는 (source, 문자열 product_id)다. 원천·가공의 집합과 실행·관측 해시가 맞아야 한다. media를 제공한 경우 집합도 동일해야 하며 각 이미지의 source/product_id가 부모와 같아야 한다. LIVE_DELTA에서는 source pointer/관측별로 쌍을 검증해 한 상품의 여러 중간 버전을 보존한다. gallery_assets 출력은 그 출력 디렉터리의 raw/products와 결합한다. 원래 수집 실행의 이전 raw와 임의로 섞지 않는다. 원문·가공에 필드가 존재해도 NULL·빈 값·다운로드 실패는 별도로 판정한다.

초기 한 스냅샷의 적재는 단일 프로세스·상품 단위 commit이다. DB·이미지·Kafka 전체 배치를 원자적으로 성공시키는 것으로 취급하지 않는다. 검증 오류 상품은 격리하고 같은 배치의 다른 상품 진행 여부를 보고서로 구분한다. 스냅샷 전체 무결성 오류는 외부 쓰기 전에 중단한다.

65,498상품과 다중 이미지 처리에서 raw 전체를 한 번에 메모리에 읽거나 매 시도 수집 checkpoint 전체를 재생하지 않는다. JSONL은 스트리밍하고 (source, product_id)의 H 앞 2자리로 임시 파일 256구간에 분할한 후 구간별 정렬/외부 병합으로 raw/products/media를 대조한다. 배치 100은 상품 처리 상한이며 한 상품 BSON/이미지 바이트 한도는 별도다. 스냅샷 전체 파일·이미지 manifest는 파일/객체에 보존하고 ingestion_snapshots에는 분할 manifest 참조·hash·건수를 기록해 Mongo 16 MiB를 넘는 한 문서를 만들지 않는다. 이 파일 분할은 스냅샷 내부 구현이며 기존 카테고리별 이미지 경로·수집 파일은 변경하지 않는다.

## 3. 관측 키와 버전 계산

### 해시 규칙

H(v)는 `json.dumps(v, ensure_ascii=False, sort_keys=True, separators=(',', ':'), allow_nan=False)`의 UTF-8 바이트 SHA256 소문자 64자리다. 규칙 ID는 `pipeline-json-v1`이다. 배열 순서와 문자열은 보존하고 Unicode 정규화·URL 쿼리 제거·공백 정리를 하지 않는다. 객체 키는 문자열만 허용하고 NaN/Infinity는 격리한다. Python·직렬화 버전과 테스트 벡터를 릴리스에 고정한다. 기존 core.payload_hash는 다른 규칙이므로 재계산 대조만 하고 교체하지 않는다.

| 계산값 | 정확한 입력·규칙 |
|---|---|
| source_record_key | `danawa:PRODUCT:<확인한 product_id>` |
| source_observation_hash | 입력 raw wrapper 전체에 H 적용. 시각·categories·detail_status/error까지 원문 그대로 포함 |
| gallery_acquisition_hash | 순서대로 된 이미지 의미 투영 목록에 H 적용. 아래 정의 참조 |
| observation_key | H({contract_version: 1, collection_run_id: raw.job_id, source_record_key, source_observation_hash, gallery_acquisition_hash}) |
| snapshot_id | 원본 파일과 이미지의 상대 경로·SHA256·바이트 수·계보·형식 버전에 H 적용. 상품 관측 ID의 대체값이 아님 |
| document_hash | document_hash 자신을 제외하고 _id 포함 envelope 전송 표현에 H 적용. ObjectId는 24자리 문자열, BSON 날짜는 UTC 밀리초 ISO8601(YYYY-MM-DDTHH:mm:ss.sssZ). 원래 payload/metadata의 시각 문자열은 보존 |

이미지 의미 투영은 URL, observed_url 또는 null, order, role, origin, selector, resolution_status 또는 null, 로컬에서 검증한 sha256·bytes·width·height·format·extension 또는 null을 가진다. 저장 경로·파일명·category_paths·downloaded_at·오류 문구·업로드 결과·버킷·접속 주소·asset_id는 제외한다. 바이트 미확보는 null 메타데이터로 통일하고 별도 acquisition_state=UNAVAILABLE, 검증한 바이트는 VERIFIED로 구분한다. 서로 다른 실패 문구·재시도 횟수만으로 새 관측을 만들지 않는다. 동일 URL의 중복/충돌·순서 충돌은 조용히 삭제하지 않고 검증 오류로 남긴다.

| 변경 | ID·원문 처리 |
|---|---|
| 같은 스냅샷 재실행·경로만 이동 | 같은 observation_key·raw ID·event ID |
| 다른 job_id의 재수집, 내용 동일 | 다른 관측. product_key는 동일 |
| 같은 job_id에서 상세·카테고리·원문 시각 변경 | 새 source_observation_hash, 새 관측 |
| 원문 동일, 원래 실패한 갤러리의 파일 확보 | 새 gallery_acquisition_hash, 새 관측. 과거 image_refs는 불변 |
| 검증한 로컬 이미지의 업로드 실패 후 성공 | 같은 관측. 처리 상태만 변경 |
| 동일 URL의 이미지 바이트 변경 | 새 이미지 확보 버전·관측. 상품 동일성을 새로 판정하는 근거는 아님 |
| 원문 동일, 가공 로직만 변경 | 원천 추가 없음. normalizer_version·processing_run_id로 별도 파생 이력 |

오프라인 reprocess는 현재 코드가 새 job_id를 발급하므로 입력 관측 실행은 해당 job_id를 그대로 보존한다. 보고서 original_run_id·execution_mode와 원래 관측 시각을 계보로 추가한다. 이를 새 HTTP 수집이라고 표시하지 않는다. 계보 근거가 없으면 원래 실행을 추정하지 않고 lineage_status=UNKNOWN으로 품질 기록한다.

## 4. MongoDB 원천과 처리 상태의 역할

### raw_products: 불변 원천

상위 공통 필드를 유지하고 다음 확장을 사용한다. raw_document_id는 Mongo ObjectId를 접수 단계에서 한 번 발급해 먼저 운영 원장에 majority·journal로 기록한다. observation_key 고유 제약 경쟁에서 승리한 원장의 ID를 모두 재사용한다. ID를 SHA256 앞부분으로 잘라 만들지 않는다.

| 확장 필드 | 형식·책임 |
|---|---|
| ingest_contract_version | `1` |
| observation_key | 위 계산값. 고유 인덱스 |
| source_observation_hash·gallery_acquisition_hash·document_hash | 해시 규칙별 대조값 |
| collector_metadata | payload를 제외한 기존 raw wrapper 전체. 원래 schema_version·job_id·categories·상태 유지 |
| lineage | 원래 실행·offline 종류와 보고서 참조. 모르면 UNKNOWN |
| hash_contract | 현재 collector payload 해시 방법 + pipeline-json-v1 이름 |
| input_snapshot_id | 처음 접수한 스냅샷. 이후 같은 관측을 다른 스냅샷에서 접수해도 갱신하지 않음 |

동일 observation_key를 다른 스냅샷에서 다시 발견하면 처음 접수 원장의 확정 envelope와 event 본문을 재사용한다. 새 snapshot_id·현재 설정·시각으로 본문을 재구성하지 않는다. 새로운 스냅샷과 기존 관측의 관계는 ingestion_snapshots에만 추가한다. 이 규칙으로 가공 로직·로컬 경로만 바뀐 입력의 document_hash 충돌을 막는다.

공통 collected_at은 raw.collected_at을 UTC BSON datetime으로 변환한다. collector_version/parser_version은 확인한 extractor_version을 보존하고 독립 버전 미기록 사실을 metadata에 남긴다. payload는 기존 listings/detail 그대로다. source_url은 확인한 detail.url, 없으면 실제 목록 관측 url, 둘 다 없으면 null이다. comparison_url과 purchase_url은 별도 파생 필드다.

### 신규 보조 컬렉션: 변경 가능한 운영 원장

| 컬렉션 제안 | 키·내용 | 변경 정책 |
|---|---|---|
| ingestion_observations | observation_key 고유, 고정 raw_document_id·event_id·등록 시각·document_hash·확정 envelope/보관 참조·대상 environment_id | 식별·본문은 접수 후 불변. stage·lease·attempt·next_retry_at·오류 상태만 변경 |
| ingestion_snapshots | (environment_id, snapshot_id) 고유, 파일 manifest·관측 목록/분할 참조·대조 건수 | 입력 참조 불변, 실행 집계 갱신 |
| ingestion_attempts | attempt UUID4, observation_key·execution_id·stage·시작/종료·duration_ms·오류 코드 | 시도별 이력. 이전 시도 덮어쓰기 금지 |
| ingestion_images | (observation_key, image_slot) 고유, checksum·bucket·object_key·업로드 시도·시각·검증 방법 | 상세 이력. 사용 가능 상태의 기준은 보강 계약의 observation 단위 image_state_vector/revision |

이는 수집기 보조 운영 데이터로 MongoDB가 처리 상태를 관리하는 상위 계약에 맞춘다. 서비스 jobs·검수·게시 원장은 MySQL 담당이며 여기에 대체하지 않는다. 로컬 snapshot/JSONL은 증거·전송·장애 대기용이고 운영 성공 판정은 Mongo 원장 기준이다. SQLite는 도입하지 않는다.

DB 연결 전에만 존재하는 로컬 접수 후보는 확정 접수가 아니다. DB 접수 원장 확인 전 이미지 업로드·raw INSERT·발행을 진행하지 않는다. 로컬에서 만든 후보 ID와 DB 기존 ID가 다르면 기존 observation_key 원장의 ID가 우선한다. DB 장애·등록 결과 미확인은 같은 키로 조회·접수 재시도하고 성공 상태를 만들지 않는다.

단일 작성자로 시작해도 observation_key·_id 고유 제약을 갖춘다. 원천 payload·envelope 불일치는 충돌 격리한다. 원장 성공 상태와 실제 원천·객체가 불일치하면 상태를 신뢰해 통과시키지 않고 복구 오류로 남긴다. 인덱스는 source_product_id 단독 고유가 아니라 observation_key 고유와 collection_run_id/collected_at, (source_code, source_product_id, collected_at) 조회용이다.

파일 상태와 별개로 작업 획득은 원장에서 원자적 조건부 lease로 처리한다. 구현 기본값은 lease 120초·갱신 30초, 획득마다 증가하는 fencing token이다. 만료·연결 끊김 시 새 외부 작업을 시작하지 않으며 낡은 token의 상태 갱신은 거절한다. 진행 중 업로드·전달은 완료 여부를 새 작업자가 대조한다. 파일 잠금으로 여러 PC 실행을 보호한다고 가정하지 않는다. 실제 다중 호스트 활성화는 경쟁 테스트 통과 후다.

### 접수와 쓰기 순서

1. 스냅샷·이미지를 읽기 전용 검증하고 관측 키·고정 envelope 후보를 만든다.
2. ingestion_observations의 고유 키로 접수한다. raw ID·event ID(UUID4)·occurred_at·목적지 설정을 고정하고 저장 확인한다.
3. 성공 이미지 객체를 확인·업로드한다. 업로드 가능 키는 접수 시 이미 고정되어 있다.
4. 부분 실패를 운영 상태에 남기되 raw image_refs 본문은 바꾸지 않는다. 로컬 검증/예정 키와 원격 사용 가능성을 구분하고 보강 계약의 image_state_vector/fingerprint/revision을 단일 문서 원자 갱신으로 판정한다. ingestion_images는 상세 이력이다.
5. raw_products INSERT를 majority·journal로 확인한다. 같은 ID가 있으면 고정 본문을 비교한다. 결과 미확인은 primary·majority 조회로 재대조하고 필요하면 같은 INSERT를 재시도한다.
6. 원천 확인 후 고정 Kafka 이벤트를 발행하고 Broker 확인 결과를 운영 원장에 기록한다.

파일·원천·발행 상태를 한 분산 트랜잭션으로 취급하지 않는다. 객체가 아직 없더라도 원천은 보존할 수 있다. raw의 object_key만으로 성공을 표시하지 않는다. 상태 vector를 기준으로 이미지 미확보를 품질 상태로 남기고 후속 업로드 성공은 보강 계약의 주기 대조·평가 receipt로 재평가한다. 새 Raw/원천 이벤트를 만들지 않는다.

## 5. 이미지 자산 ID·출처 버전·서비스 연결

원천 image_refs는 source_url·observed_url·order·role·추출 근거·source_version·검증된 파일 메타데이터·예정 bucket/object_key·local acquisition 상태를 보존한다. asset_id는 null로 고정하며 나중에 채우려고 raw를 UPDATE하지 않는다. 이미지 파일의 공개 URL은 원천에 고정하지 않고 서비스가 승인된 저장소 참조에서 생성한다. 객체 키는 `images/sha256/<해시 앞 2자리>/<전체 SHA256>.<검증한 확장자>`로 고정하고 같은 namespace에서 재사용한다. 예정 키와 실제 검증된 업로드 상태는 구분한다.

source_version은 `source-v1:` + H({source_url, observed_url, etag, last_modified, raw_document_id, image_slot, version_rule: 1})로 정의한다. 현재 manifest에는 HTTP validator가 없으므로 etag/last_modified는 null이고 validator_status=NOT_RECORDED다. 없는 값을 추정하지 않는다. 새 수집에 실제 validator가 기록되면 해당 값만 사용한다. 파일 checksum과 이 원천 버전은 다르다.

이 source_version은 시간순 비교 키가 아니다. 같은 파일의 새 원천 연결은 새 버전일 수 있다. 파일·관측·연결·승인 재사용을 구분하는 기준은 [보강 계약 8절](design_amendment_v1_1.md#8-가격-이력이미지-연결과-승인-재사용)을 따른다.

image_slot은 확인한 대표 사진에 representative, 나머지는 gallery:<원본 order>를 사용한다. role은 representative/gallery로 보존하고 image_variant는 representative 또는 gallery:<order>로 투영해 추가 사진들이 같은 original 슬롯으로 충돌하지 않게 한다. order가 없거나 중복이면 새 임의 순서를 발급하지 않고 연결 검증을 실패시킨다. URL 쿼리는 제거하지 않는다. 슬롯 재배열도 새 원천 연결 버전이며 파일은 재사용할 수 있다.

| 시점·작성자 | ID·연결 |
|---|---|
| 적재기 | checksum 기반 객체 저장, raw ID·source_version·슬롯·객체 키 연결. MySQL UUID 미발급 |
| Silver | 원천·객체 확인 결과·검수·category mapping의 입력 버전을 고정해 이미지 관계를 검증 |
| Gold 게시기 | (bucket, object_key)의 상품 자산 조회. 기존 assets UUID 재사용, 신규에 UUID4 발급 |
| Gold 게시기 | product_key로 기존 products UUID 조회, 신규에 UUID4 발급 |
| Gold 게시기 | product UUID·source_code·source_product_id·image_variant·source_version·raw ID·asset UUID의 연결 업무 키로 product_images 조회. 신규 연결만 UUID4 |
| 서비스 검수 담당 | usage_review_json·감사 event_id·scope·만료·permission_snapshot_hash 관리. 게시기가 승인으로 덮어쓰지 않음 |

연결 업무 키 전체와 고유 제약을 DDL로 표현하고 해시 키를 쓰면 원래 구성요소도 대조한다. assets가 공유돼도 상품별 연결·이용 검수는 별개다. 새 연결이 검증됐다는 이유만으로 active를 교체하지 않는다. [보강 계약 6~7절](design_amendment_v1_1.md)의 순서 근거·상품별 게시 guard·트랜잭션·검수 버전 대조를 통과해야 전환한다. 과거/순서 불명/승인 대기 후보는 현재 연결을 덮어쓰지 않는다.

원천 저장 → 자산 생성 → 상품 이미지 연결의 단방향이므로 순환 FK가 생기지 않는다. 원천 ID는 MySQL에 24자리 문자열로 추적하며 DB 간 FK를 걸지 않는다. asset_id 역조회는 서비스 연결에서 한다.

## 6. 원본 응답·시각·가격·서비스 키

### 기존 원문과 신규 응답 보존

기존 저장 HTML/JSON은 추출·디코딩 이후 자료다. raw_object_key/response_body_hash는 null, capture_status=LEGACY_EXTRACTED_ONLY를 metadata에 남긴다. 현재 payload와 wrapper를 보존하는 데 충분하지만 원본 HTTP 응답 보존 완료로 표시하지 않는다. 추출 해시는 기존 방식으로 유지한다.

향후 수집기 변경을 별도 승인받아 도입할 때 응답 content-decoding 후·문자 디코딩 전 바이트를 원본 보존 단위로 정한다. representation=CONTENT_DECODED_ENTITY_BYTES를 명시하고 해당 정확한 바이트의 SHA256을 계산한다. wire 압축 바이트 해시라고 부르지 않는다. 목록·상세 등 여러 요청은 각각의 URL·요청 시각·응답 시각·객체 참조·hash를 response_refs로 보존하고 공통 raw_object_key/response_body_hash는 source_url의 주 응답에만 대응한다. 예전 자료를 다시 요청해 당시 응답으로 채우지 않는다. 토큰·쿠키·인증 헤더는 보존 대상에서 제외한다.

원천 BSON은 직렬화 크기를 쓰기 전에 검사한다. v1은 16 MiB 이상을 RAW_TOO_LARGE로 격리하고 원본 스냅샷을 보존한다. payload를 잘라내거나 임의 참조로 바꾸지 않는다. 큰 원문을 객체 참조로 전환하는 envelope v2는 별도 계약으로 도입한다. 보조 접수 문서도 BSON 한도를 검사하고 초과 본문은 스냅샷의 불변 객체 참조·해시로 보관하며 조용히 유실하지 않는다.

### 값별 관측 시각

raw.collected_at은 상품 wrapper 최초 시각이다. source listing의 가격은 그 listing.collected_at, 상세 판매·규격은 detail.collected_at을 사용한다. image downloaded_at·linked_at·ingested_at은 각각 다르고 가격 최신성에 쓰지 않는다. 오프라인 재처리 시각을 원래 가격 관측 시각으로 승격하지 않는다. 값별 원시 시각이 없으면 UNKNOWN으로 두며 현재값 자동 교체에서 제외한다.

최신 값은 같은 업무 키·같은 가격/옵션/배송 조건 안에서 observed_at을 비교한다. 늦게 적재된 과거 관측은 이력만 추가한다. 같은 시각·같은 값은 재사용, 같은 시각·다른 값은 TIMESTAMP_VALUE_CONFLICT로 품질 대기하고 기존 현재값을 유지한다. 서로 다른 조건의 가격은 같은 값으로 경쟁시키지 않는다.

### 서비스 업무 키와 가격 조건

| 대상 | 고정 규칙 |
|---|---|
| product_key | `danawa:<확인한 product_id>`. 원천 category·이미지·모델명이 바뀌어도 동일 |
| source_record_key | `danawa:PRODUCT:<id>`. 서비스 product_key와 의미 구분 |
| offer_key | source·상품 ID·확인한 안정 seller ID·판매 항목 ID·옵션 식별자를 순서 고정한 JSON tuple로 구성. 표기 접두어 offer-v1:H(tuple), 원래 구성요소 함께 저장 |
| 옵션 없음 | 실제 옵션 없는 판매 항목임이 확인됐을 때만 UNSPECIFIED. 미수집·불명확은 UNKNOWN으로 품질 대기 |
| offer_snapshot_key | snapshot-v1:H({offer_key, raw_document_id, 가격/조건 근거 fingerprint}). 동일 관측의 중복·상충 항목 검증 |
| image asset | (bucket, object_key), 파일 checksum은 메타데이터로 대조 |

위 snapshot-v1은 원천별 증거 키다. 이미지 확보 관측만 늘었을 때 서비스 가격 시점 이력을 반복하지 않는 정책은 [보강 계약 8절](design_amendment_v1_1.md#8-가격-이력이미지-연결과-승인-재사용)의 price-history-v2 또는 v1+집계안으로 구분한다. MySQL snapshot_key 의미 변경은 서비스/DDL 인수 전 선택해야 하며 기존 Raw·UUID를 자동 변환하지 않는다.

현재 seller_id는 판매자 링크/식별 문자열일 수 있고 stable source_offer_id가 항상 있지 않다. URL의 임시 클릭 토큰이나 배열 위치·이름·가격으로 offer_key를 만들지 않는다. 안정 ID가 검증되지 않은 판매 항목은 raw/Silver에 보존하고 OFFER_ID_UNVERIFIED로 서비스 Offer 생성을 보류한다. 판매 항목 자체가 seller ID 안에서 유일함을 증명하면 검증 규칙 버전과 근거로 대체 식별을 정의한 뒤 사용한다.

aggregate의 low_price/high_price/offer_count와 listing_prices는 집계 관측이다. offers 일반 판매 가격으로 넣지 않는다. 할인·회원·쿠폰·배송·설치·옵션이 불명확한 가격은 price_basis=UNKNOWN, 조건 원문 유지다. 확인한 일반 가격만 REGULAR로 표시하고 currency 없는 숫자에 KRW를 추정해서 붙이지 않는다. 현재 코드가 명시적으로 KRW로 추출한 값은 그 provenance와 함께 보존한다.

comparison_url은 실제 product_url과 pcode 대응·허용 HTTPS 호스트를 확인해 등록한다. 직접 구매 URL이 없어도 비교 페이지 추천 경로는 가능하다. 검증되지 않은 직접 링크·실제 판매 가격을 만들지 않는다.

## 7. Silver·Gold 품질과 DDL 경계

원천 categories 71개는 서비스 분류 71개를 뜻하지 않는다. categories/category_mappings 원장을 버전별 입력으로 읽는다. 원천 분류가 여러 개면 유효 매핑이 하나의 서비스 category UUID에 합의할 때만 확정한다. 없으면 CATEGORY_UNMAPPED, 여러 서비스 품목으로 갈리면 CATEGORY_AMBIGUOUS다. 정렬 첫 분류로 결정하지 않는다. 매핑 검토자는 포함·제외 품목과 부속품 혼입을 확인한다. 매핑 미비여도 Bronze 보존은 진행한다.

규격은 원문·단위·축·옵션/측정 대상·근거 경로를 함께 보존한다. cm→mm는 10배, m→mm는 1000배로 변환하며 명시된 숫자·단위만 처리한다. 범위·조절식 값은 별도 범위 메타데이터로 두고 단일 width/depth/height를 임의 선택하지 않는다. '가로×세로'의 세로를 깊이·높이 중 하나로 추정하지 않는다. 좌판 폭을 전체 제품 폭으로 쓰지 않는다. 미확인·불명확·모순은 NULL+품질 사유다.

MySQL 최소 상품 반영 입력은 product_key·이름·검증한 서비스 category_id·출처/상품 ID·원천 참조·시각이다. 비교 URL·규격·브랜드 등은 별도 검증 상태를 보존하고 빈 값을 0이나 임의 문자열로 채우지 않는다. category가 미확정이면 서비스 게시를 보류한다. Offer가 없어도 검증한 비교 링크 상품 카탈로그는 만들 수 있다. 가격·규격 누락을 확인된 값으로 표시하지 않는다.

추천/이미지 임베딩의 활성 조건은 품목 지원·상품 상태·유효한 현재 이미지 연결·용도별 검수·현재 비교 링크를 검증하는 상위 계약을 따른다. display 승인만으로 embedding·외부 전송·학습을 승인하지 않는다. 승인 근거 없는 데이터는 PENDING으로 둔다. 원본 생성 이용은 초기 DENIED 정책을 유지한다.

실제 ORM/DDL 없이 서비스 DB를 직접 생성하지 않는다. 게시기 구현 시 기존 실제 DDL과 UUID·고유 키·nullable·FK를 검증하는 preflight를 필수로 둔다. 누락/불일치면 서비스 쓰기를 막는다. 필요한 논리 계약은 위에서 고정했고 실제 테이블 생성·마이그레이션 적용은 서비스 담당 작업이다. 검수·사용자·추천 테이블은 게시기의 쓰기 대상에서 제외한다.

## 8. 발행·복구·자동화 설정

필수 이벤트 스키마와 충돌/호환성의 단일 기준은 [보강 계약 4절](design_amendment_v1_1.md#4-이벤트-계약의-단일-기준)이다. product.raw.collected의 observation_key·document_hash를 포함한 고정 본문·ID를 재사용하고 서비스 job_id를 임의 생성하지 않는다. 같은 ID의 다른 본문은 충돌 격리한다. 기존 운영 이벤트와의 호환성 확인 전 배포하지 않는다.

Producer는 acks=all·멱등 Producer 설정과 전달 callback 확인을 사용한다. Broker 3대·replication.factor=3·min.insync.replicas=2의 상위 초기 구성과 실제 ISR을 preflight로 확인한다. 정확한 보장 범위는 클라이언트·Broker 설정으로 시험한다. Broker 확인 뒤 상태 저장 실패면 EVENT_UNKNOWN으로 보고 같은 event_id로 재발행할 수 있다. 이벤트 전달 중복은 정상 복구 경로다. Kafka 성공과 Spark 적재 성공을 구분한다.

단계 상태와 서비스 job 상태는 별개다. 수행은 RUNNING, 일시 실패는 RETRY_WAIT+next_retry_at, 예산 대기는 BUDGET_WAIT, 완료는 SUCCEEDED, 상한 초과는 FAILED와 원인 RETRY_EXHAUSTED로 투영한다. 중단·재시도 전환에서도 시도 종료와 duration_ms를 기록한다. 강제 종료로 종료 시계를 알 수 없으면 추정값을 실제 측정으로 기록하지 않고 종료 측정 불가 사유를 남긴다.

Consumer는 poll·assignment·pause/resume·commit을 한 담당 루프에서 관리한다. 초기 partition 내부 순차 처리와 유한한 in-flight/대기열을 사용한다. 재시도 대기는 원장에 저장하고 긴 sleep으로 poll을 막지 않는다. 일반 Consumer의 offset은 처리 결과 또는 복구 가능한 상태 저장 후 연속 완료 구간의 다음 offset만 확정한다. Spark Structured Streaming은 이를 Driver·microbatch checkpoint로 관리하며 임의의 수동 offset commit과 혼합하지 않는다. Bronze batch는 raw ID와 document_hash 대조·MERGE·Delta commit이 완료되어야 성공으로 반환한다. 재할당/종료 때 새 제출을 멈추고 미완료는 재전달·lease로 복구한다. 상품 준비와 사용자 추천의 그룹·동시성·예산을 분리한다.

복구는 event_id와 원장에 저장한 이벤트 본문을 읽고 Mongo raw를 primary·majority로 대조한 뒤 발행한다. 이전 24시간만 조회해 오래된 실패를 놓치지 않도록 상태/next_retry_at 인덱스로 모든 미완료를 조회한다. 주기적 범위 대조는 전체 확정 스냅샷의 접수 건수·raw·Bronze 원천 ID를 비교한다. Broker 보관 기간이 지나도 같은 원천 ID·이벤트 ID로 지정 범위를 다시 전달한다. 원장·스냅샷 모두 없으면 임의로 원천을 생성하지 않고 RECOVERY_EVIDENCE_MISSING으로 중단한다.

아래 숫자는 **초기 구현 기본값**이며 실측 성능 보장이 아니다. 배포 설정에서 변경하고 실행 이력에 고정한다.

| 설정 | v1 기본 정책 |
|---|---|
| 상품·이미지 동시성 | 각 1, 일괄 읽기 배치 100상품. 한 상품 성공 즉시 상태 기록 |
| 네트워크 timeout | 연결 5초, 개별 작업 30초. 클라이언트별 지원 설정으로 변환·검증 |
| 일시 실패 재시도 | 상위 초기 후보에 맞춰 첫 시도 포함 최대 3회, 1/2초 기반 full jitter, 지연 상한 60초. Retry-After는 우선하고 next_retry_at에 기록 |
| 재시도 소진 | RETRY_EXHAUSTED, 자동 무한 반복 금지. 명시 재시도 시 새 실행 시도 이력·기존 식별자 유지 |
| 분류 오류·권한 오류·변조 | 즉시 격리/중단, 자동 재시도 제외 |
| 예약 실행 | 배포 활성화 전 수동. 활성화 시 NiFi 한 곳에서 5분마다 새 확정 스냅샷·기한 도래 작업 발견 |
| 상태·건수 대조 | 완료 배치마다, 별도 전체 지정 범위 대조는 매일. 미완료는 기간 필터 없이 조회 |
| 자동 삭제 | v1 비활성. 검증된 백업·보관 승인 전 raw·객체·접수 원장·checkpoint 삭제 안 함 |
| 로그 | 구조화 duration_ms·ID·오류 코드. 비밀정보·HTML·이미지 본문 제외, 순환 한도는 배포 필수 설정 |

부하 측정 없이 worker를 올리지 않는다. Kafka 클라이언트 내부 재시도도 위 외부 재시도와 합산해 한도·시간을 검사한다. SDK 기본 재시도가 명령 전체를 무한 대기시키지 않게 명시 설정한다. Retry-After가 기본 지연 상한보다 길면 서버가 제시한 시각까지 원장에 대기 상태로 둔다. 429/403 등 원본 크롤링 정책은 기존 수집기 책임으로, 이 적재 기본값으로 변경하지 않는다.

## 9. 환경별 필수 설정과 실행 차단 조건

| 필수 설정 | 검증·없을 때의 동작 |
|---|---|
| environment_id·contract version | 상태/목적지 분리. 달라진 환경으로 기존 상태를 그대로 재개하지 않음 |
| Mongo URI·DB·replicaSet | 3개 멤버·rs-products·primary·majority·journal 조건 확인. 운영 모드에서 standalone 거절 |
| 이미지 S3 호환 endpoint·bucket·prefix·TLS/인증 | 전용 시험 prefix에 쓰기/조회/checksum 대조. Delta 버킷과 혼합 금지 |
| Kafka bootstrap·인증·topic | advertised 주소 접근·토픽·복제·acks/전달 확인 시험. 실제 발행 시험은 지정 시험 토픽에서만 |
| lake endpoint·Bronze/Silver/Gold 경로·checkpoint | 서로 다른 작업 checkpoint, Delta 단일 Driver 작성자 설정 |
| MySQL URI·실제 DDL·매핑/검수 버전 | UUID·고유 키·FK·쓰기 범위 preflight. 불일치 시 게시 차단 |
| 영속 볼륨·원장 백업·로그 용량 | OS 임시 디렉터리에 운영 상태 저장 금지. 백업 복원 대조 필수 |

실제 주소·비밀번호·인증서·서비스 UUID는 문서에서 만들지 않는다. 환경 manifest가 이 필드를 충족하면 실행할 수 있고, 누락되면 해당 단계 실행을 명확한 오류로 차단한다. dry-run은 원본 읽기·입력 검증·실행 계획만 수행하고 DB/버킷/토픽을 생성하지 않는다. 인프라 preflight는 별도 모드로 구분하고 쓰기 시험은 시험 자원을 명시했을 때만 한다.

논리 대상(environment_id·DB·컬렉션·버킷 namespace·topic·계약 버전)의 fingerprint를 접수 원장에 고정한다. 인증정보 교체·동일 논리 저장소의 endpoint 이전은 ID 변경 사유가 아니며 별도 이전 기록으로 승인·대조한다. 다른 실제 DB/버킷으로 복제할 때는 원천·운영 원장·객체를 같은 범위로 보존하고 건수·해시·기존 ID를 검증한 뒤 환경 전환한다. fingerprint가 다르다고 자동으로 원장을 초기화하거나 새 ID를 발급하지 않는다.

MySQL 게시·검수 읽기와 쓰기는 현재 Primary만 사용하고 Replica 임의 읽기 분산을 도입하지 않는다. 수동 승격 중 게시를 중지하고 기존 publishing_run·고정 Delta 버전·업무 키를 대조한 뒤 접속 풀을 갱신해 재개한다. UUID는 DB CHAR(32), API·Kafka·Delta의 서비스 참조는 하이픈 포함 36자리로 직렬화한다. Mongo raw ID는 24자리 문자열이다. 요청·외부 파일 I/O 중 MySQL 트랜잭션을 유지하지 않는다.

배치는 pj3-nifi의 상품 준비 프로세스, ETL은 pj3-spark-master/worker 실행 영역, 서비스는 pj3-webapp·pj3-ad-server, 모니터링은 pj3-monitor의 책임 경계를 따른다. 현재 상위 인프라의 20대 구성을 유지한다. 이미지용/Delta용 역할은 분리하되 pj3-object01·pj3-object02의 개별 역할 배정은 미확정이며 환경 manifest로 확인한다. Kafka·Mongo·MySQL 호스트 장애 분산과 별도 데이터 볼륨 용량을 preflight에서 검증한다. 실제 IP·담당자를 임의로 추가하지 않는다.

클라이언트 책임은 Mongo=PyMongo, S3=boto3, Kafka=confluent-kafka로 분리한다. 이는 구현 선택이며 이번에 설치하지 않았다. 실제 Python/OS·서버 버전에서 확인한 릴리스만 별도 requirements/lock에 기록하고 CI가 설치·직렬화·접속·전달 계약을 시험한다. 현재 수집 requirements와 가상환경을 수정하지 않는다. Spark/Delta/Java/커넥터는 별도 런타임 manifest에 호환 조합을 고정한다. 검증 전 임의 최신 버전으로 운영을 시작하지 않는다.

단계별 런타임이 준비되지 않아도 스냅샷 검증·저장소 대역 테스트는 개발할 수 있다. 실제 Replica Set·S3·Kafka·Delta·MySQL의 통합 인수는 각 환경 manifest가 준비된 뒤만 진행한다. Windows 로컬 → VM/EC2 이전은 접속 설정 교체와 기존 원장/객체/manifest/PK·FK 보존으로 수행하고 새 ID로 재적재하지 않는다.

## 10. 개발·인수 단계와 차단 기준

1. 계약 테스트: H 직렬화 벡터, 원천/가공/media 연결, 실제 시각·aggregate/Offer 분리, null·중복·변조·경로 변경·오프라인 계보.
2. 스냅샷: 원본 불변·활성 수집 차단·확정 manifest·입력 교체 감지. 첫 시험은 위 소량 실행의 3상품 대응 묶음이며 시험 복사본으로 범위를 줄인다.
3. Mongo·이미지: 접수 고유 키 경쟁, 저장 결과 미확인, 객체 업로드 재시도와 이미지 확보 버전 변경 구분, 원문 충돌·BSON 한도.
4. Kafka: 고정 event 본문, RAW_CONFIRMED 이전 발행 금지, callback 유실, 발행 후 원장 갱신 실패, 동일 ID 재전달.
5. Bronze: raw ID·document_hash 검증, 중복 MERGE, Delta commit 후 offset/checkpoint 확정, 장애 재시작·기존 ID 재처리.
6. Silver/Gold: 미매핑·다중 분류·규격 범위·판매 ID 미확인·시각 충돌·용도별 승인 누락을 품질 대기로 분리.
7. 게시: 실제 DDL preflight, 고정 Delta 버전·staging 대조·단일 트랜잭션 반영, 기존 UUID·검수 원장 보존·현재값 역전 방지.
8. 자동화: 단일 NiFi 실행 주체, lease 만료·낡은 token 거절·재시도 한도, 원장/스냅샷 백업 복구와 보관 기간 이후 재발행.

대역 시험 통과는 실제 복제·저장소·Kafka 장애 검증과 구분한다. 통합 검증 결과에는 실행 ID·입력 버전·인프라 버전·건수·실패 사유를 기록한다. 소량 통과를 65,498건 전체나 서비스 추천 품질 검증으로 표시하지 않는다. 개발 범위는 1~4를 첫 인수 단위로 하고 5~8은 입력 계약·실행 환경을 갖춘 순서로 별도 인수한다.
