# Gold 실행·복구·서버 배포

2026-10-17. 상품 파이프라인의 Gold 구현 기준이다. 실행 코드는 `product_pipeline/gold/`, 진입점은 `run_gold.py`/`danawa-gold`다. **Gold Delta 생성과 서비스 MySQL 게시를 구분한다.** 이번 구현은 고정 Silver → Gold → 고정 버전 export manifest까지이며 서비스 DB 쓰기·승인·임베딩 생성·인덱스 활성화를 수행하지 않는다.

## 입력과 책임

| 입력 | 검증·동작 |
|---|---|
| Silver 8개 테이블의 고정 version | `gold_versions.example.json`의 모든 version을 명시. NULL/음수/암묵적 latest는 거부 |
| Silver 평가 receipt | OUTPUT_COMMITTED/QUALITY_WAIT와 자식 key/hash/상품/원천 ID를 대조. receipt 없는 부분 출력은 완료 입력으로 사용하지 않음 |
| GOLD_BUILD 실행 등록 | 실제 MySQL pipeline_runs의 등록 export(UUID·stage·REGISTERED·provider·근거)를 검증·보관. MySQL 등록/실시간 조회 어댑터는 없음 |
| 상품 상태 export | 유효기간·active/supported·확인된 서비스 category UUID·Silver mapping_version 대응. 누락 시 catalog_ready=false |
| 가격 확인 export | 안정 Offer의 정확한 raw ID/source_evidence_key/price_kind/option_key에 대응하는 일반가·7개 가격 조건의 담당 확인 결과. 금액/관측 시각을 새로 만들지 않음 |
| 평가·갤러리 선택 export | 같은 Raw의 여러 평가를 hash/처리 시각으로 최신 판정하지 않음. 대응 evaluation_key의 명시적 선택이 필요. 복수 갤러리도 명시 선택 필요 |
| 임베딩·인덱스 export | READY·사용 승인·활성 index·exact image relation/source_version/checksum·모델/전처리/차원/벡터/인덱스·Silver 확인 참조와 만료 검증. 실제 벡터 생성/파일 인덱스 검사는 담당 책임 |

입력 스키마는 `config/gold_inputs.example.json`이다. 최상위 contract_version=1, policy 및 product_states/embeddings/evaluation_selection/gallery_selection/price_confirmations 5개 snapshot을 모두 명시한다. snapshot은 status=PROVIDED/NOT_PROVIDED, rows, evidence_ref다. PROVIDED와 각 행에는 담당 근거가 필요하다. 미제공은 빈 정상 결과나 승인으로 취급하지 않는다.

policy는 version/evidence_ref, price_history_mode(UNRESOLVED/SNAPSHOT_V1/PRICE_EFFECT_V2), price_max_age_seconds, representative_price_kinds, bootstrap_single_gallery를 명시한다. 예시의 숫자는 운영 합의가 아니다. content_hash는 자기 자신을 제외한 전체 객체에 `product_pipeline.contracts.digest`를 적용해 생성한다. 수정 후 직접 hash를 다시 계산해야 한다.

product_states의 행은 product_key, active/supported(boolean), service_category_id, mapping_version, checked_at/expires_at, evidence_ref다. applicable_attributes에 display_name/manufacturer/brand/model/width_mm/depth_mm/height_mm 중 품목에 실제 적용되는 필드를 명시하면 적용 분모와 확인 건수를 계산한다. 미제공 분모를 전체 상품 수로 추정하지 않는다.

평가 선택은 product_key/raw_document_id/evaluation_key/evidence_ref, 갤러리 선택은 product_key/evaluation_key/evidence_ref다. 단일 갤러리 bootstrap도 policy에서 명시적으로 허용한 경우만 가능하다. 이 초기 후보 선택은 MySQL active 이미지 전환 승인이 아니다. 여러 갤러리는 장수·checksum·source_version 문자열·적재 순서로 선택하지 않는다.

price_confirmations는 product_key/raw_document_id/source_evidence_key/price_kind/option_key/price_basis=REGULAR/condition/checked_at/expires_at/evidence_ref다. condition_schema_version=1, unresolved_evidence_key=null이며 할인·회원·쿠폰·배송·설치·세금은 CONFIRMED+값 또는 명시 NOT_APPLICABLE, option은 CONFIRMED+정확한 option_key여야 한다. 현재 다나와 자료가 이 근거를 대부분 제공하지 않는다는 한계를 승인 입력으로 위장하지 않는다.

## 출력과 선택 규칙

Gold root 아래 8개 Delta를 생성한다.

| 테이블 | 행 기준 |
|---|---|
| product_catalog | Gold 평가 × product_key. 이름/분류/속성·최초 수집 시각·원천 및 필드별 근거·후보 준비 상태 |
| current_offers | 평가 × 안정 offer_key × 확인 조건 × 가격 종류. 옵션·조건이 다른 금액을 한 최저가로 합치지 않음 |
| offer_history | 평가 × Raw/가격 근거. snapshot-v1과 price-effect-v2를 모두 기록하고 원천 증거 보존 |
| product_images | 평가 × 선택 갤러리 연결. 순서/URL/source_version/checksum/객체 키/검수 근거 |
| product_links | 평가 × Silver 링크 근거. 구조·현재 유효성·판매 항목 연결 분리 |
| quality_issues | 평가 × 사유/필드/근거 |
| quality_daily | 평가 × 상품의 일별 집계 기여값. 상품/관측/평가/이미지 관계/고유 파일·적용 속성 분모 구분 |
| pipeline_evaluations | 출력별 key/hash와 실제 Delta version을 확인한 뒤 마지막에 쓰는 완료 receipt |

상품 키는 danawa:<source_product_id>를 유지한다. 서비스 product/asset/image UUID를 임의 생성하지 않는다. 원천별 관측과 이미지 연결을 합쳐 지우지 않는다. 이름·이미지 유사도로 다른 상품을 병합하지 않는다.

같은 필드의 확인 가능한 실제 관측 시각을 비교한다. 나중에 도착한 과거 값은 최신값을 덮지 않는다. 같은 시각의 다른 값은 충돌이며 이전의 모순 없는 확인값이 있으면 그 값을 유지하고 충돌도 기록한다. 최신 실패/결측의 상태도 lineage에 함께 보존한다. 이름 시각을 치수 시각으로 쓰지 않고 product_attributes의 실제 관측 시각으로 치수를 선택한다. 제조사·브랜드·모델의 개별 시각은 현재 Silver 공통 시각으로 증명되지 않으므로 복수 값 충돌 시 최신을 추정하지 않는다. first_collected_at은 **고정 입력 범위 안** 최초 시각이며 전체 서비스 이력의 최초 시각을 덮어쓸 근거가 아니다.

대표가격은 확인된 안정 Offer·일반가·통화·옵션·전체 조건·유효기간이 있는 가격만 사용한다. 조건/옵션이 단일하게 호환되고 정책상 가격 종류가 일치하며 금액도 모순 없을 때만 투영한다. 서로 다른 판매처 가격의 임의 전역 min을 선택하지 않는다. 목록 최저가·회원가·aggregate를 일반 Offer로 승격하지 않는다. 조건 미확정 가격은 Silver에 그대로 보존되고 Gold 현재가격에서 보류된다.

상품 카탈로그 준비, 추천 준비, 비교 링크, 직접 구매 링크는 각각 판정한다. 가격/치수 없음만으로 추천을 무조건 차단하지 않는다. 추천에는 현재 매핑·상품 상태, 선택 이미지의 추천 이용 검수, 대응 임베딩/활성 인덱스 담당 결과, 현재 비교 링크가 필요하다. 직접 구매에는 안정 Offer와 정확한 옵션·확인된 최종 직접 URL 근거가 필요하며 bridge만으로 true가 되지 않는다. 모든 준비 플래그는 **고정 입력과 as-of에서 계산한 후보 상태**다. 서비스의 현재 승인·링크·인덱스 검증을 대체하지 않는다.

## 실행

서버는 공통 `requirements.lock.txt`, `requirements-bronze.lock.txt`의 Spark 4.0.1/Delta 4.0.1 호환 런타임과 현재 패키지를 설치한다. Windows .venv를 서버로 복사하지 않는다. Spark 환경 옵션은 GOLD_SPARK_OPTIONS, 공유 lease 접속은 GOLD_LEASE_MONGO_URI로 분리한다. Spark master/저장소/TLS/인증 설정은 배포 환경에서 주입한다. 비밀값을 설정 파일·Git·명령 이력에 넣지 않는다.

```bash
python run_gold.py \
  --config /etc/team3/pipeline/gold.json \
  --versions /srv/team3/handoff/silver-versions.json \
  --inputs /srv/team3/handoff/gold-inputs.json \
  --registration /srv/team3/handoff/gold-build-registration.json \
  --as-of '2026-10-17T00:00:00.000Z' \
  --state-dir /var/lib/team3-pipeline/gold
```

예시 날짜를 실제 업무 기준 시각으로 바꾼다. 동일 작업 재개는 versions/inputs/as-of를 고정한다. `--max-products 3`은 소량 시험이며 완료 export는 PARTIAL 범위라고 표시된다. 설정·버전·등록 예시는 의도적으로 바로 운영 실행할 수 없다.

모든 Gold Driver가 동일한 Mongo ops namespace와 정규화 URI를 사용한다. Gold root/checkpoint 전체의 공유 lease를 획득한 뒤 쓰며 자동 만료 takeover를 허용하지 않는다. 불확실한 종료는 claim을 유지한다. 이전 Driver/Spark 종료를 확인한 수동 해제는 [Bronze 잠금 가이드](bronze_execution.md)를 따른다. Silver 원본은 읽기 전용이며 Gold와 checkpoint 경로의 중첩을 거부한다.

제품 키 페이지는 기본 20개, 한 상품의 전체 Silver 이력은 기본 20,000행/32MiB 이하를 Driver로 읽는다. 초과하면 조용히 자르지 않고 GOLD_PRODUCT_HISTORY_LIMIT으로 중단한다. 더 큰 이력의 분산 reduction은 후속 확장이며 무조건 한도를 높이기 전에 부하를 측정한다.

## 완료 manifest와 복구

각 실행은 고정 Silver 범위를 다시 순회하며 성공 receipt의 key/hash/고정 버전/현재 출력까지 검증한 뒤 재사용한다. 별도 로컬 cursor를 완료 증거로 쓰지 않는다. 부분 Delta commit 후 종료돼도 동일 입력으로 재시도하면 확인된 출력을 보존하고 receipt를 복구한다. Gold 평가 키는 처리 실행 UUID가 아니라 의미 입력·고정 버전·유효 상태·일별 집계일로 결정된다. 무관한 상품에 대한 입력 행은 해당 상품의 평가 입력에서 제외한다.

CLI는 페이지별 상품→Gold 평가 선택을 checkpoint에 내용 주소로 봉인한다. 모든 선택 결과·receipt를 대조한 뒤 Gold 테이블별 version, 선택 페이지, 입력 참조, 집계 참조를 가진 **GOLD_EXPORT manifest를 마지막에 보존**한다. stdout/state-dir의 export_manifest_ref와 quality_summary_ref가 결과 위치다. MySQL 게시 담당은 전체 테이블의 과거/현재 행을 섞거나 processed_at 최댓값을 임의 선택하지 말고 이 manifest의 선택과 고정 버전을 사용해야 한다.

일별 집계는 Spark가 고정 Gold 선택을 기준으로 품목별/GLOBAL을 각각 계산한다. 이미지 관계 수와 checksum의 distinct 파일 수를 분리한다. 여러 상품이 공유한 파일의 수를 상품별 고유 파일 수 합계로 부풀리지 않는다. 품목별 적용 속성 규칙이 없으면 분모 미제공·부분 제공 상태를 표시한다. 이 집계는 해당 고정 범위/as-of의 스냅샷이며 하루 동안 모든 독립 실행의 결과를 무조건 누적한 통계가 아니다.

정상 0, 실패 2, 중단 130. QUALITY_WAIT는 결과 보존과 품질 대기이며 서비스 게시 완료가 아니다. export의 mysql_published=false/publication_status=BLOCKED_DDL_GUARD_REQUIRED는 의도적이다. 가격 이력 정책이 UNRESOLVED면 서비스 가격 게시도 보류한다. 2026-10-18 별도 [MySQL 게시기](mysql_publication.md)를 구현했다. 게시기는 이 export의 고정 버전과 문서 스키마·UUID·현재 guard·staging을 검증하고 트랜잭션 게시한다. Gold 자체는 DB 게시를 하지 않으며 export의 위 필드는 Gold 생성 당시 상태다. 게시 성공 여부는 별도 SQL publication receipt로 확인하고 기존 불변 export를 수정하지 않는다. 실제 서비스 DDL/guard 인수는 남아 있다.

## 검증

`tests/test_gold.py`, `test_gold_cli.py`는 미확인/만료 입력, 가격 조건, 안정 Offer, 치수 시각, 시각 충돌, 고정 버전/등록 차단, 부분 출력과 receipt 복구를 검증한다. 저장 Silver를 사용하는 실제 로컬 Spark/Delta 재현 명령:

```bash
python -m tests.smoke_gold_delta \
  --silver-root /path/to/saved-silver \
  --spark-options /path/to/local-spark-options.json \
  --output .runtime/gold-new
```

원본 Silver를 변경하지 않는 새 경로를 지정한다. 생성한 cli-fixtures는 --rehearse용 FIXTURE 등록이며 운영에 사용할 수 없다. Windows PowerShell은 JSON 날짜를 DateTime으로 자동 변환할 수 있으므로 --as-of에는 정확한 ISO UTC 문자열을 전달한다.

실제 서버 Mongo/lake/Spark, 다중 호스트 중복 실행, 대량 부하, 실제 등록/매핑/검수/임베딩 export 인수와 MySQL 게시를 로컬 시험 통과로 대체하지 않는다. 최종 결과와 누락 대조는 [요구사항 대조](issue13_acceptance.md)를 따른다.
