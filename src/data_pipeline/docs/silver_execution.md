# Silver 실행·복구·서버 배포

2026-10-17. `product_pipeline/silver/` 및 `run_silver.py`의 구현 문서다. 고정 Bronze → 정규화/품질 → 7개 출력 Delta → evaluation receipt 순서이며 Gold·MySQL 게시·승인 원장 편집은 수행하지 않는다.

## 구현 책임

| 모듈 | 책임 |
|---|---|
| inputs | 담당자 export의 형식/hash, 관련 행·만료 상태, 이미지 vector, 실행 등록 근거 검증 |
| values / normalize | 정확한 숫자·단위·날짜, 상품·속성·가격·이미지·링크·품질 및 안정 Offer 정규화 |
| schemas / delta | 고정 타입, insert-only MERGE, 키/내용 충돌, 실제 Delta version·출력 검증 |
| source | 고정 Bronze version 페이지 읽기와 Mongo primary/majority 읽기 전용 상태 조회 |
| runner | 이미지 입력 재확인, 부분 commit 복구, 출력 확인 뒤 receipt, 동일 평가 재사용 |
| checkpoint / cli | 공유 대상 바인딩·입력 보관·발견 cursor·운영 잠금·실행/종료/로그 |

테이블은 `silver_root` 아래 product_observations, product_attributes, price_observations, product_images, link_observations, quality_issues, normalized_offers, pipeline_evaluations다. 가격과 이미지를 상품 단위로 곱집합 join하지 않는다.

현재 normalizer는 `danawa-silver-1.0.2`, 품질 정책은 `silver-quality-1`이다. 사용한 관련 규칙 hash도 normalizer_version에 포함한다. 같은 키의 다른 내용을 UPDATE하지 않는다. 정책/코드를 바꿀 때 버전을 올리고 새 평가로 보존한다.

## 실행 전 필요한 입력

1. **고정 Bronze Delta version**: 현재 ingest의 raw ID·본문/hash를 검증할 수 있는 원천 테이블이다. `--bronze-version`을 명시한다.
2. **담당자 입력 묶음**: `config/silver_inputs.example.json`의 mappings/reviews/links/rules 4개 snapshot을 모두 명시한다. 미제공은 NOT_PROVIDED이며 승인·유효 매핑·빈 정상 데이터로 해석하지 않는다.
3. **NORMALIZATION 실행 등록 증거**: `config/silver_registration.example.json`은 의도적으로 NOT_REGISTERED라 실행할 수 없다. 서비스 담당이 실제 pipeline_runs에 등록한 UUID의 export를 제공해야 한다.
4. **Mongo 상태 읽기 계정**: `PIPELINE_MONGO_URI`, rs-products 3멤버, ingestion_observations의 primary/majority find 권한. raw/이미지 운영 원장을 Silver가 UPDATE하지 않는다.
5. **독립 작성자 잠금 계정**: `SILVER_LEASE_MONGO_URI`, 별도 ops DB의 공용 lease collection. 미리 생성한 unsharded·TTL 없는 컬렉션에 listIndexes/find/insert/update/remove 권한을 부여한다.
6. **Spark·공용 저장소**: Python/Java/Spark/Delta 및 S3A JAR·권한은 기존 배포 계약을 따른다. Silver는 Kafka를 직접 읽지 않는다.

실행 등록 입력은 `{processing_run_id, stage:NORMALIZATION, status:REGISTERED, provider:MYSQL_PIPELINE_RUNS_EXPORT, evidence_ref}`다. **현재 어댑터는 담당자가 내보낸 등록 근거를 검증·보관하며 MySQL에 직접 등록하거나 실시간 조회하지 않는다.** 필드명을 수동으로 REGISTERED로 바꾸는 행위를 실제 등록으로 간주하면 안 된다. 등록 export를 제공받지 못하면 운영 실행을 중단한다. MySQL 등록 API/DDL 연동은 담당 계약 인수 후 추가할 수 있다.

## 담당자 snapshot 포장

각 snapshot은 contract_version=1, kind, status(PROVIDED/NOT_PROVIDED), rows, provider_version, evidence_ref, content_hash다. content_hash는 자신을 제외한 객체의 pipeline H다. `input_snapshot()`이 이 포장을 생성한다. 실제 입력의 내용/부재·버전·근거를 보존하며 captured_at 같은 진단값을 임의로 추가하면 기존 hash 검증에 실패한다.

| kind | 행의 필수 근거 |
|---|---|
| mappings | source_code, source_category_codes(문자열 배열), service_category_id(UUID), service_category_code, active/excluded(bool). 선택 valid_from/expires_at |
| reviews | source_code, source_product_id, source_version, checksum, scopes, decision. APPROVED에는 권리 근거 evidence_ref 필수. 선택 유효 범위 |
| links | source_code, source_product_id, url, kind, status. VALID는 checked_at/expires_at/evidence_ref 필수 |
| rules | type=AXIS_RULE/ATTRIBUTE_ALIAS/OFFER_IDENTITY와 해당 행의 적용 근거. 담당자 version/evidence를 포함한 snapshot |

축 규칙은 source_name, axes, measurement_target, option_status를 명시한다. `PRODUCT + [WIDTH,DEPTH,HEIGHT] + NO_OPTION_CONFIRMED`가 확인된 단일값만 상품 전체 치수에 투영한다. 범위·좌판·포장·매트리스·세로 축 미확인은 원문과 속성 상태로 보존한다. `ATTRIBUTE_ALIAS`는 attribute_name/raw_value/codes를 제공한다. 사전 없이 색상·재질·형태 코드를 만들지 않는다.

Offer 규칙은 `verified_stable=true`, `seller_id_field=seller_id`, `offer_id_field=source_offer_id`, version/evidence_ref 및 확인된 옵션을 요구한다. 실제 seller 행에 안정 source_offer_id가 있어야 한다. bridge link_pcode·배열 위치·가격을 대신 사용하지 않는다. 옵션 없음은 담당 규칙의 명시적 NO_OPTION_CONFIRMED와 option_key를 사용한다. 미확인 판매 항목의 가격/URL은 보존하되 normalized_offers는 만들지 않는다.

관련 관측의 행만 의미 hash에 넣는다. 무관한 매핑 행·전역 provider_version만 바뀌면 평가를 다시 만들지 않는다. PROVIDED 빈 snapshot과 NOT_PROVIDED는 서로 다른 증거다. 검수/링크의 만료·철회·미래 유효 상태는 실행 시작의 고정 as_of에서 판정하며 상태 전이 때 새 평가가 된다.

## 서버 실행

Spark Driver 서버에서 기존 `requirements-bronze.lock.txt`(동일 Spark/Delta 의존성)를 설치하고 패키지를 설치한다. console entry `danawa-silver` 또는 파일 `run_silver.py`를 사용한다. 설정 예시는 `config/silver.example.json`이다. image bucket과 Delta lake를 혼동하지 않는다.

`SILVER_SPARK_OPTIONS`에는 `spark.master`, 필요한 사전 배치 `spark.jars`, S3A endpoint·TLS·credential provider 등의 `spark.*` 문자열 설정을 JSON으로 주입한다. 비밀은 Git이나 명령 인수에 넣지 않는다. Driver/Executor 모두 lake에 접근해야 하며 Driver는 Mongo에도 접근해야 한다.

Windows 로컬 직접 실행은 Java/Hadoop 경로와 `PYSPARK_PYTHON`을 해당 가상환경 Python 절대 경로로 설정한다. Spark의 기본 secret/password/token/access key redaction을 사용하며 Windows batch launcher에 미이스케이프 `|` 정규식을 startup 인자로 주입하지 않는다. 로컬 smoke에서 생성한 cli-fixtures는 전부 FIXTURE이며 운영 등록 근거로 재사용하지 않는다.

```bash
cd /opt/mlo-01-p3-team3/src/data_pipeline
spark-submit --master spark://SPARK_MASTER:7077 --deploy-mode client \
  run_silver.py --config config/silver.local.json \
  --bronze-version 12 --inputs /srv/pipeline/inputs/owners-001.json \
  --registration /srv/pipeline/inputs/normalization-run-001.json \
  --state-dir /srv/pipeline/logs/silver --max-observations 3
```

실제 서버 첫 시험은 소량으로 한다. 설정·출력 대조 후 `--max-observations`를 생략하면 고정 Bronze 전체를 처리한다. 기본 페이지는 최대 20개/32 MiB다. Driver에 전체 JSON 원문을 collect하지 않고 작은 ID/바이트 투영으로 페이지를 정한 뒤 해당 원천만 읽는다. 65,498건의 처리 시간/서버 메모리 인수는 아직 수행하지 않았으므로 전체 실행 성능을 보장하지 않는다.

rehearsal은 `--rehearse --image-states STATE_EXPORT.json`으로 명시한다. 세 경로 모두 file URI이며 등록 상태가 FIXTURE여야 한다. Mongo 장애를 파일 상태로 자동 대체하지 않는다. 운영에서는 이 옵션과 fixture 등록을 거부한다. 테스트 smoke의 lease는 대역이며 실제 운영 잠금 검증으로 표시하지 않는다.

## 저장·재평가·재개

각 원천은 상품 키, Raw ID, source 관측 키, processing_input_fingerprint, evaluation_key를 구분한다. Decimal(20,4)와 mm, UTC timestamp를 사용하며 원문 값·JSON pointer·관측 시각을 별도 보존한다. 처리 시각을 가격/규격 시각으로 대체하지 않는다. `dates.register=18.02.`는 세기를 추정하지 않고 AMBIGUOUS로 남기며 make는 제조일로 승격하지 않는다.

SOURCE 최소가격·TOTAL_REPORTED·MEMBER_REPORTED·의미 미확정 가격·판매 항목 가격은 별도 행이다. 숫자/표시값 충돌, 0·음수·소수 KRW, 미확인 통화를 구분한다. 대표가격 선택이나 환산을 수행하지 않는다. 조건 미확인과 옵션 미수집을 무료배송·옵션 없음으로 바꾸지 않는다.

이미지의 원본 URL·관측 URL·순서·역할·source_version·checksum·객체 키·로컬 확보와 원격 상태·검수 상태를 분리한다. 같은 revision에 다른 vector/fingerprint는 충돌이다. 이미지 입력이 처리 중 바뀌거나 DB 조회가 실패하면 receipt를 만들지 않는다. 임의의 asset UUID나 추천 승인을 생성하지 않는다.

출력 테이블을 먼저 commit하고 실제 Delta version·행 키/hash를 대조한 뒤 pipeline_evaluations를 마지막에 쓴다. 중간 종료는 같은 명령으로 재개한다. 기존 출력은 insert-only로 재사용하며 최초 processing_run_id/processed_at을 유지한다. receipt가 있어도 고정 version 및 현재 출력의 키/hash를 대조한 뒤 skip한다. 삭제/변조된 출력을 성공으로 건너뛰지 않는다.

공유 checkpoint에는 대상 fingerprint, 내용 주소 입력 snapshot·사용한 이미지 상태, checksum 있는 discovery cursor/epoch가 있다. 발견 cursor는 완료 증거가 아니다. 부분 실행은 마지막 안전 위치부터 이어가고, 한 순환이 끝난 다음 실행은 처음부터 대조한다. 순회 위치 이전의 상태 변경은 다음 순환에서 발견한다. 다른 Bronze version은 새 범위로 시작하며 기존 평가 receipt로 중복을 제거한다. 이 방식은 한 고정 Bronze 범위의 순환 대조이고 Mongo 원장 전체를 별도 timestamp 필터로 스캔하는 방식이 아니다.

Silver는 전체 root와 checkpoint를 공용 Mongo lease로 묶는다. 모든 Driver가 동일 ops namespace·정규화 URI를 사용해야 한다. 만료 자동 takeover는 하지 않는다. 이전 Driver/Spark 작업 종료를 확인한 뒤 token을 대조해 수동 해제하는 [Bronze 공통 잠금 절차](bronze_execution.md)를 따른다. 저장소의 진행 중 commit을 즉시 회수하는 분산 fence는 아니다.

정상 처리=exit 0, 실패=2, 중단=130이다. QUALITY_WAIT는 정규화 출력이 보존됐으나 특정 CATALOG/RECOMMENDATION/COMPARISON/PURCHASE 범위가 대기라는 뜻이며 모든 필드 무효 또는 승인 완료가 아니다. `state-dir/runs/*.json`과 stdout의 원천/평가/실패 코드·건수·duration_ms를 확인한다. 정상 종료 후 NiFi가 같은 단발 명령을 5분 간격으로 호출할 수 있지만 실제 일정 활성화는 서버 인수 후 수행한다.

## 검증 상태

단위·장애 시험은 `python -m unittest discover -s tests -v`, 실제 로컬 Delta 시험은 다음과 같다. 새 출력 폴더를 사용하며 원본을 변경하지 않는다.

```bash
python -m tests.smoke_silver_delta \
  --source-rehearsal /path/to/saved-three-product-rehearsal \
  --output .runtime/silver-delta-new
```

테스트는 저장된 3상품·9이미지 관계를 사용한다. 원천 확인·가격/치수/옵션/등록월·검수/링크 만료·입력 변경·충돌·출력 후 receipt 전 중단·반복 평가를 검증한다. 매핑/검수 미제공의 실제 표본은 QUALITY_WAIT가 정상이다. 실제 Mongo/lake/Spark VM, NORMALIZATION 등록 export 인수, Linux/AWS 이전, 대규모 부하는 미검증이다. Gold와 MySQL 게시도 미구현이다.

2026-10-17 실제 로컬 Spark 4.0.1/Delta 4.0.1 결과: `.runtime/silver-delta-v3/report.json`, passed=true, 상품 관측 3·속성 14·가격 42·이미지 관계 9·링크 27·품질 문제 94·확인된 Offer 0·평가 receipt 3. 모두 QUALITY_WAIT이며 같은 입력의 3개 평가를 재사용했다. 외부 데이터 서비스 연결은 0, 운영 lease는 대역이다. 별도 `run_silver.py` 실행도 `.runtime/silver-cli-v2.log` 및 같은 출력의 `cli-logs/runs`에 COMPLETED/processed=3/reused=3/source_drained=true를 남겼다. Windows JVM 종료 시 잠긴 임시 JAR 삭제 경고는 로그에 남지만 종료 코드와 데이터/receipt 검증은 통과했다.

최종 자동 회귀 시험은 **145개 통과 / 70.951초**다(`.runtime/silver-unit-tests.txt`). Silver 관련 23개는 순수 정규화·입력·실행 차단·부분 출력·이미지 revision·발견 cursor 손상·불완전 입력 archive 복구 등을 검증한다. 실제 데이터 서비스와의 종단 시험으로 표시하지 않는다. 시험 산출물은 Git 제외 경로에 보관하며 코드와 재현 명령은 저장소에 남긴다.
