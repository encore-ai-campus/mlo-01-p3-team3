# Gold → MySQL 게시 실행·인수

2026-10-18. `02-data-pipeline`, `03-data-model`의 상품·판매·이미지·UUID 계약과 `05-kafka-jobs`, `06-operations-migration`의 복구 원칙을 구현한다. **실제 서비스 DDL이 제공되지 않아 문서 기반 v1 계약을 명시적으로 채택했다.** 기존 서비스 DB와 동일하다고 주장하지 않는다. 실제 서버에서는 스키마 대조 후에만 실행한다.

## 책임과 입력

- `publication/source.py`: 완료된 Gold export의 고정 Delta 버전·선택 목록·완료 receipt·행 hash를 읽는다. 최신 테이블로 대체하지 않는다.
- `publication/plan.py`: 최대 100상품 기본값, 상한 1,000상품/32MiB의 불변 게시 계획을 만든다. source 미완료, 품질 대기, 담당 guard 미제공, 미확정 가격 이력 정책은 게시하지 않는다.
- `publication/mysql.py`: Primary 읽기/쓰기 가능 여부, InnoDB, 타입/NULL/CHAR(32) UUID, 업무 키 UNIQUE, FK, 추가 필수 컬럼을 검증한다. 실행 중 DDL을 적용하지 않는다.
- `publication/runner.py`: staging을 먼저 보존하고, 현재 담당 guard와 category/product/image 상태를 잠근 후 하나의 트랜잭션으로 해당 계획을 반영한다.

서비스 테이블은 `products`, `offers`, `offer_snapshots`, `assets`, `product_images`다. `categories`는 읽기만 한다. 명세의 상품 이미지 복합 UNIQUE는 `(product_id, source_code, source_product_id, image_variant, raw_document_id, source_version)`이다. `image_variant`에는 representative/gallery 슬롯 식별자를 보존하여 같은 파일의 여러 상품·갤러리 연결을 합치지 않는다. `offer_snapshots.pricing_terms_json`에 당시 조건을 보존한다.

새 `pipeline_publications`, `pipeline_publication_staging`, `pipeline_product_versions`, `pipeline_publication_guards`는 **제안 확장 계약**이다. 기존 서비스의 `pipeline_runs`/검수 테이블이 이미 있다는 가정으로 임의 쓰지 않는다. 실제 모델을 받으면 해당 담당과 이 확장을 채택하거나 어댑터를 맞춰야 한다. 런타임은 검사만 하고 자동 생성하지 않는다.

## 담당 guard 계약

`pipeline_publication_guards(product_key, revision, guard_json)`는 담당 시스템이 확정해 공급한다. 게시기는 이 행을 생성하거나 승인하지 않는다. JSON은 다음을 포함한다.

- `contract_version=1`, `evidence_ref`, 확정 `gold_evaluation_key`, `input_fingerprint`.
- `category_id`, `product_active=true`, 유효한 `expires_at`.
- Gold의 고정 정책과 동일한 `price_history_mode` (`SNAPSHOT_V1` 또는 `PRICE_EFFECT_V2`).
- `image_reviews[image_relation_key]`: Gold와 같은 `snapshot_hash`, 담당이 제공한 `usage_review` JSON, 기존 서비스 이미지 검수 JSON의 `current_review_hash`.

계획 입력 `guard-hashes.json`은 `{product_key: SHA256(canonical guard_json)}`다. 게시 시 DB의 현재 JSON과 다시 대조하고 `FOR UPDATE`로 잠근다. 계획 이후 검수·분류·상품 상태가 변경되면 재계획 대상으로 차단한다. EXPIRED/INACTIVE를 ETL이 ACTIVE로 돌리지 않는다.

기존 product UUID·asset UUID·image UUID·offer UUID를 업무 키로 재사용한다. 기존 product status/dedup, image active/usage_review, asset usage_policy는 덮어쓰지 않는다. **새 이미지 연결은 inactive**로 생성한다. 현재 연결 활성화와 검수 원장 관리는 담당의 후속 작업이며 ETL이 추천 승인을 대신하지 않는다. 기존 상품에 시각 기준 원장이 없다면 임의로 현재 값을 오래된 값으로 바꾸지 않고 `PUBLICATION_EXISTING_PRODUCT_BASELINE_REQUIRED`로 막는다.

## 준비와 실행

```bash
cd /opt/mlo-01-p3-team3/src/data_pipeline
python -m pip install -r requirements.lock.txt -r requirements-bronze.lock.txt -r requirements-publication.lock.txt
python -m pip install --no-deps -e .
```

`sql/publication_reference_v1.sql`은 문서 검토·격리 시험용이다. 실제 서비스에 그대로 실행하는 설치 스크립트가 아니다. 담당 DDL과 비교하고 승인된 migration으로만 준비한다. 재생성은 `python run_publication.py reference-ddl --output NEW_FILE.sql`이다. 기존 파일을 덮어쓰지 않는다.

`config/mysql_publication.example.json`을 Git 제외 설정으로 복사한다. `target_id`·DB 이름은 이관/재실행에서 유지하는 논리 식별자다. 접속 host·port는 환경별로 바꿀 수 있고 password/CA는 `PIPELINE_MYSQL_PASSWORD`, `PIPELINE_MYSQL_CA`로 주입한다. 실제 접속은 TLS 인증서·호스트명 검증을 켜며 root 계정은 사용하지 않는다. `--local-test`는 127.0.0.1/localhost의 3306이 아닌 격리 인스턴스에만 허용한다.

```bash
python run_publication.py preflight --config /etc/product-pipeline/mysql_publication.local.json

# Spark Driver/Worker가 Gold와 checkpoint archive를 읽을 수 있어야 한다.
# PUBLICATION_SPARK_OPTIONS에 Spark/Delta/S3A/인증 설정을 주입한다.
python run_publication.py prepare \
  --gold-export s3a://project-lake/checkpoints/gold/inputs/CONTENT_HASH.json \
  --gold-root s3a://project-lake/gold \
  --archive-root s3a://project-lake/checkpoints/gold \
  --guard-hashes /srv/pipeline/owner/guard-hashes.json \
  --mysql-config /etc/product-pipeline/mysql_publication.local.json \
  --output /srv/pipeline/publication-plans/batch-001 --batch-size 100

# ready.json에 등록된 개별 PLAN_ID.json만 게시한다.
python run_publication.py publish --apply \
  --config /etc/product-pipeline/mysql_publication.local.json \
  --plan /srv/pipeline/publication-plans/batch-001/PLAN_ID.json
```

`prepare`는 blocked 상품을 `ready.json`에 사유와 함께 남기며 임의 승인이나 0원 채우기를 하지 않는다. pinned inputs의 이력 정책·가격 유효기간도 계획에 고정한다. 게시 시 현재 시각 기준으로 만료된 링크/가격을 다시 차단한다.

## 중단·재실행

같은 계획을 다시 실행한다. staging의 body/hash·Gold 버전·게시 plan ID가 유지된다. 서비스 반영 도중 실패하면 해당 계획의 서비스 변경 전체가 롤백되고 staging은 남는다. COMMIT 응답 유실 때는 접속을 복구하고 같은 계획을 실행하여 `COMPLETED` receipt를 확인한다. 완료된 동일 계획은 UUID 재발급 없이 `REUSED`다.

필드별 관측 시각을 대조하여 과거 이름/규격이 최신 값을 덮어쓰지 않는다. 현재 판매 가격도 해당 가격 관측 시각으로 비교하며 같은 시각의 다른 값은 충돌이다. NULL로 검증 값을 지우지 않는다. 확인 시각이 없는 브랜드/모델의 다른 값은 자동 교체하지 않는다. 배치에 없다는 이유로 기존 상품/판매/이미지를 삭제하지 않는다.

## 검증과 한계

- `tests/test_publication.py`: 고정 계획·행 변조·담당 미제공·스키마/UUID/FK·시각 역전·미확인 가격.
- `tests/smoke_publication_mysql.py`: **실제 로컬 MySQL 8.0.46**을 별도 data directory/임의 loopback 포트로 실행한다. 기존 MySQL80 서비스나 VM DB에 접속하지 않는다. 문서 참조 DDL, 부분 쓰기 롤백, 재게시, 기존 ID, 담당 guard 변경, COMMIT 응답 유실, 현재 가격과 가격 이력/역전을 검증한다.
- `tests/smoke_publication_delta.py`: 실제 로컬 Spark/Delta에서 고정 export→계획을 검증하고 후속 Delta 행이 섞이지 않는지 확인한다. lease는 로컬 시험 대역이다.

Silver 1.0.3부터 이미지 evidence에 원천 payload hash와 원천 수집 시각을 보존한다. 기존 Silver를 수정하지 않는다. 이 근거가 없는 과거 Gold는 `PUBLICATION_IMAGE_RAW_PROVENANCE_REQUIRED`로 보류하고 원천으로 새 평가를 생성한다. wrapper document hash를 payload hash로 대신하지 않는다.

실제 서비스 DDL/권한/검수 공급자와 VM MySQL의 TLS·Primary 전환·전체 65,498상품 성능 인수는 남아 있다. 문서 계약 통과를 실제 서비스 스키마 승인으로 확대하지 않는다. 사업 데이터/승인 입력이 없는 상태에서 자동으로 예시 자료를 서버에 넣지 않는다.
