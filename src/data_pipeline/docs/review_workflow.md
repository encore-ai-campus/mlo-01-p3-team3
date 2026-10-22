# 검수 등록과 승인 게시

## 완료 범위와 책임

### 상시 내부 프로젝트 승인 정책 — 2026-10-19

운영자가 파이프라인에서 이후 입력에도 내부 프로젝트 승인을 자동 적용하도록 지시했다. `catalog_policy.py`의 버전·profile hash·운영자 요청 참조를 고정하고, 서버의 별도 reviewer 계정으로 CATEGORY/MAPPING/PRODUCT/POLICY/IMAGE를 등록한다. 기존 문서의 담당 원장 읽기 경계를 확장한 명시적 운영 정책이며 일반 publisher의 검수 원장 수정 권한은 확대하지 않는다.

원천과 `danawa_products`의 상품·실행·hash·카테고리·갤러리 대응을 먼저 검증한다. 문서 포함 품목과 일치하는 단일 분류만 확정하고 혼합·부품·불명확 품목은 보류한다. 서비스 분류 ID는 원천 코드가 아닌 서비스 분류 코드 기준으로 고정한다. 가격 이력은 SNAPSHOT_V1, 확인 전 대표 판매가격 종류는 빈 목록이다.

파일이 AVAILABLE이고 checksum·크기·해상도와 상품/원천 버전 연결이 확인된 이미지에 STORAGE/DISPLAY/EMBEDDING/RECOMMENDATION/ANALYSIS의 내부 이용 승인을 기록한다. 생성 이용은 제외한다. 이용 승인과 실제 임베딩 생성·인덱스 활성화는 별개이며 후자는 실행하거나 완료로 표시하지 않는다. 기존 REVOKED/REJECTED와 다른 담당자 결정은 자동 덮어쓰지 않는다.

신규 epoch의 Silver 입력 고정 전에 최대 500상품씩 원장을 대조하고 변경분만 등록한다. 재실행은 기존 유효 결정을 재사용하며 순회 cursor·동시 실행 잠금·단계별 묶음/receipt·원천 근거는 서버 실행 루트의 `catalog-review/`에 보존한다. 고정 Gold와 원장 버전을 재대조한 후 `publish_reviewed`가 guard·활성화·게시 audit를 수행한다. 원장 승인 성공, 실제 ACTIVE 반영, 추천 준비 완료를 각각 따로 집계한다.

`run_review.py`는 검토 초안, 명시적 결정 등록, Silver/Gold 입력 export, 고정 Gold에 대한 게시 승인, 활성화를 연결한다. 일반 적재 계정과 별도인 검수 담당 계정으로 실행한다. 크롤러 요청·이미지 다운로드·임베딩·추천 실행은 하지 않는다.

BRD의 현재 범위는 완제품 가구·생활용품이며 소형 소품·소모품·부품·원재료와 모호한 대상을 구분한다. `docs/BRD_3차.md`, `docs/PRD_3차_초안.md`, 상품·상품 링크 데이터 사전이 기준이다. `CHAIR` 등 문서의 예시는 확정 서비스 분류 목록이 아니다. 실제 분류 UUID·매핑과 이미지 권리 근거는 담당자가 확정해야 한다. 보관·표시·추천·임베딩·분석의 승인을 구분한다.

수동 도구는 명시적 입력만 등록한다. 현재 서버 coordinator의 자동 등록은 위 내부 프로젝트 정책과 reviewer에 한정하며 품질 충돌·기존 거절 결정을 덮어쓰지 않는다. 검수 대기 초기 적재와 기존 UUID를 보존한다.

## 계정·사전 준비

1. `sql/review_workflow.sql`을 검토한 후 관리자가 명시적으로 적용한다. 이미 존재하는 테이블은 스키마를 먼저 비교한다. 런타임은 DDL을 실행하지 않는다.
2. 별도 검수 계정은 상품/이미지/공통 자산·카테고리·매핑·게시 승인에 한정한 권한을 갖는다. `pipeline_review_events`는 SELECT/INSERT만 부여해 수정·삭제를 금지한다.
3. 일반 게시 계정에는 검수 원장·매핑 SELECT만 추가한다. 승인 원장 변경 권한은 부여하지 않는다.
4. MySQL 설정은 기존 게시 설정 형식이며 검수 전용 `user`, `password_env`를 사용한다. 비밀번호는 환경변수, 공개 CA는 `PIPELINE_MYSQL_CA`로 주입하고 인증서·호스트명을 검증한다. 보호 파일은 Git에 넣지 않는다.

서버 준비 도구는 `docs/server_setup/review_mysql_prepare.py`(기본 check, apply 명시)와 `review_spark_prepare.py`다. MySQL writable primary 하나에만 원장을 만들며 사용자·원장 DDL은 정상 복제로 전파된다. 업무 승인·guard 행은 만들지 않는다.

## 절차

아래 명령은 `src/data_pipeline`에서 실행한다. `owner.json`은 보호된 검수 접속 설정이다. Spark를 읽는 명령은 서버의 확정 Spark/Delta 런타임과 `PUBLICATION_SPARK_OPTIONS`·저장소 인증을 사용한다.

```text
python run_review.py preflight --config owner.json
python run_review.py inventory --config owner.json --limit 100 --output inventory.json
python run_review.py template --config owner.json --limit 100 --output review-template.json
python run_review.py draft --config owner.json --requests requests.json --output draft.json
```

`requests.json`은 `{kind,payload}` 목록이다. kind는 CATEGORY, MAPPING, PRODUCT, IMAGE, POLICY다. 초안에는 항상 PENDING을 넣으며 실제 현재 값의 hash와 이전 원장 revision을 고정한다.

`template`은 실제 상품·이미지 ID, 원천 버전과 checksum을 채운 DRAFT를 만든다. 카테고리·이용 범위·권리 근거·가격 정책은 미확정으로 비워 둔다. 담당자가 CATEGORY/MAPPING을 먼저 확정·등록한 뒤 PRODUCT/IMAGE/POLICY를 검토한다. 비어 있는 값이나 PENDING을 APPROVED로 일괄 치환하면 안 된다. 한 묶음은 1,000개 항목·32MiB 이하이며 큰 대상은 limit을 줄여 분할한다.

| 종류 | payload 필수 내용 |
|---|---|
| CATEGORY | 확정 category_id(UUID), code, name, active, recognition_enabled |
| MAPPING | source_code=danawa, source_category_codes(문자열 경로 배열), service_category_id, mapping_version, active, excluded |
| PRODUCT | product_key, service_category_id, active, supported, applicable_attributes, 검토한 raw_document_ids |
| IMAGE | image_id, source_version, checksum, scopes, permission_evidence_ref, asset_policy |
| POLICY | price_history_mode(SNAPSHOT_V1 또는 PRICE_EFFECT_V2), price_max_age_seconds, representative_price_kinds |

IMAGE의 asset_policy에는 decision=APPROVED, evidence_ref, allowed_scopes, denied_scopes가 필요하다. 링크 승인 scopes를 공통 파일이 허용해야 한다. 공통 파일 승인만으로 다른 상품의 링크가 승인되지는 않는다.

담당자가 draft를 검토해 `status=REVIEWED`, actor_ref·criteria_ref·evidence_ref, reviewed_at·expires_at과 항목별 decision을 입력한다. `seal`은 검토 근거를 생성하지 않으며 내용 hash만 고정한다.

```text
python run_review.py seal --bundle reviewed.json --output sealed.json
python run_review.py validate --bundle sealed.json
python run_review.py apply --config owner.json --bundle sealed.json --apply
```

동일 결정은 재사용한다. 수정된 본문·오래된 revision·바뀐 상품/이미지 원천은 거절한다. 묶음 전체를 하나의 트랜잭션으로 처리한다. 새 검수 결정은 이전에 준비된 게시 승인을 무효화하므로 다시 평가하고 guard를 만들어야 한다.

## Silver·Gold 재평가

```text
python run_review.py export-inputs --config owner.json --silver-config gold-config.json --silver-versions silver-versions.json --output review-export.json
```

`silver-config`는 Silver를 입력으로 읽는 기존 GoldSettings 설정이다. 결과의 `silver`를 Silver `--inputs`로 사용한다. 해당 NORMALIZATION은 기존 `run_registration.py`로 실제 등록한다. 고정 Bronze를 다시 평가해 검수·매핑이 반영된 새 Silver를 생성하고, 새 Silver 버전을 고정한 뒤 export-inputs를 다시 실행한다. 두 번째 결과의 `gold`를 Gold `--inputs`로 사용하고 실제 GOLD_BUILD를 등록·실행한다. JSON 최상위 wrapper 전체를 Silver/Gold CLI에 넘기면 안 된다.

원장 export는 실제 Silver의 mapping_version과 검토한 raw ID를 대조한다. 현재 검수 입력과 정확히 대응하는 확정 Silver receipt만 evaluation_selection에 넣는다. 복수 후보를 임의로 최신이라고 고르지 않는다. 바이트가 확인된 갤러리가 하나일 때만 선택하며 이용 승인은 별도다. 임베딩·링크 판정 등이 없으면 NOT_PROVIDED로 유지한다.

서버의 연속 처리 worker도 원장 존재 시 같은 export 함수를 사용한다. 각 epoch의 입력 파일은 한 번 고정하고 재시도 때 재사용한다. 검수 이후 변경은 다음 평가에서 반영하며, 승인된 상품과 이미지 검토 값을 일반 검수 대기 적재가 덮어쓰지 않는다.

## 게시 승인과 활성화

```text
python run_review.py guard --config owner.json --gold-export <완료 export 참조> --gold-root <Gold 경로> --archive-root <Gold checkpoint 경로> --product-key danawa:<ID> --output activation-plan.json --apply
python run_review.py activate --config owner.json --plan activation-plan.json --apply
```

guard는 고정된 Gold, 현재 검수 원장 집합, 상품 원천 ID, 카테고리, 이미지 이용 근거·checksum·source_version·공통 자산 정책과 유효기간을 대조한다. 이전 입력으로 만든 Gold는 다시 평가해야 한다. 활성화는 상품과 이미지의 기존 UUID를 보존하고 동일 트랜잭션에서 수행한다. 계획 생성 이후 원천·guard·이미지 정책이 바뀌면 거절한다. 같은 계획 재실행은 기존 receipt를 재사용한다.

ACTIVE가 되더라도 임베딩과 인덱스 등 후속 입력이 없으면 recommendation_ready는 false다. 이 도구는 추천 가능 상태를 임의로 만들지 않는다. 만료·권리 철회된 기존 서비스의 노출 해제 운영은 별도로 즉시 적용하고 새 승인을 발행하지 않는다.

## 검증 증거

`tests/test_review.py`: DRAFT·만료·변조 거절, 검수 이력 멱등성, revision 충돌, guard 감사 이력과 export 분리, 공통 파일 이용 범위, 활성화 UUID 보존.

`tests/review_mysql_fixture.py`를 `tests/smoke_publication_mysql.py`에서 실행한다. 임시 전용 MySQL에만 시험 결정을 넣고 실제 SQL로 등록 → Silver → Gold → guard → 활성화 → audit, 중도 실패 롤백, 재활성화 멱등성, 이전 승인 차단을 확인한다. 업무 DB 승인은 생성하지 않는다.

2026-10-19 격리 MySQL 8.0.46 시험 통과: 실제 상품·이미지 UUID 보존, 활성화 롤백, 재실행 REUSED, 변경된 승인으로 이전 Gold 재사용 차단. 보고서는 `.runtime/review-mysql-20261019-b/report.json`이며 민감한 런타임 자료이므로 Git에는 넣지 않는다.
