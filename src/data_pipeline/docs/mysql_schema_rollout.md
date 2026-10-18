# MySQL 게시 계약 v2 반영·인수

상품 파이프라인의 기존 MySQL 게시를 확장된 Gold 필드 계약과 맞춘다. Gold 생성 정책·Delta 스키마·크롤러·추천 코드·운영자 승인 원장은 변경하지 않는다. MongoDB 원천 적재와는 별도 단계다.

## 코드와 전달물

현재 계약은 `document-publication-v2`, hash는 `b09eee8814c9f602b55a1429f8883f6ccf11c955f4dda319384d065dab444b0a`다. 13테이블·160컬럼, PK 13개·UNIQUE 12개·FK 9개를 검사한다. 기존 기본 10테이블에 24컬럼과 업무 테이블 3개를 추가하는 계약이다.

| 파일/명령 | 역할 |
|---|---|
| [publication_reference_v2.sql](../sql/publication_reference_v2.sql) | 새 격리 DB 전체 생성용 참조. 코드 생성 결과와 동일 |
| [feature25_apply.sql](../sql/migrations/feature25_apply.sql) | 기존 기본 10테이블의 확장. 빈 서버 초기 구축기가 아님 |
| [feature25_rollback.sql](../sql/migrations/feature25_rollback.sql) | 이 migration이 추가한 구조만 조건부 제거 |
| [manifest.json](../sql/migrations/manifest.json) | 인계 파일 바이트 hash와 LF 정규화 hash·코드 계약·migration 정체성 |
| `migration-check` | 파일 무결성과 컬럼/PK/UNIQUE/FK 대조. DB 연결·SQL 실행 없음 |
| `schema-diff` / `preflight` | 실제 DB의 읽기 전용 차이 보고 / 필수 계약 충족 검사 |
| `prepare` / `publish` / `audit` | 고정 Gold에서 새 v2 계획 생성 / 명시 게시 / 읽기 전용 값·원문·관계 대조 |

NAS에서 받은 적용·롤백 SQL은 내용 변경 없이 편입했다. 파일 checksum은 UTF-8 BOM과 Windows/Linux 줄바꿈을 정규화해 검사한다. 다른 공백·본문 수정은 차단하며, checksum만 갱신해도 코드 계약과 다른 필드 정의는 차단한다. 향후 필드 계약이 바뀌면 SQL·manifest·스키마·계획·writer·검증을 함께 갱신해야 한다.

기존 `publication_reference_v1.sql`이라는 파일명에 v2 내용이 들어 있었으므로 파일명을 v2로 정리했다. 초기 v1 문서·과거 서버 release의 시험 기록은 당시 결과이며 새 운영 스키마 인수가 아니다. 기존 v1 계획·staging을 v2 계획으로 수정하지 말고 고정 Gold를 사용해 새 계획을 만든다.

## 운영 전 게이트

### 테이블 누락 선행 점검

인계 문서는 운영 테이블을 직접 확인하지 않았다고 명시한다. 전달된 `feature25_apply.sql`은 기본 10테이블이 존재한다는 전제의 변경 SQL이므로 빈 DB에 바로 실행하지 않는다. 실제 조회 결과 기본 테이블도 없는 현재 서버에는 전체 생성 경로가 필요하다.

| 구분 | 필요한 테이블 | 반영 경로 |
|---|---|---|
| 기존 기본 10개 | `categories`, `products`, `offers`, `offer_snapshots`, `assets`, `product_images`, `pipeline_publication_guards`, `pipeline_product_versions`, `pipeline_publications`, `pipeline_publication_staging` | 신규 DB는 v2 전체 생성 SQL, 기존 DB는 현재 구조 대조 |
| 인계 추가 3개 | `product_link_observations`, `product_embeddings`, `search_index_versions` | v2 전체 생성 SQL과 feature25 변경 SQL 모두에 포함 |
| 변경 이력 | `_feature25_schema_history` | 변경 SQL 실행 시 별도 관리. 업무 13테이블 수에 포함하지 않음 |

추가 3테이블은 코드 검사에서 생략하지 않는다. 전체 생성 SQL과 `schema.py`의 동일성 시험, 변경 SQL의 160컬럼·13 PK·12 UNIQUE·9 FK 대조, 실제 로컬 MySQL preflight를 통과했다. 임베딩·인덱스 구조를 생성해도 해당 담당자의 데이터 등록·활성화는 수행하지 않는다. 사용자·추천 등 다른 서비스의 전체 DB를 이 13테이블로 대체한다는 뜻도 아니다.

사용자는 업무 DB명을 `pj3_products`로 승인했으며 승인 자료는 검증용으로만 생성하도록 확정했다. 2026-10-18 기본/추가 테이블 대조 후 `pj3_products`와 `pj3_products_validation`에 v2 전체 생성 SQL을 적용했다. 업무 DB에는 상품·승인을 입력하지 않았다. 기존 MySQL01의 TLS 인증서는 `pj3-mysql01`, `192.168.0.131` SAN을 포함하고 `require_secure_transport=ON`이며 기존 CA를 사용해 실제 인증서·호스트 검증을 통과했다. 상세 경로·결과는 아래 서버 반영 항목을 따른다.

2026-10-18 MySQL01의 information_schema를 읽기 전용으로 조회했을 때 확인된 비시스템 테이블은 아래 시험용 probe뿐이었다.

- `pj3_stage3_smoke_1792233317119928873.probe`
- `pj3_stage3_app.probe`
- `pj3_storage_verify_8b84d1.probe`
- `pj3_replication_verify_8b84d1.probe`

최초 조회 당시 게시 대상 기본 테이블 10개를 가진 업무 DB는 없었다. 기존 시험 DB를 서비스 대상으로 재사용하지 않았다. 이후 사용자가 승인한 새 DB에만 문서 계약 스키마를 구성했다. 다른 시점/권한/서버의 모든 임의 파일에 ORM·DDL이 없다는 증명은 아니다.

실제 적용 전에는 다음이 필요하다.

1. 서비스 담당자가 확정한 업무 DB명·Primary·ORM/DDL·보호 접속 설정. 기본 테이블이 없으면 초기 서비스 스키마 구축을 별도 확정한다.
2. 기존 상품/이미지/판매 UUID와 행 수·관측 baseline. 기존 v1 데이터에 추가 필수값을 채울 고정 Gold·출처 대응.
3. 카테고리 매핑·이미지 검수·링크 판정·NORMALIZATION/GOLD_BUILD 등록·게시 guard의 담당 확정 입력.
4. 쓰기 중지 범위·백업/복구 위치·담당자·새 code release/venv와 실제 스키마의 동시 전환 절차.

값이 없는 상태를 빈 JSON·가짜 원문 참조·임의 승인으로 backfill하지 않는다. 임베딩과 인덱스 테이블은 게시 계약의 구조 검사 대상이지만 결과 등록/활성화는 별도 담당자 책임이다.

## 점검·반영 순서

파이프라인 디렉터리에서 서버 전용 Python을 사용한다. 아래 설정 변수는 담당자가 확정한 보호 파일 경로이며 새 기본값을 가정하지 않는다.

```bash
python run_publication.py migration-check
python run_publication.py schema-diff --config "$MYSQL_CONFIG" --output "$NEW_SCHEMA_DIFF"
```

`migration-check`의 성공은 `MYSQL_MIGRATION_BUNDLE_VERIFIED_NOT_APPLIED`다. `schema-diff`는 실제 차이 보고이며 표준 SQL 변경을 자동 생성/적용하지 않는다. 적용 SQL 자체는 임시 테이블과 helper procedure를 만들므로 읽기 전용 점검 명령으로 사용하지 않는다.

실제 스키마와 데이터 보완이 확정된 뒤에만, 쓰기 중지·백업을 확인한 운영자가 아래 절차를 수행한다. `MYSQL_ADMIN_CNF`는 별도 DDL 담당자의 제한 권한 접속 파일이다. 게시 프로그램에 관리자 자격을 전달하지 않는다.

```bash
umask 077
mysqldump --defaults-extra-file="$MYSQL_ADMIN_CNF" --single-transaction \
  --routines --triggers --events --hex-blob --no-tablespaces \
  --set-gtid-purged=OFF --databases "$DB" > "$BACKUP"
# 백업 성공과 복구 가능성 확인 후에만 다음 명령 실행
mysql --defaults-extra-file="$MYSQL_ADMIN_CNF" --default-character-set=utf8mb4 \
  --show-warnings --database="$DB" < sql/migrations/feature25_apply.sql
python run_publication.py preflight --config "$MYSQL_CONFIG"
```

SQL 성공 `MYSQL_SCHEMA_MATCHED_DATA_NOT_PUBLISHED`와 preflight 성공 `MYSQL_SCHEMA_VERIFIED`는 상품 게시 완료가 아니다. 승인된 환경과 고정 Gold에 대해 새 v2 계획을 준비한 뒤 명시 `publish --apply`와 `audit`을 수행한다. 정확한 Gold 입력·guard 인자는 [게시 실행](mysql_publication.md)을 따른다. 운영 DB에 `--local-test`를 사용하지 않는다.

`BACKFILL_REQUIRED_...`는 업무 테이블 변경 전에 중단하는 조건이다. 실제 원천을 확보해 검토한 보완 절차를 먼저 수행한다. 기존 사용자/서비스 전용 필수 컬럼·다른 FK·타입·NULL·정렬 규칙이 있으면 schema-diff 결과를 인수하고 어댑터를 맞춘다. 검사 조건을 무조건 완화하지 않는다.

## 중단·롤백

`_feature25_schema_history`는 적용 전 구조와 중간/완료 상태를 기록한다. 연결 유실이나 중간 DDL 실패는 같은 원본 SQL로 재실행하여 상태를 확인한다. 이 이력과 백업을 삭제하지 않는다.

```bash
mysql --defaults-extra-file="$MYSQL_ADMIN_CNF" --default-character-set=utf8mb4 \
  --show-warnings --database="$DB" < sql/migrations/feature25_rollback.sql
```

새 컬럼에 SQL NULL 이외의 값이나 새 테이블의 행이 있으면 롤백을 거부한다. 게시 이후 데이터를 삭제하면서 복구하는 기능은 아니다. 적용 후 외부 스키마 변경도 자동 롤백을 막는다. MySQL DDL은 일반 `ROLLBACK`으로 되돌아가지 않는다. `--force`나 `FOREIGN_KEY_CHECKS=0`으로 우회하지 않는다.

이미 작업을 수행한 운영 v1 queue·receipt·계획·UUID를 재작성하거나 지우지 않는다. 새 소스 release를 별도 배포하고 작업별 snapshot/상태/계약 버전을 확인해 전환한다. 이번 보완은 기존 MongoDB 원천 작업의 source·venv·fingerprint를 전환하지 않는다.

## 재현 가능한 격리 검증

자동 계약/회귀 시험과 실제 격리 SQL 시험을 구분한다.

```bash
python -m unittest discover -s tests -p 'test_publication*.py' -q
python -m tests.smoke_publication_migration --mysqld /path/to/mysqld --output NEW_MIGRATION_RESULT
python -m tests.smoke_publication_mysql --mysqld /path/to/mysqld --output NEW_PUBLICATION_RESULT
```

두 smoke 실행기는 새 data directory와 loopback 임시 포트의 자체 mysqld만 시작/종료한다. 기존 Windows 서비스·VM·운영 DB에 접속하지 않는다. 적용/롤백은 실제 mysql 클라이언트의 파일 입력 방식으로 수행한다. 결과 JSON과 로그를 보존하며 실제 운영 DDL·TLS·업무 승인 인수를 대신하지 않는다.

## 신규 VM 스키마와 TLS 반영 결과

이 절은 위 최초 조회 이후의 승인된 변경이다. 기존 업무 DB 변경 SQL을 빈 DB에 실행하지 않고 [신규 DB 준비 모듈](server_setup/pipeline_mysql_bootstrap.py)로 전체 참조 DDL을 적용했다. 기존 DB·계정이 있으면 거부하고, 비밀번호를 서버에서 생성하여 root 전용 0600 journal에 보존한다. 중간 실패는 임의 삭제/재생성하지 않으며 journal을 확인한다. 이 모듈은 Ansible의 root 작업이며 디렉터리의 다른 검증 코드를 일괄 실행하는 도구가 아니다.

- 서버: `pj3-mysql01`, `192.168.0.131:3306`, MySQL `8.4.11-0ubuntu0.26.04.1`, 쓰기 가능한 Primary.
- 업무 DB: `pj3_products`, 논리 target `pj3-products-service-v2`. 상품 0건·승인 0건을 확인했다.
- 합성 승인 시험 DB: `pj3_products_validation`, target `pj3-products-validation-v2`. 상품 1건·이미지 연결 2건·asset 1건·링크 관측 2건. 실제 수집 데이터/이용 승인이 아니다.
- 추가 3테이블을 포함한 각 13테이블·160컬럼, 코드 계약 preflight 통과. MySQL02/03에도 구조·계정·검증 데이터가 복제됐으며 두 노드는 read_only/super_read_only 상태를 유지한다.
- 게시 계정: `pj3_pipeline_publisher`@`192.168.0.121`. `REQUIRE SSL`. 모든 계약 테이블 SELECT, 파이프라인 기록 9테이블만 INSERT/UPDATE. 카테고리·guard·임베딩·인덱스 원장은 읽기 전용이다. DDL·승인 수정 거부와 평문 접속 거부를 실제 확인했다.
- Spark Master에서 PyMySQL의 CA·호스트명 검증과 `TLS_AES_256_GCM_SHA384`를 확인했다. 새 CA 발급·TLS 완화·서버 재시작은 필요하지 않았다.
- 게시 `COMPLETED` → 동일 계획 `REUSED`(같은 publication/상품 UUID) → audit `VERIFIED`. 이 시험은 고정 합성 Gold/승인 입력이며 실제 Gold export·서비스 ORM 인수와 구분한다.

실서버에서 guard의 `FOR UPDATE`가 SELECT 전용 계정에 거부되는 문제를 발견했다. guard·category는 `FOR SHARE`로 변경해 소유자 변경을 게시 트랜잭션 종료까지 막는다. 상품/게시 원장 잠금은 `FOR UPDATE`를 유지한다. 승인 UPDATE 권한을 추가하지 않았으며 실제 재시험으로 해결을 확인했다. [MySQL 잠금 읽기 규칙](https://dev.mysql.com/doc/refman/8.4/en/innodb-locking-reads.html)을 따른다.

보호 설정은 Master의 `/var/lib/pj3/product-pipeline/spark/mysql-publication-v2-20261018/`에 있다. `bootstrap.private.json`은 비밀번호를 포함하며 `pj3-spark` 소유 0600, 디렉터리 0700이다. `mysql-publication.json`/`mysql-validation.json`은 비밀 없는 환경변수 참조 설정, `ca.pem`은 신뢰 CA다. 계정 비밀번호는 보호 JSON의 `password`, 접속 계정/허용 IP는 `identity.user`/`identity.client_host`에 있다. 파이프라인 실행 시 이 비밀번호와 CA를 `PIPELINE_MYSQL_PASSWORD`/`PIPELINE_MYSQL_CA`에 주입한다. 터미널에 출력하거나 Git/NAS에 복사하지 않는다.

사용자에게는 로컬 `%LOCALAPPDATA%/Team3Pipeline/mysql-publication-v2-20261018/mysql-publisher.private.json`과 CA를 별도 전달했다. 해당 폴더·비밀번호 파일 ACL은 현재 사용자만 허용한다. 파일은 Git 대상이 아니다. 이 계정은 Master IP만 허용하므로 PC에서 직접 연결하는 계정으로 사용하지 않는다.

서버 검증 소스는 `/opt/pj3/product-pipeline/verification/mysql-v2-20261018`, 수정본 archive SHA256은 `bb617c430b8bb1196fa38d2ef57d136c622e433dc8273e12e3f243e683077dcd`다. 기존 운영 release·NiFi 일정·원천 적재 환경을 전환하지 않았다. [TLS 실행기](../tests/smoke_publication_tls.py)의 prepare는 새 합성 입력 생성용이며 이미 게시된 fixture를 다시 prepare/seed하지 않는다. publish 단계는 보존된 계획으로 재실행할 수 있다.

공개 증거는 [TLS/권한/스키마](verification/publication_v2_20261018/tls-prepare-result.json), [게시·재게시·감사](verification/publication_v2_20261018/tls-publish-result.json), [3노드 구조·권한 조회](verification/publication_v2_20261018/mysql-v2-cluster-audit.txt), [로컬 migration 16건](verification/publication_v2_20261018/migration-local.json), [로컬 게시 12건](verification/publication_v2_20261018/publication-local.json), [커밋 범위 회귀 266건](verification/publication_v2_20261018/regression.txt)이다. 실제 VM에서 migration 16개 장애 시나리오 전체를 반복한 것은 아니며 새 DB/TLS/소량 게시를 검증했다.

전체 생성 경로에는 feature25 변경 이력이 없으므로 변경 SQL의 rollback으로 신규 DB를 제거하지 않는다. 운영 원천에 대한 실제 카테고리/이미지/링크 승인, 실제 고정 Gold 계획, 외부 서비스 ORM·기존 UUID baseline 인수는 여전히 별도다. 검증 승인을 업무 DB에 복사하지 않는다.
