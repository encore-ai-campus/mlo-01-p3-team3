# Silver/Gold 실행 등록과 미제공 업무 입력

## 단계와 승인 경계

Silver는 고정 Bronze → 내부 Delta 정규화, Gold는 고정 Silver → 내부 Delta 변환이다. 실제 MySQL 상품 테이블 반영은 별도 publication 단계다. 실행 등록은 실행 추적 자료이며 카테고리·이미지 이용·게시 승인과 다르다.

기존 Silver/Gold CLI는 이미 독립 실행된다. 내부 변환을 위해 publication guard를 조회하거나 생성하지 않는다. 실제 매핑·검수 미제공은 NOT_PROVIDED/QUALITY_WAIT로 보존하며, 업무 게시기는 guard·현재 검수·스키마·품질을 계속 검증한다. guard 0건을 임의 승인으로 해소하지 않는다.

## 누락과 보완

기존 registration 입력 검사는 MySQL 등록 export를 요구했지만 실제 pipeline_runs 쓰기/export 어댑터가 없었다. 예제 파일의 NOT_REGISTERED를 REGISTERED로 고치는 방법은 올바른 등록이 아니다.

새 product_pipeline.registration은 다음을 제공한다.

- 실제 MySQL pipeline_runs 행 등록 후 커밋된 행으로부터 export.
- NORMALIZATION/GOLD_BUILD, UUID4, 코드 SHA256, 고정 Delta 버전 및 입력 hash 검증.
- 동일 UUID·동일 입력 재시도는 기존 행 재사용, 다른 범위·코드 재사용은 충돌.
- 완료·실패·실행 중인 행을 신규 REGISTERED 증거로 바꾸지 않음.
- 스키마·Primary 사전 검사. 런타임 DDL·사용자 권한 변경·MongoDB 접속 없음.
- 매핑 테이블·행 부재는 NOT_PROVIDED. orphan FK·중복 원천 경로·모호한 문자열 경로를 오류로 구분.

run_registration.py와 danawa-run-registration 진입점을 사용한다. MySQL 연결은 기존 publication 설정/TLS·비밀정보 환경변수 계약을 그대로 따른다. 새 등록 테이블에 대한 DB 계정 권한은 기존 승인 정책을 따르며 도구가 자동 GRANT하지 않는다.

## 운영자 실행 절차

1. 기존 DB schema·백업·현재 실행 상태를 확인한다.
2. pipeline_runs가 없다면 sql/pipeline_runs_registration.sql의 명시적 추가 migration을 검토·적용한다. 기존 테이블에는 덮어쓰거나 자동 변경하지 않는다.
3. 실제 고정 Bronze version 또는 Silver version 집합과 owner 입력 hash, 실제 배포 코드 hash, 재시도 시 유지할 UUID4로 request를 만든다.
4. preflight 후 register --apply로 실제 행을 등록하고 export JSON을 별도 신규 파일에 보존한다.
5. export를 기존 Silver/Gold --registration에 전달한다. 성공·실패 실행 결과와 등록 원장을 담당 운영자가 대조한다. 등록 어댑터만으로 실행 완료를 기록하지 않는다.
6. 운영 스케줄은 영향 확인·별도 인수 전까지 활성화하지 않는다.

NORMALIZATION의 input_range 키는 bronze_path, bronze_version, inputs_hash다. GOLD_BUILD는 silver_root, silver_versions(기존 8개 입력 테이블의 고정 version 전체), inputs_hash다. 요청 공통 필드는 processing_run_id, stage, input_range, code_version이다. 코드 버전은 소문자 SHA256 문자열이다.

매핑 export는 기존 categories/category_mappings를 읽기만 한다. source_category가 원천 코드 배열의 JSON 문자열인 경우에만 무손실로 변환한다. 스칼라·구분자 문자열·다른 namespace는 임의 분해하지 않으며 명시적 담당자 어댑터가 필요하다. 표준 카테고리 UUID를 원천 코드에서 새로 만들지 않는다.

## 이번 검증의 해석

새 tests/test_registration.py와 기존 Silver/Gold/publication 회귀 검사를 실행한다. 실제 MySQL 검증은 별도 신규 시험 schema에서 실제 등록·PK/FK·재시도·guard 차단·트랜잭션을 검사한다. 합성 승인 fixture는 시험 schema에만 쓰고 시험 종료 시 해당 schema만 정리한다.

실제 Delta 시험도 별도 로컬 출력과 합성 관측을 사용한다. MongoDB·원천 수집·운영 스케줄·업무 승인 자료를 사용하거나 변경하지 않는다. 따라서 이 결과는 내부 엔진의 동작 증거이며 실제 업무 상품의 매핑·이미지 이용·게시 승인이 완료됐다는 뜻이 아니다.

실제 서버명·보호된 등록 export·실행 보고서·누락 목록은 비공개 운영 인수인계 자료에서 확인한다. 이 코드·스크립트는 별도 공유 승인 없이 GitHub에 push하지 않는다.
