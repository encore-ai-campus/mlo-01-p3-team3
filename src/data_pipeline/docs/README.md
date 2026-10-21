# 파이프라인 문서 안내

현재 상태·남은 인수는 **[실서버 검증](service_validation_20261018.md)** 한 곳에서 관리한다. 실행 명령은 각 실행 가이드, 데이터와 복구 규칙은 아래 계약 문서를 따른다. 검증 기록의 과거 버전/시험 전용 경로를 운영 설정으로 사용하지 않는다.

## 인계와 검증

| 문서 | 용도 |
|---|---|
| [MySQL v2 스키마 반영](mysql_schema_rollout.md) | 적용/롤백 SQL·계약 검사·기존 데이터 보완·운영 전 게이트 |
| [서버 배포·인계](deployment.md) | 서버 역할·보호 설정 경로·설치·배포·초기 적재·재개 |
| [실서버 검증·남은 인수](service_validation_20261018.md) | 현재 확인 범위·최종 release·증거·운영 전 필요한 입력 |
| [이슈 #13 대조](issue13_acceptance.md) | 요구사항별 구현·검증·미완료 구분 |
| [실제 서비스 인수 방법](acceptance.md) | 소량 시험 준비·실행·판정 |
| [로컬 게시·운영 검증 기록](validation_20261018.md) | 로컬 MySQL/Delta, 대역 NiFi·파일 이관의 재현 근거; 현재 서버 상태와 구분 |

## 실행과 복구

| 단계 | 가이드 |
|---|---|
| 이미지 임베딩 전용 Delta 테이블·고정 벡터 export | [추천 카탈로그 연결](../../../docs/interior-recommender/server-catalog.md): `run_embedding_table.py`, 기존 정규화 Silver 8개 테이블과 독립 |
| 입력 검증·snapshot·Mongo/이미지·Kafka | [입력·적재 실행](execution.md) |
| 수집 중 확정 데이터 인계 | [LIVE 실행](live_execution.md) |
| Kafka → Bronze | [Bronze 실행](bronze_execution.md) |
| Bronze → Silver | [Silver 실행](silver_execution.md) |
| Silver → Gold | [Gold 실행](gold_execution.md) |
| 고정 Gold → MySQL | [MySQL 게시](mysql_publication.md) |
| 전체 단계 연결·완료 대조 | [FLOW 실행](flow_execution.md) |
| queue·제한 재시도·NiFi | [운영 실행](operations_execution.md) |
| 정지·백업 증거·상태 복원 | [상태 이관](state_migration.md) |
| 현재 Garage 2.4.1의 S3·Delta 조건 | [Garage 프로필](garage_v241_profile.md) |

## 설계와 계약

| 문서 | 담당하는 규칙 |
|---|---|
| [통합 설계](integration_design.md) | 시스템 경계와 전체 처리 구조 |
| [보강 계약 v1.1](design_amendment_v1_1.md) | 세부 보강 규칙의 기준; 원래 설계와 차이가 있으면 해당 규칙을 우선 |
| [데이터·복구 계약 v1](data_contract_v1.md) | 관측/상품/원천/이벤트 ID·이미지·상태·시각·복구 |
| [LIVE 인계 계약](live_handoff_v1.md) | commit·seq·hash·마이크로배치·cursor·전송 경계 |
| [Silver 설계](silver_design_v1.md) | 테이블·품질 상태·정규화·재평가 |
| [Silver/Gold 필드 대조](silver_gold_field_mapping.md) | 필드 제안 46항목과 입력·산출물 대응 |
| [상위 아키텍처 대조](architecture_alignment.md) | 프로젝트 01/02/03/05/12 설계와 책임 경계 |
| [Gold→MySQL 게시 필드 계약](../../../docs/design/13-gold-mysql-publication.md) | v2 DDL 필수 컬럼·타입·관계·전체 Gold 필드 대응·추천 어댑터 인수 |

`verification/`의 공개 결과는 성공과 실패를 함께 보존한다. `server_setup/`은 배포 도구와 검증 재현 코드이며, 일부는 특정 시험 경로에 고정돼 있다. 디렉터리 전체를 일괄 실행하거나 일반 설치기로 사용하지 않는다. 비밀 설정·키·로컬 원천 데이터는 포함하지 않는다.

초기 계획, 준비 단계의 미접속 보고, 중복 현황/누락표는 현재 가이드와 검증 기록으로 통합했다. 삭제 전 원문은 Git 이력에서 확인할 수 있다.
