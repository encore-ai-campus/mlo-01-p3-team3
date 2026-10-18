# 전용 실행 환경 배포와 MongoDB 최소 검증

이후 [상품 파이프라인 실서버 소량 검증](service_validation_20261018.md)과 최종 128dbd37... 배포까지 진행했다. 아래 f47d442/ce456c2f...와 미검증 목록은 최초 준비 단계의 기록이다.

2026-10-18, `feature/#13`. 이 기록은 상품 수집 파이프라인의 실행 준비·저장소 접속 검증이다. 사용자 입력·추천·임베딩·서비스 작업 원장 연동의 완료 기록이 아니다.

## 배포한 내용

- 소스 커밋: `f47d442`. source release ID: `ce456c2fd397215433e5b90e07eaa8ada35dacc0aac5f82f5196fba41dfb0156`.
- 아카이브 SHA256: `7862d82fcbe4440360d7145a52da1ce5985c731efbff118662ed3eb67188367a`. 181개 파일의 manifest·바이트 수·checksum을 확인했다.
- 두 서버의 root 소유 불변 소스: `/opt/pj3/product-pipeline/releases/<release_id>`.
- NiFi의 전용 Python: `/var/lib/pj3/product-pipeline/nifi/venvs/ce456c2fd3972154-base/bin/python`, 실행 계정 `pj3-nifi`.
- Spark Master의 전용 Python: `/var/lib/pj3/product-pipeline/spark/venvs/ce456c2fd3972154-spark/bin/python`, 실행 계정 `pj3-spark`.
- 서버 Python 3.14.4, Master의 PySpark 4.1.3/Delta Python 4.4.1. 기존 NiFi/Spark 서비스·공용 JAR은 교체하지 않았다. 전용 venv를 만드는 작업은 운영 데몬 활성화와 별개다.
- 두 환경에서 `pip check`, SDK import, 설치한 CLI 도움말 실행을 확인했다. 실제 설치 버전 목록은 각 서비스의 `audit/ce456c2fd3972154-<profile>-freeze.txt`에 보존했다.

[설치 도구](server_setup/pipeline_runtime_deploy.py)는 한 호스트씩 적용한다. 원본 아카이브·manifest 검증 이후 서비스 계정으로 의존성·패키지를 설치하며, DB 적재·NiFi 예약·서비스 기동을 수행하지 않는다. SHA256/manifest 불일치·경로 이탈·중복 member·symlink를 거부하는 [자동 테스트](../tests/test_runtime_deploy.py) 5개를 통과했다. 이 도구 자체는 배포한 `f47d442` 소스 묶음 이후 추가한 파일이다.

## 실제 결과

| 대상 | 결과 | 근거 |
|---|---|---|
| NiFi 전용 실행 환경 | RUNTIME_VERIFIED, 12,375ms | [receipt](verification/runtime_20261018/nifi-deploy.json) |
| Spark Master 전용 실행 환경 | RUNTIME_VERIFIED, 94,474ms | [receipt](verification/runtime_20261018/spark-deploy.json) |
| NiFi 계정 S3/Kafka | Garage HEAD 성공, mTLS로 Kafka 브로커 3개 확인 | [결과](verification/runtime_20261018/nifi-connections.json) |
| Spark 계정 S3/Kafka | Garage HEAD 성공, mTLS로 Kafka 브로커 3개 확인 | [결과](verification/runtime_20261018/spark-connections.json) |
| MongoDB Replica Set | rs-products, 9.0.4 세 멤버, Primary 1/Secondary 2 | [인증 조회](verification/runtime_20261018/mongo-replica.json) |
| Mongo 전용 계정 | 신규 두 계정 생성·역할·인증 확인 | [비밀 없는 결과](verification/runtime_20261018/mongo-accounts.json) |
| Master → Mongo 실제 쓰기/읽기 | majority+journal ACK, 세 멤버의 majority 읽기 일치, 시험 컬렉션 삭제 | [결과](verification/runtime_20261018/mongo-client-verification.json) |
| 실제 MongoLease | 획득·갱신, 같은 경로의 두 번째 writer 차단, 소유 claim 해제 확인 | 같은 결과 |
| Kafka 상품 토픽 | 관리자 조회에서도 product.raw.collected 없음 | [조회](verification/runtime_20261018/kafka-topic-audit.json) |

Kafka cluster ID는 `AGceilLlygifiV1hss1Rbw`다. mTLS metadata 조회 성공이 상품 토픽 발행·소비 권한이나 이벤트 전달 성공을 뜻하지 않는다. 이번에는 토픽/ACL을 생성하거나 메시지를 발행하지 않았다.

## MongoDB 변경과 설정 위치

기존 저장 관리자 정보로 인증이 실패했으나, 설치 번들의 `admin` 절차가 최초 사용자 예외로 생성·인증을 통과했다. 비밀번호 변경·authorization 해제·Replica Set 재초기화는 하지 않았다. 후속 인증 조회 당시 DB는 admin/config/local, 사용자는 pj3-mongo-admin 한 명이었다. 이후 사용자가 승인한 원천 DB명 `pj3_products_raw`에 다음 계정을 만들었다.

| 계정 | 역할 | 용도 |
|---|---|---|
| pipeline_ingest | pj3_products_raw readWrite, admin clusterMonitor | 원천 접수·보존·운영 원장 |
| pipeline_spark | pj3_products_raw read, pipeline_ops readWrite, admin clusterMonitor | 원천 조회·공용 writer 잠금 |

`pipeline_ops.bronze_writer_leases`, `pipeline_ops.pipeline_writer_leases`를 생성하고 TTL index가 없음을 확인했다. 이 권한은 현재 서버 준비 가이드의 DB 단위 기본 역할이며 컬렉션별 세분화된 사용자 정의 역할은 아니다. 외부 API 처리용 서비스 MySQL lease와는 별개다.

Mongo01의 새 자격 증명 원본은 `/etc/pj3-secrets/product-pipeline/mongodb-accounts.json`에 root 전용 0600으로 저장했다. 관리자 비밀번호를 애플리케이션 서버에 전달하지 않았다.

서비스별 보호 경로는 `/var/lib/pj3/product-pipeline/{nifi,spark}/config/`(0700)이며, 내부 파일은 해당 서비스 소유 0600이다.

- `mongo-ca.crt`: 공개 CA. 원본 CRLF와 읽기 시 LF의 차이를 정규화하여 대조한다.
- `mongo-accounts.json`: NiFi에는 ingest 계정만, Master에는 ingest와 spark 계정을 전달했다. Master의 전체 상품 FLOW 실행 준비용이며 실제 FLOW는 실행하지 않았다.
- `kafka-python.json`: 기존 역할별 P12/CA를 참조하는 librdkafka 설정. Java Spark Kafka source 설정으로 그대로 사용하지 않는다.

URI·비밀번호·P12 비밀번호·보호 파일 내용은 Git/결과 JSON에 저장하지 않는다. 위 파일을 만들었다고 현재 환경변수가 자동 설정되거나 NiFi에서 Master로 원격 실행되는 것은 아니다. 운영 environment/단계 설정과 비밀 주입·실행 경로 인수는 아직 필요하다.

Master의 최초 Mongo 연결 실패 원인은 Mongo 내부 호스트명 해석 누락이었다. IP의 TCP 27017 연결은 가능했다. 두 클라이언트 서버의 `/etc/hosts`에 세 내부 호스트명만 별도 관리 블록으로 추가하고 기존 파일 백업을 남겼다. 다른 환경에서는 해당 환경의 DNS/주소로 설정하며 이 VM 주소를 애플리케이션 코드에 넣지 않는다.

최소 복제 시험은 전용 무작위 이름의 시험 컬렉션에 문서 1건만 썼다. `w=majority,j=true` ACK와 Primary/Secondary 각각의 majority 읽기를 확인했다. 시험 컬렉션과 본 시험이 획득한 lease만 삭제했다. 업무 원천·스냅샷·관측·이벤트를 적재하지 않았다. 노드 중지·선거·디스크 충돌 내구성·응답 유실·장기 재시작 시험은 포함하지 않았다. [실제로 사용한 시험 본문](server_setup/verify_mongo_client.py)은 보호 JSON을 표준입력으로 받아 서비스 계정에서 실행하는 검증용 코드다. 결과의 status와 정리 여부로 판정하며 단순 프로세스 종료만으로 성공 처리하지 않는다.

## 다른 담당자와의 경계 및 남은 작업

사용자가 제공한 후속 연결 이슈는 다른 담당자가 수행한다. 다음 항목은 이번 작업에서 구현·실행하지 않는다.

- 서비스 MySQL 작업 원장 매핑·선점·lease·토큰 조건부 완료.
- 사용자 입력 접수 이벤트·Bronze 보존 완료 이벤트·해석/외부 AI 시작 게이트.
- 추천 Consumer의 offset/재할당·미완료 작업 복구와 비동기 실행 연결.
- Gold와 추천용 상품·이미지·벡터 snapshot, JSON/NPY 카탈로그 연결.
- 임베딩/인덱스·요청 정책 버전 고정, 추천 종단·성능·브라우저/UI 인수.

현재 완료한 것은 **상품 수집 파이프라인의 실행 환경과 Mongo/S3/Kafka의 제한된 준비 검증**이다. 이전 [Garage 실제 시험](garage_v241_validation_20261018.md)은 그대로 별도 근거다. 업무 Kafka 토픽/ACL, 승인된 Delta/Kafka 전이 JAR 배포, 전체 Worker 작업, 실제 상품 Mongo→Kafka→Bronze 종단·장애 시험, 업무 매핑/검수/등록 입력·실제 MySQL DDL 대조는 미완료이며 담당자와 소유 범위를 확인한 후 이어간다. NiFi 일정과 전체 수집 데이터 적재는 시작하지 않았다. 커밋·푸시도 이번 작업에서 수행하지 않았다.

## 서버 측 증거 위치

- 배포: Bastion `/home/team3ops/pipeline-deploy-4a8f9e7f47904900ae788622e9a33ca0/`.
- S3/Kafka 접속과 최초 관리자: `/home/team3ops/pipeline-connect-c3a9c7b4b48c4b5a865f6ad2f66df68c/`.
- 복제·토픽 조회: `/home/team3ops/pipeline-service-audit-ada65d8a6c18423fb55c2d3880f88d05/`.
- 계정 준비: `/home/team3ops/pipeline-mongo-accounts-9e1e1ace5fc74b5ea6fd5a04d7abf3f9/`.
- 클라이언트 설정·최종 복제/lease 시험: `/home/team3ops/pipeline-client-config-5f73004c7bd74649a26ff39deda7f480/`.

결과 JSON만 이 저장소의 verification/runtime_20261018에 복사했다. 원본 보호 설정과 비밀번호는 복사하지 않았다.
