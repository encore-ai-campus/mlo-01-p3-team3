# Garage 2.4.1 보완 및 검증 — 2026-10-18

## 구현

- conditional 기본 동작을 유지하고 garage_v241 내용 주소 프로필을 추가했다.
- 이미지/인계 청크/ready의 키-바이트 관계를 요청 전에 확인한다. 임의 키·잘못된 본문·비정규 ready 바이트는 차단한다.
- 올바른 내용만 PUT 후 SHA256으로 대조하며 응답 유실은 GET으로 확인한다. 기존 충돌 객체를 덮어써 복구하지 않는다.
- 서비스 계정 소유 0600 properties를 메모리에서 읽고 SigV4/path-style/CA와 필요한 checksum 전송 정책을 적용한다.
- 원천 적재·CLI send/receive·예약 발견/수신·종단 대조에 같은 설정 factory를 연결했다.
- Spark의 공통 Garage S3A 설정·보호 파일 로더를 Bronze/Silver/Gold·게시 계획·FLOW 대조에 연결했다. 업무 쓰기에는 기존 자동 takeover 금지 MongoLease 및 같은 버킷의 대상 URI를 요구한다.
- 저장 경로·상품/원천/이벤트 ID·기존 fingerprint는 유지한다. 구체적인 실행·신뢰 경계는 [프로필 계약](garage_v241_profile.md)을 따른다.

## 로컬 자동 검증

전체 자동 테스트 244개/88.737초 통과 후 실제 SDK의 Garage 요청 모델 테스트 1개를 추가했다. SDK 테스트 2개를 재실행하여 통과했다. 현재 서로 다른 테스트 합계는 245개다. 이 중 신규 Garage/보호 파일/Spark 경계 테스트 26개와 추가 SDK 테스트 1개가 있다. 외부 요청 없이 수행한 테스트와 아래 실제 서버 시험을 구분한다.

주요 사례: 두 writer의 동시 빈 키 관측, 내용 주소 일치/불일치, 원격 기존 본문 충돌, 응답 유실/완전 실패, canonical ready, ID 유지, lease 만료/다른 버킷/임의 잠금 차단, JAR/LogStore/truststore 충돌, 보호 파일 소유/권한/형식.

## 실제 NiFi·Spark 객체 검증

Bastion의 기존 보호된 Ansible 경로를 사용하고 각 서비스 계정으로 실제 로더/어댑터를 실행했다. 서버 기존 boto3 1.40.72를 사용했으며 운영 고정 venv 배포 완료를 뜻하지 않는다.

| 항목 | NiFi | Spark Master |
|---|---|---|
| 서비스 전용 properties/CA 사용 | 통과 | 통과 |
| 이미지 키 최초 저장·같은 바이트 재사용 | 통과 | 통과 |
| handoff 청크 저장·재사용 | 통과 | 통과 |
| .142 endpoint에서 SHA256 대조 | 통과 | 통과 |
| 실제 PUT 뒤 클라이언트 응답 유실 모사/GET 복구 | 통과 | 통과 |
| 다른 바이트의 같은 키 사용을 HTTP 요청 전에 차단 | 통과 | 통과 |
| 기존 바이트 유지 | 통과 | 통과 |
| 조건부 PUT 의존 | 없음 | 없음 |
| 시험 객체 삭제 및 반대 endpoint HEAD 404 | 통과 | 통과 |

두 계정 각각 2개의 고유 내용 주소 시험 객체만 만들고 삭제했다. 직접 원시 PUT의 덮어쓰기를 Garage 자체가 차단하게 만든 것은 아니다. 운영 ready manifest를 발행하거나 DB/Kafka에 시험 상품을 넣지 않았다.

- [NiFi 프로필 결과](verification/garage_20261018/nifi-profile-result.json)
- [Spark 프로필 결과](verification/garage_20261018/spark-profile-result.json)
- [실제 시험 코드](server_setup/pipeline_garage_profile_verify.py)
- Bastion 기록: `/home/team3ops/pipeline-garage-profile-910f4b5ca4784371bba87e6127420f02/`.

## Delta 검증 범위

최종 **Spark 4.1.3/Hadoop 3.4.2/Java21 + Delta 4.4.1(Spark 4.1용)**의 local[1] 시험을 통과했다. Garage 전용 고유 prefix에서 최초 1행 저장, MERGE 후 2행, versionAsOf=0에서 1행, 새 SparkSession 읽기 2행을 확인했다. 데이터 삭제 후 prefix가 없음을 확인하고 Scala 실행 코드가 전용 JSON proof를 생성했다. 종료 코드 0과 proof의 run_id/실제 행 수/삭제 결과가 모두 맞아야 성공으로 집계했다.

- [Delta 최종 실제 결과](verification/garage_20261018/delta441-result.json)
- [Scala 시험 코드](server_setup/garage_delta_verify.scala), [실행 모듈](server_setup/pipeline_garage_delta_verify.py)
- 최종 시험 캐시/증거: `/var/lib/pj3/spark/pipeline-garage-delta-35d2ee784458465db563146514e28ce1/`. 공식 의존성의 SHA256을 결과에 기록했다. 서비스 공용 JAR을 교체하지 않았다.

업무 MongoLease, 전체 Worker 분산 작업, 강제 종료 후 다른 Driver로 재개, 실제 Python 업무 Spark 진입점의 서버 종단 실행은 이 시험에 포함하지 않았다. 업무 경로는 기존 latched lease를 요구하도록 코드/자동 테스트로 검사했다. 이번 실제 Delta 시험의 독점성은 이 프로세스에만 배정한 고유 prefix이며 업무 잠금 시험을 대체하지 않는다.

실패/도구 보완도 구분해 보존한다. Delta 4.1.0은 실제 Catalyst의 IgnoreCachedData 누락으로 실패했다([결과](verification/garage_20261018/delta410-result.json)). 4.4.1의 최초 차단은 검증 도구가 unitycatalog-hadoop을 Hadoop 배포 JAR로 오분류한 것이며 규칙을 수정했다([당시 기록](verification/garage_20261018/delta441-blocked.txt)). 의존성 사전 해결, Scala DataFrame 형식 및 셸 입력 방식도 고쳤다. 콘솔 prompt 때문에 PASS 표식을 놓친 시도는 최종 성공으로 합산하지 않고, 마지막 시험에서 구조화 proof로 판정을 확정했다. 초기 실패 prefix 7개는 [읽기 전용 조사](verification/garage_20261018/delta-prefix-audit.json)에서 모두 빈 상태를 확인했다. 이후 마지막 두 시험은 자체 삭제 확인을 수행했다.

서비스 설치·공용 JAR·버킷·권한·기존 데이터는 변경하지 않았다. 시험 전용 의존성 캐시와 실행 기록만 생성했다.

## 운영 인수 전 남은 조건

이번 코드가 포함된 새 source release, 서비스 계정용 실행 환경/상태 경로, 승인된 Delta 전이 의존성 배포가 필요하다. 과거 5533c0a 묶음에 새 설정만 적용하지 않는다. 실제 Mongo/Kafka/MySQL 접속·담당 매핑/검수/등록·DDL 및 전체 FLOW/장애 시험은 여전히 별도 인수다. 아직 업무 수집 데이터를 적재하거나 NiFi 일정을 활성화하지 않았다.
