# Garage S3 최소 실제 검증 결과 — 2026-10-18

후속 [원인 분석](garage_conditional_write_analysis_20261018.md)에서는 실제 SDK의 조건부 헤더 전송, 설치된 Garage v2.4.1과 PUT 처리 소스, 현재 Caddy 설정까지 확인했다. 별도 NiFi 진단 1회도 동일 전용 키만 사용하고 삭제했다.

## 판정

일반 S3 접속·저장·동일 데이터 재사용은 가능하다. **현재 연결 경로는 조건부 PUT의 기존 객체 보호 시험에 실패했으므로, 불변 원천·이미지 및 Delta까지 연결 가능한 상태로 인수하지 않는다.** 운영 데이터 적재는 진행하지 않았다.

## 실행 환경과 방법

- Bastion의 기존 inventory/Vault/전용 SSH 키/승격 경로를 이용했다. 비밀 값은 외부로 전달하거나 결과에 출력하지 않았다.
- NiFi에서는 pj3-nifi, Spark Master에서는 pj3-spark 계정으로 보호된 properties를 읽었다. 파일 권한·내용 및 서비스 설정을 바꾸지 않았다.
- 서버에 설치돼 있던 boto3 1.40.72를 사용했다. 제안한 고정 의존성 설치본의 검증은 아니다.
- HTTPS endpoint .141/.142:8443, pj3-data, us-east-1, SigV4/path-style, 지정 CA를 사용했다.
- 현재 product_pipeline/objects.py와 errors.py 소스를 검증 프로세스에 전달했다. S3Objects에는 서비스 전용 키를 메모리에서 읽어 만든 명시적 설정의 boto3 client를 주입했다. 업무 CLI의 자동 자격 주입까지 구현·검증한 것은 아니다.
- 원본 objects.py SHA256: `8d8b0e289b06dbaa4d052a192eddd2f05b3f87322f1f6ba8d1ab531d7a39d910`.

## 결과

| 검사 | 결과 |
|---|---|
| 서비스 계정으로 보호 파일/CA 읽기 | 두 계정 모두 성공 |
| 버킷 HEAD | 두 계정 모두 성공 |
| S3Objects.ensure 최초 저장 및 SHA256 재확인 | 두 계정 모두 성공 |
| 같은 바이트 ensure 재실행 | 두 계정 모두 기존 데이터 재사용(False) |
| 다른 endpoint에서 같은 키에 다른 바이트를 IfNoneMatch=*로 PUT | 두 계정 모두 예상한 거절을 받지 못함 |
| NiFi 계정으로 상세 재확인 | HTTP 200. 이후 두 endpoint GET에서 다른 바이트가 실제 관측됨 |
| 시험 객체 삭제 | 두 계정 모두 삭제 후 반대 endpoint HEAD 404 확인 |
| Delta 준비 상태 | Spark 배포 JAR 디렉터리와 조사한 서비스 계정 Ivy/검증 캐시에 Delta JAR 미발견. 현재 SSH Python에서 pyspark/delta import 불가 |
| Delta 저장/MERGE/재개 | 실행하지 않음. 새 패키지/JAR 설치하지 않음 |

성공한 일반 재사용은 순차 중복 입력의 동작만 입증한다. 조건부 쓰기가 실제 덮어쓰기를 막지 못하므로 동시 작성자·다른 본문 충돌에서도 불변성을 유지한다고 판단할 수 없다. S3A PUT/GET 성공 역시 Delta의 로그 생성·동시 커밋 보장을 대신하지 않는다. 현 시험은 관측된 endpoint 경로의 결과이며 Garage 제품의 모든 버전/설정에 대한 일반 판정은 아니다.

## 시험 범위와 증거

전용 prefix: `verification/pipeline-ae11beabec444083b4fba6b3604023cc/`. NiFi/Spark 시험 객체 각각 1개, NiFi 확인을 위한 같은 시험 키 재시험 1회만 수행했다. 삭제는 해당 키로만 제한했다. 기존 객체·DB·Kafka·NiFi flow·클러스터 구성·보호 파일 권한은 변경하지 않았다.

- [NiFi 상세 결과](verification/garage_20261018/nifi-result.json)
- [Spark 결과](verification/garage_20261018/spark-result.json)
- [NiFi 사전 조건](verification/garage_20261018/nifi-preflight.json)
- [Spark 사전 조건](verification/garage_20261018/spark-preflight.json)
- 검증 코드: [pipeline_garage_verify.py](server_setup/pipeline_garage_verify.py). Ansible 모듈이며 일반 서버 설치 도구가 아니다.
- Bastion의 보호된 실행 기록: `/home/team3ops/pipeline-ae11beabec444083b4fba6b3604023cc/`. 최초 사전 점검에서 서비스 계정 전환 뒤 Ansible payload 직렬화 권한 문제가 발생했으며, 검증만 자식 프로세스에서 서비스 계정으로 실행하도록 수정 후 통과했다. 인프라 장애가 아니다.

Ansible 실행 자체의 종료 코드 0과 결과 JSON의 검증 성공은 별개다. 이번 S3 시험 결과의 status는 FAILED이며 성공으로 집계하지 않는다.

## 다음 조건

1. 저장소 담당과 현재 endpoint의 조건부 요청 처리(프록시 헤더 전달 및 backend 지원 포함)를 확인한다. 단순 PUT으로 우회하지 않는다. 변경 뒤 전용 키의 충돌 차단을 다시 확인한다.
2. 서비스 계정의 업무 Python 자격 로더/실행 경로와 고정 의존성을 준비한다.
3. Spark/Delta 의존성 및 해당 저장소의 Delta LogStore/동시성 보장 방법을 승인한 뒤 전용 prefix로 소량 시험한다. 지금 Delta 안전성을 추정하여 업무 checkpoint/lake를 만들지 않는다.

이 작업에서는 원인 수정·서비스 재설치·권한 확대·운영 적재를 수행하지 않았다.
