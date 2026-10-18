# 2026-10-18 서버 최소 사전 점검

후속 자료: 이후 사용자가 제공한 TEAM-S3-GUIDE.md는 Garage S3의 HTTPS 8443 endpoint·pj3-data 버킷·NiFi/Spark 네이티브 검증 결과를 명시한다. 아래 표는 이전 직접 점검 당시의 관측으로 보존한다. 저장소 적용 판단은 [Garage 적용 검토](garage_s3_integration.md)를 따르며, 파이프라인 조건부 업로드/Delta 검증 완료를 뜻하지 않는다.

사용자의 최종 지시는 **최소한의 동작 검증까지만**이다. 패키지 설치·가상환경 생성·서비스 설정 변경·실제 적재·NiFi 활성화는 수행하지 않았다. 이 결과는 전체 서버 통합 시험 통과가 아니다.

## 배포 준비 중 수행된 범위

지시 변경 전에 `pj3-nifi`, `pj3-spark-master`의 로그인 사용자 경로 `/home/team3ops/product-pipeline/`에 incoming/releases/environments/bootstrap/audit 디렉터리와 `incoming/source-5533c0a.tar.gz`를 전송했다. 압축 해제·애플리케이션 설치·실행 활성화는 하지 않았다. 원본 크롤러와 수집 파일은 변경하지 않았다.

- 코드 기준: feature/#13 `5533c0a`.
- 아카이브 SHA256: `b2e2a8d97f909f79e21f170404d1af7e5b48c532b089a59cd041900104f433d4`.
- release ID: `d8088a1a450c6900c29c9f962af3c23a74507c2ce2eb6b55059effbf472fa0e8`.
- 양쪽 서버에서 151개 manifest 파일 checksum 일치, Python 106개를 메모리에서 문법 검사했다. `.pyc`를 만들거나 압축을 풀지 않았다. 문법 검사는 실행 성공을 뜻하지 않는다.

## 확인 결과

| 대상 | 관측 사실 | 판정·남은 조건 |
|---|---|---|
| Python 실행 환경 | NiFi/Spark Master는 Python 3.14.4, venv 모듈 있음, ensurepip 없음 | 파이프라인 전용 환경 미설치 |
| Python 의존성 | 두 서버의 현재 Python에서 Pillow/pymongo/confluent-kafka/PyMySQL/pyspark/delta 모듈 미발견. boto3 1.40.72는 존재 | 현재 환경으로 전체 파이프라인 CLI/동작 시험 통과 불가. 전용 환경·lock 설치 후 확인 필요. 서비스 Spark 설치와 Python import 환경은 구분 |
| Java/Spark | Java 21.0.12.1, 실제 Spark Master 서비스 active, 설치 RELEASE는 Spark 4.1.3/Hadoop 3.4.2 | 기존 검증 조합 Spark 4.0.1/Delta 4.0.1과 다름. Driver/Executor/Delta 호환 조합을 확인해야 함. 클러스터 버전은 변경하지 않음 |
| Spark UI | NiFi에서 Master UI JSON 접근은 self-signed certificate chain 검증 오류 | 올바른 CA·승인 endpoint가 필요. 서비스 중단으로 단정하지 않음. Spark job/Worker 동작 미검증 |
| Kafka | 서비스 active, NiFi→Kafka01 9092 TCP 연결. 평문 ApiVersions 요청에는 TLS alert prefix `15 03 03 00 02 02 50` 응답 | TLS listener로 판단할 근거. 단순 TCP 성공을 Kafka protocol/인증/토픽 검증으로 간주하지 않음. Kafka 보안 설정·CA·권한 필요. produce/consume 미실행 |
| MongoDB | 기존 점검에서 서비스 3대 active, rs-products PRIMARY 1/SECONDARY 2와 같은 적용/majority 시각을 확인. 이번 NiFi→Mongo01 27017 TCP 연결 | 기존 저장 관리자 인증은 실패했고 로그에 UserNotFound 확인. 파이프라인 계정·권한/신뢰 설정 미인수. 쓰기 복제 시험 완료 증거와 구분 |
| MySQL | 서비스 active. NiFi→MySQL01 TCP 연결 후 서버가 `Host '192.168.0.125' is not allowed to connect to this MySQL server` 응답 | NiFi 실행 호스트에 대한 허용 계정/접속 정책 필요. 실제 DDL/권한·TLS/Primary 검증과 게시 미실행 |
| Object Storage 01/02 | 두 서버에서 S3 계열 active 서비스/리스너를 확인하지 못함. NiFi→두 서버의 후보 9000 포트는 ConnectionRefused | 실제 S3 endpoint·서비스·버킷·인증이 미확인. 9000을 확정 endpoint로 가정하지 않음 |
| NiFi | 서비스 active, 배치 경로에서 2.12.0 확인 | 실제 API 인증/CA·processor bundle/권한·일정 미인수. 이번에 flow/processor를 만들거나 시작하지 않음 |

## 접속 설정 조사

관련 home/opt/etc 경로에서 접근 가능한 파일 이름·권한을 조사했으나 파이프라인 전용 설정을 발견하지 못했다. `/etc/pj3/nifi`, `/etc/pj3/kafka`, `/etc/pj3/spark` 등 일부 경로는 현재 SSH 계정에 읽기 권한이 없었다. **설정이 서버 전체에 없다고 단정하지 않는다.** 비밀번호/키 내용을 출력하거나 인프라 서비스용 관리자 설정을 파이프라인 계정으로 임의 재사용하지 않았다.

## 최소 후속 인수 조건

1. 담당자가 파이프라인용 Mongo/Kafka/저장소/MySQL 접속 설정과 CA·권한을 제공한다. 비밀 값은 보호 파일/환경변수로 주입한다.
2. 실제 저장소 endpoint/버킷과 MySQL 실행 호스트 접속을 준비한다.
3. 기존 서버 Spark에 맞는 별도 실행 환경/Delta 조합을 확정하고 소량 실행을 시험한다. 기존 클러스터의 무조건적인 다운그레이드를 하지 않는다.
4. 승인 후에만 전용 환경 설치와 읽기 전용 preflight를 이어가고, 소량 적재는 별도 실제 인수 범위로 기록한다.

현재 결론은 **코드 전송 무결성·일부 서비스 응답 확인, 애플리케이션 실행 및 종단 동작은 준비 조건 부족으로 미검증**이다. 반복 테스트나 대량 자료 복사·업로드를 추가하지 않았다.
