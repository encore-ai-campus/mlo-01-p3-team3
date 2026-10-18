# 기존 Garage S3 적용 검토 — 2026-10-18

최신 구현: [Garage 전용 프로필](garage_v241_profile.md)과 [보완 검증](garage_v241_validation_20261018.md)이 추가됐다. 아래 초기 계획·기본 conditional 프로필의 한계와 이후 garage_v241 구현을 구분한다.

후속 실제 시험: [최소 검증 결과](garage_s3_verification_20261018.md). 일반 S3 동작은 성공했으나 현재 endpoint 경로가 조건부 PUT 덮어쓰기 차단에 실패했다. 이 문제가 해결되기 전에는 불변 적재/Delta 연결 완료로 인수하지 않는다. 아래 내용은 연결 계획이며 성공 판정이 아니다.

근거: 사용자가 제공한 `Z:/pj-3rd-team/02 Data/shinsm/pj3-object-storage/results/TEAM-S3-GUIDE.md`. 아래 인프라 검증 결과는 해당 문서의 보고 내용이며, 이번 작업에서 서버를 재검증하거나 설정하지 않았다. 문서 안의 재검증 명령도 실행하지 않았다.

## 적용할 인프라 계약

| 항목 | 기존 구성 |
|---|---|
| 기본 endpoint | `https://192.168.0.141:8443` |
| 다른 endpoint | `https://192.168.0.142:8443` — 같은 클러스터 |
| 버킷 / region | `pj3-data` / `us-east-1` |
| 클라이언트 | SigV4, path-style, TLS 검증 유지 |
| CA | `/etc/pj3-clients/s3/ca.pem` |
| Java 공개 CA truststore | `/etc/pj3-clients/s3/ca-truststore.p12` |
| NiFi 전용 키 파일 | `/etc/pj3-clients/s3/nifi.properties` — pj3-nifi 소유 0600 |
| Spark 전용 키 파일 | `/etc/pj3-clients/s3/spark.properties` — pj3-spark 소유 0600 |

저장소 신규 설치·MinIO 도입·Object01/02 역할 분리안은 이 환경에 적용하지 않는다. 두 endpoint를 서로 다른 저장소로 취급하지 않는다. 키를 Git·명령 인자·채팅으로 옮기거나 team3ops에 보호 파일 읽기 권한을 추가하지 않는다.

2노드 consistent/RF2 구성은 한 노드 장애 시 정상 노드 읽기는 가능하지만 쓰기가 503으로 거절된다. endpoint 교체만으로 쓰기 가용성을 복구할 수 없다. 파이프라인은 실패/미확인 단계를 보존하고 양 노드 복구 후 재개해야 한다. 무제한 즉시 재시도나 원천 저장 확인 전 Kafka 발행을 허용하지 않는다. 문서의 약 80GB 논리 용량은 엄격한 quota가 아니며, 이미지·인계 청크·Delta 이력·checkpoint가 공간을 함께 사용한다.

## 현재 코드와 대응

| 영역 | 적용 방법 | 아직 필요한 확인 |
|---|---|---|
| 원천 이미지 | image_bucket을 pj3-data, PIPELINE_S3_ENDPOINT를 기본 HTTPS endpoint로 설정 | boto3의 SigV4/path-style/region/CA 및 실제 요청 호환성 |
| 증분 인계 | handoff_bucket도 pj3-data 사용 가능. Settings는 두 버킷이 달라야 한다고 강제하지 않음 | handoff/chunks/sha256 및 handoff/ready의 조건부 업로드/ready-last 재개 |
| 경로 분리 | 기존 이미지 객체 키·handoff 키를 보존. lake/checkpoint는 승인한 별도 prefix | prefix는 보안 경계가 아님. 현재 키는 pj3-data 전체 read/write 권한 |
| NiFi | 기존 root Controller Services를 네이티브 S3 processor에서 재사용 | Python 외부 프로세스에는 Controller Service의 키가 자동 주입되지 않음 |
| Spark | 기존 Spark 4.1.3/Hadoop 3.4.2/AWS SDK v2/Java21 및 고정 로컬 JAR 사용 | Python 의존성, Delta 조합 및 실제 Delta 트랜잭션은 별도 시험 |

`product_pipeline/objects.py`는 GET 후 SHA256/크기를 확인하고 `put_object(IfNoneMatch="*")`로 불변 객체를 저장한다. 가이드의 일반 PUT/GET/LIST/DELETE 성공은 조건부 PUT의 충돌 차단·응답 유실 후 재실행까지 보증하지 않는다. Garage에서 지원 여부와 응답을 소량 전용 키로 확인한 뒤 인수한다. 지원되지 않으면 무조건 덮어쓰는 PUT으로 우회하지 않고 적재를 차단한다.

현재 S3Objects는 boto3 기본 credential chain을 사용하며 Java properties 파일을 직접 읽지 않는다. path-style·SigV4·region도 생성자에서 명시하지 않는다. 서비스 계정 전용 비밀 주입과 승인된 AWS 설정을 제공하거나, 보호 파일을 메모리에서 읽는 어댑터를 별도 구현해야 한다. 최신 SDK의 checksum/전송 형식도 실제 Garage로 확인한다. 비밀 없는 환경 값은 다음과 같다.

```text
PIPELINE_S3_ENDPOINT=https://192.168.0.141:8443
AWS_DEFAULT_REGION=us-east-1
AWS_CA_BUNDLE=/etc/pj3-clients/s3/ca.pem
```

NiFi의 AWSCredentialsProviderControllerService `4dc88383-01a1-1000-be8d-6fb8d05efecf`와 StandardSSLContextService `4dc8839a-01a1-1000-d504-bad2a475424e`를 재사용할 수 있다. 검증 그룹은 운영용으로 전환하지 않는다. Python 작업을 pj3-nifi로 실행할 경우 코드·전용 venv·상태 경로에 필요한 접근 권한도 별도로 준비한다. 이전 team3ops 홈 설치 명령만으로 서비스 계정 실행 준비가 끝난 것은 아니다.

## Spark 실행 시 유지할 설정

- `SPARK_CONF_DIR=/etc/pj3/spark`의 기존 인증·암호화를 유지한다.
- 모든 노드에 이미 배치된 `hadoop-aws-3.4.2.jar`, `bundle-2.29.52.jar`를 `local:/opt/pj3/s3-clients/hadoop-3.4.2/` 경로의 --jars로 지정한다. 다른 버전 JAR을 중복 추가하지 않는다.
- Driver는 SPARK_SUBMIT_OPTS, executor는 spark.executor.extraJavaOptions로 기존 PKCS12 truststore를 사용한다. 공개 CA만 담긴 truststore의 changeit은 S3 secret과 구분한다.
- 가이드의 fs.s3a.* 설정을 적용하고 pj3-spark 계정이 보호된 spark.properties의 accessKey/secretKey를 메모리에서 Hadoop Configuration에 주입한다. 키를 --conf나 Spark 옵션 JSON에 평문 기록하지 않는다.
- 현재 Python Spark 진입점은 옵션 JSON을 받아 SparkSession을 만들지만 해당 properties의 전용 로더는 없다. 기존 Scala 검증 로더가 Python 업무 코드에 자동 적용되지는 않는다. Driver/Executor의 자격 전달과 로그 노출 방지를 구현·검증해야 한다.
- Delta는 승인한 pj3-data의 lake prefix, 작업별 checkpoint prefix를 사용한다. 일반 S3A barrier 검증 결과와 Delta log/동시성/재개 검증을 구분한다.

## 최소 후속 검증 순서

1. 보호 파일의 내용 대신 소유자·권한·접근 가능한 실행 계정과 CA/JAR 존재를 확인한다.
2. NiFi Python 서비스 계정의 S3 클라이언트를 준비하고 HEAD/소량 GET으로 읽기 확인한다.
3. 승인된 전용 검증 키에서 최초 조건부 PUT, 같은 입력 재사용, 다른 바이트 덮어쓰기 차단, SHA256 대조를 시험한다. 운영 객체를 대상으로 하지 않는다.
4. Spark에서 같은 버킷의 승인된 임시 prefix에 소량 Delta 저장/MERGE/고정 버전 읽기/재시작을 시험한다. 성공 후 Bronze/Silver/Gold 연결을 확대한다.
5. 기존 키·버킷·경로와 충돌하지 않는지 확인하고 배포 환경 설정을 확정한다. MongoDB/Kafka/MySQL 접속·DDL 인수 조건은 별도로 유지한다.

이번 변경은 연결 계획과 명령서를 갱신한 것이다. 비밀 파일 열람, 서버 변경, 버킷 생성, 객체 쓰기, 장애 시험, 파이프라인 실제 적재는 수행하지 않았다.
