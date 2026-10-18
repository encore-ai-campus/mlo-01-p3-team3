# Garage 2.4.1 실행 프로필

## 객체의 내용 주소 계약

Garage 2.4.1의 원자적 조건부 PUT을 가정하지 않는다. 기존 conditional 프로필은 유지하고 이 환경은 명시적으로 garage_v241을 선택한다. [원인 분석](garage_conditional_write_analysis_20261018.md)과 [보완 검증](garage_v241_validation_20261018.md)을 함께 확인한다.

Garage 프로필은 다음 키만 허용한다.

| 객체 | 요청 전 검증 |
|---|---|
| images/sha256/앞2자리/전체SHA256.확장자 | 실제 바이트 SHA256과 키/접두어 일치 |
| handoff/chunks/sha256/전체SHA256 | 청크 바이트 SHA256 일치 |
| handoff/ready/transport_id.ready.json | transport_id 제외 내용의 canonical hash와 키/ID 일치, 전체 바이트도 canonical JSON과 일치 |

다른 내용으로 같은 키를 쓰려 하면 GET을 포함한 모든 외부 요청 전에 거절한다. 이미 다른 바이트가 있으면 충돌로 처리하고 복구 PUT을 하지 않는다. 없는 키에는 일반 PUT 후 GET/SHA256/크기를 대조한다. 참여 작성자가 이 규칙을 사용하면 동시에 빈 키를 관측해도 같은 키에는 같은 바이트만 쓸 수 있다. 응답 유실 시 GET으로 저장 여부를 확인하고 무조건 PUT을 반복하지 않는다.

이는 파이프라인 작성자의 내용 주소 규칙이며 외부 프로그램의 직접 PUT까지 차단하는 버킷 WORM 정책은 아니다. 현재 키는 pj3-data 전체 read/write 권한이므로 다른 작성자와 파이프라인 키 불변 규칙을 합의해야 한다. prefix는 보안 경계가 아니다. Garage 자체의 조건부 PUT 기능을 수정한 것도 아니다.

기존 이미지/청크/ready 경로와 상품·원천·관측·이벤트 ID를 보존한다. 실행 정책인 프로필·자격 파일 경로는 기존 Settings.fingerprint에 추가하지 않는다. 임의 파일명/Delta 로그는 이 객체 어댑터로 저장하지 않는다.

## 원천·인계 설정

[환경 예시](../config/environment.garage.example.json)의 Mongo DB/environment placeholder를 담당자가 확정한다. NiFi의 pj3-nifi 계정에는 다음 비밀 없는 환경 값만 제공한다.

```bash
export PIPELINE_S3_ENDPOINT=https://192.168.0.141:8443
export AWS_DEFAULT_REGION=us-east-1
export AWS_CA_BUNDLE=/etc/pj3-clients/s3/ca.pem
export PIPELINE_S3_CREDENTIALS_FILE=/etc/pj3-clients/s3/nifi.properties
```

Spark 계정에서 객체 어댑터를 실행하면 파일 경로를 /etc/pj3-clients/s3/spark.properties로 바꾼다. 키 값은 환경/명령행/로그로 복사하지 않는다. 로더는 POSIX 정규 파일·현재 계정 소유·0600·형식을 확인하고 메모리에서만 사용한다. symlink/다른 소유자/그룹 읽기/중복 또는 누락 필드는 차단한다. 권한을 넓히지 않는다. 다른 환경에서 기존 credential chain을 쓰면 자격 파일 옵션을 생략할 수 있다.

SDK는 SigV4/path-style/us-east-1/CA 검증을 유지하고 선택적 SDK checksum은 when_required로 설정한다. 원천 적재·CLI send/receive·예약 발견/수신·종단 객체 대조가 모두 S3Objects.from_settings를 사용한다. 업무 코드/venv/상태 경로는 서비스 계정용으로 별도 배포해야 한다.

## Spark/Delta 설정

pj3-spark Driver에 다음을 제공한다.

```bash
export PIPELINE_SPARK_S3_PROFILE=garage_v241
export PIPELINE_S3_ENDPOINT=https://192.168.0.141:8443
export AWS_DEFAULT_REGION=us-east-1
export PIPELINE_SPARK_S3_BUCKET=pj3-data
export PIPELINE_SPARK_S3_CREDENTIALS_FILE=/etc/pj3-clients/s3/spark.properties
export PIPELINE_SPARK_S3_TRUSTSTORE=/etc/pj3-clients/s3/ca-truststore.p12
export SPARK_CONF_DIR=/etc/pj3/spark
```

공통 spark_storage를 Bronze/Silver/Gold 및 읽기 전용 게시 계획/FLOW 대조에 연결했다. 기존 고정 Hadoop/AWS JAR·S3A·Driver/Executor 공개 CA truststore를 적용한다. 기존 JVM의 truststore가 다르면 차단한다. 키는 Spark 옵션/--conf에 넣지 않고 세션 생성 후, 소스/싱크에 접근하기 전에 Hadoop Configuration에 주입한다. 읽기 세션의 설정은 쓰기 소유권을 부여하지 않는다.

업무 Delta 쓰기는 S3SingleDriverLogStore와 기존 MongoLease의 조합으로 제한한다. MongoLease가 아닌 임의/local 잠금은 허용하지 않는다. 모든 변경 경로/checkpoint는 같은 pj3-data의 canonical URI와 공용 원장을 써야 한다. 기존 lease의 만료/응답 미확인은 자동 takeover가 불가능하며, 이전 Driver와 작업 종료를 증명한 수동 복구가 필요하다. TTL로 잠금을 지우거나 외부 writer가 이를 우회하면 지원 전제가 깨진다. [Delta의 단일 Driver 전제](https://docs.delta.io/delta-storage/#single-cluster-setup-default).

Spark 4.1.3/Hadoop 3.4.2/Java21 서비스는 유지한다. 실제 소량 시험을 통과한 Delta는 **4.4.1의 Spark 4.1용 아티팩트**다. 기존 제안 4.1.0은 실제 서버에서 IgnoreCachedData 클래스 오류로 인수하지 못했다. Python에는 [서버 실행 요구사항](../requirements-spark-server.txt)을 사용하고 pip check/freeze를 보존한다. 기존 pyproject의 bronze/silver/gold extras는 로컬 4.0.1 시험용 고정값이므로 이 VM에서 사용하지 않는다.

JVM은 io.delta:delta-spark_4.1_2.13:4.4.1과 공식 POM의 전이 의존성을 함께 준비한다. Python 패키지 설치가 JVM JAR 배포를 대신하지 않는다. Delta runtime JAR만 복사하고 Unity Catalog 등의 전이 의존성을 누락하지 않는다. 최종 시험의 실제 JAR 목록/checksum은 검증 JSON에 있다. 운영 배포는 승인한 JAR 위치를 쓰고 다른 Hadoop/AWS SDK를 중복 추가하지 않는다. S3 CA 전용 truststore로 Maven 공인 인증서도 검증할 수 있다고 가정하지 말고 런타임 의존성을 미리 확보한다. 시험에서는 기존 Spark의 Ivy를 기본 공인 CA로 먼저 실행해 개인 캐시에 의존성을 확보한 뒤 S3 CA를 적용했다. 공개 CA 검증을 끄지 않았다.

## 배포·재개

기존 source-5533c0a.tar.gz와 고정 prepare_python_runtime.py에는 이번 보완이 없다. 그 코드에 새 설정만 적용하지 않는다. 현재 소스에서 새 release를 만들어 검증한 뒤 서비스 계정의 새 경로에 배포한다. 진행 중인 작업/환경을 자동 전환하지 않는다.

```bash
python run_operations.py release --source . --output .runtime/garage-v241-release
python run_operations.py release-verify --bundle .runtime/garage-v241-release
```

같은 버킷/기존 키/ID 및 중단 상태를 보존한다. Mongo/Kafka/MySQL 접속·DDL·업무 입력 인수와 전체 FLOW 시험은 별도다. 자동 테스트·실제 객체 시험·단일 Driver Delta 시험·전체 Worker/업무 잠금 시험을 구분한다.
