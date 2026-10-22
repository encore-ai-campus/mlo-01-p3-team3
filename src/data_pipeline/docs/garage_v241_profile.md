# Garage 2.4.1 실행 프로필

## 객체의 내용 주소 계약

Garage 2.4.1의 원자적 조건부 PUT을 가정하지 않는다. 기존 conditional 프로필은 유지하고 이 환경은 명시적으로 garage_v241을 선택한다. 아래 원인·실제 검증 근거와 [전체 실서버 검증](service_validation_20261018.md)을 함께 확인한다.

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


## 인프라 가용성과 검증 근거

팀 TEAM-S3-GUIDE 기준 .141/.142:8443은 같은 Garage 클러스터이며 버킷은 pj3-data다. consistent/RF2의 한 노드 장애 시 읽기는 가능하지만 쓰기는 503으로 거절된다. endpoint 교체가 쓰기 가용성을 복구하지 않는다. 이미지·인계 청크·Delta·checkpoint가 같은 용량을 쓰며 문서의 약 80GB는 엄격한 quota가 아니다. 실패 단계를 보존하고 양 노드 복구 뒤 제한적으로 재개한다.

고정 S3A JAR은 hadoop-aws 3.4.2와 AWS bundle 2.29.52이며 기존 /opt/pj3/s3-clients/hadoop-3.4.2/ 배치를 사용한다. NiFi Controller Service의 키는 Python child에 자동 주입되지 않는다. Java/Python 각각 역할별 보호 파일을 메모리에서 읽어야 한다.

### Worker JAR 누적 방지 (2026-10-21 적용)

`spark_storage.storage_options`는 고정 S3A JAR 2개를 `spark.jars` 배포 목록에 추가하지 않는다. 기존 `local:` 항목도 제거하고 Driver/Executor의 `extraClassPath`에 동일한 `/opt/pj3/s3-clients/hadoop-3.4.2/` 원본 경로를 지정한다. 다른 Kafka/Delta JAR 배포는 유지한다. 별도 SDK 버전 충돌은 차단하며, 공유 Spark 세션의 동일성 검사에도 두 classpath 설정을 포함한다. 새 Driver JVM이 시작되기 전에 적용해야 하며 이미 시작한 JVM에 설정만 바꿔 소급 적용하지 않는다. 모든 Master/Worker에 같은 경로와 체크섬의 원본을 미리 준비한다.

큰 AWS bundle은 기존 641,534,749바이트 원본을 직접 읽는다. `/opt` 원본과 원천 데이터·Delta·체크포인트는 정리 대상이 아니다. 나머지 작업별 JAR과 executor stdout/stderr는 Spark 기본 Worker 정리 기능으로 관리한다. 종료된 작업 중 보관 기준 1시간이 지난 폴더를 60초마다 정리한다. 실행 중인 작업은 대상에서 제외한다. 중앙 Driver 로그·단계별 결과와 복구 원장은 별도 경로에 유지한다. [Spark 4.1.3의 Worker 정리 설정](https://spark.apache.org/docs/4.1.3/spark-standalone.html)에 따른 정책이며 즉시 삭제나 디스크 사용량의 강제 상한은 아니다. 기본 7일 보관은 이 서버의 40GB 디스크에 길어 명시적으로 단축했다.

각 Worker의 `/etc/pj3/spark/spark-env.sh`에 다음 관리 블록을 적용한다. 다른 JVM/TLS 옵션을 덮어쓰지 않는다.

```bash
export SPARK_WORKER_OPTS="${SPARK_WORKER_OPTS:-} -Dspark.worker.cleanup.enabled=true -Dspark.worker.cleanup.interval=60 -Dspark.worker.cleanup.appDataTtl=3600"
```

재현 가능한 [노드별 적용 도구](server_setup/configure_spark_worker_cleanup.py)는 기본 조회만 수행한다. 유지보수 시간에 모든 업무 Driver를 중지하고 Master UI에서 실행 중인 앱이 없음을 확인한 뒤 각 Worker에서 다음을 실행한다. 도구는 실행 중인 executor가 있으면 거절하며 설정의 기존 권한·소유자·다른 옵션을 보존한다. 백업은 고정 이름 하나로만 남기고 직접 작업 폴더를 삭제하지 않는다.

```bash
sudo python3 configure_spark_worker_cleanup.py
sudo python3 configure_spark_worker_cleanup.py --apply
sudo systemctl restart pj3-spark.service
```

재시작은 Worker를 한 대씩 수행하고 3대가 모두 ALIVE로 복귀했는지 확인한다. 이후 인프라 템플릿으로 `spark-env.sh`를 재배포할 때도 이 블록을 유지한다. Spark Master의 정상 보호 실행 환경에서 [읽기 전용 시험](server_setup/verify_spark_shared_jars.py)을 별도 Python 프로세스로 두 번 실행하면 공용 원본의 실제 로딩 위치·실제 Delta 입력 1행 읽기·Worker 참여를 확인할 수 있다. Worker 복사본 개수는 앱 밖에서 따로 비교한다.

실제 시험 앱 `app-20261021094030-0022`(30.33초), `app-20261021094119-0023`(34.84초)에서 3개 Worker 참여, 실제 Bronze/Delta 1행 읽기, 공용 JAR 경로 로딩을 확인했다. 두 실행 후 Worker별 bundle 복사본은 모두 0개였고 원본 SHA256은 `62638f33045690c6bc5e496b54b0b2e89a9ad38cbedacdf6e017fea6bc85f55c`로 유지됐다. 3대 모두 오래된 격리 시험 폴더는 자동 삭제되고 최근 시험 폴더는 보존됐다. [실제 조회 결과](verification/spark_worker_cleanup_20261021.json)를 보존한다. 최초 시험은 시험 코드가 잘못 지정한 테이블 경로 때문에 실패했고, 실제 `/bronze/products` 경로를 확인해 수정한 뒤 위 두 시험을 통과했다. 이를 업무 전체 배치 성공으로 계산하지 않는다. 관련 로컬 자동 테스트는 26개다.

초기 실패는 SDK before-send에서 If-None-Match: *의 존재를 확인했음에도 PUT 200과 다른 본문의 GET을 관측한 것이다. Caddy는 loopback reverse_proxy 구성이었고, [Garage v2.4.1 PUT 구현](https://github.com/deuxfleurs-org/garage/blob/v2.4.1/src/api/s3/put.rs)은 해당 조건을 판정하지 않는다. GET/HEAD 조건 처리와 혼동하지 않는다. 프록시 전 구간 패킷을 캡처한 시험은 아니다. 이후 버전에 대한 일반 판정도 아니다.

- 초기 실패: [NiFi](verification/garage_20261018/nifi-result.json), [Spark](verification/garage_20261018/spark-result.json), [조건 헤더 진단](verification/garage_20261018/nifi-header-diagnosis.json). 시험 객체 삭제/반대 endpoint 404 확인. 실패를 성공으로 재분류하지 않는다.
- 내용 주소 프로필: [NiFi](verification/garage_20261018/nifi-profile-result.json), [Spark](verification/garage_20261018/spark-profile-result.json). 각각 2개 전용 객체의 최초 저장·재사용·반대 endpoint checksum·PUT 응답 유실 후 GET 복구·다른 바이트 사전 차단·삭제를 확인했다. [실제 코드](server_setup/pipeline_garage_profile_verify.py).
- Delta 4.4.1 local[1]: [최종 결과](verification/garage_20261018/delta441-result.json). 최초 1행→MERGE 2행, version 0의 1행, 새 세션의 2행, prefix 삭제를 구조화 proof로 대조했다. [Scala](server_setup/garage_delta_verify.scala), [실행기](server_setup/pipeline_garage_delta_verify.py).
- 이전 실패: [Delta 4.1.0 클래스 오류](verification/garage_20261018/delta410-result.json), [의존성 오분류 차단](verification/garage_20261018/delta441-blocked.txt), [실패 prefix의 빈 상태 조사](verification/garage_20261018/delta-prefix-audit.json). 이후 Worker 3대·실제 업무 잠금/엔진 시험은 실서버 검증 문서를 따른다.

최종 전체 자동 회귀는 252개이며 초기 244/245개 결과와 합산하지 않는다. 공개 결과 JSON과 검증 코드는 유지하고, 중복 초기 계획·분석 보고는 이 절로 통합했다. 저장소 자체 WORM이나 다중 Driver 자동 인계를 검증한 것으로 확대하지 않는다.
