# 서버 준비 명령어 — 2026-10-18

**Garage 후속 보완은 [전용 실행 프로필](garage_v241_profile.md)을 우선 따른다.** 아래 고정 5533c0a 배포물에는 새 프로필이 없다. 코드 전송/런타임 준비의 과거 기준으로 보존하며, 현재 Garage 설정은 이번 소스의 새 release와 함께 배포해야 한다.

기준은 실제 점검한 `5533c0a` 소스 묶음이다. **아래는 사용자가 실행할 명령이며 이번 문서 작성 중 서버에서 실행하지 않았다.** 서버를 설치 완료로 표시하는 문서가 아니다. 기존 서비스 재설치·클러스터 초기화·보안 해제·전체 데이터 적재는 포함하지 않는다.

현재 확정되지 않은 값은 담당자가 제공해야 한다: 서비스 DB명/DDL, 클라이언트 인증서·CA/계정, 저장소 endpoint/버킷. 경로를 추정하여 비밀 파일을 복사하거나 빈 값으로 다음 단계를 실행하지 않는다. 각 절은 해당 서버에서 별도로 실행한다. 실패하면 그 절에서 멈춘다.

## 0. 실행 위치

| 서버 | 작업 |
|---|---|
| NiFi 192.168.0.125 | 파이프라인 Python/접속 설정·읽기 전용 preflight |
| Spark Master 192.168.0.121 | 같은 Python 실행 프로필·최소 Spark/Delta 호환 시험 |
| Mongo01 192.168.0.134 | 기존 관리자 확인·필요한 파이프라인 계정 생성 |
| 현재 MySQL Primary | 실행 호스트별 게시 계정·권한·TLS 확인 |
| Kafka01 192.168.0.126 | 실제 보안 설정/승인 client 설정으로 metadata·토픽 확인 |
| Object01·02 | 제공된 Garage S3 가이드 적용. 신규 설치 제외 |

## 1. PC PowerShell — 준비 도구 전송

아래 작업 폴더에서 실행한다. 코드 묶음은 이미 두 서버의 incoming에 전송했지만 동일 파일을 다시 보낼 수 있다. 원본 수집기 폴더는 변경하지 않는다.

```powershell
cd 'C:\Users\Playdata\Documents\encore_MLops\3st_project\mlo-01-p3-team3-pipeline-13'

$targets = @('pj3-nifi', 'pj3-spark-master')
foreach ($target in $targets) {
    ssh $target 'umask 077; mkdir -p "$HOME/product-pipeline/incoming"'
    if ($LASTEXITCODE -ne 0) { throw "디렉터리 준비 실패: $target" }
    scp 'src/data_pipeline/.runtime/source-5533c0a.tar.gz' "${target}:product-pipeline/incoming/"
    if ($LASTEXITCODE -ne 0) { throw "코드 전송 실패: $target" }
    scp 'src/data_pipeline/docs/server_setup/prepare_python_runtime.py' 'src/data_pipeline/docs/server_setup/verify_spark_local.py' "${target}:product-pipeline/incoming/"
    if ($LASTEXITCODE -ne 0) { throw "도구 전송 실패: $target" }
}
```

## 2. NiFi와 Spark Master — 전용 Python 환경

각각 SSH 접속한 뒤 **같은 명령**을 실행한다.

```powershell
ssh pj3-nifi
# 나중에 별도 터미널에서: ssh pj3-spark-master
```

서버 Bash:

```bash
umask 077
python3 --version
java -version
python3 "$HOME/product-pipeline/incoming/prepare_python_runtime.py" --base-only
```

작업 내용은 검증된 압축파일 hash 확인·새 release 폴더 해제·별도 venv·고정 base 의존성/PyMySQL·PySpark 4.1.3/Delta 4.1.0 설치·pip check·두 CLI help 확인이다. OS Python과 기존 Spark 서비스는 변경하지 않는다. ensurepip가 없는 현재 서버에서는 공식 pip zipapp을 별도 venv에만 사용한다. 패키지 다운로드 실패/의존성 충돌 시 자동 대체 버전으로 바꾸지 않는다.

생성 경로:

```text
~/product-pipeline/releases/5533c0a
~/product-pipeline/venvs/py314-spark413-delta410
~/product-pipeline/audit/py314-spark413-delta410-setup.json
~/product-pipeline/audit/py314-spark413-delta410-freeze.txt
```

이 조합은 서버 Spark 버전에 맞춘 **새 검증 프로필**이다. 기존 Windows의 Spark 4.0.1/Delta 4.0.1 시험을 이 프로필의 검증으로 대신하지 않는다. Delta 공식 호환표는 4.1.x와 Spark 4.1.x의 대응을 명시하며, Spark 4.1 전용 Maven 좌표를 사용한다. [호환표](https://docs.delta.io/releases/), [4.1 릴리스](https://delta.io/blog/2026-03-01-delta-lake-4-1-0-released/), [pip zipapp](https://pip.pypa.io/en/stable/installation/).

## 3. Spark Master — 최소 로컬 동작 시험

후속 실제 시험에서 이 절의 옛 4.1.0 Delta 조합은 클래스 호환성 오류로 인수하지 못했다. 아래 옛 명령을 실행하지 않고 [현재 프로필](garage_v241_profile.md)의 Spark 4.1.3/Delta 4.4.1과 승인된 전이 의존성 배포를 사용한다. 2절의 --base-only는 Spark를 설치하지 않는다.

Spark Master에서 **한 번만** 실행한다. NiFi의 기존 Java 서비스와 메모리를 경쟁시키지 않는다.

```bash
PP="$HOME/product-pipeline"
PY="$PP/venvs/py314-spark413-delta410/bin/python"
export SPARK_LOCAL_IP=127.0.0.1
export PYSPARK_PYTHON="$PY"
export PYSPARK_DRIVER_PYTHON="$PY"
"$PY" "$PP/incoming/verify_spark_local.py"
```

`LOCAL_SPARK_DELTA_PASS`가 기준이다. local[1]에서 1행 저장·MERGE·고정 버전 읽기만 시험하고 임시 시험 테이블을 정리한다. 운영 lake나 기존 Delta를 건드리지 않는다. 실패 시 버전/다운로드/Java 오류부터 해결하고 cluster 실행으로 확대하지 않는다. 성공해도 Worker 전체·S3·Kafka 연결 검증 완료는 아니다.

## 4. Mongo01 — 관리자 및 파이프라인 계정

PowerShell:

```powershell
ssh pj3-mongo01
```

서버 Bash:

```bash
BUNDLE=/home/team3ops/mongodb-node0-20261017-132226
sudo bash "$BUNDLE/cluster.sh" admin "$BUNDLE"
sudo bash "$BUNDLE/cluster.sh" verify "$BUNDLE"
```

`admin`은 저장된 관리자 인증을 먼저 확인하고 최초 사용자 예외가 가능할 때만 생성한다. 기존 사용자가 있으면 비밀번호를 강제로 바꾸지 않는다. ADMIN_CONFIRMED/REPLICA_SET_READY가 안 나오면 여기서 멈춘다. `init`을 다시 실행하거나 authorization을 끄지 않는다. 이 verify는 시험 데이터 복제/장애 시험이 아니다.

계정 생성은 관리자 인증 성공 후 진행한다. 아래는 사용자명·역할을 명시한 새 스크립트이며 비밀번호는 파일/명령행에 적지 않고 입력창에서 받는다. **Mongo 원천 DB명은 승인된 이름을 입력한다.** 기존 같은 이름의 계정은 수정하지 않고 역할만 출력한다.

```bash
read -rp '승인한 MongoDB 원천 DB명: ' MONGO_APP_DB
[[ "$MONGO_APP_DB" =~ ^[A-Za-z0-9_.-]+$ ]] || { echo '잘못된 DB명'; exit 1; }
umask 077
SCRIPT=$(mktemp "$HOME/pipeline-mongo-users.XXXXXX.js")
cat > "$SCRIPT" <<'JS'
const fs=require('fs');
const c=JSON.parse(fs.readFileSync('/home/team3ops/mongodb-node0-20261017-132226/admin.json','utf8'));
const a=db.getSiblingDB('admin');
const authenticated=a.auth(c.username,c.password);
if (!(authenticated===1 || authenticated?.ok===1)) throw new Error('ADMIN_AUTH_REQUIRED');
if (!a.runCommand({hello:1}).isWritablePrimary) throw new Error('CURRENT_PRIMARY_REQUIRED');
const app=process.env.MONGO_APP_DB;
if (!app || !/^[A-Za-z0-9_.-]+$/.test(app)) throw new Error('APP_DB_REQUIRED');
for (const [user,roles] of [
  ['pipeline_ingest',[{role:'readWrite',db:app},{role:'clusterMonitor',db:'admin'}]],
  ['pipeline_spark',[{role:'read',db:app},{role:'readWrite',db:'pipeline_ops'},{role:'clusterMonitor',db:'admin'}]]
]) {
  const existing=a.getUser(user);
  if (existing) {print(JSON.stringify({user,status:'EXISTS_NOT_CHANGED',roles:existing.roles}));continue;}
  print('신규 계정 비밀번호 입력: '+user);
  const password=passwordPrompt();
  print('동일 비밀번호 재입력: '+user);
  if (!password || password!==passwordPrompt()) throw new Error('PASSWORD_CONFIRMATION_FAILED');
  a.createUser({user,pwd:password,roles},{w:'majority',j:true,wtimeout:30000});
  print(JSON.stringify({user,status:'CREATED'}));
}
JS
sudo env MONGO_APP_DB="$MONGO_APP_DB" mongosh --quiet --host localhost --tls \
  --tlsCAFile "$BUNDLE/ca.crt" --file "$SCRIPT"
# 비밀이 없는 방금 생성한 임시 스크립트만 정리
rm -- "$SCRIPT"
```

Bronze/Silver는 `pipeline_spark`, 원천 적재는 `pipeline_ingest`를 사용한다. 공용 lease용 pipeline_ops 쓰기와 원천 읽기를 분리한 역할이다. Gold lease도 pipeline_spark를 사용할 수 있다. 기존 계정의 출력 역할이 다르면 임의 자동 수정하지 않고 담당자가 대조한다.

공개 CA는 아래 파일이다. **cluster.key/node.pem/admin.json은 애플리케이션 서버로 복사하지 않는다.**

```text
/home/team3ops/mongodb-node0-20261017-132226/ca.crt
```

## 5. 현재 MySQL Primary — 원격 호스트 계정

우선 mysql01에서 조회한다. read_only 또는 super_read_only가 1이면 현재 Primary에서 진행한다. 이 문서는 Replica의 read_only를 해제하지 않는다.

```powershell
ssh pj3-mysql01
```

```bash
sudo env MYSQL_HISTFILE=/dev/null mysql
# socket 관리자 인증이 안 되면 실제 관리 계정으로 mysql -u 계정명 -p
```

MySQL 셸:

```sql
SELECT @@hostname, @@version, @@global.read_only, @@global.super_read_only;
SELECT User, Host FROM mysql.user WHERE User = 'pipeline_publisher';
SHOW VARIABLES WHERE Variable_name IN ('ssl_ca','ssl_cert','require_secure_transport');
```

Primary를 확인한 뒤 아래 계정을 만든다. **이미 존재하는 계정은 CREATE USER를 반복하거나 비밀번호를 변경하지 않는다.** RANDOM PASSWORD 결과는 각 호스트의 실제 비밀번호이므로 보호된 환경 파일에 별도로 보관한다. 채팅·Git으로 보내지 않는다.

```sql
CREATE USER 'pipeline_publisher'@'192.168.0.125'
  IDENTIFIED BY RANDOM PASSWORD REQUIRE SSL;
CREATE USER 'pipeline_publisher'@'192.168.0.121'
  IDENTIFIED BY RANDOM PASSWORD REQUIRE SSL;
```

다음은 **`APP_DB`를 실제 승인된 서비스 DB명으로 바꾼 뒤** 실행한다. 서비스 DDL이 아직 없다면 GRANT/게시 preflight 완료를 주장하지 않는다. 참조 DDL을 기존 서비스 DB에 덮어쓰지 않는다.

```sql
GRANT SELECT ON `APP_DB`.* TO 'pipeline_publisher'@'192.168.0.125';
GRANT SELECT ON `APP_DB`.* TO 'pipeline_publisher'@'192.168.0.121';

GRANT INSERT, UPDATE ON `APP_DB`.`products` TO 'pipeline_publisher'@'192.168.0.125';
GRANT INSERT, UPDATE ON `APP_DB`.`offers` TO 'pipeline_publisher'@'192.168.0.125';
GRANT INSERT, UPDATE ON `APP_DB`.`offer_snapshots` TO 'pipeline_publisher'@'192.168.0.125';
GRANT INSERT, UPDATE ON `APP_DB`.`assets` TO 'pipeline_publisher'@'192.168.0.125';
GRANT INSERT, UPDATE ON `APP_DB`.`product_images` TO 'pipeline_publisher'@'192.168.0.125';
GRANT INSERT, UPDATE ON `APP_DB`.`pipeline_product_versions` TO 'pipeline_publisher'@'192.168.0.125';
GRANT INSERT, UPDATE ON `APP_DB`.`pipeline_publications` TO 'pipeline_publisher'@'192.168.0.125';
GRANT INSERT, UPDATE ON `APP_DB`.`pipeline_publication_staging` TO 'pipeline_publisher'@'192.168.0.125';

GRANT INSERT, UPDATE ON `APP_DB`.`products` TO 'pipeline_publisher'@'192.168.0.121';
GRANT INSERT, UPDATE ON `APP_DB`.`offers` TO 'pipeline_publisher'@'192.168.0.121';
GRANT INSERT, UPDATE ON `APP_DB`.`offer_snapshots` TO 'pipeline_publisher'@'192.168.0.121';
GRANT INSERT, UPDATE ON `APP_DB`.`assets` TO 'pipeline_publisher'@'192.168.0.121';
GRANT INSERT, UPDATE ON `APP_DB`.`product_images` TO 'pipeline_publisher'@'192.168.0.121';
GRANT INSERT, UPDATE ON `APP_DB`.`pipeline_product_versions` TO 'pipeline_publisher'@'192.168.0.121';
GRANT INSERT, UPDATE ON `APP_DB`.`pipeline_publications` TO 'pipeline_publisher'@'192.168.0.121';
GRANT INSERT, UPDATE ON `APP_DB`.`pipeline_publication_staging` TO 'pipeline_publisher'@'192.168.0.121';
```

카테고리/게시 guard는 SELECT만 사용한다. 사용자·추천·검수·승인 원장 쓰기나 DDL 권한은 부여하지 않는다. 같은 이름의 계정이라도 Host별 비밀번호는 다를 수 있다. 다른 실행 호스트에서 게시할 경우 그 승인된 IP에만 추가한다. [MySQL 사용자/TLS](https://dev.mysql.com/doc/refman/8.4/en/create-user.html).

## 6. Kafka01 — 실제 TLS client 설정 확인

```powershell
ssh pj3-kafka01
```

서버 Bash:

```bash
sudo find /etc/pj3/kafka -maxdepth 4 -type f \
  \( -name '*client*.properties' -o -name '*.crt' -o -name '*.pem' -o -name '*.p12' \) -print
```

승인된 파이프라인용 client properties가 있으면 다음을 실행한다. 없으면 인프라 담당이 실제 listener의 SSL/SASL_SSL·클라이언트 인증 요구에 맞게 발급해야 한다. 브로커 자체 인증서/개인키를 파이프라인 클라이언트에 복사하지 않는다.

```bash
read -rp 'Kafka advertised 주소 기반 bootstrap(host:port): ' BOOTSTRAP
read -rp '승인된 Kafka client properties 절대 경로: ' CLIENT_PROPERTIES
test -n "$BOOTSTRAP" && sudo test -f "$CLIENT_PROPERTIES" || exit 1
KAFKA=/opt/pj3/kafka/4.1.2

sudo "$KAFKA/bin/kafka-broker-api-versions.sh" \
  --bootstrap-server "$BOOTSTRAP" --command-config "$CLIENT_PROPERTIES"
sudo "$KAFKA/bin/kafka-topics.sh" \
  --bootstrap-server "$BOOTSTRAP" --command-config "$CLIENT_PROPERTIES" \
  --describe --topic product.raw.collected
```

실제로 Topic이 없을 때에만 최초 생성:

```bash
sudo "$KAFKA/bin/kafka-topics.sh" \
  --bootstrap-server "$BOOTSTRAP" --command-config "$CLIENT_PROPERTIES" \
  --create --if-not-exists --topic product.raw.collected \
  --partitions 3 --replication-factor 3 --config min.insync.replicas=2
```

기존 Topic은 `ReplicationFactor: 3`, ISR 2개 이상, 유효 min.insync.replicas 2 이상을 확인한다. 기존 RF가 다르면 현재 broker ID·partition 배치를 확인한 별도 재할당 계획이 필요하며 이 문서는 임의 ID로 재할당하지 않는다. 다음 명령은 설정 조회다.

```bash
sudo "$KAFKA/bin/kafka-configs.sh" \
  --bootstrap-server "$BOOTSTRAP" --command-config "$CLIENT_PROPERTIES" \
  --entity-type topics --entity-name product.raw.collected --describe
```

RF 3·ISR 2 이상을 확인했고 기존 유효 minISR이 2보다 작을 때만, 승인한 토픽의 설정을 변경한다. 이미 2 이상이면 건너뛴다.

```bash
sudo "$KAFKA/bin/kafka-configs.sh" \
  --bootstrap-server "$BOOTSTRAP" --command-config "$CLIENT_PROPERTIES" \
  --entity-type topics --entity-name product.raw.collected \
  --alter --add-config min.insync.replicas=2
```

계정/ACL은 producer의 WRITE/DESCRIBE/DESCRIBE_CONFIGS 및 idempotent write, consumer의 READ/DESCRIBE와 실제 group 권한을 용도별로 발급한다. 초기 확인에서는 produce/consume을 반복하지 않는다. TLS listener에 PLAINTEXT로 접속하거나 endpoint 검증을 끄지 않는다. [Kafka SSL](https://kafka.apache.org/27/security/encryption-and-authentication-using-ssl/).

## 7. CA·클라이언트 설정을 NiFi/Driver에 전달

담당자가 제공할 파일은 공개 CA, 파이프라인 전용 Kafka client 인증서/키 또는 SASL 설정, Spark client 설정이다. 서비스 관리자 비밀을 대신 사용하지 않는다. 사용자 홈의 config는 0700, 비밀 파일은 0600으로 유지한다.

```bash
umask 077
mkdir -p "$HOME/product-pipeline/config/tls"
chmod 700 "$HOME/product-pipeline/config" "$HOME/product-pipeline/config/tls"
```

Mongo 공개 CA 전달 예시 — PC PowerShell:

```powershell
$ca = Join-Path $env:TEMP 'pj3-pipeline-mongo-ca.crt'
scp 'pj3-mongo01:/home/team3ops/mongodb-node0-20261017-132226/ca.crt' $ca
if ($LASTEXITCODE -ne 0) { throw 'Mongo 공개 CA 복사 실패' }
scp $ca 'pj3-nifi:product-pipeline/config/tls/mongo-ca.crt'
scp $ca 'pj3-spark-master:product-pipeline/config/tls/mongo-ca.crt'
```

Replica Set에 광고된 `mongo01/02/03.pj3.internal`은 Driver/NiFi에서 올바른 주소로 해석돼야 한다. 먼저 확인한다. 잘못된 기존 DNS를 hosts로 덮지 않는다.

```bash
getent ahostsv4 mongo01.pj3.internal
getent ahostsv4 mongo02.pj3.internal
getent ahostsv4 mongo03.pj3.internal
```

예상 주소는 각각 192.168.0.134/135/136이다. 해석이 없거나 다르면 인프라 DNS/hosts를 해당 광고 주소와 일치시키는 작업이 필요하다. Kafka도 advertised.listeners의 모든 주소를 Driver/Workers가 해석·접속해야 한다.

Spark UI는 실제 HTTPS URL과 CA로 검증한다. URL과 CA 파일을 직접 입력한다.

```bash
read -rp '실제 Spark Master HTTPS UI URL: ' SPARK_UI
read -rp '공개 CA 파일 절대 경로: ' SPARK_CA
curl --fail --show-error --cacert "$SPARK_CA" "$SPARK_UI/json/"
```

404면 UI의 실제 base path, 인증 요구가 있으면 제공된 인증 방식을 확인한다. CA 해결과 Spark RPC(7077)의 인증/암호화는 별도다. 운영 cluster-submit은 기존 `/etc/pj3/spark/`의 승인된 client 설정을 사용할 수 있게 인프라 담당이 제공해야 한다. 서비스 secret 파일을 화면에 출력하거나 Spark 인증을 끄지 않는다.

## 8. Object Storage — 기존 Garage 구성 적용

**기본 conditional 프로필은 실제 시험에서 기존 객체 보호에 실패했다.** 이후 [Garage 전용 내용 주소 프로필](garage_v241_profile.md)을 추가해 객체 저장을 보완했다. [최신 검증 결과](garage_v241_validation_20261018.md)의 범위 안에서 새 소스/설정을 배포하며, 일반 S3 성공을 Delta/전체 FLOW 인수로 확대하지 않는다.

이전 점검에서는 endpoint를 확인하지 못했으나, 이후 사용자가 제공한 TEAM-S3-GUIDE.md에 기존 Garage S3와 네이티브 클라이언트 검증 결과가 명시되어 있다. 신규 설치 제안을 적용하지 않고 기존 구성을 재사용한다. 이번에 서버를 다시 시험한 결과는 아니다. [Garage S3 적용 검토](garage_s3_integration.md)를 함께 따른다.

| 항목 | 적용 값 |
|---|---|
| 기본 endpoint | https://192.168.0.141:8443 |
| 같은 클러스터의 다른 endpoint | https://192.168.0.142:8443 |
| 버킷 / region | pj3-data / us-east-1 |
| 클라이언트 | SigV4 / path-style / TLS 검증 유지 |
| CA | /etc/pj3-clients/s3/ca.pem |

한 노드 장애 시 쓰기 503이 발생한다. endpoint 교체로 쓰기 고가용성을 얻지 못한다. 인계/이미지는 같은 pj3-data의 기존 키 경로를 보존하며 lake/checkpoint는 승인된 prefix를 사용한다. prefix는 버킷 전체 read/write 권한을 제한하는 보안 경계가 아니다.

보호 키는 기존 pj3-nifi/pj3-spark 계정 파일에서만 취급하고 team3ops에 읽기 권한을 추가하거나 수동 복사하지 않는다. 기존 NiFi Controller Service의 키가 Python 프로세스에 자동 전달되지는 않는다. 서비스 계정의 자격 주입·Python 실행 경로를 준비한 뒤 해당 계정으로만 아래 읽기 확인을 수행한다. PY는 그 계정이 실행 가능한 전용 Python 경로다.

```bash
export PIPELINE_S3_ENDPOINT=https://192.168.0.141:8443
export AWS_DEFAULT_REGION=us-east-1
export AWS_CA_BUNDLE=/etc/pj3-clients/s3/ca.pem
: "${PY:?서비스 계정의 전용 Python 경로가 필요합니다}"
"$PY" - <<'PY'
import os,boto3
from botocore.config import Config
s3=boto3.client('s3',endpoint_url=os.environ['PIPELINE_S3_ENDPOINT'],region_name='us-east-1',config=Config(signature_version='s3v4',s3={'addressing_style':'path'},connect_timeout=5,read_timeout=10,retries={'total_max_attempts':1}))
s3.head_bucket(Bucket='pj3-data')
print('S3_HEAD_BUCKET_OK')
PY
```

이 예시는 Java properties의 accessKey/secretKey를 자동으로 읽지 않는다. 자격 주입 전에는 실행하지 않는다. 현재 업무 코드의 조건부 PUT(IfNoneMatch), SDK 전송 형식과 Delta 저장/재개는 별도 검증이 필요하다. Spark는 가이드에 명시된 기존 로컬 JAR·truststore·SPARK_CONF_DIR를 유지한다. 일반 S3A 검증을 Delta 검증으로 간주하지 않는다.

## 9. 마지막 읽기 전용 파이프라인 preflight

위 준비가 끝난 NiFi 서버에서 설정 예시를 새 파일로 복사하고 실제 값으로 편집한다. `cp -n`은 기존 파일을 덮지 않는다.

```bash
PP="$HOME/product-pipeline"
SRC="$PP/releases/5533c0a"
PY="$PP/venvs/py314-spark413-delta410/bin/python"
umask 077
mkdir -p "$PP/config"
cp -n "$SRC/config/environment.example.json" "$PP/config/environment.local.json"
cp -n "$SRC/config/mysql_publication.example.json" "$PP/config/mysql_publication.local.json"
nano "$PP/config/environment.local.json"
nano "$PP/config/mysql_publication.local.json"
```

environment에는 승인 environment_id/Mongo DB와 image_bucket/handoff_bucket에 pj3-data를 넣는다. 서비스 계정의 자격 주입·실행 경로 준비 전에는 아래 preflight를 실행하지 않는다. MySQL에는 현재 Primary의 인증서와 일치하는 DNS, 승인 DB, 해당 호스트의 publisher 계정과 password_env/ca_env를 넣는다. placeholder를 그대로 쓰면 차단된다.

Mongo URI·Kafka librdkafka JSON·S3/MySQL 비밀은 Git 밖 `config/pipeline.env`에 준비한다. URI는 비밀번호의 특수문자를 percent-encode하고 `authSource=admin`, `replicaSet=rs-products`, `tls=true`, `tlsCAFile`을 포함한다. Kafka는 실제 listener에 맞는 SSL 또는 SASL_SSL, CA/클라이언트 인증, bootstrap을 사용한다. 자바용 client.properties를 librdkafka JSON으로 그대로 복사하지 않는다.

필수 환경변수:

```text
PIPELINE_MONGO_URI
PIPELINE_S3_ENDPOINT
PIPELINE_KAFKA_CONFIG
AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY / AWS_CA_BUNDLE 또는 승인된 credential chain
PIPELINE_MYSQL_PASSWORD / PIPELINE_MYSQL_CA
```

Bronze 이후에는 BRONZE_KAFKA_OPTIONS(Java kafka.* 옵션), BRONZE/SILVER/GOLD/PUBLICATION_SPARK_OPTIONS와 lease URI, 실제 Kafka cluster ID·공용 lake/checkpoint 설정도 별도 필요하다. 현재 단계에서는 원천/게시 preflight까지만 실행한다.

```bash
nano "$PP/config/pipeline.env"
chmod 600 "$PP/config/pipeline.env" "$PP/config/"*.local.json
set -a
. "$PP/config/pipeline.env"
set +a
"$PY" "$SRC/run_pipeline.py" preflight --config "$PP/config/environment.local.json"
"$PY" "$SRC/run_publication.py" preflight --config "$PP/config/mysql_publication.local.json"
```

원천 preflight의 `read_preflight_passed`와 게시 preflight의 `MYSQL_SCHEMA_VERIFIED`가 기준이다. 실제 DDL이 다르거나 테이블이 없으면 실패를 유지하고 담당 DDL과 계약을 대조한다. 참조 DDL을 서비스 DB에 자동 적용하지 않는다. 이 단계에서 ingest/publish --apply/flow-run/NiFi --start 명령은 실행하지 않는다.

## 10. 실행 후 공유할 결과

다음 결과만 공유한다. 비밀번호/개인키/env 원문은 보내지 않는다.

- 두 Python 설치 도구의 최종 status 및 오류 코드.
- Spark의 LOCAL_SPARK_DELTA_PASS 또는 짧은 오류 유형.
- Mongo ADMIN_CONFIRMED/REPLICA_SET_READY와 생성된 계정의 역할.
- MySQL 현재 Primary 여부·접속 성공/실패, 실제 서비스 DDL 제공 여부.
- Kafka metadata/Topic의 RF·ISR·minISR.
- 저장소 기존 서비스가 있는지, 신규 제품 결정이 필요한지.
- 마지막 두 preflight의 결과.

여기까지는 준비 및 최소 검증이다. 실제 데이터 적재·전체 cluster job·NiFi 일정 활성화는 후속 단계다.

## 명령서 자체 검증

2026-10-18 작성 환경에서 Python 도구 2개의 구문 및 --help, Bash 블록 17개의 bash -n(최초 작성 기준), PowerShell 블록 7개의 구문 검사를 통과했다. 공식 PyPI에 pip 26.2.1/PySpark 4.1.3/Delta 4.1.0이 제공되는지 확인했으며, 버전 고정 pip zipapp의 실제 배포 주소는 `https://bootstrap.pypa.io/pip/zipapp/pip-26.2.1.pyz`(HTTP 200)다. 이 확인은 패키지 설치·SQL/계정 변경·서버 Spark 시험의 성공을 의미하지 않는다.
