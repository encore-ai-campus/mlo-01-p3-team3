# 접속·권한·키 이관 및 접근 절차

2026-10-22 실제 로컬·서버 조회와 보호 이관 결과다. 비밀번호·개인키·Vault 원문은 이 문서와 Git에 포함하지 않는다. **sudo 비밀번호가 필요한 작업은 “공용 비밀번호 필요”**로 표시한다. 기존 서버 보호 설정과 로컬 과거 사본은 구분한다.

## 1. 이관 완료 위치와 의미

| 항목 | 확인 결과 |
|---|---|
| 보관 서버 | Bastion `192.168.0.111` |
| 암호화 보관본 | `/etc/pj3-secrets/operator-handoff/local-pc-20261022/credentials.tar.vault` |
| 이관 영수증 | 같은 디렉터리의 `custody-receipt.json` |
| 보호 | 디렉터리 root:root `0700`, 파일 root:root `0600` |
| 암호화 | 기존 Ansible Vault 사용. 해제 파일은 `/home/team3ops/.config/pj3/vault-pass`이며 보관본 안에 포함하지 않음 |
| 내용 | 프로젝트 SSH 키·설정·공개 호스트 키, MongoDB 설치 state·CA·클러스터 키, MySQL 게시·DB 접속 인계 자료 **64개 파일** |
| 검증 | 서버 최종 보관 위치에서 복호화한 묶음과 원본 묶음의 SHA-256 일치, 내부 manifest와 64개 파일 체크섬 모두 일치 |
| 임시 파일 | 로컬 임시 평문 묶음·Bastion 전송용 평문/중간 암호문 제거. 로컬 원본은 보존 |
| 제외 | 이 프로젝트와 별개인 AWS `keykey.pem`·AWS alias·비프로젝트 known_hosts 항목 |

암호문 SHA-256: `1102083c62a221be55ee38ea9163fea11efbe9d0bfdc24c4729a3e4da76997dd`.

이는 **서버 보호 보관본**이다. 운영 계정 비밀번호 교체, SSH 공개키 등록 변경, 인증서 재발급, DB·Kafka·NiFi 권한 신규 부여를 실행한 것이 아니다. 로컬의 NTFS 권한을 Linux 권한으로 그대로 옮기지 않고 root 전용 보호로 보관했다. 과거 자격이 현재 서버에서 유효하다는 의미도 아니다. 실행에는 아래의 현재 서비스 보호 설정을 사용한다.

서버의 사람이 읽는 문서 사본: `/home/team3ops/pj3-handoff/20261022/ACCESS-HANDOFF.md`. 최신 개발 문서는 `feature/#13`의 이 문서를 기준으로 한다. [이관 검증](verification/credential_custody_20261022.json).

## 2. 승인 담당자와 요청 채널

확인한 Git 문서·Bastion 운영 자료에는 시스템별 승인 담당자·실제 요청 채널이 확정된 목록으로 없다. `team3ops`는 공용 운영 계정이며 승인 담당자의 이름이 아니다. 인프라 역할 구분만으로 특정 팀원에게 발급 권한이 있다고 간주하지 않는다.

인계 시 팀에서 다음을 지정한다: SSH 공개키 등록 담당, sudo/Ansible 권한 담당, MongoDB·MySQL 계정 발급 담당, Garage 키·Kafka ACL 담당, NiFi 계정·인증서 담당, 요청 채널과 승인 기록 위치. **현재 미확정 사항이며 이번 이관으로 자동 해결되지 않는다.**

Bastion의 기존 운영 자료:

```text
/home/team3ops/pj3/OPERATIONS.md
/home/team3ops/pj3/VAULT-RUNBOOK.md
/home/team3ops/pj3/STAGE3-RUNBOOK.md
/home/team3ops/pj3/OBJECT-STORAGE-RUNBOOK.md
/home/team3ops/pj3/inventory/README.md
```

## 3. 후임자 PC → Bastion 접속

### 현재 로컬 원본과 실제 설정

| 용도 | 기존 PC 경로 |
|---|---|
| 기본 SSH 설정 | `C:\Users\Playdata\.ssh\config` |
| 프로젝트 alias | `C:\Users\Playdata\.ssh\pj3_hosts.conf` |
| 프로젝트 개인키·공개키 | 같은 디렉터리의 `pj3_member`, `pj3_member.pub` |
| 호스트 키 | 같은 디렉터리의 `known_hosts`; 서버 보관본에는 프로젝트 IP의 공개 항목만 포함 |

현재 `bastion`의 유효 접속은 **192.168.0.111 / team3ops / TCP 22**다. 기존 기본 config에는 이전 AWS `Host bastion`도 있으므로 앞부분의 `Include pj3_hosts.conf`가 우선한다. 후임자에게 예전 AWS alias를 그대로 복사해 혼동시키지 않는다.

후임자 PC는 본인 키와 서버 공개키 등록을 준비한다. 기존 파일이 있는 경로에 새 키를 덮어쓰지 않는다. 아래 설정의 `IdentityFile`은 승인·등록한 키를 가리켜야 한다.

```sshconfig
Host bastion
    HostName 192.168.0.111
    User team3ops
    Port 22
    IdentityFile ~/.ssh/pj3_member
    IdentitiesOnly yes
    StrictHostKeyChecking yes
    UserKnownHostsFile ~/.ssh/known_hosts
```

1. 후임자 공개키를 관리자가 Bastion `/home/team3ops/.ssh/authorized_keys`에 등록한다. 다른 계정 등록·시스템 변경은 **공용 비밀번호 필요**. 이번 작업에서 등록 내용은 바꾸지 않았다.
2. 관리자가 확인한 공개 호스트 키를 후임자 `known_hosts`에 등록한다. 현재 기존 PC의 ED25519 지문은 `SHA256:4t7fR4sg14LJcJDjP9yhGD4g9Bh9DdrzDRmZOaBLXJU`다. 최초 신뢰는 관리자 확인값과 대조한다. `ssh-keyscan` 결과만으로 신뢰를 확정하지 않는다.
3. `ssh -G bastion`에서 hostname·user·identityfile·호스트 키 검증 설정을 확인하고 `ssh bastion`으로 접속한다.
4. 다른 VM 직접 SSH가 필요하면 `pj3_hosts.conf`의 해당 alias와 각 서버의 공개키 등록을 확인한다. PC에서 파이프라인을 제어할 때는 Bastion의 기존 Ansible 경로를 사용한다.

완전히 새 PC가 서버 보관본의 개인키만으로 최초 접속을 시작할 수 있는 것은 아니다. 공개키 등록이나 관리자가 승인한 대체 접속 수단이 먼저 필요하다. 공용 sudo 비밀번호는 SSH 키와 별개다.

### 네트워크·포트

기존 PC는 `192.168.0.0/24`를 Wi-Fi로 직접 연결한다. 후임자가 외부에 있을 때의 VPN·라우팅·외부 방화벽 허용 절차는 **미확정**이다.

| 용도 | 실제 수신을 확인한 대표 주소 |
|---|---|
| SSH | Bastion `.111` 및 조회한 VM의 TCP `22` |
| Spark Master / 화면 | `192.168.0.121:7077` / `8080` |
| NiFi UI/API | `192.168.0.125:8443` |
| Kafka Broker / Controller | `192.168.0.126:9092` / `9093` |
| MongoDB | `192.168.0.134:27017` |
| MySQL | `192.168.0.131:3306` |
| Garage HTTPS S3 | `192.168.0.141:8443` |

조회 범위는 대표 노드 수신 상태이며 모든 복제 노드·외부 경로의 방화벽 허용 시험이 아니다. 서버 `/etc/ufw/user.rules`, `/etc/nftables.conf` 확인·변경은 **공용 비밀번호 필요**. Spark Worker↔Driver 통신도 필요하며 PC를 Driver로 바꾸는 절차가 아니다.

## 4. Bastion → Ansible → 서비스 계정 접근

| 대상 | Bastion 경로 |
|---|---|
| Ansible 작업·설정 | `/home/team3ops/pj3/ansible`, 그 안의 `ansible.cfg` |
| inventory | `/home/team3ops/pj3/inventory/hosts.yml` |
| 서버 접속 키 | `/home/team3ops/.ssh/pj3_ansible` |
| Vault 해제 파일 | `/home/team3ops/.config/pj3/vault-pass` |
| become 보호 변수 | `/home/team3ops/pj3/inventory/group_vars/all/pj3-become.vault.yml` |
| 서비스 보호 변수 | 같은 디렉터리의 `pj3-stage3.vault.yml`, `pj3-object-storage.vault.yml`, `pj3-storage.vault.yml` |
| 파이프라인 제어 모듈 | `/home/team3ops/pipeline-acceptance-0594f7b79ed347c5bf9e449cdb5912af/` |

현재 `team3ops`는 조회한 서비스 VM에서 `(ALL : ALL) ALL` sudo 범위다. 서비스 계정 `pj3-spark`, `pj3-nifi`는 `nologin`이며 이 계정으로 일반 SSH 로그인하지 않는다. **sudo/become: 공용 비밀번호 필요**. 기존 Ansible 실행은 Vault에서 become 값을 주입하며 `vault-pass`는 Vault 해제 자격이지 문서에 적을 sudo 비밀번호가 아니다.

접근 확인 예시(조회만 수행):

```bash
# Bastion의 team3ops 셸
cd /home/team3ops/pj3/ansible
ANSIBLE_BECOME_EXE=/usr/bin/sudo.ws ansible pj3-spark-master \
  -m command -a 'id' --become \
  --private-key /home/team3ops/.ssh/pj3_ansible \
  --vault-password-file /home/team3ops/.config/pj3/vault-pass
```

Master에서 서비스 설정·로그 접근(공용 비밀번호 필요):

```bash
sudo -u pj3-spark -- ls -ld /var/lib/pj3/product-pipeline/spark/config
sudo -u pj3-spark -- less /var/lib/pj3/product-pipeline/spark/production-pending-20261019/flow-progress.json
```

NiFi에서 서비스 로그 접근(공용 비밀번호 필요):

```bash
sudo -u pj3-nifi -- ls -ld /var/log/pj3/nifi
sudo journalctl -u pj3-nifi --since '1 hour ago' --no-pager
```

후임자 PC의 저장소 `src/data_pipeline`에서 `python run_server_pipeline.py status`, `python run_server_pipeline.py preflight`를 실행한다. 실행·한 배치·정지·재개는 [운영 인계](operator_handoff.md)를 따른다. `preflight`도 준비된 Bastion 계정·키·Vault 접근이 있어야 작동한다.

## 5. 현재 운영 보호 설정 접근

아래 파일은 존재·소유권을 확인했다. 일반 문서 사본이 아니며 내용 조회 대신 해당 서비스 프로그램의 보호 설정 로딩을 사용한다. 보호 파일을 열거나 서비스 계정으로 전환하려면 **공용 비밀번호 필요**.

| 대상·서버 | 현재 경로 |
|---|---|
| Mongo 계정 원본 / Mongo01 `.134` | `/etc/pj3-secrets/product-pipeline/mongodb-accounts.json` (root 0600) |
| Spark Mongo / Master `.121` | `/var/lib/pj3/product-pipeline/spark/config/mongo-accounts.json`, `mongo-ca.crt` (pj3-spark 0600) |
| NiFi Mongo / `.125` | `/var/lib/pj3/product-pipeline/nifi/config/mongo-accounts.json`, `mongo-ca.crt` (pj3-nifi 0600) |
| MySQL 게시 / Master | `/var/lib/pj3/product-pipeline/spark/mysql-publication-v2-20261018/mysql-publication.json`, `bootstrap.private.json`, `ca.pem` |
| MySQL 검수 / Master | `/var/lib/pj3/product-pipeline/spark/mysql-review-owner/mysql-review.json`, `owner.private.json` |
| MySQL 검수 인계 / MySQL01 `.131` | `/home/team3ops/.config/pj3-review/owner-transfer.json` |
| MySQL 서버 TLS / MySQL01 | `/etc/mysql/pj3-tls/ca.pem`, `server.pem`, `server.key` |
| Garage Spark / Master | `/etc/pj3-clients/s3/spark.properties` |
| Garage NiFi / `.125` | `/etc/pj3-clients/s3/nifi.properties` |
| Garage 공개 CA / 해당 클라이언트 서버 | `/etc/pj3-clients/s3/ca.pem` |
| Kafka Spark 설정 / Master | `/var/lib/pj3/product-pipeline/spark/config/kafka-spark.json`, `kafka-python.json` |
| Kafka NiFi 설정 / `.125` | `/var/lib/pj3/product-pipeline/nifi/config/kafka-python.json` |
| Kafka Spark TLS / Master·Workers | `/etc/pj3-clients/kafka/pipeline/keystore.p12`, `truststore.p12` (pj3-spark 0600) |

공개 CA를 전달할 때도 서비스별 신뢰 대상을 구분한다. Garage CA로 MySQL 인증을 대체하지 않는다. CA 개인키를 클라이언트 신뢰 파일로 배포하지 않는다.

원래 로컬 보관 경로(서버 암호화 묶음에 포함한 과거 자료):

```text
C:\Users\Playdata\AppData\Local\Team3Pipeline\mysql-publication-v2-20261018\
  ca.pem
  mysql-publisher.private.json
C:\Users\Playdata\AppData\Local\Team3Pipeline\server-connections-20261019\
  DB_접속정보_팀원인계_20261019.txt
  mongo-accounts.private.json
C:\Users\Playdata\AppData\Local\Team3MongoDB\bootstrap-20261017-132226\state\
  admin.json, ca.crt, ca.key, ca.srl, cluster.json, cluster.key
  deployment.spec, version.txt, node0/, node1/, node2/
```

## 6. Kafka ACL 조회·변경 요청

Kafka01 `.126`의 현재 설정은 `/etc/pj3/kafka/server.properties`, 관리 클라이언트 설정은 `/etc/pj3/kafka/client.properties`다. 아래 조회는 **공용 비밀번호 필요**이며 ACL을 변경하지 않는다.

```bash
sudo /opt/pj3/kafka/4.1.2/bin/kafka-acls.sh \
  --bootstrap-server 192.168.0.126:9092 \
  --command-config /etc/pj3/kafka/client.properties --list
```

2026-10-22 실제 조회 목록:

| 주체 | 리소스 | 허용 작업 |
|---|---|---|
| `User:CN=pj3-spark-master` | LITERAL TOPIC `product.raw.collected` | READ, WRITE, DESCRIBE, DESCRIBE_CONFIGS |
| 동일 주체 | LITERAL TOPIC `pj3-pipeline-verify-0594f7b79ed347c5bf9e449cdb5912af` | READ, WRITE, DESCRIBE, DESCRIBE_CONFIGS |
| 동일 주체 | CLUSTER `kafka-cluster` | IDEMPOTENT_WRITE |
| `User:CN=pj3-nifi` | PREFIXED TOPIC `pj3-stage3-e2e-1792233980076028114` | WRITE, DESCRIBE |
| 동일 주체 | 같은 이름의 PREFIXED TRANSACTIONAL_ID | WRITE, DESCRIBE |
| 동일 주체 | PREFIXED TOPIC `pj3-stage3-e2e-1792233829969379096` | WRITE, DESCRIBE |
| 동일 주체 | 같은 이름의 PREFIXED TRANSACTIONAL_ID | WRITE, DESCRIBE |

GROUP ACL과 NiFi의 운영 토픽 직접 발행 ACL은 조회 결과에 없다. 현재 NiFi의 Master 실행 호출과 직접 Kafka 발행은 별개다. Broker CN 3개는 `super.users`이며 무ACL 기본 허용은 false다. 이 목록은 ACL 원장을 파일로 복사한 것이 아니라 실제 Broker 조회 결과다.

변경 요청에는 principal, 리소스 종류·이름·patternType, 작업, 접속 host, consumer group 사용 여부를 명시하고 Kafka 권한 담당에게 승인받는다. 승인 담당자·채널은 아직 미확정이다. 후임자 이름으로 ACL을 자동 생성하거나 Broker superuser 자격을 일반 클라이언트에 배포하지 않았다.

## 7. NiFi 실제 인증·권한과 접근

UI `https://192.168.0.125:8443/nifi/`, API `https://192.168.0.125:8443/nifi-api/`.

실제 실행 설정은 `/var/lib/pj3/nifi/runtime-2.12.0/conf/`의 `nifi.properties`, `authorizers.xml`, `login-identity-providers.xml`이다. `/opt/pj3/nifi/2.12.0/conf/` 설치 사본과 혼동하지 않는다.

실행 프로세스 기준 `single-user-authorizer`·`single-user-provider`다. login provider에 Username/Password 항목이 있지만 원문은 이 문서에 출력하지 않았다. **후임자별 Process Group 권한·restricted ExecuteProcess 권한을 분리한 다중 사용자 원장이 현재 구성된 것은 아니다.** 활성 authorizer에는 Users File/Authorizations File 경로가 없다.

기존 서비스 인증서는 `/etc/pj3-secrets/nifi/{ca.pem,cert.pem,key.pem,keystore.p12,truststore.p12}`이며 `pj3-nifi` 소유 0600이다. 공개 인증서 Subject는 `CN=pj3-nifi`, Issuer는 `CN=PJ3 Stage3 Service CA`다. 서비스 API 클라이언트 자격이지 후임자 개인 운영자 인증서 발급 완료 증거가 아니다.

접근·설정 확인은 **공용 비밀번호 필요**. 재발급/구성 소스는 Bastion의 다음 파일이다.

```text
/home/team3ops/pj3/ansible/roles/nifi/tasks/main.yml
/home/team3ops/pj3/ansible/module_utils/pj3_nifi_config.py
/home/team3ops/pj3/ansible/module_utils/pj3_stage3_support.py
/home/team3ops/pj3/ansible/library/pj3_stage3_node.py
/home/team3ops/pj3/inventory/group_vars/all/pj3-stage3.vault.yml
```

현재 UI 계정 인계는 담당자가 별도 보호 채널로 진행한다. 개인 인증서·다중 사용자 권한이 필요하면 담당자를 지정하고 authorizer·신뢰 CA·사용자 식별·그룹·운영 PG read/write·restricted 실행 권한을 검토해 별도 변경·검증한다. 기존 서비스 인증서를 개인용으로 복사하면 개인별 감사·권한 분리가 생긴다고 설명하지 않는다.

운영 그룹/프로세서는 [NiFi 운영 연결](nifi_production.md)에 있다. 기존 제한 SSH는 다음 경로를 사용한다.

| 서버 | 파일 |
|---|---|
| NiFi | `/etc/pj3-clients/pipeline-trigger/ssh.conf`, `known_hosts` |
| NiFi | `/etc/pj3-secrets/pipeline-trigger/id_ed25519` (서비스 개인키) |
| Master | `/var/lib/pj3/pipeline-trigger/.ssh/authorized_keys` (root 소유) |
| Master | `/usr/local/libexec/pj3-pipeline-ssh-gate` |
| Master | `/etc/sudoers.d/pj3-pipeline-trigger-production` |

`pj3-pipeline-trigger → pj3-spark → /usr/local/libexec/pj3-pipeline-production-tick` 한 고정 sudo 명령은 **NOPASSWD**다. 정책 설치·수정은 **공용 비밀번호 필요**. 일반 shell·임의 명령 권한으로 확대하지 않는다. NiFi STOPPED는 이미 실행 중인 Driver를 종료하지 않는다.

## 8. 암호화 보관본 조회·복구 접근법

관리자가 승인한 작업에만 사용한다. **Bastion에서 공용 비밀번호 필요**. 아래 명령은 복구 절차 예시이며 이번 이관 때 운영 위치로 복원하지 않았다.

```bash
ssh bastion
sudo -i
umask 077
ls -ld /etc/pj3-secrets/operator-handoff/local-pc-20261022
sha256sum /etc/pj3-secrets/operator-handoff/local-pc-20261022/credentials.tar.vault
```

제1절의 암호문 SHA-256과 비교한다. 승인된 복구가 필요할 때만 root 전용 임시 디렉터리로 해제한다.

```bash
RESTORE_DIR=$(mktemp -d /root/pj3-handoff-restore.XXXXXX)
ansible-vault decrypt \
  --vault-password-file /home/team3ops/.config/pj3/vault-pass \
  --output "$RESTORE_DIR/credentials.tar.gz" \
  /etc/pj3-secrets/operator-handoff/local-pc-20261022/credentials.tar.vault
tar -tzf "$RESTORE_DIR/credentials.tar.gz"
tar -xzf "$RESTORE_DIR/credentials.tar.gz" -C "$RESTORE_DIR"
chmod -R go-rwx "$RESTORE_DIR"
```

`manifest.private.json`은 멤버별 원본 경로·크기·SHA-256을 담는다. 복구 후 파일 체크섬과 필요한 현재 계정/CA 대응을 대조하고 승인된 파일만 목적지에 배치한다. 운영 `/etc`·서비스 config에 전체 묶음을 덮어풀지 않는다. 복호화 결과를 터미널에 출력하거나 Git에 추가하지 않는다.

기존 SSH 개인키의 PC 복원이 꼭 필요하면 관리자가 해당 파일 하나를 `team3ops` 전용 0700 전송 디렉터리/0600 파일로 일시 내보내고 SSH/SCP로 전달한다. 후임자 PC의 기존 키를 덮어쓰지 않는 새 파일명으로 받고 사용자 전용 NTFS ACL을 적용한다. 검증 후 서버의 일시 평문 내보내기 파일을 제거한다. 기본 접속 인수는 후임자 공개키 등록으로 수행한다.

이 보관본은 기존 Vault 해제 자격에 의존한다. 해당 파일의 손실·교체 전에는 관리자가 암호화 보관본의 복구 가능성을 확인하고 필요 시 `ansible-vault rekey`로 보호 자격을 교체한다. Vault 해제 파일과 암호화 묶음을 일반 공유 폴더에 함께 배포하지 않는다.

## 9. 남은 인수와 금지 오해

- 승인 담당자·요청 채널, 후임자 외부망 VPN·라우팅, 자격 재발급·폐기 담당/순서, NiFi 개인별 권한 구성은 미확정이다.
- 이번 이관은 root 보호 보관과 접근 문서화다. 새 사람의 SSH·sudo·DB·Kafka·NiFi 권한이 자동 발급된 것은 아니다.
- 서버 원천·Delta·checkpoint·lease·업무 DB·승인 원장·NiFi 일정·서비스 비밀번호를 바꾸지 않았다.
- 파이프라인 운전·복구는 [운영 요약](operator_handoff.md), 상세 환경은 [배포 가이드](deployment.md)를 함께 따른다.
