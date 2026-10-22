# MongoDB VM별 설치 도구

2026-10-17. 같은 `install.sh`를 각 VM에서 실행한다. 설치는 현재 VM만 변경하며 다른 VM에 SSH 접속하거나 Replica Set을 초기화하지 않는다. `deploy.sh`의 기존 3대 일괄 원격 설치 기능은 제거했다.

## 파일별 책임

| 파일 | 책임 |
|---|---|
| deploy.sh | 관리 환경에서 공통 CA·클러스터 키와 VM별 설치 묶음을 로컬 생성. 서버 접속 없음 |
| install.sh | 해당 VM의 사전 점검 또는 설치·동일 설정 재개 |
| node.sh | OS·경로·패키지·TLS·systemd 처리 공통 함수 |
| cluster.sh / bootstrap.js | 명시적으로 실행하는 초기화·관리자 생성·Replica Set 검증. 설치에서 호출하지 않음 |

기존 `deploy.sh --apply`, `--resume`, `--check` 원격 일괄 명령은 사용할 수 없다. 설치 묶음 생성은 `--prepare`, VM별 설치는 `install.sh --apply`로 구분한다.

## 상위 설계와 적용 범위

- `docs/design/01-infrastructure.md:77` 기준 최종 목표는 `rs-products`, 데이터·투표 멤버 3대, Primary 1·Secondary 2, Arbiter 없음이다. Primary는 선출되며 특정 VM으로 고정하지 않는다.
- 서버 한 대의 설치 성공은 3대 Replica Set 구축 완료가 아니다. 한 대만 설치한 상태에서 프로젝트의 운영 준비 완료나 장애 허용을 선언하지 않는다.
- 파이프라인은 `w=majority`, `journal=true`, `readPreference=primary`, `readConcern=majority`를 사용한다. 원천·컬렉션·인덱스·애플리케이션 계정 생성은 설치 도구의 책임이 아니다.
- 실제 URI·DB 이름·인증정보는 Git 밖 배포 설정으로 관리한다. advertised 이름은 모든 멤버와 NiFi/Python·Spark에서 해석·접근 가능해야 한다.
- `docs/design/06-operations-migration.md:128`의 선출·재연결·과반수 상실·백업 복원은 별도 실제 인수 시험이다. 복제를 백업으로 간주하지 않는다.

현재 지원 환경은 Ubuntu 26.04/resolute, systemd, x86_64/AVX, MongoDB Community 9.0.x 중 9.0.4 이상이다. 다른 OS·아키텍처·버전을 자동 지원한다고 표시하지 않는다. 기본 서버 버전은 9.0.4로 정확히 고정한다. 커널 6.19~7.0.13은 차단하며 Ubuntu upstream patch는 `/proc/version_signature`로 확인한다.

공식 근거: [플랫폼 지원](https://www.mongodb.com/docs/community-platform-support/), [Keyfile Replica Set 구축](https://www.mongodb.com/docs/manual/tutorial/deploy-replica-set-with-keyfile-access-control/), [운영 요구사항](https://www.mongodb.com/docs/manual/administration/production-notes/).

## 1. 공통 자료는 한 번 준비

SSH 계정, 인증키와는 별개로 MongoDB용 CA와 클러스터 인증키를 준비한다. 관리용 Linux 환경에서 Bash·OpenSSL·Python 3·sha256sum이 필요하다. 아래 경로는 Git 밖이어야 하며 새 디렉터리여야 한다.

```bash
bash deploy.sh \
  --inventory inventory.example.tsv \
  --data-dir /var/lib/mongodb \
  --state-dir "$HOME/mongodb-bootstrap" \
  --replica-set rs-products \
  --version 9.0.4 \
  --prepare
```

이 명령은 서버에 접속하지 않는다. inventory의 멤버 수를 코드에 고정하지 않고 1~7개의 데이터·투표 멤버와 고유 ID를 받는다. 프로젝트 예시는 3대다. 주소·데이터 경로·Replica Set 이름·지원 범위 내 버전을 설정할 수 있다. 포트 27017, cache 0.5GiB, oplog 1024MiB는 현재 고정 기본값이다.

`state/node0`, `node1`, `node2`에 서버별 주소·SAN 인증서와 동일 CA·클러스터 키를 배치한다. 각 묶음에는 `bundle.sha256`이 있다. CA 개인키와 관리자 자격 증명은 VM 설치 묶음에 포함하지 않는다. 기존 state를 덮어쓰거나 다른 VM 설치 시 새 클러스터 키를 생성하지 않는다.

## 2. 같은 스크립트를 각 VM에서 실행

해당 VM에 그 VM의 `node<ID>` 묶음만 안전하게 복사한다. CA 개인키·관리자 비밀번호·다른 노드 개인키는 보내지 않는다. 묶음 디렉터리는 0700으로 보호한다. 아래는 각 VM의 터미널에서 실행하는 명령이다.

```bash
bash /secure/node0/install.sh --bundle /secure/node0 --check
sudo bash /secure/node0/install.sh --bundle /secure/node0 --apply
```

mongo-02는 자기 `node1`, mongo-03은 자기 `node2` 경로로 동일하게 실행한다. 스크립트가 다른 VM에 설치하지 않는다. IP가 현재 VM에 실제로 할당됐는지 확인하므로 다른 VM의 묶음을 잘못 사용하면 중단한다.

`--check`는 읽기 전용이다. 묶음 체크섬, OS·커널·CPU·메모리·디스크·기존 설치 충돌을 확인한다. 설치 명령은 sudo 권한이 필요하며 스크립트가 비밀번호를 받거나 저장하지 않는다.

설치는 공식 APT 서명 키의 고정 체크섬과 저장소를 사용하고, 설치 중 서비스 자동 시작을 막은 뒤 TLS·인증·Replica Set 이름을 설정하여 서비스 시작과 TLS ping을 확인한다. 사용자 지정 IP와 localhost에만 바인딩한다. `rs.initiate`, `rs.add`, 관리자 생성은 수행하지 않는다. 서비스 시작 후에도 Replica Set은 초기화 전 상태다.

데이터 경로는 실제 마운트·용량을 확인한 뒤 지정한다. `/var/lib/mongodb`를 지정했다고 별도 데이터 디스크가 되는 것은 아니다. 포맷·마운트·방화벽 변경은 자동 수행하지 않는다.

## 3. 중간 실패와 재실행

```bash
sudo bash /secure/node0/install.sh --bundle /secure/node0 --resume
```

최초 묶음을 보존한다. 입력·인증서·클러스터 키·설정·버전을 대조해 같은 설치만 재개한다. 완료된 동일 설치는 재설치하지 않는다. 다른 설치·기존 데이터·다른 설정은 덮어쓰지 않는다. 자동 업그레이드·데이터 삭제·force reconfig는 없다. 완료 후 서비스가 중지된 경우 원인을 확인하도록 중단한다.

실패 시 `sudo systemctl status mongod`, `sudo journalctl -u mongod`, `/etc/team3-mongodb/`를 확인한다. APT 실패 시 서비스가 mask 상태일 수 있다. 저장소·패키지 작업 전체가 원자적 rollback을 보장하는 것은 아니다.

## 4. Replica Set 초기화 계획 — 이번에는 실행하지 않음

1. 세 VM 각각 설치·TLS ping·동일 서버 버전·동일 Replica Set 이름을 확인한다.
2. 멤버 간 DNS/호스트명 해석·27017 통신·인증서 SAN·공통 CA·클러스터 키를 확인한다. 클라이언트의 DNS와 CA도 별도로 준비한다.
3. 확정한 `cluster.json`의 세 멤버를 대조한다. 초기화에 필요한 관리자 비밀은 별도 보호 경로로 필요한 노드에만 전달한다. 현재 설치 묶음에는 이 비밀이 없어 `cluster.sh`를 바로 실행할 수 없다.
4. 한 노드에서 명시적으로 `sudo bash cluster.sh init BUNDLE`을 한 번 실행한다. 자동 단일 노드 초기화나 각 VM의 중복 초기화는 하지 않는다.
5. 선출된 실제 Primary를 확인하고 그 노드의 localhost에서 `sudo bash cluster.sh admin BUNDLE`을 실행한다. 최초 관리자 생성 후 인증을 확인한다. 최초 노드가 Primary라는 가정은 하지 않는다.
6. `sudo bash cluster.sh verify BUNDLE`로 인증·구성·버전·Primary 1/Secondary 2를 확인한다. 별도로 소량 majority/journal 쓰기·Primary majority 읽기·장애/복원 인수를 수행한다.
7. 설치 관리자 계정을 파이프라인에 쓰지 않고 실제 namespace에 맞는 최소 권한 계정을 별도 준비한다.

기존 Replica Set의 멤버 추가는 이번 설치 기능과 별개다. 기존 CA·인증키를 재사용한 노드 설치와 명시적인 `rs.add` 운영 절차가 필요하다. 현 도구에 멤버 추가 자동화·기존 CA로 신규 노드 인증서를 발급하는 CLI·인증서 자동 갱신은 구현하지 않았다. 기존 클러스터 확장 시 새 `--prepare`로 CA/키를 다시 만들면 안 된다. 운영 인증서·키 순환과 X.509 운영 전환은 별도 인수다.

## 최초 설치 시도 결과 (13:22)

2026-10-17 mongo-01(192.168.0.134) 한 대에만 설치 묶음을 전달했다. 수정된 설치 도구의 실제 `--check`는 통과했으며 upstream kernel 7.0.14를 확인했다.

원격 설치 명령은 sudo 대화형 인증 요구로 exit 1이었다. 설치 이후 확인 결과 `mongod` 실행 파일과 `/etc/team3-mongodb/configured`가 없다. **실제 MongoDB 설치·서비스 시작·Replica Set 초기화·관리자 생성은 완료하지 않았다.** mongo-02/03 설치는 시도하지 않았다.

mongo-01에 로그인한 사용자가 다음을 실행하면 준비된 동일 묶음으로 이어 설치할 수 있다.

```bash
sudo bash /home/team3ops/mongodb-node0-20261017-132226/install.sh \
  --bundle /home/team3ops/mongodb-node0-20261017-132226 --apply
```

sudo 비밀번호는 VM 터미널에서 직접 입력하며 채팅이나 스크립트로 전달하지 않는다. 이 명령은 mongo-01만 설치하며 Replica Set을 초기화하지 않는다. 설치·동일 설치 재실행·서비스 시작·MongoDB 네트워크 동작은 sudo 인증 이후 실제 검증해야 한다.

공통 비밀과 다른 노드 묶음은 PC의 Git 밖 접근 제한 폴더에 보존했다. 실제 경로와 시도 결과는 Git 제외 `.runtime/single-vm-attempt.json`에 기록했다. 배포 환경 주소를 포함한 이 예시는 팀 환경의 실행 기록이며 다른 환경에서는 다시 점검·준비한다.

## 테스트

```bash
python3 -m unittest discover -s ops/mongodb/tests -v
bash -n ops/mongodb/deploy.sh
bash -n ops/mongodb/install.sh
bash -n ops/mongodb/node.sh
bash -n ops/mongodb/cluster.sh
node --check ops/mongodb/bootstrap.js
```

로컬 대역 테스트와 실제 설치를 구분한다. 테스트는 가변 인벤토리, 공통 인증키와 개별 SAN 인증서, 묶음 변조 거부, 로컬 VM 점검 분기, 암묵적 설치/초기화 금지, 기존 state 덮어쓰기 거부, 중복 초기화·관리자 재사용·구성/버전/인증 충돌을 확인한다. 실제 DB 설치나 장애 선출 시험을 대신하지 않는다.

테스트 결과: `.runtime/unit-tests-v2.txt`. 단일 VM 시도: `.runtime/single-vm-attempt.json`. 둘 다 Git 제외다. Git 스테이징·커밋·푸시는 이번 변경에서 수행하지 않았다.

## 후속 설치 실패와 권한 수정 (14:30)

사용자가 sudo 설치를 실행한 뒤 MongoDB 9.0.4 패키지와 설정 파일이 생성됐지만 서비스가 시작 직후 exit 1로 종료됐다. 읽기 전용 조회에서 `/etc/mongodb`가 root:root 0700임을 확인했다. 서비스 계정 mongodb가 인증서와 클러스터 키의 상위 폴더에 접근할 수 없는 설치 도구의 오류다. 원본 MongoDB 로그는 sudo 권한이 없어 읽지 못했으므로, 추가 원인이 없다는 실제 시작 검증은 복구 후 수행해야 한다.

수정: 상위 폴더를 root:mongodb 0750, pki 폴더를 mongodb:mongodb 0700으로 명시하고 systemd drop-in 폴더/파일 모드도 명시한다. 개인키 0400과 인증 제어는 유지한다. mongo-01 사용자 설치 묶음의 node.sh와 체크섬을 갱신했고, 보존된 나머지 노드 묶음에도 같은 수정을 반영했다. 서비스/DB/Replica Set은 직접 변경하지 않았다.

부분 설치가 있으므로 --apply 대신 최초 묶음으로 --resume을 사용한다.

```bash
sudo bash /home/team3ops/mongodb-node0-20261017-132226/install.sh \
  --bundle /home/team3ops/mongodb-node0-20261017-132226 --resume
```

재개는 같은 설정·인증키를 대조하고 상위 폴더 권한을 수정한 뒤 서비스 시작과 TLS ping을 다시 검사한다. 기존 데이터를 삭제하거나 Replica Set을 초기화하지 않는다. `NODE_INSTALL_COMPLETE` 확인 전에는 설치 완료로 간주하지 않는다. 다시 실패하면 `sudo tail -n 50 /var/log/mongodb/mongod.log`와 `sudo journalctl -u mongod -n 50 --no-pager`를 확인한다.

회귀 테스트 포함 로컬 18개 통과(16.460초). 이는 실제 mongod 재시작 성공을 의미하지 않는다. 재시작은 sudo 인증이 필요한 상태로 남아 있다.
