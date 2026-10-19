# 플랫폼과 중앙 자동화

[문서 목록](README.md) · 근거: [E01~E04](07-evidence-and-handoff.md#e01), [E10~E11](07-evidence-and-handoff.md#e10)

## 아키텍처

Windows 중앙 관리 PC가 기존 SSH 별칭을 통해 Bastion에 연결한다. Bastion의 Ansible이 승인된 인벤토리와 Vault 인증을 사용하여 VM을 관리한다. Windows 팀원 PC의 VMware 파일 조사는 별도의 SSH·PowerShell 경로를 사용한다.

| 논리 역할 | VM 수 | 구성 |
|---|---:|---|
| Bastion | 1 | 관리·Ansible |
| Ad Server | 1 | 광고·추천 실행 대상; 업무 기능 완성은 별도 |
| Monitoring | 1 | Prometheus·Grafana |
| Spark | 4 | Master 1·Worker 3 |
| NiFi | 1 | 데이터 흐름 서비스 |
| Kafka | 3 | KRaft 브로커·컨트롤러 |
| Object Storage | 2 | Garage |
| MySQL | 3 | Primary 1·Replica 2 |
| MongoDB | 3 | 기존 Replica Set |
| WebApp | 1 | 웹 서비스 대상·인프라 시험 앱 |
| 합계 | 20 | Windows 호스트 5대에 분산 |

VM은 Ubuntu Server 26.04.1 LTS x86_64이며 조사 당시 커널은 7.0.0 계열이었다. VM당 메모리는 약 3.3 GiB다. 이 용량은 수집 당시 자원이며 실제 업무 부하를 수용한다는 부하 시험 결과는 아니다.

## Windows·SSH 관리

5대 Windows 호스트에 VM이 각각 4대씩 있다. 4대의 Workstation은 26.0.1, 나머지 1대는 17.6.4였다. OVF Tool도 각각 9.1.1과 4.6.3으로 달라 내보내기 전 호스트별 호환성 확인이 필요하다.

중앙 PC에서 팀원 Windows 4대에 공개키 인증 및 비대화식 원격 PowerShell 실행이 실제 성공했다. 기존 키와 Bastion 별칭을 보존했고 개인키를 원격 PC에 복사하지 않았다. 관리자 계정 여부와 실제 OpenSSH 설정에 맞는 키 파일·ACL을 확인했다.

서버 목록은 공통 CSV를 단일 원본으로 삼고 각 사용자 로컬 SSH Config를 생성하는 구조다. NAS 드라이브 문자는 고정 전제가 아니며 공유 경로 또는 명시한 CSV 경로를 사용한다. SSH 접속 때마다 NAS에 연결할 필요는 없다. 키 등록 도구와 Config 생성 도구는 별도 기능이다.

기존 Config는 보존하고 Include로 연결하며, OpenSSH의 첫 설정 우선 및 IdentityFile 누적 동작을 고려한다. 기존 키·known_hosts를 삭제하거나 호스트 키 확인을 비활성화하지 않는다. Windows PowerShell 5.1·7 검증 기록은 보호된 원본에 남아 있다.

## Ansible·Vault

Bastion의 실제 인벤토리 20대를 확인했다. 공식 작업 디렉터리의 코드는 관리 원본이며 이번 조사에서 Git 저장소로 확인되지 않아 배포 코드를 특정 Git commit과 동일하다고 주장하지 않는다.

Ansible core 2.20.1, forks 최대 4를 사용한다. 승인 런타임 검증에는 Java 21.0.12.1, MySQL Client 8.4.11과 Python venv가 포함됐다. Python 의존성 일괄 설치 완료와는 구별한다. 기존 sudo 실행 정책을 유지하고 Vault에서 become 인증을 읽어 대화형 비밀번호 입력 없이 실행한다. 2026-10-17에 20대 비대화식 sudo·Preflight가 통과했고 당시 자동 테스트는 83개였다.

Vault 암호 파일과 암호화된 변수는 보호된 Bastion에 유지한다. 복호화 값·암호 파일·인증서 개인키를 Windows나 GitHub에 복사하지 않는다. 공개 문서에는 실제 Vault 경로·변수값·접속 계정·세부 접근 정책을 포함하지 않는다.

## 설치 승인과 단계 구분

Stage 1은 고유 DEB 16종, maintainer/control 항목 21개, 고유 스크립트 해시 18개를 검토했다. 승인은 패키지명·정확한 버전·DEB SHA256·검토한 스크립트에 묶이며 미검토 코드를 자동 승인하지 않는다.

Java alternatives와 CA 저장소 변경, 패키지 설정·초기화·서비스 기동 가능성을 검토한 후 계획 해시를 다시 검증했다. 설치 단계에서는 서비스 자동 기동을 막고 제거·다운그레이드·범위 밖 업그레이드를 허용하지 않았다.

Stage 2의 20대 패키지 검증과 DEB 설치 영수증 75건은 **20개 서비스가 완성됐다는 의미가 아니다**. 당시 역할 집계는 INSTALLED 8, ALREADY_INSTALLED 1, PENDING 11, FAILED 0이었다. 서비스 구성·기동·기능 검증은 후속 Stage 3의 별도 승인으로 진행했다.
