# NiFi 운영 연결

2026-10-21. 기준은 상위 [파이프라인 설계](../../../docs/design/02-data-pipeline.md), [인프라](../../../docs/design/01-infrastructure.md), [운영 실행](operations_execution.md), [기존 제한 SSH 검증](service_validation_20261018.md#nifi--master-실행-경로)이다.

## 실제 연결

`NiFi TIMER(5분) → 제한 SSH production-tick → Master 운영 제어 → 기존 연속 처리`

- NiFi `192.168.0.125`, 버전 2.12.0. 기존 CA와 클라이언트 인증서로 TLS 인증한다. 비밀번호나 개인키를 저장소에 추가하지 않는다.
- 그룹 `product-pipeline-production`, ID `5dfe7f50-01a1-1000-f423-c433e310ab6e`.
- 프로세서 `product-pipeline-production-control`, ID `5dfe7fcc-01a1-1000-4059-f310bcb3aa66`.
- ExecuteProcess, TIMER_DRIVEN `5 min`, 동시 실행 1, PRIMARY. stdout의 success FlowFile은 자동 종료한다. 실제 업무 완료 근거는 Master의 단계별 영수증·DB 반영 결과다.
- SSH는 기존 전용 계정·키·호스트 키 고정을 재사용한다. 기존 검증용 `tick`과 운영용 `production-tick` 두 고정 명령만 허용한다. 포워딩·임의 명령을 허용하지 않는다.
- Master 실행기는 `/usr/local/libexec/pj3-pipeline-production-tick`, 고정 Python 실행기는 `/opt/pj3/product-pipeline/launchers/production_tick.py`다. `pj3-spark` 권한으로만 실행한다.

NiFi가 매번 Spark 세션을 만들거나 100건 배치를 새 프로세스로 시작하지 않는다. 현재 실행이 살아 있으면 상태 확인만 수행한다. 배치의 순서·재시도·체크포인트·게시·활성화는 기존 연속 처리기가 담당한다. 5분은 제어 호출 주기이며 배치 완료 시간 보장이 아니다.

## 실행·정지·복구 판정

| 상태 | NiFi 운영 제어 동작 |
|---|---|
| 기존 정확한 worker PID 실행 중 | `ALREADY_RUNNING`, 신규 실행 없음 |
| 실행 중이며 배치 후 정지 요청 존재 | `PAUSE_REQUESTED`, 신규 실행 없음 |
| 정지 요청·수동 중단 기록이 있고 worker 없음 | `PAUSED`, 자동 재시작 없음 |
| 완료 상태 | `COMPLETED`, 재수집 없음 |
| 최초 실행 또는 운영자가 명시한 `READY_TO_START` | 클러스터 유휴 확인 후 기존 설정으로 FLOW 시작 |
| 갱신 중 일시적인 Mongo 연결 오류·소유권 증거·전체 유휴 확인 | [제한된 자동 복구](automatic_recovery.md) 후 미완료 단계 재개 |
| 그 외 RUNNING 기록인데 worker 없음·불명확 상태·PID 정체성 변경 | `RECOVERY_REQUIRED`, 자동 강제 재개 없음 |
| 수동 제어와 시작 잠금 경쟁 | `BUSY`, 다음 timer에서 재확인 |

수동 제어와 NiFi는 `controller.lock` 및 같은 `operations/server_flow.py` 실행 함수를 사용한다. 원장 lease·checkpoint·원천·이벤트 ID를 초기화하지 않는다. 새 실행은 기존의 `runtime-overrides.json`과 Spark runtime 설정을 사용한다. 현재 운영은 100건 단위다.

2026-10-21 후속 변경으로 위 표의 제한된 Mongo 갱신 오류 자동 복구를 추가했다. 실패 상태를 READY로 편집하지 않으며, 비공개 소유권 증거·죽은 프로세스·전체 Spark 유휴·만료된 동일 token을 확인한 뒤 기존 트랜잭션 복구를 사용한다. 업무 완료 지점이 진전되지 않으면 최대 3회이며 나머지 오류는 계속 운영자 확인 대상으로 둔다.

NiFi 프로세서를 STOPPED로 바꾸면 **예약 호출만** 멈춘다. 이미 실행 중인 연속 처리는 계속된다. 연속 처리를 안전하게 멈출 때는 Master의 기존 `pause-after-window.json` 절차를 사용해 업무 DB 게시·활성화가 끝나는 경계에서 정지한다. 재개 시 해당 정지 요청과 실패 기록을 확인한 뒤 수동 실행 명령을 사용한다. 실패 상태를 READY로 임의 편집하지 않는다.

## 운영자가 확인할 파일

Master root: `/var/lib/pj3/product-pipeline/spark/production-pending-20261019`

- `nifi-control.json`: 이 운영 root의 제어 활성화 계약. 새 root에 자동 적용하지 않는다.
- `nifi-last-tick.json`: 가장 최근 호출 시각·PID·상태·신규 실행 여부·duration_ms.
- `nifi-tick-audit.jsonl`: 호출 이력. 업무 성공·실패 판정은 별도 단계 결과를 따른다.
- `flow-progress.json`, `epochs/<epoch>/windows-progress.json`: 업무 진행 단계·배치 커서.
- `epochs/<epoch>/windows/<window>/*-result.json`: 단계별 성공 영수증과 시간.

NiFi root: `/var/lib/pj3/product-pipeline/nifi`

- `production-nifi-control.json`: 그룹·프로세서·주기·구성 결과.
- `production_nifi_control.py`: 동일 구성 재실행 시 기존 그룹·프로세서를 재사용하는 설정 도구.

## 배포와 검증

1. [제어 모듈](../product_pipeline/operations/server_flow.py) 및 관련 테스트를 Master 운영 source에 추가한다. 현재 실행 중인 ETL 모듈을 바꾸지 않는다.
2. [고정 SSH 명령 설치기](server_setup/pipeline_nifi_control_install.py)를 Ansible become으로 Master에서 실행한다. 기존 게이트가 예상한 검증용/운영용 내용과 다르면 중단한다. 게이트 백업·sudoers 문법 검사·실행 파일 소유권을 확인한다.
3. 수동 제어용 `production_pending_manage.py`도 공통 launch 함수를 사용하는 버전으로 배포한다. 기존 제어 모듈은 백업한다.
4. NiFi 전용 venv에서 [프로세서 설정기](server_setup/pipeline_nifi_production.py)를 실행한다. 실제 버전·bundle·기존 동명 설정을 대조한 뒤 기동한다. VM 주소·root·버전은 이 서버 배포에 고정돼 있으므로 다른 환경에서는 해당 배포 계약을 먼저 확정한다.
5. 기존 worker PID를 유지하면서 실제 timer가 `ALREADY_RUNNING` 기록을 남기는지 확인한다. 임의 SSH 명령 거절, 중복 생성 방지, 로컬·서버 테스트를 확인한다.

이번 연결은 **기존 수집 결과의 연속 처리 제어**다. 수집기 신규 실행 일정·새 수집 run의 자동 발견·전체 65,498건 완주·물리 장애 복구 완료를 의미하지 않는다. NiFi 프로세서가 RUNNING이라는 사실만으로 MySQL 반영 완료를 판정하지 않는다.

### 확인 결과

19:25:23과 19:30:23 KST에 실제 timer 호출이 Master에 도착했다. 두 호출 간격은 약 300.4초이며, 모두 PID 176146·`ALREADY_RUNNING`·`started=false`였다. 배치 후 정지 설정은 없고 기존 연속 처리는 유지됐다. 동일 설정을 재적용해도 그룹·프로세서를 추가 생성하지 않고 `EXISTING_MATCH`를 반환했다. `id`, `production-tick;id`는 모두 exit 64로 차단됐다. 제어 로직의 로컬·서버 테스트는 각각 5개 통과했다.

[검증 JSON](verification/nifi_production_20261021.json)에 실제 프로세서 상태·호출 이력·코드 해시를 보존했다. NiFi `taskCount`는 최근 통계 구간의 값이므로 누적 호출 수로 계산하지 않는다. 두 차례 실행은 Master의 영속 시각 기록으로 확인했다. 이번 검증에서는 정상 실행 중인 업무 프로세스를 종료하거나 신규 worker를 중복 기동해 보지 않았다.

## 크롤러 호출 연결 — 수집 미시작

사용자의 요청에 따라 기존 크롤러를 NiFi VM에 배포하고 호출 프로세서를 **STOPPED**로 연결했다. 기존 업무 파이프라인의 5분 운영 제어와 별도 그룹이다.

- 원본은 `feature/#6`의 `67e837db4114ef8955a12c0b466f6f4aba50bd8f`, Git archive SHA-256은 `e312d1b605a5afb36f25c5e7252c9edb4bf023041b42efe9c30ef449c72f6329`다. 크롤러 코드는 수정하지 않았다.
- 서버 소스는 `/opt/pj3/danawa-collector/releases/<archive SHA-256>/Commerce_crawler`에 root 소유로 보존했다.
- 독립 Python은 `/var/lib/pj3/product-pipeline/nifi/collector/venv/bin/python`이며 기존 NiFi·파이프라인 venv를 변경하지 않았다. 서버의 기본 venv에 pip이 없어 기존 NiFi pip의 `--python`으로 독립 환경을 초기화했다.
- 그룹 `danawa-collector`: `5e1b2b63-01a1-1000-c17a-546dca0c0a4e`.
- 프로세서 `danawa-collector-manual-enable-required`: `5e1b2b8e-01a1-1000-62c3-de018bc5fa48`, VALID·STOPPED·active thread 0.
- 호출 대상은 `/opt/pj3/danawa-collector/collector_launcher.py`다. 설정 `/var/lib/pj3/product-pipeline/nifi/collector/config/collector-control.json`의 `enabled=false`로 실행도 차단했다. 프로세서를 실수로 기동해도 이 설정 그대로라면 수집 명령을 실행하지 않는다.
- 기존 71개 카테고리 설정과 보호된 실제 페이지 요청 템플릿을 연결했다. `--download-images`를 포함한 기존 CLI를 호출하도록 준비했으며 출력은 `/var/lib/pj3/product-pipeline/nifi/collector/runs`다.
- 프로세서의 `1440 min`은 비활성 기본값일 뿐 확정된 수집 주기가 아니다. 실제 기동 전 주기·범위·새 실행/재개 정책과 요청 템플릿 유효성을 확인한다. 현재 다나와 요청을 보내지 않았으므로 사이트의 현재 응답·페이지 전환 성공까지 검증한 것은 아니다.

[설치기](server_setup/pipeline_collector_install.py), [실행 차단 도구](server_setup/danawa_collector_launcher.py), [NiFi 연결 도구](server_setup/pipeline_nifi_collector.py)를 보존했다. 로컬 실행 차단 테스트 2개, 서버의 도움말·pip check·설정 점검·비활성 호출을 확인했다. 재적용은 EXISTING_MATCH였고 [실서버 증거](verification/nifi_collector_connection_20261021.json)를 남겼다. 실제 수집·이미지 다운로드는 시작하지 않았다.

이번에 완료한 것은 **NiFi → 크롤러 실행 연결**이다. 새 수집 run은 아직 생성하지 않았고 기존 파이프라인의 고정 운영 run에 자동 등록하지 않았다. 새 run의 확정 checkpoint·이미지 manifest를 [capture/drain](live_execution.md)에 연결해 MongoDB·이미지 스토리지로 인계하는 운영 등록은 실제 수집을 활성화하기 전에 별도로 완료해야 한다. 크롤러는 로컬 작업 경로에 이미지를 저장하며, 이미지 스토리지 적재는 파이프라인이 수행한다.
