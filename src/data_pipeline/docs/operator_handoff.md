# 파이프라인 운영 인계 요약

확인 시점: **2026-10-22 10:57:46 KST**. 문서 기준 브랜치는 `feature/#13`, 당시 HEAD는 `7edcaf5`다. 이 문서의 명령은 운영자가 필요할 때 실행하는 절차이며, 문서 업데이트로 서버 설정·수집·연속 처리를 변경하지 않았다.

## 1. 어디서 실행하는가

| 대상 | 접속·위치 |
|---|---|
| 진입 | Bastion `192.168.0.111`; 운영자 PC의 `bastion` SSH alias·키·호스트 키·권한 필요 |
| Driver·진행 원장 | Spark Master `192.168.0.121`, 서비스 계정 `pj3-spark` |
| Worker | `192.168.0.122`, `.123`, `.124` |
| 예약 제어 | NiFi `192.168.0.125`; 운영 timer는 기존 작업 제어, 신규 크롤러 프로세서는 별도 비활성 연결 |
| 원천·이미지·게시 | MongoDB `.134~.136`, Garage `.141~.142`, MySQL `.131~.133`; 실제 접속 대상·역할은 서버 보호 설정 기준 |

Master의 작업 루트(아래 `ROOT`):

`/var/lib/pj3/product-pipeline/spark/production-pending-20261019`

Python: `/var/lib/pj3/product-pipeline/spark/venvs/128dbd37d879b397-spark/bin/python`

공용 설정: `/var/lib/pj3/product-pipeline/spark/config/`. Spark 4.1.3·Delta 4.4.1·Java 21의 기존 서버 환경을 사용한다. 새 환경에 Windows venv를 복사하지 않는다. 서비스별 인증 파일·TLS 위치와 역할은 [배포 가이드](deployment.md#현재-vm의-보호-설정), S3는 [Garage 프로필](garage_v241_profile.md)을 따른다. 비밀번호·개인키·소유권 token은 인계 문서나 Git에 넣지 않는다.

## 2. 지금 어디까지 되어 있는가

- 19개 Bronze·Silver·Gold 테이블의 파일 정리와 전후 내용 불변성, 기존 100건 선택·원문 동등성 확인을 완료했다. 과거 버전 삭제·VACUUM은 하지 않았다.
- epoch **32**, 상품·관측 **100건**의 단일 시험을 10:30:56 KST에 시작했다. 확인 시점에 **MySQL 최초 게시 완료, 활성화 평가 진행 중**이며 기록된 오류는 없었다. 전체 65,498건 완료가 아니다.
- Bronze 시작부터 최초 MySQL 게시까지 **19분 31초**. 단계별 기록은 Bronze 463,771ms / 선택 19,134ms / Silver 335,238ms / Gold 279,002ms / 게시 71,214ms다. 실행 요청부터의 대기·이전 epoch 마무리는 이 시간과 구분한다.
- `pause-after-window.json`을 유지한다. **게시·활성화 완료 후 한 배치에서 정지**, 다음 배치 자동 시작 없음. NiFi가 켜져 있어도 이 정지 요청이 우선한다.
- 최종 완료 시간은 확인 시점에 미확정이다. 아래 결과 파일이 생성된 뒤 `to_activation_ms`, `mysql_before/after`, `pause_preserved`, `next_epoch_created`를 함께 확인한다. 200건 설정은 적용하지 않았다.

| 확인 내용 | `ROOT` 아래 파일 |
|---|---|
| 실행 여부·전체 단계 | `flow-progress.json`, `worker.pid`; PID 정체성·Spark 앱도 함께 확인 |
| 단일 시험의 배경 작업 상태 | `perf-20261020/single100-background-20261022.json` |
| 단일 시험의 최종 결과 | `perf-20261020/single100-range-20261022-result.json` — 확인 시점에는 미생성 |
| 단계별 성공·시간 | `epochs/000032/bronze-result.json`, `epochs/000032/windows/000000/*-result.json` |
| 활성화 하위 실행 | 같은 window의 `activation`, `activation-v2`, `activation-v3` 중 실제 생성된 경로; 과거 실행과 혼동하지 않음 |
| 커서·고정 선택 | `epochs/000032/windows-progress.json`, 같은 window의 `selection.json` |
| 배포·정리 근거 | `deployment.json`, `prepared/range-20261022/manifest.json`, `perf-20261020/range-compaction-20261022.json` |
| NiFi 호출 결과 | `nifi-last-tick.json`, `nifi-tick-audit.jsonl` |

메모리 표본은 Master·Worker 각 VM의 `/var/tmp/pj3-pipeline-single100-20261022/result.json`에 있다. 5초 간격 Spark Java RSS 관측이며 Python·OS 전체의 정확한 최고치나 JVM heap 최고치가 아니다. 운영 진행·재개는 서버 원장으로 판단하며 시험용 `perf-*`·`/var/tmp` 파일을 장기 체크포인트로 사용하지 않는다.

## 3. 브랜치와 서버 배포본의 차이

| 항목 | 확인 결과 |
|---|---|
| 이번 성능 패치 | 서버 `prepared/range-20261022/manifest.json`의 **11개 파일**과 브랜치 내용을 대조. 9개는 바이트 해시 일치, `bronze/delta.py`·`silver/source.py`는 CRLF→LF 정규화 후 일치 |
| 서버 배포 식별 | `performance_patch_sha256=92a83dda4cc4be72c8276ba7bb735acc4fafdadf60f6985cd7051f02397ca31c`; `deployment.json`의 source 해시는 Git 커밋 번호가 아님 |
| dev 병합 신규 코드 | `embedding_table.py`, `embedding_vectors.py`, `garage_embeddings.py`는 **브랜치에 있고 서버에는 없음**. 서버에 자동 배포·기동된 것으로 취급하지 않음 |
| 문서 위치 | 최신 인계 문서는 Git 브랜치 기준. 서버 `source/docs/` 전체가 최신 문서 사본인 것은 아님 |

11개 성능 패치의 일치가 서버 전체 source와 최신 Git 전체의 동일성을 뜻하지 않는다. 배포 전에는 필요한 파일·의존성·역할을 별도로 대조한다. 실행 중 ETL 파일을 교체하지 말고 [배치 경계 유지보수](server_setup/pipeline_maintenance_watch.py)·[배포 가이드](deployment.md)를 따른다. 기존 완료 실행의 ID·등록·영수증을 새 코드로 조용히 바꾸지 않는다.

## 4. 운영자가 쓰는 명령

운영자 PC의 저장소 `src/data_pipeline`에서 실행한다. SSH 비밀번호 보유만으로 Bastion alias·Ansible 제어 모듈·서비스 권한이 준비되는 것은 아니다.

```powershell
python run_server_pipeline.py status
python run_server_pipeline.py preflight
```

`status`의 worker 생존 여부와 오류·정지 표식, 실제 단계 결과를 먼저 확인한다. `preflight`는 Master·Worker 3대 Kafka TLS 파일을 확인한다. **기존 NiFi tick 자체가 동일한 네 호스트 사전 검사를 수행한다는 뜻은 아니다.**

### 안전 정지 요청

Master에 로그인한 뒤 실행한다. 이미 있는 정지 요청은 덮어쓰지 않고, 실행 중인 작업은 현재 window 게시·활성화 경계까지 마친다.

```bash
sudo -u pj3-spark /var/lib/pj3/product-pipeline/spark/venvs/128dbd37d879b397-spark/bin/python - <<'PY'
import sys
from pathlib import Path
r = Path('/var/lib/pj3/product-pipeline/spark/production-pending-20261019')
sys.path.insert(0, str(r / 'source'))
from product_pipeline.files import lock, atomic_json
from product_pipeline.contracts import now
with lock(r / 'controller.lock'):
    p = r / 'pause-after-window.json'
    if not p.exists():
        atomic_json(p, {'reason': 'OPERATOR_PAUSE_AFTER_WINDOW', 'at': now()})
print('PAUSE_REQUESTED; wait for PAUSED_AFTER_BATCH and worker exit')
PY
```

공유 Driver에는 신호 종료를 사용하지 않는다. NiFi 프로세서 정지는 예약 호출만 멈추며 이미 실행 중인 Driver를 중지하지 않는다.

### 한 배치 실행 / 연속 재개

- **한 배치:** 현재 작업이 완전히 정지했고 `pause-after-window.json`이 있는지 확인한다. `runtime-overrides.json`의 `window_records=100`을 확인하고 아래 `flow` 명령을 한 번 실행한다. 기존 성공 단계를 재사용하고 새 window 게시·활성화 뒤 다시 멈춘다. 직접 `bronze`·`downstream`·`flow`를 동시에 실행하지 않는다.
- **연속 재개:** 운영 승인을 받고 `PAUSED_AFTER_BATCH`·worker 없음·전체 Spark 유휴를 확인한다. 서비스 계정으로 기존 정지 표식을 감사용 경로에 보존한 뒤 작업 루트에서 해제하고 같은 `flow` 명령을 실행한다. 이 문서는 정지 표식을 해제하지 않았다. 실패·소유권 불명확 상태에서는 표식을 지우거나 `READY_TO_START`로 편집하지 않고 아래 복구 절차를 따른다.

```powershell
python run_server_pipeline.py flow
```

단계 결과의 `COMPLETED`, MySQL 실제 반영과 활성화 결과, worker 종료를 확인한다. stdout의 접수 성공이나 NiFi `RUNNING`만으로 업무 완료를 판정하지 않는다.

## 5. 실패 시 확인 순서

| 증상·코드 | 먼저 확인할 것 / 대응 |
|---|---|
| `RECOVERY_REQUIRED`·`BRONZE_WRITER_ALREADY_CLAIMED`·lease 손실 | `flow-progress.json`, 해당 단계 failure·비공개 `lease-owners/`, PID·Driver·Workers. [소유권 기반 복구](automatic_recovery.md); TTL·수동 원장 삭제로 우회하지 않음 |
| Kafka offset 범위 이탈 | [보관 만료 복구](kafka_retention_recovery.md). 고정 이벤트 ID 재발행·별도 체크포인트 세대 사용; 기존 offset 초기화 금지 |
| Kafka TLS·핸드셰이크 | Master·Workers 모두의 `/etc/pj3-clients/kafka/pipeline/keystore.p12`, `truststore.p12` 및 `pj3-spark` 읽기 권한·서버 인증 상태 확인 |
| JVM OOM·종료 / 저장 확인 불명확 | 단계 failure·Spark 로그·RSS 표본·실제 저장분 확인. 완료 영수증을 지우거나 같은 writer를 중복 실행하지 않음 |
| `PENDING_REVIEW`·갤러리 없음 | [검수 절차](review_workflow.md), 원천 URL 유무·파일 확보 상태·고정 입력 확인. 데이터 대기와 실행 실패를 구분 |

checkpoint·window selection·등록 ID·성공 receipt·Mongo 불변 원천·Delta 과거 버전·비공개 소유권 증거를 재개 목적으로 삭제하지 않는다. `*-failure.json`과 private log에 비밀·원문이 있을 수 있으므로 그대로 외부 공유하지 않는다. VM/AWS 이전은 [상태 이관](state_migration.md)과 보호 설정 인계를 별도로 수행한다.

## 6. 담당 경계와 남은 인수

- 크롤러 담당: 다나와 목록·상세·갤러리 URL 확보와 파일 다운로드, 수집 checkpoint. 새 수집 실행 일정은 별도 승인한다.
- 파이프라인 담당: 확정 입력·이미지 바이트를 검증하여 Garage·MongoDB·Kafka·Delta·MySQL 게시·활성화로 연결하고 재실행·부분 실패를 복구한다. 파이프라인 실행이 다나와 이미지 다운로드를 대신 시작하지 않는다.
- 추천 담당: 임베딩·벡터·검색 인덱스·추천 준비 판정. **상품 ACTIVE와 추천 준비 완료는 별도**다. dev 신규 도구를 병합한 것만으로 실제 추천 연동 완료가 되지 않는다.
- 인프라 담당: VM·DB·Kafka·저장소·TLS·백업·권한·가용성 운영. 원장 접근 오류는 애플리케이션 승인 절차와 구분한다.
- 남은 확인: 이번 100건 최종 시간·정지 결과, 전체 65,498건의 처리·품질 대기·이미지 확보 현황, 처리량 변경(200건/내부 50~100건) 판단, 장기 연속 운전·백업 복원·환경 이전·추천 서비스 통합 인수. 전체 완료로 체크하지 않는다.

자세한 절차는 [문서 목록](README.md), [현재 검증 범위](service_validation_20261018.md), [변경 이력](implementation_history.md)을 따른다.
