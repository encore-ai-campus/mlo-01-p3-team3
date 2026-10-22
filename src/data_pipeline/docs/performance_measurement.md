# 실서버 파이프라인 반복 계측

같은 계측 절차로 실제 배치의 단계별 시간과 병목을 비교한다. 이 문서는 계측 코드 수정·서버 배포·실제 배치 실행을 구분하며, 반복 측정의 기준과 결과 기록 형식을 정한다. 실제 운영 상태는 [실서버 검증](service_validation_20261018.md), 배포·정지·재개 명령은 [배포](deployment.md)와 [운영 인계](operator_handoff.md)를 따른다.

현재 계측은 Bronze의 연결·조회·원문 검증·Delta 적재 구간과 Silver·Gold의 읽기·검증·평가·증명·적재 구간을 제공한다. 상세 지표는 `7f8ff2f`부터 포함되며, 서버에 반영된 커밋과 결과 JSON의 `commit`을 확인한다. 계측은 기존 처리 동작에서 얻은 값만 기록하며 계측을 위해 별도 Spark action이나 Delta 조회를 추가하지 않는다.

## 측정 전제

- 실제 FLOW 한 번은 Delta와 업무 MySQL에 쓰기를 수행한다. 담당자가 단일 배치 측정을 명시적으로 요청한 경우에만 실행한다.
- 시작 전에 서버 상태와 기존 `pause-after-window.json`을 확인한다. 한 배치 뒤 정지하는 표식과 `window_records` 설정을 임의로 지우거나 바꾸지 않는다. 한 배치 후 정지가 보장되지 않으면 시작하지 말고 [배포·정지 절차](deployment.md)를 확인한다.
- Master·Worker 사전 점검을 통과하고, 진행 중인 Driver·Spark 앱·파이프라인 작업이 없어야 한다. 기존 epoch, 체크포인트, lease, 영수증은 보존한다.
- 측정 앞뒤 업무 DB는 읽기 전용 조회로 기록한다. 테스트 대상 아닌 운영 행을 수정하거나 측정 목적으로 승인·검수 상태를 바꾸지 않는다.
- 실제 배치가 처리될 때마다 원천 구성과 누적 Delta 이력이 달라진다. 서로 다른 100건 배치의 시간 차이를 코드 개선률로 단정하지 않는다.

## 반복 실행 순서

1. [운영 인계](operator_handoff.md), [배포](deployment.md), [FLOW 실행](flow_execution.md)을 읽고 실행 호스트에서 `python run_server_pipeline.py status`와 `python run_server_pipeline.py preflight`를 실행한다. Master·Worker 상태, 현재 epoch/stage, Driver·Spark 앱 유무, 한 배치 후 정지 설정, 코드 commit을 결과에 남긴다. 기존 정지 표식은 해시를 기록해 실행 뒤 동일한지 대조한다.
2. 같은 시각대의 read-only DB 조회로 상품 총수와 `ACTIVE`, 대기 건수를 저장한다. 실행할 window 크기와 대상 상품/관측 수를 기록한다. 전체 원문 바이트는 계측값이 있을 때만 적고, 이미지 URL 수와 이미 원천에 기록된 파일 크기는 이 배치에서 새 이미지 다운로드가 일어났다는 뜻으로 해석하지 않는다.
3. 한 배치 전체를 볼 때 `python run_server_pipeline.py flow`를 한 번 실행한다. 별도 FLOW나 `downstream`을 동시에 시작하지 않는다. 이 명령은 작업 시작을 요청하므로 반환 즉시 완료로 판단하지 않는다. `status`와 저장된 단계 영수증을 확인해 Bronze → 선택 → Silver → Gold → 최초 게시 → 활성화가 끝나고 flow가 `PAUSED_AFTER_BATCH`가 될 때까지 기다린다.
4. 종료 뒤 다시 `status`와 클러스터를 조회한다. worker/driver/Spark 앱이 끝났는지, 다음 epoch가 생성되지 않았는지, pause 표식 해시가 그대로인지, flow가 정지 상태인지 확인한다. 업무 DB의 같은 읽기 전용 건수를 다시 저장하고 해당 epoch/window의 단계 영수증과 `runtime_metrics`를 결과에 첨부한다.
5. 결과를 `docs/verification/` 아래 JSON으로 보존한다. 실패해도 부분 증거와 실패 시각·단계를 보존하고, 실패 영수증을 성공으로 바꾸거나 checkpoint/lease를 삭제해 재시도하지 않는다. 복구가 필요하면 [복구·정지 절차](automatic_recovery.md)를 따른다.

## 기록 형식

기존 결과와 같은 키를 우선 사용한다. 새 계측값을 추가해도 원래 값을 덮어쓰지 않는다.

```json
{
  "measurement_version": 1,
  "observed_at": "ISO-8601 timestamp with timezone",
  "commit": "deployed Git commit",
  "epoch": 34,
  "window": 0,
  "window_records": 100,
  "status": "PAUSED_AFTER_BATCH",
  "attempt_started_at": "ISO-8601 timestamp with timezone",
  "bronze_started_at": "ISO-8601 timestamp with timezone",
  "bronze_finished_at": "ISO-8601 timestamp with timezone",
  "finished_at": "ISO-8601 timestamp with timezone",
  "wall_elapsed_ms": 0,
  "bronze_to_activation_wall_ms": 0,
  "phase_total_ms": 0,
  "cluster": {},
  "input": {
    "products": 100,
    "observations": 100,
    "raw_bytes": null,
    "image_url_count": null,
    "source_recorded_image_bytes": null
  },
  "phases": {},
  "activation_subphases": {},
  "runtime_metrics": {
    "bronze": {},
    "silver": {},
    "gold": {},
    "activation": {}
  },
  "mysql_before": {},
  "mysql_after": {},
  "stop_checks": {
    "pause_hash_preserved": true,
    "worker_alive": false,
    "next_epoch_created": false
  },
  "comparison_notes": []
}
```

단계별 `phases`와 실제 시작~종료 경과 시간을 함께 둔다. `wall_elapsed_ms`는 FLOW attempt 시작부터 끝까지이고, `bronze_to_activation_wall_ms`는 Bronze 영수증의 시작(`finished_at - duration_ms`)부터 활성화 완료까지다. 두 값은 사전 준비 시간 포함 여부가 달라 섞어 비교하지 않는다. 이전 결과의 `after_bronze_to_activation_ms`는 후자와 대응한다. 각 단계 영수증의 세부 `runtime_metrics`는 해당 stage 아래 원형 그대로 보존한다. 아직 측정하지 않은 값은 `null` 또는 누락으로 두며 추정치로 채우지 않는다. 서버 주소·계정·키·비밀값은 JSON에 넣지 않는다.

## 지표 해석

| 구간 | 주요 키 예시 | 해석 |
|---|---|---|
| Bronze | 기존 `runtime_metrics`의 `spark_connect`, `mongo_batch_read`, `foreach_batch`, `delta_stage_*`, `delta_append_*`, `lease_*` | Mongo 조회, 원문 검증, 로컬 직렬화, 객체 저장·재읽기 검증, Spark Delta 쓰기, lease 갱신을 구분한다. 상세 목록은 [Bronze 개선](bronze_optimization.md)을 따른다. |
| Silver 입력·평가·출력 | `silver_bronze_*`, `silver_collector_input_*`, `silver_input_*`, `silver_normalization`, `silver_*receipt*`, `silver_*output*`, `silver_live_input_recheck` | 원천 읽기·대조, 상태 읽기, 정규화/재사용, 입력 보존, 완료 영수증·기존/신규 출력 증명, 저장 시간을 나눈다. |
| Gold 입력·평가·출력 | `gold_silver_*`, `gold_build_results`, `gold_*receipt*`, `gold_*output*`, `gold_export_*` | Silver 버전·입력 읽기, Gold 평가, 증거 검증, 출력/영수증 저장, 게시용 품질 집계를 나눈다. |
| 공통 Delta | `delta_metadata_read`, `delta_*_read_plan`, `delta_*_read_validate`, `delta_append_serialize*`, `delta_append_plan*`, `delta_append_commit*` | 메타데이터 확인, 읽기 계획·검증, 테이블별 직렬화·계획·커밋을 나눈다. 행 수와 payload 문자 수가 함께 있으면 지표와 같이 보존한다. |

`*_ms`는 해당 코드 구간의 누적 경과 시간이며 대응하는 `*_calls`로 호출당 평균을 구할 수 있다. 호출 횟수는 Spark job·네트워크 요청 횟수가 아니며 재시도한 호출도 포함한다. 상위 단계와 내부 지표는 시간이 겹치므로 서로 합산하지 않는다. 실제 전체 경과 시간과 순차 단계 합계도 분리해 기록한다.

`*_payload_chars_*`는 이미 만든 JSON 문자열의 문자 수이고 `silver_bronze_selected_raw_bytes`는 선택된 원문의 UTF-8 크기다. 둘 다 실제 네트워크/객체 저장소 전송 바이트가 아니다. Spark 읽기 실행 시간에는 원격 작업 대기, 데이터 전송, Python 변환·해시 검증이 함께 들어갈 수 있다. 세부 지표만으로 이 요소들을 분리했다고 주장하지 않는다.

## 병목 비교 기준

- 먼저 입력 상품·관측 수, 원문 크기, 이미지 상태, 중복/재사용 건수, Silver/Gold 출력 행 수, Delta 버전·파일 수를 비교한다.
- 다음으로 Master/Worker 가용 수와 자원, Spark/JDK 버전, 코드 commit, 새 JVM인지 재사용된 JVM인지, 실패·재시도 여부를 맞춰 비교한다.
- 단계별 실제 경과 시간을 먼저 비교하고, 그 다음 해당 단계의 내부 지표를 본다. 호출당 평균은 호출 수와 함께 본다. 첫 키 인덱스 구성·cold start와 이후 재사용을 분리한다.
- 입력과 실행 조건이 다르면 절대 소요 시간과 관찰된 차이만 보고한다. 같은 크기라는 이유만으로 같은 작업량이라고 간주하지 않으며, 동일 입력의 통제된 비교가 없으면 최적화 효과율을 계산하지 않는다.

2026-10-22에 계측한 epoch 34와 저장된 epoch 12–33 배치 이력의 대조는 [배치 시간 비교](verification/batch_timing_comparison_20261022.md)에 기록했다. epoch 34의 단계별 원본 runtime metrics와 MySQL 스냅샷은 [상세 결과 JSON](verification/detailed_metrics_single100_20261022.json)에 있다. 비교 시 phase 합계, Bronze 시작부터 활성화 완료까지, FLOW 시도 시작부터 종료까지의 시간 경계를 구분한다.

## 로그를 직접 확인할 때

요약 상태와 Spark 작업 진행은 파이프라인 폴더에서 다음 읽기 전용 조회로 본다.

```powershell
python -c "import json,run_server_pipeline as r; s=r.call('status','flow'); f=s.get('flow-progress.json',{}); print(json.dumps({'worker_alive':s.get('worker_alive'),'flow':{k:f.get(k) for k in ('status','stage','epoch','updated_at')},'log_bytes':s.get('log_bytes'),'log_age_seconds':s.get('log_age_seconds'),'error_types':s.get('error_types'),'sql_error_codes':s.get('sql_error_codes'),'spark_progress':s.get('spark_progress'),'recent_progress':s.get('recent_progress')},ensure_ascii=False,default=str))"
```

원문 로그는 Bastion 경유 Ansible `tail`로 읽는다. 아래 예시는 Master의 최근 FLOW 로그 100줄을 출력한다. `flow`를 단계명으로 바꾸거나 로그 경로를 해당 epoch/window의 파일로 바꾸면 된다.

```powershell
ssh bastion 'cd ~/pj3/ansible && ANSIBLE_NOCOLOR=1 ANSIBLE_BECOME_EXE=/usr/bin/sudo.ws ansible pj3-spark-master -m command -a ''{"argv":["tail","-n","100","/var/lib/pj3/product-pipeline/spark/production-pending-20261019/flow.private.log"]}'' --become --private-key ~/.ssh/pj3_ansible --vault-password-file ~/.config/pj3/vault-pass'
```

주요 파일은 `.../flow.private.log`, `.../epochs/<epoch>/bronze.private.log`, `.../epochs/<epoch>/windows/<window>/{silver,gold,publish,activation}.private.log`와 같은 폴더의 `*-result.json`, `*-failure.json`, `windows-progress.json`이다. epoch·window 디렉터리와 단계 영수증을 먼저 확인한 다음 해당 파일만 조회한다. private log에는 원천 데이터나 연결 오류 세부값이 들어갈 수 있으므로 외부에 공유할 때는 민감 내용을 제거한다.

## 문서와 결과 갱신

코드·지표 이름 변경은 [책임별 코드·문서 라우팅](code-routing.md)과 관련 계측 설명을 함께 갱신하고 필요한 회귀 테스트를 수행한다. 실제 서버 배포 및 배치 결과는 [실서버 검증](service_validation_20261018.md)과 날짜별 `docs/verification/` JSON에 남긴다. 성공 기록은 실패 기록을 대체하지 않으며, 측정 후 연속 처리를 재개하려면 별도의 명시적 운영 요청과 기존 배포 절차를 따른다.
