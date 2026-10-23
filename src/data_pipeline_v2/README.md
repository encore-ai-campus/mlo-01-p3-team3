# 새 상품 데이터 파이프라인 실행기

`develop/src/data_pipeline_v2`에서 시작하는 독립 실행기다. 기존 FLOW를 감싼 명령이 아니라 범위 고정, 묶음 정제, 완료 증거, 게시 인계와 복구를 새로 구현했다. 재설계 목표 경로인 [크롤 완료 유한 배치](#크롤-완료-유한-배치-실행-nifi--outbox--kafka--spark)도 같은 패키지에 있다. 현재는 **로컬 구현·대역 검증 단계**다. 서버 배포, 실제 Kafka·Delta 종단 시험, 실서비스 처리량 검증은 하지 않았다.

별도 담당 승인·이미지 활성화·승인 후 Silver/Gold 재실행은 실행 경로에 없다. 품목 매핑과 원천 사실을 기술적으로 검사해 직접 게시한다. 원천이나 관련 변환 정책이 바뀔 때만 새 작업을 만든다. `QUALITY_WAIT`는 필수 데이터 부족이라는 처리 결과이며 사람이 승인해야 진행하는 큐가 아니다.

| 작업 | 먼저 읽을 문서 | 함께 확인할 파일 |
|---|---|---|
| 계층·책임·변환 변경 | [구조와 불변 조건](docs/architecture.md) | `pipeline_v2/domain`, `pipeline_v2/ports.py`, `pipeline_v2/application` |
| 로컬 실행·입력 준비 | 이 README | `config/local.example.json`, `pipeline_v2/interfaces/cli.py` |
| 크롤 완료 유한 배치·NiFi 연결 | 이 README의 유한 배치 절, [NiFi 기동 계약](docs/nifi-contract.md) | `config/batch.*.example.json`, `pipeline_v2/interfaces/batch_cli.py`, `pipeline_v2/application/batch.py` |
| 서버 이전·환경·중단·복구·반복 배포 | [서버 이전](docs/server-migration.md) | `config/*server.example.json`, `tools/release.py`, `tools/export_batch_input.py` |
| 검증 범위와 성능 근거 | [검증 기록](docs/validation.md) | `tests/` |

## 실행 구조

```text
CLI → 구성(bootstrap) → capture / Coordinator / Processor
                         ↓                    ↓
                    State 저장소         순수 변환(domain)
                         ↓                    ↓
                  로컬 파일 상태     Source / Lake / Publisher 포트
                                      ↓       ↓       ↓
로컬 모드                         JSON      File    File
서버 모드                         Mongo     Delta   MySQL
```

서버 모드도 같은 새 유스케이스를 호출한다. 기존 `product_pipeline`에서는 원천 계약 검사·사실 파서·SQL 연결/스키마 검사·lease·Garage 연결 기반만 가져온다. 기존 CLI, FLOW worker, Gold 결정기, 검수·활성화 실행기는 가져오지 않는다. AST 의존 검사로 이 경계를 확인한다.

## 로컬 실행

Python 3.11~3.14와 기존 파이프라인의 파서 의존성을 사용한다. 기존 파이프라인 패키지가 설치된 환경에서는 `src/data_pipeline_v2`를 작업 폴더로 실행한다. 설치하지 않고 소스로 실행할 경우 다음처럼 경로를 지정할 수 있다.

```powershell
# develop 폴더에서 실행
$env:PYTHONPATH="$PWD\src\data_pipeline_v2;$PWD\src\data_pipeline"
python -B src/data_pipeline_v2/run_pipeline.py --help
Copy-Item src/data_pipeline_v2/config/local.example.json src/data_pipeline_v2/config/local.local.json
```

실행용 설정의 `source_file`은 기존 Mongo 원천의 **wire 형식**으로 내보낸 JSON 배열을 가리켜야 한다. 단순 상품 목록이나 수집 HTML을 바로 받는 인터페이스가 아니다.

```json
[
  {"document": {"_id": "24자리 ObjectId 문자열", "...": "원천 계약의 전체 필드"},
   "image_state": {"...": "해당 관측의 image_state_vector/revision/fingerprint/checked_at 전체 필드"}}
]
```

위 예시는 구조 설명용이며 실행 가능한 원천을 대신하지 않는다. 상품별 필요한 전체 관측 이력을 넣고 원천 해시를 유지한다. 로컬 JSON 파일은 최대 128MiB다. 설정 경로는 **설정 파일의 위치**를 기준으로 해석한다. `policy.mappings`에는 실제 서비스 품목 UUID와 원천 분류 대응을 넣는다. 빈 매핑은 임의 품목을 만들어 내지 않고 `QUALITY_WAIT`로 끝난다. 원천·상태·Lake·게시 출력 경로는 서로 분리한다.

```powershell
python -B src/data_pipeline_v2/run_pipeline.py --config src/data_pipeline_v2/config/local.local.json capture --scope local-001
python -B src/data_pipeline_v2/run_pipeline.py --config src/data_pipeline_v2/config/local.local.json run --scope local-001 --max-ticks 100
python -B src/data_pipeline_v2/run_pipeline.py --config src/data_pipeline_v2/config/local.local.json status --scope local-001
```

`capture`는 처리할 상품·관측과 관련 정책을 고정한다. `tick`은 제한된 한 묶음, `run`은 반복 실행이다. Gold와 게시 진행을 따로 기록하므로 첫 tick에 Gold만 완료되고 다음 tick에 게시될 수 있다. 각 명령은 단계별 `elapsed_ms` 이벤트를 상태의 `metrics/stage-times/`에 보존한다. `run`과 `status`의 최종 JSON은 `timing.stages` 요약을 포함하며, Mongo 본문 조회·Silver/Gold 정제와 테이블 저장·MySQL 사전검사/게시/감사를 나눠 볼 수 있다. `status`의 `gold_complete`, `delivery_complete`도 함께 본다. 대기량·디스크 한계 때문에 `run`의 반복 한도에 도달하면 `RUNNING`으로 반환될 수 있으며 완료라고 해석하지 않는다.

실행을 중단한 뒤 `RUNNING`이 남았으면 기존 프로세스와 Spark 작업의 정지를 확인한다. `recover --job JOB_HASH --stop-evidence "실제 정지 확인 근거"`로 명시적으로 재개한다. 검수 승인이 아니라 중복 writer 방지를 위한 장애 복구다. 차단 원인이 변환 입력이면 입력을 고치고 **새 scope**를 만든다. 원인이 이력 한도라면 자원 근거로 한도를 조정한 뒤 recover한다.

## 크롤 완료 유한 배치 실행 (NiFi → Outbox → Kafka → Spark)

재설계 목표 경로다. 크롤이 끝난 묶음만 Kafka partition별 offset 범위로 고정하고, Spark가 그 범위만 유한 배치로 처리해 Bronze·Silver·Gold를 만든 뒤 MySQL에 직접 게시한다. 상시 consumer·`readStream`·streaming checkpoint는 없다. 위의 `capture/tick/run` 실행기는 비교·이력용으로 남아 있다.

| 순서 | 명령 | 하는 일 | 완료 증거 |
|---|---|---|---|
| 1 | `batch-prepare --manifest crawl.json --input rows.jsonl` | 크롤 manifest(`SOURCE_COMPLETE`)와 입력 해시를 확인하고 행별로 받아들일지 판정한 뒤 원천과 Outbox를 대조 저장 | `crawl` 원장 |
| 2 | `batch-relay --batch ID [--generation N]` | Outbox 이벤트를 `product_key` 키로 Kafka에 발행하고 ack로 partition별 `[start, end)` 범위 확정 | `delivery` manifest `COMPLETE` |
| 3 | `batch-submit --batch ID` | 같은 batch·이벤트 집합·정책·대상을 하나의 job으로 등록하고 대기 용량 예약 | `job` 원장 `QUEUED` |
| 4 | `batch-work --job JOB --phase process` | 고정 offset 범위만 읽어 Bronze→Silver→Gold를 처리 | stage manifest, `GOLD_COMMITTED` |
| 5 | `batch-work --job JOB --phase publish` | 완료된 Gold를 묶음으로 읽어 MySQL 게시 | publication manifest, `COMPLETED` |

`--phase all`(기본값)은 4·5를 이어서 실행한다. 처리와 게시는 서로 다른 writer 잠금(`finite-process.lock`, `finite-publish.lock`)을 쓴다. 그래서 n번째 배치를 게시하는 동안 n+1번째 배치를 처리할 수 있다. 같은 대상의 writer는 계속 하나다. 겹쳐 실행할지는 backlog와 자원을 측정한 뒤 정한다. 서버 모드에서 쓰기가 있는 명령은 `--apply`가 필요하다.

| 운영 명령 | 용도 |
|---|---|
| `batch-status --job JOB` | job과 단계별 manifest 요약을 원장에서 바로 읽는다. 전수 감사는 하지 않는다. |
| `batch-backlog [--rebuild]` | 미완료 batch 수·바이트·가장 오래된 대기 시간·포화 여부. NiFi back pressure의 판단 근거다. `--rebuild`는 job 원장으로 색인을 다시 만든다. |
| `batch-audit --job JOB` | 단계 출력·격리 metadata·게시 receipt를 다시 대조하는 별도 상세 감사 |
| `batch-recover --job JOB --stop-evidence 근거 [--generation N]` | 정지를 확인한 뒤 끝나지 않은 단계만 다시 시도한다. Kafka offset이 만료됐으면 `batch-relay --generation N`으로 새 전달 generation을 만든 뒤 지정한다. |
| `batch-retract --product-key K --raw-document-id R --reason CODE` | 특정 원천 버전의 명시적 철회 기록. 다른 관측은 무효화하지 않는다. |
| `batch-repair --products K1,K2` | 해당 상품만 보존된 Bronze 이력으로 다시 계산·게시한다. 크롤 결과를 다시 만들지 않는다. 남은 근거가 없으면 숨긴다(`WITHDRAWN`). |
| `batch-retention [--as-of UTC]` | 격리 원문은 30일, 비식별 오류 metadata는 90일 뒤 삭제한다. 정상 원천·Bronze 보존 계약은 바꾸지 않는다. |
| `batch-quarantine-read --event-id E --purpose P --operator O --output FILE` | 접근 기록을 먼저 남기고 격리 원문을 운영자 파일로만 내보낸다. 표준 출력·로그에는 쓰지 않는다. |
| `metrics --scope ID`, `spark-metrics --event-log PATH --scope ID` | 단계별 시간 요약, Spark event log 오프라인 분석 |

입력 처분은 품질·진행 상태와 별도 축이다.

| 처분 | 조건 | 저장 |
|---|---|---|
| `ACCEPTED` | 식별자·해시·원천 계약 통과 | 원천 스냅샷, Bronze, Silver 입력 |
| `INPUT_QUARANTINE` | 식별자·해시 오류 또는 원천 계약 위반이고 격리가 허용됨 | 제한 접근 `quarantine_raw`(30일)나 원천 참조, 비식별 metadata(90일) |
| `PAYLOAD_OMITTED` | 위와 같으나 격리가 허용되지 않거나 행이 바이트 상한을 넘음 | payload 없이 사유·수신 위치만 기록 |
| `SOURCE_METADATA_ONLY` | `admission.field_allowlist` 밖의 필드가 있음 | payload·payload hash·필드값을 어디에도 쓰지 않고 사유만 기록 |

식별자를 만들 수 없는 행은 수신 위치(`crawl_batch_id` + 입력 순번)를 재처리 식별자로 쓴다. 크롤러가 같은 행을 다시 보내면 하나로 접는다. 같은 event ID에 다른 내용이 오면 덮어쓰지 않고 격리한다. 한 행의 오류가 크롤 전체를 막지 않는다. 미완료 크롤, 입력 전체 바이트 초과, 보관 불가 설정은 쓰기 전에 전체를 중단한다. Silver에서 고칠 수 없는 관측은 원문을 복사하지 않고 Bronze 참조와 사유만 담은 `QUALITY_HOLD` 결과로 남긴다.

설정 예제는 `config/batch.local.example.json`, `config/batch.server.example.json`이다. `admission`의 `field_allowlist`, `retention`의 30/90일, `batch_limits`의 대기 배치 수·바이트·대기 시간은 초기값이다. 실제 값은 원천별 allowlist와 복구 SLA, 보존 정책을 확인한 뒤 배포 설정에서 확정한다. 로컬 모드는 Kafka 대신 파일 broker를 쓴다. 이는 offset·재전달 계약을 확인하기 위한 대역일 뿐 Kafka 성능을 대신하지 않는다.

```powershell
# develop 폴더, PYTHONPATH는 위와 같다
$cfg = "src/data_pipeline_v2/config/batch.local.json"   # 예제를 복사해 정책·경로를 채운 파일
python -B src/data_pipeline_v2/run_pipeline.py --config $cfg batch-prepare --manifest crawl.json --input rows.jsonl
python -B src/data_pipeline_v2/run_pipeline.py --config $cfg batch-relay --batch crawl-20261023-01
python -B src/data_pipeline_v2/run_pipeline.py --config $cfg batch-submit --batch crawl-20261023-01
python -B src/data_pipeline_v2/run_pipeline.py --config $cfg batch-work --job JOB_ID
```

NiFi 연결 계약은 [NiFi 기동 계약](docs/nifi-contract.md)에 있다.

## 로컬 검증

```powershell
python -B -m unittest discover -s src/data_pipeline_v2/tests -v
# 실제 로컬 Spark JVM 시험(선택): Spark Gold 계산과 유한 배치 처리기
$env:PIPELINE_V2_TEST_SPARK="1"; python -B -m unittest discover -s src/data_pipeline_v2/tests -p "test_spark*.py" -v
```

합성 원천과 임시 디렉터리, 스키마의 필수 컬럼을 검사하는 SQL 대역을 사용한다. 실제 상품·임베딩·검색 인덱스를 생성하거나 유료 API를 호출하지 않는다. 테스트 데이터는 종료 때 정리한다. 로컬 파일 게시가 실제 MySQL 연결 시험을 대신하지 않는다.

## 서버 이전의 경계

서버 설정 예제는 미완성 값을 바꾸기 전에는 실행할 수 없다. 서버 `tick/run`은 `--apply`가 있어야 한다. 이 옵션은 오접속 방지용 쓰기 선택이며 상품 승인 단계가 아니다. 기존 프로세스와 새 프로세스를 같은 서비스 DB에 동시에 쓰지 않는다.

새 Delta는 `/direct-v2`로 끝나는 별도 경로를 사용하고 기존 Delta를 덮어쓰지 않는다. 로컬 FileLake의 절대 경로가 묶인 상태·영수증을 서버 Delta 영수증으로 바꿔 복사하지 않는다. 코드와 설정 형식을 옮긴 뒤 서버 원천으로 새 범위를 고정한다. 기존 비용 큰 결과는 별도 manifest 검증·재사용 판단 없이 재생성하지 않는다.

기존 MySQL 테이블은 사용하지만 출처 참조 JSON은 **schema_version=3 / direct-catalog-1**이다. 기존 추천 카탈로그 어댑터의 v2 Gold 참조·승인 이미지 전제와 호환되는지 전환 전에 별도로 확인해야 한다. 새 실행기만 설치해 추천 서비스 연동까지 끝났다고 설명하지 않는다. 자세한 인수 항목은 [서버 이전](docs/server-migration.md)에 있다.
