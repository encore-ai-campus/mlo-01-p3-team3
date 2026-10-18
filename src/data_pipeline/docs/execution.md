# FULL_EXPORT 실행 및 인수 기록

2026-10-16. 첫 인수 범위의 실행 코드와 로컬 검증 결과다. 전체 설계의 완료 선언이 아니다. 실행 명령은 [README](../README.md), 후속 계약은 [보강 v1.1](design_amendment_v1_1.md)을 따른다.

후속 상태: 상품 Bronze 코드를 추가했다. 이 문서의 아래 표는 기존 첫 단계의 검증 기록이며, 신규 Spark/Delta 및 남은 종단 인수는 [Bronze 실행 문서](bronze_execution.md)를 따른다.

## 구현과 책임

| 구성 | 구현 내용 | 코드 |
|---|---|---|
| 입력 동결 | raw/products/media 결합, 원천 해시·상품 집합·갤러리·실제 이미지 검증, 마지막 manifest 기록으로 봉인 | [snapshot.py](../product_pipeline/snapshot.py) |
| 식별자 | 수집기 원천 해시 유지, canonical 관측 해시, Raw/event ID 최초 접수 고정 | [contracts.py](../product_pipeline/contracts.py) |
| 운영 기록 | primary/majority 조회, majority+journal 쓰기, unique 관측 키, lease/token/heartbeat, 원천 insert-only | [mongo.py](../product_pipeline/mongo.py) |
| 이미지 | SHA-256 내용 주소, 원격 실제 바이트 검증, 조건부 업로드, 상품·URL·순서·object key 관계 | [objects.py](../product_pipeline/objects.py) |
| 상태/복구 | 이미지 벡터·revision·fingerprint 단일 문서 변경, 원천 저장 확인 후 이벤트 전달 | [runner.py](../product_pipeline/runner.py) |
| 이벤트 | 최초 ID/본문 재사용, 전달 확인·명시적 replay | [publisher.py](../product_pipeline/publisher.py) |
| 로컬 검증 | 파일 기반 원장·객체·발행 대역. SQLite 사용 없음 | [local.py](../product_pipeline/local.py) |
| 환경 분리 | CLI·비밀 환경변수·대상 fingerprint·입력 검증 선행 | [cli.py](../product_pipeline/cli.py) |

현행 20대 VM 구조를 변경하지 않으며 `pj3-object01/02` 중 이미지 서버를 임의 지정하지 않는다. Spark·MySQL·추천 서비스·검수/매핑 원장은 쓰지 않는다. 크롤러도 수정하지 않았다.

## 입력과 출력

입력은 정지/완료된 수집기 export의 `product_raw.jsonl`, `products.jsonl`이다. 옵션 `media/products_with_local_images.jsonl`은 다운로드 결과다. `run_report.json`, `gallery_report.json`은 있으면 보존한다. media의 `download_report.json`이 있으면 완료 상태를 확인한다. OS 잠금·이미지 작업 표식이 있으면 차단하고, 복사 전후 해시가 바뀌면 실패한다. 실행 중 export를 LIVE 입력처럼 사용하지 않는다.

수집기 schema 1.2, payload hash, 상품 집합 일치·중복, JSON 유효성, 이미지 경로·해시·크기·포맷·차원과 상품별 갤러리 메타데이터를 검사한다. 설명 이미지 선별이나 사이트 HTML 재해석은 수집기 책임이며 파이프라인은 전달받은 갤러리 관계를 처리한다.

스냅샷에는 `input/` 원본 복사, `observations.jsonl`, `images/sha256/` 검증 이미지, 마지막에 `snapshot.json`이 생긴다. 동일 이미지 바이트는 하나의 파일·원격 object key로 공유하며 상품별 관계는 각각 보존한다. 이후 스냅샷 수정은 검증 실패다. 재실행은 동일 스냅샷을 읽는다. 새로운 이미지 로컬 취득은 새 스냅샷/관측이며 동일 바이트의 원격 업로드 재시도는 기존 관측의 상태 revision만 변경한다.

상태 폴더의 `mode.json`은 실행 대상과 모드를 묶는다. `audit/runs/`에는 실행별 보고서, `audit/failures/`와 `audit/quarantine/`에는 실패·격리 기록, `audit/registrations/`에는 최초 접수 ID와 본문의 복구 사본을 남긴다. `rehearse`만 `ledger/`, `objects/`, `events/` 대역 파일을 만든다. live 운영 상태의 기준은 MongoDB이며 파일은 감사·복구 증거다.

## 재실행과 실패

중단 뒤 같은 snapshot/state/config로 `ingest`를 다시 실행한다. 성공 이미지도 원격 실제 바이트로 대조하므로 외부 읽기는 발생할 수 있지만 같은 바이트를 다시 쓰지 않는다. 기존 Raw는 insert-only로 대조하고 확인된 이벤트는 기본적으로 건너뛴다. Kafka 전달 결과가 불명확하면 같은 ID·본문으로 재전달한다. 소비자도 event ID 멱등 처리가 필요하며 exactly-once 전달을 주장하지 않는다.

전달 확인된 이벤트까지 다시 보내려면 `--replay-events`를 붙인다. 새 이벤트 ID를 만들지 않는다. 이미지 업로드 복구가 이미 확인된 source 이벤트를 새 관측 이벤트로 만들지는 않는다. Silver의 고정 입력 상태 대조·재평가는 별도 CLI로 구현했으며 [Silver 실행](silver_execution.md)을 따른다. 전체 종단 대조와 자동 단계 연결은 남아 있다.

외부 요청은 설정의 최대 2~3회(기본 3회)를 시도한다. Kafka는 SDK 내부 재시도와 애플리케이션 재시도를 중첩하지 않는다. 지속 실패는 다음 수동/예약 실행에서 재처리한다. DB 기록 실패 시 로컬 감사 증거를 남긴다. failures는 재시도 대상, quarantine은 불변 계약 충돌/BSON 한도 등 검토 대상이다. 과거 실패 파일은 감사 이력으로 남을 수 있으므로 현재 Mongo 상태와 최신 보고서를 함께 확인한다.

접수 원장이 유실되면 같은 상태 폴더의 최초 등록 사본으로 기존 Raw/event ID를 복구한다. 기존 Raw만 남고 접수 증거가 없으면 새 ID를 임의 발급하지 않고 중단한다. MongoDB와 감사/등록 사본을 함께 백업해야 한다. 모든 증거가 유실된 경우의 복구는 보장하지 않는다.

종료 코드는 completed=0, partial/failed=2, 사용자 중단=130이다. 이미지 결측도 partial로 보고한다. 자동 실행기는 종료 상태와 보고서를 보존해야 한다. 비밀 URI/SDK 오류 본문을 CLI 오류에 노출하지 않는다.

## 재현 가능한 검증

파이프라인 디렉터리에서 실행한다. 실제 서버 연결 없이 동작한다.

```powershell
.\.venv\Scripts\python.exe -m unittest discover -s tests -v
.\.venv\Scripts\python.exe -m tests.smoke_saved_export --export-dir 'C:\path\to\completed-export' --media-manifest 'C:\path\to\completed-export\media\products_with_local_images.jsonl' --output .runtime/smoke-new --sample-size 3
```

smoke 출력 경로는 새 폴더여야 한다. 선택 상품과 이미지 사본만 별도로 처리하며 원본 JSONL 세 파일의 전후 해시를 대조한다. 원본 전체 JSONL 해시 계산은 큰 작업에서 시간이 걸릴 수 있다.

| 검증 | 2026-10-16 결과 | 한계 |
|---|---|---|
| 자동 테스트 | 49개 통과 | 로컬/전송 대역 기반 |
| 수집기 원천 해시 | 커밋 `67e837d` 해시 함수에서 만든 golden vector 2개 일치 | 수집기 계약 변경 시 재검토 |
| 실제 보관 데이터 | run `2ab52dfe-92c3-4d0c-9ec3-879462a6c657`, 상품 `122661898`, `5097200`, `96056663` | 전체 71카테고리 처리량 검증 아님 |
| 첫 실행 | 원천 3개, 이벤트 3개, 이미지 관계 9개·객체 9개 | 파일 대역 적재 |
| 같은 입력 재실행 | 신규 원천 0, 이벤트 0, 객체 0 | 원본 JSONL 해시 유지, 외부 연결 0 |
| 장애/충돌 | 5개 중단 지점, 응답 유실, 접수 복구, lease/CAS, 원천/이벤트 충돌, 스냅샷 교체·경로 이탈 차단 | 실제 다중 호스트 경쟁·네트워크 장애 미검증 |
| SDK 계약 | PyMongo BSON/16MiB, botocore Stubber 조건부 S3 요청, Kafka callback/error/timeout | 실제 서버 동작 보증 아님 |

로컬 증거: 최초 `.runtime/unit-tests-20261016.txt`, `.runtime/actual-smoke-repro-v1/smoke_report.json`; 중단 후 재검증 `.runtime/unit-tests-resumed-20261016.txt`(49개 통과), `.runtime/actual-smoke-resumed-v1/smoke_report.json`(동일 3상품/9이미지·중복 생성 0). 실행 데이터·가상환경은 Git 제외다. 테스트와 golden vector는 [tests](../tests)에 남겼다.

검증 환경: Python 3.13.14, Pillow 12.3.0, PyMongo 4.19.0, boto3 1.43.114, confluent-kafka/librdkafka 2.16.0. [requirements.lock.txt](../requirements.lock.txt)은 이 환경의 설치 버전이며 타 OS 검증 결과를 뜻하지 않는다.

## 남은 인수와 구현 한계

- 실제 3노드 MongoDB majority+journal·장애 전환, S3 조건부 쓰기·동시 업로드·인증, Kafka 전달/복구는 시험 환경에서 인수해야 한다. 읽기 전용 preflight는 Mongo topology, 버킷 접근, 토픽 복제 수/ISR까지 확인하며 브로커 minISR 설정은 별도 확인한다.
- 이번 버전은 단일 프로세스의 순차 상품 처리다. Mongo fencing은 있지만 실제 다중 실행 경쟁·전체 처리량/용량 시험은 남아 있다. SDK timeout은 설정했으나 여러 청크로 읽는 객체 전체 작업 시간의 고정 상한을 보장하지 않는다.
- 입력 오류는 준비 단계 전체를 fail-fast한다. 잘못된 행을 제외한 부분 스냅샷은 미구현이다. JSONL 한 행 32MiB, 이미지 하나 20MiB, Mongo BSON 문서 16MiB 한도다. 큰 원문 외부 참조 fallback은 미구현으로 한도 초과는 격리한다.
- LIVE cursor/마이크로배치·이미지 후속·S3 chunk/ready 전송은 이후 구현·로컬 검증했으며 [실행 가이드](live_execution.md)를 따른다. 이후 원격 ready 발견·파일 queue·예약 재시도·NiFi REST 제어·Silver/Gold 재평가·문서 기반 MySQL 게시까지 구현했다. [현재 현황](status.md)과 [운영 가이드](operations_execution.md)를 따른다. 실제 NiFi 일정 활성화와 전체 Spark 단계 자동 연결·종단 인수는 남아 있다.
- 파일 원자적 교체·파일 fsync 및 POSIX 디렉터리 fsync를 사용한다. Windows/NAS 전원 장애 내구성은 대상 파일시스템 검증이 필요하다. 동결은 writer 잠금과 전후 해시에 의존하므로 입력 작업 종료를 운영 절차로 보장한다.
- VM/Linux/EC2에 이식하도록 환경 설정을 분리했지만 실제 배포·권한·TLS 시험은 남아 있다. 이미지 버킷 역할과 승인된 endpoint를 확정해야 한다.

상위 인프라·데이터 모델·Kafka·직접 추천 문서는 수정하지 않았다. 다음 단계는 실제 인프라 종단 인수와 Silver 이후 상태 재평가 계약 구현이다.
