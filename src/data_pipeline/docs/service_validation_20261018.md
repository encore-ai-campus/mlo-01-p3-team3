# 상품 파이프라인 실제 검증·남은 인수

현재 전달 기준은 2026-10-22 10:31 KST다. 파일명은 기존 링크 호환을 위해 유지한다. 아래 시점별 상태와 접힌 10월 18일 시험 기록을 구분한다. 사용자 입력·해석·임베딩·추천·UI 연동은 다른 담당자의 범위다.

## 2026-10-22 범위 읽기·파일 정리 완료와 단일 배치 시험

Master·Worker 3대 Kafka TLS 사전 확인, 새 코드 배포, 서버 테스트 183개 통과, Bronze·Silver·Gold 19개 테이블 정리 및 행·스키마·정체성 불변성 검증이 완료됐다. 기존 100건 선택과 원문을 새 범위 읽기로 재검증했다. [실제 정리 결과](verification/range_index_compaction_20261022.json).

정리 후 자동 연속 재개는 하지 않았다. 사용자의 별도 승인으로 10:30:56 KST에 100건 한 배치를 서버 백그라운드에서 시작했으며 게시·활성화 뒤 정지 표식을 유지한다. 이 기록은 해당 배치의 완료·속도 개선을 확정하지 않는다. 결과는 서버 `perf-20261020/single100-range-20261022-result.json`, 실행 상태는 `single100-background-20261022.json`, 5초 간격 메모리 표본은 각 Spark VM `/var/tmp/pj3-pipeline-single100-20261022/result.json`에서 확인한다. 200건 설정은 적용하지 않았다.

이전 epoch 14·30의 입력량·처리량과 12~31 시간 추이는 [조회 근거](verification/batch_input_timing_audit_20261022.json)에 보존했다. 이 값은 이번 배포 전 측정이며 전체 전송 바이트·SDK 내부 재시도·대기 시간까지 계측한 결과는 아니다.

## 2026-10-21 오후 100개 배치 완료·후속 개선 시험

19:08:27 KST 추가 개선을 적용한 연속 처리를 재개하고 배치 후 정지 설정을 해제했다. 19:25 새 epoch 8 배치의 최초 게시 완료·활성화 진행을 확인했다. NiFi 2.12.0의 전용 운영 그룹을 RUNNING으로 구성해 5분 주기 제한 SSH 호출이 실제 Master에 도착했으며, PID 176146을 재사용한 `ALREADY_RUNNING`을 확인했다. 기존 검증 그룹은 보존했다. [NiFi 운영 연결](nifi_production.md)은 현재 구성과 정지·복구 경계를 설명한다. 전체 완주나 신규 크롤링 일정까지 검증했다는 의미는 아니다.

16:06:53 KST, epoch 7의 실제 100관측 배치 완료 시점 업무 DB는 상품 **646 / ACTIVE 580 / 대기 66**, 해당 증가분은 상품 +100 / ACTIVE +89다. 당시에는 `PAUSED_AFTER_BATCH`였으며, 이후 위 기록대로 연속 처리를 재개했다. 아래 546건은 직전 비교 기준이다.

이번 기록은 Silver 326,334ms, Gold 310,077ms, 최초 게시 68,716ms, 활성화 전체 721,813ms다. 활성화 중 검수 대기 재적재 100건을 생략하고 기존 검증 행 재사용 누계 5,463건을 확인했다. [시험 결과 JSON](verification/downstream_optimization_20261021.json)과 [개선 상세](downstream_optimization.md)를 따른다. 초기 시도의 null 갤러리 입력 오류·보완·재개는 별도 기록하며 단일 연속 실행 벤치마크로 설명하지 않는다.

15:14 KST에 실제 100관측 배치가 게시·활성화까지 완료됐다. 실제 MySQL은 상품 **546 / ACTIVE 491 / 대기 55**이며 이전 대비 상품 +98, ACTIVE +85다. Bronze 181,445ms, 대상 선정 80,006ms, Silver 355,957ms, Gold 319,447ms, 최초 게시 75,766ms, 활성화 849,328ms다. 해당 배치는 `PAUSED_AFTER_BATCH`로 종료됐고 이후 Driver·Worker 3대 유휴를 확인했다.

이후 [Silver·Gold·활성화 비용 개선](downstream_optimization.md)을 적용했다. 로컬 핵심 시험 93개·서버 관련 시험 90개 통과 후, 15:28:02 KST에 다음 실제 100개 배치를 시작했다. 완료 뒤 결과를 기록하며 시험 중에는 전체 완료로 간주하지 않는다.

## 같은 날 앞선 Bronze 개선·격리 시험 기록

100관측 설정으로 연속 처리한 뒤, 운영자가 분석·개선을 위해 중지를 요청했다. 6번째 epoch의 Bronze를 기존 정상 종료 경로로 중지했고, Driver·활성 Spark 앱·Bronze 잠금 0개와 Worker 3대 idle을 확인했다. 원천·부분 성공 Delta·체크포인트는 유지했으며 미완료 epoch를 완료로 표시하지 않았다. 실제 MySQL 조회는 상품 **448 / ACTIVE 406 / 대기 42**다. 아래 234건은 오전 한 배치 시험 당시의 기록이다.

묶음 사전 조회·신규 행 append·Mongo `$in`·메타데이터 통합·구간별 계측·coordinator Spark 세션 재사용 경로를 구현·배포했다. 로컬 작업 파일 기준 전체 회귀 492개, 서버 관련 68개 통과. 실제 원천 12개 격리 비교의 최초 처리 82,569→48,432ms, 재실행 22,567→4,674ms이며 동일 원문·버전 불변·응답 유실 재개·한 application 재사용을 확인했다. 운영 연속 처리는 다시 켜지 않았다. [개선·검증 범위](bronze_optimization.md)와 [공개 결과](verification/bronze_optimization_20261021.json)를 따른다.

## 2026-10-21 오전 Kafka 보관 만료 복구 기록

24시간 보관으로 Kafka 메시지가 삭제돼 offset 310에서 재개하지 못한 장애를 확인했다. 해당 토픽을 14일 보관으로 바꾸고, Mongo 원장의 81,882개 고정 이벤트 목록·기존 대상 ID를 유지한 새 체크포인트 세대·1,000건 queue 상한의 재발행 경로를 추가했다. 이전 체크포인트를 삭제하거나 data loss 검사를 끄지 않았다.

실제 한 배치는 31분 17초에 Bronze→Silver→Gold→MySQL→활성화 평가를 완료하고 정지했다. 새 상품 25개가 게시됐고 18개 ACTIVE / 7개 품질 대기다. 현재 업무 DB는 상품 234 / ACTIVE 213 / 대기 21이며 전체 65,498개 완료가 아니다. 재발행 1,100건과 queue 상한 도달 뒤 추가 발행 0건을 확인했다. 로컬·서버 관련 자동 시험은 각각 84개다. [복구·재개 가이드](kafka_retention_recovery.md)와 [공개 결과](verification/kafka_retention_recovery_20261021.json)를 따른다. 처리 속도 병목은 별도로 남아 있다.

## 오전 복구 시험 당시 실행·배포 범위

| 항목 | 확인한 내용 | 남은 범위 |
|---|---|---|
| 업무 게시 | 실제 상품 234 / ACTIVE 213 / 대기 21. 마지막 25관측 중 18 ACTIVE·7 품질 대기 | 65,498상품 전체 완주·필드/이미지 확보율 전수 확인 |
| Bronze → MySQL | 복구 세대 한 배치 커밋 후 Silver·Gold·게시·활성화 완료 | 장기 연속 실행·실제 노드 장애·처리량 개선 |
| 처리 경계 | Bronze 100이벤트 / 이번 downstream 25관측, 고정 선택·성공 영수증 재사용 | 규모 확대 시 처리량·자원 재측정 |
| Spark·Delta | 클러스터 실행, 세션 공유·묶음 버전/기존 행/출력 proof 조회·고정 입력 캐시 | 전체 배치 속도 보장 없음. Bronze 14분 12초가 남은 병목 |
| 검수·활성화 | 내부 프로젝트 정책의 별도 reviewer·감사 원장·guard, 변경 관측 중심 재평가 | 7개 품질 대기를 임의 승인하지 않음; 추천 임베딩·인덱스는 별도 |
| Kafka 복구 | 14일·2GiB 보관, 81,882 고정 이벤트 계획 중 1,100개 재발행, 미소비 queue 1,000 상한 | 남은 80,782개 재발행·전체 재처리; 자동 보충 hook 배포 후 장기 운전 미실시 |
| JAR·작업 폴더 | 3 Worker의 공용 원본 보존·복사본 0개, 두 실제 앱 확인·완료 앱 TTL 1시간 | 장기 디스크 점유 감시 |
| 실행 상태 | 마지막 한 배치 후 PAUSED_AFTER_BATCH, 다음 window 미실행 | 운영자가 정지 제어 확인 후 재개; 이번 문서 작업으로 재시작하지 않음 |
| NiFi·이관 | 기존 제한 SSH와 격리 timer, 상태 pack/restore 코드 | 업무 전체 일정 인수·외부 DB/객체/Delta 백업 복원·AWS 이전 |

실제 업무 실행 경로는 Spark Master의 `/var/lib/pj3/product-pipeline/spark/production-pending-20261019`이다. 기존 release/venv를 기반으로 작업 루트의 `source/`와 실행기를 추가 배포했다. 아래 초기 release SHA만으로 최신 실행 소스가 전부 같다고 판단하지 않는다. 최신 배포 근거는 작업 루트 `kafka-recovery-20261021/deployment-final.json`, `verification-final.json`, `perf-20261020/recovery-single-batch-result.json`이며 Git 전달 이력은 [변경 이력](implementation_history.md)을 따른다. 비밀·개인 로그는 공개 저장소에 넣지 않는다.

관련 Kafka 자동 시험은 로컬·서버 각각 84개, 공용 JAR·정리 관련 로컬 시험은 26개 통과했다. 부분 시험 수를 합쳐 전체 회귀 수로 보고하지 않는다. 최신 커밋 준비의 전체 로컬 회귀 결과는 변경 이력에 따로 기록한다.

## 남은 인수

- 전체 65,498개 상품의 end-to-end 완주·누락·품질 대기·이미지 연결 검증. 상품 수와 이미지 버전 등을 포함한 81,882개 이벤트 수는 다르다.
- 자동 재발행 보충을 포함한 장기 연속 실행, 장기 backlog·디스크 한도·중앙 경보·처리 시간 측정.
- NiFi 신규 수집 run 발견·수집 일정, 수집과 동시에 진행하는 LIVE 인계, 목적 환경에서의 백업·복원·재개와 물리 장애 시험. 기존 업무 연속 처리의 timer 제어는 위 19:25 기록대로 연결했다.
- 기존 서비스 ORM/baseline과 추천 담당자 산출물 인수. ACTIVE를 임베딩/인덱스 생성 완료로 해석하지 않는다.

<details>
<summary>2026-10-18 초기 기술 시험·준비 이력 — 현재 현황 아님</summary>

다음은 그 날짜의 증거·제약을 보존한 기록이다. 당시 “없음/미시작/필요” 설명을 현재 실행 상태로 사용하지 않는다. 최신 업무 게시와 복구 결과는 위 표가 우선한다.

## 결과 요약

| 검증 | 결과와 실제 범위 |
|---|---|
| Mongo → Garage → Kafka → Bronze | 실제 수집 3상품·9이미지 관계. 원문/객체 바이트 재조회, raw 3/event 3/quarantine 0 |
| 반복·재전달 | 같은 snapshot 재실행 시 raw/object/event 추가 0. 같은 event 재발행·동일 checkpoint 재소비 후 raw/event 3/3 유지 |
| 등록 단계 실패 복구 | 처음 등록한 3개 원천 ID·event ID를 수정 후 재사용. 원문을 삭제하거나 ID를 재발급하여 우회하지 않음 |
| Worker 3대 | .122/.123/.124에서 실제 barrier task, Garage Delta 최초 저장·MERGE·고정 버전 읽기. 최신 2행/버전0 1행, 시험 prefix 삭제 |
| Silver | 실제 Bronze version 1의 한 상품. 실제 Mongo 이미지 상태·MongoLease·Garage Delta 사용, 8개 테이블 저장 및 반복 재사용 |
| Gold | 고정 Silver 8개 버전으로 한 상품·이미지 4관계 처리·반복 재사용. recommendation_ready 0 |
| 서버 간 인계 | Master → Garage ready-last → NiFi 수신. 3상품/9이미지 관계와 snapshot/file/chunk hash 일치, 재전송 객체 추가 0 |
| MySQL 문서 계약 | 실제 MySQL01 8.4.11의 별도 시험 스키마. 9개 게시·복구 시나리오 통과, 시험 스키마·계정 삭제 |
| 실제 프로세스 중단·큐 복구 | 별도 프로세스 exit 130 뒤 RUNNING을 명시 복구. 실제 INGEST handler 재실행 성공·성공 receipt 보존 |
| 제한 SSH | NiFi → Master의 고정 tick 성공. id 및 tick;id 차단. 별도 키·호스트 키 고정·포워딩 제한 |
| 실제 NiFi | 2.12.0 인증·bundle 대조·별도 프로세서 생성·잠시 timer 실행·정지. Master 감사 기록 2→14, 최종 active thread 0/STOPPED |
| 자동 시험 | 전체 252개/116.362초 통과. Linux VM의 운영/LIVE/전송 파일·대역 시험 43개/1.056초 통과 |

실제 서비스별 결과는 [1차 근거](verification/services_20261018/public-results.json), [최종 MySQL·SSH·NiFi·배포 근거](verification/services_20261018/final-public-results.json)에 있다. 원천 ID·event ID·snapshot ID·고정 버전은 결과에 보존했다. 비밀번호·개인키·토큰·보호 설정 내용은 포함하지 않는다.

## 수정한 오류

1. MongoLedger에서 읽은 등록 행에는 BSON datetime인 lease_until이 포함된다. runner가 행 전체를 JSON 등록 이력에 저장해 TypeError가 발생했다. 이력을 observation_key/target_fingerprint/identity/document/event의 불변 복구 정보로 제한했다. native datetime 재현 테스트는 수정 전 실패, 수정 후 통과했으며 원천·event ID 복원과 반복 무쓰기까지 검사한다. 실제 서버의 첫 실패 3건은 그대로 재개해 성공했다.
2. NiFi 버전 조회를 잘못된 `/controller/about`에서 공식 `/flow/about`으로 수정했다. [공식 REST 계약](https://nifi.apache.org/nifi-docs/rest-api.html)을 대조했고 실제 2.12.0에서 인증·plan 확인을 통과했다. 대역 테스트의 URL도 함께 수정했다.
3. NiFi와 Driver가 다른 VM이므로 `remote_processor_plan` 및 CLI `--remote-ssh-config/--remote-target`을 추가했다. 기존 `--python` 로컬 실행과 상호 배타적이다. alias/경로에 명령 삽입을 허용하지 않으며, 실제 서버는 SSH 강제 명령으로 tick만 허용한다.

자동 테스트는 `python -m unittest discover -s tests -p 'test_*.py' -q`로 실행했다. 최종 로그는 로컬 `.runtime/server-final-regression.txt`다. 이전 251개/88.447초 및 관련 28개/29.154초는 당시 결과이며 최종 252개에 더해 총계를 만들지 않는다. Linux 43개는 실제 파일과 SDK 대역 시험이다. 실제 원격 LIVE daemon이나 외부 저장소 백업·복원 시험으로 확대하지 않는다.

## 실제 자료와 시험 계약의 구분

Bronze에는 기존 실제 수집 표본 3개를 사용했다. snapshot ID는 `8be2d6779ac2901cc91659bbcf82f81a7f902c75cbcec61a55a2aa2030c7c868`이다. 원본 수집기·checkpoint·잠금·이미지 파일을 변경하지 않았다.

Silver/Gold는 실제 원천·실제 Mongo 이미지 상태·실제 잠금·실제 Delta 저장을 사용했지만, 매핑/검수/링크/규칙은 명시적 NOT_PROVIDED, Gold 정책은 fixture다. 담당 등록 export를 위조하지 않고 엔진 함수를 직접 시험했다. 운영 CLI의 NORMALIZATION/GOLD_BUILD 등록 인수 완료가 아니다. 결과는 둘 다 QUALITY_WAIT이며 확인하지 않은 판매처 Offer나 추천 승인을 만들지 않았다.

MySQL 세 서버를 조회했을 때 업무 테이블 없이 기존 시험용 probe 테이블들만 있었다. mysql01은 writable Primary, mysql02/03은 read_only와 super_read_only가 1이었다. 저장소에는 publication_reference_v1.sql과 예시 등록 JSON만 있었으며 확인한 Webapp 경로에도 서비스 ORM/DDL/입력 export를 찾지 못했다. 이 조회는 서버 전체의 모든 임의 경로에 파일이 없다고 증명한 것은 아니다.

MySQL 시험은 pipeline_verify_0594f7b7이라는 별도 스키마·임시 계정에 문서 DDL과 합성 승인 fixture를 넣고 실행했다. Unix socket으로 실제 서버 SQL 동작을 검사했다. 기존 운영 스키마·검수 원장은 수정하지 않았고 시험 스키마·계정은 삭제했다. 실제 네트워크 TLS·서비스 DDL/계정·실제 Gold의 준비 완료 상품 게시를 통과한 것으로 보지 않는다.

## NiFi → Master 실행 경로

사용자가 승인한 전용 SSH 방식을 구성했다.

- NiFi 개인키: `/etc/pj3-secrets/pipeline-trigger/id_ed25519` — pj3-nifi 소유 0600, 디렉터리 0700. 키는 다른 서버나 로컬 Git에 복사하지 않았다.
- NiFi SSH 설정: `/etc/pj3-clients/pipeline-trigger/ssh.conf`, 별도 known_hosts에 Ansible로 확인한 Master 공개 호스트 키를 고정했다.
- Master 로그인 계정: pj3-pipeline-trigger. 비밀번호 잠금, root 소유 authorized_keys의 from=.125/restrict/forced command 적용.
- Master 강제 게이트: `/usr/local/libexec/pj3-pipeline-ssh-gate`. SSH_ORIGINAL_COMMAND가 정확히 tick일 때만 실행한다.
- 허용된 sudo: pj3-spark 계정으로 `/usr/local/libexec/pj3-pipeline-verify-tick` 한 명령만. root 실행 권한을 주지 않았다. sudoers 구문을 검사했다.
- 실제 launcher: `/opt/pj3/product-pipeline/launchers/verify_tick.py` — root 소유. 검증 전용 config와 수동 복구 receipt의 hash를 확인한 뒤 tick을 수행한다.
- 검증 큐: Master의 `.../pipeline-acceptance-0594f7b79ed347c5bf9e449cdb5912af/ssh-queue-v2`, config `ssh-operations-v2.json`, 감사 `ssh-audit-v2`.

최초 launcher 시험에서 S3 환경 주입 누락이 발견되어 실행기를 백업하고 수정했다. 실패 큐는 별도 보존하고 새 v2 큐로 다시 검증했다. 종료한 child의 exit 130 확인 후 token을 명시한 복구로 실제 INGEST가 성공했다. 실패 상태를 임의 편집하거나 RUNNING을 만료만으로 빼앗지 않았다.

NiFi 검증 그룹 ID는 `4e743959-01a1-1000-bc08-5f3d31a5c4bd`, processor ID는 `4e743983-01a1-1000-8d2e-a86dd3ea2b5c`다. 최종 STOPPED이며 운영 일정은 활성화하지 않았다. NiFi 통계 카운터 polling만으로는 실행을 판정하지 못했으나, 별도 Master의 영속 감사 기록이 2개에서 14개로 증가했고 제한 SSH·프로세서 정지를 함께 대조했다. 최초 FAIL 기록과 최종 복합 판정을 모두 보존했다. 타이머는 수동 복구된 검증 큐에 대해 빈 tick을 수행했다. 7단계 업무 FLOW 전체를 예약 실행한 시험은 아니다.

## 최종 코드 배포

서버 검증 당시 수정 코드는 커밋 전 작업 내용으로 묶어 두 서버에 새 venv로 배포했다. 이후 Git 전달 커밋과 아래 서버 release ID는 구분한다.

- release ID: `128dbd37d879b397df15b9d9b21b0e2b438a934fec4614d1a455120d062cffaa`.
- archive SHA256: `b545ea8674aa053c33492dc42cefd92deea041e979b7e8bd2f62613f82d7ebf0`.
- 당시 소스 200파일, manifest/개별 파일 checksum 검증. 이후 작성한 이 최종 문서와 일부 증거 파일은 그 묶음 이후의 문서 변경이다.
- 공통 root 소스: `/opt/pj3/product-pipeline/releases/<release_id>`.
- NiFi Python: `/var/lib/pj3/product-pipeline/nifi/venvs/128dbd37d879b397-base/bin/python`.
- Master Python: `/var/lib/pj3/product-pipeline/spark/venvs/128dbd37d879b397-spark/bin/python`.
- import/CLI/pip check 통과. 원격 tick도 새 release/venv에 연결하고 호출을 다시 확인했다. 이전 release/venv와 실행기는 보존했다.

운영 승인이 나면 검증 전용 launcher/queue/config를 운영 것으로 그대로 간주하지 말고 승인된 업무 입력·서비스 DDL·고정 저장 경로·대상 fingerprint를 적용한다. 테스트용 INGEST/FLOW를 운영 큐에 자동 등록하지 않는다.

## 보존한 검증 데이터

실제 서비스 인수 시험의 계약대로 MongoDB의 검증 원천 3건·등록/이벤트 원장과 S3 표본·Delta/checkpoint는 증거로 보존했다. MongoDB는 승인된 pj3_products_raw 안의 검증 environment/topic fingerprint를 사용한다. 같은 관측을 다른 운영 fingerprint로 바로 재적재하면 충돌 차단이 정상 동작한다. 운영 전에는 이 검증 원장을 별도 보관·정리하거나 동일 논리 계약을 인수하는 절차를 정해야 하며 fingerprint를 임의로 수정하지 않는다. 당시 실제 운영 상품 게시·65,498건 전체 적재는 시작하지 않았다. MySQL 시험 스키마와 Worker 전용 Delta prefix만 자체 정리했다.

## 실제 업무 인수 전에 필요한 정보

현재 가능한 위 소량 기술 검증은 수행했다. 다음 입력 없이 운영 전체 완료로 체크하지 않는다.

1. 실제 서비스 ORM/DDL, 게시 DB명·TLS CA·게시용 보호 접속 설정, 기존 UUID/관측 baseline, 담당 publication guard.
2. 카테고리 매핑·이용/이미지 검수·링크 판정의 확정 버전과 NORMALIZATION/GOLD_BUILD 등록 export. 현재 없는 승인을 대신 생성하지 않는다.
3. 위 입력을 사용한 운영 전체 FLOW·NiFi 예약 인수. 당시 운영 Kafka product.raw.collected는 만들지 않았고 격리 RF3/minISR2 토픽만 시험했다. 운영 토픽/ACL/보관 정책과 논리 환경을 확정해야 한다.
4. 실제 환경 이전을 시험할 목적지·외부 DB/객체/Delta 백업·복원 증거, 공유 노드 장애 시험을 위한 담당 인수. 이번에는 다른 팀 서비스를 중지하거나 AWS 자원을 만들지 않았다. 파일 이관 43개 시험과 스냅샷 VM 간 전달을 외부 DB·Kafka·Delta 전체 복원으로 확대하지 않는다.

대용량 장기 적체·전체 71카테고리/65,498건 처리량, 실제 수집기와 동시 인계, 물리 노드 중지·선거·디스크 내구성은 이번 최소 소량 검증에 포함하지 않았다. 다른 담당자의 입력→해석→임베딩→추천→UI 통합도 범위 밖이다.

## 재현 근거

- Bastion: `/home/team3ops/pipeline-acceptance-0594f7b79ed347c5bf9e449cdb5912af/`의 playbook·공개 결과·비밀 없는 검증 코드. 비밀 param 작업은 no_log이며 서버의 private log는 공개하지 않는다.
- [서비스 인수 본문](../tests/smoke_live_services.py), [등록 이력 회귀](../tests/test_runner.py), [NiFi/큐 시험](../tests/test_operations.py).
- [소량 실제 서비스 실행기](server_setup/pipeline_live_acceptance.py), [Silver/Gold 기술 시험](server_setup/pipeline_silver_gold_technical.py), [VM 전송 시험](server_setup/pipeline_transport_verify.py).
- [Worker Scala](server_setup/worker_delta_verify.scala), [Worker 실행기](server_setup/pipeline_worker_delta_verify.py), [문서 MySQL 시험](server_setup/mysql_reference_verify.py).
- [SSH 구성·검증 기록 코드](server_setup/pipeline_ssh_trigger_verify.py), [NiFi 원격 시험](server_setup/pipeline_nifi_remote_verify.py). 일부 검증 코드에는 이번 run의 경로가 고정돼 있다. 일반 운영 설치기가 아니며 재실행할 때 새 경로/정확한 인터페이스와 결과 판정을 확인한다.


## 실행 환경 준비·복제 확인 근거

최종 배포 전 준비 단계에서 MongoDB 9.0.4 rs-products의 Primary 1/Secondary 2, majority+journal ACK와 세 멤버 majority 읽기, 실제 MongoLease 획득/갱신/중복 writer 차단을 확인했다. 전용 시험 컬렉션과 소유 lease만 정리했다. 노드 중지·선거·디스크 장애 시험은 수행하지 않았다.

- [Replica Set 조회](verification/runtime_20261018/mongo-replica.json), [계정 역할/인증](verification/runtime_20261018/mongo-accounts.json), [실제 복제·잠금 결과](verification/runtime_20261018/mongo-client-verification.json).
- [NiFi 초기 배포](verification/runtime_20261018/nifi-deploy.json), [Master 초기 배포](verification/runtime_20261018/spark-deploy.json), [NiFi 접속](verification/runtime_20261018/nifi-connections.json), [Master 접속](verification/runtime_20261018/spark-connections.json), [운영 토픽 최초 조회](verification/runtime_20261018/kafka-topic-audit.json). 이 준비 단계 결과의 ce456c2f... release는 위 최종 128dbd37...보다 이전이다.

현재 계정·보호 파일 위치는 [배포 가이드](deployment.md#현재-vm의-보호-설정), Garage 조건부 쓰기 실패와 프로필 보완의 성공/실패 근거는 [Garage 프로필](garage_v241_profile.md#인프라-가용성과-검증-근거)로 모았다. 검증 코드와 공개 JSON은 삭제하지 않았다.

</details>
