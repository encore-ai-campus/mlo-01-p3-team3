# LIVE 인계·복구 실행

2026-10-16. `jsonl-v1` / 수집 schema 1.2 전용이다. 수집기의 요청·파서·잠금·파일은 변경하지 않는다. 이 문서의 명령은 구현되어 있지만, 실제 VM/NiFi/S3 종단 인수는 아직 수행하지 않았다.

## 실행 위치와 상태

수집 PC에서 `capture`, 저장소/DB에 접근하는 PC 또는 Python 서버에서 `drain`을 실행한다. source는 읽기 전용, spool과 state는 파이프라인 소유의 서로 다른 디렉터리다. 이미지를 상품 상세 직후 다운로드하도록 수집기를 변경한 것은 아니다. 이미지 단계의 manifest가 생긴 뒤 추가 인계한다.

| 저장물 | 의미 |
|---|---|
| 수집 `checkpoint_state.json.commit` | source 저장 확정 상한 |
| spool `cursor.json` | 불변 배치의 보관 완료 위치. DB/Kafka 성공 아님 |
| spool `batches/<snapshot_id>/snapshot.json` | SOURCE_METADATA 또는 GALLERY_ACQUISITION 확정 배치 |
| state `receipts/<snapshot_id>.json` | 해당 대상에 원천·이벤트 전달 완료. 이미지/추천 승인 아님 |
| state `audit/` | 기존 상품별 접수·시도·격리·실패·실행 보고서 |

다음은 Linux 예시이며 Windows에서도 경로만 바꿔 같은 Python CLI를 사용한다. 아래 실행 파일은 `src/data_pipeline`에서 실행한다. 인증·endpoint는 기존 배포 환경변수로 주입한다.

```bash
# 8 GiB spool 한도와 2 GiB 최소 여유 공간은 운영자가 확정할 예시값이다.
python run_pipeline.py capture \
  --source /srv/collector/run-id --spool /srv/pipeline/spool/run-id \
  --max-spool-bytes 8589934592 --min-free-bytes 2147483648 \
  --max-records 100 \
  --media-manifest /srv/collector/run-id/media/products_with_local_images.jsonl \
  --watch --poll-seconds 2

# 다른 터미널/예약 실행. 한 번의 확정 capture 범위만 처리하고 끝난다.
python run_pipeline.py drain --spool /srv/pipeline/spool/run-id \
  --state-dir /srv/pipeline/state/run-id --config config/environment.local.json

# 실제 연결 없는 파일 대역 시험은 별도 state로 실행한다.
python run_pipeline.py drain --spool /srv/pipeline/spool/run-id \
  --state-dir /srv/pipeline/rehearsal/run-id --rehearse
```

`--watch`가 없으면 캡처 한 번만 실행한다. 최대 100개 변경 또는 제어 이벤트, 기본 32 MiB 목표 단위다. 원래 이벤트 해시를 검증할 전체 envelope는 32 MiB까지 허용한다. 한 envelope 자체가 목표보다 크면 다른 이벤트와 섞지 않고 단독 처리하며, record 단위로 나눠도 원래 해시 증거를 보존한다. 따라서 32 MiB는 최종 디렉터리의 절대 상한이 아니다. spool 총량·최소 여유 공간은 별도로 제한한다. 이미지 20 MiB 상한과 원천/이력 한도 초과는 자르거나 성공 처리하지 않는다.

원본 이력·개별 상품 JSON·이미지에 HTTP 요청을 넣지 않는다. C 이하 event의 raw/normalized 쌍, seq·collector 직렬화 해시·run 정체성을 검증한다. 동일 상품의 다른 관측을 최신 하나로 압축하지 않는다. 같은 관측의 반복 pointer는 전달 증거에 보존한다.

프로세스 재시작 시 봉인 배치 chain·cursor·이미지·journal prefix를 검증한다. `--watch` 실행 중에는 수집 계약의 append-only 이력과 파이프라인 소유 불변 spool을 전제로 검증 결과를 캐시한다. 실행 중 과거 prefix의 외부 덮어쓰기를 매 poll마다 검출한다고 보장하지 않으며, 재시작 검증에서 검출한다. 원본 journal 교체·잘림·run 변경은 재개 실패다. source 보관 담당은 미접수 이력의 삭제·rotation을 금지해야 하며, 이를 보장할 수 없는 source에는 LIVE를 활성화하지 않는다.

## 이미지 후속 인계

아직 manifest가 없으면 `WAIT_MEDIA_MANIFEST`로 기다린다. 생긴 manifest는 복사한 바이트를 동결하고, 상품·실행·payload hash·비다운로드 가공 필드·갤러리 의미를 대조한다. 원천 wrapper 그룹이 하나인 경우만 `HASH_AND_GALLERY_EQUIVALENCE`로 연결하고 모든 대응 source pointer·원래 envelope 증거를 남긴다. 서로 다른 그룹이면 `LINK_AMBIGUOUS`, 아직 대응 원천이 없으면 `WAIT_SOURCE_MATCH`, 원천 종료 후에도 없으면 `MEDIA_SOURCE_MISMATCH`다. 최신 seq를 임의 선택하지 않는다.

이전 페이지의 미연결 후보도 source commit 또는 종료 상태 변경 시 다시 대조한다. 이미지 확보 바이트가 늘면 새로운 관측이 되며, 경로·다운로드 오류·업로드 재시도만 바뀌면 같은 관측 키를 재사용한다. 이미지 개수나 hash 크기로 최신성을 판단하지 않는다. subscription·generation·전임 snapshot ID가 인계 순서를 보존한다. 원래 media 생성 commit을 정확히 복원했다는 의미는 아니다.

원천 종료와 이미지 다운로드 완료는 구분한다. 이미지 final/partial 결과는 수집기의 `media/download_report.json`을 별도로 확인한다. 현재 capture/drain의 DRAINED는 확정 배치의 전달 범위에 대한 결과이며 이미지 단계 전체 종료나 서비스 게시를 뜻하지 않는다.

## 중단·적체·복구

- 같은 source/spool로 다시 capture한다. seal 후 cursor 저장 전에 죽어도 같은 배치를 재사용한다. cursor만 다른 폴더로 옮기지 않는다.
- 같은 spool/state/config로 drain한다. 성공 receipt는 건너뛰고 미완료 배치를 재시도한다. 상품 적재는 확인된 이미지 슬롯·원천·이벤트를 건너뛴다.
- `BACKPRESSURE`에서는 cursor를 전진시키거나 source를 중단/삭제하지 않는다. 임시 작업을 포함해 quota를 확인한다. 공간·보관 정책을 운영자가 해결한 뒤 같은 명령으로 재개한다. 자동 삭제는 없다.
- 확인된 원천의 외부 유실이 의심될 때만 기존 `ingest --repair-raw`를 사용한다. `--replay-events`는 고정 이벤트를 명시 재발행하는 별도 옵션이다. 정상 재개는 외부 삭제를 매번 검사하지 않는다.
- source/spool 둘 다 사라진 경우 복구 완료로 처리하지 않는다. `RECOVERY_EVIDENCE_MISSING`, cursor/chain 충돌은 원래 보관 증거를 복원해야 한다.

종료 코드는 완료/IDLE=0, 실패·backpressure=2, 사용자 중단=130이다. `drain_report.json`의 이미지 미확보 수와 audit 실패를 함께 확인한다. source-only 배치도 원문·이벤트는 전달할 수 있지만 이미지는 미확보로 유지한다.

## 서로 다른 PC/VM으로 전달

설정에 전용 `handoff_bucket`을 지정한다. 이미지 버킷이나 Delta 경로를 인계 용도로 추정하지 않는다. 양측에서 같은 인증 정책의 S3 호환 저장소에 접근해야 한다.

```bash
python run_pipeline.py send --snapshot /srv/pipeline/spool/run-id/batches/SNAPSHOT_ID \
  --config config/environment.local.json
# 출력의 ready_key를 수신측에 전달한다.
python run_pipeline.py receive --ready-key handoff/ready/TRANSPORT_ID.ready.json \
  --output /srv/pipeline/received/batch-001 --max-total-bytes 10737418240 \
  --config config/environment.local.json
python run_pipeline.py ingest --snapshot /srv/pipeline/received/batch-001 \
  --state-dir /srv/pipeline/state/received-001 --config config/environment.local.json
```

8 MiB 내용 주소 chunk를 먼저 업로드·실제 SHA256 대조하고 ready manifest를 마지막에 게시한다. 중간 실패에는 ready가 없다. 응답 유실 후에는 같은 키·바이트로 재시도한다. 수신자는 모든 chunk·파일·snapshot hash와 상대 경로를 검증한 뒤 최종 디렉터리를 노출한다. ready/chunk 자동 발견 scheduler는 이 CLI에 포함하지 않았으며 NiFi가 ready_key를 전달한다. source cursor를 원격 적재 완료로 갱신하지 않는다.

FULL_EXPORT와 LIVE의 raw wrapper·갤러리 확보 투영이 정확히 같으면 기존 observation/raw/event ID로 수렴한다. endpoint/물리 경로만 이전할 때 snapshot, state/audit/receipts, Mongo 운영 원장·원천, 객체와 Delta/checkpoint를 함께 이전하고 논리 환경·DB·버킷·토픽 정체성을 유지해야 한다. `environment_id` 등을 바꿔 다른 환경으로 기존 state를 강제 재사용하는 기능은 없다. VM/AWS 실제 이전 시험은 미수행이다.

## NiFi 인수

먼저 [실제 서비스 인수 시험](acceptance.md)을 통과시킨다. 이후 NiFi의 ExecuteProcess 계열 실행에서 위 `capture` 단발 명령과 `drain`을 서로 다른 작업으로 등록한다. 장기 `capture --watch`를 외부 서비스로 운영한다면 같은 source에 단발 capture 예약을 동시에 켜지 않는다. concurrent tasks=1, 이전 실행 종료 전 재실행 금지, 작업 디렉터리·가상환경 Python 절대 경로·환경변수·stdout/stderr 보관을 설정한다. exit2는 운영 재시도/오류 경로, exit130은 중단으로 분리한다. OS lock과 Mongo lease는 안전장치이며 NiFi 일정 중복을 정상 운영으로 간주하지 않는다. 실제 NiFi flow 설치·예약 활성화는 서버 인수 후 수행한다.
