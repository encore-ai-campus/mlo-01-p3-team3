# Kafka 보관 만료 복구

## 이번 장애와 적용 정책

2026-10-21 `product.raw.collected`의 보관 기간은 24시간, 파티션별 보관 용량은 2GiB였다. 브로커 로그에서 시간 초과에 따른 segment 삭제와 최초 offset이 81,882로 이동한 것을 확인했다. 당시 Kafka의 earliest/latest는 모두 81,882였고, Spark 원래 체크포인트의 마지막 commit은 batch 3 / 다음 offset 310이었다. offsets/4의 410은 미완료 배치의 끝이므로 재개 기준으로 쓰지 않는다.

시험 실행은 재시도 3회 뒤 185,778ms에 실패했다. 이 시간은 정상 배치 속도가 아니다. MySQL은 당시 상품 209 / ACTIVE 195 / PENDING_REVIEW 14였고 해당 실패로 증가하지 않았다.

이 토픽의 `retention.ms`를 **1,209,600,000ms(14일)**로 변경하고 재조회했다. 파이프라인 계정에는 ALTER_CONFIGS 권한이 없어 기존 Kafka 관리용 TLS 설정으로 해당 토픽만 변경했다. 계정 권한·다른 토픽·복제 설정은 바꾸지 않았다. 2GiB 제한은 유지하므로 기간만 늘렸다고 무한 보존이 보장되지는 않는다. 향후 인프라 설정 재배포 때 토픽 override를 유지한다.

실제 관리 서버에서 사용한 형태는 다음과 같다. 기존 관리 자격 파일을 사용하며 키·암호를 명령행에 넣지 않는다.

```bash
"$KAFKA_HOME/bin/kafka-configs.sh" \
  --bootstrap-server 192.168.0.127:9092 \
  --command-config /etc/pj3/kafka/client.properties \
  --entity-type topics --entity-name product.raw.collected \
  --alter --add-config retention.ms=1209600000
# 위 --alter --add-config 대신 --describe --all로 적용값 확인
```

## 원천·이벤트 보존과 재발행

MongoDB `ingestion_observations`에서 해당 수집 실행의 RAW_CONFIRMED / EVENT_CONFIRMED 이벤트 **81,882개**를 고정했다. 65,498개 상품 수와는 다르며 이미지 확보 버전 등의 추가 관측을 포함한다. Mongo 원장 확인값을 초기화하지 않는다.

보호 디렉터리 `kafka-recovery-20261021/`에는 다음이 있다.

| 파일 | 의미 |
|---|---|
| `events.jsonl` | 기존 이벤트 ID·본문·메시지 키를 그대로 보존한 재발행 목록 |
| `manifest.json` | 81,882건 / 66,675,036바이트와 전체 파일 SHA256·manifest hash |
| `original-bronze-config.json` | 기존 논리 대상과 체크포인트 설정 |
| `replay-progress.json` | 응답 확인된 건수와 파일의 byte cursor |
| `delivery-receipts/` | 이후 묶음별 전송 확인·미확인 기록. 초기 1,000건은 기존 progress 형식으로 확인 |
| `progress.json` | 현재 준비·재발행 진행 상태 |

목록 SHA256은 `eb8a6a787ae249ea9bac4f4f6a9b5d7665295c128bc284217f9301862c0ae7d9`, manifest hash는 `d5cfd1525030836dbdc6f698e37a62ba143831817f0506a53256b1ae64d83266`다.

재발행은 최대 100건씩 원문을 majority로 읽어 전체 document hash·payload hash·관측/이미지 계약을 검증한 뒤 수행한다. 이벤트 ID·occurred_at·본문·키를 바꾸지 않는다. Kafka 레코드의 전송 위치는 새 offset이 되며 본문의 원래 관측 시각과 구분한다. 부분 응답·응답 유실은 성공 cursor를 전진시키지 않는다. 확인 영수증 저장 후 cursor 저장 전에 종료되면 영수증을 대조해 발행 요청 없이 재개한다. 미확인 전송을 다시 보내 중복이 발생할 수 있으며 Bronze는 기존 이벤트 ID와 원문 hash로 멱등 처리한다.

## 체크포인트 세대와 대상 정체성

원래 `.../checkpoints/bronze`를 삭제하거나 수정하지 않는다. 복구 설정은 배포 root의 `bronze-recovery.json`에 봉인한다. 실제 새 경로는 다음과 같다.

```text
s3a://pj3-data/lake/pj3-danawa/checkpoints/bronze-recovery/7e9a2a3fc3d3416f8e29f4ed300ed199
```

`BronzeSettings.checkpoint_path`는 원래의 논리 대상 경로이며 fingerprint가 유지된다. 명시적 `checkpoint_recovery`가 실제 실행 경로만 추가한다. 따라서 기존 Delta 테이블의 target fingerprint·원천 ID·이벤트 ID를 변경하지 않는다. 원래 네 대상과 새 실제 체크포인트 모두 writer lease로 잠근다.

복구 설정은 원래 fingerprint·원래 경로·새 경로·복구 ID·Kafka cluster/topic ID·시작 offset 81,882·고정 재발행 manifest hash를 묶는다. 새로운 체크포인트에는 코드가 lease 안에서 `_pipeline_target`과 `_pipeline_recovery`를 생성한다. 기존 commit/offset 파일을 수동 생성하거나 완료 상태를 복원하지 않는다. 기존 체크포인트를 사용할 때는 변경 전 fingerprint와 경로를 그대로 대조한다. 새 경로만 임의로 바꾸거나 다른 topic ID를 가리키면 차단한다.

## 자동 공급·중단·재개

연속 처리 coordinator는 epoch 사이에 `recover_kafka_retention.topup`을 호출한다. 실제 Kafka 끝 위치와 **Spark가 commit한** 체크포인트의 다음 읽기 위치 차이를 확인해 미확정 메시지가 최대 1,000건인 범위에서만 보충한다. 미완료 offsets 파일은 소비 완료 증거로 사용하지 않는다. 고정 목록에 남은 이벤트가 있으면 Mongo 발행 확인 건수가 바뀌지 않아도 전체 완료로 오판하지 않는다. 전체 데이터가 Kafka에 다시 쌓인 채 장기간 방치되는 것을 줄이기 위한 backpressure다.

미완료 목록과 cursor는 디스크에 남는다. 파이프라인 재개 시 이전 전송 결과를 재사용하고 남은 범위만 공급한다. Kafka earliest가 commit 위치를 다시 넘어가면 공급·처리를 중단하고 새 복구 판단을 요구한다. 보관 기간, backlog 체류 시간, 남은 디스크는 별도로 감시해야 한다.

파이프라인은 소스 시작 전에 topic ID와 파티션별 earliest/latest를 조회한다. Kafka 범위 이탈·파티션 변경·확인 불가 체크포인트는 `RECOVERY_REQUIRED`로 분류하고 같은 설정으로 세 번 재시도하지 않는다. 이 검사에는 Driver JVM과 읽기 연결이 필요하지만 Delta 테이블 생성·저장과 streaming query 시작보다 먼저 수행한다. `failOnDataLoss=true`를 유지한다.

다른 PC에서도 저장 위치는 Spark Master다. 기존 보호 실행 환경과 소스 코드를 포함해 배포해야 하며 현재 컴퓨터의 상주 프로세스에 의존하지 않는다.

```powershell
# 프로젝트 src/data_pipeline에서 기존 원격 제어 도구 사용
python run_server_pipeline.py status
python run_server_pipeline.py flow
```

`pause-after-window.json`이 있으면 후속 한 배치의 게시·활성화 후 정지한다. 해당 파일이 있는 상태를 전체 연속 처리 활성화로 설명하지 않는다. 연속 처리 전환은 운영자가 정지 제어 파일을 확인하고 해제한 뒤 실행한다. 체크포인트·Delta·완료 영수증을 지워서 재개하지 않는다.

서버에서 복구 상태만 읽을 때:

```bash
"$PY" "$ROOT/recover_kafka_retention.py" status \
  --root "$ROOT" --directory "$ROOT/kafka-recovery-20261021" \
  --run-id 0685b031-7662-45c6-bdfe-c7b499051069
```

`freeze`는 최초 고정 목록 준비용이며 일반 재개에서 반복하지 않는다. `replay --max-events 1000`도 queue 상한을 확인해 부족분만 공급한다. CLI 실행에는 기존 보호 Python/MongoDB/Kafka/S3 환경이 필요하다. Kafka 계정의 ALTER_CONFIGS 권한은 일반 데이터 처리에 필요하지 않다.

다른 보관 만료 사고에서 새 세대를 준비할 때는 새 보호 디렉터리에 `freeze`한 뒤 아래 `create`를 사용한다. 기존 설정을 직접 편집하거나 복구 JSON을 수동 작성하지 않는다. `create`는 인증된 Master의 최종 HTTPS 응답·전체 Worker·Driver 종료를 확인하고 동일 manifest의 기존 세대는 그대로 재사용한다. 8080은 HTTPS로 전환하는 진입점이므로 직접 HTTPS 8080으로 바꾸지 않는다. 재발행과 세대 전환은 배포 root의 공통 파일 잠금으로 직렬화한다.

```bash
"$PY" "$ROOT/recover_kafka_retention.py" create \
  --root "$ROOT" --directory "$NEW_RECOVERY_DIR" \
  --run-id 0685b031-7662-45c6-bdfe-c7b499051069 \
  --source-config "$EXISTING_BRONZE_CONFIG"
```

다른 manifest로 이미 설정된 세대를 교체하려면 명시적 `--replace-generation`이 필요하다. 도구가 이전 설정을 이전 manifest 디렉터리에 보존하며 이전 실제 체크포인트와 전송 상태는 삭제하지 않는다. 시작 위치는 현재 Kafka earliest로 고정해 아직 남아 있는 메시지도 보존한다. topic 재생성을 감지하면 현재 실행은 자동 전환하지 않는다.

## 검증 범위

대역 테스트는 원문 변조·전송 미확인·cursor 저장 전 종료·순서가 다른 callback·체크포인트 세대 변경·미완료 offset 배제·topic 교체·queue 상한·원장 건수 불변 상태의 재개를 포함한다. 실제 시험에서는 고정 목록 준비, 최초 1,000건 원문 대조 후 전송, 새 체크포인트의 query 시작을 확인했다. 단계별 최종 결과는 서버 `perf-20261020/recovery-single-batch-result.json`으로 남긴다. 전체 81,882개 재발행 또는 65,498개 상품의 MySQL 완료와 한 배치 검증을 구분한다.

### 2026-10-21 최종 실제 결과

10:09:08.658 KST 시작 → 10:40:25.883 종료. 최초 MySQL 게시까지 23분 6초, 활성화 평가와 정지까지 **31분 17.225초**다. 재시도 없이 Bronze 한 배치·후속 한 window만 완료했다.

| 단계 | 실제 시간 |
|---|---:|
| Bronze, 이벤트 100건 | 852.133초 |
| 후속 25상품/25관측 계획 | 44.778초 |
| Silver | 200.548초 |
| Gold | 195.213초 |
| 최초 MySQL 게시 | 34.492초 |
| 활성화 재평가 전체 | 490.384초 |

전체 경과 시간에는 coordinator 준비와 Spark 세션 시작·정리 시간도 포함된다. 단계 시간의 합과 다르다. 후속 window의 측정 시간은 965.438초다. Bronze는 새 원문/이벤트 21개씩을 저장하고 나머지를 재사용했다. 기존 Bronze 적체 범위를 함께 후속 처리했으므로 신규 원문 수와 선택된 25상품 수를 같은 지표로 합산하지 않는다.

업무 DB는 상품 **209→234**, ACTIVE **195→213**, 대기 **14→21**이다. 이번 25상품 중 18개가 활성화됐고 7개는 `GOLD_CATALOG_QUALITY_WAIT`로 남았다. 해당 7개는 상품 상태 검증이 미완료이며 그중 6개는 카테고리도 미확정이다. 가격·비교 링크 등의 별도 품질 미완료도 기록돼 있다. 이를 Kafka 복구 실패나 임베딩 준비 완료로 표시하지 않는다. 활성화 재평가에서 변하지 않은 10개 관측의 정규화를 재사용했다.

실제 CLI로 기존 세대 재사용을 확인했고, 100건을 추가 공급한 뒤 같은 명령을 다시 실행했을 때 발행 증가 0건을 확인했다. 현재 재발행은 **1,100/81,882**, 미재발행은 80,782건이며 Kafka 미확정 대기는 1,000건이다. 새 commit의 다음 offset은 81,982다. 한 배치 정지 제어는 유지하며 전체 처리 자동 실행을 새로 켜지 않았다. 다음 재개부터 epoch 사이의 자동 보충을 사용한다.

관련 로컬·서버 자동 테스트는 각각 **84개** 통과했다. [공개 검증 결과](verification/kafka_retention_recovery_20261021.json)에 실제 환경 조회와 한 배치 결과를 보존한다. 복구 경로의 동작 확인과 처리 속도 개선을 구분하며, 현재 Bronze의 긴 처리 시간은 남아 있다.

자동 보충 훅은 대역 회귀와 동일 함수의 실제 서버 CLI로 검증했다. 변경 후 coordinator의 장시간 연속 운전·전체 목록 소진은 이번 한 배치 시험에 포함하지 않았다.
