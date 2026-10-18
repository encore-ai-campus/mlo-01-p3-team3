# 상태를 보존하는 환경 이관

2026-10-18. `operations/migration.py`는 파이프라인 파일 상태를 바이트 그대로 포장·검증·복원한다. MongoDB/MySQL 백업 자체를 구현한 도구가 아니며 외부 시스템의 일관된 백업·복원 증거를 필수 입력으로 받는다. 원본 삭제·lease 초기화·ID 재발급·자동 기동은 하지 않는다.

## 이관 범위와 전제

1. 수집 인계 producer, queue worker, 적재, Spark Driver, MySQL 게시를 정지하고 기존 프로세스가 다시 쓰지 못하게 한다. DB/Kafka/객체/Delta 백업 범위를 고정한다.
2. Mongo의 원천·접수·고정 이벤트·운영 원장, MySQL UUID·FK·staging/receipt·담당 검수, 객체 key/checksum, Kafka event/topic/offset, Delta 데이터와 `_delta_log`·checkpoint의 일관된 보존을 담당 방식으로 수행한다.
3. 백업 ID와 ID/상태 대조 digest, 정지 증거를 `migration_attestation.example.json` 형식으로 제공한다. 실제 구성에 사용한 서비스를 빠뜨리지 않는다. 예시 placeholder는 통과하지 않는다.
4. 파일 roots에는 pipeline 소유 spool/snapshot/audit/queue/local checkpoint를 넣는다. 수집기 원본 checkpoint·writer.lock을 이관하려는 명령은 거절한다. 데이터에 포함된 Mongo raw ID·이벤트 ID·서비스 UUID를 수정하지 않는다.

## 포장·검증·복원

roots JSON 예시:

```json
{
  "queue": "/srv/pipeline/operations/queue",
  "audit": "/srv/pipeline/operations/audit",
  "ingest": "/srv/pipeline/ingest-state",
  "spool": "/srv/pipeline/spool"
}
```

```bash
python run_operations.py migration-pack --roots /secure/migration-roots.json \
  --attestation /secure/quiesced-backup.json --output /srv/migration/bundle-001
python run_operations.py migration-verify --bundle /srv/migration/bundle-001

# 외부 데이터 저장소를 먼저 복원·ID/hash 대조한다.
# 같은 backup_id/identity_digest와 logical_target을 갖고 status를
# RESTORED_AND_VERIFIED로 한 목적지 증거를 제공한다.
python run_operations.py migration-restore --bundle /srv/migration/bundle-001 \
  --attestation /secure/restored-verified.json --output /srv/pipeline/restored-001
```

포장은 임시 디렉터리에서 파일별 hash·크기와 변경 여부를 검사하고 마지막에 manifest를 확정한다. 복원도 모든 hash를 확인한 후 최종 디렉터리를 공개한다. 같은 bundle 재복원은 검증 후 재사용하며 다른 상태를 덮어쓰지 않는다. RUNNING/lease_owner가 남았거나 실제 파일 worker lock을 획득할 수 없으면 중단한다. 만료 lease를 삭제해 성공으로 만들지 않는다.

가상환경·Git·캐시는 복사하지 않는다. 인증서 개인키·클러스터 키·관리자 비밀·.env는 별도 비밀 관리 경로로 이관하며 일반 bundle에 넣지 않는다. Windows 파일/디렉터리 fsync의 전원 장애 내구성은 Linux와 같다고 보장하지 않는다. 생성 manifest와 원천을 보존해 재검증한다.

## 경로·환경 설정

- 논리 target, DB명, bucket/object key, 토픽, 원천/이벤트 ID, 서비스 UUID, Delta 논리 경로와 버전은 보존한다. 실제 host·인증·endpoint는 배포 설정/환경변수로 바꾼다.
- queue spec의 로컬 파일 참조는 `operations.local.json.path_bindings`로 읽을 때만 해석한다. 예: `{"C:/pipeline-state": "/srv/pipeline/restored-001"}`. 불변 spec·등록 문서·이벤트 본문 자체를 치환하지 않는다.
- 이 매핑은 **task 파일 참조 해석**이다. 임의의 Spark/Delta checkpoint 안에 든 Windows file URI나 Kafka 클러스터 ID를 자동 변환하는 도구가 아니다. 운영 Delta와 checkpoint는 이관 전부터 보존 가능한 공용 논리 저장소 경로를 사용한다.
- Windows 로컬 Spark 시험 폴더를 복사해 Linux 운영 스트림 재개가 된다고 주장하지 않는다. 필요한 경우 승인된 원천/고정 export에서 새 처리 checkpoint로 재구축하고 업무 ID로 중복을 대조하는 별도 전환 계획이 필요하다. Kafka 클러스터가 바뀌면 consumer offset을 무조건 그대로 신뢰하지 않는다.

## 목적지 인수

1. Linux/AWS 대상에서 새 가상환경·동일 의존성·인증서·권한을 준비한다.
2. 외부 저장소 ID·hash·버전/offset과 복원 bundle의 hash를 대조한다. 누락된 snapshot/receipt를 건너뛰지 않는다.
3. endpoint만 바뀐 동일 논리 설정으로 preflight를 통과시킨다. 논리 fingerprint가 달라졌으면 기존 state로 억지 실행하지 않는다.
4. queue의 RUNNING·공용 Mongo lease·Spark writer가 없는지 확인한다. 필요한 수동 복구는 토큰과 정지 증거를 기록한다.
5. 명시적인 소량 수동 재개에서 성공한 원천/이벤트/객체/SQL UUID가 재생성되지 않는지 확인한 뒤 NiFi를 활성화한다.

서버 백업·복원 자체, EC2 IAM/네트워크, 다른 Kafka 클러스터로의 offset 전환, 실제 장애 복원은 현 로컬 대역 시험으로 완료 처리하지 않는다. 복원 결과는 `RESTORED_REQUIRES_DEPLOYMENT_PREFLIGHT`이며 자동 시작하지 않는다.
