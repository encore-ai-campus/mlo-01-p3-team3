# 파이프라인 제작 현황

2026-10-18 갱신. 기준 브랜치는 `feature/#13`, 현재 원격 반영 커밋은 `f85e59e`다. 구현 커밋 `460f6df` 이후 `origin/dev`의 `22168b8`을 병합하고 푸시했다. 이 문서 갱신 자체의 커밋·푸시를 의미하지 않는다. 수집 중인 원본 작업 폴더와 크롤러는 변경하지 않았고, 별도 MongoDB 설치 스크립트는 로컬에 보존했다.

## 완료 판단

**주요 처리 단계 구현·로컬 검증 완료, 전체 자동화·실제 서버 운영 인수 대기**다. 단계별 코드 존재, 문서 계약 검증, 업무 승인, 실제 종단 운영 완료를 구분한다. 이슈 종료 여부는 담당자가 [이슈 대조표](issue13_acceptance.md)로 확인한다.

| 영역 | 현재 구현·검증 | 남은 연결·인수 |
|---|---|---|
| 입력·FULL_EXPORT | 원천/정규화/media 대응, checksum·안전 경로, 스트리밍 분할·불변 snapshot·검증 전용 모드 | 전체 수집량 성능과 대상 파일시스템 내구성 |
| 이미지·Mongo·Kafka | 내용 주소 이미지, 불변 원천·접수 원장·lease, majority/journal·고정 이벤트·재실행 복구 | 실제 Replica Set/S3/Kafka 종단·장애 시험 |
| LIVE | 확정 journal 쌍·seq/hash·cursor, 불변 배치·quota·미디어 후속·원격 ready-last·수신 | 실제 동시 수집·PC↔VM 전송·장기 적체 |
| Bronze | 원천 ID 멱등 Delta·receipt/격리·별도 checkpoint·공용 Driver 잠금 | 실제 Kafka/Mongo/lake·다중 호스트 인수 |
| Silver | 8개 Delta의 정규화/품질/평가, 고정 입력·재평가·복구 | 실제 업무 snapshot·NORMALIZATION 등록 export 인수 |
| Gold | 8개 Delta의 현재값/Offer/이력/이미지/링크/집계/평가, 고정 export | 실제 업무 입력·GOLD_BUILD 등록 export 인수 |
| MySQL 게시 | 문서 계약 preflight, 고정 계획·staging·UUID/검수/시각 보호·원자 게시·응답 유실 복구 | 실제 서비스 DDL·기존 상품 baseline·담당 guard·권한/TLS/Primary |
| 예약 재시도 | 단일 소유 파일 queue, due·Retry-After·한도·격리·token 수동 복구 | 지정 실행 호스트·보관/로그 운영 인수 |
| 자동 발견·대조 | 원격 ready/게시 계획 ready 발견, 지정 snapshot과 Mongo Raw·접수/발행 상태 대조·repair | Bronze/Silver/Gold/MySQL까지의 전체 종단 대조 |
| NiFi | REST 버전/bundle 대조·계획·STOPPED 생성·명시 시작/정지, REST 대역 시험 | 실제 버전/권한·수동 복구 확인 후 일정 활성화 |
| 상태 이관·전달 | 정지/외부 백업 증거, 파일 hash/ID 보존 pack/verify/restore·task 경로 별칭, source release | 외부 DB/객체/Kafka/Delta 백업·복원·Linux/AWS 실제 재개 |

## 자동화 지원 경계

현재 queue의 허용 작업은 `RECEIVE`, `INGEST`, `PUBLISH`, `RECONCILE`이다. Bronze/Silver/Gold 실행과 고정 게시 계획 생성을 자동으로 이어주는 전체 일정·의존성·실패 재개 연결은 아직 없다. 해당 CLI를 수동 실행할 수 있다는 사실로 전체 자동화 완료를 표시하지 않는다. Silver/Gold 등록은 담당 export를 검증·보관하며 서비스 `pipeline_runs` 직접 생성/상태 갱신 어댑터는 없다. export를 공식 운영 입력으로 인수할지 직접 어댑터를 추가할지 확정해야 한다.

queue는 지정 호스트 한 대 소유이며 분산 scheduler가 아니다. 상태 이관 도구는 외부 저장소 백업 자체나 Windows Spark checkpoint URI/다른 Kafka cluster offset 자동 변환을 수행하지 않는다. 이 지원 한계를 모두 필수 개발 누락으로 취급하지 않고 실제 배포 요구와 구분한다.

## 검증 근거

- 최신 dev 병합 후 전체 회귀 **192개 / 74.540초 / OK**: `.runtime/post-dev-merge-regression-20261018.txt`.
- 실제 격리 MySQL **8.0.46**: UUID 재사용·롤백·COMMIT 응답 유실·guard 변경·과거 값 차단. 기존 Windows 서비스/VM 데이터 미접속.
- 실제 로컬 Spark **4.0.1/Delta**: 고정 Gold export→게시 계획, 후속 Delta 행 제외. 실제 서버 클러스터 시험과 구분한다.
- 코드 전달 묶음 140파일 checksum 확인, 묶음 내 게시 5개·운영 25개 테스트 재통과.
- 세부 결과·경계는 [2026-10-18 검증 기록](validation_20261018.md). 실제 NiFi는 REST 대역, 파일 이관은 로컬 시험이며 VM/AWS 외부 저장소 복원을 대신하지 않는다.

이전 기록: 2026-10-16 이슈 보완 시험 122개, 2026-10-17 Gold 시험 162개는 당시 결과다. 저장 실제 journal 5건·반복 drain, Bronze 실제 로컬 Delta 3원천/3receipt/2격리, Silver/Gold 표본 3상품·9이미지 관계를 확인했다. Gold 실제 표본은 QUALITY_WAIT·현재 Offer 0이며 확인 가격/완전 준비 경로는 별도 fixture다. 이를 실제 데이터 확보율·추천 승인으로 확대하지 않는다.

## 배포·책임 경계

[배포](deployment.md) → [MySQL 게시](mysql_publication.md) → [운영](operations_execution.md) → [이관](state_migration.md)을 따른다. 실제 DDL/매핑/이용 승인/링크·임베딩 결과가 없으면 품질 대기 또는 실행 차단한다. 승인 원장 편집·벡터 생성·추천/API·인프라 설치는 파이프라인 범위 밖이다. 새 이미지 연결은 inactive로 게시한다.

기준 문서는 최신 dev에서 병합한 `docs/design/`이며 이번 문서 정리로 상위 계약을 수정하지 않는다. 이전 작업 폴더에서 검토한 20대와 dev 분리 당시 19대의 차이는 과거 환경 기록이다. 실제 역할/endpoint·버전은 배포 담당 인수가 필요하고 코드에 VM IP를 하드코딩하지 않는다. SSH hostname 조회는 데이터 서비스 인수가 아니다. 크롤러는 import하지 않고 완료 export/LIVE 파일 계약으로 연결한다.
