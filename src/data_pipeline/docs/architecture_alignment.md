# 상세 설계 대조와 구현 범위

분리 시점 안내: 이 대조표의 20대 인프라·상위 설계 검토는 이전 작업 폴더의 설계안을 대상으로 했다. feature/#13의 dev 기준 문서와의 차이 및 현 구현 범위는 [제작 현황](status.md)을 따른다.

2026-10-16. 저장소의 실제 문서와 수집기 코드를 읽어 [통합 설계](integration_design.md)·[데이터 계약 v1](data_contract_v1.md)을 대조했다. **설계상 대응 검토**이며 배포·DB·Kafka·서비스 인수 테스트 결과가 아니다. 이 문서는 다나와 상품 데이터 파이프라인의 해당 계약을 검토한다. 사용자 입력 해석·학습·추천 API 등 서비스 전체가 구현됐다는 뜻은 아니다.

현재 20대·pj3-* 인프라 기준으로 이전 검토를 갱신했다. 접수/이벤트·상태 재평가·최신성·키 정책의 세부 보강 기준은 [v1.1 보강 계약](design_amendment_v1_1.md)이며 이 대조표 자체를 정합성의 독립 증거로 사용하지 않는다.

최종 범위는 저장 파일 이후의 스냅샷·적재·전달·ETL·상품 영역 게시·복구다. 타 담당의 크롤링·이용 승인/분류 원장 편집·임베딩·추천·API·운영 화면·인프라 구축은 포함하지 않는다. 파이프라인은 해당 담당의 고정된 결과를 읽고 검증한다. 상세 읽기/쓰기 허용 범위는 계약 v1의 0절을 따른다.

[증분 인계 계약](live_handoff_v1.md)은 이 경계를 유지한 입력 방식 확장이다. source commit까지의 쌍을 파이프라인이 읽기 전용으로 동결하며 raw 원장·majority 확인·Kafka 동일 ID·Bronze 계약은 바꾸지 않는다. 현재 수집기의 이미지 실행 순서도 바꾸지 않는다. 인계 cursor는 적재 receipt·Kafka 완료·서비스 게시 상태를 대체하지 않는다.

판정은 대응=계약을 설계에 명시, 확장=상위 원칙을 유지한 신규 세부 계약, 실행 조건=실제 인프라·DDL·검수 자료를 받아 시험해야 함, 범위 외=담당 서비스의 책임을 보존함으로 구분한다. 확장 필드는 상위 문서에 이미 존재하는 것처럼 설명하지 않으며 구현 시 스키마·마이그레이션에 반영한다.

## 1. 03-data-model.md

근거: [데이터 모델](../../../docs/design/03-data-model.md)의 공통 규칙·UUID·MongoDB 원천·MySQL 상품과 판매, [상품 연결 속성](../../../docs/design/data-dictionary/08-product-links.md).

| 상위 계약 | 이 설계의 대응 | 판정 |
|---|---|---|
| MongoDB 불변 원천·보조 운영 데이터, MySQL 서비스 원장 | raw_products INSERT 전용, ingestion_* 운영 원장, 서비스 jobs·게시·검수는 MySQL | 대응·확장 |
| ObjectId 원천, MySQL UUID4·재처리 PK 보존 | 접수 시 ObjectId 고정, Gold 신규 행만 UUID4, 기존 업무 키 조회 | 대응 |
| UUID FK·문자열 직렬화·DB 간 FK 없음 | MySQL CHAR(32), API/Delta/Kafka UUID는 하이픈 포함 36자리, raw ID는 24자리 문자열 | 대응 |
| UTC·KRW 정수·미제공 NULL·mm+원문 | 값별 원시 관측 시각, 단위·축 검증, UNKNOWN을 숫자 0으로 바꾸지 않음 | 대응 |
| raw 공통 필드·PRODUCT/OFFER 구분 | 기존 wrapper는 PRODUCT, 판매 하위 항목 보존. source_record_key 명시 | 대응 |
| payload_hash와 response_body_hash 분리 | 기존 추출 해시 유지, legacy 응답 바이트 미보존은 null/명시 상태 | 대응: 기존 자료 결측 |
| product_key={source_code}:{source_product_id} | danawa:<id>, 원천 observation key와 별도 | 대응 |
| 안정 판매·옵션 키·판매 이력 | 확인한 tuple만 offer_key 생성, 불명확 판매 ID는 게시 보류 | 대응·확장 |
| assets 공유·product_images 원천별 버전·검수 | checksum 객체 공유, 자산/상품/이미지 UUID 발급 순서·슬롯·source_version·용도별 검수 | 대응·확장 |
| 서비스 DDL·FK·고유 제약 | 실제 DDL preflight, 불일치 시 서비스 쓰기 차단 | 실행 조건 |

collector_metadata·lineage·해시/관측 키와 ingestion_*는 이번 설계에서 새로 정의한 확장이다. 이를 공통 설계에 이미 구현된 필드·컬렉션으로 주장하지 않는다. 기존 raw에서 response_body_hash를 복원할 수 없다는 사실은 보존하고 향후 바이트 보존은 별도 수집기 변경으로 다룬다.

## 2. 05-kafka-jobs.md

근거: [Kafka 계약](../../../docs/design/05-kafka-jobs.md)의 메시지·발행과 복구·작업 상태·offset·상품 원천 이벤트·클러스터.

| 상위 계약 | 이 설계의 대응 | 판정 |
|---|---|---|
| DB 커밋/majority·journal 확인 후 직접 발행 | RAW_CONFIRMED 이전 발행 금지 | 대응 |
| product.raw.collected 최소 필드 | 원천 ID·source·type·record key·run·관측 시각·payload hash·schema version | 대응 |
| 같은 event_id·입력으로 복구 | event ID·발생 시각·본문을 접수 원장에 고정, 재발행 때 유지 | 대응 |
| DB·Kafka 분산 트랜잭션 아님 | 미확인·성공 후 응답 유실·발행 전 종료를 별도 처리 | 대응 |
| 업무 근거로 누락 발행 재구성 | ingestion 원장·확정 snapshot·raw·Bronze 대조, 복구 근거 없으면 중단 | 대응·확장 |
| 조건부 lease·낡은 워커 쓰기 방지 | 원자 lease·fencing token, 외부 호출 중 DB 트랜잭션 미유지 | 대응 |
| 최초 포함 3회 후보·Retry-After·비동기 대기 | 초기 기본 3회·next_retry_at·RETRY_WAIT, 긴 sleep으로 Consumer poll 막지 않음 | 대응 |
| 결과/복구 가능한 상태 저장 후 offset 확정 | Bronze Delta commit 확인 뒤 연속 완료 구간만 commit, 재할당 시 미완료 재전달 | 대응 |
| Broker 3대·RF3·minISR2·acks all 후보 | 시험 토픽/클러스터 preflight·Producer 확인 | 실행 조건 |
| body/이미지/인증정보를 메시지에 넣지 않음 | ID·hash만 전달, 원문은 Mongo/객체 저장소 참조 | 대응 |

서비스 jobs의 PENDING/RUNNING/SUCCEEDED·RETRY_WAIT·BUDGET_WAIT·FAILED는 유지한다. 적재 stage(SNAPSHOT_READY 등)는 상품 준비의 진행 지점이며 서비스 job 상태 enum을 대체하지 않는다. 예산 대기는 BUDGET_WAIT, 인프라 장애 재시도는 RETRY_WAIT로 구분한다. 새로운 이벤트 확장 필드의 버전·호환성을 Consumer와 계약 테스트한 뒤 배포한다. 기존 운영 v1 스키마가 있으면 같은 버전으로 비호환 메시지를 섞지 않는다.

## 3. 01-infrastructure.md

근거: [인프라](../../../docs/design/01-infrastructure.md)의 기반 사양·DB 다중화·물리 배치·저장소·접속 계약.

| 상위 계약 | 이 설계의 대응 | 판정 |
|---|---|---|
| 모니터링 포함 20대 역할·AI VM 추가 없음 | 상품 준비 pj3-nifi, Spark pj3-spark-master/worker, Webapp/Ad 분리·pj3-monitor 역할 유지 | 대응 |
| pj3-object01/02, 이미지·Delta 개별 배정 미확정 | 역할별 endpoint/bucket·권한 분리. 어느 노드인지 배포 manifest 확인 전 임의 확정 금지 | 대응·실행 조건 |
| Mongo 3노드·rs-products·과반수·journal | 운영 preflight·primary/majority 읽기. standalone 운영 거절 | 대응·실행 조건 |
| MySQL Primary+Replica2·초기 Primary 접근 | 게시/검수 읽기·쓰기는 현재 Primary. 수동 승격 때 게시 중지·대조·재연결 | 대응·실행 조건 |
| 실제 주소·인증정보는 외부 배포 설정 | environment_id·설정 fingerprint·환경 manifest, 누락 시 단계 차단 | 대응 |
| Kafka/Mongo/MySQL 호스트 장애 분산 | 별도 노트북 배치 증적과 장애 시험을 배포 체크리스트에 요구 | 실행 조건 |
| 제한된 RAM·디스크·I/O·동시성 | 초기 동시성 1·배치 100·별도 영속 볼륨·로그 한도, 실측 후 확대 | 대응·실행 조건 |
| AWS에서 저장 VM을 S3로 대체 가능 | 저장소 adapter+endpoint 설정 교체. 기존 ID·객체 키·상태 보존 | 대응·실행 조건 |

객체 저장소의 실제 제품·API 호환은 배포에서 시험한다. NiFi에서 Python 실행 배포·ACL에 pj3-nifi→Mongo/Kafka/역할 확인한 이미지 저장소 접근이 필요하다. S3 클라이언트 선택을 실제 호환 검증으로 표시하지 않는다. 문서 작업으로 네트워크 규칙·노드를 변경하지 않았다. 실제 20대 서버명과 AWS 저장 VM 대체 기준은 상위 인프라 문서를 참조한다.

## 4. 12-direct-recommendation.md

근거: [직접 추천 v2](../../../docs/design/12-direct-recommendation.md)의 변경 범위·책임·크기 근거·검색/운영 순위·실패/모델 배포.

| 상위 계약 | 상품 파이프라인에서 보존할 경계 | 판정 |
|---|---|---|
| 실제 상품 직접 추천, 생성·template 선행 경로 제외 | 관련 원천·토픽·FK를 신규 상품 준비 필수 조건으로 넣지 않음 | 대응 |
| AI 추정을 상품 실측·가격·URL로 승격 금지 | 확인한 원천·규격 축·단위·조건만 정규화, 결측·충돌 품질 상태 보존 | 대응 |
| 필수 조건의 미확인은 MATCH와 구분 | NULL·UNKNOWN·품질 이슈를 서비스에 인계. 추천 단계의 판정으로 남김 | 대응 |
| 활성 승인 이미지·권한·현재 상품/링크 검증 | product_images 현재 연결·scope/만료·source_version·검수 근거 인계 | 대응 |
| 상품 임베딩은 사전 계산·Silver 확인 후 인덱스 | 광고 서버 준비 Consumer 책임, asset·checksum·모델/전처리/차원 고정 | 대응·범위 외 구현 |
| 입력 Bronze→해석·검증→검색·scikit-learn CPU 순위 | 사용자 입력·추천 run·context·모델 점수 원장을 상품 게시가 수정하지 않음 | 범위 외 구현 |
| 모델·특징·인덱스 버전 고정, 미준비/실패 구분 | 골드 카탈로그 버전과 품질 근거 인계. 적재기가 모델 준비 상태·추천 성공을 생성하지 않음 | 대응·범위 외 구현 |
| 승인 라벨·노출/선택·학습 분리 | 상품 수집/후보를 실제 노출·행동·학습 라벨로 변환하지 않음 | 대응 |

이 파이프라인은 추천이 사용할 원천·카탈로그를 준비한다. 멀티모달 해석, 입력 Bronze 확인, context 저장, scikit-learn 학습·배포·추론, FAISS 검색, 추천 이력/행동 API는 각 서비스의 별도 구현·인수 범위다. 따라서 네 문서의 **상품 데이터 경로에 해당하는 계약을 반영했다**고 평가하며 전체 추천 아키텍처 구현 완료라고 평가하지 않는다.

## 5. 교차 검토에서 보완한 지점

1. 파일 상태만으로 운영하던 초기 제안을 MongoDB 보조 운영 원장+파일 복구 증거로 수정했다.
2. 이미 정해진 danawa:<id> product_key를 반영하고 Offer·관측·스냅샷 키를 구분했다.
3. 새로운 로컬 이미지 확보와 원격 업로드 재시도를 분리해 원천 이력이 불필요하게 늘어나지 않게 했다.
4. raw 불변 image_refs에 asset_id를 사후 UPDATE하지 않고 Gold 게시 때 MySQL 관계로 연결하게 했다.
5. 갤러리 사진마다 variant 슬롯을 두어 현재 연결 하나 규칙이 모든 사진을 하나로 축소하지 않게 했다.
6. 기존 response_body_hash 결측을 숨기지 않고 향후 응답 바이트 보존 규칙과 분리했다.
7. 상품 최초 시각·상세·가격·다운로드·적재 시각을 분리하고 시점 충돌을 품질 대기로 처리했다.
8. 상위 재시도 초기 후보 3회에 맞추고 Consumer offset·재할당·poll·lease 경계를 명시했다.

## 6. 실행 입력·후속 계약·검증

실제 IP/인증서/계정, 검증한 버전 lock, 시험 버킷·토픽, 실제 서비스 DDL, 사람이 확인한 카테고리 매핑·이용 검수는 만들어낼 수 없는 배포·업무 입력이다. 각각 preflight·품질 대기·게시 차단 조건을 계약 v1에 명시했다. 환경이 준비되지 않았는데 통과로 표시하지 않는다.

v1.1의 LIVE 모호성 대기·source pointer·capture chain은 구현 후 로컬 검증했다. 2026-10-17에는 Silver의 고정 입력 상태 대조·평가 receipt·순환 재평가를 구현하고 로컬 Delta 시험을 수행했다. Gold/MySQL 게시 guard는 아직 미구현이다. 가격 효과 키 정책과 실제 서비스 DDL/잠금·승인 재사용 규칙은 담당 계약 인수가 필요하다. Silver의 실제 지원·등록 export·서버 미검증 범위는 [실행 가이드](silver_execution.md)를 따른다. 이슈 완료 체크는 담당자가 검토하며 실제 조건·실패 주입 인수는 보강 계약 9절을 따른다.

상위 03/05/01/12 문서는 수정하지 않았다. FULL_EXPORT 첫 단계와 상품 Bronze 코드를 작성하고 데이터 계약·대역 장애 및 로컬 Delta 테스트를 수행했다. 실제 시험 인프라 종단 테스트와 Silver/Gold/서비스 게시 인수는 남아 있다. [첫 단계 실행 기록](execution.md)과 [Bronze 실행 기록](bronze_execution.md)에서 구현/미구현 범위와 검증 한계를 구분한다.
