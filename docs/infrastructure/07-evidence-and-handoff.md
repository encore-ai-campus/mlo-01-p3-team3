# 근거 목록과 인수인계

[문서 목록](README.md)

## 근거 사용 방식

아래 목록은 공개하지 않는 원본을 승인된 운영자가 찾기 위한 참조다. PRIVATE_RECORDS는 중앙 PC의 보호된 작업 자료, BASTION_PROJECT는 공식 Bastion 프로젝트를 뜻하는 플레이스홀더다. 원본 파일·스크립트·로그·Vault·보고서 본문은 이 저장소에 추가하지 않는다.

문서 작성 시 실제 Bastion 프로젝트·인벤토리 20대·코드 130개 항목의 경로와 해시·보고서 존재를 읽기 전용으로 대조했다. 코드 해시는 비공개 감사 자료에 보존한다. Bastion 디렉터리는 Git 저장소로 확인되지 않아 이 PR commit이 실제 배포 코드 commit이라는 의미는 아니다.

아래 상대 경로는 **보호된 자료 안의 경로**이며 GitHub 파일 링크가 아니다. 공용 문서가 없는 원본에 깨진 공개 링크나 접근 가능한 것처럼 보이는 URL을 만들지 않는다.

## E01

Stage 1, 2026-10-17: PRIVATE_RECORDS의 pj3-stage1/SUMMARY.md·final-summary.json 및 Bastion reports/stage1-20261017T052719Z-172183. 패키지 16종·스크립트 검토 범위·검토 해시·안전성 테스트 근거다. 실제 승인 hash 값과 승인 정책 원문은 보호된 원본에서 확인한다.

## E02

Vault, 2026-10-17: pj3-vault-integration/SUMMARY.md·VAULT-RUNBOOK.md·final-summary.json 및 reports/vault-integration-20261017T055316Z. 인증 구조와 20대 sudo·Preflight 결과의 근거다. 비밀 변수·암호 파일은 포함하지 않는다.

## E03

Stage 2: pj3-stage2/SUMMARY.md 및 reports/stage2-20261017T055948Z-2352c1. 승인 패키지·실제 설치 검사·역할별 상태·137개 테스트 근거다.

## E04

공식 배포 코드: BASTION_PROJECT의 inventory, ansible/playbooks·ansible/roles·ansible/library·tests·scripts 및 cluster/playbooks 하위 배포 자산. 문서 작성 시 존재·경로·SHA256 목록을 비공개로 대조했다. 스크립트·Playbook·Role은 작성자 비공개 자산이며 이 문서 PR에 복사하지 않는다.

| 검토 영역 | 보호된 코드 참조 예 | 보호된 테스트 참조 예 |
|---|---|---|
| 설치·승인 | ansible/playbooks/install-packages.yml, scripts/pj3-stage1.py | tests/test_stage1.py, tests/test_stage2.py |
| 인벤토리 충돌 | ansible/roles/bastion/tasks/main.yml | tests/test_bastion_groups.py |
| Vault 실행 | scripts/pj3-vault-check.py | tests/test_vault.py |
| Kafka·Spark·NiFi | cluster/playbooks/configure.yml, start.yml, verify.yml | tests/test_cluster_plan.py, test_cluster_health.py, test_nifi_config.py |
| Stage 3 서비스 | cluster/playbooks/stage3-secure.yml, stage3-final.yml | tests/test_stage3.py, test_stage3_review.py |
| 디스크·MySQL 복제 | cluster/playbooks/storage-scan.yml, storage-migrate.yml, storage-mysql-replication.yml | E07의 디스크·복제·모니터링 검사 29건 |
| S3 | cluster/playbooks/object-storage-deploy.yml, object-storage-s3-verify.yml, object-storage-spark-verify.yml | E08의 안전성 검사 9건·실제 API 보고서 |
| 모니터링 | cluster/playbooks/health-deploy.yml, health-check.yml | E06의 로컬 검사·promtool 시나리오 |

표는 파일 위치를 알려주는 참조이며 코드 본문·배포 설정을 제공하지 않는다. 인벤토리의 bastion_nodes 그룹과 bastion Role은 별개로 관리하며 그룹/호스트 이름 충돌을 피한 회귀 검사도 기존 원본에 있다.

## E05

Stage 3: pj3-stage3/결과보고서.md·STAGE3-FINAL.json·checks-final.json·changed-files.json 및 reports/stage3-20261017T071318Z-b6aa69. 서비스별 VERIFIED/PENDING, 시험 E2E, 153개 테스트 근거다. 단계 당시 후보 제품과 후속 실제 설치 제품을 구별한다.

## E06

Grafana: pj3-grafana-health/results/RESULT.md 및 reports/grafana-health-20261017T114234Z-d7b889를 포함한 단계별 보고서. 20대 메트릭·변수·규칙·기존 서비스 보존 결과를 담는다. 후속 패널·규칙 변경은 E07·E08을 함께 확인한다.

## E07

디스크·DB: pj3-storage-cluster/results/RESULT.md·MONGODB-AUTH-RESULT.md·MONGODB-AUTH-FINAL.json·INTEGRATION-DIAGNOSIS.md 및 reports/storage-cluster-20261018T040842Z-8b84d1. 장치·UUID·mount·백업 위치·무결성·MySQL 복제와 Mongo 인증 보류 근거다.

## E08

S3: pj3-object-storage/results/RESULT.md·TEAM-S3-GUIDE.md 및 reports/object-storage-20261018T064320Z. 제품·라이선스·객체 무결성·multipart·native 클라이언트·장애 시험, 실제 팀원 접속 안내의 보호된 원본이다.

## E09

통합 조사: pj3-final-integration-aws/A-PIPELINE.md·B-AWS-MIGRATION.md·FINAL.json 및 reports/final-integration-aws-20261018T082333Z. 이때 조사한 dev는 9972ce080478fded68a7495313b8bd9a522884dd다. 이후 dev의 구현·의존성·검증 추가는 E13과 구별한다.

## E10

Windows 관리: pj3-windows-ssh/RESULT.md·actual-audit.json·latest-result.json. 중앙 PC에서 다른 Windows 4대에 실제 비대화식 공개키 인증·원격 PowerShell 성공, 계정별 적용·기존 SSH 보존 근거다. 팀원 계정·PC 주소·키 지문은 공개하지 않는다.

## E11

VMware: pj3-vmware-inventory/REPORT.md·FINAL.json·VM-ANALYSIS.json·HOST-CAPACITY-5.csv·VM-INVENTORY-20.csv·DISKS-SNAPSHOTS.md. VMX·VMDK 경로, 20대·28디스크 체인, 호스트 여유 공간, 23개 검사와 이전 순서의 원본이다.

## E12

AWS pilot: pj3-aws-pilot/FINAL.json·AWS-BOOTSTRAP.md·FOLLOWUP.md·user-confirmed-aws.json. 이미지 정합성 관문·CLI 설치·사용자가 제공한 CloudShell 결과·리소스 bootstrap 계획·예상 비용·미실행 상태의 근거다. 이 자료는 문서 조사 당시 이전 보호 보관 위치에 남아 있었고 원본을 이동·변경하지 않았다.

## E13

팀원 후속 파이프라인 문서와 코드의 읽기 전용 비교 기준은 dev의 c23a09fe5c6611f4894924747ccb9899868b1a74다.

- [팀 파이프라인 개요](https://github.com/encore-ai-campus/mlo-01-p3-team3/blob/c23a09fe5c6611f4894924747ccb9899868b1a74/src/data_pipeline/README.md)
- [팀 담당자 서비스 검증 기록](https://github.com/encore-ai-campus/mlo-01-p3-team3/blob/c23a09fe5c6611f4894924747ccb9899868b1a74/src/data_pipeline/docs/service_validation_20261018.md)
- [서버 Spark 의존성 기준](https://github.com/encore-ai-campus/mlo-01-p3-team3/blob/c23a09fe5c6611f4894924747ccb9899868b1a74/src/data_pipeline/requirements-spark-server.txt)

이 파일들은 기존 팀원이 관리한 자료다. 내용을 수정하거나 구현·시험을 신성민의 단독 성과로 옮겨 적지 않는다. 본 문서는 해당 담당자가 기록한 제한된 시험 결과를 인용하며 그 환경을 이번에 다시 실행한 것은 아니다.

## 남은 작업과 담당 경계

| 작업 | 현재 경계 | 다음 조건 |
|---|---|---|
| Mongo 인증·운영 검증 | 인프라 실행 보류, 팀원 제한 인증 시험 별도 | 승인된 인증 보관 위치·시험 범위와 운영 기준 인수인계 |
| NiFi → MySQL | 기존 직접 접속 1130 해결 확인 없음 | DB 담당자 최소 권한 승인·실제 네트워크 TLS 시험 |
| 업무 E2E | 인프라 smoke 및 팀원 부분 파이프라인 시험 | 실제 업무 계약·품질 조건·추천 화면까지 공동 재현 |
| Python·Spark 의존성 | 최신 dev pin 확인 | 팀원별 설치 lock·실제 호환성 재검증 |
| Garage | 실제 S3 연동, 두 노드 쓰기 HA 미보장 | 장애 허용 목표·독립 백업·확장 구조 결정 |
| DB 복구 | 같은 VM 내 원본·복구 검증 | 호스트 독립 백업·운영 중 복구 훈련 |
| 모니터링 | 규칙 배포·실제 수집 확인 | 수신자 승인·외부 전달·당직 절차 시험 |
| AWS | 조사·절차·초안 단계 | 일관된 이미지·정책/권한·지원·비용 승인 |

## 문서 변경 복구

이 PR은 Markdown만 변경한다. 병합 전에는 브랜치를 검토하며 기존 main과 운영 자산은 그대로다. 병합 후 문서 오류는 후속 문서 PR 또는 해당 문서 commit의 revert로 복구한다. 서비스·VM·DB·AWS 상태를 변경하는 명령은 포함하지 않는다.
