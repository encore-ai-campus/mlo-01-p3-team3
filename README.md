# MLOps 3차 프로젝트 - Team 3

## 프로젝트 소개
사진·텍스트·스케치의 공간 맥락과 크기 근거를 해석해 이미지 생성 없이 실제 상품을 추천한다. 초기 추천은 CPU에서 scikit-learn 콘텐츠 유사도와 수동 가중합으로 순위를 결정하며, 학습은 후속 평가로 진행한다. 후속 프로젝트에서는 데이터·모델·운영 고도화와 임베딩/멀티모달 추가 내재화를 진행한다. 문서 설계와 기존 생성 MVP 코드 구현 상태를 구분한다.

## 팀 구성
- 팀원 5명

## 프로젝트 문서
- [interior_recommender 역할별 코드·문서 라우팅](docs/interior-recommender/README.md)
- [현재 호출 구조와 패키지 계층](docs/interior-recommender/call-structure.md)
- [SketchOn SQLite 작업 워커·복구·파이프라인 연결 경계](docs/interior-recommender/execution.md)
- [BRD (Business Requirements Document)](docs/BRD_3차.md)
- [PRD (Product Requirements Document)](docs/PRD_3차_초안.md)
- [시스템 아키텍처](docs/design/00-architecture.md)
- [데이터 파이프라인 설계](docs/design/02-data-pipeline.md)

## 협업 규칙
- GitHub Issues를 통한 작업 관리
- 기능별 브랜치 생성
- Pull Request를 통한 코드 리뷰 및 병합
- main 브랜치 직접 Push 금지


## 설계 문서

[역할별 설계 안내](docs/design/index.md) · [데이터 모델과 ERD](docs/design/03-data-model.md) · [광고 서버](docs/design/07-ad-server.md) · [상품 추천·다나와 연결](docs/design/09-product-links.md)

## 직접 추천 현행 설계

[문서 안내](docs/README.md) · [담당별 다이어그램·내재화 단계](docs/design/12-direct-recommendation.md). 이미지 생성 중심 문서는 접힘 영역에 이력으로 보존한다.
