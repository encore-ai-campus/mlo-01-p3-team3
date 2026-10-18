# SketchOn Operations Implementation Plan

Goal: 승인한 운영 콘솔 8개 메뉴와 행동/수집 추적 및 광고 운영 준비를 구현한다.
Spec: ../specs/2026-10-16-sketchon-operations-design.md
Architecture: operations Django 앱, 기존 sketch 모델의 최소 확장, 로컬 광고 초안. 실제 광고 서버는 연동 계약이 준비되기 전 호출하지 않는다.

1. 별도 작업본·DB 온라인 백업·소스/미디어/Raw 해시·기존 PK 보관. 사용자 docs 이동은 수정하지 않는다.
2. 권한·광고 상태·추적·민감자료 테스트를 먼저 작성해 RED 확인. operations 모델과 migration, sketch flow/actor snapshot 확장.
3. 조회/페이지 필터·운영 권한·민감 미디어·Raw 인덱싱/검수·감사 구현. 원문은 변경하지 않는다.
4. 생성/수집 추적 및 flow 전파, 클라이언트 보고 인증을 수신 인증과 분리. 기존 이벤트 재전송 계약 유지.
5. 8개 화면, 캠페인 작성·승인/거절/중지·소재 재심사·revision 충돌, 역할별 버튼 구현. 광고 성과 미연결 표시.
6. 문서 현행화, 전체 Django/Node 및 운영 브라우저/기존 회귀, 최종 독립 리뷰와 중요 오류 수정.
7. 변경 파일 해시 확인 후 적용·migration·기존 데이터 보존 확인·hilee 운영 그룹 연결·LAN 재시작. 검증용 계정은 실제 DB에 생성하지 않는다.

Review focus: 회원의 운영 API 우회, 개인자료/경로 노출, 동시 생성 성공 상태 보존, 비회원 Raw의 계정 귀속 오인, 광고 소재 변경 승인 무효화, 캠페인 동시 수정, 초안과 실제 송출/성과 혼동.
