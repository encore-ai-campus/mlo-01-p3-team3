# 카탈로그 준비와 사전 임베딩

2026-10-18: 기존 파일 카탈로그를 유지하며 `Product.interior_profile`, `attributes_json`을 선택 입력으로 지원한다. [방 특징 추천](../interior-ranking.md)의 확인 상태·옵션·부위 규칙으로 특징을 인덱스 로딩 시 준비한다. MySQL에서 읽는 어댑터는 아직 없고 실제 이미지/벡터 대응·현재 검수 인수는 후속 범위다. 이 변경만을 이유로 임베딩/카탈로그를 재생성하지 않는다.

[역할별 라우팅](../README.md) · [실행 명령·manifest 예시](../../../src/interior_recommender/README.md) · [데이터 파이프라인 설계](../../design/02-data-pipeline.md)

`catalog/`는 사용자 요청 전에 상품 원장·이미지·벡터를 준비한다. `catalog/danawa.py::DanawaCatalog`는 저장 임베딩의 이미지·벡터·checksum·설정 대응을 읽는 공통 로더다.

| 단계 | 구현 | 입출력·호출 책임 |
|---|---|---|
| 수집 자료 준비 | `crawl.prepare` | 수집 파일/이미지 검증 → prepared.json·embedding_inputs. AI 호출 없음 |
| 규격 정제 | `metadata.product_metadata`, `normalize_specifications` | 원장 → 출처가 있는 상품 속성·축별 치수/범위 |
| 사전 임베딩 | `cli embed-directory → batch_embedding.BatchEmbedding.run` | 준비 이미지 → `Pipeline.stage → ModelAdapter` → 저장 임베딩 index |
| 수집 결과 결합 | `crawl.export` | 준비 manifest·완료 벡터 대응 검증 → 상품 manifest·NPY·이미지 |
| 별도 원장 결합 | `export.export_catalog` | 저장 다나와 벡터·별도 metadata.json → 상품 카탈로그 |
| 온라인 연결 | `DirectStudio → ProductIndex.load` | `--products` manifest 로딩·설정 호환 검사 |

기존 `python -m interior_recommender.crawl_catalog`·`product_catalog` 명령은 유지한다. 새 구현 진입점은 각각 `python -m interior_recommender.catalog.crawl`·`interior_recommender.catalog.export`다. 일괄 임베딩 명령은 기존 `python -m interior_recommender embed-directory`다.

새 이미지만 사전 임베딩하며 export가 벡터를 다시 생성하지 않는다. `crawl.export(with_links=True)`는 원장의 상품번호/URL을 검증해 링크 포함 사본을 만든다. 원장 결측을 파일명이나 AI로 채우지 않고 미확인으로 남긴다. 표본 export의 `test_only`·`bronze_verified=False`는 서비스 수집/보존 완료로 바꾸지 않는다.

상품 행·벡터 행·이미지 checksum·모델/차원/전처리 일치를 보존한다. 이동 전 기본 `sample/` 위치를 유지한다. 신규 데이터 모델 변경은 [설계 라우팅](../../design/index.md)의 데이터 항목으로 돌아간다. 회귀 검증은 `test_catalog_metadata.py`, `test_crawl_catalog.py`, `test_batch_embedding.py`, `test_danawa_references.py`, `test_recommendation.py`다.
