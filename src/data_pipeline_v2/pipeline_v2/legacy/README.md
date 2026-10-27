# pipeline_v2.legacy

v2가 쓰는 v1 모듈의 복사본이다. v1(`src/data_pipeline`)은 v2 착수 전 상태로 보존하므로, v2는 v1 패키지를 import하지 않고 이 복사본만 쓴다.

- 출처: `src/data_pipeline/product_pipeline`의 커밋 `b8424ee` 상태. v2가 직접 쓰는 모듈, 서버 스크립트가 쓰는 `kafka_tls`·`spark_runtime`·`objects`, 이들이 import하는 모듈까지 43개를 바이트 그대로 옮겼다.
- 모듈 안의 import는 모두 상대 경로라서 수정하지 않았다. `silver/offer_identity.py`의 `repo://product_pipeline/...` 문자열은 출력 데이터에 들어가는 값이므로 Gold 결과가 같게 유지되도록 그대로 둔다.
- v2를 위해 바꿀 코드는 여기서 고친다. v1은 고치지 않는다.
