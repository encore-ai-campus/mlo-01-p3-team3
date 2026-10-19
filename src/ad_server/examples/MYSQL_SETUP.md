# 광고 서버 MySQL 연결 준비

현재 기본 DB는 SQLite입니다. 이 안내서는 연결 준비용이며 실제 MySQL 접속·DB 생성·마이그레이션을 수행한 기록이 아닙니다. UI와 상품 파이프라인은 변경하지 않았습니다.

## 현재 연결 없이 확인하기

ad_server 디렉토리의 가상환경에서 실행합니다.

```text
python -m pip install -r requirements-mysql.txt
python manage.py check_ad_database_config
```

점검 명령은 DB에 접속하지 않으며 접속 주소·계정·비밀번호도 출력하지 않습니다. MySQL 모드에서도 설정과 드라이버 준비 상태만 확인합니다. 서버 버전·실제 권한·테이블·네트워크는 확인하지 않습니다.

## 추후 실제 연결할 때

1. 인프라 담당자가 광고 전용 빈 MySQL 데이터베이스와 계정을 준비합니다. 기존 Application/상품 DB를 대상으로 지정하지 않습니다. Django 인증·세션·관리자 테이블도 함께 생성되는 독립 서버 구조입니다.
2. 실행 환경에 AD_DB_BACKEND=mysql, AD_MYSQL_DATABASE, AD_MYSQL_USER, AD_MYSQL_PASSWORD, AD_MYSQL_HOST, AD_MYSQL_PORT를 설정합니다. `.env.example`은 예시이며 자동으로 읽지 않습니다. 비밀번호를 소스에 작성하지 않습니다.
3. TLS를 사용하는 경우 AD_MYSQL_SSL_CA에 인증기관 파일 경로를 지정합니다. 지정하면 인증서와 호스트명을 검증합니다.
4. `python manage.py check_ad_database_config`로 오프라인 설정 점검을 실행합니다.
5. 실제 DB 연결 승인이 난 뒤에만 `python manage.py migrate --plan`으로 계획을 확인합니다. 이 명령부터 실제 DB에 접속합니다.
6. 대상 DB와 계획을 확인한 뒤 `python manage.py migrate`를 별도로 실행합니다. 테이블 및 잠금 기준 행을 생성합니다. 서버 시작 시 자동 실행하지 않습니다.
7. 별도의 폐기 가능한 MySQL 테스트 DB에서 기존 테스트를 수행합니다. AD_MYSQL_TEST_DATABASE는 test_ 접두사와 별도 이름을 요구합니다. `python manage.py test advertising`는 테스트 DB를 생성·삭제하므로 운영 DB 계정으로 실행하지 않습니다.
8. 동시 광고 요청, 동일 요청 재시도, 입찰 버전 생성, 노출 제한, 클릭 중복 기록을 실제 MySQL에서 통과한 뒤 서비스에 연결합니다.

## 구조 및 제한

- InnoDB, utf8mb4, READ COMMITTED, 엄격 모드를 설정합니다. 지원 서버 버전은 설치한 Django의 MySQL 지원 범위와 맞춰 실제 연결 때 확인합니다.
- 광고주·캠페인·심사·선정·이벤트는 광고 운영 테이블입니다. Bronze/Silver/Gold 계층이나 MongoDB 연결을 추가하지 않습니다.
- `0002_seed_serving_gate`는 기존 행 잠금 처리에 필요한 1번 기준 행을 준비합니다. 글로벌 잠금은 정확성 우선이며 고처리량 최적화는 별도 작업입니다.
- 마이그레이션은 기존 SQLite 운영 데이터를 MySQL로 복사하지 않습니다. 기존 데이터 이관이 필요하면 별도 계획이 필요합니다.
- MySQL 테스트에서의 락·데드락·대소문자 비교·동시 처리 동작은 실제 MySQL 검증 전까지 미확인입니다.
- `AD_TEST_OPERATIONS`는 기존대로 기본 0입니다. MySQL을 설정해도 광고 모의 운영이 자동 활성화되지 않습니다. CPC 차감·결제는 추가하지 않습니다.
