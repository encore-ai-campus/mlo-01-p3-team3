# 로컬 시연 MVP

2026-10-21. `feature/#34`의 상품 추천·선택형 이미지 생성 시연은 이 폴더에서 실행한다. 기존 `src/demo_sketchon`은 PR 대상 `origin/dev`의 `22926f29051bd0373d19b1a3f312114532579812` 소스를 보존한다.

**MVP 구현 당시 데이터 파이프라인 구축과 서비스 연동 준비가 완료되지 않아 실제 데이터 파이프라인과 연동하지 못했다.** 그래서 검증한 로컬 배치와 Mongo·Garage·Delta 추출 자료를 고정 파일 카탈로그로 결합해 시연했다. 이는 당시 시연 구성을 선택한 배경이며, 현재 파이프라인 전체의 구축 상태를 판정하는 자료가 아니다.

## 폴더와 데이터

- `src/local_demo/`: 시연 전용 Django 앱·화면·워커·테스트.
- `src/demo_sketchon/`: dev 원본 앱. #34 시연 변경으로 덮어쓰지 않는다.
- `src/interior_recommender/`: 공유 엔진과 MySQL·Garage·Delta 어댑터. DB 연결 코드를 보존한다.
- `data/local-demo/snapshot-v43/`: 기존 상품·벡터·이미지·manifest. 이동·재생성하지 않는다.
- `outputs/local-demo-v43/`: 기존 시연 SQLite·미디어·Raw·추천 결과. 원본 앱의 데이터를 복사하거나 migration하지 않는다.

두 앱은 각각 별도 프로세스로 실행한다. Django 내부 앱 이름이 같으므로 한 프로세스의 INSTALLED_APPS에 함께 등록하지 않는다. 시연 기본 포트는 8005이며 광고 서버 안내의 8004와 구분한다. 쿠키·브라우저 이력·이벤트 큐·IndexedDB도 분리했다. 포트 변경 후 기존 8004 origin의 브라우저 저장소는 새 8005 origin으로 자동 이전되지 않는다. 이전 dev 로그인·브라우저 이력을 가져오지 않으며 시연 DB의 기존 계정·프로젝트·결과 파일은 보존한다.

## 실행

[분리·검증·배포 안내](../../docs/interior-recommender/local-demo.md)를 먼저 확인한다. 저장소 `develop` 루트의 PowerShell에서 각 명령을 한 줄씩 실행한다.

기존 스냅샷과 설정만 검사한다. 모델 API나 DB migration을 호출하지 않는다.

```powershell
.\scripts\Start-LocalDemo.ps1 -Mode Check
```

처음 준비할 때에만 시연 SQLite에 migration을 적용한다. DB 변경 명령이므로 운영 DB 경로를 지정하지 않는다.

```powershell
.\scripts\Start-LocalDemo.ps1 -Mode Init
```

아래 두 명령은 각각 별도 터미널에서 실행한다.

```powershell
.\scripts\Start-LocalDemo.ps1 -Mode Worker
```

```powershell
.\scripts\Start-LocalDemo.ps1 -Mode Web
```

브라우저는 `http://127.0.0.1:8005/`를 연다. 상품 스냅샷과 완료 임베딩을 다시 만들지 않는다. 신규 입력 해석·질의 임베딩·선택형 이미지 생성은 기존 보호 설정의 외부 API를 사용한다. `requirements.txt`의 `-e ../interior_recommender`는 같은 공유 엔진을 가리킨다.

현재 시연 앱에는 광고 서버가 연결되지 않았다. 광고 중계 코드는 있으나 기본 `SKETCHON_AD_ENABLED=0`으로 꺼져 있다. 시연 실행기는 광고 서버를 실행하거나 연결 설정을 추가하지 않는다. 별도 광고 서버·인증·승인 캠페인 설정과 실제 화면 확인이 필요하다.

소파 3종은 추적된 정적 파일이다. 별도 준비한 의자·책상·침대 PNG 4종은 기존 파일을 `sketch/static/sketch/assets/stickers/`로 복사했고 해당 하위 `.gitignore`를 유지한다. 다른 체크아웃에는 같은 PNG를 별도로 전달하며 자동 이미지 생성·재다운로드로 대체하지 않는다.
