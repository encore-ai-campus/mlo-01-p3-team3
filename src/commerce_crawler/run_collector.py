"""Direct Python entry point: run this file to collect the configured 71 categories."""
from pathlib import Path
import sys


def main():
    print("[준비] 다나와 수집기를 시작합니다.", flush=True)
    dependencies = Path(__file__).resolve().parents[2] / ".local-deps"
    if dependencies.is_dir():
        sys.path.insert(0, str(dependencies))
    try:
        from product_collector.__main__ import main as collect_main
    except ModuleNotFoundError as exc:
        print(f"[오류] 필요한 모듈이 없습니다: {exc.name}. requirements-collector.txt를 설치해주세요.", file=sys.stderr, flush=True)
        return 1
    return collect_main()


if __name__ == "__main__":
    raise SystemExit(main())
