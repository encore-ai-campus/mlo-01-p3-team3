"""Give up a run that will not be resumed: status "abandoned", no longer the active run.

The next collection then starts a new run. The run's files stay until
`python -m product_collector.prune --prune-abandoned --apply` removes them."""
import argparse
import json
from pathlib import Path

from .checkpoint import abandon_run


def main():
    parser = argparse.ArgumentParser(description='재개하지 않을 수집 실행을 포기(abandoned)로 표시하고 활성 실행에서 뺀다')
    parser.add_argument('--output', type=Path, required=True, help='수집기 --output과 같은 실행 루트')
    parser.add_argument('--run', type=Path, help='포기할 실행 폴더. 없으면 활성 실행(active_run.json)')
    parser.add_argument('--reason', help='포기 사유(운영 기록용)')
    args = parser.parse_args()
    try:
        result = abandon_run(args.output, args.run, args.reason)
    except (ValueError, OSError) as exc:
        parser.exit(3, f'{parser.prog}: error: {exc}\n')
    print(json.dumps(result, ensure_ascii=False))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
