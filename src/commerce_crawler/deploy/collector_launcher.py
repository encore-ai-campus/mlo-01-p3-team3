"""Fixed entry point NiFi calls: /opt/pj3/danawa-collector/collector_launcher.py <command>.

The install script copies this file there once. It reads the control file's `source` (the installed
release's commerce_crawler/ folder) and runs that release's product_collector.launcher, so a new
release changes neither this file nor the NiFi processor."""
import json
from pathlib import Path
import sys

DEFAULT_CONTROL = '/var/lib/pj3/product-pipeline/nifi/collector/config/collector-control.json'


def main(argv=None):
    argv = list(sys.argv[1:] if argv is None else argv)
    control = Path(argv[argv.index('--control') + 1] if '--control' in argv[:-1] else DEFAULT_CONTROL)
    try:
        source = json.loads(control.read_text(encoding='utf-8'))['source']
    except (OSError, ValueError, KeyError, TypeError) as exc:
        print(json.dumps({'command': argv[0] if argv else None, 'status': 'CONFIG_INVALID',
                          'reason': f'{type(exc).__name__}: {exc}'}, ensure_ascii=False), flush=True)
        return 3
    sys.path.insert(0, str(source))
    from product_collector.launcher import main as launcher_main
    if '--control' not in argv:
        argv = ['--control', str(control)] + argv
    return launcher_main(argv)


if __name__ == '__main__':
    raise SystemExit(main())
