"""NiFi collector entry point. Deployment is disabled until explicitly armed."""
import argparse,json,subprocess,sys
from pathlib import Path

CONFIG=Path('/var/lib/pj3/product-pipeline/nifi/collector/config/collector-control.json')


def command(config):
    source=Path(config['source'])
    return [config['python'],str(source/'run_collector.py'),
        '--categories',config['categories'],'--pagination-template',config['pagination_template'],
        '--output',config['output'],'--workers',str(config['workers']),
        '--listing-interval',str(config['listing_interval']),'--interval',str(config['interval']),
        '--download-images']


def main(argv=None):
    p=argparse.ArgumentParser();p.add_argument('--check',action='store_true');args=p.parse_args(argv)
    config=json.loads(CONFIG.read_text())
    required=[Path(config['python']),Path(config['source'])/'run_collector.py',Path(config['categories']),Path(config['pagination_template'])]
    if not all(x.is_file() for x in required):raise RuntimeError('COLLECTOR_DEPLOYMENT_INCOMPLETE')
    if args.check:
        print(json.dumps(dict(status='CONFIGURED',enabled=config['enabled'],network_requests=0,
            output=config['output'],download_images=True,source_commit=config['source_commit'])));return 0
    if not config['enabled']:
        print(json.dumps(dict(status='DISABLED_NOT_STARTED',network_requests=0)));return 0
    import fcntl
    root=Path(config['output']).parent;root.mkdir(parents=True,exist_ok=True)
    with (root/'collector.lock').open('a+b') as lock:
        try:fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        except BlockingIOError:
            print(json.dumps(dict(status='ALREADY_RUNNING',started=False)));return 0
        with (root/'collector.private.log').open('ab') as log:
            result=subprocess.run(command(config),cwd=config['source'],stdout=log,stderr=log)
        print(json.dumps(dict(status='COMPLETED' if result.returncode==0 else 'NEEDS_REVIEW',exit_code=result.returncode)))
        return result.returncode


if __name__=='__main__':raise SystemExit(main())
