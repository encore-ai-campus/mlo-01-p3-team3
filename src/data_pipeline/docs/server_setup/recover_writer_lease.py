"""Inspect or explicitly release expired writer claims; never start a pipeline."""
import argparse,os,sys,json
from pathlib import Path
from uuid import uuid4

HERE=Path(__file__).resolve().parent
sys.path.insert(0,str(HERE/'source' if (HERE/'source/product_pipeline').is_dir() else HERE.parents[1]))
from product_pipeline.files import read_json,atomic_json,lock
from product_pipeline.errors import PipelineError
from product_pipeline.bronze.lease import MongoLease
from product_pipeline import lease_recovery
from product_pipeline.recovery_probe import probe


def settings_for(stage,path):
    if stage=='bronze':
        from product_pipeline.bronze.config import BronzeSettings
        return BronzeSettings.load(path)
    from dataclasses import asdict
    from types import SimpleNamespace
    if stage=='silver':
        from product_pipeline.silver.config import SilverSettings
        value=SilverSettings.load(path);target=value.silver_root
    else:
        from product_pipeline.gold.config import GoldSettings
        value=GoldSettings.load(path);target=value.gold_root
    return SimpleNamespace(**{**asdict(value),'fingerprint':value.fingerprint,
        'bronze_path':target,'events_path':target,'quarantine_path':target})


def main(argv=None):
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action',choices=['inspect','apply'])
    parser.add_argument('--stage',choices=['bronze','silver','gold'],required=True)
    parser.add_argument('--config',type=Path,required=True)
    parser.add_argument('--state-dir',type=Path,required=True)
    parser.add_argument('--plan',type=Path)
    parser.add_argument('--deployment-root',type=Path,help='Existing protected production_work.setup credentials')
    parser.add_argument('--master-url',required=True)
    parser.add_argument('--worker',action='append',required=True)
    parser.add_argument('--ca',type=Path,required=True)
    parser.add_argument('--client-cert',type=Path,required=True)
    parser.add_argument('--client-key',type=Path,required=True)
    args=parser.parse_args(argv);lease=None
    try:
        if args.action=='apply' and args.plan is None:raise PipelineError('RECOVERY_PLAN_REQUIRED')
        args.state_dir.mkdir(parents=True,exist_ok=True,mode=0o700)
        if os.name!='nt' and args.state_dir.stat().st_mode&0o077:raise PipelineError('RECOVERY_PRIVATE_DIRECTORY_REQUIRED')
        if args.deployment_root:
            sys.path.insert(0,str(args.deployment_root.resolve()))
            import production_work
            production_work.setup()
        settings=settings_for(args.stage,args.config)
        uri=os.environ.get(settings.lease_uri_env)
        if not uri:raise PipelineError('RECOVERY_LEASE_CREDENTIALS_REQUIRED')
        lease=MongoLease(settings,uri)  # Never acquire a production writer claim.
        def live():return probe(master_url=args.master_url,ca=args.ca,client_cert=args.client_cert,
            client_key=args.client_key,worker_hosts=args.worker)
        run_id=uuid4().hex
        def save(name,value):
            path=args.state_dir/name;atomic_json(path,value);path.chmod(0o600);return path
        def audit(value):save(run_id+'-'+value['status'].lower()+'.json',value)
        with lock(args.state_dir/'recovery.lock'):
            if args.action=='inspect':
                plan=lease_recovery.inspect(lease,live);path=save('plan-'+run_id+'.json',plan)
                print(json.dumps(dict(status='INSPECTED',claims=len(plan['claims']),plan_path=str(path),plan_hash=plan['content_hash'])))
            else:print(json.dumps(lease_recovery.apply(lease,read_json(args.plan),live,audit)))
        return 0
    except Exception as exc:
        print(json.dumps(dict(status='FAILED',error_code=getattr(exc,'code',type(exc).__name__))))
        return 2
    finally:
        if lease is not None:lease.close(release=False)


if __name__=='__main__':raise SystemExit(main())
