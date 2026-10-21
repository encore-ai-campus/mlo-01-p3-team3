"""Explicit real Mongo transaction test using isolated path claims only."""
import sys,os,json,argparse
from pathlib import Path
from dataclasses import replace
from uuid import uuid4
parser=argparse.ArgumentParser(description=__doc__)
parser.add_argument('--deployment-root',type=Path,required=True)
parser.add_argument('--config',type=Path,required=True)
parser.add_argument('--state-dir',type=Path,required=True)
parser.add_argument('--master-url',required=True)
parser.add_argument('--worker',action='append',required=True)
parser.add_argument('--ca',required=True)
parser.add_argument('--client-cert',required=True)
parser.add_argument('--client-key',required=True)
parser.add_argument('--apply-isolated',action='store_true',required=True)
args=parser.parse_args()
r=args.deployment_root.resolve();sys.path[:0]=[str(r),str(r/'source')]
from production_work import setup
from product_pipeline.bronze.config import BronzeSettings
from product_pipeline.bronze.lease import MongoLease
from product_pipeline import lease_recovery as recovery
from product_pipeline.recovery_probe import probe
from product_pipeline.contracts import now,digest
from product_pipeline.files import atomic_json
setup();production=BronzeSettings.load(args.config)
uri=os.environ[production.lease_uri_env];prod=MongoLease(production,uri)
before=recovery.claims(prod)
prefix=production.bronze_path.rsplit('/',1)[0]+'/verification/lease-recovery-'+uuid4().hex
settings=replace(production,environment_id='verification',bronze_path=prefix+'/products',events_path=prefix+'/events',quarantine_path=prefix+'/quarantine',checkpoint_path=prefix+'/checkpoint')
lease=MongoLease(settings,uri);audits=[]
directory=args.state_dir;directory.mkdir(mode=0o700,exist_ok=True)
if os.name!='nt' and directory.stat().st_mode&0o077:raise SystemExit('PRIVATE_DIRECTORY_REQUIRED')
def live():return probe(master_url=args.master_url,ca=args.ca,client_cert=args.client_cert,client_key=args.client_key,worker_hosts=args.worker)
def audit(value):
    audits.append(value['status']);path=directory/('isolated-'+lease.token+'-'+value['status']+'.json');atomic_json(path,value);path.chmod(0o600)
try:
    lease.acquire()
    # Only test-owned claims are expired. Production IDs are never updated.
    result=lease.collection.update_many({'_id':{'$in':lease._claimed},'token':lease.token},[{'$set':{'expires_at':{'$subtract':['$$NOW',1000]}}}])
    assert result.matched_count==4
    plan=recovery.inspect(lease,live)
    released=recovery.apply(lease,plan,live,audit)
    assert released['status']=='RELEASED' and released['released']==4
    repeated=recovery.apply(lease,plan,live,audit)
    assert repeated['status']=='ALREADY_CLEAR'
    after=recovery.claims(prod);assert before==after
    evidence=dict(status='VERIFIED',at=now(),isolated_target=prefix,released=4,repeated=repeated['status'],audit_statuses=audits,production_claims_unchanged=True,production_claims=len(after),production_evidence_hash=digest(after))
    atomic_json(directory/'verification.json',evidence)
    print(json.dumps(evidence))
finally:
    lease.close(release=False);prod.close(release=False)
