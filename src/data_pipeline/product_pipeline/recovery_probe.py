"""Read-only Linux Driver and authenticated Spark cluster quiescence evidence."""
import json,ssl,urllib.request
from pathlib import Path
from urllib.parse import urlsplit
from .contracts import now
from .errors import PipelineError

WRITERS={'production_work.py','continuous_work.py','production_window_work.py','production_downstream_work.py',
         'repair_current_batch.py','recover_offer_observations.py'}


def probe(*,master_url,ca,client_cert,client_key,worker_hosts,proc_root='/proc'):
    if not worker_hosts or len(set(worker_hosts))!=len(worker_hosts):raise PipelineError('RECOVERY_WORKER_INVENTORY_REQUIRED')
    pids=[]
    for p in Path(proc_root).iterdir():
        if not p.name.isdigit():continue
        try:args=[v.decode(errors='replace') for v in (p/'cmdline').read_bytes().split(b'\0') if v]
        except FileNotFoundError:continue
        except OSError:raise PipelineError('RECOVERY_PROCESS_PROBE_UNCONFIRMED') from None
        if any(Path(a).name in WRITERS for a in args) or ('org.apache.spark.deploy.SparkSubmit' in args):pids.append(int(p.name))
    context=ssl.create_default_context(cafile=str(ca));context.load_cert_chain(str(client_cert),str(client_key))
    opener=urllib.request.build_opener(urllib.request.ProxyHandler({}),urllib.request.HTTPSHandler(context=context))
    try:
        with opener.open(master_url,timeout=8) as response:
            if urlsplit(response.geturl()).scheme!='https':raise PipelineError('RECOVERY_AUTHENTICATED_MASTER_REQUIRED')
            data=response.read(1024*1024+1)
        if len(data)>1024*1024:raise PipelineError('RECOVERY_CLUSTER_RESPONSE_TOO_LARGE')
        cluster=json.loads(data)
    except PipelineError:raise
    except Exception:raise PipelineError('RECOVERY_CLUSTER_PROBE_UNCONFIRMED') from None
    workers=[{k:w.get(k) for k in ('host','state','coresused','memoryused')} for w in cluster.get('workers',[])]
    apps=cluster.get('activeapps')
    if (not isinstance(apps,list) or len(workers)!=len(worker_hosts) or {w['host'] for w in workers}!=set(worker_hosts)):
        raise PipelineError('RECOVERY_CLUSTER_INVENTORY_CHANGED')
    quiet=(not pids and not apps and all(w['state']=='ALIVE' and w['coresused']==0 and w['memoryused']==0 for w in workers))
    return dict(status='QUIESCENT' if quiet else 'BUSY',driver_pids=sorted(pids),
        active_applications=[str(a.get('id')) for a in apps],workers=sorted(workers,key=lambda w:w['host']),at=now())
