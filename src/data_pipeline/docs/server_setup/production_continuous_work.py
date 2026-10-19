"""Drain the complete real source range through repeated fixed-version epochs."""
import json,os,sys,subprocess,time
from pathlib import Path
ROOT=Path(__file__).resolve().parent
sys.path.insert(0,str(ROOT/'source'))
from production_work import setup,BASE
from product_pipeline.files import read_json,atomic_json,lock
from product_pipeline.contracts import now
from pymongo import MongoClient
from pymongo.read_concern import ReadConcern
RUN='0685b031-7662-45c6-bdfe-c7b499051069'

def child(script,args,logpath,env):
    log=logpath.open('ab');os.chmod(logpath,0o600)
    return subprocess.Popen([sys.executable,str(script),*args],cwd=ROOT,env=env,stdin=subprocess.DEVNULL,stdout=log,stderr=log)

class Existing:
    def __init__(self,pid,script,receipt,statuses):self.pid=pid;self.script=script;self.receipt=receipt;self.statuses=statuses
    def poll(self):
        proc=Path('/proc')/str(self.pid)/'cmdline'
        if proc.exists() and self.script.encode() in proc.read_bytes():return None
        return 0 if self.receipt.exists() and read_json(self.receipt).get('status') in self.statuses else 1

def producer(previous,script,args,log,env,receipt,statuses):
    if previous:
        current=Existing(previous,script.name,receipt,statuses)
        if current.poll() is None or current.poll()==0:return current
    if receipt.exists() and read_json(receipt).get('status') in statuses:return Existing(0,script.name,receipt,statuses)
    return child(script,args,log,env)

def main():
    setup();client=MongoClient(os.environ['PIPELINE_MONGO_URI'],serverSelectionTimeoutMS=8000)
    db=client.get_database('pj3_products_raw',read_concern=ReadConcern('majority'))
    env=dict(os.environ,PIPELINE_CONTINUOUS='1')
    progress_path=ROOT/'flow-progress.json'
    progress=read_json(progress_path) if progress_path.exists() else dict(epoch=0,last_events=0,scope='FULL_65498_PRODUCTS_AND_AVAILABLE_IMAGES')
    raw=producer(progress.get('raw_relay_pid'),ROOT/'raw_relay.py',[],ROOT/'raw-relay.private.log',env,ROOT/'raw-relay-progress.json',{'COMPLETED'})
    image_root=BASE/'crawler-connect-0685b031-20261019'
    images=producer(progress.get('image_intake_pid'),image_root/'connect.py',['follow'],ROOT/'image-intake.private.log',env,image_root/'progress.json',{'CURRENT_READY_RANGE_INGESTED'})
    progress.pop('error_code',None)
    progress.update(status='RUNNING',stage_attempt=0,raw_relay_pid=raw.pid,image_intake_pid=images.pid)
    atomic_json(progress_path,progress)
    while True:
        events=db.ingestion_observations.count_documents({'document.collection_run_id':RUN,'event_confirmed':True})
        progress.update(events_confirmed=events,raw_relay_alive=raw.poll() is None,image_intake_alive=images.poll() is None,updated_at=now())
        if raw.poll() not in (None,0) or images.poll() not in (None,0):
            raise RuntimeError('UPSTREAM_INTAKE_FAILED')
        upstream_done=raw.poll()==0 and images.poll()==0
        if events==progress['last_events']:
            if upstream_done:
                if progress.get('mysql_counts',{}).get('products',0)<65498:raise RuntimeError('FULL_PRODUCT_RANGE_NOT_PUBLISHED')
                progress.update(status='COMPLETED',finished_at=now());atomic_json(progress_path,progress);return
            progress['stage']='WAITING_CONFIRMED_EVENTS';atomic_json(progress_path,progress);time.sleep(10);continue
        if events-progress['last_events']<100 and not upstream_done:
            atomic_json(progress_path,progress);time.sleep(5);continue
        epoch=ROOT/'epochs'/('%06d'%progress['epoch']);epoch.mkdir(parents=True,exist_ok=True,mode=0o700)
        stage_env=dict(env,PIPELINE_EXECUTION_ROOT=str(epoch))
        for stage in ('bronze','silver','gold','publish'):
            result_path=epoch/(stage+'-result.json')
            if result_path.exists():continue
            progress.update(stage=stage,execution_root=str(epoch));atomic_json(progress_path,progress)
            succeeded=False
            for attempt in range(3):
                worker=child(ROOT/'production_work.py',[stage],epoch/(stage+'.private.log'),stage_env)
                code=worker.wait()
                if code==0 and result_path.exists():succeeded=True;break
                progress.update(stage_attempt=attempt+1,last_failure=read_json(epoch/(stage+'-failure.json')) if (epoch/(stage+'-failure.json')).exists() else {'error_code':'WORKER_EXIT_'+str(code)})
                atomic_json(progress_path,progress);time.sleep(5)
            if not succeeded:raise RuntimeError('PIPELINE_STAGE_FAILED_'+stage)
            if stage=='publish':
                result=read_json(result_path)
                progress['mysql_counts']=result['counts'];progress['last_mysql_commit_at']=result['finished_at']
                atomic_json(progress_path,progress)
        bronze=read_json(epoch/'bronze-result.json')
        # A bounded Bronze window may leave already-confirmed Kafka backlog.
        # Do not mark that backlog drained merely because producers finished.
        progress['last_events']=events if bronze.get('source_drained',True) else -1
        progress['epoch']+=1;atomic_json(progress_path,progress)

if __name__=='__main__':
    with lock(ROOT/'continuous.lock'):
        try:main()
        except Exception as exc:
            p=read_json(ROOT/'flow-progress.json') if (ROOT/'flow-progress.json').exists() else {}
            p.update(status='FAILED',error_code=getattr(exc,'code',str(exc) if isinstance(exc,RuntimeError) else type(exc).__name__),updated_at=now())
            atomic_json(ROOT/'flow-progress.json',p);raise
