"""Run already stored Bronze through Silver/Gold/publication, without Kafka drain."""
import os,subprocess,sys
from datetime import datetime,timezone
from pathlib import Path
ROOT=Path(__file__).resolve().parent
sys.path.insert(0,str(ROOT/'source'))
from product_pipeline.files import read_json,atomic_json
from product_pipeline.contracts import now


def main():
    progress_path=ROOT/'downstream-progress.json'
    state=read_json(progress_path) if progress_path.exists() else dict(
        execution_root=str(ROOT/'downstream'/datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')),
        scope='SAVED_BRONZE_ONLY',started_at=now())
    cut=Path(state['execution_root']);cut.mkdir(parents=True,exist_ok=True,mode=0o700)
    env=dict(os.environ,PIPELINE_EXECUTION_ROOT=str(cut),PIPELINE_CONTINUOUS='1')
    for stage in ('silver','gold','publish'):
        result=cut/(stage+'-result.json')
        if result.exists() and read_json(result).get('status')=='COMPLETED':continue
        state.update(status='RUNNING',stage=stage,updated_at=now());atomic_json(progress_path,state)
        with (cut/(stage+'.private.log')).open('ab') as log:
            code=subprocess.run([sys.executable,str(ROOT/'production_work.py'),stage],cwd=ROOT,env=env,
                stdin=subprocess.DEVNULL,stdout=log,stderr=log).returncode
        if code or not result.exists() or read_json(result).get('status')!='COMPLETED':
            failure=cut/(stage+'-failure.json')
            state.update(status='FAILED',failure=read_json(failure) if failure.exists() else dict(error_code='PROCESS_EXIT_'+str(code)),updated_at=now())
            atomic_json(progress_path,state);return 2
        if stage=='silver':
            request=read_json(cut/'normalization-request.json')
            state['bronze_version']=request['input_range']['bronze_version']
        state.update(last_completed_stage=stage,updated_at=now());atomic_json(progress_path,state)
    state.update(status='COMPLETED',stage='publish',publication=read_json(cut/'publish-result.json'),finished_at=now())
    atomic_json(progress_path,state);return 0


def run():
    if os.environ.get('PIPELINE_WINDOWED_FLOW')=='1':
        progress_path=ROOT/'downstream-progress.json'
        cut=ROOT/'bounded-downstream-v1'
        if progress_path.exists():
            previous=read_json(progress_path)
            if previous.get('execution_root')!=str(cut):
                from product_pipeline.contracts import digest
                atomic_json(ROOT/'downstream/status-history'/(digest(previous)+'.json'),previous)
        state=dict(status='RUNNING',stage='windows',scope='SAVED_BRONZE_BOUNDED_WINDOWS',execution_root=str(cut),updated_at=now())
        atomic_json(progress_path,state)
        env=dict(os.environ,PIPELINE_EXECUTION_ROOT=str(cut),PIPELINE_CONTINUOUS='1')
        code=subprocess.run([sys.executable,str(ROOT/'production_window_work.py')],cwd=ROOT,env=env,stdin=subprocess.DEVNULL).returncode
        state.update(status='COMPLETED' if code==0 else 'PAUSED_AFTER_BATCH' if code==75 else 'INTERRUPTED',updated_at=now())
        atomic_json(progress_path,state)
    else:code=main()
    if code==0 and os.environ.get('PIPELINE_RESUME_FLOW_AFTER_DOWNSTREAM')=='1':
        os.execv(sys.executable,[sys.executable,str(ROOT/'continuous_work.py')])
    return code


if __name__=='__main__':raise SystemExit(run())
