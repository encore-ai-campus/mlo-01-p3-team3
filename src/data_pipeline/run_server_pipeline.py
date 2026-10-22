"""Control the deployed pipeline through Bastion; no local database secrets."""
import argparse
import json
import shlex
import subprocess
import time

STAGES=('bronze','silver','gold','publish')


def worker_tls_preflight():
    """Readability on every execution host, without exporting TLS contents."""
    code=("import os; paths=['/etc/pj3-clients/kafka/pipeline/keystore.p12',"
          "'/etc/pj3-clients/kafka/pipeline/truststore.p12']; "
          "missing=[p for p in paths if not os.path.isfile(p) or not os.access(p,os.R_OK)]; "
          "print('KAFKA_TLS_FILES_READABLE' if not missing else 'KAFKA_TLS_FILES_MISSING '+','.join(missing)); "
          "raise SystemExit(bool(missing))")
    args=json.dumps({'argv':['runuser','-u','pj3-spark','--','/usr/bin/python3','-c',code]})
    command=('cd /home/team3ops/pj3/ansible && ANSIBLE_BECOME_EXE=/usr/bin/sudo.ws ansible '+
        shlex.quote('pj3-spark-master:pj3-spark-worker*')+' -m command -a '+shlex.quote(args)+
        ' --become --private-key /home/team3ops/.ssh/pj3_ansible '+
        '--vault-password-file /home/team3ops/.config/pj3/vault-pass')
    response=subprocess.run(['ssh','-o','BatchMode=yes','-o','ConnectTimeout=10','bastion',command],
        capture_output=True,text=True,encoding='utf-8',timeout=60)
    if response.returncode or response.stdout.count('KAFKA_TLS_FILES_READABLE')!=4:
        raise RuntimeError('Kafka TLS 사전 검사 실패: Master와 Worker 3대의 파일·pj3-spark 읽기 권한을 확인하세요.')


def call(action,stage):
    if action in {'start','preflight'}:worker_tls_preflight()
    command=('cd /home/team3ops/pj3/ansible && '
        'ANSIBLE_LIBRARY=/home/team3ops/pipeline-acceptance-0594f7b79ed347c5bf9e449cdb5912af '
        'ANSIBLE_BECOME_EXE=/usr/bin/sudo.ws ansible pj3-spark-master -m production_manage -a '+
        shlex.quote('action='+action+' stage='+stage)+
        ' --become --private-key /home/team3ops/.ssh/pj3_ansible '
        '--vault-password-file /home/team3ops/.config/pj3/vault-pass')
    response=subprocess.run(['ssh','-o','BatchMode=yes','-o','ConnectTimeout=10','bastion',command],
        capture_output=True,text=True,encoding='utf-8',timeout=75)
    if response.returncode:raise RuntimeError('서버 제어 실패: SSH/Ansible 연결 상태를 확인하세요.')
    body=json.loads(response.stdout[response.stdout.index('{'):])
    if body.get('failed'):raise RuntimeError(body.get('msg','서버 실행 거부'))
    return body['result']


def main(argv=None):
    parser=argparse.ArgumentParser(description='실서버 파이프라인 상태·TLS 점검·단계 실행')
    parser.add_argument('action',choices=['status','preflight','all','flow','downstream',*STAGES])
    args=parser.parse_args(argv)
    try:
        if args.action in {'flow','downstream'}:
            print(json.dumps(call('start',args.action),ensure_ascii=False,indent=2));return 0
        if args.action in {'status','preflight'}:
            print(json.dumps(call(args.action,'bronze'),ensure_ascii=False,indent=2));return 0
        stages=STAGES if args.action=='all' else (args.action,)
        for stage in stages:
            call('start',stage);print(stage+': 시작',flush=True)
            while True:
                time.sleep(10);state=call('status',stage)
                if state['worker_alive']:
                    progress=state.get('recent_progress',[])
                    if progress:print(json.dumps(progress[-1],ensure_ascii=False),flush=True)
                    continue
                result=state.get(stage+'-result.json');failure=state.get(stage+'-failure.json')
                if (not result or result['finished_at']<state.get('attempt_started_at','') or
                        (failure and failure['at']>result['finished_at'])):
                    raise RuntimeError(stage+': 실패. '+json.dumps(failure,ensure_ascii=False))
                print(json.dumps(result,ensure_ascii=False,indent=2),flush=True);break
        return 0
    except (RuntimeError,ValueError,subprocess.TimeoutExpired) as exc:
        print(str(exc));return 2


if __name__=='__main__':raise SystemExit(main())
