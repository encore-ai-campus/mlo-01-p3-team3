"""Install an additional fixed production SSH command; retain verification tick."""
from ansible.module_utils.basic import AnsibleModule
import json,os,pwd,subprocess,hashlib,shutil
from pathlib import Path
from datetime import datetime,timezone

ROOT=Path('/var/lib/pj3/product-pipeline/spark/production-pending-20261019')
PY='/var/lib/pj3/product-pipeline/spark/venvs/128dbd37d879b397-spark/bin/python'
GATE=Path('/usr/local/libexec/pj3-pipeline-ssh-gate')
OLD='#!/bin/sh\n[ "$SSH_ORIGINAL_COMMAND" = "tick" ] || { echo REMOTE_COMMAND_DENIED; exit 64; }\nexec /usr/bin/sudo.ws -n -u pj3-spark -- /usr/local/libexec/pj3-pipeline-verify-tick\n'
NEW='''#!/bin/sh
case "$SSH_ORIGINAL_COMMAND" in
  tick) exec /usr/bin/sudo.ws -n -u pj3-spark -- /usr/local/libexec/pj3-pipeline-verify-tick ;;
  production-tick) exec /usr/bin/sudo.ws -n -u pj3-spark -- /usr/local/libexec/pj3-pipeline-production-tick ;;
  *) echo REMOTE_COMMAND_DENIED; exit 64 ;;
esac
'''
LAUNCHER='''import sys,json
from pathlib import Path
root=Path(ROOT_LITERAL)
sys.path.insert(0,str(root/'source'))
from product_pipeline.operations.server_flow import tick
r=tick(root,root.parent,sys.executable)
print(json.dumps(r))
raise SystemExit(2 if r['status']=='RECOVERY_REQUIRED' else 0)
'''.replace('ROOT_LITERAL',repr(str(ROOT)))


def main():
    m=AnsibleModule(argument_spec={})
    try:
        assert not GATE.is_symlink() and GATE.read_text() in (OLD,NEW),'SSH_GATE_CHANGED'
        u=pwd.getpwnam('pj3-spark')
        backup=ROOT/'nifi-control-backups';backup.mkdir(exist_ok=True,mode=0o700)
        stamp=datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')
        def write(path,body,mode=0o755):
            p=Path(path);assert not p.is_symlink(),'SYMLINK_FORBIDDEN'
            if p.exists() and p.read_text()!=body:
                saved=backup/(p.name+'.'+stamp);saved.write_bytes(p.read_bytes());saved.chmod(0o600)
            temp=p.with_name(p.name+'.nifi-new');temp.write_text(body);temp.chmod(mode);os.chown(temp,0,0);temp.replace(p)
        script='/opt/pj3/product-pipeline/launchers/production_tick.py'
        write(script,LAUNCHER,0o644)
        write('/usr/local/libexec/pj3-pipeline-production-tick',
            '#!/bin/sh\nexec /usr/bin/env -i PATH=/usr/bin:/bin HOME='+str(ROOT)+' '+PY+' '+script+'\n')
        sudo=Path('/etc/sudoers.d/pj3-pipeline-trigger-production')
        candidate=sudo.with_name(sudo.name+'.check')
        candidate.write_text('pj3-pipeline-trigger ALL=(pj3-spark) NOPASSWD: /usr/local/libexec/pj3-pipeline-production-tick\n');candidate.chmod(0o440)
        subprocess.run([shutil.which('visudo.ws') or shutil.which('visudo'),'-cf',str(candidate)],check=True,capture_output=True)
        candidate.replace(sudo)
        write(GATE,NEW)
        config=ROOT/'nifi-control.json'
        body=dict(contract_version=1,enabled=True,root=str(ROOT))
        if config.exists():assert json.loads(config.read_text())==body,'CONTROL_CONFIG_DIFFERS'
        else:config.write_text(json.dumps(body));config.chmod(0o600);os.chown(config,u.pw_uid,u.pw_gid)
        lock=ROOT/'controller.lock'
        if not lock.exists():lock.touch(mode=0o600)
        assert not lock.is_symlink(),'LOCK_SYMLINK_FORBIDDEN'
        os.chown(lock,u.pw_uid,u.pw_gid);lock.chmod(0o600)
        for name in ('worker.pid','flow-attempt.json','flow.private.log'):
            p=ROOT/name
            if p.exists():
                assert not p.is_symlink(),'CONTROL_FILE_SYMLINK_FORBIDDEN'
                os.chown(p,u.pw_uid,u.pw_gid)
        m.exit_json(changed=True,result=dict(status='FIXED_PRODUCTION_TRIGGER_INSTALLED',existing_tick_preserved=True,gate_sha256=hashlib.sha256(NEW.encode()).hexdigest()))
    except Exception as e:m.fail_json(msg=str(e) if isinstance(e,AssertionError) else type(e).__name__)


if __name__=='__main__':main()
