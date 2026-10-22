"""Install an immutable collector release and a disabled NiFi launcher."""
from ansible.module_utils.basic import AnsibleModule
import hashlib,json,os,pwd,subprocess,tarfile
from pathlib import Path


def main():
    m=AnsibleModule(argument_spec=dict(sha256=dict(required=True),commit=dict(required=True)))
    try:
        u=pwd.getpwnam('pj3-nifi');archive=Path('/var/tmp/pj3-collector-source.tar')
        assert hashlib.sha256(archive.read_bytes()).hexdigest()==m.params['sha256'],'ARCHIVE_HASH_MISMATCH'
        release=Path('/opt/pj3/danawa-collector/releases')/m.params['sha256']
        if not (release/'source-manifest.json').exists():
            release.mkdir(parents=True,exist_ok=True,mode=0o755)
            with tarfile.open(archive) as tar:
                for member in tar.getmembers():
                    assert (member.name=='Commerce_crawler' or member.name.startswith('Commerce_crawler/')) and '..' not in Path(member.name).parts and not member.issym() and not member.islnk(),'ARCHIVE_PATH_INVALID'
                tar.extractall(release,filter='data')
            for p in release.rglob('*'):p.chmod(0o755 if p.is_dir() else 0o644)
            (release/'source-manifest.json').write_text(json.dumps(dict(commit=m.params['commit'],sha256=m.params['sha256'])))
        root=Path('/var/lib/pj3/product-pipeline/nifi/collector')
        for p in [root,root/'config',root/'runs']:
            p.mkdir(parents=True,exist_ok=True,mode=0o700);os.chown(p,u.pw_uid,u.pw_gid)
        py=root/'venv/bin/python'
        def run(args,timeout=60):
            r=subprocess.run(args,user=u.pw_uid,group=u.pw_gid,extra_groups=[],capture_output=True,text=True,timeout=timeout)
            assert r.returncode==0,'DEPENDENCY_OR_PREFLIGHT_FAILED'
            return r.stdout
        if not py.exists():run(['/usr/bin/python3','-m','venv','--without-pip',str(root/'venv')])
        source=release/'Commerce_crawler'
        bootstrap='/var/lib/pj3/product-pipeline/nifi/venvs/128dbd37d879b397-base/bin/python'
        run([bootstrap,'-m','pip','--python',str(py),'install','--disable-pip-version-check','--timeout','15','--retries','1','pip','-r',str(source/'requirements-collector.txt')],timeout=150)
        control=root/'config/collector-control.json'
        config=dict(contract_version=1,enabled=False,source=str(source),source_commit=m.params['commit'],python=str(py),
            categories=str(source/'config/danawa_categories_71.json'),pagination_template=str(root/'config/danawa_pagination.local.json'),
            output=str(root/'runs'),workers=3,listing_interval=0,interval=1)
        if control.exists():assert json.loads(control.read_text())==config,'EXISTING_COLLECTOR_CONFIG_DIFFERS'
        else:control.write_text(json.dumps(config));control.chmod(0o600);os.chown(control,u.pw_uid,u.pw_gid)
        launcher=Path('/opt/pj3/danawa-collector/collector_launcher.py')
        checks=json.loads(run([str(py),str(launcher),'--check']))
        disabled=json.loads(run([str(py),str(launcher)]));assert disabled['status']=='DISABLED_NOT_STARTED','COLLECTOR_NOT_DISABLED'
        run([str(py),str(source/'run_collector.py'),'--help'])
        run([str(py),'-m','pip','check'])
        m.exit_json(changed=True,result=dict(status='INSTALLED_DISABLED',commit=m.params['commit'],source_sha256=m.params['sha256'],source=str(source),python=str(py),preflight=checks,guard=disabled,actual_collection_started=False))
    except Exception as e:m.fail_json(msg=str(e) if isinstance(e,AssertionError) else type(e).__name__)


if __name__=='__main__':main()
