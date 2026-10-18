"""One-host, service-account runtime deployment. No daemon or database changes."""
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import shutil
import subprocess
import tarfile
import tempfile
import time
import urllib.request


def inspect_archive(archive, expected_sha256, expected_release_id):
    archive = Path(archive)
    if archive.stat().st_size > 8 * 1024 * 1024:
        raise ValueError('SOURCE_ARCHIVE_TOO_LARGE')
    if hashlib.sha256(archive.read_bytes()).hexdigest() != expected_sha256:
        raise ValueError('SOURCE_ARCHIVE_CHECKSUM_CHANGED')
    files = {}
    with tarfile.open(archive, 'r:gz') as stream:
        total = 0
        for member in stream.getmembers():
            name = member.name
            path = PurePosixPath(name)
            total += member.size
            if (not member.isfile() or path.is_absolute() or '..' in path.parts
                    or '\\' in name or str(path) != name or name in files
                    or member.size > 4 * 1024 * 1024 or total > 16 * 1024 * 1024
                    or len(files) >= 2000):
                raise ValueError('UNSAFE_SOURCE_ARCHIVE')
            files[name] = stream.extractfile(member).read()
    manifest = json.loads(files['release.json'])
    body = {k: v for k, v in manifest.items() if k != 'release_id'}
    digest = hashlib.sha256(json.dumps(body, ensure_ascii=False, sort_keys=True,
                                      separators=(',', ':'), allow_nan=False).encode()).hexdigest()
    if (manifest.get('kind') != 'PIPELINE_SOURCE_RELEASE' or digest != expected_release_id
            or manifest.get('release_id') != digest
            or set(files) != set(manifest['files']) | {'release.json'}):
        raise ValueError('SOURCE_MANIFEST_CHANGED')
    for name, proof in manifest['files'].items():
        data = files[name]
        if len(data) != proof['bytes'] or hashlib.sha256(data).hexdigest() != proof['sha256']:
            raise ValueError('SOURCE_FILE_CHANGED')
    return manifest, files


def directory(path, uid=0, gid=0, mode=0o755):
    path = Path(path)
    if path.is_symlink():
        raise ValueError('DEPLOY_PATH_SYMLINK_FORBIDDEN')
    if path.exists():
        info = path.stat()
        if not path.is_dir() or info.st_uid != uid or (info.st_mode & 0o777) != mode:
            raise ValueError('EXISTING_DEPLOY_DIRECTORY_DIFFERS')
    else:
        path.mkdir(mode=mode)
        os.chown(path, uid, gid)
    return path


def deploy(params):
    import pwd
    role = params['role']
    if os.uname().nodename.split('.')[0] != params['expected_host']:
        raise ValueError('DEPLOY_HOST_MISMATCH')
    if os.geteuid() != 0:
        raise ValueError('DEPLOY_ROOT_REQUIRED')
    identity = pwd.getpwnam('pj3-' + role)
    if identity.pw_uid == 0:
        raise ValueError('SERVICE_IDENTITY_REQUIRED')
    if shutil.disk_usage('/var/lib/pj3').free < 4 * 1024**3:
        raise ValueError('RUNTIME_DISK_HEADROOM_REQUIRED')
    manifest, files = inspect_archive(params['archive'], params['archive_sha256'], params['release_id'])
    release_id = manifest['release_id']
    root = directory('/opt/pj3/product-pipeline')
    releases = directory(root/'releases')
    release = releases/release_id
    if release.exists():
        directory(release)
        for name, content in files.items():
            candidate = release/name
            if (candidate.is_symlink() or candidate.stat().st_uid != 0
                    or candidate.stat().st_mode & 0o022 or candidate.read_bytes() != content):
                raise ValueError('EXISTING_SOURCE_RELEASE_CHANGED')
    else:
        with tempfile.TemporaryDirectory(prefix='.release-', dir=releases) as temporary:
            staged = Path(temporary)/'source'; staged.mkdir(mode=0o755)
            for name, content in files.items():
                target = staged/name; target.parent.mkdir(parents=True, exist_ok=True)
                with target.open('xb') as stream:
                    stream.write(content)
                target.chmod(0o644)
            staged.rename(release)
    state_root = directory('/var/lib/pj3/product-pipeline')
    state = directory(state_root/role, identity.pw_uid, identity.pw_gid, 0o700)
    for name in ['venvs', 'tmp', 'audit', 'state']:
        directory(state/name, identity.pw_uid, identity.pw_gid, 0o700)
    profile = 'spark' if params['with_spark'] else 'base'
    venv = state/'venvs'/(release_id[:16]+'-'+profile)
    receipts = directory(root/'deployments')
    receipt = receipts/(role+'-'+release_id[:16]+'-'+profile+'.json')
    expected = dict(release_id=release_id, role=role, profile=profile,
                    source=str(release), python=str(venv/'bin/python'))
    started = time.monotonic()

    def progress(phase):
        record = dict(expected, phase=phase, duration_ms=round((time.monotonic()-started)*1000))
        temporary = receipt.with_suffix('.tmp'); temporary.write_text(json.dumps(record)+'\n')
        temporary.chmod(0o644); temporary.replace(receipt)

    env = dict(PATH='/usr/bin:/bin', HOME=str(state), TMPDIR=str(state/'tmp'),
               LANG='C.UTF-8', PYTHONNOUSERSITE='1', PIP_CONFIG_FILE='/dev/null',
               PIP_INDEX_URL='https://pypi.org/simple', PIP_DISABLE_PIP_VERSION_CHECK='1')

    def command(args, timeout=240):
        output = subprocess.run([str(a) for a in args], cwd=state, env=env,
            user=identity.pw_uid, group=identity.pw_gid, extra_groups=[],
            capture_output=True, text=True, timeout=timeout)
        if output.returncode:
            # Keep install diagnostics private; no credentials are supplied here.
            log = state/'audit'/(release_id[:16]+'-'+profile+'-failure.log')
            log.write_text(output.stdout+'\n'+output.stderr); log.chmod(0o600)
            os.chown(log, identity.pw_uid, identity.pw_gid)
            raise ValueError('RUNTIME_COMMAND_FAILED_SEE_PRIVATE_AUDIT')
        return output.stdout

    progress('PREFLIGHT')
    if command(['/usr/bin/python3', '-c', 'import sys;print("%d.%d"%sys.version_info[:2])']).strip() != '3.14':
        raise ValueError('SERVER_PYTHON_PROFILE_MISMATCH')
    if venv.exists() and (venv.is_symlink() or venv.stat().st_uid != identity.pw_uid):
        raise ValueError('EXISTING_VENV_OWNER_MISMATCH')
    progress('CREATING_VENV')
    command(['/usr/bin/python3', '-c',
        'import sys,venv;venv.EnvBuilder(with_pip=False).create(sys.argv[1])', venv])
    python = venv/'bin/python'
    bootstrap = directory(root/'bootstrap')
    pip = bootstrap/'pip-26.2.1.pyz'
    if not pip.exists():
        with urllib.request.urlopen('https://bootstrap.pypa.io/pip/zipapp/pip-26.2.1.pyz', timeout=30) as response:
            data = response.read(4 * 1024 * 1024 + 1)
        if not 10000 < len(data) <= 4 * 1024 * 1024:
            raise ValueError('PIP_BOOTSTRAP_SIZE_INVALID')
        with pip.open('xb') as stream:
            stream.write(data)
        pip.chmod(0o644)
    if pip.is_symlink() or pip.stat().st_uid != 0 or pip.stat().st_mode & 0o022:
        raise ValueError('PIP_BOOTSTRAP_OWNERSHIP_INVALID')
    progress('INSTALLING_BASE')
    command([python, pip, 'install', '--only-binary=:all:', 'pip==26.2.1', 'setuptools>=75', 'wheel'])
    command([python, '-m', 'pip', 'install', '--only-binary=:all:', '-r', release/'requirements.lock.txt',
             '-r', release/'requirements-publication.lock.txt'])
    if params['with_spark']:
        progress('INSTALLING_SPARK_CLIENT')
        command([python, '-m', 'pip', 'install', '-r', release/'requirements-spark-server.txt'], timeout=1000)
    progress('INSTALLING_PIPELINE')
    build = state/'tmp'/('build-'+release_id[:16]+'-'+profile)
    if build.exists():
        if build.is_symlink() or build.stat().st_uid != identity.pw_uid:
            raise ValueError('PRIOR_BUILD_DIRECTORY_REQUIRES_REVIEW')
        for name, content in files.items():
            if (build/name).is_symlink() or (build/name).read_bytes() != content:
                raise ValueError('PRIOR_BUILD_SOURCE_CHANGED')
    else:
        command([python, '-c', 'import shutil,sys;shutil.copytree(sys.argv[1],sys.argv[2])', release, build])
    command([python, '-m', 'pip', 'install', '--force-reinstall', '--no-deps', '--no-build-isolation', build])
    progress('VERIFYING_RUNTIME')
    command([python, '-m', 'pip', 'check'])
    imports = 'import boto3,pymongo,confluent_kafka,pymysql,PIL,product_pipeline'
    if params['with_spark']:
        imports += ',pyspark,delta;assert pyspark.__version__=="4.1.3"'
    command([python, '-c', imports])
    for entry in ['danawa-pipeline', 'danawa-operations', 'danawa-publication']:
        command([venv/'bin'/entry, '--help'])
    if params['with_spark']:
        for entry in ['danawa-bronze', 'danawa-silver', 'danawa-gold']:
            command([venv/'bin'/entry, '--help'])
    freeze = command([python, '-m', 'pip', 'freeze'])
    frozen = state/'audit'/(release_id[:16]+'-'+profile+'-freeze.txt')
    frozen.write_text(freeze); frozen.chmod(0o600); os.chown(frozen, identity.pw_uid, identity.pw_gid)
    progress('RUNTIME_VERIFIED')
    result = dict(expected, status='RUNTIME_VERIFIED', receipt=str(receipt), freeze=str(frozen),
                  service_activated=False, database_written=False,
                  duration_ms=round((time.monotonic()-started)*1000))
    receipt.write_text(json.dumps(result)+'\n'); receipt.chmod(0o644)
    return result


def main():
    from ansible.module_utils.basic import AnsibleModule
    module = AnsibleModule(argument_spec=dict(role=dict(required=True, choices=['nifi', 'spark']),
        expected_host=dict(required=True), archive=dict(required=True), archive_sha256=dict(required=True),
        release_id=dict(required=True), with_spark=dict(type='bool', default=False)))
    try:
        module.exit_json(changed=True, result=deploy(module.params))
    except Exception as exc:
        code = str(exc) if isinstance(exc, ValueError) else type(exc).__name__
        module.fail_json(msg=code)


if __name__ == '__main__':
    main()
