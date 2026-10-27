"""Operator-run user-owned runtime setup. No DB writes, services, or cluster changes."""
import argparse
import hashlib
import importlib.metadata
import json
from pathlib import Path
import subprocess
import sys
import tarfile
import tempfile
import urllib.request
import venv
import zipfile

ARCHIVE_SHA256 = 'b2e2a8d97f909f79e21f170404d1af7e5b48c532b089a59cd041900104f433d4'
RELEASE_ID = 'd8088a1a450c6900c29c9f962af3c23a74507c2ce2eb6b55059effbf472fa0e8'
PIP_VERSION = '26.2.1'


def verify(source):
    manifest = json.loads((source/'release.json').read_text())
    if manifest.get('release_id') != RELEASE_ID: raise RuntimeError('unexpected release')
    for name, proof in manifest['files'].items():
        path = (source/name).resolve()
        if not path.is_relative_to(source.resolve()) or path.is_symlink(): raise RuntimeError('unsafe path')
        data = path.read_bytes()
        if len(data) != proof['bytes'] or hashlib.sha256(data).hexdigest() != proof['sha256']:
            raise RuntimeError('release checksum mismatch: '+name)


def command(args, **kwargs):
    subprocess.run([str(a) for a in args], check=True, **kwargs)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=Path.home()/'product-pipeline')
    parser.add_argument('--base-only', action='store_true', help='ingest/publication only, no Spark libraries')
    args = parser.parse_args()
    if not args.base_only:
        parser.error('legacy Spark 4.1.3/Delta 4.1.0 profile failed server validation; '
                     'use --base-only for this old source bundle and see garage_v241_profile.md for the new release')
    if sys.version_info[:2] != (3,14): parser.error('this server profile requires Python 3.14')
    root = args.root.resolve(); archive = root/'incoming/source-5533c0a.tar.gz'
    if hashlib.sha256(archive.read_bytes()).hexdigest() != ARCHIVE_SHA256: raise RuntimeError('archive changed')
    release = root/'releases/5533c0a'
    release.parent.mkdir(parents=True, exist_ok=True)
    if not release.exists():
        with tempfile.TemporaryDirectory(prefix='.unpack-', dir=release.parent) as temporary:
            unpack = Path(temporary)/'source'; unpack.mkdir()
            with tarfile.open(archive,'r:gz') as stream:
                members = stream.getmembers()
                if any(not m.isfile() or Path(m.name).is_absolute() or '..' in Path(m.name).parts for m in members):
                    raise RuntimeError('unsafe archive')
                stream.extractall(unpack, filter='data')
            verify(unpack); unpack.rename(release)
    verify(release)
    profile = 'py314-base' if args.base_only else 'py314-spark413-delta410'
    environment = root/'venvs'/profile; environment.parent.mkdir(parents=True, exist_ok=True)
    marker = environment/'pipeline-profile.json'
    expected = dict(release_id=RELEASE_ID, python='3.14', profile=profile)
    if environment.exists() and not marker.exists(): raise RuntimeError('unidentified existing venv; use another root')
    if marker.exists() and json.loads(marker.read_text()) != expected: raise RuntimeError('venv profile differs')
    if not environment.exists():
        venv.EnvBuilder(with_pip=False).create(environment)
        marker.write_text(json.dumps(expected)+'\n')
    python = environment/'bin/python'
    bootstrap = root/'bootstrap'; bootstrap.mkdir(exist_ok=True)
    pip = bootstrap/f'pip-{PIP_VERSION}.pyz'
    if not pip.exists():
        url = f'https://bootstrap.pypa.io/pip/zipapp/pip-{PIP_VERSION}.pyz'
        with urllib.request.urlopen(url, timeout=60) as response: data = response.read(16*1024*1024+1)
        if not 10000 < len(data) <= 16*1024*1024: raise RuntimeError('pip bootstrap size invalid')
        with tempfile.NamedTemporaryFile(dir=bootstrap, delete=False) as temporary:
            temporary.write(data); staged = Path(temporary.name)
        try:
            with zipfile.ZipFile(staged) as packed:
                if packed.testzip() is not None: raise RuntimeError('pip archive damaged')
            staged.rename(pip)
        finally:
            if staged.exists(): staged.unlink()
    command([python,pip,'install','--disable-pip-version-check',f'pip=={PIP_VERSION}','setuptools>=75'])
    command([python,'-m','pip','install','--disable-pip-version-check',
        '-r',release/'requirements.lock.txt','-r',release/'requirements-publication.lock.txt'])
    if not args.base_only:
        command([python,'-m','pip','install','--disable-pip-version-check','pyspark==4.1.3','delta-spark==4.1.0'])
    command([python,'-m','pip','install','--no-deps','--no-build-isolation','-e',release])
    command([python,'-m','pip','check'])
    command([python,release/'run_pipeline.py','--help'])
    command([python,release/'run_operations.py','--help'])
    audit = root/'audit'; audit.mkdir(exist_ok=True)
    freeze = subprocess.check_output([str(python),'-m','pip','freeze'], text=True)
    (audit/(profile+'-freeze.txt')).write_text(freeze)
    result = dict(status='RUNTIME_INSTALLED_CLI_HELP_VERIFIED',profile=profile,python=str(python),
        release=str(release),source_commit='5533c0a',spark_cluster_modified=False,
        database_data_written=False,service_activated=False,
        pip_bootstrap_sha256=hashlib.sha256(pip.read_bytes()).hexdigest(),
        spark_delta_profile_runtime_test_required=not args.base_only)
    (audit/(profile+'-setup.json')).write_text(json.dumps(result,indent=2)+'\n')
    print(json.dumps(result)); return 0


if __name__=='__main__': raise SystemExit(main())
