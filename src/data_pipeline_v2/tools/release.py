"""Build/verify a source-only release; never collect environments, secrets or data."""
import argparse
import hashlib
import json
from pathlib import Path, PurePosixPath
import subprocess
import zipfile


ROOT=Path(__file__).resolve().parents[1]


def checksum(value):return hashlib.sha256(value).hexdigest()


def build(output, root=ROOT):
    root=Path(root);files={}
    for directory in ('pipeline_v2','tools','docs'):
        for path in sorted((root/directory).rglob('*')):
            if path.is_file() and not path.is_symlink() and path.suffix in {'.py','.md'}:
                files['v2/'+path.relative_to(root).as_posix()]=path.read_bytes()
    for name in ('README.md','pyproject.toml','run_pipeline.py','config/local.example.json','config/server.example.json',
                 'config/batch.local.example.json','config/batch.server.example.json'):
        files['v2/'+name]=(root/name).read_bytes()
    # Ship the exact parser/transport dependency, including its transitive imports.
    legacy=root.parent/'data_pipeline'
    for path in sorted((legacy/'product_pipeline').rglob('*.py')):
        if '__pycache__' not in path.parts and not path.is_symlink():
            files['legacy/'+path.relative_to(legacy).as_posix()]=path.read_bytes()
    files['legacy/pyproject.toml']=(legacy/'pyproject.toml').read_bytes()
    for name in ('requirements.lock.txt','requirements-bronze.lock.txt','requirements-publication.lock.txt'):
        files['legacy/'+name]=(legacy/name).read_bytes()
    try:
        revision=subprocess.check_output(['git','rev-parse','HEAD'],cwd=root,text=True,stderr=subprocess.DEVNULL).strip()
    except (OSError,subprocess.CalledProcessError):revision=None
    manifest=dict(contract_version=2,base_git_commit=revision,
        provenance='Working tree source bytes; base commit alone does not identify this release.',
        files={name:checksum(body) for name,body in sorted(files.items())})
    with zipfile.ZipFile(output,'x',compression=zipfile.ZIP_DEFLATED) as archive:
        for name,body in files.items():archive.writestr(name,body)
        archive.writestr('manifest.json',json.dumps(manifest,ensure_ascii=False,sort_keys=True,indent=2))
    return verify(output)


def verify(path):
    with zipfile.ZipFile(path) as archive:
        names=archive.namelist()
        if len(names)!=len(set(names)):raise ValueError('RELEASE_DUPLICATE_PATH')
        if any(PurePosixPath(n).is_absolute() or '..' in PurePosixPath(n).parts or '\\' in n or ':' in n for n in names):
            raise ValueError('RELEASE_UNSAFE_PATH')
        manifest=json.loads(archive.read('manifest.json'))
        if set(names)!={'manifest.json',*manifest['files']}:raise ValueError('RELEASE_MEMBERSHIP_CHANGED')
        for name,expected in manifest['files'].items():
            if checksum(archive.read(name))!=expected:raise ValueError('RELEASE_CONTENT_CHANGED')
        return dict(status='SOURCE_RELEASE_VERIFIED',files=len(manifest['files']),manifest_hash=checksum(archive.read('manifest.json')))


if __name__=='__main__':
    parser=argparse.ArgumentParser(description='비밀값·실행 데이터 없는 소스 이전 묶음')
    parser.add_argument('command',choices=['build','verify']);parser.add_argument('archive',type=Path)
    args=parser.parse_args()
    print(json.dumps(build(args.archive) if args.command=='build' else verify(args.archive)))
