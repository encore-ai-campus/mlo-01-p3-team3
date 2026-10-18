"""Portable source-only release with exact file hashes. No venv, data or secrets."""
from pathlib import Path
import shutil
import tempfile

from ..contracts import digest
from ..errors import Conflict,PipelineError
from ..files import atomic_json,confined,read_json,sha_file,sync_dir

ALLOWED={'.py','.md','.txt','.toml','.json','.sql'}
EXCLUDE={'.runtime','.venv','.git','__pycache__','spark-warehouse'}


def build(source,destination):
    source=Path(source).resolve();destination=Path(destination).resolve()
    if not (source/'pyproject.toml').is_file() or not (source/'product_pipeline').is_dir():
        raise PipelineError('RELEASE_PIPELINE_SOURCE_REQUIRED')
    if destination.exists():raise PipelineError('RELEASE_DESTINATION_EXISTS')
    if destination.is_relative_to(source) and '.runtime' not in destination.relative_to(source).parts:
        raise PipelineError('RELEASE_OUTPUT_MUST_BE_SEPARATE')
    destination.parent.mkdir(parents=True,exist_ok=True)
    with tempfile.TemporaryDirectory(prefix='.release-',dir=destination.parent) as tmp:
        out=Path(tmp)/'bundle';out.mkdir();proofs={}
        for file in sorted(source.rglob('*')):
            parts=file.relative_to(source).parts
            if any(p in EXCLUDE or p.endswith('.egg-info') for p in parts):continue
            top=parts[0]
            included=(top in {'product_pipeline','tests','docs','sql'} or
                      (top=='config' and file.name.endswith('.example.json')) or
                      (len(parts)==1 and (file.name in {'README.md','pyproject.toml'} or
                          (file.name.startswith('run_') and file.suffix=='.py') or
                          (file.name.startswith('requirements') and file.suffix=='.txt'))))
            if not included:continue
            if file.is_symlink():raise PipelineError('RELEASE_SYMLINK_FORBIDDEN')
            if not file.is_file() or file.suffix not in ALLOWED or file.name.endswith('.local.json') or file.name.startswith('.env'):continue
            name=file.relative_to(source).as_posix();target=confined(out,name);target.parent.mkdir(parents=True,exist_ok=True)
            before=sha_file(file);shutil.copyfile(file,target)
            if sha_file(file)!=before or sha_file(target)!=before:raise Conflict('RELEASE_SOURCE_CHANGED')
            proofs[name]=dict(sha256=before,bytes=file.stat().st_size)
        manifest=dict(contract_version=1,kind='PIPELINE_SOURCE_RELEASE',files=proofs,
                      contains_data=False,contains_venv=False,automatic_install=False)
        manifest['release_id']=digest(manifest);atomic_json(out/'release.json',manifest)
        out.rename(destination);sync_dir(destination.parent)
    return dict(status='RELEASE_BUILT',release_id=manifest['release_id'],files=len(proofs),bytes=sum(v['bytes'] for v in proofs.values()))


def verify(bundle):
    bundle=Path(bundle).resolve();value=read_json(bundle/'release.json')
    if value.get('kind')!='PIPELINE_SOURCE_RELEASE' or value.get('release_id')!=digest({k:v for k,v in value.items() if k!='release_id'}):
        raise Conflict('RELEASE_MANIFEST_CHANGED')
    for name,proof in value['files'].items():
        path=confined(bundle,name)
        if path.is_symlink() or sha_file(path)!=proof['sha256'] or path.stat().st_size!=proof['bytes']:
            raise Conflict('RELEASE_FILE_CHANGED')
    return dict(status='RELEASE_VERIFIED',release_id=value['release_id'],files=len(value['files']))
