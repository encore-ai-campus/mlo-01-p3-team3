"""Byte-preserving state relocation. No source deletion or lease reset.

Requires stopped producers plus externally supplied backup/restore attestations.
Logical namespaces and fingerprints stay identical; endpoints/credentials are
injected at deployment, NOT rewritten into immutable registration/event files.
"""
from pathlib import Path
from contextlib import ExitStack
import os
import shutil
import tempfile

from ..contracts import digest, now
from ..errors import Conflict, PipelineError
from ..files import atomic_json, confined, lock, read_json, sha_file, sync_dir

DENIED={'.venv','.git','__pycache__'}
SECRET_SUFFIX={'.key','.pem','.pfx','.p12'}


def _receipt(value, expected):
    if (not isinstance(value,dict) or value.get('status')!=expected or not value.get('evidence_ref') or
        not value.get('logical_target') or not isinstance(value.get('services'),dict) or not value['services']):
        raise PipelineError('MIGRATION_ATTESTATION_REQUIRED')
    for name, proof in value['services'].items():
        if (name not in {'mongo','mysql','objects','kafka','delta'} or not proof.get('backup_id') or
            proof['backup_id'].startswith('replace-with-') or not isinstance(proof.get('identity_digest'),str) or
            len(proof['identity_digest'])!=64 or any(c not in '0123456789abcdef' for c in proof['identity_digest'])):
            raise PipelineError('MIGRATION_EXTERNAL_PROOF_REQUIRED')


def pack(roots,destination,stopped):
    with ExitStack() as stack:
        for source in roots.values():
            root=Path(source)
            if root.is_symlink() or (root/'checkpoint_state.json').exists():
                raise PipelineError('MIGRATION_PIPELINE_STATE_ONLY')
            for name in sorted(root.rglob('*.lock')):
                if name.name in {'worker.lock','queue.lock','writer.lock','drain.lock','mode.lock','discovery.lock'}:
                    stack.enter_context(lock(name,existing=True))
        return _pack(roots,destination,stopped)


def _pack(roots, destination, stopped):
    _receipt(stopped,'QUIESCED_AND_BACKED_UP')
    destination=Path(destination).resolve()
    if destination.exists():raise Conflict('MIGRATION_DESTINATION_EXISTS')
    resolved={name:Path(path).resolve() for name,path in roots.items()}
    if not resolved or any(not name.isidentifier() for name in resolved):raise PipelineError('MIGRATION_ROOTS_REQUIRED')
    for path in resolved.values():
        if not path.is_dir() or destination.is_relative_to(path) or path.is_relative_to(destination):
            raise PipelineError('MIGRATION_PATH_OVERLAP')
    destination.parent.mkdir(parents=True,exist_ok=True)
    with tempfile.TemporaryDirectory(prefix='.migration-',dir=destination.parent) as temporary:
        build=Path(temporary)/'bundle';build.mkdir(mode=0o700);manifest={}
        # Locks serialize packaging. A frozen external owner receipt remains mandatory:
        # these locks cannot stop arbitrary programs or prove cross-host quiescence.
        for name,root in resolved.items():
            for source in sorted(root.rglob('*')):
                relative=source.relative_to(root)
                if any(part in DENIED for part in relative.parts):continue
                if source.is_symlink():raise PipelineError('MIGRATION_SYMLINK_FORBIDDEN')
                if not source.is_file() or source.suffix=='.lock':continue
                if source.suffix.lower() in SECRET_SUFFIX or source.name.startswith('.env') or source.name in {'admin.json','cluster.key'}:
                    raise PipelineError('MIGRATION_SECRETS_REQUIRE_SEPARATE_TRANSFER')
                if source.suffix=='.json':
                    value=read_json(source)
                    if isinstance(value,dict):
                        # runner archives the FIRST registration after acquiring a lease.
                        # That immutable historical copy is not the current lease owner.
                        historical_registration=(relative.parent.name=='registrations' and
                            value.get('observation_key')==source.stem and 'document' in value and 'event' in value)
                        if value.get('status') in {'RUNNING','running'} or (value.get('lease_owner') and not historical_registration):
                            raise PipelineError('MIGRATION_ACTIVE_STATE_PRESENT')
                key=name+'/'+relative.as_posix();target=confined(build,key)
                target.parent.mkdir(parents=True,exist_ok=True)
                before=sha_file(source);shutil.copyfile(source,target)
                if sha_file(source)!=before or sha_file(target)!=before:raise Conflict('MIGRATION_SOURCE_CHANGED')
                manifest[key]=dict(sha256=before,bytes=source.stat().st_size)
        body=dict(contract_version=1,kind='PIPELINE_STATE_MIGRATION',logical_target=stopped['logical_target'],
                  files=manifest,roots=sorted(resolved),external_backups=stopped['services'],quiescence=stopped)
        body['bundle_id']=digest(body)
        atomic_json(build/'migration.json',body)
        build.rename(destination);sync_dir(destination.parent)
    return body


def verify(bundle):
    bundle=Path(bundle).resolve();value=read_json(bundle/'migration.json')
    if value.get('kind')!='PIPELINE_STATE_MIGRATION' or value.get('bundle_id')!=digest({k:v for k,v in value.items() if k!='bundle_id'}):
        raise Conflict('MIGRATION_MANIFEST_INVALID')
    for name,proof in value['files'].items():
        p=confined(bundle,name)
        if p.is_symlink() or p.stat().st_size!=proof['bytes'] or sha_file(p)!=proof['sha256']:
            raise Conflict('MIGRATION_FILE_CHANGED')
    return value


def restore(bundle, destination, restored):
    _receipt(restored,'RESTORED_AND_VERIFIED')
    value=verify(bundle);destination=Path(destination).resolve();bundle=Path(bundle).resolve()
    if restored['logical_target']!=value['logical_target'] or restored['services']!=value['external_backups']:
        raise Conflict('MIGRATION_TARGET_IDENTITY_MISMATCH')
    if destination.is_relative_to(bundle) or bundle.is_relative_to(destination):raise PipelineError('MIGRATION_PATH_OVERLAP')
    if destination.exists():
        receipt=read_json(destination/'restored.json')
        if receipt.get('bundle_id')!=value['bundle_id']:raise Conflict('MIGRATION_RESTORE_CONFLICT')
        for name,proof in value['files'].items():
            if sha_file(confined(destination,name))!=proof['sha256']:raise Conflict('MIGRATION_RESTORED_FILE_CHANGED')
        return receipt
    destination.parent.mkdir(parents=True,exist_ok=True)
    with tempfile.TemporaryDirectory(prefix='.restore-',dir=destination.parent) as temporary:
        build=Path(temporary)/'state';build.mkdir(mode=0o700)
        for name,proof in value['files'].items():
            target=confined(build,name);target.parent.mkdir(parents=True,exist_ok=True)
            shutil.copyfile(confined(bundle,name),target)
            if sha_file(target)!=proof['sha256']:raise Conflict('MIGRATION_RESTORE_CHANGED')
        verify(bundle)
        receipt=dict(bundle_id=value['bundle_id'],logical_target=value['logical_target'],restored_at=now(),
                     status='RESTORED_REQUIRES_DEPLOYMENT_PREFLIGHT',automatic_start=False)
        atomic_json(build/'restored.json',receipt);build.rename(destination);sync_dir(destination.parent)
    return receipt
