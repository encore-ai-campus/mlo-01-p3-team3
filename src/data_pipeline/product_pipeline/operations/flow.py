"""Sealed, single-host pipeline runs. Receipts authorize reuse, never a cursor alone."""
from pathlib import Path
import time
from uuid import uuid4

from ..contracts import digest, now, utc
from ..errors import Conflict, PipelineError
from ..files import atomic_json, lock, read_json, sha_file

STAGES = ('INGEST', 'BRONZE', 'SILVER', 'GOLD', 'PREPARE', 'PUBLISH', 'RECONCILE')
REQUIRED = {'pipeline_config', 'bronze_config', 'silver_config', 'silver_inputs',
            'silver_registration', 'gold_config', 'gold_inputs', 'gold_registration',
            'guard_hashes', 'mysql_config'}


class WorkerUnconfirmed(KeyboardInterrupt):
    """Retain both flow and scheduler RUNNING until all child writers are fenced."""


def seal(value):
    return {**value, 'content_hash': digest(value)}


def verified(value):
    if not isinstance(value, dict) or value.get('content_hash') != digest(
            {k: v for k, v in value.items() if k != 'content_hash'}):
        raise Conflict('FLOW_PROOF_CHANGED')
    return value


def prepare(config, output):
    """Freeze file references; no service connection or source mutation."""
    from ..snapshot import verify
    config = Path(config).resolve(); source = read_json(config)
    if (source.get('contract_version') != 1 or source.get('mode') != 'live'
            or not isinstance(source.get('target'), str) or not source['target']
            or source['target'].startswith('replace-with-') or set(source.get('files', {})) != REQUIRED):
        raise PipelineError('FLOW_CONFIG_REQUIRED')
    utc(source['as_of'])
    def path(name):
        p = Path(name)
        return (config.parent / p).resolve() if not p.is_absolute() else p.resolve()
    snapshot = path(source['snapshot']); manifest = verify(snapshot)
    files = {}
    for key, name in source['files'].items():
        p = path(name)
        if p.stat().st_size > 32 * 1024 * 1024: raise PipelineError('FLOW_INPUT_TOO_LARGE')
        # Copies are NOT fabricated approval/registration inputs.
        files[key] = dict(path=str(p), sha256=sha_file(p))
    output = Path(output).resolve()
    if output.exists(): raise PipelineError('FLOW_OUTPUT_EXISTS')
    if snapshot.is_relative_to(output) or output.is_relative_to(snapshot) or any(
            Path(v['path']).is_relative_to(output) for v in files.values()):
        raise PipelineError('FLOW_STATE_NOT_SEPARATE')
    value = seal(dict(contract_version=1, kind='PIPELINE_FLOW', mode='live', target=source['target'],
        as_of=utc(source['as_of']), snapshot=str(snapshot), snapshot_id=manifest['snapshot_id'],
        files=files, stages=list(STAGES)))
    from .flow_runtime import settings
    settings(value)  # Validate targets/owner exports before creating runnable state.
    output.mkdir(parents=True)
    atomic_json(output / 'flow.json', value)
    return dict(status='FLOW_PREPARED', flow_id=value['content_hash'], state=str(output))


def load(root):
    from ..snapshot import verify
    root = Path(root).resolve(); plan = verified(read_json(root / 'flow.json'))
    if plan.get('kind') != 'PIPELINE_FLOW' or plan.get('stages') != list(STAGES):
        raise Conflict('FLOW_CONTRACT_CHANGED')
    if set(plan.get('files', {})) != REQUIRED: raise Conflict('FLOW_INPUT_SET_CHANGED')
    for ref in plan['files'].values():
        if sha_file(ref['path']) != ref['sha256']: raise Conflict('FLOW_INPUT_CHANGED')
    if verify(plan['snapshot'])['snapshot_id'] != plan['snapshot_id']:
        raise Conflict('FLOW_SNAPSHOT_CHANGED')
    return plan


def run(root, executor=None, *, hook=None):
    """Each executor returns a small proof and checksum-bound artifact references.

    A failed process is retried with the same context. Crash/RUNNING requires
    explicit fencing evidence; existing downstream transactions remain idempotent.
    """
    root = Path(root).resolve()
    if executor is None:
        from .flow_runtime import execute
        executor = execute
    with lock(root / 'flow.lock'):
        plan = load(root); outputs = {}; previous = plan['content_hash']; reused = 0
        for stage in STAGES:
            proof_path = root / 'receipts' / (stage + '.json')
            if proof_path.exists():
                receipt = verified(read_json(proof_path))
                if (receipt['flow_id'] != plan['content_hash'] or receipt['stage'] != stage
                        or receipt['previous_hash'] != previous): raise Conflict('FLOW_RECEIPT_CHAIN_CHANGED')
                for ref in receipt['output'].get('artifacts', []):
                    if sha_file(ref['path']) != ref['sha256']: raise Conflict('FLOW_ARTIFACT_CHANGED')
                outputs[stage] = receipt['output']; previous = receipt['content_hash']; reused += 1
                continue
            if any((root/'receipts'/(later+'.json')).exists() for later in STAGES[STAGES.index(stage)+1:]):
                raise Conflict('FLOW_RECEIPT_GAP')
            attempt_path = root / 'attempts' / (stage + '.json')
            old = read_json(attempt_path) if attempt_path.exists() else {}
            if old.get('status') == 'RUNNING': raise PipelineError('FLOW_STOP_EVIDENCE_REQUIRED')
            token = str(uuid4()); start = time.monotonic()
            attempt = dict(stage=stage, status='RUNNING', token=token, started_at=now(),
                           attempts=old.get('attempts', 0) + 1, flow_id=plan['content_hash'])
            atomic_json(attempt_path, attempt)
            context = seal(dict(flow=plan, root=str(root), stage=stage, outputs=outputs, previous_hash=previous))
            try:
                output = executor(context)
                if not isinstance(output, dict) or output.get('status') != 'VERIFIED':
                    raise PipelineError('FLOW_STAGE_NOT_VERIFIED')
                for ref in output.get('artifacts', []):
                    if sha_file(ref['path']) != ref['sha256']: raise Conflict('FLOW_ARTIFACT_CHANGED')
                if hook: hook(stage, output)  # Test actual work complete before receipt.
                receipt = seal(dict(flow_id=plan['content_hash'], stage=stage, previous_hash=previous,
                                    output=output, completed_at=now()))
                atomic_json(proof_path, receipt)
            except Exception as exc:
                attempt.update(status='FAILED', error_code=exc.code if isinstance(exc, PipelineError) else type(exc).__name__)
                atomic_json(attempt_path, {**attempt, 'duration_ms': round((time.monotonic()-start)*1000)})
                raise
            # BaseException intentionally preserves RUNNING for operator fencing.
            atomic_json(attempt_path, {**attempt, 'status': 'SUCCEEDED',
                                     'duration_ms': round((time.monotonic()-start)*1000)})
            outputs[stage] = output; previous = receipt['content_hash']
        result = seal(dict(flow_id=plan['content_hash'], status='COMPLETED_WITH_QUALITY_WAIT'
            if outputs['PREPARE'].get('blocked', 0) else 'COMPLETED',
            reused_stages=reused, receipt_hash=previous, outputs=outputs))
        atomic_json(root / 'result.json', result)
        return result


def recover(root, stage, token, evidence):
    if stage not in STAGES or not isinstance(evidence, str) or not evidence.strip():
        raise PipelineError('FLOW_STOP_EVIDENCE_REQUIRED')
    root = Path(root).resolve()
    with lock(root / 'flow.lock'):
        load(root); path = root / 'attempts' / (stage + '.json'); attempt = read_json(path)
        if attempt['status'] != 'RUNNING' or attempt['token'] != token:
            raise Conflict('FLOW_RECOVERY_CONFLICT')
        attempt.update(status='RECOVERED', stop_evidence=evidence, recovered_at=now())
        atomic_json(path, attempt)
    return dict(status='FLOW_RECOVERED', stage=stage)


def audit(root, executor=None):
    """Fresh external reads without rerunning writers or changing original receipts."""
    root=Path(root).resolve()
    if executor is None:
        from .flow_runtime import execute
        executor=execute
    with lock(root/'flow.lock'):
        plan=load(root);previous=plan['content_hash'];outputs={}
        for stage in STAGES[:-1]:
            receipt=verified(read_json(root/'receipts'/(stage+'.json')))
            if receipt['flow_id']!=plan['content_hash'] or receipt['stage']!=stage or receipt['previous_hash']!=previous:
                raise Conflict('FLOW_RECEIPT_CHAIN_CHANGED')
            for ref in receipt['output'].get('artifacts',[]):
                if sha_file(ref['path'])!=ref['sha256']:raise Conflict('FLOW_ARTIFACT_CHANGED')
            outputs[stage]=receipt['output'];previous=receipt['content_hash']
        directory=root/'audits'/str(uuid4());directory.mkdir(parents=True)
        context=seal(dict(flow=plan,root=str(root),stage='RECONCILE',outputs=outputs,
            previous_hash=previous,audit_directory=str(directory)))
        result=executor(context)
        if result.get('status')!='VERIFIED':raise PipelineError('FLOW_AUDIT_NOT_VERIFIED')
        atomic_json(directory/'receipt.json',seal(dict(flow_id=plan['content_hash'],result=result,recorded_at=now())))
        return dict(status='AUDIT_VERIFIED',audit=str(directory/'receipt.json'))
