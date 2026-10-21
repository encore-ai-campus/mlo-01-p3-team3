"""Reevaluate only waiting products, reusing unchanged confirmed Silver inputs."""
from pathlib import Path
from product_pipeline.contracts import digest,now
from product_pipeline.files import read_json,atomic_json
from product_pipeline.errors import Conflict
from product_pipeline.windows import validate_window
from product_pipeline.silver.selection import validate,seal
from product_pipeline.spark_session import timed,bump


def time_inputs_unchanged(inputs,parent_result,at):
    """Do not reuse across an effective-date boundary, even with equal hashes."""
    from datetime import datetime,timedelta
    from product_pipeline.contracts import utc
    dates=[row[field] for snapshot in inputs.values() for row in snapshot.get('rows',[])
           for field in ('valid_from','expires_at','checked_at') if row.get(field)]
    if not dates:return True
    try:
        end=datetime.fromisoformat(utc(parent_result['finished_at']).replace('Z','+00:00'))
        elapsed=parent_result['duration_ms']
        if type(elapsed) is not int or elapsed<0:return False
        start=end-timedelta(milliseconds=elapsed)
        current=datetime.fromisoformat(utc(at).replace('Z','+00:00'))
        if current<end:return False
        return not any(start<=datetime.fromisoformat(utc(d).replace('Z','+00:00'))<=current for d in dates)
    except (KeyError,ValueError,TypeError):return False


def waiting_selection(root):
    parent=validate_window(read_json(root/'selection.json'))
    reviewed=read_json(root/'publish-result.json').get('reviewed',{})
    # Old successful receipts have no per-product list: keep the old full scope.
    if 'waiting_product_keys' not in reviewed:return parent
    keys=reviewed['waiting_product_keys']
    if (not isinstance(keys,list) or any(not isinstance(k,str) for k in keys) or len(set(keys))!=len(keys)
            or not set(keys)<=set(parent['product_keys']) or len(keys)!=reviewed.get('waiting')
            or len(keys)+reviewed.get('activated',-1)!=len(parent['product_keys'])):
        raise Conflict('ACTIVATION_WAITING_SCOPE_CONFLICT')
    if not keys:return None
    choices=validate(read_json(root/'silver-state/evaluation-selection.json'),parent)
    chosen=[c for c in choices if c['product_key'] in keys]
    body={k:v for k,v in parent.items() if k!='content_hash'}
    body.update(product_keys=sorted(keys),observation_keys=sorted(c['observation_key'] for c in chosen))
    return validate_window(dict(body,content_hash=digest(body)))


def freeze_silver(work,config,target,selection):
    """Reuse only exact input hashes and a completed parent selection.

    Gold still reads and validates the actual pinned Silver receipts. This
    local reuse record never substitutes for those storage proofs.
    """
    from product_pipeline.publication.mysql import MySQL
    from product_pipeline.review import export_inputs
    parent=work.ROOT;path=target/'silver-inputs.json'
    if not path.exists():
        db=MySQL(config)
        try:
            with db.read_snapshot():inputs=export_inputs(db,[],as_of=now())['silver']
        finally:db.close()
        atomic_json(path,inputs)
    inputs=read_json(path)
    if (target/'silver-result.json').exists():return
    if inputs!=read_json(parent/'silver-inputs.json'):return
    parent_result=read_json(parent/'silver-result.json')
    if parent_result.get('status')!='COMPLETED':raise Conflict('ACTIVATION_PARENT_SILVER_INCOMPLETE')
    if not time_inputs_unchanged(inputs,parent_result,now()):return
    parent_window=read_json(parent/'selection.json')
    choices=validate(read_json(parent/'silver-state/evaluation-selection.json'),parent_window)
    choices=[c for c in choices if c['observation_key'] in selection['observation_keys']]
    atomic_json(target/'silver-state/evaluation-selection.json',seal(selection,choices))
    proof=dict(parent_window_hash=parent_window['content_hash'],window_hash=selection['content_hash'],
        inputs_hash=digest(inputs),parent_selection_hash=digest(read_json(parent/'silver-state/evaluation-selection.json')))
    atomic_json(target/'silver-reuse.json',proof)
    atomic_json(target/'silver-result.json',dict(status='COMPLETED',stage='silver',reused=True,
        window_hash=selection['content_hash'],duration_ms=0,reuse_proof_hash=digest(proof)))
    bump('activation_silver_reused_products',len(selection['product_keys']))


def freeze_observation_inputs(work,config,target,selection):
    """Freeze owner inputs and candidates, never synthesize a Silver completion."""
    from product_pipeline.publication.mysql import MySQL
    from product_pipeline.review import export_inputs
    parent=work.ROOT
    if read_json(parent/'silver-result.json').get('status')!='COMPLETED':raise Conflict('ACTIVATION_PARENT_SILVER_INCOMPLETE')
    parent_window=validate_window(read_json(parent/'selection.json'))
    parent_choices=validate(read_json(parent/'silver-state/evaluation-selection.json'),parent_window)
    selected=[c for c in parent_choices if c['observation_key'] in selection['observation_keys']]
    candidates=seal(selection,selected)
    path=target/'silver-reuse-selection.json'
    if path.exists() and read_json(path)!=candidates:raise Conflict('ACTIVATION_PARENT_SELECTION_CHANGED')
    if not path.exists():atomic_json(path,candidates)
    path=target/'silver-inputs.json'
    if not path.exists():
        db=MySQL(config)
        try:
            with db.read_snapshot():inputs=export_inputs(db,[],as_of=now())['silver']
        finally:db.close()
        atomic_json(path,inputs)


def run(work,config,options):
    root=work.ROOT
    # Resume already-frozen old attempts with their original full-window scope.
    legacy=root/'activation'
    selection=read_json(legacy/'selection.json') if (legacy/'selection.json').exists() else waiting_selection(root)
    if selection is None:return dict(status='COMPLETED',stage='activation',reason='ALL_PRODUCTS_ALREADY_ACTIVATED')
    review_path=root/'post-intake-review.json'
    with timed('activation_owner_decisions'):
        if not review_path.exists():atomic_json(review_path,work.catalog_review(product_keys=selection['product_keys']))
    if not read_json(review_path).get('created'):
        return dict(status='COMPLETED',stage='activation',reason='NO_NEW_OWNER_DECISIONS')
    previous=root/'activation-v2'
    target=legacy if (legacy/'selection.json').exists() else previous if (previous/'selection.json').exists() else root/'activation-v3'
    target.mkdir(exist_ok=True,mode=0o700)
    path=target/'selection.json'
    if path.exists() and read_json(path)!=selection:raise Conflict('ACTIVATION_SELECTION_CHANGED')
    if not path.exists():atomic_json(path,selection)
    if target.name=='activation-v3':
        with timed('activation_silver_reuse_check'):freeze_observation_inputs(work,config,target,selection)
    elif target!=legacy:
        with timed('activation_silver_reuse_check'):freeze_silver(work,config,target,selection)
    for stage in ('silver','gold','publish'):
        path=target/(stage+'-result.json')
        if path.exists() and read_json(path).get('status')=='COMPLETED':continue
        with timed('activation_'+stage):work.run_at(stage,target)
        if not path.exists() or read_json(path).get('status')!='COMPLETED':raise Conflict('ACTIVATION_REEVALUATION_FAILED_'+stage)
    return dict(status='COMPLETED',stage='activation',reviewed=read_json(target/'publish-result.json').get('reviewed'),
        silver_normalization_skipped=read_json(target/'silver-result.json').get('runtime_metrics',{}).get('silver_normalization_skipped',0),
        reevaluated_products=len(selection['product_keys']),
        skipped_already_activated=len(read_json(root/'selection.json')['product_keys'])-len(selection['product_keys']),
        reason='POST_INTAKE_DECISIONS_REEVALUATED')
