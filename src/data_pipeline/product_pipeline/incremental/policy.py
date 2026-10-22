"""Pure scope and dependency rules; no clocks, connections or approval writes."""
import re
from ..contracts import canonical, digest, utc
from ..errors import PipelineError

STAGES=('silver','gold','publish','activation')


def snapshot(product_key, observations, dependencies, *, next_due_at=None, publication_hash=None):
    if (not isinstance(product_key,str) or ':' not in product_key or len(product_key)>200
            or not 1<=len(observations)<=100 or len({r['observation_key'] for r in observations})!=len(observations)):
        raise PipelineError('INCREMENTAL_PRODUCT_SCOPE_INVALID')
    for row in observations:
        if not re.fullmatch('[a-f0-9]{64}',row['observation_key']) or not re.fullmatch('[a-f0-9]{24}',row['raw_document_id']):
            raise PipelineError('INCREMENTAL_OBSERVATION_INVALID')
    for dependency in dependencies.values():
        if dependency.get('stage') not in {'silver','gold'} or not re.fullmatch('[a-f0-9]{64}',dependency.get('hash','')):
            raise PipelineError('INCREMENTAL_DEPENDENCY_INVALID')
    body=dict(contract_version=1,product_key=product_key,observations=sorted(observations,key=canonical),
              dependencies=dependencies)
    # Deadlines and external storage evidence do not change the semantic input.
    return dict(body,content_hash=digest(body),next_due_at=utc(next_due_at) if next_due_at else None,publication_hash=publication_hash)


def first_stage(previous, current):
    if not previous:return 'silver'
    if previous['observations']!=current['observations']:return 'silver'
    old,new=previous['dependencies'],current['dependencies']
    changed=[k for k in set(old)|set(new) if old.get(k)!=new.get(k)]
    if any((new.get(k) or old[k])['stage']=='silver' for k in changed):return 'silver'
    return 'gold'


def batches(captures):
    """Group by starting stage, <=100 products AND <=100 observations."""
    pending={stage:[] for stage in ('silver','gold')};sizes=dict(silver=0,gold=0)
    for capture in captures:
        stage=capture['first_stage'];n=len(capture['snapshot']['observations'])
        if pending[stage] and sizes[stage]+n>100:
            yield pending[stage];pending[stage]=[];sizes[stage]=0
        pending[stage].append(capture);sizes[stage]+=n
    for stage in pending:
        if pending[stage]:yield pending[stage]
