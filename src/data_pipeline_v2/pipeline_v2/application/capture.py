from itertools import groupby
import hashlib
from ..domain.contracts import canonical, digest, seal, stamp, Invalid
from ..adapters.files import read, write, immutable, locked, confined
from ..domain.transform import related_policy, VERSION


def bind(state, directory, scope):
    if (directory/'bound.json').exists():return
    for ref in scope['index_pages']:
        page=read(confined(state.root,ref['path']))
        if page['content_hash']!=ref['hash']:raise Invalid('SCOPE_INDEX_CHANGED')
        for key in page['jobs']:state.desired(state.spec(key))
    immutable(directory/'bound.json',seal(dict(scope_hash=scope['content_hash'])))


def capture(state, source, scope_id, policy, limits):
    if not scope_id or len(scope_id)>128 or any(c not in 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_' for c in scope_id):
        raise Invalid('SCOPE_ID_INVALID')
    directory = state.root/'scopes'/scope_id
    with locked(state.root/'coordinator.lock'), state.timing.measure('capture.total',scope_id):
        if (directory/'scope.json').exists():
            scope = state.scope(scope_id)
            if scope['source'] != source.identity or scope['policy_hash'] != digest(policy): raise Invalid('SCOPE_ALREADY_SEALED')
            bind(state,directory,scope)
            return scope
        index, index_pages, total, previous = [], [], 0, None
        raw_ids = set()
        for product, values in groupby(state.timing.iterate('capture.source_discovery',scope_id,source.references()), key=lambda r:r['product_key']):
            if previous is not None and product <= previous: raise Invalid('SOURCE_PRODUCT_ORDER_INVALID')
            previous = product
            pages, pending, size, count = [], [], 0, 0
            identity = hashlib.sha256()
            product_dir = directory/'references'/digest(product)
            last = None
            dependencies=[]
            for ref in values:
                if ref['raw_document_id'] in raw_ids or (last is not None and ref['observation_key'] <= last):
                    raise Invalid('SOURCE_DUPLICATE_OR_ORDER_INVALID')
                raw_ids.add(ref['raw_document_id']); last=ref['observation_key']; count+=1
                dependencies.append(dict(categories=ref['categories'],attributes=ref['attributes']))
                identity.update((canonical(ref)+'\n').encode())
                amount=len(canonical(ref).encode())
                if pending and (len(pending)>=limits.observations or size+amount>limits.page_bytes):
                    value=seal(dict(references=pending)); path=product_dir/(str(len(pages))+'.json')
                    immutable(path,value); pages.append(dict(path=path.relative_to(state.root).as_posix(),hash=value['content_hash']))
                    pending=[];size=0
                pending.append(ref);size+=amount
            if pending:
                value=seal(dict(references=pending));path=product_dir/(str(len(pages))+'.json')
                immutable(path,value);pages.append(dict(path=path.relative_to(state.root).as_posix(),hash=value['content_hash']))
            product_policy=related_policy(policy,dependencies)
            input_hash=digest(dict(references=identity.hexdigest(),policy=product_policy,engine_version=VERSION))
            key=digest(dict(product_key=product,input_hash=input_hash))
            spec=seal(dict(job_id=key,product_key=product,input_hash=input_hash,observations=count,
                reference_pages=pages,policy=product_policy,created_at=stamp()))
            # The same semantic job may be required by multiple scopes. Keep its original immutable refs.
            if not (state.root/'jobs'/key/'spec.json').exists():
                immutable(state.root/'jobs'/key/'spec.json',spec)
            index.append(key);total+=1
            if len(index)==128:
                value=seal(dict(jobs=index));path=directory/'index'/(str(len(index_pages))+'.json')
                immutable(path,value);index_pages.append(dict(path=path.relative_to(state.root).as_posix(),hash=value['content_hash']));index=[]
        if index:
            value=seal(dict(jobs=index));path=directory/'index'/(str(len(index_pages))+'.json')
            immutable(path,value);index_pages.append(dict(path=path.relative_to(state.root).as_posix(),hash=value['content_hash']))
        if not total: raise Invalid('EMPTY_SCOPE')
        scope=seal(dict(contract_version=2,scope_id=scope_id,scope_sealed=True,source=source.identity,
            policy_hash=digest(policy),products=total,observations=len(raw_ids),index_pages=index_pages))
        immutable(directory/'scope.json',scope)
        bind(state,directory,scope)
        return scope
