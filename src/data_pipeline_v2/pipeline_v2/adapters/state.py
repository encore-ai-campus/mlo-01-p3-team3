"""Versioned scoped manifests, active delivery index and recovery evidence."""
from pathlib import Path
from ..domain.contracts import digest, seal, verified, Invalid
from .files import read, write, immutable, confined
from .timing import Timing
import re


class State:
    def __init__(self, root, target):
        self.root = Path(root).resolve()
        self.root.mkdir(parents=True, exist_ok=True)
        self.timing=Timing(self.root)
        immutable(self.root/'target.json', seal(dict(contract_version=2, target=target)))

    def scope(self, scope_id):
        if not re.fullmatch('[A-Za-z0-9_-]{1,128}',scope_id):raise Invalid('SCOPE_ID_INVALID')
        return verified(read(self.root/'scopes'/scope_id/'scope.json'))

    def spec(self, key):
        if not re.fullmatch('[a-f0-9]{64}',key):raise Invalid('JOB_ID_INVALID')
        return verified(read(self.root/'jobs'/key/'spec.json'))

    def job(self, key):
        if not re.fullmatch('[a-f0-9]{64}',key):raise Invalid('JOB_ID_INVALID')
        path = self.root/'jobs'/key/'state.json'
        return verified(read(path)) if path.exists() else dict(processing='PENDING', delivery='NOT_READY', resolution=None)

    def update(self, key, **values):
        body = {k:v for k,v in self.job(key).items() if k != 'content_hash'}
        write(self.root/'jobs'/key/'state.json', seal(dict(body, **values)))

    def current(self, spec):
        path = self.root/'products'/digest(spec['product_key'])/'desired.json'
        return verified(read(path))['job_id'] == spec['job_id']

    def desired(self, spec):
        write(self.root/'products'/digest(spec['product_key'])/'desired.json', seal(dict(job_id=spec['job_id'], input_hash=spec['input_hash'])))

    def receipt(self, key, name):
        path = self.root/'jobs'/key/'receipts'/(digest(name)+'.json')
        return verified(read(path)) if path.exists() else None

    def save_receipt(self, key, name, value):
        immutable(self.root/'jobs'/key/'receipts'/(digest(name)+'.json'), seal(value))

    def enqueue(self, key, delivery):
        immutable(self.root/'jobs'/key/'delivery.json', delivery)
        immutable(self.root/'active'/(key+'.json'), seal(dict(job_id=key, delivery_hash=delivery['content_hash'])))
        self.update(key, delivery='QUEUED')

    def pending(self):
        return [verified(read(p)) for p in sorted((self.root/'active').glob('*.json'))]

    def resolve(self, key, **values):
        self.update(key, **values)
        (self.root/'active'/(key+'.json')).unlink(missing_ok=True)

    def refs(self, spec):
        for ref in spec['reference_pages']:
            value = verified(read(confined(self.root,ref['path'])))
            if value['content_hash'] != ref['hash']: raise Invalid('REFERENCE_PAGE_CHANGED')
            yield from value['references']
