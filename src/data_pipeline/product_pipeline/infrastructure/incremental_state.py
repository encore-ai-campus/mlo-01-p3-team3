"""Single-owner files alongside the existing operational queue; atomic records."""
from pathlib import Path
from ..contracts import digest,now,utc
from ..errors import Conflict
from ..files import atomic_json,read_json,lock,sha_file


class IncrementalState:
    def __init__(self,root,target):self.root,self.target=Path(root).resolve(),target

    def _path(self,key):return self.root/'products'/(digest(key)+'.json')

    def bind(self,sources):
        with lock(self.root/'state.lock'):
            path=self.root/'target.json';expected=dict(target=self.target,sources=sources)
            if path.exists() and read_json(path)!=expected:raise Conflict('INCREMENTAL_SOURCE_TARGET_CHANGED')
            if not path.exists():atomic_json(path,expected)

    def get(self,key):
        path=self._path(key)
        if not path.exists():return None
        row=read_json(path)
        if row.get('product_key')!=key or row.get('target')!=self.target:raise Conflict('INCREMENTAL_STATE_TARGET_CHANGED')
        return row

    def capture(self,snapshot,*,at,full=False):
        from ..incremental.policy import first_stage
        key=snapshot['product_key']
        with lock(self.root/'state.lock'):
            row=self.get(key) or dict(product_key=key,target=self.target,generation=0)
            done=row.get('completed');desired=row.get('desired')
            # Derived lookup files can be restored from authoritative snapshots.
            # Reconciliation must repair them even when no evaluation is needed.
            if full:
                for dependency in snapshot['dependencies']:
                    atomic_json(self.root/'dependencies'/digest(dependency)/(digest(key)+'.json'),dict(product_key=key))
                if done and done.get('next_due_at'):
                    atomic_json(self.root/'deadlines'/done['next_due_at'][:10]/(digest(key)+'.json'),
                        dict(product_key=key,next_due_at=done['next_due_at']))
            deadline=(done or {}).get('next_due_at')
            due=bool(deadline and utc(deadline)<=utc(at))
            damaged=bool(full and done and (snapshot.get('publication_hash')!=done.get('publication_hash') or
                any(not Path(e['path']).is_file() or sha_file(e['path'])!=e['sha256'] for e in done.get('artifacts',[]))))
            if done and done['snapshot_hash']==snapshot['content_hash'] and not due and not damaged:
                if desired and desired['status']!='COMPLETED':
                    # A -> B -> A: fence queued B while retaining the valid A.
                    row['generation']+=1
                    row['desired']=dict(product_key=key,generation=row['generation'],snapshot=snapshot,status='COMPLETED')
                    atomic_json(self._path(key),row)
                    (self.root/'rediscover'/(digest(key)+'.json')).unlink(missing_ok=True)
                return None
            token=digest(dict(snapshot=snapshot['content_hash'],due=deadline if due else None,
                              repair=done.get('receipt_hash') if damaged else None))
            if desired and desired.get('token')==token and desired['status']=='QUEUED':return None
            generation=row['generation']+1
            capture=dict(product_key=key,generation=generation,token=token,snapshot=snapshot,
                         first_stage=first_stage((done or {}).get('snapshot'),snapshot))
            row.update(generation=generation,desired=dict(**capture,status='CAPTURED'))
            atomic_json(self._path(key),row)
            atomic_json(self.root/'rediscover'/(digest(key)+'.json'),dict(product_key=key))
            for dependency in snapshot['dependencies']:
                member=self.root/'dependencies'/digest(dependency)/(digest(key)+'.json')
                if not member.exists():atomic_json(member,dict(product_key=key))
            return capture

    def queued(self,captures,work):
        with lock(self.root/'state.lock'):
            for capture in captures:
                row=self.get(capture['product_key'])
                if row['generation']==capture['generation']:
                    row['desired'].update(status='QUEUED',work=work);atomic_json(self._path(capture['product_key']),row)
                    (self.root/'rediscover'/(digest(capture['product_key'])+'.json')).unlink(missing_ok=True)

    def current(self,captures):
        return all((self.get(c['product_key']) or {}).get('generation')==c['generation'] for c in captures)

    def complete(self,capture,receipt):
        with lock(self.root/'state.lock'):
            row=self.get(capture['product_key'])
            # A late completion can never acknowledge a newer captured revision.
            if row['generation']!=capture['generation']:return False
            old_due=row.get('completed',{}).get('next_due_at');new_due=receipt.get('next_due_at')
            if new_due:atomic_json(self.root/'deadlines'/new_due[:10]/(digest(capture['product_key'])+'.json'),
                dict(product_key=capture['product_key'],next_due_at=new_due))
            row['completed']=dict(snapshot=capture['snapshot'],snapshot_hash=capture['snapshot']['content_hash'],
                receipt_hash=digest(receipt),**receipt)
            row['desired']['status']='COMPLETED'
            atomic_json(self._path(capture['product_key']),row)
            if old_due and old_due[:10]!=(new_due or '')[:10]:
                (self.root/'deadlines'/old_due[:10]/(digest(capture['product_key'])+'.json')).unlink(missing_ok=True)
            return True

    def superseded(self,captures):
        with lock(self.root/'state.lock'):
            for capture in captures:
                row=self.get(capture['product_key'])
                if row['generation']==capture['generation']:
                    row['desired']['status']='SUPERSEDED';atomic_json(self._path(capture['product_key']),row)
                    atomic_json(self.root/'rediscover'/(digest(capture['product_key'])+'.json'),dict(product_key=capture['product_key']))

    def due_keys(self,at):
        for path in (self.root/'deferred').glob('*.json'):yield read_json(path)['product_key']
        for path in (self.root/'rediscover').glob('*.json'):yield read_json(path)['product_key']
        for folder in (self.root/'deadlines').glob('*'):
            if folder.name>utc(at)[:10]:continue
            for path in folder.glob('*.json'):
                item=read_json(path)
                row=self.get(item['product_key']);due=(row or {}).get('completed',{}).get('next_due_at')
                if due and due==item['next_due_at'] and utc(due)<=utc(at):yield item['product_key']

    def affected(self,dependencies):
        seen=set()
        for dependency in dependencies:
            for path in (self.root/'dependencies'/digest(dependency)).glob('*.json'):
                key=read_json(path)['product_key']
                if key in seen:continue
                row=self.get(key)
                if row and dependency in row.get('desired',{}).get('snapshot',{}).get('dependencies',{}):
                    seen.add(key);yield key

    def archive_work(self,captures):
        value=dict(contract_version=1,target=self.target,captures=captures)
        path=self.root/'work'/(digest(value)+'.json')
        if path.exists() and read_json(path)!=value:raise Conflict('INCREMENTAL_WORK_CHANGED')
        if not path.exists():atomic_json(path,value)
        return path

    def reconciliation_due(self,at):
        path=self.root/'reconciliation.json'
        return not path.exists() or read_json(path)['completed_at'][:10]!=utc(at)[:10]

    def reconciled(self,at):atomic_json(self.root/'reconciliation.json',dict(completed_at=utc(at),target=self.target))
