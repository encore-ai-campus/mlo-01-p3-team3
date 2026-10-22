"""One writer, separate processing/delivery progress, bounded durable handoff."""
from datetime import datetime, timedelta, timezone
import shutil
import time

from ..adapters.files import read, write, locked, confined
from ..domain.contracts import seal, verified, digest, stamp, Invalid
from .processing import Processor


def transient(exc):
    if isinstance(exc,(OSError,TimeoutError,ConnectionError)):return True
    # Retry only driver failures whose outcome can be reconciled by the receipt.
    return (type(exc).__module__.startswith('pymysql.') and type(exc).__name__ in {'OperationalError','InterfaceError'}
        and exc.args and exc.args[0] in {0,1040,1205,1213,2002,2003,2006,2013})


class Coordinator:
    def __init__(self, state, source, lake, publisher, limits):
        self.state, self.source, self.lake, self.publisher, self.limits = state, source, lake, publisher, limits
        self.processor = Processor(source, lake, state, limits)

    def _index(self, scope, page):
        ref = scope['index_pages'][page]
        value = verified(read(confined(self.state.root,ref['path'])))
        if value['content_hash'] != ref['hash']: raise Invalid('SCOPE_INDEX_CHANGED')
        return value['jobs']

    def _delivery(self):
        processed = 0
        for task in self.state.pending():
            key = task['job_id']; current = self.state.job(key)
            if current.get('delivery') == 'RUNNING': raise Invalid('DELIVERY_RECOVERY_EVIDENCE_REQUIRED')
            if current.get('next_retry_at', '') > stamp(): continue
            spec = self.state.spec(key)
            delivery = verified(read(self.state.root/'jobs'/key/'delivery.json'))
            if delivery['content_hash'] != task['delivery_hash']: raise Invalid('DELIVERY_MANIFEST_CHANGED')
            event_scope=delivery.get('scope_id','delivery-'+key)
            with self.state.timing.measure('delivery.check_desired_generation',event_scope,job_id=key):
                is_current=self.state.current(spec)
            if not is_current:
                self.state.resolve(key, delivery='RESOLVED', resolution='SUPERSEDED'); continue
            references = list(self.state.refs(spec))
            with self.state.timing.measure('delivery.validate_source_revision',event_scope,job_id=key):
                source_is_current=self.source.current(references)
            if not source_is_current:
                self.state.resolve(key, delivery='RESOLVED', resolution='SUPERSEDED'); continue
            tries = current.get('attempts', 0) + 1
            self.state.update(key, delivery='RUNNING', attempts=tries)
            try:
                with self.state.timing.measure('delivery.materialize_gold',event_scope,job_id=key,counters=self.lake.metrics):
                    group = self.processor.materialize(delivery['gold'],event_scope)
                request = dict(delivery, product=group)
                with self.state.timing.measure('delivery.publish',event_scope,job_id=key):
                    receipt = self.publisher.publish(request)
                with self.state.timing.measure('delivery.audit',event_scope,job_id=key):
                    self.publisher.audit(request, receipt)
                if not self.state.current(spec): raise Invalid('GENERATION_CHANGED_DURING_DELIVERY')
                self.state.resolve(key, delivery='RESOLVED', resolution=receipt['resolution'], publication=receipt)
                processed += 1
            except Exception as exc:
                if transient(exc):
                    due = (datetime.now(timezone.utc) + timedelta(seconds=min(60, 2**tries))).isoformat(timespec='milliseconds').replace('+00:00','Z')
                    self.state.update(key, delivery='RETRY_WAIT' if tries < self.limits.max_attempts else 'BLOCKED',
                        error=type(exc).__name__, next_retry_at=due if tries < self.limits.max_attempts else '9999')
                    continue
                self.state.update(key, delivery='BLOCKED', error=getattr(exc,'code',type(exc).__name__), next_retry_at='9999')
                raise
        return processed

    def tick(self, scope_id):
        with self.state.timing.measure('tick.total',scope_id,counters=self.lake.metrics):
            return self._tick(scope_id)

    def _tick(self, scope_id):
        with locked(self.state.root/'coordinator.lock'):
            scope = self.state.scope(scope_id)
            progress_path = self.state.root/'scopes'/scope_id/'progress.json'
            progress = read(progress_path) if progress_path.exists() else dict(page=0, offset=0, planned=0)
            delivered = self._delivery()
            with self.state.timing.measure('tick.pending_capacity_snapshot',scope_id):
                active = self.state.pending()
                used = sum(read(self.state.root/'jobs'/t['job_id']/'delivery.json')['gold']['retained_bytes'] for t in active)
                high = len(active) >= self.limits.pending_high or used >= self.limits.pending_bytes
                if progress.get('backpressure') and len(active) > self.limits.pending_low: high = True
                free = shutil.disk_usage(self.state.root).free
            specs, count = [], 0
            while not high and progress['page'] < len(scope['index_pages']):
                jobs = self._index(scope, progress['page'])
                if progress['offset'] >= len(jobs):
                    progress.update(page=progress['page']+1, offset=0); continue
                spec = self.state.spec(jobs[progress['offset']])
                status = self.state.job(spec['job_id'])
                if status.get('processing') == 'RUNNING': raise Invalid('PROCESSING_RECOVERY_EVIDENCE_REQUIRED')
                if status.get('delivery')=='RESOLVED' and status.get('resolution') in {'PUBLISHED','QUALITY_WAIT','LAST_GOOD_RETAINED'}:
                    receipt=self.state.receipt(spec['job_id'],'gold')
                    with self.state.timing.measure('tick.reuse_confirmed_gold',scope_id,job_id=spec['job_id'],counters=self.lake.metrics):
                        group=self.processor.materialize(receipt,scope_id)
                    delivery=verified(read(self.state.root/'jobs'/spec['job_id']/'delivery.json'))
                    with self.state.timing.measure('tick.audit_existing_publication',scope_id,job_id=spec['job_id']):
                        self.publisher.audit(dict(delivery,product=group),status['publication'])
                    progress['offset']+=1;progress['planned']+=1
                    continue
                if len(active)+len(specs) >= self.limits.pending_high: break
                if specs and count+spec['observations'] > self.limits.observations: break
                if free < self.limits.min_free_bytes+self.limits.history_bytes*(len(specs)+1):
                    high=True; break
                if used+self.limits.history_bytes*(len(specs)+1)>self.limits.pending_bytes:
                    high=True; break
                specs.append(spec); count+=spec['observations']
                progress['offset'] += 1; progress['planned'] += 1
                if count >= self.limits.observations: break
            if specs:
                # Reserve before any new Gold files; the progress cursor is persisted only after handoff.
                reservation = seal(dict(jobs=[s['job_id'] for s in specs], bytes=self.limits.history_bytes*len(specs)))
                write(self.state.root/'reservation.json', reservation)
                for spec in specs: self.state.update(spec['job_id'], processing='RUNNING')
                try:
                    completed = self.processor.process_many(specs,scope_id)
                    for spec in specs:
                        key = spec['job_id']
                        if key not in completed: continue
                        delivery = seal(dict(contract_version=2, job_id=key, product_key=spec['product_key'],
                            input_hash=spec['input_hash'], target=self.publisher.identity, gold=completed[key],scope_id=scope_id))
                        with self.state.timing.measure('delivery.durable_handoff',scope_id,job_id=key):
                            self.state.enqueue(key, delivery)
                except Exception as exc:
                    for spec in specs:
                        if self.state.job(spec['job_id'])['processing'] == 'RUNNING':
                            self.state.update(spec['job_id'], processing='FAILED', error=getattr(exc,'code',type(exc).__name__))
                    raise
                finally:
                    (self.state.root/'reservation.json').unlink(missing_ok=True)
            progress['backpressure'] = high
            with self.state.timing.measure('tick.persist_progress_cursor',scope_id):write(progress_path, progress)
            return dict(status='WAITING_DELIVERY' if high else 'TICK_COMPLETED',
                planned=progress['planned'], processed=len(specs), delivered=delivered,
                scan_complete=progress['page']>=len(scope['index_pages']) or (progress['page']==len(scope['index_pages'])-1
                    and progress['offset']>=len(self._index(scope,progress['page']))),
                pending=len(self.state.pending()), metrics=dict(self.processor.metrics),storage_metrics=dict(self.lake.metrics))

    def status(self, scope_id, verify_outputs=True):
        with self.state.timing.measure('status.total',scope_id,counters=self.lake.metrics):
            result=self._status(scope_id,verify_outputs)
        if verify_outputs:result['timing']=self.state.timing.summary(scope_id)
        return result

    def _status(self, scope_id, verify_outputs=True):
        scope = self.state.scope(scope_id)
        counts, total, gold_complete, delivery_complete, blocked = {}, 0, True, True, False
        for page in range(len(scope['index_pages'])):
            for key in self._index(scope, page):
                job = self.state.job(key); total+=1
                blocked |= job.get('delivery')=='BLOCKED' or job.get('processing') in {'BLOCKED','FAILED'}
                outcome = job.get('resolution') or job.get('processing', 'PENDING')
                counts[outcome] = counts.get(outcome,0)+1
                receipt = self.state.receipt(key,'gold')
                gold_complete &= job.get('processing') == 'GOLD_CONFIRMED' and receipt is not None
                delivery_complete &= job.get('delivery') == 'RESOLVED' and outcome in {'PUBLISHED','QUALITY_WAIT','LAST_GOOD_RETAINED'}
                if verify_outputs and receipt:
                    with self.state.timing.measure('status.verify_gold',scope_id,job_id=key,counters=self.lake.metrics):self.processor.verify_gold(receipt,scope_id)
                if verify_outputs and job.get('delivery')=='RESOLVED' and job.get('resolution') in {'PUBLISHED','QUALITY_WAIT','LAST_GOOD_RETAINED'}:
                    delivery=verified(read(self.state.root/'jobs'/key/'delivery.json'))
                    with self.state.timing.measure('status.verify_publication',scope_id,job_id=key,counters=self.lake.metrics):
                        group=self.processor.materialize(receipt,scope_id)
                        self.publisher.audit(dict(delivery,product=group),job['publication'])
        if total != scope['products']: raise Invalid('SCOPE_MEMBERSHIP_CHANGED')
        done = gold_complete and delivery_complete
        status = ('COMPLETED_WITH_QUALITY_WAIT' if counts.get('QUALITY_WAIT') or counts.get('LAST_GOOD_RETAINED') else 'COMPLETED') if done else 'RUNNING'
        if blocked: status='ATTENTION_REQUIRED'
        return dict(status=status, scope_id=scope_id, products=total, counts=counts,
            gold_complete=bool(gold_complete), delivery_complete=bool(delivery_complete))

    def recover(self, job_id, evidence):
        if not isinstance(evidence,str) or not evidence.strip(): raise Invalid('STOP_EVIDENCE_REQUIRED')
        with locked(self.state.root/'coordinator.lock'):
            spec=self.state.spec(job_id); saved=self.state.job(job_id)
            write(self.state.root/'jobs'/job_id/'recovery'/ (digest(dict(evidence=evidence,at=stamp()))+'.json'),
                seal(dict(before=saved, stop_evidence=evidence, at=stamp())))
            self.state.update(job_id, processing='GOLD_CONFIRMED' if self.state.receipt(job_id,'gold') else 'PENDING',
                delivery='QUEUED' if (self.state.root/'jobs'/job_id/'delivery.json').exists() else 'NOT_READY',
                attempts=0,next_retry_at='',error=None)
            delivery_path=self.state.root/'jobs'/job_id/'delivery.json'
            if delivery_path.exists():self.state.enqueue(job_id,verified(read(delivery_path)))
            # A blocked parent may already be behind a durable scope cursor.
            # Recovery must make it discoverable again, without re-running resolved jobs.
            for scope_path in (self.state.root/'scopes').glob('*/scope.json'):
                scope=verified(read(scope_path));progress_path=scope_path.parent/'progress.json'
                if not progress_path.exists():continue
                progress=read(progress_path)
                for page in range(len(scope['index_pages'])):
                    jobs=self._index(scope,page)
                    if job_id not in jobs:continue
                    position=(page,jobs.index(job_id))
                    if position<(progress['page'],progress['offset']):
                        progress.update(page=position[0],offset=position[1],backpressure=False)
                        write(progress_path,progress)
                    break
            return dict(status='RECOVERED',job_id=spec['job_id'])
