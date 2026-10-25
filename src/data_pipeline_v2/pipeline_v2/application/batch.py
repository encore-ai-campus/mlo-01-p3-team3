"""NiFi-facing durable submit/status and finite worker orchestration.

Processing and publication are separate phases with separate writer locks, so the
publication of batch n can overlap the processing of batch n+1. Each lock covers one
target writer; nothing holds a global lock across Spark actions and MySQL calls.
"""
from contextlib import nullcontext
from datetime import datetime, timezone
from uuid import uuid4
from ..domain.batch import (delivery_contract, logical_job, batch_id, retraction, repair_job,
                            Retention, CODE_VERSION)
from ..domain.contracts import digest, seal, stamp, verified, utc, Invalid

ACTIVE = {'RUNNING', 'PUBLISHING', 'RECOVERY_REQUIRED'}
# Terminal states: neither holds a capacity reservation.
CLOSED = {'COMPLETED', 'ABANDONED'}


def _age(value, now):
    return max(0, (now - datetime.fromisoformat(value.replace('Z', '+00:00'))).total_seconds())


class BatchRunner:
    def __init__(self, store, processor, publisher, timing, limits, policy, target, lock=None, retention=None,
                 kafka_retention_seconds=None):
        self.store, self.processor, self.publisher, self.timing = store, processor, publisher, timing
        self.limits, self.policy, self.target = limits, policy, target
        self.kafka_retention_seconds = kafka_retention_seconds
        self.lock = lock or (lambda name: nullcontext())
        self.retention = retention or Retention()

    # Submission and capacity -------------------------------------------------------------

    def _reserve(self, key, amount, now=None):
        now = now or datetime.now(timezone.utc)
        added = []
        def reserve(old):
            reservations = {}
            for reserved, value in dict((old or {}).get('reservations',{})).items():
                registered = self.store.get('job',reserved)
                if not (registered and registered['status'] in CLOSED):
                    reservations[reserved] = value
            if key not in reservations:
                oldest = max((_age(v['reserved_at'],now) for v in reservations.values()), default=0)
                if (len(reservations) >= self.limits.pending_batches or oldest > self.limits.pending_age_seconds or
                        sum(v['bytes'] for v in reservations.values())+amount > self.limits.pending_bytes):
                    raise Invalid('BATCH_BACKPRESSURE')
                reservations[key] = dict(bytes=amount,reserved_at=utc(now.isoformat()))
                added.append(key)
            return dict(reservations=reservations)
        self.store.mutate('capacity','global',reserve)
        return bool(added)

    def _release(self, key):
        def release(old):
            reservations = dict((old or {}).get('reservations',{}))
            reservations.pop(key,None)
            return dict(reservations=reservations)
        self.store.mutate('capacity','global',release)

    def _register(self, key, amount, record, accept):
        with self.timing.measure('nifi.submit_registry',record['crawl_batch_id']):
            existing = self.store.get('job',key)
            if existing and existing['status'] in CLOSED:
                # A closed job is never reserved again; resubmitting returns it unchanged.
                return existing
            if existing:
                accept(existing)
            added = self._reserve(key,amount)
            # Single CAS record binds a batch+policy to exactly one logical job.
            def register(old):
                return accept(old) if old else record
            try:
                return self.store.mutate('job', key, register)
            except BaseException:
                if added:
                    self._release(key)
                raise

    def submit(self, batch, generation=0):
        batch_id(batch)
        manifest = delivery_contract(self.store.get('delivery', [batch, generation]))
        source = self.store.get('crawl', batch)
        if not source or source['content_hash'] != manifest['source_manifest_hash']:
            raise Invalid('SOURCE_DELIVERY_MANIFEST_CONFLICT')
        key = logical_job(manifest,digest(self.policy),digest(self.target))
        def accept(old):
            if old['event_set_hash'] != manifest['event_set_hash'] or old['job_id'] != key:
                raise Invalid('BATCH_REQUEST_CONFLICT')
            # A new delivery generation may repair expired Kafka offsets.
            if old['status'] == 'QUEUED' and generation > old['generation']:
                return dict(old,generation=generation,delivery_hash=manifest['content_hash'])
            return old
        return self._register(key, manifest['source_bytes'], dict(job_id=key,kind='CRAWL',crawl_batch_id=batch,
            event_set_hash=manifest['event_set_hash'],generation=generation,delivery_hash=manifest['content_hash'],
            status='QUEUED',submitted_at=stamp(),source_bytes=manifest['source_bytes'],code_version=CODE_VERSION,
            policy_hash=digest(self.policy),target_hash=digest(self.target),attempts=0), accept)

    def retract(self, value):
        value = retraction(value)
        return self.store.put('retraction', value['raw_document_id'], value)

    def repair(self, products):
        """Recompute only named products from retained Bronze history, without re-crawling."""
        wanted = set(products)
        retractions = [v for v in self.store.values('retraction') if v['product_key'] in wanted]
        key, name, retracted = repair_job(list(products),retractions,digest(self.policy),digest(self.target))
        def accept(old):
            if old['event_set_hash'] != retracted:
                raise Invalid('BATCH_REQUEST_CONFLICT')
            return old
        return self._register(key, 0, dict(job_id=key,kind='REPAIR',crawl_batch_id=name,products=sorted(wanted),
            event_set_hash=retracted,generation=0,delivery_hash=None,status='QUEUED',submitted_at=stamp(),
            source_bytes=0,code_version=CODE_VERSION,policy_hash=digest(self.policy),target_hash=digest(self.target),
            attempts=0), accept)

    def _boundary(self, *args, **kwargs):
        # A reported timing boundary is evidence about finished work, never a condition of it.
        try:
            self.timing.boundary(*args, **kwargs)
        except Exception:
            self.timing.failures += 1

    # Lightweight status ------------------------------------------------------------------

    def status(self, key):
        job = self.store.get('job', key)
        if not job:
            raise Invalid('BATCH_JOB_NOT_FOUND')
        stages = {}
        for name in ('bronze','silver','gold','publication'):
            value = self.store.get('stage',[key,name])
            stages[name] = {k:value[k] for k in ('status','stage','content_hash','parent','completed_at','counts',
                'resolutions','gold_version') if k in value} if value else None
        report = dict(job,stages=stages)
        if self.timing.failures:
            report['metric_failures'] = self.timing.failures
        return report

    def backlog(self, rebuild=False, now=None):
        """Incomplete-job index from the capacity record; rebuild scans job records only on request."""
        now = now or datetime.now(timezone.utc)
        if rebuild:
            open_jobs = {v['job_id']:v for v in self.store.values('job') if v['status'] not in CLOSED}
            def rebuilt(old):
                current = dict((old or {}).get('reservations',{}))
                return dict(reservations={k:current.get(k,dict(bytes=v['source_bytes'],reserved_at=v['submitted_at']))
                    for k,v in open_jobs.items()})
            self.store.mutate('capacity','global',rebuilt)
        reservations = (self.store.get('capacity','global') or {}).get('reservations',{})
        jobs = self.store.get_many('job',list(reservations))
        items, statuses = [], {}
        for key, value in sorted(reservations.items(), key=lambda v:v[1]['reserved_at']):
            job = jobs.get(key) or {}
            status = job.get('status','MISSING')
            statuses[status] = statuses.get(status,0)+1
            items.append(dict(job_id=key,crawl_batch_id=job.get('crawl_batch_id'),status=status,
                bytes=value['bytes'],age_seconds=round(_age(value['reserved_at'],now),3)))
        oldest = max((v['age_seconds'] for v in items), default=0)
        # Alert before the oldest unprocessed batch's offsets can expire from Kafka.
        margin = None if self.kafka_retention_seconds is None else round(self.kafka_retention_seconds-oldest,3)
        return dict(pending_batches=len(items),pending_bytes=sum(v['bytes'] for v in items),oldest_age_seconds=oldest,
            kafka_retention_margin_seconds=margin,
            statuses=statuses,saturated=(len(items) >= self.limits.pending_batches or oldest > self.limits.pending_age_seconds
                or sum(v['bytes'] for v in items) >= self.limits.pending_bytes),
            limits=dict(pending_batches=self.limits.pending_batches,pending_bytes=self.limits.pending_bytes,
                pending_age_seconds=self.limits.pending_age_seconds),jobs=items)

    # Finite work -------------------------------------------------------------------------

    def _job(self, key):
        job = self.store.get('job',key)
        if not job:
            raise Invalid('BATCH_JOB_NOT_FOUND')
        if job['policy_hash'] != digest(self.policy) or job['target_hash'] != digest(self.target):
            raise Invalid('BATCH_RUNTIME_CHANGED')
        return job

    def work(self, key, phase='all'):
        if phase not in {'all','process','publish'}:
            raise Invalid('BATCH_PHASE_INVALID')
        job = self._job(key)
        if job['status'] in ACTIVE:
            raise Invalid('BATCH_RECOVERY_EVIDENCE_REQUIRED')
        measured = self.timing.measure('batch.total',job['crawl_batch_id']) if phase == 'all' else nullcontext()
        try:
            with measured:
                if phase in {'all','process'} and job['status'] == 'QUEUED':
                    with self.lock('process'):
                        job = self._process(key)
                if phase in {'all','publish'} and job['status'] == 'GOLD_COMMITTED':
                    with self.lock('publish'):
                        job = self._publish(key)
                elif phase == 'publish' and job['status'] != 'COMPLETED':
                    raise Invalid('BATCH_GOLD_NOT_COMMITTED')
        finally:
            self.timing.flush()
        return self.status(key)

    def _claim(self, key, expected, running):
        def claim(old):
            if old['status'] != expected:
                raise Invalid('BATCH_RECOVERY_EVIDENCE_REQUIRED' if old['status'] in ACTIVE else 'BATCH_STATE_CHANGED')
            update = dict(old,status=running,error_code=None,attempts=old['attempts']+1)
            update.setdefault('started_at',stamp())
            return update
        return self.store.mutate('job',key,claim)

    def _fail(self, key, phase, exc):
        # Unknown failures are not converted into quality outcomes or auto-reclaimed.
        self.store.mutate('job',key,lambda old:dict(old,status='RECOVERY_REQUIRED',failed_phase=phase,
            failed_at=stamp(),error_code=str(exc) if isinstance(exc,Invalid) else type(exc).__name__))

    def _process(self, key):
        job = self.store.get('job',key)
        manifest = None
        if job['kind'] == 'CRAWL':
            # Validate the fixed input before claiming, so a changed delivery never strands RUNNING.
            manifest = delivery_contract(self.store.get('delivery',[job['crawl_batch_id'],job['generation']]))
            if manifest['content_hash'] != job['delivery_hash']:
                raise Invalid('BATCH_DELIVERY_CHANGED')
        job = self._claim(key,'QUEUED','RUNNING')
        batch = job['crawl_batch_id']
        if job['attempts'] == 1:
            self._boundary('nifi.dispatch_wait',batch,job['submitted_at'],job['started_at'],identity=[key,job['attempts']])
        try:
            with self.timing.measure('batch.phase.process',batch):
                self.processor.process(job,manifest)
            return self.store.mutate('job',key,lambda old:dict(old,status='GOLD_COMMITTED',gold_committed_at=stamp()))
        except BaseException as exc:
            self._fail(key,'process',exc)
            raise

    def _deliveries(self, key, batch, gold, products):
        built = []
        with self.timing.measure('publish.build_requests',batch,counts={'products':len(products)}):
            for product in products:
                product_job = digest(dict(product=product['product_key'],body=digest(product)))
                built.append(dict(contract_version=2,job_id=product_job,product_key=product['product_key'],
                    input_hash=gold['input_hash'],target=self.publisher.identity,
                    gold=dict(proofs=gold['proofs'],gold_version=gold['gold_version']),scope_id=batch,product=product))
        # One keyed read and one bulk write per bounded chunk, not a round trip per product.
        saved = self.store.get_many('delivery_output',[d['job_id'] for d in built])
        fresh = [(d['job_id'],d) for d in built if saved[d['job_id']] is None]
        if fresh:
            self.store.put_many('delivery_output',fresh)
        return [dict(saved[d['job_id']] or d,scope_id=batch) for d in built]

    def _publish(self, key):
        job = self._claim(key,'GOLD_COMMITTED','PUBLISHING')
        batch = job['crawl_batch_id']
        try:
            with self.timing.measure('batch.phase.publish',batch):
                gold = verified(self.store.get('stage',[key,'gold']))
                published = self.store.get('stage',[key,'publication'])
                if not published:
                    started, resolutions, receipts = stamp(), {}, []
                    with self.timing.measure('mysql.publication_total',batch) as counts:
                        # Lake read time is accumulated between yields, excluding the publication work.
                        for products in self.timing.iterate('publish.lake_read',batch,self.processor.products(gold)):
                            # Materialized once per bounded chunk, outside service transactions.
                            requests = self._deliveries(key,batch,gold,products)
                            with self.timing.measure('mysql.publish_chunk',batch,counts={'products':len(requests)}):
                                if hasattr(self.publisher,'publish_many'):
                                    answers = self.publisher.publish_many(requests)
                                else:
                                    answers = [self.publisher.publish(delivery) for delivery in requests]
                            if len(answers) != len(requests):
                                raise Invalid('PUBLICATION_RECEIPT_COUNT_MISMATCH')
                            for delivery,receipt in zip(requests,answers):
                                verified(receipt)
                                if receipt['job_id'] != delivery['job_id'] or receipt['product_hash'] != digest(delivery['product']):
                                    raise Invalid('PUBLICATION_RECEIPT_CHANGED')
                            self.store.put_many('publication_receipt',[([key,delivery['job_id']],dict(delivery=delivery,receipt=receipt))
                                for delivery,receipt in zip(requests,answers)])
                            for delivery,receipt in zip(requests,answers):
                                resolutions[receipt['resolution']] = resolutions.get(receipt['resolution'],0)+1
                                receipts.append(delivery['job_id'])
                            counts['products'] = counts.get('products',0)+len(products)
                    published = seal(dict(stage='publication',status='COMPLETE',job_id=key,parent=gold['content_hash'],
                        gold_version=gold['gold_version'],resolutions=resolutions,receipts=sorted(receipts),
                        counts=dict(products=len(receipts)),attempts=job['attempts'],started_at=started,
                        completed_at=stamp(),code_version=CODE_VERSION))
                    self.store.put('stage',[key,'publication'],published)
            completed = published['completed_at']
            finalize = getattr(self.processor,'finalize',None)
            if finalize:
                # Work kept off the visible path (the raw commit) is durable before the job closes.
                with self.timing.measure('batch.finalize',batch):
                    finalize(job)
            job = self.store.mutate('job',key,lambda old:dict(old,status='COMPLETED',completed_at=completed))
            self._release(key)
            if job['kind'] == 'CRAWL':
                crawl = self.store.get('crawl',batch)['crawl']
                manifest = self.store.get('delivery',[batch,job['generation']])
                self._boundary('e2e.crawl_start_to_receipt',batch,crawl['started_at'],completed,identity=key)
                self._boundary('e2e.crawl_complete_to_receipt',batch,crawl['completed_at'],completed,identity=key)
                self._boundary('kafka.manifest_wait',batch,crawl['completed_at'],manifest['completed_at'],identity=manifest['content_hash'])
            return job
        except BaseException as exc:
            self._fail(key,'publish',exc)
            raise

    def abandon(self, key, evidence, reason):
        """Close a stopped or never-started job for good and release its capacity reservation.

        Without this, one job whose Kafka offsets expired holds its reservation forever and the
        pending-age limit eventually refuses every new submit. Committed Bronze history and stage
        records stay: they are valid accepted input that later batches keep reading.
        """
        if not isinstance(evidence,str) or not evidence.strip() or not isinstance(reason,str) or not reason.strip():
            raise Invalid('STOP_EVIDENCE_REQUIRED')
        def abandon(old):
            if not old or old['status'] not in {'RECOVERY_REQUIRED','QUEUED'}:
                raise Invalid('BATCH_NOT_ABANDONABLE')
            return dict(old,status='ABANDONED',abandon_evidence=evidence,abandon_reason=reason,abandoned_at=stamp())
        job = self.store.mutate('job',key,abandon)
        self._release(key)
        return job

    def recover(self, key, evidence, generation=None):
        if not isinstance(evidence,str) or not evidence.strip():
            raise Invalid('STOP_EVIDENCE_REQUIRED')
        gold = self.store.get('stage',[key,'gold'])
        def recover(old):
            if not old or old['status'] not in ACTIVE:
                raise Invalid('BATCH_NOT_RECOVERABLE')
            # Completed stage manifests are kept; only the unfinished phase is retried.
            update = dict(old,status='GOLD_COMMITTED' if gold else 'QUEUED',recovery_evidence=evidence,recovered_at=stamp())
            if generation is not None:
                if old['kind'] != 'CRAWL':
                    raise Invalid('RECOVERY_GENERATION_NOT_APPLICABLE')
                manifest = delivery_contract(self.store.get('delivery',[old['crawl_batch_id'],generation]))
                if manifest['event_set_hash'] != old['event_set_hash']:
                    raise Invalid('RECOVERY_EVENT_SET_CHANGED')
                update.update(generation=generation,delivery_hash=manifest['content_hash'])
            return update
        return self.store.mutate('job',key,recover)

    # Detailed audit and quarantine operations -------------------------------------------

    def audit(self, key):
        job = self.status(key)
        for name in ('bronze','silver','gold'):
            saved = self.store.get('stage',[key,name])
            if saved:
                self.processor.audit(verified(saved))
        published = self.store.get('stage',[key,'publication'])
        if published:
            verified(published)
            entries = self.store.get_list('publication_receipt',[[key,v] for v in published['receipts']])
            if any(v is None for v in entries):
                raise Invalid('PUBLICATION_RECEIPT_MISSING')
            for entry in entries:
                self.publisher.audit(entry['delivery'],entry['receipt'])
        return dict(status='AUDIT_OK',job_id=key,crawl_batch_id=job['crawl_batch_id'])

    def retention_purge(self, now=None):
        """Quarantine raw expires first, de-identified metadata later. Normal raw is untouched."""
        now = utc(now or stamp())
        raw = 0
        for record in list(self.store.values('quarantine_raw')):
            if record['raw_expires_at'] <= now:
                self.store.delete('quarantine_raw',record['event_id'])
                raw += 1
        metadata = self.processor.purge_quarantine_metadata(now)
        return dict(status='RETENTION_APPLIED',as_of=now,quarantine_raw_deleted=raw,metadata_deleted=metadata,
            policy=dict(quarantine_raw_days=self.retention.quarantine_raw_days,error_metadata_days=self.retention.error_metadata_days))

    def quarantine_read(self, event_id, purpose, operator):
        if not purpose or not operator:
            raise Invalid('QUARANTINE_ACCESS_PURPOSE_REQUIRED')
        record = self.store.get('quarantine_raw',event_id)
        # Every successful or missing lookup is recorded before any payload leaves the store.
        self.store.put('quarantine_access',str(uuid4()),dict(event_id=event_id,purpose=purpose,operator=operator,
            accessed_at=stamp(),found=record is not None))
        if record is None:
            raise Invalid('QUARANTINE_RECORD_NOT_FOUND')
        return record
