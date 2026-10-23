"""Bounded table-wise commits, content-keyed Silver reuse and receipt-last Gold."""
from collections import defaultdict
from ..domain.contracts import digest, row, canonical, Invalid, pages, seal, SourcePageLimit
from ..domain.transform import normalize, silver_identity, gold


GOLD_LISTS = dict(images='gold_images', offers='gold_offers', attributes='gold_attributes',
    links='gold_links', price_observations='gold_prices', quality_reason_codes='gold_quality')


class Processor:
    def __init__(self, source, lake, state, limits):
        self.source, self.lake, self.state, self.limits = source, lake, state, limits
        self.metrics = dict(normalized=0, silver_reused=0, gold_reused=0, source_rows=0)
        self.blocked={}

    def _silver(self, references, policies, scope_id):
        try:
            with self.state.timing.measure('source.read_raw_page',scope_id,counts={'observations':len(references)}):
                items = self.source.read_many(references)
        except Invalid as exc:
            if str(exc) not in {'SOURCE_PAGE_BYTES','SOURCE_CHANGED','SOURCE_RANGE_MISSING_OR_DUPLICATE','IMAGE_STATE_MISSING'}:raise
            if len(references)>1:
                middle=len(references)//2
                return {**self._silver(references[:middle],policies,scope_id),**self._silver(references[middle:],policies,scope_id)}
            self.blocked[references[0]['product_key']]=str(exc)
            return {}
        self.metrics['source_rows'] += len(items)
        result = {}
        for chunk in pages(items, self.limits):
            from product_pipeline.bronze.contracts import validate_raw
            from product_pipeline.contracts import event_for
            from ..domain.transform import RUN
            bronze=[];observations=[]
            valid=[]
            from product_pipeline.bronze.contracts import Poison
            for item in chunk:
                doc=item['document']
                try:validate_raw(doc,event_for(doc,RUN,doc['collected_at']))
                except Poison as exc:
                    self.blocked[doc['source_code']+':'+doc['source_product_id']]=exc.code
                    continue
                valid.append(item)
                product=doc['source_code']+':'+doc['source_product_id']
                bronze.append(row(doc['_id'],product,doc))
                observations.append(row(doc['observation_key'],product,dict(raw_document_id=doc['_id'],document_hash=doc['document_hash'])))
            for table, values in (('bronze_raw',bronze),('bronze_observations',observations)):
                with self.state.timing.measure('bronze.table_commit',scope_id,counters=self.lake.metrics):
                    self.lake.verify(self.lake.commit(table,values))
            chunk=valid
            identities = {item['document']['_id']:silver_identity(item, policies[item['document']['source_code']+':'+item['document']['source_product_id']]) for item in chunk}
            existing = self.lake.read_keys('silver_receipts', list(identities.values()))
            output, metadata = defaultdict(list), []
            for item in chunk:
                doc = item['document']; identity = identities[doc['_id']]
                if identity in existing:
                    result[doc['_id']] = existing[identity]['payload']
                    self.metrics['silver_reused'] += 1
                    continue
                with self.state.timing.measure('silver.normalize_observation',scope_id,job_id=identities[doc['_id']]):
                    normalized = normalize(item, policies[doc['source_code']+':'+doc['source_product_id']])
                records = {}
                product = doc['source_code']+':'+doc['source_product_id']
                from product_pipeline.silver.normalize import KEYS
                for table, rows in normalized['tables'].items():
                    encoded = [row(value[KEYS[table]], product, value) for value in rows]
                    output['silver_'+table].extend(encoded)
                    records['silver_'+table] = {v['row_key']:v['record_hash'] for v in encoded}
                meta = dict(input_hash=identity, raw_ref=normalized['raw_ref'], direct_images=normalized['direct_images'], records=records,
                    row_count=sum(len(v) for v in normalized['tables'].values()),
                    table_bytes=sum(len(canonical(v).encode()) for values in normalized['tables'].values() for v in values))
                metadata.append(row(identity, product, meta)); result[doc['_id']] = meta
                self.metrics['normalized'] += 1
            # Every table commits once per bounded chunk, then the observation receipts.
            for table, rows in output.items():
                with self.state.timing.measure('silver.table_commit',scope_id,counters=self.lake.metrics):
                    proof = self.lake.commit(table, rows); self.lake.verify(proof)
            if metadata:
                with self.state.timing.measure('silver.receipts_commit',scope_id,counters=self.lake.metrics):
                    proof = self.lake.commit('silver_receipts', metadata); self.lake.verify(proof)
        return result

    def process_many(self, specs, scope_id=None):
        scope_id=scope_id or 'batch-'+specs[0]['job_id']
        with self.state.timing.measure('processing.total',scope_id,counters=self.lake.metrics):
            return self._process_many(specs,scope_id)

    def _process_many(self, specs, scope_id):
        self.blocked={}
        policies = {s['product_key']:s['policy'] for s in specs}
        references = {s['job_id']:list(self.state.refs(s)) for s in specs}
        completed, pending = {}, []
        for spec in specs:
            saved = self.state.receipt(spec['job_id'], 'gold')
            if saved:
                self.verify_gold(saved,scope_id)
                self.state.update(spec['job_id'],processing='GOLD_CONFIRMED',gold_outcome=saved['outcome'])
                completed[spec['job_id']] = saved
                self.metrics['gold_reused'] += 1
            else:
                pending.append(spec)
        if not pending: return completed
        metadata = {}
        flattened = (ref for s in pending for ref in references[s['job_id']])
        for page in pages(flattened, self.limits):
            metadata.update(self._silver(page, policies,scope_id))
        retained=[]
        for spec in pending:
            refs=references[spec['job_id']]
            reason=self.blocked.get(spec['product_key'])
            if not reason:
                counts=sum(metadata[r['raw_document_id']]['row_count'] for r in refs)
                size=sum(metadata[r['raw_document_id']]['table_bytes'] for r in refs)
                if counts>self.limits.history_rows or size>self.limits.history_bytes:reason='BLOCKED_HISTORY_LIMIT'
            if reason:self.state.update(spec['job_id'],processing='BLOCKED',error=reason)
            else:retained.append(spec)
        pending=retained
        needed={r['raw_document_id'] for s in pending for r in references[s['job_id']]}
        metadata={k:v for k,v in metadata.items() if k in needed}
        wanted = defaultdict(dict)
        for meta in metadata.values():
            for table, records in meta['records'].items(): wanted[table].update(records)
        tables = {}
        for table, records in wanted.items():
            with self.state.timing.measure('silver.read_pinned_rows',scope_id,counters=self.lake.metrics):
                actual = self.lake.read_keys(table, list(records))
            if {k:v['record_hash'] for k,v in actual.items()} != records: raise Invalid('SILVER_OUTPUT_MISSING')
            tables[table] = actual
        results, outputs = {}, defaultdict(list)
        for spec in pending:
            history, rows, amount = [], 0, 0
            for reference in references[spec['job_id']]:
                meta = metadata[reference['raw_document_id']]
                group = dict(raw_ref=meta['raw_ref'], direct_images=meta['direct_images'], tables={})
                for table, records in meta['records'].items():
                    group['tables'][table[len('silver_'):]] = [tables[table][k]['payload'] for k in records]
                rows += sum(len(v) for v in group['tables'].values())
                amount += len(canonical(group).encode())
                if rows > self.limits.history_rows or amount > self.limits.history_bytes:
                    self.state.update(spec['job_id'], processing='BLOCKED', error='BLOCKED_HISTORY_LIMIT', history_rows=rows, history_bytes=amount)
                    break
                history.append(group)
            else:
                with self.state.timing.measure('gold.reduce_product_history',scope_id,job_id=spec['job_id']):
                    result = gold(spec['product_key'], history, spec['policy'])
                records = {}
                for field, table in GOLD_LISTS.items():
                    encoded = [row(digest(dict(job=spec['job_id'], field=field, value=v)), spec['product_key'], dict(value=v)) for v in result[field]]
                    outputs[table].extend(encoded); records[table] = {v['row_key']:v['record_hash'] for v in encoded}
                catalog = {k:v for k,v in result.items() if k not in GOLD_LISTS}
                encoded = row(spec['job_id'], spec['product_key'], catalog)
                outputs['gold_catalog'].append(encoded); records['gold_catalog'] = {encoded['row_key']:encoded['record_hash']}
                results[spec['job_id']] = dict(job_id=spec['job_id'], product_key=spec['product_key'],
                    input_hash=spec['input_hash'], records=records, outcome=result['outcome'],retained_bytes=len(canonical(result).encode()))
        proofs = {}
        for table, rows in outputs.items():
            with self.state.timing.measure('gold.table_commit',scope_id,counters=self.lake.metrics):
                proofs[table] = self.lake.commit(table, rows)
                self.lake.verify(proofs[table])
        # No successful receipt exists until every required Gold table is confirmed.
        for key, result in results.items():
            own_proofs = []
            for table, records in result['records'].items():
                if not records: continue
                proof = seal({**{k:v for k,v in proofs[table].items() if k not in {'content_hash','records'}},'records':records})
                own_proofs.append(proof)
            receipt = dict(**result, proofs=own_proofs)
            with self.state.timing.measure('gold.receipt_commit',scope_id,job_id=key):
                self.state.save_receipt(key, 'gold', receipt)
            self.state.update(key, processing='GOLD_CONFIRMED', gold_outcome=result['outcome'])
            completed[key] = self.state.receipt(key, 'gold')
        return completed

    def verify_gold(self, receipt, scope_id=None):
        for proof in receipt['proofs']:
            with self.state.timing.measure('gold.verify_table_proof',scope_id or 'job-'+receipt['job_id'],job_id=receipt['job_id'],counters=self.lake.metrics):
                self.lake.verify(proof)

    def materialize(self, receipt, scope_id=None):
        self.verify_gold(receipt,scope_id)
        result = {}
        for table, records in receipt['records'].items():
            with self.state.timing.measure('gold.read_pinned_rows',scope_id or 'job-'+receipt['job_id'],job_id=receipt['job_id'],counters=self.lake.metrics):
                actual = self.lake.read_keys(table, list(records))
            if {k:v['record_hash'] for k,v in actual.items()} != records: raise Invalid('GOLD_OUTPUT_CHANGED')
            if table == 'gold_catalog': result.update(next(iter(actual.values()))['payload'])
            else:
                field = next(k for k,v in GOLD_LISTS.items() if v == table)
                result[field] = [actual[k]['payload']['value'] for k in sorted(actual)]
        return result
