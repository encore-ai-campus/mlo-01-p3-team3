"""Bounded, file-only LIVE capture; its cursor never means DB/Kafka success."""
import copy
import hashlib
import os
from pathlib import Path
import shutil
import tempfile

from .contracts import canonical, digest, now, observation_key
from .errors import PipelineError
from .files import atomic_bytes, atomic_json, lock, read_json, rows, sha_file, sync_dir
from .journal import envelopes, identity, prefix
from .snapshot import _gallery, _validate, product_key, verify


class CaptureSession:
    """Process-local cache for the collector's append-only history and owned spool.

    A new process always verifies the entire committed prefix and sealed chain.
    Active sessions trust the documented append-only source invariant; an in-place
    prefix rewrite is detected on restart, not promised on every polling cycle.
    """
    def __init__(self):
        self.cursor_hash = None
        self.prefix_state = None


class _Budget:
    def __init__(self, spool, maximum, minimum_free):
        self.spool, self.used = spool, _usage(spool)
        self.maximum, self.minimum_free = maximum, minimum_free

    def reserve(self, size):
        if self.used+size > self.maximum or shutil.disk_usage(self.spool).free < self.minimum_free+size:
            raise PipelineError('BACKPRESSURE')
        self.used += size


def _usage(root):
    return sum(p.stat().st_size for p in root.rglob('*') if p.is_file())


def _manifest(build, run_id, phase, control, count, images):
    files = {p.relative_to(build).as_posix(): dict(sha256=sha_file(p), bytes=p.stat().st_size)
             for p in sorted(build.rglob('*')) if p.is_file()}
    stable = dict(schema_version=1, handoff_schema_version=1, kind='LIVE_DELTA', phase=phase,
                  source_format='jsonl-v1', collection_run_id=run_id, files=files,
                  product_count=count, verified_image_relations=images, **control)
    return dict(stable, snapshot_id=digest(stable), prepared_at=now())


def verify_evidence(directory, manifest):
    from .journal import event_hash
    from .contracts import GALLERY_FIELDS
    if manifest['subscription']['job_id'] != manifest['collection_run_id']:
        raise PipelineError('LIVE_SOURCE_IDENTITY_INVALID')
    if manifest['phase'] == 'SOURCE_METADATA':
        events = {}
        for envelope in rows(directory/'events.jsonl'):
            event = envelope['event']
            if (envelope['sha256'] != event_hash(event) or event['seq'] > manifest['source_commit']
                    or event['seq'] in events):
                raise PipelineError('SOURCE_COMMIT_INVALID')
            events[event['seq']] = envelope
        evidence_keys = set()
        for pointer in manifest['source_range']['pointers']:
            try:
                envelope = events[pointer['seq']]
                pair = envelope['event']['records'][pointer['record_index']]
            except (KeyError, IndexError, TypeError) as exc:
                raise PipelineError('LIVE_POINTER_INVALID') from exc
            if envelope['sha256'] != pointer['envelope_hash'] or digest(pair['raw']) != pointer['source_observation_hash']:
                raise PipelineError('LIVE_POINTER_INVALID')
            _validate(pair['raw'], pair['normalized'], None)
            images = _gallery(pair['normalized'], None, None, directory)
            evidence_keys.add(observation_key(pair['raw'], images))
        actual = set()
        for row in rows(directory/'observations.jsonl'):
            # Exact raw/normal pair equality, not only a self-reported observation hash.
            pointer = row['lineage']['source_pointer']
            pair = events[pointer['seq']]['event']['records'][pointer['record_index']]
            if row['raw'] != pair['raw'] or row['normalized'] != pair['normalized']:
                raise PipelineError('LIVE_PAIR_CHANGED')
            actual.add(row['observation_key'])
        if actual != evidence_keys:
            raise PipelineError('LIVE_POINTER_SET_MISMATCH')
    else:
        selected = {product_key(r['raw']): r for r in rows(directory/'observations.jsonl')}
        media = {product_key(r): r for r in rows(directory/'media_manifest.jsonl') if product_key(r) in selected}
        if sha_file(directory/'media_manifest.jsonl') != manifest['source_range']['media_selection_hash']:
            raise PipelineError('LIVE_MEDIA_MANIFEST_CHANGED')
        for row in rows(directory/'observations.jsonl'):
            localized = media.get(product_key(row['raw']))
            _validate(row['raw'], row['normalized'], localized)
            if localized is None or [{k: i.get(k) for k in GALLERY_FIELDS} for i in localized.get('images') or []] != [
                    {k: i.get(k) for k in GALLERY_FIELDS} for i in row['images']]:
                raise PipelineError('LIVE_MEDIA_PAIR_MISMATCH')
        expected = {}
        for row in selected.values():
            for pointer in row['lineage']['source_pointers']:
                expected[(pointer['seq'], pointer['record_index'], pointer['envelope_hash'])] = row
        found = set()
        for envelope in rows(directory/'source_events.jsonl'):
            event = envelope['event']
            if envelope['sha256'] != event_hash(event) or event['seq'] > manifest['source_commit']:
                raise PipelineError('SOURCE_COMMIT_INVALID')
            for key, row in expected.items():
                if key[0] != event['seq'] or key[2] != envelope['sha256']:
                    continue
                pair = event['records'][key[1]]
                if pair['raw'] != row['raw'] or pair['normalized'] != row['normalized']:
                    raise PipelineError('LIVE_MEDIA_SOURCE_CHANGED')
                found.add(key)
        if found != set(expected):
            raise PipelineError('LIVE_MEDIA_EVIDENCE_MISSING')


def _seal(build, spool, manifest, max_bytes, min_free):
    # Temporary files already occupy spool; compare full retained usage before publish.
    if _usage(spool) + len(canonical(manifest).encode()) > max_bytes or shutil.disk_usage(spool).free < min_free:
        return None
    atomic_json(build/'snapshot.json', manifest)
    verify(build)
    target = spool/'batches'/manifest['snapshot_id']
    target.parent.mkdir(exist_ok=True)
    if target.exists():
        if verify(target)['snapshot_id'] != manifest['snapshot_id']:
            raise PipelineError('LIVE_SNAPSHOT_CONFLICT')
    else:
        build.rename(target)
        sync_dir(target.parent)
    return target


def _restore(spool, subscription, state, session=None):
    path = spool/'cursor.json'
    if not path.exists():
        return dict(subscription=subscription, offset=0, last_seq=0, pending_seq=None,
                    next_record_index=0, envelope_hash=None, prefix_sha256=hashlib.sha256(b'').hexdigest(),
                    generation=0, snapshot_id=None, chain=[])
    cursor = read_json(path)
    if (cursor['subscription'] != subscription or cursor['last_seq'] > state['commit']
            or (cursor.get('pending_seq') or 0) > state['commit']):
        raise PipelineError('SOURCE_IDENTITY_CHANGED')
    if session is not None and session.cursor_hash == digest(cursor):
        return cursor
    # Verify all referenced evidence on restart, not just the last batch.
    predecessor = None
    for generation, snapshot_id in enumerate(cursor['chain'], 1):
        try:
            manifest = verify(spool/'batches'/snapshot_id)
            if (manifest['snapshot_id'] != snapshot_id or manifest['predecessor_snapshot_id'] != predecessor
                    or manifest['capture_generation'] != generation or manifest['subscription'] != subscription):
                raise PipelineError('HANDOFF_CHAIN_INVALID')
            predecessor = snapshot_id
        except (PipelineError, OSError) as exc:
            raise PipelineError('RECOVERY_EVIDENCE_MISSING') from exc
    if (predecessor != cursor['snapshot_id'] or cursor['generation'] != len(cursor['chain'])
            or (predecessor is not None and manifest['capture_cursor_end'] != {
                k: v for k, v in cursor.items() if k not in {'chain', 'snapshot_id', 'generation'}})):
        raise PipelineError('RECOVERY_CURSOR_CONFLICT')
    if session is not None:
        session.cursor_hash = digest(cursor)
    return cursor


def _write_row(stream, raw, normal, images, lineage, budget=None):
    row = dict(raw=raw, normalized=normal, images=images, lineage=lineage,
               observation_key=observation_key(raw, images))
    encoded = canonical(row)+'\n'
    if budget is not None:
        budget.reserve(len(encoded.encode()))
    stream.write(encoded)
    return row['observation_key']


def _source_batch(source, spool, build, cursor, state, max_records, max_bytes, budget, session):
    updated, evidence, fragments, pointers = copy.deepcopy(cursor), [], [], []
    count, image_count, seen, byte_count = 0, 0, set(), 0
    journal = source/'checkpoint_events.jsonl'
    if not journal.exists() and state['commit'] == 0 and cursor['offset'] == 0:
        atomic_bytes(build/'observations.jsonl', b'')
        return None, cursor
    with journal.open('rb') as stream, (build/'observations.jsonl').open('w', encoding='utf-8', newline='\n') as output:
        file_identity = (os.fstat(stream.fileno()).st_dev, os.fstat(stream.fileno()).st_ino)
        cached = session.prefix_state if session is not None else None
        if cached is not None and cached[:2] == (file_identity, cursor['offset']):
            if journal.stat().st_size < cursor['offset']:
                raise PipelineError('SOURCE_IDENTITY_CHANGED')
            checksum = cached[2].copy()
        else:
            checksum = prefix(stream, cursor['offset'])
        if checksum.hexdigest() != cursor['prefix_sha256']:
            raise PipelineError('SOURCE_IDENTITY_CHANGED')
        verified_prefix = checksum.copy()
        for start, end, envelope, skipped, before, after in envelopes(stream, cursor, state['commit'], checksum):
            event, first = envelope['event'], updated['next_record_index']
            if first > len(event['records']):
                raise PipelineError('SOURCE_COMMIT_INVALID')
            envelope_bytes = len(canonical(envelope).encode())+1
            if evidence and (len(evidence) >= max_records or byte_count+envelope_bytes > max_bytes):
                break
            budget.reserve(envelope_bytes)
            evidence.append(envelope)
            byte_count += envelope_bytes
            fragments.extend(skipped)
            updated.update(offset=start, pending_seq=event['seq'], envelope_hash=envelope['sha256'])
            verified_prefix = before
            for index in range(first, len(event['records'])):
                pair = event['records'][index]
                raw, normal = pair['raw'], pair['normalized']
                _validate(raw, normal, None)
                if raw['job_id'] != cursor['subscription']['job_id']:
                    raise PipelineError('SOURCE_IDENTITY_CHANGED')
                images = _gallery(normal, None, None, build)
                projected = len(canonical(pair).encode())
                if pointers and (len(pointers) >= max_records or byte_count + projected > max_bytes):
                    break
                pointer = dict(seq=event['seq'], record_index=index, envelope_hash=envelope['sha256'],
                               source_observation_hash=digest(raw))
                pointers.append(pointer)
                key = observation_key(raw, images)
                if key not in seen:
                    _write_row(output, raw, normal, images, dict(status='OBSERVED',
                        linkage_method='COMMITTED_JOURNAL_PAIR', source_pointer=pointer), budget)
                    seen.add(key)
                    count += 1
                byte_count += projected
                updated['next_record_index'] = index+1
            if updated['next_record_index'] < len(event['records']):
                break
            updated.update(offset=end, last_seq=event['seq'], pending_seq=None,
                           next_record_index=0, envelope_hash=envelope['sha256'])
            verified_prefix = after
            if len(pointers) >= max_records or byte_count >= max_bytes:
                break
        output.flush()
        os.fsync(output.fileno())
        # Include whitespace/fragments before the next pending event in prefix evidence.
        updated['prefix_sha256'] = verified_prefix.hexdigest()
        if session is None and prefix(stream, updated['offset']).hexdigest() != updated['prefix_sha256']:
            raise PipelineError('SOURCE_CHANGED_DURING_CAPTURE')
        if (journal.stat().st_dev, journal.stat().st_ino) != file_identity:
            raise PipelineError('SOURCE_IDENTITY_CHANGED')
        if session is not None:
            session.prefix_state = (file_identity, updated['offset'], verified_prefix.copy())
    if not evidence:
        return None, cursor
    atomic_bytes(build/'events.jsonl', ''.join(canonical(e)+'\n' for e in evidence).encode())
    control = dict(source_range=dict(from_seq=cursor['last_seq']+1, to_seq=evidence[-1]['event']['seq'],
                                    from_record_index=cursor['next_record_index'],
                                    to_record_index=updated['next_record_index'], pointers=pointers),
                   source_fragments=fragments, source_commit=state['commit'])
    return (control, count, image_count), updated


def _gallery_batch(source, spool, build, cursor, state, media_manifest, max_records, budget):
    """Join a frozen media stream to captured committed pairs via disk partitions."""
    from .snapshot import _partition, _sorted_unique
    # Copy once, then parse exactly that immutable manifest version.
    frozen = build.parent/'frozen-media.jsonl'
    with media_manifest.open('rb') as src, frozen.open('xb') as dst:
        while data := src.read(1024*1024):
            budget.reserve(len(data))
            dst.write(data)
        dst.flush()
        os.fsync(dst.fileno())
    manifest_hash = sha_file(frozen)
    media_cursor = cursor.get('media', {})
    start = media_cursor.get('next_index', 0) if media_cursor.get('manifest_hash') == manifest_hash else 0
    if media_cursor.get('manifest_hash') == manifest_hash and media_cursor.get('complete'):
        return None, cursor, []
    candidates = build.parent/'candidates'
    candidates.mkdir()
    # Bounded memory and one pass over captured batches; never read mutable product mirrors.
    for snapshot_id in cursor['chain']:
        directory = spool/'batches'/snapshot_id
        manifest = read_json(directory/'snapshot.json')
        if manifest['phase'] != 'SOURCE_METADATA':
            continue
        for row in rows(directory/'observations.jsonl'):
            row = copy.deepcopy(row)
            row['lineage']['source_batch'] = snapshot_id
            row['lineage']['source_pointers'] = [p for p in manifest['source_range']['pointers']
                if p['source_observation_hash'] == digest(row['raw'])]
            part = digest(list(product_key(row['raw'])))[:2]
            encoded = canonical(row)+'\n'
            budget.reserve(len(encoded.encode()))
            with (candidates/(part+'.jsonl')).open('a', encoding='utf-8', newline='\n') as f:
                f.write(encoded)
    media_parts = build.parent/'media-parts'
    budget.reserve(frozen.stat().st_size*2)  # partition and bounded sort files, before writes
    _partition(frozen, media_parts)
    count, image_count, processed, waits, matching, exhausted = 0, 0, 0, [], [], True
    output = build/'observations.jsonl'
    selected_media = build/'media_manifest.jsonl'
    evidence_written = set()
    with output.open('w', encoding='utf-8', newline='\n') as out, selected_media.open('w', encoding='utf-8', newline='\n') as selected, (build/'source_events.jsonl').open('w', encoding='utf-8', newline='\n') as proof:
        for number in range(256):
            name = f'{number:02x}.jsonl'
            sort_dir = build.parent/f'sort-{number}'
            sort_dir.mkdir()
            # Candidate partition streamed for each bounded media group; no all-source dict.
            for localized in _sorted_unique(media_parts/name, sort_dir):
                index = processed
                processed += 1
                if index < start:
                    continue
                if index - start >= max_records:
                    processed -= 1
                    exhausted = False
                    break
                encoded = canonical(localized)+'\n'
                budget.reserve(len(encoded.encode()))
                selected.write(encoded)
                groups = {}
                path = candidates/name
                if path.exists():
                    for row in rows(path):
                        if product_key(row['raw']) != product_key(localized):
                            continue
                        try:
                            _validate(row['raw'], row['normalized'], localized)
                            # Validate gallery semantics without opening any downloaded files.
                            projection = copy.deepcopy(localized)
                            for image in projection.get('images') or []:
                                image['status'] = 'unavailable'
                            _gallery(row['normalized'], projection, None, build)
                        except PipelineError:
                            continue
                        wrapper = digest(row['raw'])
                        group = groups.setdefault(wrapper, dict(row=row, pointers=[], batches=set()))
                        group['pointers'].extend(row['lineage']['source_pointers'])
                        group['batches'].add(row['lineage']['source_batch'])
                        if len(groups) > 1:
                            break  # Two different wrappers already prove ambiguity; keep memory bounded.
                if len(groups) != 1:
                    status = 'LINK_AMBIGUOUS' if groups else ('WAIT_SOURCE_MATCH' if state['status'] == 'running' else 'MEDIA_SOURCE_MISMATCH')
                    waits.append(dict(source=localized['source'], product_id=localized['product_id'], status=status,
                                      manifest_hash=manifest_hash, groups=len(groups)))
                    continue
                wrapper, group = next(iter(groups.items()))
                row = group['row']
                required_seq = {p['seq'] for p in group['pointers']}
                for snapshot_id in sorted(group['batches']):
                    for envelope in rows(spool/'batches'/snapshot_id/'events.jsonl'):
                        if envelope['event']['seq'] in required_seq and envelope['sha256'] not in evidence_written:
                            encoded = canonical(envelope)+'\n'
                            budget.reserve(len(encoded.encode()))
                            proof.write(encoded)
                            evidence_written.add(envelope['sha256'])
                images = _gallery(row['normalized'], localized, media_manifest.parent, build, budget=budget)
                lineage = dict(status='OBSERVED', linkage_method='HASH_AND_GALLERY_EQUIVALENCE',
                               source_pointers=group['pointers'], source_observation_hash=wrapper,
                               media_manifest_hash=manifest_hash)
                _write_row(out, row['raw'], row['normalized'], images, lineage, budget)
                matching.append(dict(source_observation_hash=wrapper, source_pointers=group['pointers']))
                count += 1
                image_count += sum(i['acquisition_state'] == 'VERIFIED' for i in images)
            if not exhausted:
                break
        out.flush()
        os.fsync(out.fileno())
        selected.flush()
        os.fsync(selected.fileno())
        proof.flush()
        os.fsync(proof.fileno())
    updated = copy.deepcopy(cursor)
    # Retry unmatched candidates after source advances, without claiming complete linkage.
    previous_unresolved = media_cursor.get('unresolved_count', 0) if start else 0
    updated['media'] = dict(manifest_hash=manifest_hash, next_index=processed, complete=exhausted,
                            source_seq=cursor['last_seq'], source_status=state['status'],
                            unresolved_count=previous_unresolved+len(waits), waits=waits)
    control = dict(source_range=dict(media_manifest_hash=manifest_hash, media_selection_hash=sha_file(selected_media), from_index=start,
                                    next_index=processed, matches=matching), source_fragments=[],
                   source_commit=state['commit'], media_wait=waits)
    return (control, count, image_count), updated, waits


def capture(source, spool, *, max_spool_bytes, min_free_bytes, max_records=100,
            max_batch_bytes=32*1024*1024, media_manifest=None, hook=None, session=None):
    source, spool = Path(source).resolve(), Path(spool).resolve()
    if source.is_relative_to(spool) or spool.is_relative_to(source):
        raise PipelineError('LIVE_SPOOL_NOT_SEPARATE')
    if (type(max_spool_bytes) is not int or max_spool_bytes < 1
            or type(min_free_bytes) is not int or min_free_bytes < 0
            or type(max_records) is not int or not 1 <= max_records <= 100
            or type(max_batch_bytes) is not int or not 1 <= max_batch_bytes <= 32*1024*1024):
        raise PipelineError('LIVE_LIMITS_REQUIRED')
    spool.mkdir(parents=True, exist_ok=True)
    with lock(spool/'capture.lock'):
        config, state = identity(source)
        subscription = dict(job_id=config['job_id'], source_format=config['storage'], config_hash=digest(config))
        cursor = _restore(spool, subscription, state, session)
        if _usage(spool) >= max_spool_bytes or shutil.disk_usage(spool).free < min_free_bytes:
            return dict(result='BACKPRESSURE', cursor_advanced=False)
        with tempfile.TemporaryDirectory(prefix='.capture-', dir=spool) as tmp:
            build = Path(tmp)/'sealed'
            build.mkdir()
            budget = _Budget(spool, max_spool_bytes, min_free_bytes)
            try:
                result, updated = _source_batch(source, spool, build, cursor, state, max_records, max_batch_bytes, budget, session)
            except PipelineError as exc:
                if exc.code == 'BACKPRESSURE': return dict(result='BACKPRESSURE', cursor_advanced=False)
                raise
            waits, phase = [], 'SOURCE_METADATA'
            if result is None and media_manifest is not None:
                if not Path(media_manifest).is_file():
                    return dict(result='IDLE', source_commit=state['commit'], captured_seq=cursor['last_seq'],
                                media_status='WAIT_MEDIA_MANIFEST')
                phase = 'GALLERY_ACQUISITION'
                if (cursor.get('media', {}).get('unresolved_count') and
                        (cursor['media'].get('source_seq') != cursor['last_seq']
                         or cursor['media'].get('source_status') != state['status'])):
                    cursor = copy.deepcopy(cursor)
                    cursor.pop('media', None)
                try:
                    result, updated, waits = _gallery_batch(source, spool, build, cursor, state, Path(media_manifest).resolve(), max_records, budget)
                except PipelineError as exc:
                    if exc.code == 'BACKPRESSURE': return dict(result='BACKPRESSURE', cursor_advanced=False)
                    raise
            if result is None:
                return dict(result='IDLE', source_commit=state['commit'], captured_seq=cursor['last_seq'],
                            source_status=state['status'], media_wait=cursor.get('media', {}).get('waits', []))
            control, count, images = result
            generation = cursor['generation']+1
            control.update(subscription=subscription, capture_generation=generation,
                           predecessor_snapshot_id=cursor['snapshot_id'],
                           capture_cursor_end={k: v for k, v in updated.items() if k not in {'chain', 'snapshot_id', 'generation'}})
            manifest = _manifest(build, config['job_id'], phase, control, count, images)
            updated.update(generation=generation, snapshot_id=manifest['snapshot_id'],
                           chain=cursor['chain']+[manifest['snapshot_id']])
            try:
                # Reserve atomic-replace temporary cursor plus final manifest before publishing.
                budget.reserve(len(canonical(updated).encode())+len(canonical(manifest).encode()))
            except PipelineError as exc:
                if exc.code == 'BACKPRESSURE': return dict(result='BACKPRESSURE', cursor_advanced=False)
                raise
            target = _seal(build, spool, manifest, max_spool_bytes, min_free_bytes)
            if target is None:
                return dict(result='BACKPRESSURE', cursor_advanced=False)
            if hook: hook('SEALED')
            atomic_json(spool/'cursor.json', updated)
            if session is not None:
                session.cursor_hash = digest(updated)
            return dict(result='CAPTURED_WAITING_INGEST', snapshot=str(target),
                        snapshot_id=manifest['snapshot_id'], phase=phase, records=count,
                        source_commit=state['commit'], media_wait=waits)
