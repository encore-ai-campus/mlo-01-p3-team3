"""Read-only jsonl-v1 collector journal. Never acquire a collector lock."""
import hashlib
import json

from .errors import PipelineError
from .files import parse, read_json

MAX_EVENT_BYTES = 32 * 1024 * 1024


def event_hash(event):
    # Collector checkpoint.encoded: spaces are significant, unlike pipeline H.
    text = json.dumps(event, ensure_ascii=False, sort_keys=True, allow_nan=False)
    return hashlib.sha256(text.encode()).hexdigest()


def identity(source):
    config = read_json(source / 'run_config.json')
    state = read_json(source / 'checkpoint_state.json')
    if (config.get('storage') != 'jsonl-v1' or not config.get('job_id')
            or config['job_id'] != state.get('job_id')
            or type(state.get('commit')) is not int or state['commit'] < 0):
        raise PipelineError('SOURCE_IDENTITY_CHANGED')
    return config, state


def prefix(stream, end):
    stream.seek(0)
    checksum = hashlib.sha256()
    remaining = end
    while remaining:
        data = stream.read(min(1024 * 1024, remaining))
        if not data:
            raise PipelineError('SOURCE_IDENTITY_CHANGED')
        checksum.update(data)
        remaining -= len(data)
    return checksum


def envelopes(stream, cursor, ceiling, checksum=None):
    """Yield verified committed events and isolated interrupted-fragment receipts."""
    expected = cursor['last_seq'] + 1
    stream.seek(cursor['offset'])
    fragments = []
    skipped_bytes = 0
    checksum = checksum.copy() if checksum is not None else prefix(stream, cursor['offset'])
    while expected <= ceiling:
        start = stream.tell()
        before = checksum.copy()
        line = stream.readline(MAX_EVENT_BYTES + 1)
        if not line or len(line) > MAX_EVENT_BYTES or not line.endswith(b'\n'):
            raise PipelineError('SOURCE_COMMIT_INVALID')
        checksum.update(line)
        skipped_bytes += len(line)
        if skipped_bytes > MAX_EVENT_BYTES or len(fragments) >= 100:
            raise PipelineError('SOURCE_COMMIT_INVALID')
        if not line.strip():
            continue
        try:
            envelope = parse(line.decode('utf-8'))
        except (PipelineError, UnicodeError):
            fragments.append({'offset': start, 'bytes': len(line),
                              'sha256': hashlib.sha256(line).hexdigest(),
                              'status': 'INTERRUPTED_FRAGMENT'})
            continue
        if not isinstance(envelope, dict) or not isinstance(envelope.get('event'), dict):
            raise PipelineError('SOURCE_COMMIT_INVALID')
        event = envelope['event']
        if (type(event.get('seq')) is not int or event['seq'] != expected
                or not isinstance(event.get('records'), list)
                or envelope.get('sha256') != event_hash(event)):
            raise PipelineError('SOURCE_COMMIT_INVALID')
        if cursor.get('pending_seq') and (event['seq'] != cursor['pending_seq']
                or envelope['sha256'] != cursor['envelope_hash']):
            raise PipelineError('SOURCE_IDENTITY_CHANGED')
        yield start, stream.tell(), envelope, fragments, before, checksum.copy()
        fragments = []
        skipped_bytes = 0
        expected += 1
