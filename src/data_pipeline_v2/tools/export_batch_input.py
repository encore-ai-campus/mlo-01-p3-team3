"""Export fixed source observations from Mongo as a finite crawl input and its manifest.

Read-only against the source database. Used to replay an already-collected window (for example
the observation keys of a previous measurement) through the finite batch path without re-crawling.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import sys

HERE = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(HERE)]


def main(argv=None):
    parser = argparse.ArgumentParser(description='고정 관측 키 → 유한 배치 입력(JSONL)과 크롤 manifest')
    parser.add_argument('--mongo-uri-env', required=True)
    parser.add_argument('--database', required=True)
    parser.add_argument('--replica-set', required=True)
    parser.add_argument('--keys', type=Path, required=True, help='observation_keys 배열이 있는 JSON 또는 키 배열')
    parser.add_argument('--batch', required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--manifest', type=Path, required=True)
    args = parser.parse_args(argv)
    from pymongo import MongoClient, ReadPreference
    from pymongo.read_concern import ReadConcern
    from product_pipeline.mongo import wire_document
    from pipeline_v2.domain.batch import batch_id, source_contract
    from pipeline_v2.domain.contracts import canonical, utc
    batch_id(args.batch)
    if args.output.exists() or args.manifest.exists():
        raise SystemExit('OUTPUT_EXISTS')
    value = json.loads(args.keys.read_text(encoding='utf-8'))
    keys = value['observation_keys'] if isinstance(value, dict) else value
    if not keys or len(keys) != len(set(keys)) or len(keys) > 10000:
        raise SystemExit('OBSERVATION_KEYS_INVALID')
    from pipeline_v2.domain.contracts import stamp
    # A replay's crawl window is the export itself, so crawl-complete -> receipt measures the pipeline.
    started = stamp()
    client = MongoClient(os.environ[args.mongo_uri_env], replicaSet=args.replica_set, tz_aware=True,
        serverSelectionTimeoutMS=5000, socketTimeoutMS=60000, retryWrites=False)
    try:
        db = client.get_database(args.database, read_preference=ReadPreference.PRIMARY, read_concern=ReadConcern('majority'))
        rows = []
        for start in range(0, len(keys), 100):
            chunk = keys[start:start+100]
            docs = {d['observation_key']:wire_document(d) for d in db['raw_products'].find({'observation_key':{'$in':chunk}})}
            states = {s['observation_key']:s for s in db['ingestion_observations'].find({'observation_key':{'$in':chunk}},
                {'_id':0,'observation_key':1,'image_state_vector':1,'image_state_revision':1,
                 'image_state_fingerprint':1,'image_state_checked_at':1})}
            for key in chunk:
                if key not in docs or key not in states:
                    raise SystemExit('SOURCE_OBSERVATION_MISSING')
                state = dict(states[key], raw_document_id=docs[key]['_id'])
                if hasattr(state.get('image_state_checked_at'), 'isoformat'):
                    state['image_state_checked_at'] = state['image_state_checked_at'].isoformat()
                rows.append(dict(document=docs[key], image_state=state))
    finally:
        client.close()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    body = ''.join(canonical(row) + '\n' for row in rows).encode('utf-8')
    args.output.write_bytes(body)
    times = sorted(utc(r['document']['collected_at']) for r in rows)
    completed = stamp()
    manifest = dict(schema_version=1, crawl_batch_id=args.batch, status='SOURCE_COMPLETE',
        required_sources=['danawa'], completed_sources=['danawa'], event_count=len(rows),
        input_sha256=hashlib.sha256(body).hexdigest(), input_bytes=len(body),
        started_at=started, completed_at=completed, source_collected_from=times[0], source_collected_to=times[-1],
        input_semantics='OBSERVATION_SNAPSHOT',
        source_observation_keys_sha256=hashlib.sha256(canonical(sorted(keys)).encode()).hexdigest())
    source_contract(manifest)
    args.manifest.write_text(canonical(manifest), encoding='utf-8')
    print(json.dumps(dict(status='EXPORTED', events=len(rows), input_bytes=len(body),
        input_sha256=manifest['input_sha256'])))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
