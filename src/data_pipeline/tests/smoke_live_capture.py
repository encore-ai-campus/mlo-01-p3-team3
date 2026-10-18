"""Read a small real committed journal sample; all downstream systems are doubles."""
import argparse
from pathlib import Path

from product_pipeline.files import atomic_json, read_json, sha_file
from product_pipeline.handoff import drain_spool
from product_pipeline.live import capture


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--max-records', type=int, default=5)
    args = parser.parse_args()
    source, output = args.source.resolve(), args.output.resolve()
    if output.exists() or output.is_relative_to(source) or source.is_relative_to(output):
        parser.error('new output must be separate from the source')
    inputs = [source/name for name in ('run_config.json', 'checkpoint_state.json', 'checkpoint_events.jsonl')]
    before = {p.name: sha_file(p) for p in inputs}
    output.mkdir(parents=True)
    result = capture(source, output/'spool', max_spool_bytes=2*1024**3, min_free_bytes=0,
                     max_records=args.max_records)
    first = drain_spool(output/'spool', output/'state', rehearse=True)
    second = drain_spool(output/'spool', output/'state', rehearse=True)
    unchanged = before == {p.name: sha_file(p) for p in inputs}
    passed = unchanged and first['result'] == second['result'] == 'DRAINED' and second['batches_ingested'] == 0
    report = dict(passed=passed, captured=result, first=first, repeated=second,
                  original_inputs_unchanged=unchanged, external_service_connections=0,
                  note='Existing saved journal; not a concurrently running crawler or actual Mongo/S3/Kafka test.')
    atomic_json(output/'report.json', report)
    print(report)
    return 0 if passed else 2


if __name__ == '__main__':
    raise SystemExit(main())
