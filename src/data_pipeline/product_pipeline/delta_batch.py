"""Preflight all immutable rows once; never retain negative lookup results."""
from .delta_proofs import existing_hashes_many
from .errors import Conflict
from .spark_session import measured


@measured('delta_output_insert_many')
def insert_many(sink,tables,*,hook=None):
    sink.lease.check(sink.settings)
    expected={}
    for table,rows in tables.items():
        expected[table]={r[sink.keys[table]]:r['record_hash'] for r in rows}
        if len(expected[table])!=len(rows):raise Conflict('SILVER_DUPLICATE_BATCH_OUTPUT')
        if any(r['record_hash']!=sink.hash_row(r) for r in rows):raise Conflict('SILVER_OUTPUT_CONFLICT')
    # Every existing row is checked before the first table commit. This map is
    # local to this call, so an uncertain commit must read storage on retry.
    existing=existing_hashes_many(sink,expected)
    for table,values in existing.items():
        if any(expected[table].get(k)!=h for k,h in values.items()):raise Conflict('SILVER_OUTPUT_CONFLICT')
    from .spark_session import bump
    for table,rows in tables.items():
        sink.lease.check(sink.settings)
        missing=[r for r in rows if r[sink.keys[table]] not in existing[table]]
        if missing:sink._append(table,missing)
        else:bump('delta_unchanged_tables_skipped')
        if hook:hook(table)
