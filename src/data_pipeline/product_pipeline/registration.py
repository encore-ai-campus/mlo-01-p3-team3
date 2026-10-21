"""MySQL-backed Silver/Gold run registration, independent of publication approval.

No runtime DDL, grants, MongoDB access, or scheduler activation.
"""
import argparse
import json
import re
import time
from pathlib import Path
from uuid import UUID

from .contracts import canonical, digest, now
from .errors import Conflict, PipelineError
from .files import atomic_json, read_json

STAGES = {'NORMALIZATION', 'GOLD_BUILD'}
COLUMNS = {'id', 'type', 'input_range_json', 'code_version', 'status',
           'started_at', 'finished_at', 'created_at', 'updated_at'}


def validate_request(value):
    try:
        if not isinstance(value, dict) or set(value) != {
                'processing_run_id', 'stage', 'input_range', 'code_version'}:
            raise ValueError()
        uid = UUID(value['processing_run_id'])
        if str(uid) != value['processing_run_id'] or uid.version != 4:
            raise ValueError()
        if value['stage'] not in STAGES or not re.fullmatch(
                r'[a-f0-9]{64}', value['code_version']):
            raise ValueError()
        scope = value['input_range']
        if not isinstance(scope, dict) or len(canonical(scope).encode()) > 65536:
            raise ValueError()
        fields=set(scope)
        if 'window_hash' in fields:
            if not isinstance(scope['window_hash'],str) or not re.fullmatch('[a-f0-9]{64}',scope['window_hash']):raise ValueError()
            fields.remove('window_hash')
        if value['stage'] == 'NORMALIZATION':
            if 'reuse_selection_hash' in fields:
                if ('window_hash' not in scope or not isinstance(scope['reuse_selection_hash'],str)
                        or not re.fullmatch('[a-f0-9]{64}',scope['reuse_selection_hash'])):raise ValueError()
                fields.remove('reuse_selection_hash')
            keys={'bronze_path', 'bronze_version', 'inputs_hash'}
            if fields!=keys and fields!=keys|{'normalized_input'}:
                raise ValueError()
            if 'normalized_input' in scope and scope['normalized_input']!='danawa_products/observation-binding-v1':
                raise ValueError()
            if type(scope['bronze_version']) is not int or scope['bronze_version'] < 0:
                raise ValueError()
            location = scope['bronze_path']
        else:
            from .gold.contracts import validate_versions
            if fields != {'silver_root', 'silver_versions', 'inputs_hash'}:
                raise ValueError()
            validate_versions(scope['silver_versions'])
            location = scope['silver_root']
        if not isinstance(location, str) or not location.startswith(('file:', 's3a:')):
            raise ValueError()
        from urllib.parse import urlsplit
        if urlsplit(location).username or urlsplit(location).password or urlsplit(location).query:
            raise ValueError()
        if not re.fullmatch(r'[a-f0-9]{64}', scope['inputs_hash']):
            raise ValueError()
        return uid.hex
    except (KeyError, TypeError, ValueError, AttributeError) as exc:
        raise PipelineError('RUN_REGISTRATION_REQUEST_INVALID') from exc


def preflight(db):
    database = db.settings['database']
    table = db.one('SELECT ENGINE AS engine FROM information_schema.TABLES '
                   'WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s', (database, 'pipeline_runs'))
    if not table:
        raise PipelineError('RUN_REGISTRATION_TABLE_MISSING')
    columns = db.all('SELECT COLUMN_NAME AS name,COLUMN_TYPE AS kind,IS_NULLABLE AS nullable '
                     'FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s',
                     (database, 'pipeline_runs'))
    by_name = {x['name']: x for x in columns}
    if set(by_name) != COLUMNS or table['engine'].lower() != 'innodb':
        raise PipelineError('RUN_REGISTRATION_SCHEMA_MISMATCH')
    if by_name['id']['kind'].lower() != 'char(32)' or by_name['input_range_json']['kind'].lower() != 'json':
        raise PipelineError('RUN_REGISTRATION_SCHEMA_MISMATCH')
    for name in ('type', 'code_version', 'status'):
        if not by_name[name]['kind'].lower().startswith('varchar(') or by_name[name]['nullable'] != 'NO':
            raise PipelineError('RUN_REGISTRATION_SCHEMA_MISMATCH')
    for name in ('started_at', 'finished_at'):
        if not by_name[name]['kind'].lower().startswith('datetime') or by_name[name]['nullable'] != 'YES':
            raise PipelineError('RUN_REGISTRATION_SCHEMA_MISMATCH')
    primary = db.all('SELECT COLUMN_NAME AS name FROM information_schema.KEY_COLUMN_USAGE '
                     "WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s AND CONSTRAINT_NAME='PRIMARY' "
                     'ORDER BY ORDINAL_POSITION', (database, 'pipeline_runs'))
    if [x['name'] for x in primary] != ['id']:
        raise PipelineError('RUN_REGISTRATION_SCHEMA_MISMATCH')
    server = db.one('SELECT @@global.read_only AS ro,@@global.super_read_only AS sro')
    if server['ro'] or server['sro']:
        raise PipelineError('MYSQL_WRITABLE_PRIMARY_REQUIRED')
    return {'status': 'REGISTRATION_SCHEMA_VERIFIED', 'target': db.target}


def export_row(db, value):
    """Read committed registration back; no registration evidence without a DB row."""
    key = validate_request(value)
    row = db.one('SELECT id,type,input_range_json,code_version,status FROM pipeline_runs WHERE id=%s', (key,))
    if not row:
        raise PipelineError('RUN_REGISTRATION_NOT_FOUND')
    stored = json.loads(row['input_range_json']) if isinstance(row['input_range_json'], str) else row['input_range_json']
    if row['type'] != value['stage'] or stored != value['input_range'] or row['code_version'] != value['code_version']:
        raise Conflict('RUN_REGISTRATION_ID_CONFLICT')
    if row['status'] != 'REGISTERED':
        raise PipelineError('RUN_REGISTRATION_NOT_READY')
    return dict(processing_run_id=value['processing_run_id'], stage=value['stage'],
                status='REGISTERED', provider='MYSQL_PIPELINE_RUNS_EXPORT',
                evidence_ref='mysql-pipeline-runs://' + db.target + '/' + value['processing_run_id'],
                registration_hash=digest(value))


def register(db, value):
    key = validate_request(value)
    preflight(db)
    # Existing rows are never overwritten, including a conflicting reused UUID.
    with db.transaction():
        existing = db.one('SELECT id FROM pipeline_runs WHERE id=%s FOR UPDATE', (key,))
        if not existing:
            db.execute('INSERT INTO pipeline_runs '
                       '(id,type,input_range_json,code_version,status,started_at,finished_at,created_at,updated_at) '
                       'VALUES (%s,%s,%s,%s,%s,NULL,NULL,UTC_TIMESTAMP(3),UTC_TIMESTAMP(3))',
                       (key, value['stage'], canonical(value['input_range']), value['code_version'], 'REGISTERED'))
        result = export_row(db, value)
    # A committed row, rather than an optimistic write, supplies the exported proof.
    return export_row(db, value)


def mapping_snapshot(db, *, evidence_ref, version):
    """Read real mapping rows, never infer a category from its name or raw code."""
    from .silver.inputs import input_snapshot, validate_inputs, KINDS
    if not evidence_ref or not version:
        raise PipelineError('MAPPING_EVIDENCE_REQUIRED')
    tables = db.all('SELECT TABLE_NAME AS name FROM information_schema.TABLES WHERE TABLE_SCHEMA=%s',
                    (db.settings['database'],))
    names = {x['name'] for x in tables}
    missing = [name for name in ('categories', 'category_mappings') if name not in names]
    if missing:
        return {'snapshot': input_snapshot('mappings'), 'status': 'NOT_PROVIDED',
                'missing_tables': missing, 'reason': 'MAPPING_SOURCE_NOT_CONFIGURED'}
    columns = db.all('SELECT COLUMN_NAME AS name FROM information_schema.COLUMNS '
                     'WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s', (db.settings['database'], 'category_mappings'))
    expected = {'source_code', 'source_category', 'category_id', 'mapping_version', 'active'}
    if not expected.issubset({x['name'] for x in columns}):
        raise PipelineError('MAPPING_SCHEMA_ADAPTER_REQUIRED')
    rows = db.all('SELECT m.source_code,m.source_category,m.category_id,m.mapping_version,m.active,'
                  'c.code AS category_code,c.active AS category_active FROM category_mappings m '
                  'LEFT JOIN categories c ON c.id=m.category_id')
    output = []; seen = set()
    for row in rows:
        if not row['category_code']:
            raise PipelineError('MAPPING_CATEGORY_FK_INVALID')
        # The data dictionary defines source_category as a string. A lossless JSON
        # array export is required; a scalar code/path is ambiguous, not guessed.
        try: codes = json.loads(row['source_category'])
        except (ValueError, TypeError): raise PipelineError('MAPPING_SOURCE_PATH_ADAPTER_REQUIRED') from None
        if not isinstance(codes, list) or not codes or not all(isinstance(c, str) and c for c in codes):
            raise PipelineError('MAPPING_SOURCE_PATH_ADAPTER_REQUIRED')
        category = str(UUID(str(row['category_id'])))
        identity = (row['source_code'], canonical(codes))
        if identity in seen: raise Conflict('MAPPING_SOURCE_DUPLICATE')
        seen.add(identity)
        if row['active'] not in (0, 1) or row['category_active'] not in (0, 1):
            raise PipelineError('MAPPING_ACTIVE_FLAG_INVALID')
        output.append(dict(source_code=row['source_code'], source_category_codes=codes,
                           service_category_id=category, service_category_code=row['category_code'],
                           active=bool(row['active'] and row['category_active']), excluded=False))
    if not rows:
        return {'snapshot': input_snapshot('mappings'), 'status': 'NOT_PROVIDED',
                'missing_tables': [], 'reason': 'MAPPING_ROWS_ABSENT'}
    value = input_snapshot('mappings', output, version, evidence_ref)
    validate_inputs({k: value if k == 'mappings' else input_snapshot(k) for k in KINDS})
    return {'snapshot': value, 'status': 'PROVIDED', 'row_count': len(output), 'missing_tables': []}


def main(argv=None):
    parser = argparse.ArgumentParser(description='MySQL Silver/Gold 실행 등록·읽기 전용 매핑 export')
    parser.add_argument('command', choices=['preflight', 'register', 'export', 'mappings'])
    parser.add_argument('--config', required=True)
    parser.add_argument('--request')
    parser.add_argument('--output', required=True, type=Path)
    parser.add_argument('--apply', action='store_true')
    parser.add_argument('--evidence-ref')
    parser.add_argument('--mapping-version')
    args = parser.parse_args(argv)
    started = time.monotonic(); db = None
    try:
        if args.output.exists(): raise PipelineError('RUN_REGISTRATION_OUTPUT_EXISTS')
        if args.request and args.output.resolve() == Path(args.request).resolve():
            raise PipelineError('RUN_REGISTRATION_OUTPUT_OVERLAP')
        from .publication.mysql import MySQL
        db = MySQL(read_json(args.config))
        if args.command == 'mappings':
            value = mapping_snapshot(db, evidence_ref=args.evidence_ref, version=args.mapping_version)
        else:
            preflight(db)
            if args.command == 'preflight': value = preflight(db)
            else:
                if not args.request: raise PipelineError('RUN_REGISTRATION_REQUEST_REQUIRED')
                request = read_json(args.request)
                if args.command == 'register':
                    if not args.apply: raise PipelineError('RUN_REGISTRATION_APPLY_REQUIRED')
                    value = register(db, request)
                else: value = export_row(db, request)
        atomic_json(args.output, value)
        print(canonical({'status':'COMPLETED','command':args.command,'duration_ms':round((time.monotonic()-started)*1000)}))
        return 0
    except Exception as exc:
        print(canonical({'status':'FAILED','error_code':exc.code if isinstance(exc,PipelineError) else type(exc).__name__,
                         'duration_ms':round((time.monotonic()-started)*1000)}))
        return 2
    finally:
        if db: db.connection.close()
