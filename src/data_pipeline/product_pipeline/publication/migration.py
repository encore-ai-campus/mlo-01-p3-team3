"""Read-only release checks for owner-supplied schema migrations; never execute SQL."""
import hashlib
from pathlib import Path
import re

from ..errors import PipelineError
from ..files import read_json
from .schema import CONTRACT_HASH, FKS, PRIMARY, TABLES, UNIQUE, VERSION

SCRIPTS = ('feature25_apply.sql', 'feature25_rollback.sql')


def verify_bundle(directory):
    directory = Path(directory)
    manifest = read_json(directory / 'manifest.json')
    if (manifest.get('format_version') != 1 or manifest.get('contract_hash') != CONTRACT_HASH
            or manifest.get('schema_version') != VERSION
            or manifest.get('hash_normalization') != 'utf8-no-bom-lf'
            or set(manifest.get('scripts', {})) != set(SCRIPTS)):
        raise PipelineError('MYSQL_MIGRATION_CONTRACT_MISMATCH')
    scripts = {}
    for name in SCRIPTS:
        text = (directory / name).read_text(encoding='utf-8-sig')
        checksum = hashlib.sha256(text.encode('utf-8')).hexdigest()
        if checksum != manifest['scripts'][name]['sha256']:
            raise PipelineError('MYSQL_MIGRATION_FILE_CHANGED', name)
        if manifest['migration_key'] not in text or set(re.findall(r"schema_hash='([0-9a-f]{64})'", text)) != {CONTRACT_HASH}:
            raise PipelineError('MYSQL_MIGRATION_CONTRACT_MISMATCH', name)
        scripts[name] = text
    apply = scripts[SCRIPTS[0]]
    columns = {(table, col): (kind, nullable == 'YES') for table, col, kind, nullable
               in re.findall(r"\('([^']+)','([^']+)','([^']+)','(YES|NO)',[01]\)", apply)}
    expected = {(table, col): value for table, cols in TABLES.items() for col, value in cols.items()}
    keys = re.findall(r"\('([^']+)','(PRIMARY|UNIQUE)','([^']+)'\)", apply)
    primary = {(table, tuple(cols.split(','))) for table, kind, cols in keys if kind == 'PRIMARY'}
    unique = {(table, tuple(cols.split(','))) for table, kind, cols in keys if kind == 'UNIQUE'}
    fks = set(re.findall(r"\('([^']+)','([^']+)','([^']+)','([^']+)'\)",
                        apply.split('INSERT INTO f25_fks VALUES', 1)[-1].split('DROP PROCEDURE', 1)[0]))
    if (columns != expected or primary != {(table, tuple(cols)) for table, cols in PRIMARY.items()}
            or unique != {(table, tuple(cols)) for table, keys in UNIQUE.items() for cols in keys}
            or fks != {(table, col, parent, pk) for table, entries in FKS.items() for col, parent, pk in entries}):
        raise PipelineError('MYSQL_MIGRATION_SCHEMA_MISMATCH')
    return dict(status='MYSQL_MIGRATION_BUNDLE_VERIFIED_NOT_APPLIED', schema_version=VERSION,
                contract_hash=CONTRACT_HASH, columns=len(columns), primary_keys=len(primary),
                unique_keys=len(unique), foreign_keys=len(fks), scripts=list(SCRIPTS),
                external_connections=0, ddl_applied=False, service_rows_changed=False)
