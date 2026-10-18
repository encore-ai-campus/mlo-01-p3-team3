"""Run migration files through a real mysql client in a new loopback-only mysqld.

Never connects to the Windows service, VM, or a caller-supplied service database.
"""
import argparse
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import time
from uuid import UUID, uuid4

from product_pipeline.contracts import canonical
from product_pipeline.files import atomic_json
from product_pipeline.publication.migration import verify_bundle
from product_pipeline.publication.mysql import MySQL
from product_pipeline.publication.runner import publish
from product_pipeline.publication.schema import BASE_COLUMNS, FKS, PRIMARY, UNIQUE, reference_ddl
sys.path.insert(0, str(Path(__file__).resolve().parent))
from publication_fixture import fixture


def base_ddl():
    statements = []
    for table, cols in BASE_COLUMNS.items():
        parts = [f'`{name}` {kind}' + (' NULL' if nullable else ' NOT NULL')
                 for name, (kind, nullable) in cols.items()]
        parts.append('PRIMARY KEY (' + ','.join(f'`{c}`' for c in PRIMARY[table]) + ')')
        parts += ['UNIQUE KEY (' + ','.join(f'`{c}`' for c in keys) + ')'
                  for keys in UNIQUE.get(table, []) if set(keys).issubset(cols)]
        statements.append(f'CREATE TABLE `{table}` (' + ','.join(parts)
                          + ') ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;')
    for table, entries in FKS.items():
        for col, parent, pk in entries:
            if table in BASE_COLUMNS and col in BASE_COLUMNS[table] and parent in BASE_COLUMNS:
                statements.append(f'ALTER TABLE `{table}` ADD CONSTRAINT `fk_{table}_{col}` '
                                  f'FOREIGN KEY (`{col}`) REFERENCES `{parent}` (`{pk}`) ON DELETE RESTRICT;')
    return '\n'.join(statements)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--mysqld', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    executable = args.mysqld.resolve()
    client = executable.with_name('mysql.exe' if os.name == 'nt' else 'mysql')
    assert client.is_file()
    migration = Path(__file__).resolve().parents[1] / 'sql/migrations'
    bundle = verify_bundle(migration)
    args.output.mkdir(parents=True, exist_ok=False)
    data = (args.output / 'data').resolve()
    data.mkdir()
    flags = subprocess.CREATE_NO_WINDOW if os.name == 'nt' else 0
    init = subprocess.run([str(executable), '--no-defaults', '--initialize-insecure', '--datadir=' + str(data)],
                          capture_output=True, timeout=90, creationflags=flags)
    assert init.returncode == 0, 'ISOLATED_MYSQL_INITIALIZATION_FAILED'
    with socket.socket() as probe:
        probe.bind(('127.0.0.1', 0))
        port = probe.getsockname()[1]
    assert port != 3306
    log = (args.output / 'mysqld.log').open('wb')
    child = subprocess.Popen([str(executable), '--no-defaults', '--datadir=' + str(data),
                              '--bind-address=127.0.0.1', '--port=' + str(port), '--mysqlx=OFF'],
                             stdout=log, stderr=log, creationflags=flags)
    import pymysql
    admin = None
    report = dict(actual_mysql=True, vm_connections=0, service_data_modified=False,
                  port=port, migration_bundle=bundle, checks=[])
    try:
        for _ in range(90):
            if child.poll() is not None:
                raise RuntimeError('ISOLATED_MYSQL_EXITED')
            try:
                admin = pymysql.connect(host='127.0.0.1', port=port, user='root', charset='utf8mb4',
                                        autocommit=True, cursorclass=pymysql.cursors.DictCursor)
                break
            except pymysql.err.OperationalError:
                time.sleep(0.5)
        assert admin is not None

        def execute(sql):
            with admin.cursor() as cursor:
                cursor.execute(sql)
                return list(cursor.fetchall())

        def sql_file(database, sql, *, expected=None):
            result = subprocess.run([str(client), '--no-defaults', '--protocol=TCP', '--host=127.0.0.1',
                '--port=' + str(port), '--user=root', '--default-character-set=utf8mb4', '--batch',
                '--database=' + database], input=sql.encode('utf-8'), capture_output=True,
                timeout=90, creationflags=flags)
            output = (result.stdout + result.stderr).decode('utf-8', errors='replace')
            if expected:
                assert result.returncode != 0 and expected in output, output[-1500:]
            else:
                assert result.returncode == 0, output[-1500:]
            return output

        from publication_migration_cases import run_cases
        def connect(database):
            return pymysql.connect(host='127.0.0.1', port=port, user='root', database=database,
                                   charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)
        run_cases(execute, sql_file, connect, args.output, report, migration, base_ddl)
    finally:
        if admin:
            try:execute('SHUTDOWN')
            except Exception:pass
            admin.close()
        try:child.wait(timeout=20)
        except subprocess.TimeoutExpired:
            child.terminate()
            child.wait(timeout=10)
        log.close()


if __name__ == '__main__':
    main()
