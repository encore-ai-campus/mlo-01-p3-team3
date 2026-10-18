"""Ansible root module: explicitly bootstrap a NEW document-contract DB only.

Existing databases/accounts are refused unless this module's completed private
journal owns them. Does not populate approvals, publish products, or change TLS.
Passwords are generated on the server and never included in module output.
"""
from ansible.module_utils.basic import AnsibleModule
import hashlib
import json
import os
from pathlib import Path
import re
import secrets
import subprocess


def main():
    module = AnsibleModule(argument_spec=dict(
        ddl=dict(type='path', required=True), ddl_sha256=dict(required=True),
        database=dict(required=True), validation_database=dict(required=True),
        user=dict(required=True), client_host=dict(required=True),
        state_file=dict(type='path', required=True)), supports_check_mode=True)
    p = module.params
    def sql(statement):
        result = subprocess.run(['mysql', '--protocol=SOCKET', '--batch', '--skip-column-names'],
                                input=statement, text=True, capture_output=True, timeout=90)
        if result.returncode:
            # MySQL errors can echo credential SQL. Keep private evidence only.
            raise RuntimeError('MYSQL_ADMIN_COMMAND_FAILED; inspect private bootstrap journal')
        return result.stdout.strip()
    def write_private(path, value):
        path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        os.chmod(path.parent, 0o700)
        fd = os.open(str(path), os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        with os.fdopen(fd, 'w') as f:
            json.dump(value, f, indent=2); f.flush(); os.fsync(f.fileno())
    try:
        for key in ('database', 'validation_database', 'user'):
            if not re.fullmatch(r'[a-z][a-z0-9_]{2,45}', p[key]):
                raise RuntimeError('MYSQL_IDENTIFIER_INVALID')
        if p['database'] == p['validation_database'] or not p['validation_database'].endswith('_validation'):
            raise RuntimeError('MYSQL_VALIDATION_DATABASE_MUST_BE_SEPARATE')
        if not re.fullmatch(r'\d{1,3}(?:\.\d{1,3}){3}', p['client_host']):
            raise RuntimeError('MYSQL_EXACT_CLIENT_IP_REQUIRED')
        ddl_path = Path(p['ddl']); ddl = ddl_path.read_bytes()
        if hashlib.sha256(ddl).hexdigest() != p['ddl_sha256']:
            raise RuntimeError('MYSQL_BOOTSTRAP_DDL_HASH_MISMATCH')
        text = ddl.decode('utf-8-sig').replace('\r\n', '\n')
        tables = re.findall(r'^CREATE TABLE `([^`]+)`', text, re.M)
        required = {'categories','products','offers','offer_snapshots','assets','product_images',
                    'pipeline_publication_guards','pipeline_product_versions','pipeline_publications',
                    'pipeline_publication_staging','product_link_observations','product_embeddings','search_index_versions'}
        if len(tables) != 13 or set(tables) != required:
            raise RuntimeError('MYSQL_BOOTSTRAP_TABLE_SET_MISMATCH')
        server = sql("SELECT @@hostname,@@version,@@global.read_only,@@global.super_read_only,@@require_secure_transport")
        if server.split('\t')[-3:] != ['0', '0', '1']:
            raise RuntimeError('MYSQL_WRITABLE_TLS_PRIMARY_REQUIRED')
        state_path = Path(p['state_file'])
        identity = {key:p[key] for key in ('database','validation_database','user','client_host','ddl_sha256')}
        if state_path.exists():
            state = json.loads(state_path.read_text())
            if state.get('identity') != identity or state.get('status') != 'COMPLETE':
                raise RuntimeError('MYSQL_BOOTSTRAP_INCOMPLETE_OR_DIFFERENT; no automatic destructive recovery')
            module.exit_json(changed=False, status='MYSQL_BOOTSTRAP_ALREADY_PREPARED', server=server,
                             databases=[p['database'],p['validation_database']], tables_per_database=13,
                             credential_file=str(state_path))
        names = [p['database'], p['validation_database']]
        for name in names:
            if sql("SELECT COUNT(*) FROM information_schema.SCHEMATA WHERE SCHEMA_NAME='"+name+"'") != '0':
                raise RuntimeError('MYSQL_EXISTING_DATABASE_REFUSED: '+name)
        if sql("SELECT COUNT(*) FROM mysql.user WHERE user='"+p['user']+"'") != '0':
            raise RuntimeError('MYSQL_EXISTING_USER_REFUSED')
        if module.check_mode:
            module.exit_json(changed=False, status='MYSQL_NEW_DATABASE_BOOTSTRAP_READY', server=server,
                             databases=names, tables_per_database=13)
        password = secrets.token_urlsafe(36)
        state = dict(identity=identity, status='STARTED', password=password, server=server,
                     scope='document-contract schema; validation approvals are NOT operational approval')
        write_private(state_path, state)
        for name in names:
            sql('CREATE DATABASE `'+name+'` CHARACTER SET utf8mb4 COLLATE utf8mb4_bin; USE `'+name+'`;\n'+text)
        account = "'"+p['user']+"'@'"+p['client_host']+"'"
        sql('CREATE USER '+account+" IDENTIFIED BY '"+password+"' REQUIRE SSL")
        writable = required - {'categories','pipeline_publication_guards','product_embeddings','search_index_versions'}
        for name in names:
            for table in sorted(required):
                privileges = 'SELECT,INSERT,UPDATE' if table in writable else 'SELECT'
                sql('GRANT '+privileges+' ON `'+name+'`.`'+table+'` TO '+account)
        state['status'] = 'COMPLETE'
        final = state_path.with_name(state_path.name+'.complete')
        write_private(final, state); os.replace(final, state_path)
        module.exit_json(changed=True, status='MYSQL_NEW_DATABASE_BOOTSTRAP_COMPLETE', server=server,
                         databases=names, tables_per_database=13, user=p['user'], client_host=p['client_host'],
                         tls_required=True, credential_file=str(state_path), products_published=0)
    except Exception as exc:
        module.fail_json(msg=str(exc), credentials_exposed=False, destructive_rollback=False)


if __name__ == '__main__':main()
