"""Explicit MySQL transactions. Schema discovery is read-only; no runtime DDL."""
from contextlib import contextmanager
from datetime import datetime,timezone
import os
import re
from uuid import uuid4

from ..contracts import canonical, digest, utc
from ..errors import Conflict, PipelineError
from ..files import parse
from .values import sql_time
from .schema import TABLES, PRIMARY, UNIQUE, FKS, CONTRACT_HASH, validate_schema



class MySQL:
    def __init__(self, settings, *, local_test=False, connection=None):
        self.settings=settings
        if settings.get('contract_version')!=1 or not re.fullmatch(r'[A-Za-z0-9_]+',settings.get('database','')):
            raise PipelineError('MYSQL_SETTINGS_INVALID')
        if not settings.get('target_id') or settings['target_id'].startswith('replace-with-'):
            raise PipelineError('MYSQL_LOGICAL_TARGET_REQUIRED')
        if connection is not None:self.connection=connection;return
        import pymysql
        self.driver=pymysql
        options=dict(host=settings['host'],port=settings.get('port',3306),user=settings['user'],database=settings['database'],
            password=os.environ.get(settings.get('password_env','PIPELINE_MYSQL_PASSWORD'),''),
            charset='utf8mb4',autocommit=False,connect_timeout=5,read_timeout=30,write_timeout=30,
            cursorclass=pymysql.cursors.DictCursor)
        if local_test:
            if options['host'] not in {'127.0.0.1','localhost'} or not settings.get('local_test_only') or options['port']==3306:
                raise PipelineError('MYSQL_ISOLATED_TEST_REQUIRED')
        else:
            ca=os.environ.get(settings.get('ca_env','PIPELINE_MYSQL_CA'))
            if not ca or not options['password'] or settings.get('local_test_only'):
                raise PipelineError('MYSQL_TLS_AND_SECRET_REQUIRED')
            options.update(ssl_ca=ca,ssl_verify_cert=True,ssl_verify_identity=True)
        self.connection=pymysql.connect(**options)
        self.execute("SET time_zone = '+00:00'")
        self.execute('SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED')
        self.execute('SET SESSION innodb_lock_wait_timeout = 15')
        self.connection.commit()

    @property
    def target(self):return digest(dict(target_id=self.settings['target_id'],database=self.settings['database']))

    def execute(self,sql,args=()):
        with self.connection.cursor() as c:c.execute(sql,args);return c.rowcount

    def all(self,sql,args=()):
        with self.connection.cursor() as c:c.execute(sql,args);return list(c.fetchall())

    def one(self,sql,args=()):
        rows=self.all(sql,args)
        if len(rows)>1:raise Conflict('MYSQL_EXPECTED_SINGLE_ROW')
        return rows[0] if rows else None

    def inspect_schema(self):
        server=self.one('SELECT @@version AS version, @@global.read_only AS ro, @@global.super_read_only AS sro')
        if server['ro'] or server['sro']:raise PipelineError('MYSQL_WRITABLE_PRIMARY_REQUIRED')
        if not re.match(r'^(8|9)\.',server['version']):raise PipelineError('MYSQL_VERSION_UNSUPPORTED')
        database=self.settings['database']; actual={}
        for table in TABLES:
            metadata=self.one('SELECT ENGINE AS engine FROM information_schema.TABLES WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s',(database,table))
            if not metadata:continue
            columns=self.all('SELECT COLUMN_NAME AS name,COLUMN_TYPE AS kind,IS_NULLABLE AS nullable,COLUMN_DEFAULT AS def,EXTRA AS extra FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s',(database,table))
            indexes=self.all('SELECT INDEX_NAME AS idx,COLUMN_NAME AS col,SEQ_IN_INDEX AS seq,NON_UNIQUE AS non_unique,SUB_PART AS prefix FROM information_schema.STATISTICS WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s ORDER BY INDEX_NAME,SEQ_IN_INDEX',(database,table))
            unique={};invalid=set()
            for i in indexes:
                if not i['non_unique']:
                    unique.setdefault(i['idx'],[]).append(i['col'])
                    if i['prefix'] is not None:invalid.add(i['idx'])
            unique={k:v for k,v in unique.items() if k not in invalid}
            fks=self.all('SELECT COLUMN_NAME AS col,REFERENCED_TABLE_NAME AS parent,REFERENCED_COLUMN_NAME AS pk,REFERENCED_TABLE_SCHEMA AS parent_schema FROM information_schema.KEY_COLUMN_USAGE WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s AND REFERENCED_TABLE_NAME IS NOT NULL',(database,table))
            rules=self.all('SELECT k.COLUMN_NAME AS col,r.DELETE_RULE AS rule FROM information_schema.REFERENTIAL_CONSTRAINTS r JOIN information_schema.KEY_COLUMN_USAGE k ON k.CONSTRAINT_SCHEMA=r.CONSTRAINT_SCHEMA AND k.TABLE_NAME=r.TABLE_NAME AND k.CONSTRAINT_NAME=r.CONSTRAINT_NAME WHERE r.CONSTRAINT_SCHEMA=%s AND r.TABLE_NAME=%s',(database,table))
            if any(r['parent_schema']!=database for r in fks):raise PipelineError('MYSQL_CROSS_SCHEMA_FK_UNSUPPORTED',table)
            actual[table]=dict(engine=metadata['engine'],columns={c['name']:(c['kind'].lower(),c['nullable']=='YES') for c in columns},
                primary=unique.get('PRIMARY',[]),unique=list(unique.values()),fks=[(r['col'],r['parent'],r['pk']) for r in fks],
                defaulted=[c['name'] for c in columns if c['def'] is not None or 'GENERATED' in c['extra'] or 'auto_increment' in c['extra']],
                fk_delete_rules=[r['rule'] for r in rules if r['col'] in {key[0] for key in FKS.get(table,[])}])
        return server,actual

    def preflight(self):
        server,actual=self.inspect_schema()
        checksum=validate_schema(actual)
        self.connection.rollback()
        return dict(status='MYSQL_SCHEMA_VERIFIED',contract_hash=checksum,target=self.target,server_version=server['version'])

    @contextmanager
    def read_snapshot(self):
        self.connection.rollback()
        self.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ')
        self.execute('START TRANSACTION WITH CONSISTENT SNAPSHOT, READ ONLY')
        try:yield self
        finally:self.connection.rollback()

    @contextmanager
    def review_lock(self):
        # A stable lock closes the append-only ledger's absent-row race without
        # inventing a cross-database transaction or runtime DDL.
        key='pipeline-review-'+self.target[:48]
        if self.one('SELECT GET_LOCK(%s,0) AS acquired',(key,))['acquired']!=1:
            raise PipelineError('REVIEW_OWNER_BUSY')
        try:yield self
        finally:self.one('SELECT RELEASE_LOCK(%s) AS released',(key,))

    @contextmanager
    def transaction(self):
        self.connection.begin()
        try:yield self;self.connection.commit()
        except BaseException:self.connection.rollback();raise

    def get(self,table,key,*,locking=True):
        if table not in TABLES or not key or any(k not in TABLES[table] for k in key):raise PipelineError('MYSQL_IDENTIFIER_NOT_ALLOWED')
        # Owner inputs are read-only to the publisher. A shared row lock blocks
        # owner changes until commit without granting publisher UPDATE rights.
        lock=(' FOR SHARE' if table in {'categories','pipeline_publication_guards'} else ' FOR UPDATE') if locking else ''
        return self.one('SELECT * FROM `'+table+'` WHERE '+' AND '.join('`'+k+'`=%s' for k in key)+lock,tuple(key.values()))

    def insert(self,table,values):
        if table not in TABLES or not values or any(k not in TABLES[table] for k in values):raise PipelineError('MYSQL_IDENTIFIER_NOT_ALLOWED')
        return self.execute('INSERT INTO `'+table+'` ('+','.join('`'+k+'`' for k in values)+') VALUES ('+','.join('%s' for _ in values)+')',tuple(values.values()))

    def update(self,table,key,values):
        if table not in TABLES or not key or any(k not in TABLES[table] for k in (*values,*key)) or 'id' in values:
            raise PipelineError('MYSQL_IDENTIFIER_NOT_ALLOWED')
        return self.execute('UPDATE `'+table+'` SET '+','.join('`'+k+'`=%s' for k in values)+' WHERE '+
            ' AND '.join('`'+k+'`=%s' for k in key),tuple(values.values())+tuple(key.values()))

    def identity(self,table,key,values):
        old=self.get(table,key)
        if old:
            if any(old[k]!=v for k,v in key.items()):raise Conflict('MYSQL_BUSINESS_KEY_COLLATION_CONFLICT')
            return old,False
        import pymysql
        for _ in range(3):
            values={**values,**key,'id':uuid4().hex}
            try:self.insert(table,values);return values,True
            except pymysql.err.IntegrityError as exc:
                if exc.args[0]!=1062:raise
                old=self.get(table,key)
                if old:
                    if any(old[k]!=v for k,v in key.items()):raise Conflict('MYSQL_BUSINESS_KEY_COLLATION_CONFLICT')
                    return old,False
        raise Conflict('MYSQL_UUID_INSERT_CONFLICT')

    def close(self):self.connection.close()


def schema_difference(db):
    server,actual=db.inspect_schema()
    missing=[];columns=[];constraints=[];engines=[];unmapped=[]
    for table,expected in TABLES.items():
        row=actual.get(table)
        if row is None:
            missing.append(table);continue
        if row['engine'].lower()!='innodb':engines.append(dict(table=table,expected='InnoDB',actual=row['engine']))
        for name,wanted in expected.items():
            found=row['columns'].get(name)
            if found!=wanted:
                columns.append(dict(table=table,column=name,expected=wanted,actual=found))
        for name,(kind,nullable) in row['columns'].items():
            if name not in expected and not nullable and name not in row.get('defaulted',[]):
                unmapped.append(dict(table=table,column=name,type=kind))
        if row['primary']!=PRIMARY[table]:constraints.append(dict(table=table,kind='PRIMARY',expected=PRIMARY[table]))
        for key in UNIQUE.get(table,[]):
            if key not in row['unique']:constraints.append(dict(table=table,kind='UNIQUE',expected=key))
        for key in FKS.get(table,[]):
            if tuple(key) not in row['fks']:constraints.append(dict(table=table,kind='FOREIGN_KEY',expected=key))
        if any(rule not in {'RESTRICT','NO ACTION'} for rule in row.get('fk_delete_rules',[])):
            constraints.append(dict(table=table,kind='FOREIGN_KEY_DELETE_RULE',expected='RESTRICT'))
    db.connection.rollback()
    return dict(status='SCHEMA_DIFFERENCES_NOT_APPLIED',target=db.target,server_version=server['version'],
        schema_contract_hash=CONTRACT_HASH,missing_tables=missing,column_differences=columns,
        constraint_differences=constraints,engine_differences=engines,unmapped_required_columns=unmapped,
        ddl_applied=False,service_rows_changed=False)
