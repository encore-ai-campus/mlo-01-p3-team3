"""V2 transport and one bounded schema discovery, without review/index tables."""
import re
from contextlib import contextmanager, nullcontext

from product_pipeline.publication.mysql import MySQL
from product_pipeline.publication.schema import TABLES, PRIMARY, UNIQUE, FKS
from ..domain.contracts import canonical, digest, Invalid


USED=('categories','products','assets','product_images','offers','offer_snapshots',
    'pipeline_product_versions','pipeline_publications','pipeline_publication_staging','product_link_observations')


class DirectMySQL(MySQL):
    def _timed(self, stage, counts=None):
        timing = getattr(self, 'timing', None)
        return timing.measure(stage, timing.active_scope or 'mysql', counts=counts) if timing else nullcontext()

    def all(self,sql,args=()):
        if hasattr(self,'query_metrics'):self.query_metrics['sql_queries']+=1
        kind = sql.split(None, 1)[0].lower()
        with self._timed('mysql.client.' + kind, {'sql_calls':1}) as counts:
            rows = super().all(sql,args)
            if counts is not None:counts['returned_rows'] = len(rows)
            return rows

    def execute(self,sql,args=()):
        if hasattr(self,'query_metrics'):self.query_metrics['sql_queries']+=1
        kind = sql.split(None, 1)[0].lower()
        with self._timed('mysql.client.' + kind, {'sql_calls':1}) as counts:
            rows = super().execute(sql,args)
            if counts is not None:counts['affected_rows'] = max(0, rows)
            return rows

    @contextmanager
    def transaction(self):
        with self._timed('mysql.transaction.begin'):
            self.connection.begin()
        self._read_cache = {}
        try:
            yield self
            with self._timed('mysql.transaction.commit'):
                self.connection.commit()
        except BaseException:
            with self._timed('mysql.transaction.rollback'):
                self.connection.rollback()
            raise
        finally:
            self._read_cache = None

    def get(self,table,key,*,locking=True):
        cache = getattr(self,'_read_cache',None)
        identity = (table,canonical(key),locking)
        if cache is not None and identity in cache:
            return cache[identity]
        return super().get(table,key,locking=locking)

    def prefetch(self,table,keys):
        if table not in USED or getattr(self,'_read_cache',None) is None:
            raise Invalid('MYSQL_PREFETCH_TRANSACTION_REQUIRED')
        unique = {canonical(key):key for key in keys}
        for body in unique.values():
            if not body or any(k not in TABLES[table] for k in body):
                raise Invalid('MYSQL_PREFETCH_KEY_INVALID')
        values = list(unique.values())
        for start in range(0,len(values),100):
            chunk = values[start:start+100]
            where = ' OR '.join('('+' AND '.join('`'+k+'`=%s' for k in body)+')' for body in chunk)
            lock = ' FOR SHARE' if table == 'categories' else ' FOR UPDATE'
            rows = self.all('SELECT * FROM `'+table+'` WHERE '+where+lock,tuple(v for body in chunk for v in body.values()))
            for body in chunk:
                matches = [row for row in rows if all(row[k] == v for k,v in body.items())]
                if len(matches) > 1:
                    raise Invalid('MYSQL_PREFETCH_DUPLICATE_KEY')
                self._read_cache[(table,canonical(body),True)] = matches[0] if matches else None

    def _invalidate(self,table,identity):
        cache = getattr(self,'_read_cache',None)
        if cache is not None:
            import json
            for key, row in list(cache.items()):
                body = row if row is not None else json.loads(key[1])
                if key[0] == table and any(k in body and body[k] == v for k,v in identity.items()):
                    cache.pop(key,None)

    def insert(self,table,values):
        self._invalidate(table,values)
        try:
            return super().insert(table,values)
        except BaseException:
            if getattr(self,'_read_cache',None) is not None:
                self._read_cache.clear()
            raise

    def update(self,table,key,values):
        self._invalidate(table,key)
        return super().update(table,key,values)

    def preflight(self):
        server=self.one('SELECT @@version AS version, @@global.read_only AS ro, @@global.super_read_only AS sro')
        if server['ro'] or server['sro'] or not re.match(r'^(8|9)\.',server['version']):raise Invalid('MYSQL_PRIMARY_VERSION_REQUIRED')
        scope='TABLE_SCHEMA=%s AND TABLE_NAME IN ('+','.join('%s' for _ in USED)+')'
        args=(self.settings['database'],*USED)
        engines=self.all('SELECT TABLE_NAME AS tab,ENGINE AS engine FROM information_schema.TABLES WHERE '+scope,args)
        columns=self.all('SELECT TABLE_NAME AS tab,COLUMN_NAME AS name,COLUMN_TYPE AS kind,IS_NULLABLE AS nullable,COLUMN_DEFAULT AS def,EXTRA AS extra FROM information_schema.COLUMNS WHERE '+scope,args)
        indexes=self.all('SELECT TABLE_NAME AS tab,INDEX_NAME AS idx,COLUMN_NAME AS col,SEQ_IN_INDEX AS seq,NON_UNIQUE AS non_unique,SUB_PART AS prefix FROM information_schema.STATISTICS WHERE '+scope+' ORDER BY TABLE_NAME,INDEX_NAME,SEQ_IN_INDEX',args)
        foreign=self.all('SELECT TABLE_NAME AS tab,COLUMN_NAME AS col,REFERENCED_TABLE_NAME AS parent,REFERENCED_COLUMN_NAME AS pk,REFERENCED_TABLE_SCHEMA AS parent_schema FROM information_schema.KEY_COLUMN_USAGE WHERE '+scope+' AND REFERENCED_TABLE_NAME IS NOT NULL',args)
        rules=self.all('SELECT TABLE_NAME AS tab,DELETE_RULE AS rule FROM information_schema.REFERENTIAL_CONSTRAINTS WHERE CONSTRAINT_SCHEMA=%s AND TABLE_NAME IN ('+','.join('%s' for _ in USED)+')',args)
        for table in USED:
            if not any(r['tab']==table and r['engine'].lower()=='innodb' for r in engines):raise Invalid('MYSQL_ENGINE_MISMATCH')
            actual={r['name']:(r['kind'].lower(),r['nullable']=='YES') for r in columns if r['tab']==table}
            if any(actual.get(name)!=value for name,value in TABLES[table].items()):raise Invalid('MYSQL_COLUMN_MISMATCH')
            if any(r['tab']==table and r['name'] not in TABLES[table] and r['nullable']=='NO' and r['def'] is None
                    and not any(v in r['extra'] for v in ('GENERATED','auto_increment')) for r in columns):raise Invalid('MYSQL_UNMAPPED_REQUIRED_COLUMN')
            keys={};invalid=set()
            for value in indexes:
                if value['tab']==table and not value['non_unique']:
                    keys.setdefault(value['idx'],[]).append(value['col'])
                    if value['prefix'] is not None:invalid.add(value['idx'])
            keys={k:v for k,v in keys.items() if k not in invalid}
            if keys.get('PRIMARY')!=PRIMARY[table] or any(k not in keys.values() for k in UNIQUE.get(table,[])):
                raise Invalid('MYSQL_STABLE_KEY_MISMATCH')
            actual_fks=[(r['col'],r['parent'],r['pk']) for r in foreign if r['tab']==table]
            if any(k not in actual_fks for k in FKS.get(table,[])):raise Invalid('MYSQL_FK_MISMATCH')
        if any(r['parent_schema']!=self.settings['database'] for r in foreign):raise Invalid('MYSQL_CROSS_SCHEMA_FK')
        if any(r['rule'] not in {'RESTRICT','NO ACTION'} for r in rules):raise Invalid('MYSQL_DELETE_RULE_UNSAFE')
        self.connection.rollback()
        return dict(status='DIRECT_SCHEMA_VERIFIED',tables=len(USED),queries=6,
            contract_hash=digest({t:TABLES[t] for t in USED}))
