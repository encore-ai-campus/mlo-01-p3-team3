"""V2 transport and one bounded schema discovery, without review/index tables."""
import re
from contextlib import contextmanager, nullcontext

from pipeline_v2.legacy.publication.mysql import MySQL
from pipeline_v2.legacy.publication.schema import TABLES, PRIMARY, UNIQUE, FKS
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
        self._read_cache, self._pending, self._deferred = {}, {}, []
        try:
            yield self
            self.flush()
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
        if getattr(self,'_pending',None) or getattr(self,'_deferred',None):
            # A read that reaches the database sees every buffered row and update first.
            self.flush()
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

    def fetch_many(self,table,keys):
        """Plain (non-locking) keyed reads in chunks of 100: canonical(key) -> row or None."""
        if table not in USED:
            raise Invalid('MYSQL_PREFETCH_KEY_INVALID')
        if getattr(self,'_pending',None):
            self.flush()
        found = {}
        for start in range(0,len(keys),100):
            chunk = keys[start:start+100]
            for body in chunk:
                if not body or any(k not in TABLES[table] for k in body):
                    raise Invalid('MYSQL_PREFETCH_KEY_INVALID')
            where = ' OR '.join('('+' AND '.join('`'+k+'`=%s' for k in body)+')' for body in chunk)
            rows = self.all('SELECT * FROM `'+table+'` WHERE '+where,tuple(v for body in chunk for v in body.values()))
            for body in chunk:
                matches = [r for r in rows if all(r[k] == v for k,v in body.items())]
                if len(matches) > 1:
                    raise Invalid('MYSQL_PREFETCH_DUPLICATE_KEY')
                found[canonical(body)] = matches[0] if matches else None
        return found

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
        pending = getattr(self,'_pending',None) or {}
        ids = {row['id'] for items in pending.values() for _,_,row in items}
        targets = [row for _,_,row in pending.get(table,[]) if all(row.get(k) == v for k,v in key.items())]
        if any(v in ids for v in values.values()):
            # It references a row not written yet (e.g. a new product's representative image, a
            # cycle with product_images): it runs right after the buffered inserts.
            self._invalidate(table,key)
            self._deferred.append((table,key,values))
            return 1
        # A deferred update that may touch the same row keeps its place before this one.
        queued = any(t == table and (k == key or set(k) != {'id'} or set(key) != {'id'})
            for t,k,_ in getattr(self,'_deferred',None) or [])
        if targets and set(key) == {'id'} and not queued:
            # A buffered row: the change goes into its INSERT (the cached row is the same dict).
            targets[0].update(values)
            return 1
        if targets or queued:
            self.flush()
        self._invalidate(table,key)
        return super().update(table,key,values)

    def identity(self,table,key,values):
        cache = getattr(self,'_read_cache',None)
        if cache is not None and cache.get((table,canonical(key),True),False) is None:
            # Prefetched FOR UPDATE and absent in this transaction: buffered for one multi-row INSERT
            # per table (a concurrent duplicate still falls back to the checked path in flush()).
            return self.identity_absent(table,key,values)
        saved, created = super().identity(table,key,values)
        self._remember(table,key,saved,created)
        return saved, created

    def identity_absent(self,table,key,values):
        """Business-key insert when the caller knows no row can exist yet (its parent was created
        in this transaction): the lookup is skipped and the row waits in a per-table buffer that
        `flush()` writes as one multi-row INSERT. A duplicate falls back to the checked path."""
        from uuid import uuid4
        row = {**values,**key,'id':uuid4().hex}
        buffer = self.__dict__.setdefault('_pending',{})
        buffer.setdefault(table,[]).append((key,values,row))
        self._remember(table,key,row,True)
        return row, True

    @staticmethod
    def _parents_first(pending):
        """Buffered tables with every referenced parent table before its children. An edge whose
        column is None in every buffered row (products.representative_image_id, set by a deferred
        update) is ignored, which breaks the products/product_images cycle."""
        order, state = [], {}
        def visit(table):
            if state.get(table) == 'done':
                return
            if state.get(table) == 'open':
                raise Invalid('MYSQL_BUFFER_FK_CYCLE')
            state[table] = 'open'
            for column, parent, _ in FKS.get(table,()):
                if parent != table and parent in pending and any(row.get(column) is not None for _,_,row in pending[table]):
                    visit(parent)
            state[table] = 'done'
            order.append(table)
        for table in pending:
            visit(table)
        return order

    # Each multi-row INSERT stays far below max_allowed_packet (64 MiB by default).
    INSERT_BYTES, INSERT_ROWS = 4 * 1024 * 1024, 1000

    def _bounded(self, rows):
        group, size = [], 0
        for item in rows:
            row = item[2]
            n = sum(len(v) if isinstance(v,(str,bytes)) else 16 for v in row.values()) + 8 * len(row)
            if group and (size + n > self.INSERT_BYTES or len(group) >= self.INSERT_ROWS):
                yield group
                group, size = [], 0
            group.append(item)
            size += n
        if group:
            yield group

    def flush(self):
        """Write buffered rows, one INSERT per table and column set in parent-first order (on a
        duplicate, row by row), then the deferred updates in their original order."""
        import pymysql
        pending, self._pending = getattr(self,'_pending',{}), {}
        deferred, self._deferred = getattr(self,'_deferred',None) or [], []
        for table in self._parents_first(pending):
            items = pending[table]
            groups = {}
            for key, values, row in items:
                groups.setdefault(tuple(row),[]).append((key,values,row))
            for columns, rows in groups.items():
              if table not in TABLES or any(c not in TABLES[table] for c in columns):
                  raise Invalid('MYSQL_IDENTIFIER_NOT_ALLOWED')
              for group in self._bounded(rows):
                values_sql = '('+','.join('%s' for _ in columns)+')'
                sql = ('INSERT INTO `'+table+'` ('+','.join('`'+c+'`' for c in columns)+') VALUES '+
                    ','.join(values_sql for _ in group))
                try:
                    self.execute(sql,tuple(row[c] for _,_,row in group for c in columns))
                except pymysql.err.IntegrityError as exc:
                    if exc.args[0] != 1062:
                        raise
                    self._read_cache.clear()
                    for key, values, row in group:
                        saved = MySQL.get(self,table,key)
                        if saved is None:
                            # Callers already hold the buffered id: store the row with it.
                            super().insert(table,row)
                            continue
                        # Another writer stored this key after the prefetch: the caller already
                        # holds the buffered id, so the transaction is retried (its prefetch then
                        # finds the stored row) rather than the id silently swapped.
                        if saved['id'] != row['id']:
                            raise Invalid('MYSQL_CONCURRENT_INSERT')
        for table, key, values in deferred:
            super().update(table,key,values)

    def _remember(self,table,key,saved,created):
        # A row inserted in this transaction is served from the cache to later locking reads.
        cache = getattr(self,'_read_cache',None)
        if created and cache is not None:
            cache[(table,canonical(key),True)] = saved

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
