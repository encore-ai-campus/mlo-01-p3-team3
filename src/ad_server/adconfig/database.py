"""Explicit DB selection. Constructing settings never opens a connection."""
import re
from django.core.exceptions import ImproperlyConfigured


def database_config(env, runtime):
    backend=env.get('AD_DB_BACKEND','sqlite').strip().lower()
    if backend=='sqlite':
        return {'ENGINE':'django.db.backends.sqlite3','NAME':runtime/'ads.sqlite3',
                'OPTIONS':{'timeout':20,'transaction_mode':'IMMEDIATE'},
                'TEST':{'NAME':str(runtime/'test-ads.sqlite3')}}
    if backend!='mysql':
        raise ImproperlyConfigured('AD_DB_BACKEND must be sqlite or mysql')
    required=('AD_MYSQL_DATABASE','AD_MYSQL_USER','AD_MYSQL_PASSWORD','AD_MYSQL_HOST')
    missing=[key for key in required if not env.get(key,'').strip()]
    if missing:raise ImproperlyConfigured('Missing MySQL settings: '+', '.join(missing))
    name=env['AD_MYSQL_DATABASE']
    if not re.fullmatch(r'[A-Za-z][A-Za-z0-9_]{0,63}',name):
        raise ImproperlyConfigured('Use a simple dedicated advertisement database name')
    try:
        port=int(env.get('AD_MYSQL_PORT','3306'))
        if not 1<=port<=65535:raise ValueError()
    except ValueError:raise ImproperlyConfigured('Invalid AD_MYSQL_PORT') from None
    options={'charset':'utf8mb4','isolation_level':'read committed','connect_timeout':5,
             'init_command':"SET sql_mode='STRICT_TRANS_TABLES', default_storage_engine=INNODB"}
    if env.get('AD_MYSQL_SSL_CA'):
        options['ssl']={'ca':env['AD_MYSQL_SSL_CA']}
        options['ssl_mode']='VERIFY_IDENTITY'
    # Explicit separate test database; never use production DB as a test database.
    test_name=env.get('AD_MYSQL_TEST_DATABASE') or 'test_'+name
    if test_name==name or not re.fullmatch(r'test_[A-Za-z0-9_]{1,59}',test_name):
        raise ImproperlyConfigured('Use a distinct test_ prefixed MySQL test database')
    return {'ENGINE':'django.db.backends.mysql','NAME':name,'USER':env['AD_MYSQL_USER'],
            'PASSWORD':env['AD_MYSQL_PASSWORD'],'HOST':env['AD_MYSQL_HOST'],'PORT':str(port),
            'CONN_MAX_AGE':0,'OPTIONS':options,'TEST':{'NAME':test_name,'CHARSET':'utf8mb4'}}
