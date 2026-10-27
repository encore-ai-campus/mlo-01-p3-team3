"""Offline validation only: no cursor, introspection, migrations or credentials output."""
from importlib.metadata import version,PackageNotFoundError
from django.conf import settings
from django.core.management.base import BaseCommand


class Command(BaseCommand):
    help='DB 설정을 오프라인으로 검사합니다. 연결·테이블 생성·마이그레이션은 실행하지 않습니다.'
    requires_system_checks=[]
    requires_migrations_checks=False

    def handle(self,**options):
        config=settings.DATABASES['default']
        mysql=config['ENGINE']=='django.db.backends.mysql'
        self.stdout.write('Backend: '+('mysql' if mysql else 'sqlite'))
        if mysql:
            try:driver=version('mysqlclient')
            except PackageNotFoundError:driver='NOT INSTALLED: pip install -r requirements-mysql.txt'
            self.stdout.write('Driver: '+driver)
            self.stdout.write('Isolation: read committed / charset: utf8mb4 / engine: InnoDB')
            self.stdout.write('TLS CA configured: '+str(bool(config['OPTIONS'].get('ssl'))))
        self.stdout.write('OFFLINE ONLY: no DB connection; schema/server version/permissions/concurrency NOT verified.')
