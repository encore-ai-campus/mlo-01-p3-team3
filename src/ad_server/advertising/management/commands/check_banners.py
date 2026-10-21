from django.core.management.base import BaseCommand,CommandError
from advertising.assets import manifest

class Command(BaseCommand):
    help='검증된 10종 배너 형식·비율·해시 확인 (DB 불필요)'
    def handle(self,**options):
        rows=manifest()
        for r in rows:self.stdout.write(f"{r['id']}: {r['status']} {r.get('width','-')}x{r.get('height','-')} {r.get('sha256','-')}")
        if any(r['status']!='READY' for r in rows):raise CommandError('일부 소재를 확인하세요.')
