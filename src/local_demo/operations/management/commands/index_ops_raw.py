from django.core.management.base import BaseCommand
from operations.raw_index import index_raw


class Command(BaseCommand):
    help='기존 Raw 파일을 변경하지 않고 수집 메타데이터를 생성합니다.'
    def add_arguments(self,parser):parser.add_argument('--limit',type=int,default=1000)
    def handle(self,*args,**options):self.stdout.write(str(index_raw(max(1,min(10000,options['limit'])))))
