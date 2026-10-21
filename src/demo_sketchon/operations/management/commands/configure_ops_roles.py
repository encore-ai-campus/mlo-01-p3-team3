from django.core.management.base import BaseCommand,CommandError
from django.contrib.auth import get_user_model
from django.contrib.auth.models import Group,Permission
from django.db import transaction
from operations.models import OperatorAudit


class Command(BaseCommand):
    help='기존 계정에 SketchOn 운영 권한을 부여합니다. 계정·비밀번호는 생성하지 않습니다.'
    def add_arguments(self,parser):parser.add_argument('--username',required=True)
    @transaction.atomic
    def handle(self,*args,**options):
        try:user=get_user_model().objects.get(username=options['username'],is_active=True)
        except get_user_model().DoesNotExist:raise CommandError('활성 상태의 기존 계정을 찾을 수 없습니다.')
        group,_=Group.objects.get_or_create(name='SketchOn 운영자')
        codes=[code for code,_ in OperatorAudit._meta.permissions]
        permissions=Permission.objects.filter(content_type__app_label='operations',codename__in=codes)
        if permissions.count()!=len(codes):raise CommandError('먼저 migrate를 실행해 주세요.')
        group.permissions.set(permissions)
        before={'is_staff':user.is_staff,'in_group':user.groups.filter(pk=group.pk).exists()}
        user.is_staff=True;user.save(update_fields=['is_staff']);user.groups.add(group)
        if not all(before.values()):OperatorAudit.objects.create(actor=user,action='ops.role_granted',object_type='User',object_id=str(user.pk),before=before,after={'is_staff':True,'group':group.name},reason='지정 계정 운영 권한 설정')
        self.stdout.write(self.style.SUCCESS(f'{user.username}: 운영 권한 설정 완료 (superuser={user.is_superuser})'))
