import os,io,json,sys
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
os.environ.setdefault('DJANGO_SETTINGS_MODULE','config.settings')
import django
django.setup()
from django.conf import settings
if Path(settings.DATABASES['default']['NAME']).name!='ops-test.sqlite3':raise RuntimeError('Test database required')
from django.contrib.auth import get_user_model
from django.contrib.auth.models import Permission
from django.core.management import call_command
from django.core.files.uploadedfile import SimpleUploadedFile
from django.test import Client
from sketch.models import Project,GuestGenerationUsage
from sketch.test_collection import CollectionTests
from types import SimpleNamespace
from uuid import uuid4
from PIL import Image
GuestGenerationUsage.objects.all().delete()
user,_=get_user_model().objects.get_or_create(username='ops_demo',defaults={'is_staff':True,'first_name':'운영 테스트'})
user.set_password('Ops-test-only-2468!');user.save();call_command('configure_ops_roles',username=user.username)
scoped,_=get_user_model().objects.get_or_create(username='collection_demo',defaults={'is_staff':True})
scoped.set_password('Ops-test-only-2468!');scoped.save()
scoped.user_permissions.add(Permission.objects.get(content_type__app_label='operations',codename='view_collection'))
client=Client(HTTP_HOST='localhost');client.force_login(user)
stream=io.BytesIO();Image.new('RGB',(1200,800),'#e3e1d7').save(stream,'PNG')
data=client.post('/api/projects/',{'photo':SimpleUploadedFile('room.png',stream.getvalue(),'image/png')}).json();project=Project.objects.get(pk=data['id'])
sample=CollectionTests.sample(SimpleNamespace(project=project,writer=project.owner));sample['project_width']=1200;sample['project_height']=800
client.post(f'/api/projects/{project.id}/samples/',sample,content_type='application/json')
project.document=sample['final_document'];project.save()
result=client.post(f'/api/projects/{project.id}/search/',{'request_id':str(uuid4()),'revision':0,'document':project.document,'text_requirements':'화분'},content_type='application/json').json()
(Path(__file__).resolve().parents[1]/'ops-fixture.json').write_text(json.dumps({'project_id':str(project.id),'result_id':result['result_id']}),encoding='utf-8')
print('Isolated operator fixture ready')
