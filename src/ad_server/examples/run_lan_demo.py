"""Explicitly create a demo-only DB/account and start a hidden local/LAN process.

Passwords/service tokens are generated in memory; only the temporary operator
password is printed once to the invoking terminal. No credential file is written.
Existing default SQLite and MySQL databases are never used.
"""
import argparse
import json
import os
from pathlib import Path
import secrets
import socket
import subprocess
import sys


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--lan-host',required=True,help='Explicit IPv4 address of this computer')
    parser.add_argument('--port',type=int,default=8004)
    parser.add_argument('--reset-password',action='store_true',help='Explicitly reset the demo operator password')
    args=parser.parse_args()
    socket.inet_aton(args.lan_host)
    if not 1<=args.port<=65535:parser.error('Invalid port')
    # Refuse to reset a live session or leave a password printed for an unstarted server.
    with socket.socket() as probe:
        if probe.connect_ex(('127.0.0.1',args.port))==0:parser.error('Port is already in use; stop the intended demo first')
    root=Path(__file__).resolve().parents[1]
    sys.path.insert(0,str(root))
    demo=root/'runtime'/'lan_demo';demo.mkdir(parents=True,exist_ok=True)
    os.environ.update(AD_DB_BACKEND='sqlite',AD_ISOLATED_LAN_DEMO='1',
                      DJANGO_SETTINGS_MODULE='adconfig.demo_settings',AD_TEST_OPERATIONS='1',
                      AD_SERVER_HOSTS='127.0.0.1,localhost,'+args.lan_host,
                      AD_SERVER_SECRET_KEY=secrets.token_urlsafe(48),AD_SERVICE_TOKEN=secrets.token_urlsafe(48))
    config=json.loads((root/'test_policy.example.json').read_text(encoding='utf-8'))
    config['version']='test-lan-demo-1'
    (demo/'policy.json').write_text(json.dumps(config,indent=2),encoding='utf-8')
    import django
    django.setup()
    from django.conf import settings
    assert Path(settings.DATABASES['default']['NAME']).resolve()==(demo/'ads-demo.sqlite3').resolve()
    from django.core.management import call_command
    call_command('migrate',interactive=False,verbosity=0)
    call_command('seed_ads_demo',verbosity=0)
    from django.contrib.auth import get_user_model
    account,created=get_user_model().objects.get_or_create(username='demo_operator')
    password=secrets.token_urlsafe(18) if created or args.reset_password else None
    if password:account.set_password(password)
    account.is_staff=True;account.is_superuser=True;account.is_active=True;account.save()
    output=open(demo/'server.stdout.log','ab')
    errors=open(demo/'server.stderr.log','ab')
    try:
        process=subprocess.Popen([sys.executable,str(root/'manage.py'),'runserver',f'0.0.0.0:{args.port}',
                                  '--settings=adconfig.demo_settings','--noreload','--insecure'],
                                 cwd=root,env=os.environ.copy(),stdin=subprocess.DEVNULL,stdout=output,stderr=errors,
                                 creationflags=getattr(subprocess,'CREATE_NO_WINDOW',0))
    finally:output.close();errors.close()
    (demo/'server.pid').write_text(str(process.pid),encoding='ascii')
    print('DEMO_URL=http://'+args.lan_host+':'+str(args.port))
    print('DEMO_USERNAME=demo_operator')
    print('DEMO_PASSWORD='+password if password else 'DEMO_PASSWORD_UNCHANGED')
    print('SERVER_PID='+str(process.pid))


if __name__=='__main__':main()
