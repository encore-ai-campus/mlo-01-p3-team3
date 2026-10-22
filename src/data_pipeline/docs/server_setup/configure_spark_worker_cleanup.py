"""Per-worker retention. Inspect by default; apply only during an idle window.

Run as root on a Worker, then restart its existing Spark service while idle.
Only spark-env.sh is changed. No work directory or shared JAR is deleted here.
"""
import argparse
import json
import os
from pathlib import Path
import re
import shutil
import tempfile

BEGIN='# BEGIN PJ3 WORKER CLEANUP'
END='# END PJ3 WORKER CLEANUP'


def configured(text, interval=60, retention=3600):
    if interval < 10 or retention < interval:
        raise ValueError('CLEANUP_RETENTION_INVALID')
    pattern=re.compile(r'(?m)^'+re.escape(BEGIN)+r'\n.*?^'+re.escape(END)+r'\n?',re.S)
    if (text.count(BEGIN)!=text.count(END) or text.count(BEGIN)>1
            or (BEGIN in text and not pattern.search(text))):
        raise ValueError('CLEANUP_MANAGED_BLOCK_INVALID')
    original=pattern.sub('',text)
    if 'spark.worker.cleanup.' in original:
        raise ValueError('CLEANUP_EXISTING_UNMANAGED_POLICY')
    block=(BEGIN+'\nexport SPARK_WORKER_OPTS="${SPARK_WORKER_OPTS:-}'
           ' -Dspark.worker.cleanup.enabled=true'
           f' -Dspark.worker.cleanup.interval={interval}'
           f' -Dspark.worker.cleanup.appDataTtl={retention}"\n'+END+'\n')
    return original.rstrip()+'\n\n'+block


def main(argv=None):
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--apply',action='store_true')
    parser.add_argument('--config',default='/etc/pj3/spark/spark-env.sh')
    parser.add_argument('--interval',type=int,default=60)
    parser.add_argument('--retention',type=int,default=3600)
    args=parser.parse_args(argv)
    if os.geteuid()!=0:raise SystemExit('ROOT_REQUIRED')
    workers=[];executors=[]
    for entry in Path('/proc').iterdir():
        if not entry.name.isdigit():continue
        try:cmd=(entry/'cmdline').read_bytes().split(b'\0')
        except OSError:continue
        if b'org.apache.spark.deploy.worker.Worker' in cmd:workers.append(entry.name)
        if any(name in cmd for name in (b'org.apache.spark.executor.CoarseGrainedExecutorBackend',
                                       b'org.apache.spark.deploy.worker.DriverWrapper')):
            executors.append(entry.name)
    if len(workers)!=1 or executors:raise SystemExit('EXACTLY_ONE_IDLE_WORKER_REQUIRED')
    path=Path(args.config)
    if path.is_symlink() or not path.is_file():raise SystemExit('REGULAR_CONFIG_REQUIRED')
    old=path.read_text();new=configured(old,args.interval,args.retention)
    changed=new!=old
    if args.apply and changed:
        # One fixed before-image; never generate timestamped backup accumulation.
        backup=path.with_name(path.name+'.before-pj3-cleanup')
        if not backup.exists():shutil.copy2(path,backup)
        info=path.stat()
        fd,name=tempfile.mkstemp(prefix='.spark-env-',dir=path.parent)
        try:
            os.fchmod(fd,info.st_mode & 0o777);os.fchown(fd,info.st_uid,info.st_gid)
            with os.fdopen(fd,'w') as f:f.write(new);f.flush();os.fsync(f.fileno())
            os.replace(name,path)
        finally:
            if os.path.exists(name):os.unlink(name)
    print(json.dumps(dict(applied=args.apply,changed=changed,interval_seconds=args.interval,
                          retention_seconds=args.retention,restart_required=args.apply and changed,
                          deleted_files=0)))


if __name__=='__main__':main()
