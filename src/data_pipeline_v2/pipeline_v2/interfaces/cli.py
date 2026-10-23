import argparse
import json
from pathlib import Path
import time
import re

from ..bootstrap import configuration, runtime
from ..application.capture import capture


def main(argv=None):
    import sys
    from .batch_cli import COMMANDS, main as batch_main
    arguments = list(sys.argv[1:] if argv is None else argv)
    if any(value in COMMANDS for value in arguments):
        return batch_main(arguments)
    parser=argparse.ArgumentParser(description='새 데이터 파이프라인: 범위 고정·정제·직접 게시·복구')
    parser.add_argument('--config',type=Path,required=True)
    commands=parser.add_subparsers(dest='command',required=True)
    for name in ('capture','tick','run','status'):
        command=commands.add_parser(name)
        command.add_argument('--scope',required=True)
        if name in {'tick','run'}:command.add_argument('--apply',action='store_true')
        if name=='run':command.add_argument('--max-ticks',type=int,default=100000)
    recovery=commands.add_parser('recover');recovery.add_argument('--job',required=True);recovery.add_argument('--stop-evidence',required=True)
    args=parser.parse_args(argv)
    try:
        config=configuration(args.config)
        if args.command in {'tick','run'} and config['mode']=='server' and not args.apply:
            raise ValueError('SERVER_EXECUTION_REQUIRES_APPLY')
        event_scope=getattr(args,'scope','job-'+args.job if args.command=='recover' else 'runtime')
        with runtime(config,writable=args.command in {'tick','run'},scope_id=event_scope) as engine:
            if args.command=='capture':
                with engine.state.timing.measure('capture.command_total',args.scope):
                    result=capture(engine.state,engine.source,args.scope,config.get('policy',{}),engine.limits)
            elif args.command=='recover':
                with engine.state.timing.measure('recovery.command_total','job-'+args.job,job_id=args.job):result=engine.recover(args.job,args.stop_evidence)
            elif args.command=='status':result=engine.status(args.scope)
            elif args.command=='tick':result=engine.tick(args.scope)
            else:
                if args.max_ticks<1:raise ValueError('MAX_TICKS_POSITIVE_REQUIRED')
                with engine.state.timing.measure('run.total',args.scope):
                    for _ in range(args.max_ticks):
                        result=engine.status(args.scope,verify_outputs=False)
                        if result['status'].startswith('COMPLETED'):break
                        progress=engine.tick(args.scope)
                        print(json.dumps(progress,ensure_ascii=False),flush=True)
                        actionable=any(engine.state.job(t['job_id']).get('delivery')!='BLOCKED' for t in engine.state.pending())
                        if progress['scan_complete'] and result['status']=='ATTENTION_REQUIRED' and not actionable:break
                        if not progress['processed'] and not progress['delivered']:time.sleep(1)
                    result=engine.status(args.scope)
            if hasattr(args,'scope'):result['timing']=engine.state.timing.summary(args.scope)
        print(json.dumps(result,ensure_ascii=False),flush=True)
        return 0 if result.get('status')!='ATTENTION_REQUIRED' else 2
    except KeyboardInterrupt:
        print(json.dumps(dict(status='INTERRUPTED')),flush=True);return 130
    except Exception as exc:
        # Never print SDK messages containing connection strings, source values or secrets.
        code=exc.args[0] if type(exc) is ValueError and len(exc.args)==1 else getattr(exc,'code',type(exc).__name__)
        from ..domain.contracts import Invalid
        if isinstance(exc,Invalid):code=str(exc)
        if not isinstance(code,str) or not re.fullmatch('[A-Za-z][A-Za-z0-9_]{0,127}',code):code=type(exc).__name__
        print(json.dumps(dict(status='FAILED',error_code=code)),flush=True);return 2
