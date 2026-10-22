"""One fixed stage per child process; only a sealed response authorizes success."""
import argparse
import io
from pathlib import Path

from ..errors import PipelineError
from ..files import atomic_json, parse, read_json
from .flow import seal, verified


class Progress(io.TextIOBase):
    def __init__(self): self.tail=''; self.last=None
    def write(self, text):
        self.tail=(self.tail+text)[-65536:]
        while '\n' in self.tail:
            line,self.tail=self.tail.split('\n',1)
            try:
                value=parse(line)
                if isinstance(value,dict): self.last=value
            except Exception: pass
        return len(text)


def invoke(main, arguments):
    from contextlib import redirect_stdout
    capture=Progress()
    with redirect_stdout(capture): code=main(arguments)
    result=capture.last or {}
    if code:
        error=PipelineError(result.get('error_code','FLOW_STAGE_COMMAND_FAILED'))
        error.stage_error=True
        for name in ('lease_failure','lease_owner_ref'):
            if result.get(name) is not None:setattr(error,name,result[name])
        raise error
    if result.get('status') not in {'COMPLETED','PLANS_PREPARED'} and result.get('result')!='completed':
        raise PipelineError('FLOW_COMMAND_RESULT_UNCONFIRMED')
    return result


def main():
    p=argparse.ArgumentParser();p.add_argument('--context',required=True,type=Path);p.add_argument('--output',required=True,type=Path)
    args=p.parse_args();context=verified(read_json(args.context))
    try:
        from .flow_runtime import perform
        output=perform(context)
        result=dict(status='SUCCEEDED',context_hash=context['content_hash'],output=output);code=0
    except Exception as exc:
        from ..retry import retryable
        code_name=exc.code if isinstance(exc,PipelineError) else type(exc).__name__
        result=dict(status='FAILED',context_hash=context['content_hash'],
                    error_code=code_name,retryable=retryable(exc) or code_name in {
                        'TimeoutError','AutoReconnect','NetworkTimeout','ConnectionFailure',
                        'ServerSelectionTimeoutError','OperationalError','FLOW_SILVER_PARTIAL',
                        'FLOW_GOLD_INCOMPLETE_OR_EMPTY'});code=2
        for name in ('lease_failure','lease_owner_ref'):
            if getattr(exc,name,None) is not None:result[name]=getattr(exc,name)
    atomic_json(args.output,seal(result));return code


if __name__=='__main__':raise SystemExit(main())
