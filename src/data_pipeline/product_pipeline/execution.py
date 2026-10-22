"""Structured stage outcomes, independent of argument parsing and stdout."""
from dataclasses import dataclass

from .errors import PipelineError


@dataclass(frozen=True)
class StageResult:
    code: int
    report: dict

    def verified(self):
        if self.code:
            error = PipelineError(self.report.get('error_code', 'FLOW_STAGE_COMMAND_FAILED'))
            error.stage_error = True
            for name in ('lease_failure', 'lease_owner_ref'):
                if self.report.get(name) is not None:setattr(error,name,self.report[name])
            raise error
        if (self.report.get('status') not in {'COMPLETED', 'PLANS_PREPARED'}
                and self.report.get('result') != 'completed'):
            raise PipelineError('FLOW_COMMAND_RESULT_UNCONFIRMED')
        return self.report


def ignore_progress(value):
    """Default for callers which consume only the final structured outcome."""
