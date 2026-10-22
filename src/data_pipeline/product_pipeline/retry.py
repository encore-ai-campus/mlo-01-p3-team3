"""Transient error classification shared by ingestion and scheduled workers."""
from .errors import Conflict, LeaseLost, PipelineError


def retryable(exc):
    if isinstance(exc, (Conflict, LeaseLost)):
        return False
    if isinstance(exc, PipelineError):
        return exc.code in {"OBJECT_READ_FAILED", "OBJECT_WRITE_UNCONFIRMED", "RAW_UNCONFIRMED",
                            "REGISTRATION_UNCONFIRMED", "ATTEMPT_LOG_UNCONFIRMED"}
    try:
        from pymongo.errors import ConnectionFailure, ExecutionTimeout, WriteConcernError
        if isinstance(exc, (ConnectionFailure, ExecutionTimeout, WriteConcernError)):
            return True
    except ImportError:
        pass
    return isinstance(exc, (OSError, TimeoutError))



def error_code(exc):
    return exc.code if isinstance(exc, PipelineError) else type(exc).__name__
