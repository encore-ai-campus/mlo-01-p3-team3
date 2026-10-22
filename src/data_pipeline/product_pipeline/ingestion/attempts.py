"""Bounded external attempts with lease renewal and durable failure evidence."""
import random
import time
from uuid import uuid4
from ..contracts import now
from ..retry import retryable, error_code


class Attempts:
    def __init__(self, settings, ledger, audit, run_id, sleep):
        self.settings, self.ledger, self.audit = settings, ledger, audit
        self.run_id, self.sleep = run_id, sleep

    def perform(self, key, stage, operation, token=None, attempts=None):
        settings, ledger, audit, run_id, sleep = self.settings, self.ledger, self.audit, self.run_id, self.sleep
        attempts = attempts or settings.max_attempts
        for index in range(attempts):
            execution = str(uuid4())
            started = time.monotonic()
            try:
                if token is not None:
                    ledger.renew(key, token)
                result = operation()
                ledger.attempt(key, stage, "SUCCEEDED", execution, round((time.monotonic() - started) * 1000))
                return result
            except Exception as exc:
                code = error_code(exc)
                # A local attempt is recoverable evidence when DB logging itself is unavailable.
                audit.write("attempts/" + execution + ".json", {
                    "ingestion_run_id": run_id, "execution_id": execution, "observation_key": key,
                    "stage": stage, "attempt": index + 1, "status": "FAILED", "error_code": code,
                    "duration_ms": round((time.monotonic() - started) * 1000), "recorded_at": now()})
                try:
                    ledger.attempt(key, stage, "FAILED", execution, round((time.monotonic() - started) * 1000), error=code)
                except Exception:
                    pass  # local evidence above remains until Mongo can be reconciled
                if index + 1 >= attempts or not retryable(exc):
                    raise
                sleep(random.uniform(0, min(60, 2 ** index)))
