"""Reuse the direct policy/stages while yielding provider I/O to an async runner.

_prepare runs in short synchronous sections. A missing response yields a call;
the runner awaits it, persists its receipt, and resumes with completed stages.
"""
import random
import time
from email.utils import parsedate_to_datetime
from uuid import uuid4

import httpx

from ..assets import checksum, write_json
from ..errors import ProviderCallRequired, RetryScheduled, provider_message
from ..pipeline import Pipeline, now
from ..services import PROMPT_VERSION, SCHEMA_VERSION, SCHEMAS


class DeferredPipeline(Pipeline):
    def __init__(self, directory, adapter_factory, guard, generation=0):
        super().__init__(directory, adapter_factory)
        self.guard, self.generation = guard, generation

    def save(self):
        self.guard()
        super().save()

    def persist_asset(self, *args):
        self.guard()
        return super().persist_asset(*args)

    def response(self, key, kind, prompt, images):
        self.guard()
        state = self.manifest["stages"].setdefault(key, {"status": "PENDING", "attempts": []})
        receipt_path = self.directory / f"receipts/{key}.json"
        if receipt_path.exists():
            # The base method's receipt branch never instantiates an SDK client.
            return super().response(key, kind, prompt, images)
        if state.get("next_retry_at", 0) > time.time():
            raise RetryScheduled(state["next_retry_at"])
        if state.get("retry_generation", 0) < self.generation:
            state.update(attempt_base=len(state["attempts"]), retry_generation=self.generation)
        if len(state["attempts"]) - state.get("attempt_base", 0) >= self.settings.max_attempts:
            raise ValueError("Provider attempt limit reached; explicit retry required")
        schema = self.response_schema(kind).model_json_schema() if kind in SCHEMAS else None
        raise ProviderCallRequired(key, kind, prompt, images, schema)

    def begin_call(self, call):
        self.guard()
        state = self.manifest["stages"][call.key]
        attempt = {"id": str(uuid4()), "attempt_no": len(state["attempts"]) + 1,
                   "started_at": now(), "status": "RUNNING", "estimated_cost_usd": None, "usage_json": None,
                   "model": self.settings.embedding_model if call.kind == "embed_text" else self.settings.image_model if call.kind == "generate" else self.settings.analysis_model,
                   "prompt_version": PROMPT_VERSION, "schema_version": SCHEMA_VERSION}
        # A lost process is counted, not silently overwritten or given a fresh budget.
        for previous in state["attempts"]:
            if previous["status"] == "RUNNING":
                previous.update(status="INTERRUPTED", finished_at=now())
        state["attempts"].append(attempt)
        state["status"] = "RUNNING"
        if call.kind == "generate":
            attempt.update(provider=self.settings.image_provider, output_width=self.settings.output_width,
                           output_height=self.settings.output_height,
                           preprocessing_version=self.settings.generation_preprocessing_version)
        self.manifest["status"] = "GENERATING" if call.kind == "generate" else "INTERPRETING" if call.key == "context" else "SEARCHING"
        self.save()

    def accept_call(self, call, response, duration_ms):
        self.guard()
        state = self.manifest["stages"][call.key]
        attempt = state["attempts"][-1]
        path = self.directory / f"receipts/{call.key}.json"
        # This precedes validation/output; a local failure must not repeat a paid call.
        write_json(path, {**response, "attempt_id": attempt["id"]})
        state["receipt_checksum"] = checksum(path.read_bytes())
        state.pop("next_retry_at", None)
        attempt.update(status="SUCCEEDED", finished_at=now(), latency_ms=duration_ms,
                       usage_json=response.get("usage_json"), provider_request_id=response.get("provider_request_id"))
        self.save()

    def reject_call(self, call, error, duration_ms):
        self.guard()
        state = self.manifest["stages"][call.key]
        attempt = state["attempts"][-1]
        code = getattr(error, "code", None)
        attempt.update(status="FAILED", error_code=str(code or type(error).__name__),
                       finished_at=now(), latency_ms=duration_ms)
        diagnostic = provider_message(error)
        if diagnostic:
            attempt["error_message"] = diagnostic
        if getattr(error, "budget_exhausted", False):
            from datetime import datetime, timedelta, timezone
            attempt.update(status="BUDGET_WAIT", error_code="3036")
            reset = (datetime.now(timezone.utc) + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
            state.update(status="BUDGET_WAIT", next_retry_at=reset.timestamp())
            self.manifest["status"] = "BUDGET_WAIT"
            self.save()
            raise RetryScheduled(state["next_retry_at"])
        transient = code in {429, 500, 502, 503, 504} or isinstance(error, (httpx.TimeoutException, httpx.NetworkError, TimeoutError))
        used = len(state["attempts"]) - state.get("attempt_base", 0)
        if transient and used < self.settings.max_attempts:
            delay = min(2 ** (used - 1) + random.random(), 30)
            headers = getattr(getattr(error, "response", None), "headers", {}) or {}
            retry_after = headers.get("retry-after")
            if retry_after:
                try:
                    delay = max(delay, float(retry_after))
                except (ValueError, TypeError):
                    try:
                        delay = max(delay, parsedate_to_datetime(retry_after).timestamp() - time.time())
                    except (ValueError, TypeError, OverflowError):
                        pass
            state.update(status="RETRY_WAIT", next_retry_at=time.time() + delay)
            self.manifest["status"] = "RETRY_WAIT"
            self.save()
            raise RetryScheduled(state["next_retry_at"])
        state["status"] = "FAILED"
        self.manifest["status"] = "FAILED"
        self.save()
        raise ValueError("Provider request failed; inspect sanitized manifest attempts") from None
