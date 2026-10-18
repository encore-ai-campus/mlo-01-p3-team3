"""Bounded async job runner; every blocking section uses one shared executor."""
import asyncio
import functools
import contextvars
import json
import logging
import time
import traceback
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone as utc

from asgiref.sync import sync_to_async
from django.conf import settings
from django.db import close_old_connections, connections
from django.utils import timezone
from threadpoolctl import threadpool_limits

from interior_recommender.application.deferred import DeferredPipeline
from interior_recommender.application import preview
from interior_recommender.application.locks import RunBusy, execution_lock
from interior_recommender.assets import checksum, png_bytes, read_json, write_json
from interior_recommender.config import Settings
from interior_recommender.errors import LeaseLost, ProviderCallRequired, RetryScheduled
from interior_recommender.pipeline import contract_hash

from . import queue, services
from .execution_config import limits, scope
from .models import RecommendationJob

log = logging.getLogger("sketchon.recommendations")
execution_context = contextvars.ContextVar("recommendation_execution", default={})


def database_call(function, *args, **kwargs):
    started = time.monotonic()
    close_old_connections()
    try:
        return function(*args, **kwargs)
    finally:
        close_old_connections()
        log.debug(json.dumps({**execution_context.get(), "event": "db.finished", "operation": getattr(function, "__qualname__", type(function).__name__),
                              "duration_ms": round((time.monotonic() - started) * 1000)}))


async def db(function, *args, **kwargs):
    return await sync_to_async(database_call, thread_sensitive=True)(function, *args, **kwargs)


class SyncPool:
    def __init__(self, count):
        self.executor = ThreadPoolExecutor(max_workers=count, thread_name_prefix="recommendation-sync")
        self.slots = asyncio.Semaphore(count)

    async def run(self, function, *args):
        requested = time.monotonic()
        context = execution_context.get()
        async with self.slots:
            def invoke():
                started = time.monotonic()
                try:
                    return function(*args)
                finally:
                    connections.close_all()
                    log.debug(json.dumps({**context, "event": "sync.finished", "operation": getattr(function, "__qualname__", type(function).__name__),
                        "wait_ms": round((started - requested) * 1000), "duration_ms": round((time.monotonic() - started) * 1000)}))
            future = asyncio.get_running_loop().run_in_executor(self.executor, invoke)
            try:
                return await asyncio.shield(future)
            except asyncio.CancelledError:
                # A Python thread cannot be cancelled. Keep its run lock until
                # it has actually stopped touching files.
                try:
                    await asyncio.shield(future)
                except Exception:
                    pass
                raise

    def close(self):
        self.executor.shutdown(wait=True, cancel_futures=True)


class Worker:
    def __init__(self, engine=None):
        self.cfg = limits()
        self.pool = SyncPool(self.cfg["sync_workers"])
        self.engine = engine
        self.tasks = {}
        self.presence = None
        self.heartbeat_task = None
        self.blas_limit = None

    async def start(self):
        try:
            self.engine = self.engine or await self.pool.run(services.studio)
            model_settings = Settings.from_env()
            self.engine.index.validate_settings(model_settings)
            self.blas_limit = threadpool_limits(limits=self.cfg["blas_threads"])
            contract = {"index_version": self.engine.index.version, "settings": model_settings.model_dump(),
                        "policy": self.engine.policy.model_dump(),
                        "categories": sorted({p.category_code for p in self.engine.index.products if p.category_code}
                                             | {"CHAIR", "SOFA", "TABLE"})}
            self.presence = await db(queue.advertise, contract)
            self.heartbeat_task = asyncio.create_task(self._heartbeat())
        except BaseException:
            if self.blas_limit:
                self.blas_limit.restore_original_limits()
            self.pool.close()
            raise

    async def _heartbeat(self):
        while True:
            await asyncio.sleep(self.cfg["heartbeat_seconds"])
            await db(queue.heartbeat, [claimed for _, claimed in self.tasks.values()], self.presence)

    def _recover_staging(self):
        # File verification is outside all DB transactions. The status/token
        # update is conditional so a late HTTP response cannot steal a claim.
        expired = list(RecommendationJob.objects.filter(scope=scope(), status="STAGING", lease_until__lte=timezone.now()))
        for job in expired:
            directory = self.engine.root / "runs" / str(job.pk)
            try:
                with execution_lock(directory):
                    manifest = read_json(directory / "manifest.json")
                    if str(job.id) != manifest["run_id"] or manifest["input_hash"] != contract_hash(manifest):
                        raise ValueError("Input hash mismatch")
                    for name, asset in manifest["assets"].items():
                        if checksum((directory / name).read_bytes()) != asset["checksum"]:
                            raise ValueError("Incomplete input")
                    queue.staged(job)
            except (RunBusy, LeaseLost):
                continue
            except (OSError, ValueError, KeyError):
                queue.staging_failed(job)

    async def tick(self):
        if self.heartbeat_task and self.heartbeat_task.done():
            # Fail closed if leases/readiness can no longer be renewed.
            await self.heartbeat_task
        await self.pool.run(self._recover_staging)
        for identifier, (task, _) in list(self.tasks.items()):
            if task.done():
                await task
                del self.tasks[identifier]
        for _ in range(self.cfg["max_inflight"] - len(self.tasks)):
            claimed = await db(queue.claim)
            if not claimed:
                break
            task = asyncio.create_task(self.execute(claimed))
            self.tasks[claimed["id"]] = (task, claimed)

    async def drain_due(self):
        """Process the current due work; future RETRY_WAIT jobs stay durable."""
        while True:
            await self.tick()
            if not self.tasks:
                return
            await asyncio.gather(*(task for task, _ in self.tasks.values()))

    async def run(self, stop):
        while not stop.is_set():
            await self.tick()
            try:
                await asyncio.wait_for(stop.wait(), timeout=self.cfg["poll_seconds"])
            except TimeoutError:
                pass

    async def close(self):
        # Stop admitting work, drain active calls, then retire readiness.
        try:
            if self.presence:
                await db(queue.retire, self.presence)
                self.presence = None
            await asyncio.gather(*(task for task, _ in self.tasks.values()), return_exceptions=True)
        finally:
            if self.heartbeat_task:
                self.heartbeat_task.cancel()
                await asyncio.gather(self.heartbeat_task, return_exceptions=True)
            if self.blas_limit:
                self.blas_limit.restore_original_limits()
            self.pool.close()

    def _load(self, directory, claimed):
        pipeline = DeferredPipeline(directory, self.engine.factory, functools.partial(queue.assert_lease, claimed), claimed["generation"])
        if pipeline.manifest.get("execution_backend") != "sqlite-jobs-v1":
            raise ValueError("Old runs must not be silently migrated")
        if "preview_contract" in pipeline.manifest:
            preview.validate(pipeline, self.engine.demo)
            return pipeline
        self.engine.validate_run(pipeline)
        if not pipeline.manifest["stages"].get("input", {}).get("status") == "SUCCEEDED":
            sizes = []
            for role in ("room", "sketch"):
                source = f"inputs/original-{role}.bin"
                if source not in pipeline.manifest["assets"]:
                    continue
                data, size = png_bytes(pipeline.asset(source), white_background=True, allow_webp=role == "room")
                if size[0] * size[1] > 16_000_000:
                    raise ValueError("Image pixel limit exceeded")
                sizes.append(size)
                pipeline.persist_asset(f"derived/{role}.png", data, size)
            if len(sizes) == 2 and sizes[0] != sizes[1]:
                raise ValueError("Normalized image dimensions differ")
            pipeline.manifest["stages"]["input"] = {"status": "SUCCEEDED", "attempts": []}
            pipeline.save()
        return pipeline

    def _advance(self, pipeline):
        pipeline.guard()
        if "preview_contract" in pipeline.manifest:
            preview.prepare(pipeline)
        else:
            self.engine.prepare(pipeline)

    def _adapter(self, pipeline, kind):
        adapter = self.engine.factory(pipeline.settings)
        if hasattr(adapter, "prepare_async"):
            adapter.prepare_async(kind)
        return adapter

    async def execute(self, claimed):
        started = time.monotonic()
        context_token = execution_context.set({"job_id": claimed["id"], "execution_id": claimed["execution_id"]})
        directory = self.engine.root / "runs" / claimed["id"]
        lock, pipeline, adapter = execution_lock(directory), None, None
        locked = False
        outcome, error = "FAILED", "추천 처리에 실패했습니다. 입력을 확인하거나 같은 요청을 다시 시도해 주세요."
        retry_at = None
        log.info(json.dumps({"event": "job.started", "job_id": claimed["id"], "execution_id": claimed["execution_id"],
                             "occurred_at": timezone.now().isoformat(), "duration_ms": 0, "wait_ms": claimed["wait_ms"]}))
        try:
            await self.pool.run(lock.__enter__)
            locked = True
            pipeline = await self.pool.run(self._load, directory, claimed)
            while True:
                try:
                    await self.pool.run(self._advance, pipeline)
                    outcome, error = "SUCCEEDED", ""
                    break
                except ProviderCallRequired as call:
                    model = pipeline.settings.embedding_model if call.kind == "embed_text" else pipeline.settings.image_model if call.kind == "generate" else pipeline.settings.analysis_model
                    permit = None
                    if not self.engine.demo:
                        provider = pipeline.settings.image_provider if call.kind == "generate" else "gemini"
                        permit, due = await db(queue.acquire_provider, claimed, model, call.kind, provider)
                        if due:
                            raise RetryScheduled(due.timestamp())
                    try:
                        if adapter is None:
                            adapter = await self.pool.run(self._adapter, pipeline, call.kind)
                        await self.pool.run(pipeline.begin_call, call)
                        api_started = time.monotonic()
                        try:
                            if hasattr(adapter, "acall"):
                                async with asyncio.timeout(pipeline.settings.timeout_seconds):
                                    response = await adapter.acall(call.kind, call.prompt, call.images, call.schema)
                            else:
                                response = await self.pool.run(adapter.call, call.kind, call.prompt, call.images, call.schema)
                        except Exception as failure:
                            await self.pool.run(pipeline.reject_call, call, failure, round((time.monotonic() - api_started) * 1000))
                        else:
                            await self.pool.run(pipeline.accept_call, call, response, round((time.monotonic() - api_started) * 1000))
                    finally:
                        if permit:
                            await db(queue.release_provider, permit)
        except RetryScheduled as scheduled:
            outcome, error = "RETRY_WAIT", ""
            retry_at = datetime.fromtimestamp(scheduled.timestamp, tz=utc.utc)
        except RunBusy:
            outcome, error = "RETRY_WAIT", ""
            retry_at = timezone.now() + timedelta(seconds=self.cfg["poll_seconds"])
        except LeaseLost:
            outcome = "LEASE_LOST"
        except asyncio.CancelledError:
            outcome, error = "RETRY_WAIT", ""
            retry_at = timezone.now()
            raise
        except Exception as failure:
            # Exception strings may carry provider request URLs. Persist codes
            # and fixed public text; provider diagnostics are sanitized by the pipeline.
            frame = traceback.extract_tb(failure.__traceback__)[-1]
            log.warning(json.dumps({"event": "job.error", "job_id": claimed["id"],
                                   "error_code": type(failure).__name__, "module": frame.name, "line": frame.lineno}))
        finally:
            try:
                if adapter is not None:
                    try:
                        if hasattr(adapter, "aclose"):
                            await adapter.aclose()
                    except Exception:
                        log.warning("Recommendation async client close failed")
                    finally:
                        try:
                            await self.pool.run(adapter.close)
                        except Exception:
                            log.warning("Recommendation sync client close failed")
                duration = round((time.monotonic() - started) * 1000)
                if outcome != "LEASE_LOST":
                    try:
                        await db(queue.finish, claimed, outcome, duration, retry_at=retry_at, error=error)
                    except LeaseLost:
                        outcome = "LEASE_LOST"
                log.info(json.dumps({"event": "job.finished", "job_id": claimed["id"], "execution_id": claimed["execution_id"],
                                     "status": outcome, "duration_ms": duration}))
            finally:
                try:
                    if locked:
                        await self.pool.run(lock.__exit__, None, None, None)
                finally:
                    execution_context.reset(context_token)
