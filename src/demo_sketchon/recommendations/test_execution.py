"""Exercise recovery/concurrency with isolated SQLite and synthetic, unpaid AI."""
import asyncio
import io
import json
import os
import tempfile
import subprocess
import sys
import time
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from pathlib import Path
from unittest.mock import patch
from uuid import uuid4

import httpx
from django.conf import settings
from django.db import connection, connections
from django.core.files.uploadedfile import SimpleUploadedFile
from django.test import TransactionTestCase, override_settings
from django.utils import timezone
from PIL import Image

from interior_recommender.application.direct import DirectDemoAdapter, DirectStudio
from interior_recommender.config import Settings
from interior_recommender.application.locks import RunBusy, execution_lock
from interior_recommender.assets import read_json, write_json
from interior_recommender.errors import LeaseLost
from interior_recommender.pipeline import Pipeline
from . import queue, submissions
from .models import ProviderGate, ProviderPermit, RecommendationJob
from .worker import Worker

BASE = "/interior-recommand/"


class ExecutionTests(TransactionTestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix=".execution-test-", dir=settings.BASE_DIR)
        self.addCleanup(self.temp.cleanup)
        self.override = override_settings(INTERIOR_RECOMMENDER_OUTPUT_ROOT=Path(self.temp.name), INTERIOR_RECOMMENDER_DEMO=True)
        self.override.enable()
        self.addCleanup(self.override.disable)
        self.calls = []
        calls = self.calls

        class Offline(DirectDemoAdapter):
            def call(self, kind, *args, **kwargs):
                calls.append(kind)
                return super().call(kind, *args, **kwargs)

        self.engine = DirectStudio(Path(self.temp.name), demo=True, adapter_factory=Offline, background_workers=0)
        self.calls.clear()  # synthetic catalog setup is not online recommendation traffic
        self.runner = asyncio.Runner()
        self.worker = Worker(self.engine)
        self.runner.run(self.worker.start())
        self.addCleanup(self.close_worker)

    def close_worker(self):
        self.runner.run(self.worker.close())
        self.runner.close()

    def image(self):
        data = io.BytesIO()
        Image.new("RGB", (32, 24), "white").save(data, "PNG")
        return data.getvalue()

    def submit(self, request_id=None):
        return self.client.post(BASE + "api/runs", {
            "photo": SimpleUploadedFile("room.png", self.image()), "text": "의자",
            "request_id": request_id or str(uuid4())})

    def make_due(self, job_id):
        RecommendationJob.objects.filter(pk=job_id).update(available_at=timezone.now() - timedelta(seconds=1))
        path = self.engine.root / "runs" / str(job_id) / "manifest.json"
        manifest = read_json(path)
        for stage in manifest["stages"].values():
            if "next_retry_at" in stage:
                stage["next_retry_at"] = 0
        write_json(path, manifest)

    def drain(self):
        self.runner.run(self.worker.drain_due())

    def test_http_only_enqueues_and_survives_worker_unavailability(self):
        request_id = str(uuid4())
        with patch("recommendations.services.studio", side_effect=AssertionError("HTTP loaded a catalog")):
            response = self.submit(request_id)
            self.assertEqual(response.status_code, 202)
            job = RecommendationJob.objects.get()
            self.assertEqual(job.status, "QUEUED")
            self.assertEqual(self.calls, [])
            self.assertEqual(self.client.get(BASE + f"api/runs/{job.id}").json()["job"]["status"], "QUEUED")
            queue.retire(self.worker.presence)
            self.assertEqual(self.submit(request_id).json()["run_id"], str(job.id))
            unavailable = self.submit()
            self.assertEqual(unavailable.status_code, 503)
            self.assertEqual(unavailable["Retry-After"], "3")
            self.assertEqual(RecommendationJob.objects.count(), 1)

    def test_capacity_is_reserved_before_files_and_replays_do_not_take_slots(self):
        cfg = {**settings.RECOMMENDATION_EXECUTION, "max_pending": 2}
        with override_settings(RECOMMENDATION_EXECUTION=cfg):
            self.runner.run(self.worker.close())
            self.worker = Worker(self.engine)
            self.runner.run(self.worker.start())
            request_id = str(uuid4())
            self.assertEqual(self.submit(request_id).status_code, 202)
            self.assertEqual(self.submit().status_code, 202)
            self.assertEqual(self.submit(request_id).status_code, 202)
            self.assertEqual(self.submit().status_code, 503)
            self.assertEqual(RecommendationJob.objects.count(), 2)
            self.assertEqual(len(list((self.engine.root / "runs").iterdir())), 2)

    def test_old_file_request_replay_keeps_existing_result_without_enqueuing(self):
        from .views import engine_owner
        session = self.client.session
        session["sketch_owner"] = "legacy-visitor"
        session.save()
        access = {"visitor_id": "legacy-visitor"}
        owner, request_id = engine_owner(access), str(uuid4())
        document = {"strokes": [], "items": []}
        run_id = self.engine.submit(self.image(), None, "의자", [], None, owner, request_id,
            canvas_document=document, project_metadata={"sketchon_access": access})
        directory = self.engine.directory(run_id, owner)
        pipeline = Pipeline(directory, self.engine.factory)
        try:
            self.engine.prepare(pipeline)
        finally:
            pipeline.adapter.close()
        write_json(directory / "job.json", {"status": "SUCCEEDED", "instance": "old-process"})
        original = (directory / "recommendations.json").read_bytes()
        response = self.submit(request_id)
        self.assertEqual(response.status_code, 202)
        self.assertEqual(response.json()["run_id"], run_id)
        self.assertEqual(RecommendationJob.objects.count(), 0)
        self.assertEqual(self.calls, ["direct_context", "embed_text"])
        self.assertEqual((directory / "recommendations.json").read_bytes(), original)

    def test_expired_lease_is_reclaimed_and_old_completion_is_rejected(self):
        job_id = self.submit().json()["run_id"]
        old = queue.claim()
        directory = self.engine.root / "runs" / job_id
        pipeline = self.worker._load(directory, old)
        original_manifest = (directory / "manifest.json").read_bytes()
        RecommendationJob.objects.filter(pk=job_id).update(lease_until=timezone.now() - timedelta(seconds=1))
        new = queue.claim()
        self.assertNotEqual(old["token"], new["token"])
        with self.assertRaises(LeaseLost):
            queue.finish(old, "SUCCEEDED", 123)
        with self.assertRaises(LeaseLost):
            queue.assert_lease(old)
        pipeline.manifest["status"] = "COMPLETED"
        with self.assertRaises(LeaseLost):
            pipeline.save()
        self.assertEqual((directory / "manifest.json").read_bytes(), original_manifest)
        self.runner.run(self.worker.execute(new))
        job = RecommendationJob.objects.get(pk=job_id)
        self.assertEqual(job.status, "SUCCEEDED")
        lost = job.executions.get(lease_token=old["token"])
        self.assertEqual(lost.status, "ABANDONED")
        self.assertIsNone(lost.duration_ms)

    def test_failed_input_storage_reuses_reserved_id_on_resubmission(self):
        request_id = str(uuid4())
        with patch("recommendations.submissions.write_bytes", side_effect=OSError("disk unavailable")):
            self.assertEqual(self.submit(request_id).status_code, 503)
        job = RecommendationJob.objects.get()
        self.assertEqual(job.status, "STAGING_FAILED")
        response = self.submit(request_id)
        self.assertEqual(response.status_code, 202)
        self.assertEqual(response.json()["run_id"], str(job.id))
        self.assertEqual(RecommendationJob.objects.count(), 1)
        self.drain()
        self.assertEqual(RecommendationJob.objects.get().status, "SUCCEEDED")

    def test_concurrent_identical_submissions_reserve_one_run(self):
        owner, request_id = str(uuid4()), str(uuid4())
        data, barrier = self.image(), threading.Barrier(2)

        def submit_identical():
            try:
                barrier.wait(timeout=5)
                return submissions.submit(data, None, "의자", {"strokes": [], "items": []}, None,
                    owner, request_id, {"sketchon_access": {"visitor_id": "fixture"}})[0]
            except queue.QueueUnavailable:
                return None  # the winning request is still saving immutable input
            finally:
                connections.close_all()

        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(lambda _: submit_identical(), range(2)))
        self.assertEqual(RecommendationJob.objects.count(), 1)
        identifier = str(RecommendationJob.objects.get().id)
        self.assertTrue(any(result == identifier for result in results))
        self.assertTrue(all(result in {None, identifier} for result in results))

    def test_live_os_lock_is_not_deleted_when_database_lease_expires(self):
        job_id = self.submit().json()["run_id"]
        claimed = queue.claim()
        directory = self.engine.root / "runs" / job_id
        with execution_lock(directory):
            self.runner.run(self.worker.execute(claimed))
            self.assertEqual(RecommendationJob.objects.get(pk=job_id).status, "RETRY_WAIT")
            self.assertEqual(self.calls, [])
        self.make_due(job_id)
        self.drain()
        self.assertEqual(RecommendationJob.objects.get(pk=job_id).status, "SUCCEEDED")

    def test_process_death_releases_os_lock_without_deleting_lock_file(self):
        directory = self.engine.root / "crash-lock"
        directory.mkdir()
        script = ("from pathlib import Path\nimport sys,time\n"
                  "from interior_recommender.application.locks import execution_lock\n"
                  "with execution_lock(Path(sys.argv[1])):\n"
                  " Path(sys.argv[1], 'ready').write_text('ready')\n time.sleep(60)\n")
        process = subprocess.Popen([sys.executable, "-B", "-c", script, str(directory)],
            stdout=subprocess.DEVNULL, stderr=subprocess.PIPE,
            creationflags=subprocess.CREATE_NO_WINDOW if sys.platform == "win32" else 0)
        try:
            deadline = time.monotonic() + 10
            while not (directory / "ready").exists() and process.poll() is None and time.monotonic() < deadline:
                time.sleep(.01)
            self.assertTrue((directory / "ready").exists(), "Child failed to acquire execution lock")
            process.kill()
            process.wait(timeout=5)
            deadline = time.monotonic() + 5
            while True:
                try:
                    with execution_lock(directory):
                        self.assertTrue((directory / ".worker-lock").exists())
                    break
                except RunBusy:
                    if time.monotonic() >= deadline:
                        raise
                    time.sleep(.01)
        finally:
            if process.poll() is None:
                process.kill()
                process.wait(timeout=5)
            process.stderr.close()

    def test_crash_after_snapshot_before_enqueue_is_recovered(self):
        with patch("recommendations.queue.staged", side_effect=SystemExit("simulated crash")):
            with self.assertRaises(SystemExit):
                submissions.submit(self.image(), None, "의자", {"strokes": [], "items": []}, None,
                    str(uuid4()), str(uuid4()), {"sketchon_access": {"visitor_id": "fixture"}})
        job = RecommendationJob.objects.get()
        self.assertEqual(job.status, "STAGING")
        RecommendationJob.objects.filter(pk=job.pk).update(lease_until=timezone.now() - timedelta(seconds=1))
        self.drain()
        self.assertEqual(RecommendationJob.objects.get(pk=job.pk).status, "SUCCEEDED")

    def test_retry_wait_releases_worker_and_preserves_successful_stages(self):
        original = self.engine.factory
        failed = False

        class Flaky(original):
            def call(adapter, kind, *args, **kwargs):
                nonlocal failed
                if not failed:
                    failed = True
                    self.calls.append(kind)
                    raise httpx.ConnectError("private request URL must not be persisted")
                return super().call(kind, *args, **kwargs)

        self.engine.factory = Flaky
        first = self.submit().json()["run_id"]
        self.drain()
        waiting = RecommendationJob.objects.get(pk=first)
        self.assertEqual(waiting.status, "RETRY_WAIT")
        self.assertIsNone(waiting.lease_until)
        self.assertGreater(waiting.available_at, timezone.now())
        second = self.submit().json()["run_id"]
        self.drain()
        self.assertEqual(RecommendationJob.objects.get(pk=second).status, "SUCCEEDED")
        self.assertEqual(RecommendationJob.objects.get(pk=first).status, "RETRY_WAIT")
        self.make_due(first)
        self.drain()
        self.assertEqual(RecommendationJob.objects.get(pk=first).status, "SUCCEEDED")
        manifest = read_json(self.engine.root / "runs" / first / "manifest.json")
        self.assertEqual(len(manifest["stages"]["context"]["attempts"]), 2)
        self.assertNotIn("private request", json.dumps(manifest))

    def test_retry_after_and_attempt_budget_survive_new_worker_instances(self):
        class Limited(Exception):
            code = 429
            response = httpx.Response(429, headers={"Retry-After": "120"})

        class AlwaysFail(DirectDemoAdapter):
            def call(adapter, kind, *args, **kwargs):
                self.calls.append(kind)
                raise Limited()

        self.engine.factory = AlwaysFail
        job_id = self.submit().json()["run_id"]
        self.drain()
        self.assertGreater((RecommendationJob.objects.get(pk=job_id).available_at - timezone.now()).total_seconds(), 110)
        self.runner.run(self.worker.close())
        self.worker = Worker(self.engine)
        self.runner.run(self.worker.start())
        for _ in range(2):
            self.make_due(job_id)
            self.drain()
        self.assertEqual(RecommendationJob.objects.get(pk=job_id).status, "FAILED")
        self.assertEqual(len(self.calls), 3)
        queue.retry(job_id)
        self.drain()
        self.assertEqual(len(self.calls), 4)
        self.assertEqual(RecommendationJob.objects.get(pk=job_id).status, "RETRY_WAIT")

    def test_ranking_failure_retry_reuses_both_paid_stage_receipts(self):
        job_id = self.submit().json()["run_id"]
        with patch.object(self.engine.index, "rank", side_effect=ValueError("ranking failure")):
            self.drain()
        self.assertEqual(RecommendationJob.objects.get(pk=job_id).status, "FAILED")
        self.assertEqual(self.calls, ["direct_context", "embed_text"])
        queue.retry(job_id)
        self.drain()
        self.assertEqual(RecommendationJob.objects.get(pk=job_id).status, "SUCCEEDED")
        self.assertEqual(self.calls, ["direct_context", "embed_text"])

    def test_native_async_api_wait_does_not_occupy_sync_pool_threads(self):
        async def exercise():
            entered, release = [], asyncio.Event()

            class AsyncOffline(DirectDemoAdapter):
                async def acall(adapter, kind, *args):
                    if kind == "direct_context":
                        entered.append(kind)
                        await release.wait()
                    return adapter.call(kind, *args)

            self.engine.factory = AsyncOffline
            await self.worker.tick()
            async with asyncio.timeout(5):
                while len(entered) != 2:
                    await asyncio.sleep(.01)
                self.assertEqual(await self.worker.pool.run(lambda: "free"), "free")
                release.set()
                await self.worker.drain_due()

        self.submit()
        self.submit()
        self.runner.run(exercise())
        self.assertEqual(list(RecommendationJob.objects.values_list("status", flat=True)), ["SUCCEEDED", "SUCCEEDED"])

    def test_provider_rate_and_concurrency_are_database_wide(self):
        self.submit()
        self.submit()
        first, second = queue.claim(), queue.claim()
        cfg = {**settings.RECOMMENDATION_EXECUTION, "analysis_concurrency": 1}
        with override_settings(RECOMMENDATION_EXECUTION=cfg):
            permit, due = queue.acquire_provider(first, "fixture-model", "direct_context")
            self.assertIsNotNone(permit)
            self.assertIsNone(due)
            blocked, due = queue.acquire_provider(second, "fixture-model", "direct_context")
            self.assertIsNone(blocked)
            self.assertGreater(due, timezone.now())
            ProviderGate.objects.update(next_call_at=timezone.now() - timedelta(seconds=1))
            blocked, due = queue.acquire_provider(second, "fixture-model", "direct_context")
            self.assertIsNone(blocked)  # rate is free, concurrency still occupied
            queue.release_provider(permit)
            available, due = queue.acquire_provider(second, "fixture-model", "direct_context")
            self.assertIsNotNone(available)
            self.assertEqual(ProviderPermit.objects.count(), 1)

    def test_generation_gate_uses_provider_and_separate_limits(self):
        self.submit()
        self.submit()
        first, second = queue.claim(), queue.claim()
        cfg = {**settings.RECOMMENDATION_EXECUTION, "generation_concurrency": 1, "generation_rpm": 2}
        with override_settings(RECOMMENDATION_EXECUTION=cfg):
            permit, due = queue.acquire_provider(first, "same-model", "generate", "cloudflare")
            self.assertIsNotNone(permit)
            self.assertIsNone(due)
            blocked, due = queue.acquire_provider(second, "same-model", "generate", "cloudflare")
            self.assertIsNone(blocked)
            self.assertGreater((due - timezone.now()).total_seconds(), 20)
            independent, due = queue.acquire_provider(second, "same-model", "generate", "gemini")
            self.assertIsNotNone(independent)
            self.assertIsNone(due)
            queue.release_provider(permit)
            queue.release_provider(independent)

    def test_management_command_executes_enqueued_job_in_another_process(self):
        # Reuse this fixture's three synthetic images/vectors; no new embedding.
        catalog = self.engine.root / "products.json"
        rows = [product.model_dump(mode="json") for product in self.engine.index.products]
        for row in rows:
            row["image"] = Path(row["image"]).relative_to(self.engine.root).as_posix()
        write_json(catalog, {"products": rows, "vectors": self.engine.index.vectors.tolist(),
            "embedding_model": self.engine.index.model, "dimension": self.engine.index.dimension,
            "preprocessing_version": self.engine.index.preprocessing})
        self.runner.run(self.worker.close())
        with override_settings(INTERIOR_RECOMMENDER_PRODUCTS=catalog):
            self.engine = DirectStudio(self.engine.root, products=catalog, demo=True, background_workers=0)
            self.worker = Worker(self.engine)
            self.runner.run(self.worker.start())
            job_id = self.submit().json()["run_id"]
            environment = os.environ.copy()
            environment.update(ROOMPICK_DB_PATH=str(connection.settings_dict["NAME"]),
                INTERIOR_RECOMMENDER_OUTPUT_ROOT=str(self.engine.root), INTERIOR_RECOMMENDER_PRODUCTS=str(catalog),
                INTERIOR_RECOMMENDER_DEMO="1", PYTHON_DOTENV_DISABLED="1", PYTHONDONTWRITEBYTECODE="1")
            for key, value in settings.RECOMMENDATION_EXECUTION.items():
                environment["RECOMMENDATION_" + key.upper()] = str(value)
            environment["RECOMMENDATION_API_ACCOUNT"] = settings.RECOMMENDATION_API_ACCOUNT
            model_settings = Settings.from_env()
            for field, name in {"analysis_model":"AI_ANALYSIS_MODEL", "embedding_model":"AI_EMBEDDING_MODEL",
                "dimension":"AI_EMBEDDING_DIMENSION", "image_model":"AI_IMAGE_MODEL", "image_provider":"AI_IMAGE_PROVIDER",
                "timeout_seconds":"AI_TIMEOUT_SECONDS", "max_attempts":"AI_MAX_ATTEMPTS",
                "output_width":"AI_OUTPUT_WIDTH", "output_height":"AI_OUTPUT_HEIGHT"}.items():
                environment[name] = str(getattr(model_settings, field))
            process = subprocess.run([sys.executable, "-B", "manage.py", "run_recommendation_worker", "--once"],
                cwd=settings.BASE_DIR, env=environment, capture_output=True, text=True, timeout=30,
                creationflags=subprocess.CREATE_NO_WINDOW if sys.platform == "win32" else 0)
            self.assertEqual(process.returncode, 0, process.stderr)
            self.assertEqual(RecommendationJob.objects.get(pk=job_id).status, "SUCCEEDED")
            self.assertEqual(self.client.get(BASE + f"api/runs/{job_id}").status_code, 200)
