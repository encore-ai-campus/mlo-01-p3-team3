import asyncio
import signal
import logging

from django.core.management.base import BaseCommand
from recommendations.worker import Worker


class Command(BaseCommand):
    help = "Run the SQLite-backed direct recommendation worker (no Kafka required)."

    def add_arguments(self, parser):
        parser.add_argument("--once", action="store_true", help="Drain currently due jobs, then exit.")

    def handle(self, *args, **options):
        logger = logging.getLogger("sketchon.recommendations")
        if not logger.handlers:
            logger.addHandler(logging.StreamHandler(self.stderr))
        logger.setLevel(logging.DEBUG if options["verbosity"] > 1 else logging.INFO)
        asyncio.run(self.run_worker(options["once"]))

    async def run_worker(self, once):
        worker = Worker()
        await worker.start()
        self.stdout.write(self.style.SUCCESS("추천 카탈로그 준비 완료. 작업 워커를 시작합니다."))
        stop = asyncio.Event()
        loop = asyncio.get_running_loop()
        previous = {}
        try:
            for signum in (signal.SIGINT, signal.SIGTERM):
                previous[signum] = signal.signal(signum, lambda *_: loop.call_soon_threadsafe(stop.set))
            if once:
                await worker.drain_due()
            else:
                await worker.run(stop)
        finally:
            for signum, handler in previous.items():
                signal.signal(signum, handler)
            await worker.close()
