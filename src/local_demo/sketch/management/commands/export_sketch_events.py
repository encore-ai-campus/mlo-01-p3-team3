import json
from django.core.management.base import BaseCommand
from sketch.models import SketchEvent


class Command(BaseCommand):
    help = "행동 이벤트를 NDJSON으로 출력합니다. Kafka 전송 또는 published_at 변경은 하지 않습니다."

    def add_arguments(self, parser):
        parser.add_argument("--project", help="특정 프로젝트 UUID만 내보내기")

    def handle(self, *args, **options):
        events = SketchEvent.objects.select_related("project").order_by("event_time", "event_id")
        if options["project"]:
            events = events.filter(project_id=options["project"])
        for event in events.iterator():
            self.stdout.write(json.dumps({"schema_version": 1, "source": "roompick", "event_id": str(event.event_id),
                "event_type": event.event_type, "visitor_id": event.project.owner, "project_id": str(event.project_id), "event_time": event.event_time.isoformat(),
                "payload": event.payload}, ensure_ascii=False, allow_nan=False))
