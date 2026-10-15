import uuid
from django.db import models


def empty_document():
    return {"strokes": [], "items": []}


class Project(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    owner = models.CharField(max_length=32, db_index=True)
    title = models.CharField(max_length=100)
    photo = models.FileField(upload_to="rooms/", blank=True)
    original_photo = models.FileField(upload_to="originals/", blank=True)
    original_checksum = models.CharField(max_length=64, blank=True)
    request_context = models.JSONField(default=dict)
    width = models.PositiveIntegerField(default=1200)
    height = models.PositiveIntegerField(default=800)
    document = models.JSONField(default=empty_document)
    revision = models.PositiveIntegerField(default=0)
    locked = models.BooleanField(default=False)
    updated_at = models.DateTimeField(auto_now=True)


class SketchEvent(models.Model):
    event_id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    project = models.ForeignKey(Project, on_delete=models.PROTECT)
    event_type = models.CharField(max_length=40)
    event_time = models.DateTimeField(auto_now_add=True)
    payload = models.JSONField(default=dict)
    published_at = models.DateTimeField(null=True, blank=True)

    class Meta:
        indexes = [models.Index(fields=["published_at", "event_time"])]


class Composition(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    project = models.ForeignKey(Project, on_delete=models.PROTECT)
    image = models.FileField(upload_to="results/")
    input_document = models.JSONField()
    placements = models.JSONField()
    revision = models.PositiveIntegerField()
    version = models.PositiveIntegerField(default=0)
    generation_context = models.JSONField(default=dict)
    initial_placements = models.JSONField(default=list)
    created_at = models.DateTimeField(auto_now_add=True)


class RecommendationRun(models.Model):
    """Application-owned request/result; an Ad provider never writes user history."""
    id = models.UUIDField(primary_key=True, editable=False)
    composition = models.ForeignKey(Composition, on_delete=models.PROTECT)
    scene_object_id = models.UUIDField()
    input_version = models.PositiveIntegerField()
    input_hash = models.CharField(max_length=64)
    lease_token = models.UUIDField(null=True, blank=True)
    lease_expires_at = models.DateTimeField(null=True, blank=True)
    attempt_count = models.PositiveIntegerField(default=0)
    status = models.CharField(max_length=20, default="SUCCEEDED")
    mode = models.CharField(max_length=20, default="mock")
    items = models.JSONField(default=list)
    metadata = models.JSONField(default=dict)
    created_at = models.DateTimeField(auto_now_add=True)


class SceneObjectRevision(models.Model):
    """An explicit search correction; the generated scene image stays unchanged."""
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    composition = models.ForeignKey(Composition, on_delete=models.PROTECT)
    placement_id = models.CharField(max_length=80)
    base_version = models.PositiveIntegerField()
    previous_object_id = models.UUIDField()
    category_id = models.CharField(max_length=40)
    bbox = models.JSONField()
    request_id = models.UUIDField(unique=True)
    input_hash = models.CharField(max_length=64)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        indexes = [models.Index(fields=["composition", "placement_id", "created_at"], name="scene_revision_lookup")]
