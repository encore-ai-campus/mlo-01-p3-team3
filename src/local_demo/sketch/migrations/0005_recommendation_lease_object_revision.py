from django.db import migrations, models
import django.db.models.deletion
import uuid


class Migration(migrations.Migration):
    dependencies = [("sketch", "0004_platform_preview")]
    operations = [
        migrations.AddField(model_name="recommendationrun", name="lease_token", field=models.UUIDField(blank=True, null=True)),
        migrations.AddField(model_name="recommendationrun", name="lease_expires_at", field=models.DateTimeField(blank=True, null=True)),
        migrations.AddField(model_name="recommendationrun", name="attempt_count", field=models.PositiveIntegerField(default=0)),
        migrations.CreateModel(name="SceneObjectRevision", fields=[
            ("id", models.UUIDField(default=uuid.uuid4, editable=False, primary_key=True, serialize=False)),
            ("placement_id", models.CharField(max_length=80)),
            ("base_version", models.PositiveIntegerField()),
            ("previous_object_id", models.UUIDField()),
            ("category_id", models.CharField(max_length=40)),
            ("bbox", models.JSONField()),
            ("request_id", models.UUIDField(unique=True)),
            ("input_hash", models.CharField(max_length=64)),
            ("created_at", models.DateTimeField(auto_now_add=True)),
            ("composition", models.ForeignKey(on_delete=django.db.models.deletion.PROTECT, to="sketch.composition")),
        ], options={"indexes": [models.Index(fields=["composition", "placement_id", "created_at"], name="scene_revision_lookup")]}),
    ]
