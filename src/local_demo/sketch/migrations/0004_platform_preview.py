from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):
    dependencies = [("sketch", "0003_composition_version_project_locked")]
    operations = [
        migrations.AddField(model_name="project", name="original_photo", field=models.FileField(blank=True, upload_to="originals/")),
        migrations.AddField(model_name="project", name="original_checksum", field=models.CharField(blank=True, max_length=64)),
        migrations.AddField(model_name="composition", name="generation_context", field=models.JSONField(default=dict)),
        migrations.CreateModel(name="RecommendationRun", fields=[
            ("id", models.UUIDField(editable=False, primary_key=True, serialize=False)),
            ("scene_object_id", models.UUIDField()),
            ("input_version", models.PositiveIntegerField()),
            ("input_hash", models.CharField(max_length=64)),
            ("status", models.CharField(default="SUCCEEDED", max_length=20)),
            ("mode", models.CharField(default="mock", max_length=20)),
            ("items", models.JSONField(default=list)),
            ("metadata", models.JSONField(default=dict)),
            ("created_at", models.DateTimeField(auto_now_add=True)),
            ("composition", models.ForeignKey(on_delete=django.db.models.deletion.PROTECT, to="sketch.composition")),
        ]),
    ]
