import copy
from django.db import migrations, models


def preserve_initial_placements(apps, schema_editor):
    Composition = apps.get_model('sketch', 'Composition')
    for result in Composition.objects.using(schema_editor.connection.alias).iterator():
        result.initial_placements = copy.deepcopy(result.placements)
        result.save(update_fields=['initial_placements'])


class Migration(migrations.Migration):
    dependencies = [('sketch', '0005_recommendation_lease_object_revision')]
    operations = [
        migrations.AddField(model_name='project', name='request_context', field=models.JSONField(default=dict)),
        migrations.AddField(model_name='composition', name='initial_placements', field=models.JSONField(default=list)),
        migrations.RunPython(preserve_initial_placements, migrations.RunPython.noop),
    ]
