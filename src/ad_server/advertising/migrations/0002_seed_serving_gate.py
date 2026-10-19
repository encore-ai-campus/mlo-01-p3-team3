from django.db import migrations


def seed_gate(apps,schema_editor):
    apps.get_model('advertising','ServingGate').objects.using(schema_editor.connection.alias).get_or_create(pk=1)


class Migration(migrations.Migration):
    dependencies=[('advertising','0001_initial')]
    operations=[migrations.RunPython(seed_gate,migrations.RunPython.noop)]
