from django.db import migrations, models
from django.utils import timezone


def fill_created_at(apps, schema_editor):
    IngestRecord = apps.get_model("core", "IngestRecord")
    IngestRecord.objects.filter(created_at__isnull=True).update(created_at=timezone.now())


class Migration(migrations.Migration):
    dependencies = [
        ("core", "0045_ingest_record_fields"),
    ]

    operations = [
        migrations.RunPython(fill_created_at, migrations.RunPython.noop),
        migrations.AlterField(
            model_name="ingestrecord",
            name="created_at",
            field=models.DateTimeField(auto_now_add=True),
        ),
    ]
