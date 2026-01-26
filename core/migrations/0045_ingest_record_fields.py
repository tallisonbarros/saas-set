from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("core", "0044_ingest_record"),
    ]

    operations = [
        migrations.AddField(
            model_name="ingestrecord",
            name="agent_id",
            field=models.CharField(blank=True, max_length=120),
        ),
        migrations.AddField(
            model_name="ingestrecord",
            name="client_id",
            field=models.CharField(blank=True, max_length=120),
        ),
        migrations.AddField(
            model_name="ingestrecord",
            name="source",
            field=models.CharField(blank=True, max_length=120),
        ),
        migrations.AddField(
            model_name="ingestrecord",
            name="created_at",
            field=models.DateTimeField(auto_now_add=True, null=True),
        ),
        migrations.RemoveField(
            model_name="ingestrecord",
            name="recebido_em",
        ),
    ]
