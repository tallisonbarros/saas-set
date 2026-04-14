from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("core", "0088_ioimportjob_progress_payload"),
    ]

    operations = [
        migrations.CreateModel(
            name="IOImportAICache",
            fields=[
                ("id", models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("stage", models.CharField(choices=[("WORKBOOK", "Analise do workbook"), ("SHEET", "Analise da guia")], max_length=20)),
                ("fingerprint", models.CharField(max_length=64)),
                ("file_sha256", models.CharField(blank=True, default="", max_length=64)),
                ("sheet_name", models.CharField(blank=True, default="", max_length=120)),
                ("provider", models.CharField(blank=True, default="", max_length=20)),
                ("model", models.CharField(blank=True, default="", max_length=120)),
                ("settings_fingerprint", models.CharField(blank=True, default="", max_length=64)),
                ("response_payload", models.JSONField(blank=True, default=dict)),
                ("payload_meta", models.JSONField(blank=True, default=dict)),
                ("hits", models.PositiveIntegerField(default=0)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                ("last_used_at", models.DateTimeField(auto_now=True)),
            ],
            options={
                "ordering": ["-updated_at"],
            },
        ),
        migrations.AddIndex(
            model_name="ioimportaicache",
            index=models.Index(fields=["stage", "file_sha256", "updated_at"], name="core_ioimpo_stage_5a6484_idx"),
        ),
        migrations.AddIndex(
            model_name="ioimportaicache",
            index=models.Index(fields=["last_used_at"], name="core_ioimpo_last_us_8a5d4a_idx"),
        ),
        migrations.AddConstraint(
            model_name="ioimportaicache",
            constraint=models.UniqueConstraint(fields=("stage", "fingerprint"), name="unique_io_import_ai_cache_stage_fingerprint"),
        ),
    ]
