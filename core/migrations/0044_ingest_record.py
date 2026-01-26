from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("core", "0043_radar_classificacao"),
    ]

    operations = [
        migrations.CreateModel(
            name="IngestRecord",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("source_id", models.CharField(max_length=120, unique=True)),
                ("payload", models.JSONField()),
                ("recebido_em", models.DateTimeField(auto_now_add=True)),
            ],
            options={
                "ordering": ["-recebido_em"],
            },
        ),
    ]
