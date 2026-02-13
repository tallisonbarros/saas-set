from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("core", "0067_radaratividade_ordem"),
    ]

    operations = [
        migrations.AddField(
            model_name="radaratividade",
            name="finalizada_em",
            field=models.DateTimeField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name="radaratividade",
            name="inicio_execucao_em",
            field=models.DateTimeField(blank=True, null=True),
        ),
    ]
