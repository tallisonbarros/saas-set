from django.db import migrations, models
from django.db.models import F


def backfill_ultimo_status_evento(apps, schema_editor):
    RadarTrabalho = apps.get_model("core", "RadarTrabalho")
    RadarTrabalho.objects.filter(
        ultimo_status_evento_em__isnull=True,
        status__in=["EXECUTANDO", "FINALIZADA"],
    ).update(ultimo_status_evento_em=F("criado_em"))


class Migration(migrations.Migration):

    dependencies = [
        ("core", "0069_radaratividadediaexecucao_radartrabalhocolaborador_and_more"),
    ]

    operations = [
        migrations.AddField(
            model_name="radartrabalho",
            name="ultimo_status_evento_em",
            field=models.DateTimeField(blank=True, null=True),
        ),
        migrations.RunPython(backfill_ultimo_status_evento, migrations.RunPython.noop),
    ]
