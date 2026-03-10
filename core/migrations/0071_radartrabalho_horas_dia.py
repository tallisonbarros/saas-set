from decimal import Decimal

from django.db import migrations, models
from django.db.models import Count


def recalculate_horas_trabalho_from_agenda(apps, schema_editor):
    RadarAtividade = apps.get_model("core", "RadarAtividade")
    RadarAtividadeDiaExecucao = apps.get_model("core", "RadarAtividadeDiaExecucao")

    days_by_atividade = dict(
        RadarAtividadeDiaExecucao.objects.values("atividade_id")
        .annotate(total=Count("id"))
        .values_list("atividade_id", "total")
    )

    atividades = list(RadarAtividade.objects.select_related("trabalho").all())
    changed = []
    for atividade in atividades:
        horas_dia = getattr(atividade.trabalho, "horas_dia", None)
        if horas_dia is None:
            horas_dia = Decimal("8.00")
        total_dias = days_by_atividade.get(atividade.id, 0)
        horas_calculadas = horas_dia * Decimal(total_dias)
        if atividade.horas_trabalho != horas_calculadas:
            atividade.horas_trabalho = horas_calculadas
            changed.append(atividade)
    if changed:
        RadarAtividade.objects.bulk_update(changed, ["horas_trabalho"])


class Migration(migrations.Migration):

    dependencies = [
        ("core", "0070_radartrabalho_ultimo_status_evento_em"),
    ]

    operations = [
        migrations.AddField(
            model_name="radartrabalho",
            name="horas_dia",
            field=models.DecimalField(decimal_places=2, default=Decimal("8.00"), max_digits=5),
        ),
        migrations.RunPython(recalculate_horas_trabalho_from_agenda, migrations.RunPython.noop),
    ]
