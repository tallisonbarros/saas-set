from decimal import Decimal

from django.db import migrations
from django.db.models import Count


def recalculate_horas_with_colaboradores(apps, schema_editor):
    RadarAtividade = apps.get_model("core", "RadarAtividade")
    RadarAtividadeDiaExecucao = apps.get_model("core", "RadarAtividadeDiaExecucao")
    RadarTrabalhoColaborador = apps.get_model("core", "RadarTrabalhoColaborador")

    days_by_atividade = dict(
        RadarAtividadeDiaExecucao.objects.values("atividade_id")
        .annotate(total=Count("id"))
        .values_list("atividade_id", "total")
    )
    colaboradores_by_trabalho = dict(
        RadarTrabalhoColaborador.objects.values("trabalho_id")
        .annotate(total=Count("id"))
        .values_list("trabalho_id", "total")
    )

    changed = []
    for atividade in RadarAtividade.objects.select_related("trabalho").all():
        horas_dia = getattr(atividade.trabalho, "horas_dia", None) or Decimal("8.00")
        total_dias = days_by_atividade.get(atividade.id, 0)
        total_colaboradores = colaboradores_by_trabalho.get(atividade.trabalho_id, 0)
        if total_colaboradores <= 0:
            total_colaboradores = 1
        horas_calculadas = horas_dia * Decimal(total_dias) * Decimal(total_colaboradores)
        if atividade.horas_trabalho != horas_calculadas:
            atividade.horas_trabalho = horas_calculadas
            changed.append(atividade)
    if changed:
        RadarAtividade.objects.bulk_update(changed, ["horas_trabalho"])


class Migration(migrations.Migration):

    dependencies = [
        ("core", "0071_radartrabalho_horas_dia"),
    ]

    operations = [
        migrations.RunPython(recalculate_horas_with_colaboradores, migrations.RunPython.noop),
    ]
