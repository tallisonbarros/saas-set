import django.db.models.deletion
from decimal import Decimal
from django.db import migrations, models


def backfill_atividade_colaboradores(apps, schema_editor):
    RadarAtividade = apps.get_model("core", "RadarAtividade")
    RadarAtividadeColaborador = apps.get_model("core", "RadarAtividadeColaborador")
    RadarAtividadeDiaExecucao = apps.get_model("core", "RadarAtividadeDiaExecucao")
    RadarTrabalhoColaborador = apps.get_model("core", "RadarTrabalhoColaborador")

    trabalho_rows_map = {}
    for row in RadarTrabalhoColaborador.objects.order_by("trabalho_id", "nome", "id").iterator():
        trabalho_rows_map.setdefault(row.trabalho_id, []).append(row)

    to_create = []
    for atividade in RadarAtividade.objects.order_by("id").iterator():
        seen = set()
        for row in trabalho_rows_map.get(atividade.trabalho_id, []):
            nome = " ".join((row.nome or "").strip().split())[:120]
            if not nome:
                continue
            key = f"id:{row.colaborador_id}" if row.colaborador_id else f"nome:{nome.casefold()}"
            if key in seen:
                continue
            seen.add(key)
            to_create.append(
                RadarAtividadeColaborador(
                    atividade_id=atividade.id,
                    colaborador_id=row.colaborador_id,
                    nome=nome,
                )
            )
        if len(to_create) >= 1000:
            RadarAtividadeColaborador.objects.bulk_create(to_create, ignore_conflicts=True)
            to_create = []

    if to_create:
        RadarAtividadeColaborador.objects.bulk_create(to_create, ignore_conflicts=True)

    days_by_atividade = dict(
        RadarAtividadeDiaExecucao.objects.values("atividade_id")
        .annotate(total=models.Count("id"))
        .values_list("atividade_id", "total")
    )
    colaboradores_by_atividade = dict(
        RadarAtividadeColaborador.objects.values("atividade_id")
        .annotate(total=models.Count("id"))
        .values_list("atividade_id", "total")
    )

    changed = []
    for atividade in RadarAtividade.objects.select_related("trabalho").all():
        horas_dia = getattr(atividade.trabalho, "horas_dia", None) or Decimal("8.00")
        total_dias = days_by_atividade.get(atividade.id, 0)
        total_colaboradores = colaboradores_by_atividade.get(atividade.id, 0)
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
        ("core", "0074_radarcolaborador_catalogo"),
    ]

    operations = [
        migrations.CreateModel(
            name="RadarAtividadeColaborador",
            fields=[
                ("id", models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("nome", models.CharField(max_length=120)),
                ("criado_em", models.DateTimeField(auto_now_add=True)),
                (
                    "atividade",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="colaboradores",
                        to="core.radaratividade",
                    ),
                ),
                (
                    "colaborador",
                    models.ForeignKey(
                        blank=True,
                        null=True,
                        on_delete=django.db.models.deletion.SET_NULL,
                        related_name="atividades_vinculadas",
                        to="core.radarcolaborador",
                    ),
                ),
            ],
            options={
                "ordering": ["nome", "id"],
            },
        ),
        migrations.AddConstraint(
            model_name="radaratividadecolaborador",
            constraint=models.UniqueConstraint(
                fields=("atividade", "nome"),
                name="unique_radar_atividade_colaborador",
            ),
        ),
        migrations.RunPython(backfill_atividade_colaboradores, migrations.RunPython.noop),
    ]
