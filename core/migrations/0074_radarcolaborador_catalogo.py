import django.db.models.deletion
from django.db import migrations, models


def migrate_legacy_trabalho_colaboradores(apps, schema_editor):
    RadarColaborador = apps.get_model("core", "RadarColaborador")
    RadarTrabalhoColaborador = apps.get_model("core", "RadarTrabalhoColaborador")

    by_perfil_nome = {}
    for colaborador in RadarColaborador.objects.all().iterator():
        perfil_map = by_perfil_nome.setdefault(colaborador.perfil_id, {})
        key = (colaborador.nome or "").strip().casefold()
        if key and key not in perfil_map:
            perfil_map[key] = colaborador.id

    changed = []
    for row in (
        RadarTrabalhoColaborador.objects.select_related("trabalho__radar")
        .order_by("id")
        .iterator()
    ):
        if row.colaborador_id:
            continue
        nome = " ".join((row.nome or "").strip().split())[:120]
        if not nome:
            continue
        trabalho = getattr(row, "trabalho", None)
        radar = getattr(trabalho, "radar", None)
        perfil_id = getattr(radar, "cliente_id", None)
        if not perfil_id:
            continue
        key = nome.casefold()
        perfil_map = by_perfil_nome.setdefault(perfil_id, {})
        colaborador_id = perfil_map.get(key)
        if not colaborador_id:
            colaborador = RadarColaborador.objects.create(
                perfil_id=perfil_id,
                nome=nome,
                cargo="",
                ativo=True,
            )
            colaborador_id = colaborador.id
            perfil_map[key] = colaborador_id
        row.colaborador_id = colaborador_id
        changed.append(row)

    if changed:
        RadarTrabalhoColaborador.objects.bulk_update(changed, ["colaborador"])


class Migration(migrations.Migration):

    dependencies = [
        ("core", "0073_radartrabalhoobservacao"),
    ]

    operations = [
        migrations.CreateModel(
            name="RadarColaborador",
            fields=[
                ("id", models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("nome", models.CharField(max_length=120)),
                ("cargo", models.CharField(blank=True, max_length=120)),
                ("atributos", models.JSONField(blank=True, default=dict)),
                ("ativo", models.BooleanField(default=True)),
                ("criado_em", models.DateTimeField(auto_now_add=True)),
                ("atualizado_em", models.DateTimeField(auto_now=True)),
                (
                    "perfil",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="radar_colaboradores",
                        to="core.perfilusuario",
                    ),
                ),
            ],
            options={
                "ordering": ["nome", "id"],
            },
        ),
        migrations.AddConstraint(
            model_name="radarcolaborador",
            constraint=models.UniqueConstraint(
                fields=("perfil", "nome"),
                name="unique_radar_colaborador_por_perfil_nome",
            ),
        ),
        migrations.AddField(
            model_name="radartrabalhocolaborador",
            name="colaborador",
            field=models.ForeignKey(
                blank=True,
                null=True,
                on_delete=django.db.models.deletion.SET_NULL,
                related_name="trabalhos_vinculados",
                to="core.radarcolaborador",
            ),
        ),
        migrations.RunPython(migrate_legacy_trabalho_colaboradores, migrations.RunPython.noop),
    ]
