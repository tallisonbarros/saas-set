from django.db import migrations, models


def preencher_ordem_atividades(apps, schema_editor):
    RadarTrabalho = apps.get_model("core", "RadarTrabalho")
    RadarAtividade = apps.get_model("core", "RadarAtividade")

    for trabalho in RadarTrabalho.objects.all().iterator():
        for status in ["EXECUTANDO", "PENDENTE", "FINALIZADA"]:
            atividades = list(
                RadarAtividade.objects.filter(trabalho_id=trabalho.id, status=status).order_by("criado_em", "id")
            )
            for ordem, atividade in enumerate(atividades, start=1):
                atividade.ordem = ordem
            if atividades:
                RadarAtividade.objects.bulk_update(atividades, ["ordem"])


class Migration(migrations.Migration):

    dependencies = [
        ("core", "0066_proposta_trabalho_vinculo"),
    ]

    operations = [
        migrations.AddField(
            model_name="radaratividade",
            name="ordem",
            field=models.PositiveIntegerField(db_index=True, default=0),
        ),
        migrations.AlterModelOptions(
            name="radaratividade",
            options={"ordering": ["ordem", "criado_em", "id"]},
        ),
        migrations.RunPython(preencher_ordem_atividades, migrations.RunPython.noop),
    ]
