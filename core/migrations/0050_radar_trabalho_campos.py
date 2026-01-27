from django.db import migrations, models
import django.db.models.deletion


def copy_atividade_fields_to_trabalho(apps, schema_editor):
    RadarTrabalho = apps.get_model("core", "RadarTrabalho")
    RadarAtividade = apps.get_model("core", "RadarAtividade")

    trabalhos = RadarTrabalho.objects.all()
    for trabalho in trabalhos.iterator():
        primeira = (
            RadarAtividade.objects.filter(trabalho_id=trabalho.id)
            .order_by("criado_em", "id")
            .first()
        )
        if not primeira:
            continue
        updated = False
        if not trabalho.setor and primeira.setor:
            trabalho.setor = primeira.setor
            updated = True
        if not trabalho.solicitante and primeira.solicitante:
            trabalho.solicitante = primeira.solicitante
            updated = True
        if not trabalho.responsavel and primeira.responsavel:
            trabalho.responsavel = primeira.responsavel
            updated = True
        if not trabalho.contrato_id and primeira.contrato_id:
            trabalho.contrato_id = primeira.contrato_id
            updated = True
        if updated:
            trabalho.save(update_fields=["setor", "solicitante", "responsavel", "contrato"])


class Migration(migrations.Migration):

    dependencies = [
        ("core", "0049_app_logo"),
    ]

    operations = [
        migrations.AddField(
            model_name="radartrabalho",
            name="contrato",
            field=models.ForeignKey(
                blank=True,
                null=True,
                on_delete=django.db.models.deletion.SET_NULL,
                related_name="trabalhos",
                to="core.radarcontrato",
            ),
        ),
        migrations.AddField(
            model_name="radartrabalho",
            name="responsavel",
            field=models.CharField(blank=True, max_length=120),
        ),
        migrations.AddField(
            model_name="radartrabalho",
            name="setor",
            field=models.CharField(blank=True, max_length=120),
        ),
        migrations.AddField(
            model_name="radartrabalho",
            name="solicitante",
            field=models.CharField(blank=True, max_length=120),
        ),
        migrations.RunPython(copy_atividade_fields_to_trabalho, migrations.RunPython.noop),
        migrations.RemoveField(
            model_name="radaratividade",
            name="classificacao",
        ),
        migrations.RemoveField(
            model_name="radaratividade",
            name="contrato",
        ),
        migrations.RemoveField(
            model_name="radaratividade",
            name="responsavel",
        ),
        migrations.RemoveField(
            model_name="radaratividade",
            name="setor",
        ),
        migrations.RemoveField(
            model_name="radaratividade",
            name="solicitante",
        ),
    ]
