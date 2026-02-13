from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("core", "0065_radartrabalho_criado_por"),
    ]

    operations = [
        migrations.RenameField(
            model_name="proposta",
            old_name="origem_trabalho",
            new_name="trabalho",
        ),
        migrations.AlterField(
            model_name="proposta",
            name="trabalho",
            field=models.ForeignKey(
                blank=True,
                null=True,
                on_delete=models.SET_NULL,
                related_name="propostas_vinculadas",
                to="core.radartrabalho",
            ),
        ),
    ]
