from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("core", "0063_approtaconfig"),
    ]

    operations = [
        migrations.AddField(
            model_name="proposta",
            name="origem_trabalho",
            field=models.ForeignKey(
                blank=True,
                null=True,
                on_delete=models.SET_NULL,
                related_name="propostas_origem",
                to="core.radartrabalho",
            ),
        ),
    ]
