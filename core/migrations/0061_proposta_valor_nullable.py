from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("core", "0060_proposta_finalizada_em"),
    ]

    operations = [
        migrations.AlterField(
            model_name="proposta",
            name="valor",
            field=models.DecimalField(blank=True, decimal_places=2, max_digits=12, null=True),
        ),
    ]
