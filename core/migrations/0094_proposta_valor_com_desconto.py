from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("core", "0093_compra_anexo_foto"),
    ]

    operations = [
        migrations.AddField(
            model_name="proposta",
            name="valor_com_desconto",
            field=models.DecimalField(blank=True, decimal_places=2, max_digits=12, null=True),
        ),
    ]
