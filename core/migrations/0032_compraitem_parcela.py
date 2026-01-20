from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("core", "0031_inventario_models"),
    ]

    operations = [
        migrations.AddField(
            model_name="compraitem",
            name="parcela",
            field=models.CharField(default="1/1", max_length=15),
        ),
    ]
