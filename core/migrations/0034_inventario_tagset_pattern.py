from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("core", "0033_ativoitem"),
    ]

    operations = [
        migrations.AddField(
            model_name="inventario",
            name="tagset_pattern",
            field=models.CharField(choices=[("TIPO_SEQ", "Tipo + sequencia"), ("SETORIZADO", "Setorizado"), ("INVENTARIO", "Inventario + tipo")], default="TIPO_SEQ", max_length=20),
        ),
    ]

