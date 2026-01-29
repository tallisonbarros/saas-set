from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("core", "0041_lista_ip"),
    ]

    operations = [
        migrations.AddField(
            model_name="listaipitem",
            name="descricao",
            field=models.CharField(blank=True, max_length=200),
        ),
    ]
