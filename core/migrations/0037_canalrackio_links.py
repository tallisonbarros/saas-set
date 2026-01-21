from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ("core", "0036_rackio_inventario"),
    ]

    operations = [
        migrations.AddField(
            model_name="canalrackio",
            name="comissionado",
            field=models.BooleanField(default=False),
        ),
        migrations.AddField(
            model_name="canalrackio",
            name="ativo",
            field=models.ForeignKey(
                blank=True,
                null=True,
                on_delete=django.db.models.deletion.SET_NULL,
                related_name="canais_io",
                to="core.ativo",
            ),
        ),
        migrations.AddField(
            model_name="canalrackio",
            name="ativo_item",
            field=models.ForeignKey(
                blank=True,
                null=True,
                on_delete=django.db.models.deletion.SET_NULL,
                related_name="canais_io",
                to="core.ativoitem",
            ),
        ),
    ]
