from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ("core", "0035_tipo_ativo"),
    ]

    operations = [
        migrations.AddField(
            model_name="rackio",
            name="inventario",
            field=models.ForeignKey(
                blank=True,
                null=True,
                on_delete=django.db.models.deletion.SET_NULL,
                related_name="racks_io",
                to="core.inventario",
            ),
        ),
    ]
