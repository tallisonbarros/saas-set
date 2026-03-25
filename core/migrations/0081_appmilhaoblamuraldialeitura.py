import django.db.models.deletion
import django.utils.timezone
from django.conf import settings
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("core", "0080_appmilhaoblamuraldia"),
    ]

    operations = [
        migrations.CreateModel(
            name="AppMilhaoBlaMuralDiaLeitura",
            fields=[
                ("id", models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("data_referencia", models.DateField(db_index=True)),
                ("visualizado_em", models.DateTimeField(default=django.utils.timezone.now)),
                (
                    "usuario",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="app_milhao_bla_mural_leituras",
                        to=settings.AUTH_USER_MODEL,
                    ),
                ),
            ],
            options={
                "ordering": ["-visualizado_em", "-id"],
                "indexes": [
                    models.Index(fields=["usuario", "data_referencia"], name="core_appmil_usuario_3f9f2d_idx"),
                ],
                "constraints": [
                    models.UniqueConstraint(
                        fields=("usuario", "data_referencia"),
                        name="unique_app_milhao_bla_mural_leitura_usuario_data",
                    ),
                ],
            },
        ),
    ]
