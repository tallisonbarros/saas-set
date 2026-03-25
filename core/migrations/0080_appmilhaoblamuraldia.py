import django.db.models.deletion
import django.utils.timezone
from django.conf import settings
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("core", "0079_remove_unused_moduloacesso_fields"),
    ]

    operations = [
        migrations.CreateModel(
            name="AppMilhaoBlaMuralDia",
            fields=[
                ("id", models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("data_referencia", models.DateField(db_index=True, default=django.utils.timezone.localdate)),
                ("texto", models.TextField()),
                (
                    "visibilidade",
                    models.CharField(
                        choices=[("PUBLICA", "Publica"), ("PRIVADA", "Privada")],
                        default="PUBLICA",
                        max_length=12,
                    ),
                ),
                ("criado_em", models.DateTimeField(auto_now_add=True)),
                ("atualizado_em", models.DateTimeField(auto_now=True)),
                (
                    "autor",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="app_milhao_bla_mural_notas",
                        to=settings.AUTH_USER_MODEL,
                    ),
                ),
            ],
            options={
                "ordering": ["-criado_em", "-id"],
                "indexes": [
                    models.Index(fields=["data_referencia", "criado_em"], name="core_appmil_data_re_660d24_idx"),
                    models.Index(fields=["autor", "data_referencia"], name="core_appmil_autor_i_3e40ca_idx"),
                ],
            },
        ),
    ]
