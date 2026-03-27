import django.db.models.deletion
from django.conf import settings
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("core", "0081_appmilhaoblamuraldialeitura"),
    ]

    operations = [
        migrations.CreateModel(
            name="SystemConfiguration",
            fields=[
                ("id", models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("maintenance_mode_enabled", models.BooleanField(default=False)),
                (
                    "maintenance_message",
                    models.TextField(blank=True, default="O site esta em manutencao. Tente novamente em instantes."),
                ),
                ("updated_at", models.DateTimeField(auto_now=True)),
                (
                    "updated_by",
                    models.ForeignKey(
                        blank=True,
                        null=True,
                        on_delete=django.db.models.deletion.SET_NULL,
                        related_name="system_configuration_updates",
                        to=settings.AUTH_USER_MODEL,
                    ),
                ),
            ],
            options={
                "verbose_name": "Configuracao do sistema",
                "verbose_name_plural": "Configuracoes do sistema",
            },
        ),
    ]
