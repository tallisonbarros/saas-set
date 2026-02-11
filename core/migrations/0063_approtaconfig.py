from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ("core", "0062_approtasmap_alter_ingestrecord_options_and_more"),
    ]

    operations = [
        migrations.CreateModel(
            name="AppRotaConfig",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("prefixo", models.CharField(max_length=80)),
                ("nome_exibicao", models.CharField(blank=True, default="", max_length=120)),
                ("ordem", models.IntegerField(default=0)),
                ("ativo", models.BooleanField(default=True)),
                ("criado_em", models.DateTimeField(auto_now_add=True)),
                ("atualizado_em", models.DateTimeField(auto_now=True)),
                (
                    "app",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="rotas_configs",
                        to="core.app",
                    ),
                ),
            ],
            options={
                "ordering": ["app_id", "ordem", "prefixo"],
            },
        ),
        migrations.AddConstraint(
            model_name="approtaconfig",
            constraint=models.UniqueConstraint(fields=("app", "prefixo"), name="unique_app_rota_config"),
        ),
    ]

