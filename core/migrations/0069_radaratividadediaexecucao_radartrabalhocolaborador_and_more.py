import django.db.models.deletion
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("core", "0068_radaratividade_datas_status"),
    ]

    operations = [
        migrations.CreateModel(
            name="RadarTrabalhoColaborador",
            fields=[
                (
                    "id",
                    models.AutoField(
                        auto_created=True,
                        primary_key=True,
                        serialize=False,
                        verbose_name="ID",
                    ),
                ),
                ("nome", models.CharField(max_length=120)),
                ("criado_em", models.DateTimeField(auto_now_add=True)),
                (
                    "trabalho",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="colaboradores",
                        to="core.radartrabalho",
                    ),
                ),
            ],
            options={
                "ordering": ["nome", "id"],
            },
        ),
        migrations.CreateModel(
            name="RadarAtividadeDiaExecucao",
            fields=[
                (
                    "id",
                    models.AutoField(
                        auto_created=True,
                        primary_key=True,
                        serialize=False,
                        verbose_name="ID",
                    ),
                ),
                ("data_execucao", models.DateField()),
                ("criado_em", models.DateTimeField(auto_now_add=True)),
                (
                    "atividade",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="dias_execucao",
                        to="core.radaratividade",
                    ),
                ),
            ],
            options={
                "ordering": ["data_execucao", "id"],
            },
        ),
        migrations.AddConstraint(
            model_name="radartrabalhocolaborador",
            constraint=models.UniqueConstraint(
                fields=("trabalho", "nome"),
                name="unique_radar_trabalho_colaborador",
            ),
        ),
        migrations.AddConstraint(
            model_name="radaratividadediaexecucao",
            constraint=models.UniqueConstraint(
                fields=("atividade", "data_execucao"),
                name="unique_radar_atividade_dia_execucao",
            ),
        ),
    ]
