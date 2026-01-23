from django.conf import settings
from django.db import migrations, models
import django.db.models.deletion
import django.utils.timezone


class Migration(migrations.Migration):

    dependencies = [
        ("core", "0041_lista_ip"),
    ]

    operations = [
        migrations.CreateModel(
            name="RadarID",
            fields=[
                ("id", models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("codigo", models.CharField(max_length=40, unique=True)),
            ],
            options={
                "ordering": ["codigo"],
            },
        ),
        migrations.AddField(
            model_name="perfilusuario",
            name="radares",
            field=models.ManyToManyField(blank=True, related_name="usuarios", to="core.radarid"),
        ),
        migrations.CreateModel(
            name="RadarContrato",
            fields=[
                ("id", models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("nome", models.CharField(max_length=120, unique=True)),
            ],
            options={
                "ordering": ["nome"],
            },
        ),
        migrations.CreateModel(
            name="Radar",
            fields=[
                ("id", models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("nome", models.CharField(max_length=120)),
                ("descricao", models.TextField(blank=True)),
                ("local", models.CharField(blank=True, max_length=120)),
                ("criado_em", models.DateTimeField(auto_now_add=True)),
                (
                    "cliente",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="radares",
                        to="core.perfilusuario",
                    ),
                ),
                (
                    "criador",
                    models.ForeignKey(
                        blank=True,
                        null=True,
                        on_delete=django.db.models.deletion.SET_NULL,
                        related_name="radares_criados",
                        to=settings.AUTH_USER_MODEL,
                    ),
                ),
                (
                    "id_radar",
                    models.ForeignKey(
                        blank=True,
                        null=True,
                        on_delete=django.db.models.deletion.SET_NULL,
                        related_name="radares",
                        to="core.radarid",
                    ),
                ),
            ],
            options={
                "ordering": ["nome"],
            },
        ),
        migrations.CreateModel(
            name="RadarTrabalho",
            fields=[
                ("id", models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("nome", models.CharField(max_length=120)),
                ("descricao", models.TextField(blank=True)),
                ("data_registro", models.DateField(default=django.utils.timezone.localdate)),
                (
                    "status",
                    models.CharField(
                        choices=[
                            ("PENDENTE", "Pendente"),
                            ("EXECUTANDO", "Executando"),
                            ("FINALIZADA", "Finalizada"),
                        ],
                        default="PENDENTE",
                        max_length=20,
                    ),
                ),
                ("criado_em", models.DateTimeField(auto_now_add=True)),
                (
                    "radar",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="trabalhos",
                        to="core.radar",
                    ),
                ),
            ],
            options={
                "ordering": ["-data_registro", "nome"],
            },
        ),
        migrations.CreateModel(
            name="RadarAtividade",
            fields=[
                ("id", models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("nome", models.CharField(max_length=120)),
                ("descricao", models.TextField(blank=True)),
                ("setor", models.CharField(blank=True, max_length=120)),
                ("solicitante", models.CharField(blank=True, max_length=120)),
                ("responsavel", models.CharField(blank=True, max_length=120)),
                ("horas_trabalho", models.DecimalField(blank=True, decimal_places=2, max_digits=8, null=True)),
                (
                    "status",
                    models.CharField(
                        choices=[
                            ("PENDENTE", "Pendente"),
                            ("EXECUTANDO", "Executando"),
                            ("FINALIZADA", "Finalizada"),
                        ],
                        default="PENDENTE",
                        max_length=20,
                    ),
                ),
                ("criado_em", models.DateTimeField(auto_now_add=True)),
                (
                    "contrato",
                    models.ForeignKey(
                        blank=True,
                        null=True,
                        on_delete=django.db.models.deletion.SET_NULL,
                        related_name="atividades",
                        to="core.radarcontrato",
                    ),
                ),
                (
                    "trabalho",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="atividades",
                        to="core.radartrabalho",
                    ),
                ),
            ],
            options={
                "ordering": ["-criado_em"],
            },
        ),
    ]
