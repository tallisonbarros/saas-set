from django.conf import settings
from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ("core", "0030_proposta_anexo"),
    ]

    operations = [
        migrations.CreateModel(
            name="InventarioID",
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
            name="inventarios",
            field=models.ManyToManyField(blank=True, related_name="usuarios", to="core.inventarioid"),
        ),
        migrations.CreateModel(
            name="Inventario",
            fields=[
                ("id", models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("nome", models.CharField(max_length=120)),
                ("descricao", models.TextField(blank=True)),
                ("responsavel", models.CharField(blank=True, max_length=120)),
                ("cidade", models.CharField(blank=True, max_length=80)),
                ("estado", models.CharField(blank=True, max_length=80)),
                ("pais", models.CharField(blank=True, max_length=80)),
                ("criado_em", models.DateTimeField(auto_now_add=True)),
                (
                    "cliente",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="inventarios_cliente",
                        to="core.perfilusuario",
                    ),
                ),
                (
                    "criador",
                    models.ForeignKey(
                        blank=True,
                        null=True,
                        on_delete=django.db.models.deletion.SET_NULL,
                        related_name="inventarios_criados",
                        to=settings.AUTH_USER_MODEL,
                    ),
                ),
                (
                    "id_inventario",
                    models.ForeignKey(
                        blank=True,
                        null=True,
                        on_delete=django.db.models.deletion.SET_NULL,
                        related_name="inventarios",
                        to="core.inventarioid",
                    ),
                ),
            ],
            options={
                "ordering": ["nome"],
            },
        ),
        migrations.CreateModel(
            name="Ativo",
            fields=[
                ("id", models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("setor", models.CharField(blank=True, max_length=120)),
                ("nome", models.CharField(max_length=120)),
                ("tipo", models.CharField(blank=True, max_length=80)),
                ("identificacao", models.CharField(blank=True, max_length=120)),
                ("tag_interna", models.CharField(blank=True, max_length=120)),
                ("tag_set", models.CharField(blank=True, max_length=120)),
                ("comissionado", models.BooleanField(default=False)),
                ("comissionado_em", models.DateTimeField(blank=True, null=True)),
                ("em_manutencao", models.BooleanField(default=False)),
                ("manutencao_em", models.DateTimeField(blank=True, null=True)),
                ("criado_em", models.DateTimeField(auto_now_add=True)),
                (
                    "comissionado_por",
                    models.ForeignKey(
                        blank=True,
                        null=True,
                        on_delete=django.db.models.deletion.SET_NULL,
                        related_name="ativos_comissionados",
                        to=settings.AUTH_USER_MODEL,
                    ),
                ),
                (
                    "inventario",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="ativos",
                        to="core.inventario",
                    ),
                ),
                (
                    "manutencao_por",
                    models.ForeignKey(
                        blank=True,
                        null=True,
                        on_delete=django.db.models.deletion.SET_NULL,
                        related_name="ativos_manutencao",
                        to=settings.AUTH_USER_MODEL,
                    ),
                ),
                (
                    "pai",
                    models.ForeignKey(
                        blank=True,
                        null=True,
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="subativos",
                        to="core.ativo",
                    ),
                ),
            ],
            options={
                "ordering": ["nome"],
            },
        ),
    ]

