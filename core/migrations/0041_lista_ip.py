from django.db import migrations, models
import django.db.models.deletion
import django.core.validators


class Migration(migrations.Migration):
    dependencies = [
        ("core", "0040_grouprackio_model"),
    ]

    operations = [
        migrations.CreateModel(
            name="ListaIPID",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("codigo", models.CharField(max_length=40, unique=True)),
            ],
            options={
                "ordering": ["codigo"],
            },
        ),
        migrations.CreateModel(
            name="ListaIP",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("nome", models.CharField(max_length=120)),
                ("descricao", models.TextField(blank=True)),
                ("faixa_inicio", models.GenericIPAddressField()),
                ("faixa_fim", models.GenericIPAddressField()),
                ("protocolo_padrao", models.CharField(blank=True, max_length=30)),
                ("criado_em", models.DateTimeField(auto_now_add=True)),
                (
                    "cliente",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="listas_ip_listas",
                        to="core.perfilusuario",
                    ),
                ),
                (
                    "id_listaip",
                    models.ForeignKey(
                        blank=True,
                        null=True,
                        on_delete=django.db.models.deletion.SET_NULL,
                        related_name="listas",
                        to="core.listaipid",
                    ),
                ),
            ],
            options={
                "ordering": ["nome"],
            },
        ),
        migrations.CreateModel(
            name="ListaIPItem",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("ip", models.GenericIPAddressField()),
                ("nome_equipamento", models.CharField(blank=True, max_length=120)),
                (
                    "mac",
                    models.CharField(
                        blank=True,
                        max_length=30,
                        validators=[
                            django.core.validators.RegexValidator(
                                message="MAC deve estar no formato 00:11:22:33:44:55.",
                                regex="^[0-9A-Fa-f]{2}([:-]?[0-9A-Fa-f]{2}){5}$",
                            )
                        ],
                    ),
                ),
                ("protocolo", models.CharField(blank=True, max_length=30)),
                (
                    "lista",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="ips",
                        to="core.listaip",
                    ),
                ),
            ],
            options={
                "ordering": ["ip"],
            },
        ),
        migrations.AddConstraint(
            model_name="listaipitem",
            constraint=models.UniqueConstraint(fields=("lista", "ip"), name="unique_lista_ip"),
        ),
        migrations.AddField(
            model_name="perfilusuario",
            name="listas_ip",
            field=models.ManyToManyField(blank=True, related_name="usuarios", to="core.listaipid"),
        ),
    ]
