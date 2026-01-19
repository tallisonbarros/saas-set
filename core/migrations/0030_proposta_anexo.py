from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("core", "0029_proposta_andamento"),
    ]

    operations = [
        migrations.CreateModel(
            name="PropostaAnexo",
            fields=[
                ("id", models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                (
                    "arquivo",
                    models.FileField(upload_to="propostas/anexos/"),
                ),
                (
                    "tipo",
                    models.CharField(
                        choices=[
                            ("NF", "NF"),
                            ("CONTRATO", "Contrato"),
                            ("PROPOSTA_FORMAL", "Proposta formal"),
                            ("QUEBRA_CONTRATO", "Quebra de contrato"),
                            ("PEDIDO_COMPRA", "Pedido de compra"),
                            ("ORDEM_SERVICO", "Ordem de servico"),
                            ("OUTROS", "Outros"),
                        ],
                        default="OUTROS",
                        max_length=30,
                    ),
                ),
                ("criado_em", models.DateTimeField(auto_now_add=True)),
                (
                    "proposta",
                    models.ForeignKey(on_delete=models.deletion.CASCADE, related_name="anexos", to="core.proposta"),
                ),
            ],
            options={
                "ordering": ["-criado_em"],
            },
        ),
    ]
