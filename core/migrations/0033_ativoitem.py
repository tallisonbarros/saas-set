from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ("core", "0032_compraitem_parcela"),
    ]

    operations = [
        migrations.CreateModel(
            name="AtivoItem",
            fields=[
                ("id", models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("nome", models.CharField(max_length=120)),
                ("tipo", models.CharField(blank=True, choices=[("MOTOR", "Motor"), ("VALVULA", "Valvula"), ("EQUIPAMENTO", "Equipamento"), ("CONJUNTO", "Conjunto"), ("TRANSMISSOR_ANALOGICO", "Transmissor analogico"), ("TRANSMISSOR_DIGITAL", "Transmissor digital"), ("SONORO", "Sonoro"), ("VISUAL", "Visual"), ("OUTRO", "Outro")], max_length=80)),
                ("identificacao", models.CharField(blank=True, max_length=120)),
                ("tag_interna", models.CharField(blank=True, max_length=120)),
                ("tag_set", models.CharField(blank=True, max_length=120)),
                ("comissionado", models.BooleanField(default=False)),
                ("comissionado_em", models.DateTimeField(blank=True, null=True)),
                ("em_manutencao", models.BooleanField(default=False)),
                ("manutencao_em", models.DateTimeField(blank=True, null=True)),
                ("criado_em", models.DateTimeField(auto_now_add=True)),
                ("ativo", models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name="itens", to="core.ativo")),
                ("comissionado_por", models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.SET_NULL, related_name="ativo_itens_comissionados", to="auth.user")),
                ("manutencao_por", models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.SET_NULL, related_name="ativo_itens_manutencao", to="auth.user")),
            ],
            options={
                "ordering": ["nome"],
            },
        ),
    ]

