from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ("core", "0042_radar_atividades"),
    ]

    operations = [
        migrations.CreateModel(
            name="RadarClassificacao",
            fields=[
                ("id", models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("nome", models.CharField(max_length=120, unique=True)),
            ],
            options={
                "ordering": ["nome"],
            },
        ),
        migrations.AddField(
            model_name="radartrabalho",
            name="classificacao",
            field=models.ForeignKey(
                blank=True,
                null=True,
                on_delete=django.db.models.deletion.SET_NULL,
                related_name="trabalhos",
                to="core.radarclassificacao",
            ),
        ),
        migrations.AddField(
            model_name="radaratividade",
            name="classificacao",
            field=models.ForeignKey(
                blank=True,
                null=True,
                on_delete=django.db.models.deletion.SET_NULL,
                related_name="atividades",
                to="core.radarclassificacao",
            ),
        ),
    ]
