from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("core", "0045_ingest_record_fields"),
    ]

    operations = [
        migrations.CreateModel(
            name="App",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("slug", models.SlugField(max_length=60, unique=True)),
                ("nome", models.CharField(max_length=120)),
                ("descricao", models.TextField(blank=True)),
                ("ativo", models.BooleanField(default=True)),
                ("criado_em", models.DateTimeField(auto_now_add=True)),
            ],
            options={
                "ordering": ["nome"],
            },
        ),
        migrations.AddField(
            model_name="perfilusuario",
            name="apps",
            field=models.ManyToManyField(blank=True, related_name="usuarios", to="core.app"),
        ),
    ]
