from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("core", "0039_localrackio_model"),
    ]

    operations = [
        migrations.CreateModel(
            name="GrupoRackIO",
            fields=[
                ("id", models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("nome", models.CharField(max_length=120, unique=True)),
            ],
            options={"ordering": ["nome"]},
        ),
        migrations.AddField(
            model_name="rackio",
            name="grupo",
            field=models.ForeignKey(
                blank=True,
                null=True,
                on_delete=models.SET_NULL,
                related_name="racks",
                to="core.gruporackio",
            ),
        ),
    ]
