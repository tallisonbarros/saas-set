from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ("core", "0055_local_grupo_por_usuario"),
    ]

    operations = [
        migrations.RenameField(
            model_name="canalrackio",
            old_name="nome",
            new_name="descricao",
        ),
    ]
