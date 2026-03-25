from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ("core", "0078_cleanup_access_control_transition"),
    ]

    operations = [
        migrations.RemoveField(
            model_name="moduloacesso",
            name="app",
        ),
        migrations.RemoveField(
            model_name="moduloacesso",
            name="oid",
        ),
        migrations.RemoveField(
            model_name="moduloacesso",
            name="rota_base",
        ),
        migrations.RemoveField(
            model_name="moduloacesso",
            name="sistema",
        ),
    ]
