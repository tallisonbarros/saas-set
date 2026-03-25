from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ("core", "0077_access_control_shadow_log"),
    ]

    operations = [
        migrations.RemoveField(
            model_name="moduloacesso",
            name="auth_mode",
        ),
        migrations.RemoveField(
            model_name="moduloacesso",
            name="somente_dev",
        ),
        migrations.RemoveField(
            model_name="moduloacesso",
            name="mantem_escopo_ids",
        ),
        migrations.DeleteModel(
            name="AccessControlShadowLog",
        ),
    ]
