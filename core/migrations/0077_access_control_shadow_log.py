import django.db.models.deletion
from django.conf import settings
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("core", "0076_access_control_foundation"),
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
    ]

    operations = [
        migrations.CreateModel(
            name="AccessControlShadowLog",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("request_path", models.CharField(blank=True, default="", max_length=220)),
                ("response_status", models.PositiveSmallIntegerField(default=200)),
                ("legacy_allowed", models.BooleanField(default=False)),
                ("candidate_allowed", models.BooleanField(default=False)),
                ("auth_mode", models.CharField(blank=True, default="", max_length=12)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                (
                    "modulo",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="shadow_logs",
                        to="core.moduloacesso",
                    ),
                ),
                (
                    "user",
                    models.ForeignKey(
                        blank=True,
                        null=True,
                        on_delete=django.db.models.deletion.SET_NULL,
                        related_name="access_control_shadow_logs",
                        to=settings.AUTH_USER_MODEL,
                    ),
                ),
            ],
            options={"ordering": ["-created_at"]},
        ),
        migrations.AddIndex(
            model_name="accesscontrolshadowlog",
            index=models.Index(fields=["modulo", "created_at"], name="core_access_modulo__8d922a_idx"),
        ),
        migrations.AddIndex(
            model_name="accesscontrolshadowlog",
            index=models.Index(fields=["user", "created_at"], name="core_access_user_id_60440e_idx"),
        ),
    ]
