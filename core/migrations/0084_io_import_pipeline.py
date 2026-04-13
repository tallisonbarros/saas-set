from django.conf import settings
from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ("core", "0083_product_access_foundation"),
    ]

    operations = [
        migrations.CreateModel(
            name="IOImportSettings",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("enabled", models.BooleanField(default=False)),
                ("provider", models.CharField(choices=[("OPENAI", "OpenAI")], default="OPENAI", max_length=20)),
                ("api_key", models.CharField(blank=True, default="", max_length=255)),
                ("api_base_url", models.CharField(blank=True, default="https://api.openai.com/v1", max_length=255)),
                ("model", models.CharField(blank=True, default="gpt-5-mini", max_length=120)),
                ("reasoning_effort", models.CharField(blank=True, default="medium", max_length=20)),
                ("max_rows_for_ai", models.PositiveIntegerField(default=150)),
                ("header_prompt", models.TextField(blank=True, default="")),
                ("grouping_prompt", models.TextField(blank=True, default="")),
                ("updated_at", models.DateTimeField(auto_now=True)),
                (
                    "updated_by",
                    models.ForeignKey(
                        blank=True,
                        null=True,
                        on_delete=django.db.models.deletion.SET_NULL,
                        related_name="io_import_settings_updates",
                        to=settings.AUTH_USER_MODEL,
                    ),
                ),
            ],
            options={
                "verbose_name": "Configuracao de importacao IO",
                "verbose_name_plural": "Configuracoes de importacao IO",
            },
        ),
        migrations.CreateModel(
            name="IOImportJob",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("requested_rack_name", models.CharField(blank=True, default="", max_length=120)),
                ("requested_planta_code", models.CharField(blank=True, default="", max_length=40)),
                (
                    "mode",
                    models.CharField(
                        choices=[("CREATE_RACK", "Criar rack novo"), ("MERGE_RACK", "Preencher rack existente")],
                        default="CREATE_RACK",
                        max_length=20,
                    ),
                ),
                (
                    "status",
                    models.CharField(
                        choices=[
                            ("UPLOADED", "Upload recebido"),
                            ("REVIEW", "Pronto para revisao"),
                            ("APPLIED", "Aplicado"),
                            ("FAILED", "Falhou"),
                        ],
                        default="UPLOADED",
                        max_length=20,
                    ),
                ),
                (
                    "file_format",
                    models.CharField(
                        choices=[("xlsx", "Excel"), ("csv", "CSV"), ("tsv", "TSV"), ("unknown", "Desconhecido")],
                        default="unknown",
                        max_length=20,
                    ),
                ),
                ("source_file", models.FileField(upload_to="io/imports/")),
                ("original_filename", models.CharField(max_length=255)),
                ("file_sha256", models.CharField(blank=True, default="", max_length=64)),
                ("sheet_name", models.CharField(blank=True, default="", max_length=120)),
                ("header_row_index", models.PositiveIntegerField(blank=True, null=True)),
                ("rows_total", models.PositiveIntegerField(default=0)),
                ("rows_parsed", models.PositiveIntegerField(default=0)),
                (
                    "ai_status",
                    models.CharField(
                        choices=[
                            ("SKIPPED", "Nao executado"),
                            ("SUCCESS", "Concluido"),
                            ("FAILED", "Falhou"),
                        ],
                        default="SKIPPED",
                        max_length=20,
                    ),
                ),
                ("ai_model", models.CharField(blank=True, default="", max_length=120)),
                ("ai_error", models.TextField(blank=True, default="")),
                ("column_map", models.JSONField(blank=True, default=dict)),
                ("extracted_payload", models.JSONField(blank=True, default=dict)),
                ("proposal_payload", models.JSONField(blank=True, default=dict)),
                ("ai_payload", models.JSONField(blank=True, default=dict)),
                ("warnings", models.JSONField(blank=True, default=list)),
                ("apply_log", models.JSONField(blank=True, default=dict)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                (
                    "applied_rack",
                    models.ForeignKey(
                        blank=True,
                        null=True,
                        on_delete=django.db.models.deletion.SET_NULL,
                        related_name="import_jobs_applied",
                        to="core.rackio",
                    ),
                ),
                (
                    "cliente",
                    models.ForeignKey(
                        blank=True,
                        null=True,
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="io_import_jobs",
                        to="core.perfilusuario",
                    ),
                ),
                (
                    "created_by",
                    models.ForeignKey(
                        blank=True,
                        null=True,
                        on_delete=django.db.models.deletion.SET_NULL,
                        related_name="io_import_jobs",
                        to=settings.AUTH_USER_MODEL,
                    ),
                ),
                (
                    "requested_grupo",
                    models.ForeignKey(
                        blank=True,
                        null=True,
                        on_delete=django.db.models.deletion.SET_NULL,
                        related_name="io_import_jobs",
                        to="core.gruporackio",
                    ),
                ),
                (
                    "requested_inventario",
                    models.ForeignKey(
                        blank=True,
                        null=True,
                        on_delete=django.db.models.deletion.SET_NULL,
                        related_name="io_import_jobs",
                        to="core.inventario",
                    ),
                ),
                (
                    "requested_local",
                    models.ForeignKey(
                        blank=True,
                        null=True,
                        on_delete=django.db.models.deletion.SET_NULL,
                        related_name="io_import_jobs",
                        to="core.localrackio",
                    ),
                ),
                (
                    "target_rack",
                    models.ForeignKey(
                        blank=True,
                        null=True,
                        on_delete=django.db.models.deletion.SET_NULL,
                        related_name="import_jobs_target",
                        to="core.rackio",
                    ),
                ),
            ],
            options={
                "ordering": ["-created_at"],
            },
        ),
        migrations.AddIndex(
            model_name="ioimportjob",
            index=models.Index(fields=["status", "created_at"], name="core_ioimpo_status_81e4d0_idx"),
        ),
        migrations.AddIndex(
            model_name="ioimportjob",
            index=models.Index(fields=["ai_status", "created_at"], name="core_ioimpo_ai_stat_40d0a0_idx"),
        ),
    ]
