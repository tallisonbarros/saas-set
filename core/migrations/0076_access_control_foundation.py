import re

import django.db.models.deletion
from django.db import migrations, models


SYSTEM_TYPE_CODES = {
    "MASTER": "MASTER",
    "DEV": "DEV",
    "CONTRATANTE": "CONTRATANTE",
    "CLIENTE": "CLIENTE",
    "FINANCEIRO": "FINANCEIRO",
    "VENDEDOR": "VENDEDOR",
}

SYSTEM_TYPE_NAMES = (
    ("MASTER", "MASTER"),
    ("DEV", "Dev"),
    ("CONTRATANTE", "Contratante"),
    ("CLIENTE", "Cliente"),
    ("FINANCEIRO", "Financeiro"),
    ("VENDEDOR", "Vendedor"),
)

MODULE_DEFINITIONS = (
    {
        "codigo": "PROPOSTAS",
        "nome": "Propostas",
        "oid": "",
        "tipo": "CORE",
        "rota_base": "propostas",
        "auth_mode": "LEGACY",
        "somente_dev": False,
        "mantem_escopo_ids": False,
        "ativo": True,
        "sistema": True,
        "app_slug": "",
    },
    {
        "codigo": "FINANCEIRO",
        "nome": "Financeiro",
        "oid": "",
        "tipo": "CORE",
        "rota_base": "financeiro",
        "auth_mode": "LEGACY",
        "somente_dev": False,
        "mantem_escopo_ids": True,
        "ativo": True,
        "sistema": True,
        "app_slug": "",
    },
    {
        "codigo": "IOS",
        "nome": "Listas de IOs",
        "oid": "",
        "tipo": "CORE",
        "rota_base": "ios",
        "auth_mode": "LEGACY",
        "somente_dev": False,
        "mantem_escopo_ids": True,
        "ativo": True,
        "sistema": True,
        "app_slug": "",
    },
    {
        "codigo": "INVENTARIO",
        "nome": "Inventario",
        "oid": "",
        "tipo": "CORE",
        "rota_base": "inventarios",
        "auth_mode": "LEGACY",
        "somente_dev": False,
        "mantem_escopo_ids": True,
        "ativo": True,
        "sistema": True,
        "app_slug": "",
    },
    {
        "codigo": "LISTA_IP",
        "nome": "Listas de IPs",
        "oid": "",
        "tipo": "CORE",
        "rota_base": "listas-ip",
        "auth_mode": "LEGACY",
        "somente_dev": False,
        "mantem_escopo_ids": True,
        "ativo": True,
        "sistema": True,
        "app_slug": "",
    },
    {
        "codigo": "RADAR",
        "nome": "Radar",
        "oid": "",
        "tipo": "CORE",
        "rota_base": "radar",
        "auth_mode": "LEGACY",
        "somente_dev": False,
        "mantem_escopo_ids": True,
        "ativo": True,
        "sistema": True,
        "app_slug": "",
    },
    {
        "codigo": "APP_MILHAO_BLA",
        "nome": "App Milhao Bla",
        "oid": "",
        "tipo": "APP",
        "rota_base": "apps/appmilhaobla",
        "auth_mode": "LEGACY",
        "somente_dev": False,
        "mantem_escopo_ids": False,
        "ativo": True,
        "sistema": True,
        "app_slug": "appmilhaobla",
    },
    {
        "codigo": "APP_ROTAS",
        "nome": "App Rotas",
        "oid": "",
        "tipo": "APP",
        "rota_base": "apps/approtas",
        "auth_mode": "LEGACY",
        "somente_dev": False,
        "mantem_escopo_ids": False,
        "ativo": True,
        "sistema": True,
        "app_slug": "approtas",
    },
)


def _normalize_code(value):
    cleaned = re.sub(r"[^0-9A-Za-z]+", "_", (value or "").strip().upper()).strip("_")
    return cleaned[:60]


def _unique_code(TipoPerfil, base_code, pk, using):
    base_code = base_code or f"TIPO_{pk}"
    candidate = base_code[:60]
    suffix = 2
    while TipoPerfil.objects.using(using).filter(codigo=candidate).exclude(pk=pk).exists():
        suffix_str = f"_{suffix}"
        candidate = f"{base_code[: max(1, 60 - len(suffix_str))]}{suffix_str}"
        suffix += 1
    return candidate


def backfill_and_seed_access_control(apps, schema_editor):
    TipoPerfil = apps.get_model("core", "TipoPerfil")
    App = apps.get_model("core", "App")
    ModuloAcesso = apps.get_model("core", "ModuloAcesso")
    db_alias = schema_editor.connection.alias

    for tipo in TipoPerfil.objects.using(db_alias).all().order_by("id"):
        normalized_name = _normalize_code(tipo.nome)
        canonical_code = SYSTEM_TYPE_CODES.get(normalized_name, normalized_name)
        tipo.codigo = _unique_code(TipoPerfil, canonical_code, tipo.pk, db_alias)
        if tipo.codigo in SYSTEM_TYPE_CODES.values():
            tipo.sistema = True
        tipo.ativo = True
        tipo.save(update_fields=["codigo", "sistema", "ativo"])

    for codigo, nome in SYSTEM_TYPE_NAMES:
        TipoPerfil.objects.using(db_alias).get_or_create(
            codigo=codigo,
            defaults={
                "nome": nome,
                "sistema": True,
                "ativo": True,
            },
        )

    apps_by_slug = {
        (app.slug or "").strip().lower(): app.id
        for app in App.objects.using(db_alias).all().only("id", "slug")
    }
    dev = TipoPerfil.objects.using(db_alias).filter(codigo="DEV").first()
    master = TipoPerfil.objects.using(db_alias).filter(codigo="MASTER").first()

    for module_data in MODULE_DEFINITIONS:
        defaults = {
            "nome": module_data["nome"],
            "oid": module_data["oid"],
            "tipo": module_data["tipo"],
            "rota_base": module_data["rota_base"],
            "auth_mode": module_data["auth_mode"],
            "somente_dev": module_data["somente_dev"],
            "mantem_escopo_ids": module_data["mantem_escopo_ids"],
            "ativo": module_data["ativo"],
            "sistema": module_data["sistema"],
        }
        app_id = apps_by_slug.get(module_data["app_slug"].lower()) if module_data["app_slug"] else None
        if app_id:
            defaults["app_id"] = app_id
        modulo, created = ModuloAcesso.objects.using(db_alias).get_or_create(
            codigo=module_data["codigo"],
            defaults=defaults,
        )
        update_fields = []
        for field_name, field_value in defaults.items():
            if getattr(modulo, field_name) != field_value:
                setattr(modulo, field_name, field_value)
                update_fields.append(field_name)
        if update_fields:
            modulo.save(update_fields=update_fields)
        if dev:
            modulo.tipos.add(dev)
        if master:
            modulo.tipos.add(master)


class Migration(migrations.Migration):

    dependencies = [
        ("core", "0075_radaratividadecolaborador"),
    ]

    operations = [
        migrations.AddField(
            model_name="tipoperfil",
            name="ativo",
            field=models.BooleanField(default=True),
        ),
        migrations.AddField(
            model_name="tipoperfil",
            name="codigo",
            field=models.CharField(blank=True, default="", max_length=60),
        ),
        migrations.AddField(
            model_name="tipoperfil",
            name="sistema",
            field=models.BooleanField(default=False),
        ),
        migrations.AlterModelOptions(
            name="tipoperfil",
            options={"ordering": ["nome"]},
        ),
        migrations.CreateModel(
            name="ModuloAcesso",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("codigo", models.CharField(max_length=60, unique=True)),
                ("nome", models.CharField(max_length=120)),
                ("oid", models.CharField(blank=True, default="", max_length=120)),
                (
                    "tipo",
                    models.CharField(
                        choices=[("CORE", "Modulo interno"), ("APP", "App dedicado")],
                        default="CORE",
                        max_length=12,
                    ),
                ),
                ("rota_base", models.CharField(blank=True, default="", max_length=160)),
                (
                    "auth_mode",
                    models.CharField(
                        choices=[("LEGACY", "Legado"), ("HYBRID", "Hibrido"), ("STRICT", "Estrito")],
                        default="LEGACY",
                        max_length=12,
                    ),
                ),
                ("somente_dev", models.BooleanField(default=False)),
                ("mantem_escopo_ids", models.BooleanField(default=True)),
                ("ativo", models.BooleanField(default=True)),
                ("sistema", models.BooleanField(default=False)),
                ("criado_em", models.DateTimeField(auto_now_add=True)),
                ("atualizado_em", models.DateTimeField(auto_now=True)),
                (
                    "app",
                    models.ForeignKey(
                        blank=True,
                        null=True,
                        on_delete=django.db.models.deletion.SET_NULL,
                        related_name="modulos_acesso",
                        to="core.app",
                    ),
                ),
                (
                    "tipos",
                    models.ManyToManyField(blank=True, related_name="modulos_acesso", to="core.tipoperfil"),
                ),
            ],
            options={"ordering": ["nome"]},
        ),
        migrations.RunPython(backfill_and_seed_access_control, migrations.RunPython.noop),
        migrations.AlterField(
            model_name="tipoperfil",
            name="codigo",
            field=models.CharField(blank=True, max_length=60, unique=True),
        ),
    ]
