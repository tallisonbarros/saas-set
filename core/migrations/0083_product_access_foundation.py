from django.conf import settings
from django.db import migrations, models
from django.utils import timezone


PRODUCT_CODE = "DOCUMENTACAO_TECNICA"


def seed_product_and_existing_access(apps, schema_editor):
    db_alias = schema_editor.connection.alias
    ProdutoPlataforma = apps.get_model("core", "ProdutoPlataforma")
    AcessoProdutoUsuario = apps.get_model("core", "AcessoProdutoUsuario")
    ModuloAcesso = apps.get_model("core", "ModuloAcesso")
    PerfilUsuario = apps.get_model("core", "PerfilUsuario")
    User = apps.get_model(*settings.AUTH_USER_MODEL.split("."))

    produto, _ = ProdutoPlataforma.objects.using(db_alias).get_or_create(
        codigo=PRODUCT_CODE,
        defaults={
            "nome": "Documentacao tecnica",
            "descricao": "Acesso conjunto aos modulos de IOs e Listas de IP.",
            "ativo": True,
        },
    )

    module_ids = list(
        ModuloAcesso.objects.using(db_alias)
        .filter(codigo__in=["IOS", "LISTA_IP"])
        .values_list("id", flat=True)
    )
    if not module_ids:
        return

    modulo_tipo_ids = set(
        ModuloAcesso.tipos.through.objects.using(db_alias)
        .filter(moduloacesso_id__in=module_ids)
        .values_list("tipoperfil_id", flat=True)
    )
    if not modulo_tipo_ids:
        return

    perfil_ids = set(
        PerfilUsuario.tipos.through.objects.using(db_alias)
        .filter(tipoperfil_id__in=modulo_tipo_ids)
        .values_list("perfilusuario_id", flat=True)
    )
    if not perfil_ids:
        return

    user_ids = list(
        PerfilUsuario.objects.using(db_alias)
        .filter(id__in=perfil_ids, usuario_id__isnull=False)
        .values_list("usuario_id", flat=True)
    )
    if not user_ids:
        return

    privileged_user_ids = set(
        User.objects.using(db_alias)
        .filter(id__in=user_ids)
        .filter(models.Q(is_superuser=True) | models.Q(is_staff=True))
        .values_list("id", flat=True)
    )

    now = timezone.now()
    for user_id in user_ids:
        if user_id in privileged_user_ids:
            continue
        AcessoProdutoUsuario.objects.using(db_alias).get_or_create(
            usuario_id=user_id,
            produto_id=produto.id,
            defaults={
                "origem": "MANUAL",
                "status": "ATIVO",
                "acesso_inicio": now,
                "observacao": "Migrado automaticamente a partir do acesso legado aos modulos IOS/LISTA_IP.",
            },
        )


class Migration(migrations.Migration):

    dependencies = [
        ("core", "0082_systemconfiguration"),
    ]

    operations = [
        migrations.CreateModel(
            name="ProdutoPlataforma",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("codigo", models.CharField(max_length=60, unique=True)),
                ("nome", models.CharField(max_length=120)),
                ("descricao", models.TextField(blank=True)),
                ("ativo", models.BooleanField(default=True)),
                ("criado_em", models.DateTimeField(auto_now_add=True)),
                ("atualizado_em", models.DateTimeField(auto_now=True)),
            ],
            options={"ordering": ["nome"]},
        ),
        migrations.CreateModel(
            name="AcessoProdutoUsuario",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                (
                    "origem",
                    models.CharField(
                        choices=[("TRIAL", "Trial"), ("MANUAL", "Manual"), ("INTERNO", "Interno")],
                        default="TRIAL",
                        max_length=16,
                    ),
                ),
                (
                    "status",
                    models.CharField(
                        choices=[
                            ("TRIAL_ATIVO", "Trial ativo"),
                            ("ATIVO", "Ativo"),
                            ("EXPIRADO", "Expirado"),
                            ("BLOQUEADO", "Bloqueado"),
                        ],
                        default="TRIAL_ATIVO",
                        max_length=20,
                    ),
                ),
                ("trial_inicio", models.DateTimeField(blank=True, null=True)),
                ("trial_fim", models.DateTimeField(blank=True, null=True)),
                ("acesso_inicio", models.DateTimeField(default=timezone.now)),
                ("acesso_fim", models.DateTimeField(blank=True, null=True)),
                ("observacao", models.TextField(blank=True)),
                ("criado_em", models.DateTimeField(auto_now_add=True)),
                ("atualizado_em", models.DateTimeField(auto_now=True)),
                (
                    "produto",
                    models.ForeignKey(on_delete=models.deletion.CASCADE, related_name="acessos_usuario", to="core.produtoplataforma"),
                ),
                (
                    "usuario",
                    models.ForeignKey(
                        on_delete=models.deletion.CASCADE,
                        related_name="acessos_produto",
                        to=settings.AUTH_USER_MODEL,
                    ),
                ),
            ],
            options={"ordering": ["produto__nome", "usuario__username"]},
        ),
        migrations.AddConstraint(
            model_name="acessoprodutousuario",
            constraint=models.UniqueConstraint(fields=("usuario", "produto"), name="unique_acesso_produto_usuario"),
        ),
        migrations.RunPython(seed_product_and_existing_access, migrations.RunPython.noop),
    ]
