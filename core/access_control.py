import re

from django.utils import timezone


TRIAL_DURATION_DAYS = 30
COMMERCIAL_PRODUCT_BY_MODULE = {
    "IOS": "DOCUMENTACAO_TECNICA",
    "LISTA_IP": "DOCUMENTACAO_TECNICA",
}

def normalize_access_code(value):
    cleaned = re.sub(r"[^0-9A-Za-z]+", "_", (value or "").strip().upper()).strip("_")
    return cleaned[:60]


def resolve_perfil(user):
    if not user or not getattr(user, "is_authenticated", False):
        return None
    from .models import PerfilUsuario

    try:
        return user.perfilusuario
    except PerfilUsuario.DoesNotExist:
        email = (getattr(user, "email", "") or getattr(user, "username", "") or "").strip().lower()
        if not email:
            return None
        return PerfilUsuario.objects.filter(email__iexact=email).first()


def user_tipo_codes(user):
    if not user or not getattr(user, "is_authenticated", False):
        return set()
    perfil = resolve_perfil(user)
    if not perfil:
        return set()
    return {
        (codigo or "").strip().upper()
        for codigo in perfil.tipos.values_list("codigo", flat=True)
        if codigo
    }


def has_tipo_code(user, code):
    normalized = normalize_access_code(code)
    if not normalized:
        return False
    return normalized in user_tipo_codes(user)


def is_admin_user(user):
    if not user or not getattr(user, "is_authenticated", False):
        return False
    if getattr(user, "is_superuser", False) or getattr(user, "is_staff", False):
        return True
    codes = user_tipo_codes(user)
    return "MASTER" in codes or "DEV" in codes


def is_dev_user(user):
    return has_tipo_code(user, "DEV") or is_admin_user(user)


def can_access_internal_module(user, module_code):
    if not user or not getattr(user, "is_authenticated", False):
        return False
    if is_admin_user(user):
        return True
    module = resolve_module_by_code(module_code)
    if not module or not module.ativo or module.tipo != module.Tipo.CORE:
        return False
    allowed_codes = {
        (codigo or "").strip().upper()
        for codigo in module.tipos.values_list("codigo", flat=True)
        if codigo
    }
    if not allowed_codes:
        return False
    return bool(user_tipo_codes(user) & allowed_codes)


def resolve_product_by_code(product_code):
    from .models import ProdutoPlataforma

    normalized = normalize_access_code(product_code)
    if not normalized:
        return None
    return ProdutoPlataforma.objects.filter(codigo=normalized).first()


def resolve_commercial_product_code(module_code):
    return COMMERCIAL_PRODUCT_BY_MODULE.get(normalize_access_code(module_code))


def get_user_product_access(user, product_code, sync_status=True):
    from .models import AcessoProdutoUsuario

    if not user or not getattr(user, "is_authenticated", False):
        return None
    normalized = normalize_access_code(product_code)
    if not normalized:
        return None
    access = (
        AcessoProdutoUsuario.objects.select_related("produto")
        .filter(usuario=user, produto__codigo=normalized, produto__ativo=True)
        .first()
    )
    if not access or not sync_status:
        return access
    now = timezone.now()
    updated_fields = []
    if access.status == AcessoProdutoUsuario.Status.TRIAL_ATIVO and access.trial_fim and access.trial_fim <= now:
        access.status = AcessoProdutoUsuario.Status.EXPIRADO
        updated_fields.append("status")
    elif access.status == AcessoProdutoUsuario.Status.ATIVO and access.acesso_fim and access.acesso_fim <= now:
        access.status = AcessoProdutoUsuario.Status.EXPIRADO
        updated_fields.append("status")
    if updated_fields:
        access.save(update_fields=updated_fields + ["atualizado_em"])
    return access


def user_has_product_access(user, product_code):
    from .models import AcessoProdutoUsuario

    if not user or not getattr(user, "is_authenticated", False):
        return False
    if is_admin_user(user):
        return True
    normalized = normalize_access_code(product_code)
    if normalized == "DOCUMENTACAO_TECNICA":
        from .services.billing import resolve_entitlement

        return bool(resolve_entitlement(user, normalized).get("has_access"))
    access = get_user_product_access(user, product_code)
    if not access:
        return False
    if access.status == AcessoProdutoUsuario.Status.BLOQUEADO:
        return False
    if access.status == AcessoProdutoUsuario.Status.TRIAL_ATIVO:
        return not access.trial_fim or access.trial_fim > timezone.now()
    if access.status == AcessoProdutoUsuario.Status.ATIVO:
        return not access.acesso_fim or access.acesso_fim > timezone.now()
    return False


def user_has_commercial_module_access(user, module_code):
    product_code = resolve_commercial_product_code(module_code)
    if not product_code:
        return False
    return user_has_product_access(user, product_code)


def visible_internal_module_codes(user):
    from .models import ModuloAcesso

    modules = ModuloAcesso.objects.filter(tipo=ModuloAcesso.Tipo.CORE, ativo=True).prefetch_related("tipos")
    if is_admin_user(user):
        return {module.codigo for module in modules}
    visible = set()
    for module in modules:
        if can_access_internal_module(user, module.codigo):
            visible.add(module.codigo)
    for module_code, product_code in COMMERCIAL_PRODUCT_BY_MODULE.items():
        if user_has_product_access(user, product_code):
            visible.add(module_code)
    return visible


def resolve_module_by_code(module_code):
    from .models import ModuloAcesso

    normalized = normalize_access_code(module_code)
    if not normalized:
        return None
    return ModuloAcesso.objects.filter(codigo=normalized).first()
