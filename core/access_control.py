import re


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


def visible_internal_module_codes(user):
    from .models import ModuloAcesso

    modules = ModuloAcesso.objects.filter(tipo=ModuloAcesso.Tipo.CORE, ativo=True).prefetch_related("tipos")
    if is_admin_user(user):
        return {module.codigo for module in modules}
    visible = set()
    for module in modules:
        if can_access_internal_module(user, module.codigo):
            visible.add(module.codigo)
    return visible


def resolve_module_by_code(module_code):
    from .models import ModuloAcesso

    normalized = normalize_access_code(module_code)
    if not normalized:
        return None
    return ModuloAcesso.objects.filter(codigo=normalized).first()
