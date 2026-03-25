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


def resolve_module_from_path(path):
    from .models import ModuloAcesso

    normalized_path = "/" + (path or "").strip().lstrip("/")
    if normalized_path in {"/", ""}:
        return None
    if normalized_path.startswith("/static/") or normalized_path.startswith("/media/") or normalized_path.startswith("/admin/static/"):
        return None

    modules = (
        ModuloAcesso.objects.filter(ativo=True)
        .exclude(rota_base="")
        .select_related("app")
        .prefetch_related("tipos")
        .order_by("-rota_base")
    )
    for module in modules:
        route_base = "/" + (module.rota_base or "").strip().strip("/")
        if route_base == "/":
            continue
        if normalized_path == route_base or normalized_path.startswith(route_base + "/"):
            return module
    return None


def legacy_module_entry_allowed(user, module):
    if not user or not getattr(user, "is_authenticated", False) or not module:
        return False
    if is_admin_user(user):
        return True
    perfil = resolve_perfil(user)
    if module.codigo == "PROPOSTAS":
        return True
    if module.codigo in {"FINANCEIRO", "IOS", "INVENTARIO", "LISTA_IP", "RADAR"}:
        return bool(perfil)
    if module.codigo in {"APP_MILHAO_BLA", "APP_ROTAS"}:
        if not perfil or not module.app_id:
            return False
        return perfil.apps.filter(pk=module.app_id).exists()
    return bool(perfil)


def candidate_module_allowed(user, module):
    if not user or not getattr(user, "is_authenticated", False) or not module or not module.ativo:
        return False
    if is_admin_user(user):
        return True
    if module.somente_dev:
        return is_dev_user(user)
    allowed_codes = {
        (codigo or "").strip().upper()
        for codigo in module.tipos.values_list("codigo", flat=True)
        if codigo
    }
    if not allowed_codes:
        return False
    return bool(user_tipo_codes(user) & allowed_codes)


def shadow_decision_for_request(user, path):
    module = resolve_module_from_path(path)
    if not module or module.auth_mode == module.AuthMode.LEGACY:
        return None
    legacy_allowed = legacy_module_entry_allowed(user, module)
    candidate_allowed = candidate_module_allowed(user, module)
    return {
        "module": module,
        "legacy_allowed": legacy_allowed,
        "candidate_allowed": candidate_allowed,
        "divergent": legacy_allowed != candidate_allowed,
    }
