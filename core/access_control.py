import re


def normalize_access_code(value):
    cleaned = re.sub(r"[^0-9A-Za-z]+", "_", (value or "").strip().upper()).strip("_")
    return cleaned[:60]


def user_tipo_codes(user):
    if not user or not getattr(user, "is_authenticated", False):
        return set()
    from .models import PerfilUsuario

    try:
        perfil = user.perfilusuario
    except PerfilUsuario.DoesNotExist:
        perfil = None
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
