from datetime import timedelta

from django.contrib.auth.models import User
from django.utils import timezone

from .access_control import shadow_decision_for_request
from .models import AdminAccessLog, AccessControlShadowLog, PerfilUsuario


ADMIN_PRIVILEGED_TIPOS = {"MASTER", "DEV"}


class AdminAccessLogMiddleware:
    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        self._ensure_staff_from_profile(request)
        response = self.get_response(request)

        try:
            path = request.path or ""
            if path.startswith("/static/") or path.startswith("/media/") or path.startswith("/admin/static/"):
                return response

            user = getattr(request, "user", None)
            if not user or not user.is_authenticated:
                return response

            module = self._module_from_path(path)
            AdminAccessLog.objects.create(
                user=user,
                module=module,
            )
            cutoff = timezone.now() - timedelta(days=90)
            AdminAccessLog.objects.filter(created_at__lt=cutoff).delete()
        except Exception:
            # Evita quebrar o admin se o banco estiver indisponivel.
            pass

        return response

    def _ensure_staff_from_profile(self, request):
        user = getattr(request, "user", None)
        if not user or not user.is_authenticated or user.is_superuser or user.is_staff:
            return
        perfil = self._resolve_perfil(user)
        if not perfil:
            return
        tipo_nomes = ((nome or "").strip().upper() for nome in perfil.tipos.values_list("nome", flat=True))
        if not any(nome in ADMIN_PRIVILEGED_TIPOS for nome in tipo_nomes):
            return
        User.objects.filter(pk=user.pk, is_staff=False).update(is_staff=True)
        user.is_staff = True

    def _resolve_perfil(self, user):
        try:
            return user.perfilusuario
        except PerfilUsuario.DoesNotExist:
            email = (user.email or user.username or "").strip().lower()
            if not email:
                return None
            return PerfilUsuario.objects.filter(email__iexact=email).first()

    def _module_from_path(self, path):
        stripped = path.strip("/")
        if not stripped:
            return "home"
        first = stripped.split("/", 1)[0]
        if first == "apps":
            parts = stripped.split("/")
            if len(parts) >= 2 and parts[1]:
                return f"apps:{parts[1]}"
        if first == "admin":
            return "admin"
        return first


class AccessControlShadowMiddleware:
    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        response = self.get_response(request)

        try:
            user = getattr(request, "user", None)
            if not user or not user.is_authenticated:
                return response
            decision = shadow_decision_for_request(user, request.path or "")
            if not decision or not decision["divergent"]:
                return response

            modulo = decision["module"]
            cutoff_recent = timezone.now() - timedelta(minutes=5)
            already_logged = AccessControlShadowLog.objects.filter(
                user=user,
                modulo=modulo,
                request_path=request.path or "",
                legacy_allowed=decision["legacy_allowed"],
                candidate_allowed=decision["candidate_allowed"],
                created_at__gte=cutoff_recent,
            ).exists()
            if not already_logged:
                AccessControlShadowLog.objects.create(
                    user=user,
                    modulo=modulo,
                    request_path=request.path or "",
                    response_status=getattr(response, "status_code", 200) or 200,
                    legacy_allowed=decision["legacy_allowed"],
                    candidate_allowed=decision["candidate_allowed"],
                    auth_mode=modulo.auth_mode,
                )
            retention_cutoff = timezone.now() - timedelta(days=30)
            AccessControlShadowLog.objects.filter(created_at__lt=retention_cutoff).delete()
        except Exception:
            pass

        return response
