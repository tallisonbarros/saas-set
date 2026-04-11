from datetime import timedelta

from django.contrib.auth.models import User
from django.shortcuts import render
from django.shortcuts import redirect
from django.urls import reverse
from django.utils import timezone

from .access_control import has_tipo_code
from .models import AdminAccessLog, PerfilUsuario, SystemConfiguration


ADMIN_PRIVILEGED_TIPOS = {"MASTER", "DEV"}


class MaintenanceModeMiddleware:
    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        if self._should_redirect_to_maintenance(request):
            return redirect("maintenance")
        return self.get_response(request)

    def _should_redirect_to_maintenance(self, request):
        if self._is_allowed_path(request.path or ""):
            return False
        user = getattr(request, "user", None)
        if not user or not user.is_authenticated:
            return False
        if getattr(user, "is_superuser", False) or has_tipo_code(user, "DEV"):
            return False
        try:
            config = SystemConfiguration.load()
        except Exception:
            return False
        return bool(config.maintenance_mode_enabled)

    def _is_allowed_path(self, path):
        allowed_prefixes = ("/static/", "/media/", "/admin/static/")
        if path.startswith(allowed_prefixes):
            return True
        allowed_paths = {
            reverse("maintenance"),
            reverse("logout"),
        }
        return path in allowed_paths


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


class PrettyForbiddenMiddleware:
    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        response = self.get_response(request)
        if not self._should_render_error_page(request, response):
            return response
        return render(
            request,
            "error_minimal.html",
            self._error_page_context(response.status_code),
            status=response.status_code,
        )

    def _should_render_error_page(self, request, response):
        status_code = getattr(response, "status_code", None)
        if status_code not in {403, 404}:
            return False
        path = request.path or ""
        if path.startswith(("/static/", "/media/", "/admin/static/")):
            return False
        if request.headers.get("X-Requested-With") == "XMLHttpRequest":
            return False
        if getattr(response, "streaming", False):
            return False
        accept = (request.headers.get("Accept") or "").lower()
        if accept and "text/html" not in accept and "*/*" not in accept:
            return False
        content_type = (response.get("Content-Type") or "").lower()
        if "application/json" in content_type or "text/javascript" in content_type:
            return False
        return True

    def _error_page_context(self, status_code):
        pages = {
            403: {
                "status_code": "403",
                "title": "Acesso restrito",
                "message": "contate a set",
            },
            404: {
                "status_code": "404",
                "title": "Pagina indisponivel",
                "message": "contate a set",
            },
        }
        return pages.get(
            status_code,
            {
                "status_code": str(status_code or ""),
                "title": "Indisponivel",
                "message": "contate a set",
            },
        )
