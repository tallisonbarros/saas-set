from datetime import timedelta

from django.utils import timezone

from .models import AdminAccessLog


class AdminAccessLogMiddleware:
    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
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
