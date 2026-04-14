from __future__ import annotations

from datetime import timedelta

from django.utils import timezone

from core.access_control import TRIAL_DURATION_DAYS, get_user_product_access, is_admin_user, resolve_perfil
from core.models import (
    AcessoProdutoUsuario,
    AssinaturaUsuario,
    ConfiguracaoPagamento,
    PlanoComercial,
    ProdutoPlataforma,
    RackIO,
)


DOCUMENTATION_PRODUCT_CODE = "DOCUMENTACAO_TECNICA"
STARTER_PLAN_CODE = PlanoComercial.Codigo.STARTER
PROFESSIONAL_PLAN_CODE = PlanoComercial.Codigo.PROFESSIONAL


def ensure_billing_catalog():
    product, _ = ProdutoPlataforma.objects.get_or_create(
        codigo=DOCUMENTATION_PRODUCT_CODE,
        defaults={
            "nome": "Documentacao tecnica",
            "descricao": "Acesso conjunto aos modulos de IOs e Listas de IP.",
            "ativo": True,
        },
    )
    PlanoComercial.objects.get_or_create(
        produto=product,
        codigo=STARTER_PLAN_CODE,
        defaults={
            "nome": "Plano Iniciante",
            "descricao": "Uso gratuito apos o trial com ate 3 racks simultaneos.",
            "ativo": True,
            "is_free": True,
            "ordem": 10,
            "rack_limit_simultaneous": 3,
            "preco_mensal": 0,
            "preco_anual": 0,
        },
    )
    PlanoComercial.objects.get_or_create(
        produto=product,
        codigo=PROFESSIONAL_PLAN_CODE,
        defaults={
            "nome": "Plano Profissional",
            "descricao": "Uso completo com racks ilimitados.",
            "ativo": True,
            "is_free": False,
            "ordem": 20,
            "rack_limit_simultaneous": None,
        },
    )
    ConfiguracaoPagamento.load()
    return product


def product_by_code(product_code: str):
    normalized = (product_code or "").strip().upper()
    if not normalized:
        return None
    ensure_billing_catalog()
    return ProdutoPlataforma.objects.filter(codigo=normalized).first()


def plan_by_code(product_code: str, plan_code: str):
    product = product_by_code(product_code)
    if not product:
        return None
    return (
        PlanoComercial.objects.filter(produto=product, codigo=(plan_code or "").strip().upper(), ativo=True)
        .order_by("ordem", "nome")
        .first()
    )


def payment_config():
    return ConfiguracaoPagamento.load()


def trial_duration_days():
    config = payment_config()
    return int(config.trial_duration_days or TRIAL_DURATION_DAYS)


def count_user_racks(user):
    perfil = resolve_perfil(user)
    if not perfil:
        return 0
    return RackIO.objects.filter(cliente=perfil).count()


def sync_subscription_status(subscription):
    if not subscription:
        return None
    now = timezone.now()
    updated_fields = []
    if subscription.status in {AssinaturaUsuario.Status.ACTIVE, AssinaturaUsuario.Status.TRIALING}:
        if subscription.expires_at and subscription.expires_at <= now:
            subscription.status = AssinaturaUsuario.Status.EXPIRED
            updated_fields.append("status")
        elif subscription.current_period_end and subscription.current_period_end <= now and not subscription.auto_renew:
            subscription.status = AssinaturaUsuario.Status.EXPIRED
            updated_fields.append("status")
    if updated_fields:
        subscription.save(update_fields=updated_fields + ["updated_at"])
    return subscription


def active_subscription(user, product_code):
    product = product_by_code(product_code)
    if not product or not user or not getattr(user, "is_authenticated", False):
        return None
    subscription = (
        AssinaturaUsuario.objects.select_related("plano", "produto")
        .filter(usuario=user, produto=product)
        .order_by("-updated_at", "-created_at")
        .first()
    )
    return sync_subscription_status(subscription)


def starter_plan(product_code=DOCUMENTATION_PRODUCT_CODE):
    return plan_by_code(product_code, STARTER_PLAN_CODE)


def professional_plan(product_code=DOCUMENTATION_PRODUCT_CODE):
    return plan_by_code(product_code, PROFESSIONAL_PLAN_CODE)


def _trial_days_remaining(access):
    if not access or access.status != AcessoProdutoUsuario.Status.TRIAL_ATIVO or not access.trial_fim:
        return None
    remaining = access.trial_fim - timezone.now()
    if remaining.total_seconds() <= 0:
        return 0
    return max(1, remaining.days + (1 if remaining.seconds > 0 else 0))


def _starter_limit(starter):
    if not starter or starter.rack_limit_simultaneous is None:
        return 3
    return int(starter.rack_limit_simultaneous)


def resolve_entitlement(user, product_code=DOCUMENTATION_PRODUCT_CODE):
    ensure_billing_catalog()
    starter = starter_plan(product_code)
    professional = professional_plan(product_code)
    product = product_by_code(product_code)
    rack_count = count_user_racks(user)
    starter_limit = _starter_limit(starter)
    starter_available = rack_count <= starter_limit
    access = None
    subscription = None
    current_plan = None
    status = "requires_plan_selection"
    current_label = ""
    days_remaining = None
    has_access = False
    if not user or not getattr(user, "is_authenticated", False):
        return {
            "product": product,
            "starter_plan": starter,
            "professional_plan": professional,
            "subscription": None,
            "access": None,
            "current_plan": None,
            "status": "anonymous",
            "has_access": False,
            "requires_plan_selection": True,
            "trial_days_remaining": None,
            "rack_count": rack_count,
            "starter_limit": starter_limit,
            "starter_available": starter_available,
            "starter_excess": max(rack_count - starter_limit, 0),
            "badge_label": "",
            "badge_tone": "info",
            "legacy_manual_access": False,
        }
    if is_admin_user(user):
        return {
            "product": product,
            "starter_plan": starter,
            "professional_plan": professional,
            "subscription": None,
            "access": None,
            "current_plan": None,
            "status": "admin_access",
            "has_access": True,
            "requires_plan_selection": False,
            "trial_days_remaining": None,
            "rack_count": rack_count,
            "starter_limit": starter_limit,
            "starter_available": starter_available,
            "starter_excess": max(rack_count - starter_limit, 0),
            "badge_label": "Acesso administrativo",
            "badge_tone": "success",
            "legacy_manual_access": False,
        }

    access = get_user_product_access(user, product_code)
    subscription = active_subscription(user, product_code)
    if access and access.status == AcessoProdutoUsuario.Status.TRIAL_ATIVO:
        days_remaining = _trial_days_remaining(access)
        if days_remaining and days_remaining > 0:
            has_access = True
            status = "trial_active"
            current_label = f"Trial ativo · {days_remaining} dia{'s' if days_remaining != 1 else ''}"
    if not has_access and subscription and subscription.status in {
        AssinaturaUsuario.Status.ACTIVE,
        AssinaturaUsuario.Status.TRIALING,
    }:
        current_plan = subscription.plano
        if current_plan and current_plan.codigo == STARTER_PLAN_CODE and not starter_available:
            status = "starter_blocked_by_usage"
            has_access = False
            current_label = "Plano Iniciante acima do limite"
        else:
            status = "plan_active"
            has_access = True
            current_label = current_plan.nome if current_plan else "Plano ativo"
    if (
        not has_access
        and access
        and access.status == AcessoProdutoUsuario.Status.ATIVO
        and status != "starter_blocked_by_usage"
    ):
        status = "legacy_active"
        has_access = True
        current_label = "Acesso ativo"
    if not has_access and access and access.status == AcessoProdutoUsuario.Status.BLOQUEADO:
        status = "blocked"
    elif not has_access and access and access.status == AcessoProdutoUsuario.Status.EXPIRADO:
        status = "trial_expired"

    if status == "requires_plan_selection" and access and access.status == AcessoProdutoUsuario.Status.TRIAL_ATIVO:
        status = "trial_expired"

    badge_label = ""
    badge_tone = "info"
    if status == "trial_active":
        badge_label = current_label
        badge_tone = "warning"
    elif status == "plan_active":
        badge_label = current_label
        badge_tone = "success"
    elif status == "legacy_active":
        badge_label = "Acesso liberado"
        badge_tone = "success"
    elif status == "starter_blocked_by_usage":
        badge_label = f"Starter indisponivel · {rack_count}/{starter_limit} racks"
        badge_tone = "warning"
    elif status in {"trial_expired", "requires_plan_selection", "blocked"}:
        badge_label = "Escolha um plano"
        badge_tone = "warning"

    return {
        "product": product,
        "starter_plan": starter,
        "professional_plan": professional,
        "subscription": subscription,
        "access": access,
        "current_plan": current_plan,
        "status": status,
        "has_access": has_access,
        "requires_plan_selection": not has_access,
        "trial_days_remaining": days_remaining,
        "rack_count": rack_count,
        "starter_limit": starter_limit,
        "starter_available": starter_available,
        "starter_excess": max(rack_count - starter_limit, 0),
        "badge_label": badge_label,
        "badge_tone": badge_tone,
        "legacy_manual_access": bool(access and access.status == AcessoProdutoUsuario.Status.ATIVO and not subscription),
    }


def activate_trial(user, product_code=DOCUMENTATION_PRODUCT_CODE):
    product = product_by_code(product_code)
    if not product:
        return None
    now = timezone.now()
    trial_end = now + timedelta(days=trial_duration_days())
    access, _ = AcessoProdutoUsuario.objects.update_or_create(
        usuario=user,
        produto=product,
        defaults={
            "origem": AcessoProdutoUsuario.Origem.TRIAL,
            "status": AcessoProdutoUsuario.Status.TRIAL_ATIVO,
            "trial_inicio": now,
            "trial_fim": trial_end,
            "acesso_inicio": now,
            "acesso_fim": None,
            "observacao": "Trial iniciado pela tela comercial do produto.",
        },
    )
    return access


def activate_starter_plan(user, product_code=DOCUMENTATION_PRODUCT_CODE):
    entitlement = resolve_entitlement(user, product_code)
    if not entitlement["starter_available"]:
        return None, "O plano Iniciante permite ate 3 racks simultaneos. Exclua racks para liberar este plano."
    starter = entitlement["starter_plan"]
    product = entitlement["product"]
    now = timezone.now()
    subscription, _ = AssinaturaUsuario.objects.update_or_create(
        usuario=user,
        produto=product,
        defaults={
            "plano": starter,
            "provider": AssinaturaUsuario.Provider.INTERNAL,
            "status": AssinaturaUsuario.Status.ACTIVE,
            "billing_interval": AssinaturaUsuario.BillingInterval.MONTHLY,
            "auto_renew": True,
            "preco_ciclo": 0,
            "moeda": "BRL",
            "current_period_start": now,
            "current_period_end": None,
            "expires_at": None,
            "checkout_url": "",
            "observacao": "Plano Iniciante ativado internamente.",
        },
    )
    if product:
        AcessoProdutoUsuario.objects.update_or_create(
            usuario=user,
            produto=product,
            defaults={
                "origem": AcessoProdutoUsuario.Origem.INTERNO,
                "status": AcessoProdutoUsuario.Status.ATIVO,
                "trial_inicio": None,
                "trial_fim": None,
                "acesso_inicio": subscription.current_period_start or now,
                "acesso_fim": None,
                "observacao": "Entitlement sincronizado com o Plano Iniciante.",
            },
        )
    return subscription, ""


def start_professional_checkout(user, billing_interval=AssinaturaUsuario.BillingInterval.MONTHLY, product_code=DOCUMENTATION_PRODUCT_CODE):
    product = product_by_code(product_code)
    plan = professional_plan(product_code)
    config = payment_config()
    now = timezone.now()
    price = plan.preco_anual if billing_interval == AssinaturaUsuario.BillingInterval.YEARLY else plan.preco_mensal
    subscription, _ = AssinaturaUsuario.objects.update_or_create(
        usuario=user,
        produto=product,
        defaults={
            "plano": plan,
            "provider": AssinaturaUsuario.Provider.MERCADO_PAGO,
            "status": AssinaturaUsuario.Status.PENDING,
            "billing_interval": billing_interval,
            "auto_renew": True,
            "preco_ciclo": price,
            "moeda": "BRL",
            "current_period_start": now,
            "current_period_end": None,
            "checkout_url": "",
            "observacao": "Aguardando conclusao do checkout profissional.",
        },
    )
    if not config.enabled or not config.mercado_pago_public_key or not config.mercado_pago_access_token:
        return subscription, "A integracao de pagamento ainda nao foi configurada no painel administrativo."
    return subscription, "Checkout preparado para integracao com Mercado Pago."
