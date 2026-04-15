from __future__ import annotations

import hashlib
import hmac
import json
from decimal import Decimal
from datetime import datetime, timedelta
from urllib.parse import urlencode
from urllib import error as urlerror
from urllib import request as urlrequest

from django.utils import timezone
from django.utils.dateparse import parse_datetime

from core.access_control import TRIAL_DURATION_DAYS, get_user_product_access, is_admin_user, resolve_perfil
from core.models import (
    AcessoProdutoUsuario,
    AssinaturaUsuario,
    ConsumoImportacaoDiaria,
    ConfiguracaoPagamento,
    PlanoComercial,
    ProdutoPlataforma,
    RackIO,
)


DOCUMENTATION_PRODUCT_CODE = "DOCUMENTACAO_TECNICA"
STARTER_PLAN_CODE = PlanoComercial.Codigo.STARTER
PROFESSIONAL_PLAN_CODE = PlanoComercial.Codigo.PROFESSIONAL
DEFAULT_DAILY_IO_IMPORT_LIMIT = 3
DEFAULT_DAILY_IP_IMPORT_LIMIT = 3
STRIPE_API_BASE_URL = "https://api.stripe.com/v1"


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
            "daily_io_import_limit": DEFAULT_DAILY_IO_IMPORT_LIMIT,
            "daily_ip_import_limit": DEFAULT_DAILY_IP_IMPORT_LIMIT,
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
            "daily_io_import_limit": None,
            "daily_ip_import_limit": None,
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


def _stripe_api_request(config, method, path, payload=None, *, timeout=60, idempotency_key=""):
    token = (config.stripe_secret_key or "").strip()
    if not token:
        raise ValueError("Secret key do Stripe nao configurada.")

    method = (method or "GET").upper()
    url = f"{STRIPE_API_BASE_URL}{path}"
    body = None
    if method in {"GET", "DELETE"}:
        if payload:
            query = urlencode(payload, doseq=True)
            separator = "&" if "?" in url else "?"
            url = f"{url}{separator}{query}"
    elif payload is not None:
        body = urlencode(payload, doseq=True).encode("utf-8")

    request = urlrequest.Request(url, data=body, method=method)
    request.add_header("Authorization", f"Bearer {token}")
    request.add_header("Accept", "application/json")
    if body is not None:
        request.add_header("Content-Type", "application/x-www-form-urlencoded")
    if idempotency_key:
        request.add_header("Idempotency-Key", idempotency_key)

    try:
        with urlrequest.urlopen(request, timeout=timeout) as response:
            raw = response.read().decode("utf-8")
    except urlerror.HTTPError as exc:
        raw = exc.read().decode("utf-8", errors="ignore")
        detail = raw.strip() or f"HTTP {exc.code}"
        try:
            parsed = json.loads(raw) if raw else {}
        except ValueError:
            parsed = {}
        error_message = parsed.get("error", {}).get("message") if isinstance(parsed, dict) else ""
        raise ValueError(f"Stripe retornou erro ao processar a requisicao: {error_message or detail}") from exc
    except urlerror.URLError as exc:
        raise ValueError("Nao foi possivel conectar ao Stripe.") from exc

    if not raw:
        return {}
    try:
        return json.loads(raw)
    except ValueError as exc:
        raise ValueError("Stripe retornou uma resposta invalida.") from exc


def _provider_plan_code(plan, billing_interval):
    if not plan:
        return ""
    if billing_interval == AssinaturaUsuario.BillingInterval.YEARLY:
        return (plan.provider_plan_code_anual or "").strip()
    return (plan.provider_plan_code_mensal or "").strip()


def _build_subscription_reason(product, plan, billing_interval):
    product_name = product.nome if product else "Produto SET"
    interval_label = "Anual" if billing_interval == AssinaturaUsuario.BillingInterval.YEARLY else "Mensal"
    return f"{product_name} - {plan.nome} ({interval_label})"


def _checkout_success_url_with_session_id(base_url):
    normalized = str(base_url or "").strip()
    if not normalized:
        return ""
    separator = "&" if "?" in normalized else "?"
    return f"{normalized}{separator}session_id={{CHECKOUT_SESSION_ID}}"


def _subscription_external_reference(subscription):
    timestamp = int(timezone.now().timestamp())
    return f"SETSUB-{subscription.id}-{timestamp}"


def _parse_provider_datetime(value):
    if not value:
        return None
    parsed = parse_datetime(str(value))
    if not parsed:
        return None
    if timezone.is_naive(parsed):
        return timezone.make_aware(parsed, timezone.get_current_timezone())
    return parsed


def _from_unix_timestamp(value):
    try:
        normalized = int(value or 0)
    except (TypeError, ValueError):
        return None
    if normalized <= 0:
        return None
    return timezone.make_aware(datetime.fromtimestamp(normalized), timezone.get_current_timezone())


def _billing_interval_from_remote(interval_value, default_interval):
    normalized = (interval_value or "").strip().lower()
    if normalized == "year":
        return AssinaturaUsuario.BillingInterval.YEARLY
    if normalized == "month":
        return AssinaturaUsuario.BillingInterval.MONTHLY
    return default_interval


def _subscription_status_from_provider(remote_status):
    normalized = (remote_status or "").strip().lower()
    if normalized in {"active", "authorized"}:
        return AssinaturaUsuario.Status.ACTIVE
    if normalized in {"trialing"}:
        return AssinaturaUsuario.Status.TRIALING
    if normalized in {"pending", "in_process", "incomplete"}:
        return AssinaturaUsuario.Status.PENDING
    if normalized in {"past_due", "unpaid", "paused"}:
        return AssinaturaUsuario.Status.PAST_DUE
    if normalized in {"canceled", "cancelled", "cancelled_by_user"}:
        return AssinaturaUsuario.Status.CANCELED
    if normalized in {"expired", "incomplete_expired"}:
        return AssinaturaUsuario.Status.EXPIRED
    return AssinaturaUsuario.Status.PENDING


def _sync_access_from_subscription(subscription):
    if not subscription or not subscription.usuario_id or not subscription.produto_id:
        return None

    now = timezone.now()
    access = AcessoProdutoUsuario.objects.filter(usuario=subscription.usuario, produto=subscription.produto).first()

    if subscription.status in {AssinaturaUsuario.Status.ACTIVE, AssinaturaUsuario.Status.TRIALING}:
        access, _ = AcessoProdutoUsuario.objects.update_or_create(
            usuario=subscription.usuario,
            produto=subscription.produto,
            defaults={
                "origem": AcessoProdutoUsuario.Origem.INTERNO,
                "status": AcessoProdutoUsuario.Status.ATIVO,
                "trial_inicio": None,
                "trial_fim": None,
                "acesso_inicio": subscription.current_period_start or now,
                "acesso_fim": None,
                "observacao": f"Entitlement sincronizado com a assinatura profissional via {subscription.get_provider_display().lower()}.",
            },
        )
        return access

    if access and access.status == AcessoProdutoUsuario.Status.TRIAL_ATIVO and access.trial_fim and access.trial_fim > now:
        return access

    if not access:
        return None

    access.origem = AcessoProdutoUsuario.Origem.INTERNO
    if subscription.status in {AssinaturaUsuario.Status.CANCELED, AssinaturaUsuario.Status.EXPIRED}:
        access.status = AcessoProdutoUsuario.Status.EXPIRADO
        access.acesso_fim = now
        access.observacao = "A assinatura profissional foi encerrada e o acesso foi expirado."
    else:
        access.status = AcessoProdutoUsuario.Status.BLOQUEADO
        access.acesso_fim = None
        access.observacao = "A assinatura profissional ainda nao foi confirmada pelo provider."
    access.save(update_fields=["origem", "status", "acesso_fim", "observacao", "atualizado_em"])
    return access


def _build_professional_checkout_payload(subscription, config):
    provider_plan_code = _provider_plan_code(subscription.plano, subscription.billing_interval)
    interval = "year" if subscription.billing_interval == AssinaturaUsuario.BillingInterval.YEARLY else "month"
    payload = [
        ("mode", "subscription"),
        ("success_url", _checkout_success_url_with_session_id(config.checkout_success_url)),
        ("cancel_url", config.checkout_failure_url),
        ("customer_email", subscription.usuario.email),
        ("client_reference_id", subscription.external_reference),
        ("metadata[external_reference]", subscription.external_reference),
        ("metadata[user_id]", str(subscription.usuario_id)),
        ("metadata[product_code]", subscription.produto.codigo if subscription.produto else DOCUMENTATION_PRODUCT_CODE),
        ("metadata[billing_interval]", subscription.billing_interval),
        ("subscription_data[metadata][external_reference]", subscription.external_reference),
        ("subscription_data[metadata][user_id]", str(subscription.usuario_id)),
        ("subscription_data[metadata][product_code]", subscription.produto.codigo if subscription.produto else DOCUMENTATION_PRODUCT_CODE),
        ("line_items[0][quantity]", "1"),
    ]
    if provider_plan_code:
        payload.append(("line_items[0][price]", provider_plan_code))
    else:
        unit_amount = int((subscription.preco_ciclo or Decimal("0")) * 100)
        payload.extend(
            [
                ("line_items[0][price_data][currency]", (subscription.moeda or "BRL").lower()),
                ("line_items[0][price_data][unit_amount]", str(unit_amount)),
                ("line_items[0][price_data][product_data][name]", _build_subscription_reason(subscription.produto, subscription.plano, subscription.billing_interval)),
                ("line_items[0][price_data][product_data][description]", subscription.plano.descricao or "Assinatura da plataforma SET."),
                ("line_items[0][price_data][recurring][interval]", interval),
            ]
        )
    return payload


def _resolve_checkout_url(response_payload):
    return str(response_payload.get("url") or "").strip()


def fetch_provider_subscription(config, provider_subscription_id):
    if not provider_subscription_id:
        raise ValueError("Identificador da assinatura no provider nao informado.")
    return _stripe_api_request(
        config,
        "GET",
        f"/subscriptions/{provider_subscription_id}",
        payload=[("expand[]", "items.data.price")],
        timeout=60,
    )


def reconcile_provider_subscription(remote_payload, *, fallback_subscription=None):
    provider_subscription_id = str(remote_payload.get("id") or "").strip()
    metadata = remote_payload.get("metadata") or {}
    external_reference = str(metadata.get("external_reference") or remote_payload.get("client_reference_id") or "").strip()
    if not provider_subscription_id and not external_reference and not fallback_subscription:
        raise ValueError("Nao foi possivel identificar a assinatura retornada pelo provider.")

    subscription = fallback_subscription
    if not subscription and provider_subscription_id:
        subscription = (
            AssinaturaUsuario.objects.select_related("usuario", "produto", "plano")
            .filter(provider=AssinaturaUsuario.Provider.STRIPE, provider_subscription_id=provider_subscription_id)
            .order_by("-updated_at", "-created_at")
            .first()
        )
    if not subscription and external_reference:
        subscription = (
            AssinaturaUsuario.objects.select_related("usuario", "produto", "plano")
            .filter(provider=AssinaturaUsuario.Provider.STRIPE, external_reference=external_reference)
            .order_by("-updated_at", "-created_at")
            .first()
        )
    if not subscription:
        raise ValueError("A assinatura recebida do Stripe nao foi encontrada no sistema.")

    item = (((remote_payload.get("items") or {}).get("data") or [{}])[0]) if isinstance(remote_payload.get("items"), dict) else {}
    price = item.get("price") or {}
    recurring = price.get("recurring") or {}
    subscription.provider_subscription_id = provider_subscription_id or subscription.provider_subscription_id
    subscription.provider_plan_id = str(price.get("id") or subscription.provider_plan_id or "").strip()
    subscription.external_reference = external_reference or subscription.external_reference
    subscription.provider_customer_id = str(remote_payload.get("customer") or subscription.provider_customer_id or "").strip()
    subscription.status = _subscription_status_from_provider(remote_payload.get("status"))
    subscription.billing_interval = _billing_interval_from_remote(recurring.get("interval"), subscription.billing_interval)
    unit_amount = price.get("unit_amount_decimal") or price.get("unit_amount")
    subscription.preco_ciclo = Decimal(str(unit_amount or 0)) / Decimal("100") if unit_amount is not None else subscription.preco_ciclo
    subscription.moeda = str(price.get("currency") or subscription.moeda or "BRL").strip().upper() or "BRL"
    subscription.current_period_start = _from_unix_timestamp(remote_payload.get("current_period_start")) or subscription.current_period_start
    subscription.current_period_end = _from_unix_timestamp(remote_payload.get("current_period_end")) or subscription.current_period_end
    if subscription.status == AssinaturaUsuario.Status.CANCELED:
        subscription.canceled_at = _from_unix_timestamp(remote_payload.get("canceled_at")) or timezone.now()
        subscription.expires_at = _from_unix_timestamp(remote_payload.get("ended_at")) or subscription.canceled_at
    elif subscription.status == AssinaturaUsuario.Status.EXPIRED:
        subscription.expires_at = _from_unix_timestamp(remote_payload.get("ended_at")) or timezone.now()
    else:
        subscription.canceled_at = None
        subscription.expires_at = _from_unix_timestamp(remote_payload.get("cancel_at")) if remote_payload.get("cancel_at_period_end") else None
    subscription.observacao = f"Assinatura sincronizada com Stripe ({remote_payload.get('status') or 'sem status'})."
    subscription.save()
    _sync_access_from_subscription(subscription)
    return subscription


def process_provider_webhook_payload(payload):
    config = payment_config()
    event_type = str(payload.get("type") or "").strip()
    event_object = payload.get("data", {}).get("object") or {}
    if event_type == "checkout.session.completed":
        provider_subscription_id = str(event_object.get("subscription") or "").strip()
        fallback_subscription = None
        external_reference = str(event_object.get("client_reference_id") or (event_object.get("metadata") or {}).get("external_reference") or "").strip()
        if external_reference:
            fallback_subscription = (
                AssinaturaUsuario.objects.select_related("usuario", "produto", "plano")
                .filter(provider=AssinaturaUsuario.Provider.STRIPE, external_reference=external_reference)
                .order_by("-updated_at", "-created_at")
                .first()
            )
        if provider_subscription_id:
            remote_payload = fetch_provider_subscription(config, provider_subscription_id)
            return reconcile_provider_subscription(remote_payload, fallback_subscription=fallback_subscription)
        return fallback_subscription
    if event_type.startswith("customer.subscription."):
        return reconcile_provider_subscription(event_object)
    if event_type.startswith("invoice."):
        provider_subscription_id = str(event_object.get("subscription") or "").strip()
        if not provider_subscription_id:
            return None
        remote_payload = fetch_provider_subscription(config, provider_subscription_id)
        return reconcile_provider_subscription(remote_payload)
    return None


def validate_provider_webhook_signature(config, raw_body, signature_header=""):
    normalized_secret = (config.stripe_webhook_secret or "").strip()
    if not normalized_secret or not signature_header or not raw_body:
        return True

    parts = {}
    signatures = []
    for chunk in str(signature_header).split(","):
        if "=" not in chunk:
            continue
        key, value = chunk.split("=", 1)
        normalized_key = key.strip()
        normalized_value = value.strip()
        if normalized_key == "v1" and normalized_value:
            signatures.append(normalized_value)
        else:
            parts[normalized_key] = normalized_value

    ts = parts.get("t")
    if not ts or not signatures:
        return True

    payload_text = raw_body.decode("utf-8")
    manifest = f"{ts}.{payload_text}"
    expected = hmac.new(
        normalized_secret.encode("utf-8"),
        manifest.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
    return any(hmac.compare_digest(expected, signature) for signature in signatures)


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


def _normalize_import_module(module_code):
    normalized = (module_code or "").strip().upper()
    if normalized in {"IO", "IOS"}:
        return ConsumoImportacaoDiaria.Modulo.IO
    if normalized in {"IP", "IPS"}:
        return ConsumoImportacaoDiaria.Modulo.IP
    raise ValueError("Modulo de importacao invalido.")


def _module_import_label(module_code):
    return "planilhas de IO" if _normalize_import_module(module_code) == ConsumoImportacaoDiaria.Modulo.IO else "planilhas de IP"


def _trial_daily_import_limit(settings_obj, module_code):
    module_key = _normalize_import_module(module_code)
    if module_key == ConsumoImportacaoDiaria.Modulo.IO:
        return max(1, int(getattr(settings_obj, "trial_daily_io_import_limit", DEFAULT_DAILY_IO_IMPORT_LIMIT) or DEFAULT_DAILY_IO_IMPORT_LIMIT))
    return max(1, int(getattr(settings_obj, "trial_daily_ip_import_limit", DEFAULT_DAILY_IP_IMPORT_LIMIT) or DEFAULT_DAILY_IP_IMPORT_LIMIT))


def _plan_daily_import_limit(plan, module_code):
    if not plan:
        return None
    module_key = _normalize_import_module(module_code)
    if plan.codigo == PROFESSIONAL_PLAN_CODE:
        return None
    if module_key == ConsumoImportacaoDiaria.Modulo.IO:
        fallback = DEFAULT_DAILY_IO_IMPORT_LIMIT if plan.codigo == STARTER_PLAN_CODE else None
        return int(plan.daily_io_import_limit if plan.daily_io_import_limit is not None else fallback) if fallback or plan.daily_io_import_limit is not None else None
    fallback = DEFAULT_DAILY_IP_IMPORT_LIMIT if plan.codigo == STARTER_PLAN_CODE else None
    return int(plan.daily_ip_import_limit if plan.daily_ip_import_limit is not None else fallback) if fallback or plan.daily_ip_import_limit is not None else None


def _usage_reference_date(moment=None):
    return timezone.localdate(moment or timezone.now())


def count_successful_imports_today(user, module_code, product_code=DOCUMENTATION_PRODUCT_CODE, moment=None):
    product = product_by_code(product_code)
    if not product or not user or not getattr(user, "is_authenticated", False):
        return 0
    module_key = _normalize_import_module(module_code)
    reference_date = _usage_reference_date(moment)
    usage = ConsumoImportacaoDiaria.objects.filter(
        usuario=user,
        produto=product,
        modulo=module_key,
        referencia_data=reference_date,
    ).first()
    return int(usage.importacoes_bem_sucedidas if usage else 0)


def resolve_import_quota(user, module_code, product_code=DOCUMENTATION_PRODUCT_CODE, moment=None):
    ensure_billing_catalog()
    product = product_by_code(product_code)
    entitlement = resolve_entitlement(user, product_code)
    module_key = _normalize_import_module(module_code)
    settings_obj = payment_config()
    limit = None
    source = "unlimited"
    current_plan = entitlement.get("current_plan")

    if entitlement.get("status") == "trial_active":
        limit = _trial_daily_import_limit(settings_obj, module_key)
        source = "trial"
    elif current_plan and current_plan.codigo == STARTER_PLAN_CODE and entitlement.get("status") == "plan_active":
        limit = _plan_daily_import_limit(current_plan, module_key)
        source = "starter"

    used = count_successful_imports_today(user, module_key, product_code=product_code, moment=moment)
    remaining = None if limit is None else max(limit - used, 0)
    return {
        "product": product,
        "module": module_key,
        "source": source,
        "limit": limit,
        "used": used,
        "remaining": remaining,
        "enforced": limit is not None,
        "label": _module_import_label(module_key),
        "entitlement": entitlement,
    }


def import_quota_error_message(user, module_code, product_code=DOCUMENTATION_PRODUCT_CODE, moment=None):
    quota = resolve_import_quota(user, module_code, product_code=product_code, moment=moment)
    if not quota["enforced"]:
        return ""
    plan_label = "Seu trial" if quota["source"] == "trial" else "O plano Iniciante"
    return (
        f"{plan_label} permite ate {quota['limit']} importacoes concluidas de {quota['label']} por dia. "
        f"Hoje voce ja concluiu {quota['used']}."
    )


def register_successful_import_usage(user, module_code, product_code=DOCUMENTATION_PRODUCT_CODE, moment=None):
    quota = resolve_import_quota(user, module_code, product_code=product_code, moment=moment)
    if not quota["enforced"]:
        return quota

    product = quota["product"]
    reference_date = _usage_reference_date(moment)
    usage, _ = ConsumoImportacaoDiaria.objects.select_for_update().get_or_create(
        usuario=user,
        produto=product,
        modulo=quota["module"],
        referencia_data=reference_date,
        defaults={"importacoes_bem_sucedidas": 0},
    )
    if usage.importacoes_bem_sucedidas >= quota["limit"]:
        raise ValueError(import_quota_error_message(user, module_code, product_code=product_code, moment=moment))
    usage.importacoes_bem_sucedidas += 1
    usage.save(update_fields=["importacoes_bem_sucedidas", "atualizado_em"])
    quota["used"] = usage.importacoes_bem_sucedidas
    quota["remaining"] = max(quota["limit"] - usage.importacoes_bem_sucedidas, 0)
    return quota


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
    if not product or not plan:
        return None, "O produto ou o plano profissional ainda nao foi configurado."
    if not getattr(user, "email", "").strip():
        return None, "Informe um e-mail valido na sua conta antes de iniciar a assinatura."
    now = timezone.now()
    price = plan.preco_anual if billing_interval == AssinaturaUsuario.BillingInterval.YEARLY else plan.preco_mensal
    provider_plan_code = _provider_plan_code(plan, billing_interval)
    if not provider_plan_code and (price is None or Decimal(str(price)) <= 0):
        return None, "Defina o valor do plano profissional antes de iniciar o checkout."
    subscription, _ = AssinaturaUsuario.objects.update_or_create(
        usuario=user,
        produto=product,
        defaults={
            "plano": plan,
            "provider": AssinaturaUsuario.Provider.STRIPE,
            "status": AssinaturaUsuario.Status.PENDING,
            "billing_interval": billing_interval,
            "auto_renew": True,
            "preco_ciclo": price,
            "moeda": "BRL",
            "current_period_start": now,
            "current_period_end": None,
            "provider_customer_id": "",
            "provider_subscription_id": "",
            "provider_plan_id": provider_plan_code,
            "external_reference": "",
            "checkout_url": "",
            "observacao": "Aguardando conclusao do checkout profissional.",
        },
    )
    if not config.enabled or not (config.stripe_secret_key or "").strip():
        return subscription, "A integracao Stripe ainda nao foi configurada no painel administrativo."
    subscription.external_reference = _subscription_external_reference(subscription)
    request_payload = _build_professional_checkout_payload(subscription, config)
    try:
        response_payload = _stripe_api_request(
            config,
            "POST",
            "/checkout/sessions",
            request_payload,
            timeout=60,
            idempotency_key=subscription.external_reference,
        )
    except ValueError as exc:
        subscription.observacao = f"Falha ao iniciar checkout profissional: {exc}"
        subscription.save(update_fields=["observacao", "updated_at"])
        return subscription, "Nao foi possivel iniciar o checkout do plano profissional."

    subscription.provider_customer_id = str(response_payload.get("customer") or "").strip()
    subscription.checkout_url = _resolve_checkout_url(response_payload)
    subscription.status = AssinaturaUsuario.Status.PENDING
    subscription.observacao = "Checkout profissional iniciado no Stripe."
    subscription.save(
        update_fields=[
            "external_reference",
            "provider_customer_id",
            "checkout_url",
            "status",
            "observacao",
            "updated_at",
        ]
    )
    if not subscription.checkout_url:
        return subscription, "O Stripe nao retornou a URL do checkout da assinatura."
    return subscription, ""
