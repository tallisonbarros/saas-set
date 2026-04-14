import calendar
import hmac
import json
import logging
import os
import ipaddress
import re
import subprocess
import sys
from pathlib import Path
from io import BytesIO
from datetime import date, datetime, timedelta
from decimal import Decimal, InvalidOperation

from django.contrib.auth import authenticate, login, logout
from django.contrib.auth.decorators import login_required
from django.conf import settings
from django.core.paginator import Paginator
from django.http import HttpResponse, HttpResponseForbidden, HttpResponseNotAllowed, JsonResponse
from django.shortcuts import get_object_or_404, redirect, render
from django.template.loader import render_to_string
from django.urls import reverse
from django.contrib.staticfiles import finders
from urllib.parse import urlencode
from django.utils import timezone
from django.utils.http import url_has_allowed_host_and_scheme
from django.views.decorators.http import require_POST
from django.views.decorators.csrf import csrf_exempt
from django.utils import timezone

from django.contrib.auth.models import User
from django.db import DatabaseError, connections, transaction
from django.db.models import Case, Count, DecimalField, F, IntegerField, Max, Min, OuterRef, Q, Subquery, Sum, TextField, Value, When
from django.db.models.expressions import ExpressionWrapper
from django.db.models.functions import Cast, Coalesce

from .forms import RegisterForm, TipoPerfilCreateForm, UserCreateForm
from .models import (
    CanalRackIO,
    CategoriaCompra,
    Caderno,
    ConfiguracaoPagamento,
    CentroCusto,
    PerfilUsuario,
    Compra,
    CompraItem,
    AssinaturaUsuario,
    EventoPagamentoWebhook,
    GrupoRackIO,
    IPImportJob,
    IPImportSettings,
    LocalRackIO,
    ModuloIO,
    ModuloAcesso,
    ModuloRackIO,
    FinanceiroID,
    Inventario,
    InventarioID,
    ListaIP,
    ListaIPID,
    ListaIPItem,
    App,
    AcessoProdutoUsuario,
    IngestRecord,
    IngestErrorLog,
    IngestRule,
    IOImportJob,
    IOImportSettings,
    AdminAccessLog,
    PlanoComercial,
    ProdutoPlataforma,
    SystemConfiguration,
    Radar,
    RadarAtividade,
    RadarAtividadeColaborador,
    RadarAtividadeDiaExecucao,
    RadarColaborador,
    RadarClassificacao,
    RadarContrato,
    RadarID,
    RadarTrabalho,
    RadarTrabalhoColaborador,
    RadarTrabalhoObservacao,
    PlantaIO,
    Proposta,
    PropostaAnexo,
    RackIO,
    RackSlotIO,
    TipoCompra,
    TipoCanalIO,
    TipoPerfil,
    Ativo,
    AtivoItem,
    TipoAtivo,
)
from .services.billing import (
    DOCUMENTATION_PRODUCT_CODE,
    activate_starter_plan,
    activate_trial,
    count_user_racks,
    ensure_billing_catalog,
    payment_config,
    resolve_entitlement,
    start_professional_checkout,
)
from .services.io_import import (
    DEFAULT_GROUPING_PROMPT,
    DEFAULT_HEADER_PROMPT,
    IOImportError,
    apply_import_job,
    build_file_sha256,
    reprocess_import_job,
    serialize_module_catalog,
)
from .services.ip_import import (
    DEFAULT_GROUPING_PROMPT as DEFAULT_IP_HEADER_GROUPING_PROMPT,
    DEFAULT_HEADER_PROMPT as DEFAULT_IP_HEADER_PROMPT,
    IPImportError,
    apply_import_job as apply_ip_import_job,
    build_file_sha256 as build_ip_file_sha256,
    reprocess_import_job as reprocess_ip_import_job,
)
from .access_control import (
    TRIAL_DURATION_DAYS,
    can_access_internal_module,
    get_user_product_access,
    has_tipo_code,
    resolve_commercial_product_code,
    user_has_product_access,
    visible_internal_module_codes,
)

logger = logging.getLogger(__name__)
ADMIN_PRIVILEGED_TIPOS = {"MASTER", "DEV"}


def _build_module_signal_badges(user, cliente):
    """
    Module Signal Badges
    Estrutura padrão para selos em cards do painel:
    - visible: bool
    - count: int
    - label: str
    - tone: warning|info|success
    """
    propostas_aprovar_count = _pendencias_total(user, cliente)

    if _is_admin_user(user) and not cliente:
        ios_count = RackIO.objects.count()
        listas_ip_count = ListaIP.objects.count()
        radar_count = Radar.objects.count()
    elif cliente:
        ios_count = RackIO.objects.filter(cliente=cliente).count()
        listas_ip_count = (
            ListaIP.objects.filter(Q(cliente=cliente) | Q(id_listaip__in=cliente.listas_ip.all()))
            .distinct()
            .count()
        )
        radar_count = (
            Radar.objects.filter(Q(cliente=cliente) | Q(id_radar__in=cliente.radares.all()))
            .distinct()
            .count()
        )
    else:
        ios_count = 0
        listas_ip_count = 0
        radar_count = 0
    documentation_entitlement = resolve_entitlement(user, DOCUMENTATION_PRODUCT_CODE)
    commercial_badge_label = documentation_entitlement.get("badge_label") or ""
    commercial_badge_tone = documentation_entitlement.get("badge_tone") or "info"
    badges = {
        "propostas": {
            "count": propostas_aprovar_count,
            "visible": propostas_aprovar_count > 0,
            "label": f"{propostas_aprovar_count} proposta{'s' if propostas_aprovar_count != 1 else ''} pra aprovar",
            "tone": "warning",
        },
        "financeiro": {"count": 0, "visible": False, "label": "", "tone": "info"},
        "ios": {
            "count": ios_count,
            "visible": ios_count > 0,
            "label": f"{ios_count} rack{'s' if ios_count != 1 else ''}",
            "tone": "info",
            "commercial_label": commercial_badge_label,
            "commercial_tone": commercial_badge_tone,
        },
        "inventarios": {"count": 0, "visible": False, "label": "", "tone": "info"},
        "listas_ip": {
            "count": listas_ip_count,
            "visible": listas_ip_count > 0,
            "label": f"{listas_ip_count} lista{'s' if listas_ip_count != 1 else ''}",
            "tone": "info",
            "commercial_label": commercial_badge_label,
            "commercial_tone": commercial_badge_tone,
        },
        "radar": {
            "count": radar_count,
            "visible": radar_count > 0,
            "label": f"{radar_count} radar{'es' if radar_count != 1 else ''}",
            "tone": "info",
        },
        "nuvem_projetos": {"count": 0, "visible": False, "label": "", "tone": "info"},
        "planta_conectada": {"count": 0, "visible": False, "label": "", "tone": "info"},
    }
    return badges

def _parse_bearer_token(auth_header):
    if not auth_header:
        return ""
    parts = auth_header.strip().split()
    if len(parts) < 2 or parts[0].lower() != "bearer":
        return ""
    return " ".join(parts[1:]).strip()


def _validate_payload_by_source(source, payload_data, rules_by_source):
    source_key = (source or "").strip().lower()
    rules = rules_by_source.get(source_key)
    if rules is None:
        return False, "unknown_source"
    if not isinstance(payload_data, dict):
        return False, "payload_not_object"
    payload_keys = {str(key).strip().lower() for key in payload_data.keys()}
    missing = [key for key in rules if str(key).strip().lower() not in payload_keys]
    if missing:
        return False, f"missing:{','.join(missing)}"
    return True, None


def _log_ingest_error(error, item=None, raw_body=None):
    try:
        item = item or {}
        IngestErrorLog.objects.create(
            source_id=str(item.get("source_id", "")).strip(),
            client_id=str(item.get("client_id", "")).strip(),
            agent_id=str(item.get("agent_id", "")).strip(),
            source=str(item.get("source", "")).strip(),
            error=error,
            raw_payload=item if isinstance(item, dict) else None,
            raw_body=raw_body or "",
        )
    except Exception:
        logger.exception("Failed to log ingest error")

def _normalize_required_fields(required_fields):
    normalized = []
    seen = set()
    for item in required_fields or []:
        value = str(item).strip()
        if not value:
            continue
        key = value.lower()
        if key in seen:
            continue
        seen.add(key)
        normalized.append(value)
    return normalized


def _upsert_ingest_items(items_by_source):
    if not items_by_source:
        return 0
    to_create = [
        IngestRecord(source_id=source_id, **data)
        for source_id, data in items_by_source.items()
    ]
    if to_create:
        IngestRecord.objects.bulk_create(to_create, ignore_conflicts=True)
    records = IngestRecord.objects.filter(source_id__in=items_by_source.keys())
    to_update = []
    now = timezone.now()
    for record in records:
        data = items_by_source.get(record.source_id)
        if not data:
            continue
        record.client_id = data["client_id"]
        record.agent_id = data["agent_id"]
        record.source = data["source"]
        record.payload = data["payload"]
        record.updated_at = now
        to_update.append(record)
    if to_update:
        IngestRecord.objects.bulk_update(
            to_update,
            ["client_id", "agent_id", "source", "payload", "updated_at"],
        )
    return len(items_by_source)


def _is_partial_request(request):
    return request.headers.get("X-Requested-With") == "XMLHttpRequest" or request.GET.get("partial") == "1"


def _paginate_queryset(request, qs, per_page=15):
    page_number = request.GET.get("page", 1)
    paginator = Paginator(qs, per_page)
    return paginator.get_page(page_number)


def _clean_tag_prefix(value):
    value = re.sub(r"[^0-9A-Za-z]", "", (value or "").strip().upper())
    return value[:3] if value else ""


def _clean_app_slug(value):
    value = re.sub(r"[^0-9A-Za-z_-]", "", (value or "").strip().lower())
    value = value.replace(" ", "_")
    return value[:60]


def _extract_app_ingest_fields(data):
    return {
        "ingest_client_id": (data.get("ingest_client_id", "") or "").strip(),
        "ingest_agent_id": (data.get("ingest_agent_id", "") or "").strip(),
        "ingest_source": (data.get("ingest_source", "") or "").strip(),
    }


def _tipo_prefix(tipo, fallback=None):
    if hasattr(tipo, "codigo") and tipo.codigo:
        cleaned = _clean_tag_prefix(tipo.codigo)
    elif hasattr(tipo, "nome") and tipo.nome:
        cleaned = _clean_tag_prefix(tipo.nome)
    else:
        cleaned = _clean_tag_prefix(tipo)
    if cleaned:
        return cleaned
    if fallback:
        return fallback
    return "ATV"


def _inventario_prefix(inventario):
    if inventario.nome:
        base = _clean_tag_prefix(inventario.nome)
        if base:
            return base
    if inventario.id_inventario:
        base = _clean_tag_prefix(inventario.id_inventario.codigo)
        if base:
            return base
    return "INV"


def _next_tagset_for_ativos(inventario, prefix, tipo=None):
    last_num = 0
    ativos = Ativo.objects.filter(inventario=inventario, tag_set__startswith=prefix)
    if tipo:
        ativos = ativos.filter(tipo=tipo)
    tags = ativos.values_list("tag_set", flat=True)
    pattern = re.compile(rf"^{re.escape(prefix)}(\d+)$")
    for tag in tags:
        match = pattern.match(tag or "")
        if not match:
            continue
        try:
            last_num = max(last_num, int(match.group(1)))
        except ValueError:
            continue
    return f"{prefix}{last_num + 1:02d}"


def _next_tagset_for_itens(ativo, prefix, tipo=None):
    if not ativo:
        return f"{prefix}01"
    last_num = 0
    itens = AtivoItem.objects.filter(ativo=ativo, tag_set__contains=prefix)
    if tipo:
        itens = itens.filter(tipo=tipo)
    tags = itens.values_list("tag_set", flat=True)
    pattern = re.compile(rf"(?:^|-)({re.escape(prefix)})(\d+)$")
    for tag in tags:
        match = pattern.search(tag or "")
        if not match:
            continue
        try:
            last_num = max(last_num, int(match.group(2)))
        except ValueError:
            continue
    return f"{prefix}{last_num + 1:02d}"


def _generate_tagset(inventario, tipo, setor, target, fallback_tipo=None, ativo=None):
    pattern = inventario.tagset_pattern or Inventario.TagsetPattern.TIPO_SEQ
    fallback_prefix = _tipo_prefix(fallback_tipo) if fallback_tipo else None
    tipo_prefix = _tipo_prefix(tipo, fallback=fallback_prefix)
    tipo_for_count = tipo or fallback_tipo
    if pattern == Inventario.TagsetPattern.SETORIZADO:
        setor_prefix = _clean_tag_prefix(setor) or _inventario_prefix(inventario)
        base = f"{setor_prefix}{tipo_prefix}"
    elif pattern == Inventario.TagsetPattern.INVENTARIO:
        base = f"{_inventario_prefix(inventario)}{tipo_prefix}"
    else:
        base = tipo_prefix
    if target == "ativo":
        return _next_tagset_for_ativos(inventario, base, tipo=tipo_for_count)
    if not ativo:
        return _next_tagset_for_itens(None, base, tipo=tipo_for_count)
    return _next_tagset_for_itens(ativo, base, tipo=tipo_for_count)


def _sync_ativo_status(ativo):
    stats = ativo.itens.aggregate(
        total_count=Count("id"),
        comissionado_count=Count("id", filter=Q(comissionado=True)),
        manutencao_count=Count("id", filter=Q(em_manutencao=True)),
    )
    total = stats["total_count"]
    new_comissionado = total > 0 and stats["comissionado_count"] == total
    new_manutencao = stats["manutencao_count"] > 0
    update_fields = []
    if new_comissionado != ativo.comissionado:
        ativo.comissionado = new_comissionado
        ativo.comissionado_em = timezone.now() if new_comissionado else None
        ativo.comissionado_por = None
        update_fields.extend(["comissionado", "comissionado_em", "comissionado_por"])
    if new_manutencao != ativo.em_manutencao:
        ativo.em_manutencao = new_manutencao
        ativo.manutencao_em = timezone.now() if new_manutencao else None
        ativo.manutencao_por = None
        update_fields.extend(["em_manutencao", "manutencao_em", "manutencao_por"])
    if update_fields:
        ativo.save(update_fields=update_fields)


def _get_cliente(user):
    try:
        return user.perfilusuario
    except PerfilUsuario.DoesNotExist:
        email = (user.email or user.username or "").strip().lower()
        if not email:
            return None
        return PerfilUsuario.objects.filter(email__iexact=email).first()


def _cliente_has_admin_privileges(cliente):
    if not cliente:
        return False
    nomes = ((nome or "").strip().upper() for nome in cliente.tipos.values_list("nome", flat=True))
    return any(nome in ADMIN_PRIVILEGED_TIPOS for nome in nomes)


def _is_admin_user(user):
    if not user or not user.is_authenticated:
        return False
    if user.is_superuser or user.is_staff:
        return True
    cliente = _get_cliente(user)
    return _cliente_has_admin_privileges(cliente)


def _is_dev_user(user):
    if not user or not user.is_authenticated:
        return False
    if user.is_superuser or user.is_staff:
        return True
    cliente = _get_cliente(user)
    if not cliente:
        return False
    return cliente.tipos.filter(nome__iexact="DEV").exists()


def _can_user_view_inactive_apps(user):
    return _is_dev_user(user)


def _filter_visible_apps_queryset(qs, user):
    if _can_user_view_inactive_apps(user):
        return qs
    return qs.filter(ativo=True)


def _get_app_by_slug_for_user(slug, user):
    qs = _filter_visible_apps_queryset(App.objects.filter(slug=slug), user)
    return get_object_or_404(qs)


def _user_has_app_access(user, app):
    if _is_admin_user(user):
        return True
    cliente = _get_cliente(user)
    return bool(cliente) and cliente.apps.filter(pk=app.pk).exists()


def _user_role(user):
    if _is_admin_user(user):
        return "ADMIN"
    cliente = _get_cliente(user)
    if not cliente:
        return "CLIENTE"
    has_cliente = cliente.tipos.filter(nome__iexact="Contratante").exists() or cliente.tipos.filter(
        nome__iexact="Cliente"
    ).exists()
    has_financeiro = cliente.tipos.filter(nome__iexact="Financeiro").exists()
    has_vendedor = cliente.tipos.filter(nome__iexact="Vendedor").exists()
    if has_cliente:
        return "CLIENTE"
    if has_financeiro:
        return "FINANCEIRO"
    if has_vendedor:
        return "VENDEDOR"
    return "CLIENTE"


def _has_tipo(user, nome):
    cliente = _get_cliente(user)
    if not cliente:
        return False
    return cliente.tipos.filter(nome__iexact=nome).exists()


def _has_tipo_any(user, nomes):
    cliente = _get_cliente(user)
    if not cliente:
        return False
    return cliente.tipos.filter(nome__in=nomes).exists()


def _get_safe_next_url(request, fallback="painel"):
    next_url = (request.POST.get("next") or request.GET.get("next") or "").strip()
    if next_url and url_has_allowed_host_and_scheme(
        next_url,
        allowed_hosts={request.get_host()},
        require_https=request.is_secure(),
    ):
        return next_url
    return reverse(fallback)


def _documentacao_tecnica_product():
    try:
        return ensure_billing_catalog()
    except DatabaseError:
        return None


def _documentacao_tecnica_entitlement_fallback():
    return {
        "product": None,
        "starter_plan": None,
        "professional_plan": None,
        "subscription": None,
        "access": None,
        "current_plan": None,
        "status": "disponivel",
        "has_access": False,
        "requires_plan_selection": False,
        "trial_days_remaining": None,
        "rack_count": 0,
        "starter_limit": 0,
        "starter_available": True,
        "starter_excess": 0,
        "badge_label": "",
        "badge_tone": "info",
        "legacy_manual_access": False,
    }


def _resolve_documentacao_entitlement_safe(user):
    try:
        return resolve_entitlement(user, DOCUMENTATION_PRODUCT_CODE)
    except DatabaseError:
        return _documentacao_tecnica_entitlement_fallback()


def _redirect_documentacao_tecnica_billing_unavailable(next_url=None):
    query = {}
    if next_url:
        query["next"] = next_url
    response = redirect(
        f"{reverse('produto_documentacao_tecnica')}{f'?{urlencode(query)}' if query else ''}"
    )
    response.set_cookie(
        "product_checkout_notice",
        "Recursos comerciais indisponiveis neste ambiente atual.",
        max_age=20,
    )
    return response


def _documentacao_tecnica_entry_url():
    return reverse("ios_list")


def _platform_plans_url(next_url=None, state=None):
    query = {}
    if next_url:
        query["next"] = next_url
    if state:
        query["state"] = state
    return f"{reverse('produtos_planos')}{f'?{urlencode(query)}' if query else ''}"


def _documentacao_tecnica_plans_url(next_url=None, state=None):
    return _platform_plans_url(next_url=next_url, state=state)


def _payment_checkout_default_urls(request):
    return {
        "success": request.build_absolute_uri(reverse("pagamento_checkout_sucesso")),
        "failure": request.build_absolute_uri(reverse("pagamento_checkout_falha")),
        "pending": request.build_absolute_uri(reverse("pagamento_checkout_pendente")),
    }


def _ensure_payment_checkout_urls(request, settings_obj):
    defaults = _payment_checkout_default_urls(request)
    updated_fields = []
    if not (settings_obj.checkout_success_url or "").strip():
        settings_obj.checkout_success_url = defaults["success"]
        updated_fields.append("checkout_success_url")
    if not (settings_obj.checkout_failure_url or "").strip():
        settings_obj.checkout_failure_url = defaults["failure"]
        updated_fields.append("checkout_failure_url")
    if not (settings_obj.checkout_pending_url or "").strip():
        settings_obj.checkout_pending_url = defaults["pending"]
        updated_fields.append("checkout_pending_url")
    if updated_fields:
        settings_obj.save(update_fields=updated_fields + ["updated_at"])
    return defaults


def _parse_local_date_boundary(value, end=False):
    raw = (value or "").strip()
    if not raw:
        return None
    try:
        parsed = date.fromisoformat(raw)
    except ValueError:
        return None
    clock = datetime.max.time().replace(microsecond=0) if end else datetime.min.time()
    combined = datetime.combine(parsed, clock)
    if timezone.is_naive(combined):
        combined = timezone.make_aware(combined, timezone.get_current_timezone())
    return combined


def _user_documentacao_access_state(user):
    entitlement = _resolve_documentacao_entitlement_safe(user)
    subscription = entitlement.get("subscription")
    access = entitlement.get("access")
    status_map = {
        "trial_active": "trial_ativo",
        "plan_active": "ativo",
        "legacy_active": "ativo",
        "trial_expired": "expirado",
        "blocked": "bloqueado",
        "starter_blocked_by_usage": "starter_bloqueado",
        "requires_plan_selection": "disponivel",
        "admin_access": "ativo",
        "anonymous": "disponivel",
    }
    return status_map.get(entitlement.get("status"), "disponivel"), subscription or access


def _build_product_access_rows(user):
    products = list(ProdutoPlataforma.objects.order_by("nome"))
    accesses = {
        access.produto_id: access
        for access in AcessoProdutoUsuario.objects.select_related("produto").filter(usuario=user)
    }
    rows = []
    for product in products:
        rows.append({"product": product, "access": accesses.get(product.id)})
    return rows


def _require_internal_module_access(request, module_code):
    product_code = resolve_commercial_product_code(module_code)
    if product_code:
        entitlement = resolve_entitlement(request.user, product_code)
        if entitlement.get("has_access"):
            return None
        state, _ = _user_documentacao_access_state(request.user)
        return redirect(_documentacao_tecnica_plans_url(next_url=request.path, state=state))
    if can_access_internal_module(request.user, module_code):
        return None
    return HttpResponseForbidden("Sem permissao.")


def _documentacao_tecnica_plan_badge(user):
    entitlement = resolve_entitlement(user, DOCUMENTATION_PRODUCT_CODE)
    label = entitlement.get("badge_label") or ""
    if not label:
        return {"visible": False, "label": "", "tone": "info"}
    return {
        "visible": True,
        "label": label,
        "tone": entitlement.get("badge_tone") or "info",
    }


def _documentacao_tecnica_status_context(user):
    entitlement = resolve_entitlement(user, DOCUMENTATION_PRODUCT_CODE)
    return {
        "entitlement": entitlement,
        "plan_badge": _documentacao_tecnica_plan_badge(user),
    }


def _starter_limit_error_message(user, product_code=DOCUMENTATION_PRODUCT_CODE):
    entitlement = resolve_entitlement(user, product_code)
    return (
        f"O plano Iniciante permite ate {entitlement['starter_limit']} racks simultaneos. "
        f"Sua conta possui {entitlement['rack_count']} racks. "
        "Exclua racks para liberar este plano ou siga com o Profissional."
    )


def _can_create_more_racks(user, increment=1, product_code=DOCUMENTATION_PRODUCT_CODE):
    entitlement = resolve_entitlement(user, product_code)
    if not entitlement.get("has_access"):
        return False, "Escolha um plano para continuar usando o modulo."
    current_plan = entitlement.get("current_plan")
    if current_plan and current_plan.codigo == PlanoComercial.Codigo.STARTER:
        limit = entitlement["starter_limit"]
        if entitlement["rack_count"] + increment > limit:
            return False, _starter_limit_error_message(user, product_code=product_code)
    return True, ""


def _create_grupo_payload(request, cliente):
    if not cliente:
        return {
            "ok": False,
            "created": False,
            "id": None,
            "nome": None,
            "message": "Sem cadastro de cliente.",
            "level": "error",
        }
    nome = request.POST.get("grupo_nome", "").strip()
    if not nome:
        return {
            "ok": False,
            "created": False,
            "id": None,
            "nome": None,
            "message": "Informe um nome de grupo.",
            "level": "error",
        }
    grupo, created = GrupoRackIO.objects.get_or_create(nome=nome, cliente=cliente)
    return {
        "ok": True,
        "created": created,
        "id": grupo.id,
        "nome": grupo.nome,
        "message": "Grupo criado." if created else "Grupo ja existe.",
        "level": "success" if created else "warning",
    }


def _ensure_default_cadernos(cliente):
    if not cliente:
        return
    capex = Caderno.objects.filter(nome="CAPEX", criador=cliente).first()
    if not capex:
        Caderno.objects.create(nome="CAPEX", ativo=True, criador=cliente)
    opex = Caderno.objects.filter(nome="OPEX", criador=cliente).first()
    if not opex:
        Caderno.objects.create(nome="OPEX", ativo=True, criador=cliente)


def _financeiro_allowed_cadernos_qs(user, cliente):
    if _is_admin_user(user) and not cliente:
        return Caderno.objects.all()
    if not cliente:
        return Caderno.objects.none()
    return Caderno.objects.filter(
        Q(criador=cliente) | Q(id_financeiro__in=cliente.financeiros.all())
    ).distinct()


def _financeiro_allowed_compras_qs(user, cliente):
    if _is_admin_user(user) and not cliente:
        return Compra.objects.all()
    if not cliente:
        return Compra.objects.none()
    return Compra.objects.filter(
        Q(caderno__criador=cliente) | Q(caderno__id_financeiro__in=cliente.financeiros.all())
    ).distinct()


def _compra_status_label(compra):
    itens = list(compra.itens.all())
    if itens and all(item.pago for item in itens):
        return "Pago"
    return "Pendente"


def _add_months(base_date, months):
    month_index = (base_date.month - 1) + months
    year = base_date.year + (month_index // 12)
    month = (month_index % 12) + 1
    last_day = calendar.monthrange(year, month)[1]
    day = min(base_date.day, last_day)
    return date(year, month, day)


def _parse_parcela(value):
    value = (value or "").strip()
    if not value:
        return None
    if value == "1/-":
        return ("recorrente", None, None)
    if not re.match(r"^\d{1,5}/\d{1,5}$", value):
        return None
    num_str, den_str = value.split("/")
    try:
        num = int(num_str)
        den = int(den_str)
    except ValueError:
        return None
    if num < 1 or den < 1:
        return None
    return ("parcelado", num, den)


def _format_parcela(num, den):
    num_str = f"{num:02d}" if num < 100 else str(num)
    den_str = f"{den:02d}" if den < 100 else str(den)
    return f"{num_str}/{den_str}"


def _parcela_for_copy(value, offset):
    parsed = _parse_parcela(value)
    if not parsed:
        return value
    kind, num, den = parsed
    if kind == "recorrente":
        return "1/-"
    if num == 1 and den == 1:
        return None
    new_num = num + offset
    if new_num > den:
        return None
    return _format_parcela(new_num, den)


def _normalize_parcela(value, fallback):
    parsed = _parse_parcela(value)
    if not parsed:
        return fallback
    kind, num, den = parsed
    if kind == "recorrente":
        return "1/-"
    return _format_parcela(num, den)


def _is_parcela_valid(value):
    value = (value or "").strip()
    if not value:
        return True
    return bool(_parse_parcela(value))


def _normalize_channel_tag(value):
    value = (value or "").strip()
    if not value:
        return ""
    value = re.sub(r"\s+", "_", value)
    return value.upper()


def _ios_inventarios_queryset(user, cliente):
    if _is_admin_user(user) and not cliente:
        return Inventario.objects.all()
    return Inventario.objects.filter(Q(cliente=cliente) | Q(id_inventario__in=cliente.inventarios.all()))


def _ios_locais_grupos(cliente):
    locais = LocalRackIO.objects.none()
    grupos = GrupoRackIO.objects.none()
    if cliente:
        locais = LocalRackIO.objects.filter(cliente=cliente).order_by("nome")
        grupos = GrupoRackIO.objects.filter(cliente=cliente).order_by("nome")
    return locais, grupos


def _ios_racks_queryset(user, cliente):
    if _is_admin_user(user) and not cliente:
        racks = RackIO.objects.all()
    else:
        racks = RackIO.objects.filter(Q(cliente=cliente) | Q(id_planta__in=cliente.plantas.all()))
    return racks.select_related("inventario", "local", "grupo").annotate(
        ocupados=Count("slots", filter=Q(slots__modulo__isnull=False)),
        canais_total=Count("slots__modulo__canais", distinct=True),
        canais_comissionados=Count(
            "slots__modulo__canais",
            filter=Q(slots__modulo__canais__comissionado=True),
            distinct=True,
        ),
    )


def _ios_build_rack_groups(racks, locais=None):
    rack_groups = []
    grouped = {}
    for local in locais or []:
        local_name = (local.nome or "").strip()
        local_key = local_name.lower() if local_name else "__sem_local__"
        grouped.setdefault(
            local_key,
            {
                "local": local,
                "groups": {},
                "racks_sem_grupo": [],
            },
        )
    for rack in racks.order_by("local__nome", "grupo__nome", "inventario__nome", "nome"):
        rack.all_canais_comissionados = bool(rack.canais_total) and rack.canais_total == rack.canais_comissionados
        local_name = (rack.local.nome if rack.local_id and rack.local else "").strip()
        grupo_name = (rack.grupo.nome if rack.grupo_id and rack.grupo else "").strip()
        local_key = local_name.lower() if local_name else "__sem_local__"
        grupo_key = grupo_name.lower() if grupo_name else "__sem_grupo__"
        local_bucket = grouped.setdefault(
            local_key,
            {
                "local": rack.local if rack.local_id else None,
                "groups": {},
                "racks_sem_grupo": [],
            },
        )
        if rack.grupo_id:
            group_bucket = local_bucket["groups"].setdefault(
                grupo_key,
                {
                    "grupo": rack.grupo if rack.grupo_id else None,
                    "racks": [],
                },
            )
            group_bucket["racks"].append(rack)
        else:
            local_bucket["racks_sem_grupo"].append(rack)

    for _, local_data in grouped.items():
        groups = list(local_data["groups"].values())
        if local_data["racks_sem_grupo"]:
            groups.insert(
                0,
                {
                    "grupo": None,
                    "racks": local_data["racks_sem_grupo"],
                },
            )
        rack_groups.append(
            {
                "local": local_data["local"],
                "groups": groups,
            }
        )
    return rack_groups


def _ios_search_channels(racks, search_term="", rack_filter="", local_filter="", grupo_filter=""):
    search_term = (search_term or "").strip()
    rack_filter = (rack_filter or "").strip()
    local_filter = (local_filter or "").strip()
    grupo_filter = (grupo_filter or "").strip()
    if not (search_term or rack_filter or local_filter or grupo_filter):
        return []

    slot_pos_subquery = RackSlotIO.objects.filter(modulo_id=OuterRef("modulo_id")).values("posicao")[:1]
    search_filter = Q()
    if search_term:
        search_filter = (
            Q(tag__icontains=search_term)
            | Q(descricao__icontains=search_term)
            | Q(modulo__nome__icontains=search_term)
            | Q(modulo__modulo_modelo__nome__icontains=search_term)
            | Q(modulo__modulo_modelo__marca__icontains=search_term)
            | Q(modulo__modulo_modelo__modelo__icontains=search_term)
            | Q(modulo__rack__nome__icontains=search_term)
            | Q(modulo__rack__local__nome__icontains=search_term)
            | Q(modulo__rack__grupo__nome__icontains=search_term)
        )
        if search_term.isdigit():
            search_filter = search_filter | Q(indice=int(search_term))

    channels = CanalRackIO.objects.filter(modulo__rack__in=racks)
    if rack_filter.isdigit():
        channels = channels.filter(modulo__rack_id=int(rack_filter))
    if local_filter.isdigit():
        channels = channels.filter(modulo__rack__local_id=int(local_filter))
    if grupo_filter.isdigit():
        channels = channels.filter(modulo__rack__grupo_id=int(grupo_filter))
    if search_term:
        channels = channels.filter(search_filter)

    return list(
        channels.select_related("modulo", "modulo__rack", "modulo__modulo_modelo", "tipo")
        .annotate(slot_pos=Subquery(slot_pos_subquery))
        .order_by("modulo__rack__nome", "slot_pos", "indice")[:200]
    )


def _ios_search_payload(search_results):
    payload = []
    for channel in search_results:
        payload.append(
            {
                "rack": channel.modulo.rack.nome,
                "slot": f"S{channel.slot_pos}" if channel.slot_pos else "-",
                "modulo": channel.modulo.modulo_modelo.modelo or channel.modulo.modulo_modelo.nome,
                "canal": f"CH{channel.indice:02d}",
                "canal_tag": channel.tag or "-",
                "tipo": channel.tipo.nome,
                "local": channel.modulo.rack.local.nome if channel.modulo.rack.local_id else "-",
                "grupo": channel.modulo.rack.grupo.nome if channel.modulo.rack.grupo_id else "-",
                "url": _ios_module_panel_url(channel.modulo.rack_id, channel.modulo.id),
            }
        )
    return payload


def _ios_module_panel_url(rack_id, module_id):
    return f"{reverse('ios_rack_detail', kwargs={'pk': rack_id})}?module={module_id}#rack-module-panel"


def _ios_build_module_editor_data(slots, channel_types):
    module_editor_data = {}
    for slot in slots:
        if not slot.modulo_id:
            continue
        modulo = slot.modulo
        channels_payload = []
        for channel in modulo.canais.all():
            channels_payload.append(
                {
                    "id": channel.id,
                    "indice": channel.indice,
                    "tag": channel.tag or "",
                    "descricao": channel.descricao or "",
                    "tipo_id": channel.tipo_id,
                    "comissionado": bool(channel.comissionado),
                }
            )
        module_editor_data[str(modulo.id)] = {
            "id": modulo.id,
            "slot_id": slot.id,
            "slot_pos": slot.posicao,
            "nome": "",
            "display_name": modulo.modulo_modelo.modelo or modulo.modulo_modelo.nome,
            "model_name": modulo.modulo_modelo.modelo or modulo.modulo_modelo.nome,
            "type_name": modulo.modulo_modelo.tipo_base.nome if modulo.modulo_modelo.tipo_base_id else "",
            "brand": modulo.modulo_modelo.marca or "",
            "model": modulo.modulo_modelo.modelo or "",
            "channels": channels_payload,
            "all_canais_comissionados": bool(getattr(modulo, "all_canais_comissionados", False)),
        }
    return module_editor_data


def _ios_build_module_channels_summary(module_editor_data, channel_types_data):
    type_map = {str(item["id"]): item["nome"] for item in channel_types_data}
    payload = {}
    for module_id, module_info in module_editor_data.items():
        payload[module_id] = [
            {
                "canal": f"{int(channel['indice']):02d}",
                "tag": channel["tag"] or "-",
                "tipo": type_map.get(str(channel["tipo_id"]), "-"),
            }
            for channel in module_info.get("channels", [])
        ]
    return payload


def _io_import_module_catalog(cliente):
    modules_qs = ModuloIO.objects.filter(Q(cliente=cliente) | Q(is_default=True)).select_related("tipo_base")
    return modules_qs.order_by("modelo", "id")


def _io_import_can_manage(request, cliente):
    return bool(cliente or _is_admin_user(request.user))


def _is_ajax_request(request):
    return request.headers.get("X-Requested-With") == "XMLHttpRequest"


def _json_error_response(message, status=400, **extra):
    payload = {"ok": False, "message": message}
    payload.update({key: value for key, value in extra.items() if value is not None})
    return JsonResponse(payload, status=status)


def _io_import_user_message(user, detailed_message, generic_message=None):
    if _is_dev_user(user):
        return detailed_message
    return generic_message or "Nao foi possivel concluir a importacao de IO. Revise o arquivo e tente novamente."


def _initial_io_import_progress_payload(filename=""):
    return {
        "stage": "upload",
        "percent": 4,
        "title": "Arquivo recebido",
        "message": (
            f"{filename} foi recebido e a analise da planilha esta sendo iniciada."
            if filename
            else "O arquivo foi recebido e a analise da planilha esta sendo iniciada."
        ),
        "progress_label": "Arquivo recebido",
        "snapshots": [],
        "sheets_total": 0,
        "sheets_processed": 0,
    }


def _failed_io_import_progress_payload(message, existing_payload=None):
    payload = dict(existing_payload or {})
    payload.update(
        {
            "stage": payload.get("stage") or "preview",
            "title": "Analise interrompida",
            "message": "A analise nao conseguiu ser concluida. Revise o arquivo e tente novamente.",
        }
    )
    payload["failed"] = True
    return payload


def _save_io_import_progress(job, payload):
    progress_payload = dict(payload or {})
    IOImportJob.objects.filter(pk=job.pk).update(progress_payload=progress_payload, updated_at=timezone.now())
    job.progress_payload = progress_payload
    return progress_payload


def _build_io_import_status_progress(job):
    payload = dict(job.progress_payload or {})
    if not payload:
        payload = _initial_io_import_progress_payload(job.original_filename)
    stage = (payload.get("stage") or "upload").lower()
    percent = max(0, min(int(payload.get("percent") or 0), 100))
    stage_order = ["upload", "parse", "ai", "preview"]
    current_index = stage_order.index(stage) if stage in stage_order else 0
    steps = {}
    for index, step_name in enumerate(stage_order):
        if job.status in {IOImportJob.Status.REVIEW, IOImportJob.Status.APPLIED}:
            state = "done"
        elif job.status == IOImportJob.Status.FAILED:
            state = "done" if index < current_index else "active" if index == current_index else "idle"
        else:
            state = "done" if index < current_index else "active" if index == current_index else "idle"
        steps[step_name] = state
    payload["stage"] = stage
    payload["percent"] = percent
    payload["steps"] = steps
    payload["snapshots"] = list(payload.get("snapshots") or [])[:3]
    return payload


def _spawn_io_import_job_processor(job_id):
    manage_py = Path(settings.BASE_DIR) / "manage.py"
    env = os.environ.copy()
    env.setdefault("PYTHONUNBUFFERED", "1")
    subprocess.Popen(
        [sys.executable, str(manage_py), "process_io_import_job", str(job_id)],
        cwd=str(settings.BASE_DIR),
        env=env,
        start_new_session=True,
    )


def _spawn_io_import_job_processor_safe(job_id):
    try:
        _spawn_io_import_job_processor(job_id)
    except Exception as exc:
        logger.exception("Failed to spawn IO import background processor", extra={"job_id": job_id})
        job = IOImportJob.objects.filter(pk=job_id).first()
        if not job:
            return
        job.status = IOImportJob.Status.FAILED
        job.ai_status = IOImportJob.AIStatus.FAILED
        job.ai_error = str(exc)
        warnings = list(job.warnings or [])
        warnings.append(f"Falha interna ao iniciar o processamento em segundo plano: {exc}")
        job.warnings = warnings
        job.progress_payload = _failed_io_import_progress_payload(str(exc), job.progress_payload)
        job.save(update_fields=["status", "ai_status", "ai_error", "warnings", "progress_payload", "updated_at"])


def _io_import_settings():
    settings_obj = IOImportSettings.load()
    if not settings_obj.header_prompt:
        settings_obj.header_prompt = DEFAULT_HEADER_PROMPT
    if not settings_obj.grouping_prompt:
        settings_obj.grouping_prompt = DEFAULT_GROUPING_PROMPT
    return settings_obj


def _ip_import_can_manage(request, cliente):
    return bool(cliente or _is_admin_user(request.user))


def _ip_import_settings():
    settings_obj = IPImportSettings.load()
    if not settings_obj.header_prompt:
        settings_obj.header_prompt = DEFAULT_IP_HEADER_PROMPT
    if not settings_obj.grouping_prompt:
        settings_obj.grouping_prompt = DEFAULT_IP_HEADER_GROUPING_PROMPT
    return settings_obj


def _ip_import_user_message(user, detailed_message, generic_message=None):
    if _is_dev_user(user):
        return detailed_message
    return generic_message or "Nao foi possivel concluir a importacao da planilha de IP. Revise o arquivo e tente novamente."


def _initial_ip_import_progress_payload(filename=""):
    return {
        "stage": "upload",
        "percent": 4,
        "title": "Arquivo recebido",
        "message": (
            f"{filename} foi recebido e a analise da planilha de IP esta sendo iniciada."
            if filename
            else "O arquivo foi recebido e a analise da planilha de IP esta sendo iniciada."
        ),
        "progress_label": "Arquivo recebido",
        "snapshots": [],
        "sheets_total": 0,
        "sheets_processed": 0,
    }


def _failed_ip_import_progress_payload(message, existing_payload=None):
    payload = dict(existing_payload or {})
    payload.update(
        {
            "stage": payload.get("stage") or "preview",
            "title": "Analise interrompida",
            "message": "A analise nao conseguiu ser concluida. Revise o arquivo e tente novamente.",
        }
    )
    payload["failed"] = True
    return payload


def _save_ip_import_progress(job, payload):
    progress_payload = dict(payload or {})
    IPImportJob.objects.filter(pk=job.pk).update(progress_payload=progress_payload, updated_at=timezone.now())
    job.progress_payload = progress_payload
    return progress_payload


def _build_ip_import_status_progress(job):
    payload = dict(job.progress_payload or {})
    if not payload:
        payload = _initial_ip_import_progress_payload(job.original_filename)
    stage = (payload.get("stage") or "upload").lower()
    percent = max(0, min(int(payload.get("percent") or 0), 100))
    stage_order = ["upload", "parse", "ai", "preview"]
    current_index = stage_order.index(stage) if stage in stage_order else 0
    steps = {}
    for index, step_name in enumerate(stage_order):
        if job.status in {IPImportJob.Status.REVIEW, IPImportJob.Status.APPLIED}:
            state = "done"
        elif job.status == IPImportJob.Status.FAILED:
            state = "done" if index < current_index else "active" if index == current_index else "idle"
        else:
            state = "done" if index < current_index else "active" if index == current_index else "idle"
        steps[step_name] = state
    payload["stage"] = stage
    payload["percent"] = percent
    payload["steps"] = steps
    payload["snapshots"] = list(payload.get("snapshots") or [])[:6]
    return payload


def _spawn_ip_import_job_processor(job_id):
    manage_py = Path(settings.BASE_DIR) / "manage.py"
    env = os.environ.copy()
    env.setdefault("PYTHONUNBUFFERED", "1")
    subprocess.Popen(
        [sys.executable, str(manage_py), "process_ip_import_job", str(job_id)],
        cwd=str(settings.BASE_DIR),
        env=env,
        start_new_session=True,
    )


def _spawn_ip_import_job_processor_safe(job_id):
    try:
        _spawn_ip_import_job_processor(job_id)
    except Exception as exc:
        logger.exception("Failed to spawn IP import background processor", extra={"job_id": job_id})
        job = IPImportJob.objects.filter(pk=job_id).first()
        if not job:
            return
        job.status = IPImportJob.Status.FAILED
        job.ai_status = IPImportJob.AIStatus.FAILED
        job.ai_error = str(exc)
        warnings = list(job.warnings or [])
        warnings.append(f"Falha interna ao iniciar o processamento em segundo plano: {exc}")
        job.warnings = warnings
        job.progress_payload = _failed_ip_import_progress_payload(str(exc), job.progress_payload)
        job.save(update_fields=["status", "ai_status", "ai_error", "warnings", "progress_payload", "updated_at"])


def _build_ip_import_job_queryset(request, cliente):
    queryset = IPImportJob.objects.select_related("cliente", "created_by", "applied_lista")
    if _is_admin_user(request.user) and not cliente:
        return queryset
    if not cliente:
        return queryset.none()
    return queryset.filter(cliente=cliente)


def _reprocess_ip_import_job(job):
    settings_obj = _ip_import_settings()
    result = reprocess_ip_import_job(
        job=job,
        settings_obj=settings_obj,
        progress_callback=lambda payload: _save_ip_import_progress(job, payload),
    )
    job.file_format = result["file_format"]
    job.sheet_name = result["sheet_name"]
    job.header_row_index = result["header_row_index"]
    job.rows_total = result["rows_total"]
    job.rows_parsed = result["rows_parsed"]
    job.column_map = result["column_map"]
    job.extracted_payload = {"rows": result["normalized_rows"], "sheets": result.get("sheet_summaries") or []}
    job.proposal_payload = result["proposal"]
    job.warnings = result["warnings"]
    job.ai_status = result["ai_status"]
    job.ai_payload = result["ai_payload"]
    job.ai_error = result["ai_error"]
    job.ai_model = result["ai_model"]
    job.progress_payload = result.get("progress_payload") or job.progress_payload
    job.status = IPImportJob.Status.FAILED if result["proposal"].get("conflicts") else IPImportJob.Status.REVIEW
    job.save(
        update_fields=[
            "file_format",
            "sheet_name",
            "header_row_index",
            "rows_total",
            "rows_parsed",
            "column_map",
            "extracted_payload",
            "proposal_payload",
            "warnings",
            "ai_status",
            "ai_payload",
            "ai_error",
            "ai_model",
            "progress_payload",
            "status",
            "updated_at",
        ]
    )
    return job


def _build_ip_import_preview_lists(proposal, applied_list_key_map=None):
    preview_lists = []
    applied_list_key_map = dict(applied_list_key_map or {})
    for list_index, list_payload in enumerate((proposal or {}).get("lists") or [], start=1):
        list_key = str(list_payload.get("list_key") or f"list_{list_index}")
        applied_lista_id = applied_list_key_map.get(list_key)
        items = list_payload.get("preview_items") or []
        preview_lists.append(
            {
                "key": list_key,
                "name": list_payload.get("name") or f"Lista {list_index}",
                "description": list_payload.get("description") or "Preview da importacao com os enderecos sugeridos pela analise.",
                "id_listaip": list_payload.get("id_listaip") or "",
                "faixa_inicio": list_payload.get("faixa_inicio") or "-",
                "faixa_fim": list_payload.get("faixa_fim") or "-",
                "protocolo_padrao": list_payload.get("protocolo_padrao") or "",
                "total_ips": int(list_payload.get("total_ips") or len(list_payload.get("items") or [])),
                "filled_devices": int(list_payload.get("filled_devices") or 0),
                "source_sheets": list_payload.get("source_sheets") or [],
                "is_sparse": bool(list_payload.get("is_sparse")),
                "items": items,
                "extra_items": max(int(list_payload.get("total_ips") or len(list_payload.get("items") or [])) - len(items), 0),
                "is_applied": bool(applied_lista_id),
                "applied_lista_id": applied_lista_id,
            }
        )
    return preview_lists


def _build_io_import_preview_racks(proposal, applied_rack_key_map=None):
    preview_racks = []
    preview_payload = {}
    applied_rack_key_map = dict(applied_rack_key_map or {})
    for rack_index, rack_payload in enumerate((proposal or {}).get("racks") or [], start=1):
        rack_key = str(rack_payload.get("rack_key") or f"rack_{rack_index}")
        applied_rack_id = applied_rack_key_map.get(rack_key)
        modules = rack_payload.get("modules") or []
        slots_total = max(int(rack_payload.get("slots_total") or len(modules) or 1), 1)
        module_by_slot = {}
        canais_disponiveis_map = {}
        module_data = {}

        for module_index, module in enumerate(modules, start=1):
            slot_index = int(module.get("slot_index") or module_index)
            module_id = f"preview_{rack_key}_{slot_index}"
            module_type = (module.get("module_type") or "-").strip() or "-"
            channel_capacity = int(module.get("channel_capacity") or len(module.get("channels") or []))
            filled_channels = 0
            channel_summary = []
            for channel in module.get("channels") or []:
                tag = (channel.get("tag") or "").strip()
                channel_type = (channel.get("type") or module_type or "-").strip() or "-"
                if tag:
                    filled_channels += 1
                channel_summary.append(
                    {
                        "canal": f"{int(channel.get('index') or 0):02d}",
                        "tag": tag or "-",
                        "tipo": channel_type,
                        "descricao": (channel.get("description") or "").strip() or "-",
                    }
                )
                if not tag:
                    canais_disponiveis_map[channel_type] = canais_disponiveis_map.get(channel_type, 0) + 1

            module_info = {
                "id": module_id,
                "slot_pos": slot_index,
                "display_name": module.get("module_model_name") or "Modulo nao resolvido",
                "model_name": module.get("module_model_name") or "Modulo nao resolvido",
                "type_name": module_type,
                "brand": "",
                "channels": channel_summary,
                "channel_capacity": channel_capacity,
                "source": module.get("source") or "-",
                "filled_channels": filled_channels,
                "all_canais_comissionados": False,
            }
            module_by_slot[slot_index] = module_info
            module_data[module_id] = module_info

        slots = []
        ocupados = 0
        for slot_index in range(1, slots_total + 1):
            modulo = module_by_slot.get(slot_index)
            if modulo:
                ocupados += 1
            slots.append(
                {
                    "posicao": slot_index,
                    "modulo": {
                        "id": modulo["id"],
                        "all_canais_comissionados": modulo["all_canais_comissionados"],
                        "modulo_modelo": {
                            "modelo": modulo["model_name"],
                            "nome": modulo["display_name"],
                            "marca": modulo["brand"],
                            "quantidade_canais": modulo["channel_capacity"],
                            "tipo_base": {"nome": modulo["type_name"]},
                        },
                    }
                    if modulo
                    else None,
                }
            )

        preview_racks.append(
            {
                "key": rack_key,
                "name": rack_payload.get("name") or f"Rack {rack_index}",
                "descricao": f"Preview da importacao com {len(modules)} modulo(s) proposto(s).",
                "slots_total": slots_total,
                "ocupados": ocupados,
                "slots_livres": max(slots_total - ocupados, 0),
                "canais_disponiveis": [
                    {"tipo": tipo, "total": total}
                    for tipo, total in sorted(canais_disponiveis_map.items(), key=lambda item: item[0])
                ],
                "slots": slots,
                "selected_module_id": "",
                "source_sheets": rack_payload.get("source_sheets") or [],
                "summary": rack_payload.get("summary") or {},
                "is_applied": bool(applied_rack_id),
                "applied_rack_id": applied_rack_id,
            }
        )
        preview_payload[rack_key] = module_data
    return preview_racks, preview_payload


def _io_import_upload_context(request, cliente, target_rack=None):
    racks = _ios_racks_queryset(request.user, cliente).order_by("nome")
    inventarios = _ios_inventarios_queryset(request.user, cliente).order_by("nome")
    locais, grupos = _ios_locais_grupos(cliente)
    return {
        "io_import_can_upload": _io_import_can_manage(request, cliente),
        "io_import_racks": racks,
        "io_import_inventarios": inventarios,
        "io_import_locais": locais,
        "io_import_grupos": grupos,
        "io_import_default_target_rack": target_rack,
    }


def _build_io_import_job_queryset(request, cliente):
    queryset = IOImportJob.objects.select_related(
        "cliente",
        "created_by",
        "target_rack",
        "applied_rack",
        "requested_local",
        "requested_grupo",
        "requested_inventario",
    )
    if _is_admin_user(request.user):
        return queryset
    if not cliente:
        return queryset.none()
    return queryset.filter(cliente=cliente)


def _reprocess_io_import_job(job):
    settings_obj = _io_import_settings()
    module_catalog = serialize_module_catalog(_io_import_module_catalog(job.cliente))
    _save_io_import_progress(
        job,
        {
            "stage": "parse",
            "percent": 10,
            "title": "Leitura estrutural iniciada",
            "message": "A planilha foi aberta e as guias da importacao estao sendo organizadas.",
            "progress_label": "Leitura estrutural",
            "snapshots": [],
            "sheets_total": 0,
            "sheets_processed": 0,
        },
    )
    result = reprocess_import_job(
        job=job,
        module_catalog=module_catalog,
        settings_obj=settings_obj,
        progress_callback=lambda payload: _save_io_import_progress(job, payload),
    )
    job.file_format = result["file_format"]
    job.sheet_name = result["sheet_name"]
    job.header_row_index = result["header_row_index"]
    job.rows_total = result["rows_total"]
    job.rows_parsed = result["rows_parsed"]
    job.column_map = result["column_map"]
    job.extracted_payload = {"rows": result["normalized_rows"], "sheets": result.get("sheet_summaries") or []}
    job.proposal_payload = result["proposal"]
    job.warnings = result["warnings"]
    job.ai_status = result["ai_status"]
    job.ai_payload = result["ai_payload"]
    job.progress_payload = result.get("progress_payload") or job.progress_payload
    job.ai_error = result["ai_error"]
    job.ai_model = result["ai_model"]
    job.status = IOImportJob.Status.FAILED if result["proposal"].get("conflicts") else IOImportJob.Status.REVIEW
    job.save(
        update_fields=[
            "file_format",
            "sheet_name",
            "header_row_index",
            "rows_total",
            "rows_parsed",
            "column_map",
            "extracted_payload",
            "proposal_payload",
              "warnings",
              "ai_status",
              "ai_payload",
              "progress_payload",
              "ai_error",
              "ai_model",
              "status",
            "updated_at",
        ]
    )
    return job


def _ip_range_values(start, end, limit=2048):
    try:
        start_ip = ipaddress.ip_address((start or "").strip())
        end_ip = ipaddress.ip_address((end or "").strip())
    except ValueError:
        return None, "IP invalido."
    if start_ip.version != end_ip.version:
        return None, "Faixa deve usar o mesmo tipo de IP."
    start_int = int(start_ip)
    end_int = int(end_ip)
    if end_int < start_int:
        return None, "Faixa invalida."
    total = end_int - start_int + 1
    if total > limit:
        return None, f"Faixa excede limite de {limit} IPs."
    values = [str(ipaddress.ip_address(ip_int)) for ip_int in range(start_int, end_int + 1)]
    return values, None


def _sync_lista_ip_items(lista, ip_values):
    existing = {item.ip: item for item in lista.ips.all()}
    incoming = set(ip_values)
    to_create = [
        ListaIPItem(
            lista=lista,
            ip=ip_value,
            protocolo=lista.protocolo_padrao or "",
        )
        for ip_value in ip_values
        if ip_value not in existing
    ]
    if to_create:
        ListaIPItem.objects.bulk_create(to_create)
    remove_ips = set(existing.keys()) - incoming
    if remove_ips:
        lista.ips.filter(ip__in=remove_ips).delete()


def _sync_trabalho_status(trabalho):
    def _is_status_valido(status):
        return status in {
            RadarTrabalho.Status.EXECUTANDO,
            RadarTrabalho.Status.FINALIZADA,
        }

    statuses = list(trabalho.atividades.values_list("status", flat=True))
    if not statuses:
        novo_status = RadarTrabalho.Status.PENDENTE
    elif all(status == RadarTrabalho.Status.FINALIZADA for status in statuses):
        novo_status = RadarTrabalho.Status.FINALIZADA
    elif any(status == RadarTrabalho.Status.EXECUTANDO for status in statuses):
        novo_status = RadarTrabalho.Status.EXECUTANDO
    else:
        novo_status = RadarTrabalho.Status.PENDENTE
    if trabalho.status != novo_status:
        trabalho.status = novo_status
        update_fields = ["status"]
        if _is_status_valido(novo_status):
            trabalho.ultimo_status_evento_em = timezone.now()
            update_fields.append("ultimo_status_evento_em")
        trabalho.save(update_fields=update_fields)


def _parse_colaboradores_input(raw_value, max_items=40):
    raw = (raw_value or "").strip()
    if not raw:
        return []
    colaboradores = []
    seen = set()
    for part in re.split(r"[,;\n\r]+", raw):
        nome = " ".join((part or "").strip().split())
        if not nome:
            continue
        nome = nome[:120]
        key = nome.casefold()
        if key in seen:
            continue
        seen.add(key)
        colaboradores.append(nome)
        if len(colaboradores) >= max_items:
            break
    return colaboradores


def _parse_colaborador_ids_input(raw_values, max_items=40):
    if not raw_values:
        return []
    ids = []
    seen = set()
    for item in raw_values:
        if item is None:
            continue
        for raw_part in re.split(r"[,;\s]+", str(item).strip()):
            if not raw_part:
                continue
            try:
                colaborador_id = int(raw_part)
            except (TypeError, ValueError):
                continue
            if colaborador_id <= 0 or colaborador_id in seen:
                continue
            seen.add(colaborador_id)
            ids.append(colaborador_id)
            if len(ids) >= max_items:
                return ids
    return ids


def _parse_horas_dia_input(raw_value, default=Decimal("8.00")):
    raw = (raw_value or "").replace(",", ".").strip()
    if not raw:
        return default.quantize(Decimal("0.01")), None
    try:
        value = Decimal(raw)
    except InvalidOperation:
        return None, "Informe um valor valido para horas/dia."
    if value <= 0:
        return None, "Horas/dia deve ser maior que zero."
    value = value.quantize(Decimal("0.01"))
    if value > Decimal("999.99"):
        return None, "Horas/dia excede o maximo permitido (999.99)."
    return value, None


def _radar_colaborador_nome(row):
    if getattr(row, "colaborador", None) and row.colaborador.nome:
        return row.colaborador.nome
    return row.nome or ""


def _trabalho_colaboradores_nomes(trabalho):
    prefetched = getattr(trabalho, "_prefetched_objects_cache", {})
    if "colaboradores" in prefetched:
        return [
            _radar_colaborador_nome(colaborador)
            for colaborador in sorted(
                prefetched["colaboradores"],
                key=lambda item: ((_radar_colaborador_nome(item) or "").casefold(), item.id),
            )
        ]
    rows = list(trabalho.colaboradores.select_related("colaborador").all())
    rows.sort(key=lambda item: ((_radar_colaborador_nome(item) or "").casefold(), item.id))
    return [_radar_colaborador_nome(row) for row in rows]


def _trabalho_colaboradores_ids(trabalho):
    prefetched = getattr(trabalho, "_prefetched_objects_cache", {})
    rows = prefetched.get("colaboradores")
    if rows is None:
        rows = trabalho.colaboradores.all()
    ids = []
    seen = set()
    for row in rows:
        colab_id = getattr(row, "colaborador_id", None)
        if not colab_id or colab_id in seen:
            continue
        seen.add(colab_id)
        ids.append(colab_id)
    return ids


def _atividade_colaboradores_nomes(atividade):
    prefetched = getattr(atividade, "_prefetched_objects_cache", {})
    if "colaboradores" in prefetched:
        return [
            _radar_colaborador_nome(colaborador)
            for colaborador in sorted(
                prefetched["colaboradores"],
                key=lambda item: ((_radar_colaborador_nome(item) or "").casefold(), item.id),
            )
        ]
    rows = list(atividade.colaboradores.select_related("colaborador").all())
    rows.sort(key=lambda item: ((_radar_colaborador_nome(item) or "").casefold(), item.id))
    return [_radar_colaborador_nome(row) for row in rows]


def _atividade_colaboradores_ids(atividade):
    prefetched = getattr(atividade, "_prefetched_objects_cache", {})
    rows = prefetched.get("colaboradores")
    if rows is None:
        rows = atividade.colaboradores.all()
    ids = []
    seen = set()
    for row in rows:
        colab_id = getattr(row, "colaborador_id", None)
        if not colab_id or colab_id in seen:
            continue
        seen.add(colab_id)
        ids.append(colab_id)
    return ids


def _trabalho_colaboradores_catalogo(trabalho):
    prefetched = getattr(trabalho, "_prefetched_objects_cache", {})
    rows = prefetched.get("colaboradores")
    if rows is None:
        rows = list(trabalho.colaboradores.select_related("colaborador").all())
    catalogo = []
    seen = set()
    ordered_rows = sorted(
        rows,
        key=lambda item: ((_radar_colaborador_nome(item) or "").casefold(), item.id),
    )
    for row in ordered_rows:
        colaborador = getattr(row, "colaborador", None)
        if not colaborador or not row.colaborador_id or row.colaborador_id in seen:
            continue
        seen.add(row.colaborador_id)
        catalogo.append(colaborador)
    return catalogo


def _atividade_editor_colaboradores_catalogo(trabalho):
    if not trabalho:
        return []
    return list(
        RadarColaborador.objects.filter(
            Q(trabalhos_vinculados__trabalho=trabalho)
            | Q(atividades_vinculadas__atividade__trabalho=trabalho)
        )
        .distinct()
        .order_by("nome", "id")
    )


def _atividade_colaboradores_count_map(atividade_ids):
    atividade_ids = [item for item in atividade_ids if item]
    if not atividade_ids:
        return {}
    return {
        row["atividade_id"]: row["total_colaboradores"]
        for row in (
            RadarAtividadeColaborador.objects.filter(atividade_id__in=atividade_ids)
            .values("atividade_id")
            .annotate(total_colaboradores=Count("id"))
        )
    }


def _atividade_colaboradores_rows_map(atividade_ids):
    atividade_ids = [item for item in atividade_ids if item]
    if not atividade_ids:
        return {}
    rows_map = {}
    for row in (
        RadarAtividadeColaborador.objects.filter(atividade_id__in=atividade_ids)
        .select_related("colaborador")
        .order_by("atividade_id", "nome", "id")
    ):
        rows_map.setdefault(row.atividade_id, []).append(row)
    return rows_map


def _radar_colaboradores_catalogo(radar):
    if not radar or not radar.cliente_id:
        return RadarColaborador.objects.none()
    return RadarColaborador.objects.filter(perfil=radar.cliente).order_by("nome", "id")


def _sync_colaboradores_rows(
    existing_rows_qs,
    relation_model,
    parent_field_name,
    parent_instance,
    owner_cliente,
    colaboradores_nomes=None,
    colaboradores_ids=None,
):
    incoming = []
    incoming_keys = set()
    cliente_dono = owner_cliente

    colaboradores_by_id = {}
    if cliente_dono and colaboradores_ids:
        queryset = RadarColaborador.objects.filter(
            perfil=cliente_dono,
            id__in=[int(colab_id) for colab_id in colaboradores_ids if str(colab_id).isdigit()],
        )
        colaboradores_by_id = {item.id: item for item in queryset}

    for colaborador_id in colaboradores_ids or []:
        colaborador = colaboradores_by_id.get(colaborador_id)
        if not colaborador:
            continue
        nome = " ".join((colaborador.nome or "").strip().split())[:120]
        if not nome:
            continue
        key = f"id:{colaborador.id}"
        if key in incoming_keys:
            continue
        incoming_keys.add(key)
        incoming.append(
            {
                "key": key,
                "nome": nome,
                "colaborador_id": colaborador.id,
            }
        )

    nomes_sem_id = _parse_colaboradores_input(
        ",".join(colaboradores_nomes or []),
        max_items=40,
    )
    for nome in nomes_sem_id:
        key_nome = f"nome:{nome.casefold()}"
        if key_nome in incoming_keys:
            continue
        colaborador_id = None
        if cliente_dono:
            mapped = RadarColaborador.objects.filter(perfil=cliente_dono, nome__iexact=nome).first()
            if mapped:
                colaborador_id = mapped.id
                mapped_key = f"id:{mapped.id}"
                if mapped_key in incoming_keys:
                    continue
                incoming_keys.add(mapped_key)
                incoming.append(
                    {
                        "key": mapped_key,
                        "nome": mapped.nome[:120],
                        "colaborador_id": mapped.id,
                    }
                )
                continue
        incoming_keys.add(key_nome)
        incoming.append(
            {
                "key": key_nome,
                "nome": nome,
                "colaborador_id": colaborador_id,
            }
        )

    existing_rows = list(existing_rows_qs.all())
    existing_by_key = {}
    duplicate_ids = []
    for row in existing_rows:
        if row.colaborador_id:
            key = f"id:{row.colaborador_id}"
        else:
            nome_key = (row.nome or "").strip().casefold()
            key = f"nome:{nome_key}" if nome_key else ""
        if not key:
            if row.id:
                duplicate_ids.append(row.id)
            continue
        if key in existing_by_key:
            if row.id:
                duplicate_ids.append(row.id)
            continue
        existing_by_key[key] = row

    used_existing_ids = set()
    to_update = []
    to_create = []
    ordered_nomes = []

    for item in incoming:
        key = item["key"]
        nome = item["nome"]
        colaborador_id = item["colaborador_id"]
        row = existing_by_key.get(key)
        if not row and key.startswith("id:"):
            row = existing_by_key.get(f"nome:{nome.casefold()}")
        if row:
            used_existing_ids.add(row.id)
            if row.nome != nome or row.colaborador_id != colaborador_id:
                row.nome = nome
                row.colaborador_id = colaborador_id
                to_update.append(row)
        else:
            create_kwargs = {
                parent_field_name: parent_instance,
                "nome": nome,
                "colaborador_id": colaborador_id,
            }
            to_create.append(
                relation_model(**create_kwargs)
            )
        ordered_nomes.append(nome)

    to_delete_ids = duplicate_ids + [
        row.id
        for row in existing_rows
        if row.id and row.id not in used_existing_ids
    ]
    if to_delete_ids:
        relation_model.objects.filter(pk__in=to_delete_ids).delete()
    if to_update:
        relation_model.objects.bulk_update(to_update, ["nome", "colaborador"])
    if to_create:
        relation_model.objects.bulk_create(to_create, ignore_conflicts=True)
    return ordered_nomes


def _sync_trabalho_colaboradores(trabalho, colaboradores_nomes=None, colaboradores_ids=None):
    return _sync_colaboradores_rows(
        existing_rows_qs=trabalho.colaboradores.select_related("colaborador"),
        relation_model=RadarTrabalhoColaborador,
        parent_field_name="trabalho",
        parent_instance=trabalho,
        owner_cliente=getattr(getattr(trabalho, "radar", None), "cliente", None),
        colaboradores_nomes=colaboradores_nomes,
        colaboradores_ids=colaboradores_ids,
    )


def _sync_atividade_colaboradores(atividade, colaboradores_nomes=None, colaboradores_ids=None):
    return _sync_colaboradores_rows(
        existing_rows_qs=atividade.colaboradores.select_related("colaborador"),
        relation_model=RadarAtividadeColaborador,
        parent_field_name="atividade",
        parent_instance=atividade,
        owner_cliente=getattr(getattr(getattr(atividade, "trabalho", None), "radar", None), "cliente", None),
        colaboradores_nomes=colaboradores_nomes,
        colaboradores_ids=colaboradores_ids,
    )


def _parse_agenda_execucao_input(raw_value, max_days=730):
    raw = (raw_value or "").strip()
    if not raw:
        return [], None
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        parsed = [part for part in re.split(r"[,;\s]+", raw) if part]
    if isinstance(parsed, str):
        parsed = [parsed]
    if not isinstance(parsed, list):
        return None, "Formato de agenda invalido."
    datas = []
    seen = set()
    for item in parsed:
        value = str(item or "").strip()
        if not value:
            continue
        try:
            current = datetime.strptime(value, "%Y-%m-%d").date()
        except ValueError:
            return None, "Data invalida na agenda. Use o formato YYYY-MM-DD."
        key = current.isoformat()
        if key in seen:
            continue
        seen.add(key)
        datas.append(current)
        if len(datas) > max_days:
            return None, f"Agenda excede limite de {max_days} datas."
    datas.sort()
    return datas, None


def _atividade_agenda_datas(atividade):
    prefetched = getattr(atividade, "_prefetched_objects_cache", {})
    if "dias_execucao" in prefetched:
        datas = [item.data_execucao for item in prefetched["dias_execucao"] if item.data_execucao]
        return sorted(datas)
    return list(
        atividade.dias_execucao.order_by("data_execucao", "id").values_list("data_execucao", flat=True)
    )


def _atividade_agenda_dias_iso(atividade):
    return [item.isoformat() for item in _atividade_agenda_datas(atividade)]


def _trabalho_colaboradores_multiplier(trabalho):
    if not trabalho:
        return Decimal("1")
    prefetched = getattr(trabalho, "_prefetched_objects_cache", {})
    if "colaboradores" in prefetched:
        total_colaboradores = len(prefetched["colaboradores"])
    else:
        total_colaboradores = trabalho.colaboradores.count()
    if total_colaboradores <= 0:
        total_colaboradores = 1
    return Decimal(total_colaboradores)


def _atividade_colaboradores_multiplier(atividade):
    if not atividade:
        return Decimal("1")
    prefetched = getattr(atividade, "_prefetched_objects_cache", {})
    if "colaboradores" in prefetched:
        total_colaboradores = len(prefetched["colaboradores"])
    else:
        total_colaboradores = atividade.colaboradores.count()
    if total_colaboradores <= 0:
        total_colaboradores = 1
    return Decimal(total_colaboradores)


def _atividade_horas_from_agenda(atividade, agenda_datas=None, colaboradores_multiplier=None):
    datas = agenda_datas if agenda_datas is not None else _atividade_agenda_datas(atividade)
    horas_dia = (
        atividade.trabalho.horas_dia
        if atividade.trabalho and atividade.trabalho.horas_dia is not None
        else Decimal("8.00")
    )
    multiplier = (
        colaboradores_multiplier
        if colaboradores_multiplier is not None
        else _atividade_colaboradores_multiplier(atividade)
    )
    return horas_dia * Decimal(len(datas)) * multiplier


def _sync_atividade_execucao_metrics_from_agenda(atividade, agenda_datas=None):
    datas = agenda_datas if agenda_datas is not None else _atividade_agenda_datas(atividade)
    inicio = None
    fim = None
    if datas:
        tz = timezone.get_current_timezone()
        inicio = timezone.make_aware(datetime.combine(datas[0], datetime.min.time()), tz)
        fim = timezone.make_aware(datetime.combine(datas[-1], datetime.min.time()), tz)
    horas = _atividade_horas_from_agenda(atividade, agenda_datas=datas)
    mudou = (
        atividade.inicio_execucao_em != inicio
        or atividade.finalizada_em != fim
        or atividade.horas_trabalho != horas
    )
    atividade.inicio_execucao_em = inicio
    atividade.finalizada_em = fim
    atividade.horas_trabalho = horas
    return mudou


def _recalcular_horas_atividades_trabalho(trabalho):
    atividades = list(
        RadarAtividade.objects.filter(trabalho=trabalho)
        .select_related("trabalho")
        .prefetch_related("dias_execucao", "colaboradores")
    )
    changed = []
    for atividade in atividades:
        horas = _atividade_horas_from_agenda(atividade)
        if atividade.horas_trabalho != horas:
            atividade.horas_trabalho = horas
            changed.append(atividade)
    if changed:
        RadarAtividade.objects.bulk_update(changed, ["horas_trabalho"])


def _normalizar_ordem_atividades(trabalho, status=None):
    atividades = RadarAtividade.objects.filter(trabalho=trabalho)
    if status:
        atividades = atividades.filter(status=status)
    atividades = list(atividades.order_by("ordem", "criado_em", "id"))
    changed = []
    for idx, atividade in enumerate(atividades, start=1):
        if atividade.ordem != idx:
            atividade.ordem = idx
            changed.append(atividade)
    if changed:
        RadarAtividade.objects.bulk_update(changed, ["ordem"])


def _get_radar_trabalho_acessivel(user, trabalho_pk):
    if not _radar_trabalho_schema_ready():
        return None
    cliente = _get_cliente(user)
    trabalhos = RadarTrabalho.objects.select_related(
        "radar",
        "radar__cliente",
        "radar__id_radar",
        "classificacao",
        "contrato",
    ).prefetch_related("atividades", "colaboradores")
    if _is_admin_user(user) and not cliente:
        return trabalhos.filter(pk=trabalho_pk).first()
    if not cliente:
        return None
    return trabalhos.filter(
        Q(pk=trabalho_pk),
        Q(radar__cliente=cliente) | Q(radar__id_radar__in=cliente.radares.all()),
    ).first()


def _db_column_exists(table_name, column_name):
    connection = connections["default"]
    if connection.vendor == "sqlite":
        try:
            with connection.cursor() as cursor:
                cursor.execute(f"PRAGMA table_info({table_name})")
                columns = [row[1] for row in cursor.fetchall()]
                return column_name in columns
        except DatabaseError:
            return False
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT 1
                FROM information_schema.columns
                WHERE table_schema = 'public'
                  AND table_name = %s
                  AND column_name = %s
                LIMIT 1
                """,
                [table_name, column_name],
            )
            return cursor.fetchone() is not None
    except DatabaseError:
        return False


def _radar_trabalho_schema_ready():
    return _db_column_exists("core_radartrabalho", "criado_por_id")


def _descricao_proposta_de_trabalho(trabalho):
    return (trabalho.descricao or "").strip()


def _is_radar_creator_user(user, radar):
    cliente = _get_cliente(user)
    return bool(cliente and radar and radar.cliente_id == cliente.id)


def home(request):
    if request.user.is_authenticated:
        logout(request)
    return render(request, "core/home.html")


def maintenance_page(request):
    try:
        config = SystemConfiguration.load()
    except Exception:
        config = None
    maintenance_enabled = bool(config and config.maintenance_mode_enabled)
    maintenance_message = (
        config.maintenance_message
        if config and (config.maintenance_message or "").strip()
        else SystemConfiguration.DEFAULT_MAINTENANCE_MESSAGE
    )
    if request.user.is_authenticated and (request.user.is_superuser or has_tipo_code(request.user, "DEV")):
        return redirect("painel")
    if not maintenance_enabled:
        if request.user.is_authenticated:
            return redirect("painel")
        return redirect("home")
    return render(
        request,
        "core/maintenance.html",
        {
            "maintenance_message": maintenance_message,
        },
    )


@csrf_exempt
def api_ingest(request):
    if request.method != "POST":
        return HttpResponseNotAllowed(["POST"])
    expected_token = (os.environ.get("API_TOKEN") or "").strip()
    auth_header = request.headers.get("Authorization", "")
    token = _parse_bearer_token(auth_header)
    if not expected_token or not token or not hmac.compare_digest(token, expected_token):
        _log_ingest_error("unauthorized", raw_body=request.body.decode("utf-8", errors="replace") if request.body else "")
        return JsonResponse({"ok": False, "error": "unauthorized"}, status=401)
    try:
        raw_body = request.body.decode("utf-8") if request.body else ""
        payload = json.loads(raw_body or "[]")
    except json.JSONDecodeError:
        _log_ingest_error("invalid_json", raw_body=request.body.decode("utf-8", errors="replace") if request.body else "")
        return JsonResponse({"ok": False, "error": "invalid_json"}, status=400)
    if not isinstance(payload, list):
        _log_ingest_error("invalid_payload", raw_body=raw_body)
        return JsonResponse({"ok": False, "error": "invalid_payload"}, status=400)
    rules_by_source = {
        rule.source.strip().lower(): (rule.required_fields or [])
        for rule in IngestRule.objects.all()
        if rule.source
    }
    items_by_source = {}
    for item in payload:
        if not isinstance(item, dict):
            _log_ingest_error("invalid_payload", item=item, raw_body=raw_body)
            return JsonResponse({"ok": False, "error": "invalid_payload"}, status=400)
        source_id = str(item.get("source_id", "")).strip()
        client_id = str(item.get("client_id", "")).strip()
        agent_id = str(item.get("agent_id", "")).strip()
        source = str(item.get("source", "")).strip()
        if not source_id or not client_id or not agent_id or not source:
            _log_ingest_error("invalid_payload", item=item, raw_body=raw_body)
            return JsonResponse({"ok": False, "error": "invalid_payload"}, status=400)
        payload_data = item.get("payload", None)
        if isinstance(payload_data, str):
            try:
                payload_data = json.loads(payload_data)
            except json.JSONDecodeError:
                _log_ingest_error("invalid_payload", item=item, raw_body=raw_body)
                return JsonResponse({"ok": False, "error": "invalid_payload"}, status=400)
        if payload_data is None:
            _log_ingest_error("invalid_payload", item=item, raw_body=raw_body)
            return JsonResponse({"ok": False, "error": "invalid_payload"}, status=400)
        is_valid, validation_error = _validate_payload_by_source(
            source,
            payload_data,
            rules_by_source,
        )
        if not is_valid:
            _log_ingest_error(f"invalid_payload:{validation_error}", item=item, raw_body=raw_body)
            return JsonResponse({"ok": False, "error": "invalid_payload"}, status=400)
        items_by_source[source_id] = {
            "client_id": client_id,
            "agent_id": agent_id,
            "source": source,
            "payload": payload_data,
        }
    if items_by_source:
        _upsert_ingest_items(items_by_source)
    return JsonResponse({"ok": True, "count": len(payload)})

@csrf_exempt
def api_ingest_rules(request):
    if request.method != "GET":
        return HttpResponseNotAllowed(["GET"])
    expected_token = (os.environ.get("API_TOKEN") or "").strip()
    auth_header = request.headers.get("Authorization", "")
    token = _parse_bearer_token(auth_header)
    if not expected_token or not token or not hmac.compare_digest(token, expected_token):
        return JsonResponse({"ok": False, "error": "unauthorized"}, status=401)
    source = request.GET.get("source", "").strip().lower()
    rules_qs = IngestRule.objects.all()
    if source:
        rules_qs = rules_qs.filter(source=source)
    rules = {rule.source: (rule.required_fields or []) for rule in rules_qs}
    return JsonResponse({"ok": True, "rules": rules})


@login_required
def painel(request):
    cliente = _get_cliente(request.user)
    display_name = None
    if cliente and cliente.nome:
        display_name = cliente.nome
    else:
        display_name = request.user.first_name or request.user.username
    role = _user_role(request.user)
    if _is_admin_user(request.user) and not cliente:
        apps = _filter_visible_apps_queryset(App.objects.all(), request.user).order_by("nome")
    elif cliente:
        apps = _filter_visible_apps_queryset(cliente.apps.all(), request.user).order_by("nome")
    else:
        apps = App.objects.none()
    module_signal_badges = _build_module_signal_badges(request.user, cliente)
    is_dev_user = _is_dev_user(request.user)
    visible_internal_modules = visible_internal_module_codes(request.user)
    return render(
        request,
        "core/painel.html",
        {
            "display_name": display_name,
            "role": role,
            "apps": apps,
            "module_signal_badges": module_signal_badges,
            "is_dev_user": is_dev_user,
            "visible_internal_modules": visible_internal_modules,
        },
    )


@login_required
def planta_conectada(request):
    if not _is_admin_user(request.user):
        return HttpResponseForbidden("Sem permissao.")
    if request.method == "POST":
        action = request.POST.get("action")
        if action == "clear_ingest":
            return redirect("ingest_limpar")
        if action == "reprocess_ingest_errors":
            rules_by_source = {
                rule.source.strip().lower(): (rule.required_fields or [])
                for rule in IngestRule.objects.all()
                if rule.source
            }
            items_by_source = {}
            logs_to_resolve = []
            for log in IngestErrorLog.objects.filter(resolved=False).order_by("created_at"):
                item = log.raw_payload
                if not isinstance(item, dict):
                    continue
                source_id = str(item.get("source_id", "")).strip()
                client_id = str(item.get("client_id", "")).strip()
                agent_id = str(item.get("agent_id", "")).strip()
                source = str(item.get("source", "")).strip()
                if not source_id or not client_id or not agent_id or not source:
                    continue
                payload_data = item.get("payload", None)
                if isinstance(payload_data, str):
                    try:
                        payload_data = json.loads(payload_data)
                    except json.JSONDecodeError:
                        continue
                if payload_data is None:
                    continue
                is_valid, _ = _validate_payload_by_source(source, payload_data, rules_by_source)
                if not is_valid:
                    continue
                items_by_source[source_id] = {
                    "client_id": client_id,
                    "agent_id": agent_id,
                    "source": source,
                    "payload": payload_data,
                }
                logs_to_resolve.append(log)
            if items_by_source:
                _upsert_ingest_items(items_by_source)
                now = timezone.now()
                for log in logs_to_resolve:
                    log.resolved = True
                    log.resolved_at = now
                IngestErrorLog.objects.bulk_update(logs_to_resolve, ["resolved", "resolved_at"])
            return redirect("planta_conectada")
    registros_qs = IngestRecord.objects.all()
    source = request.GET.get("source", "").strip()
    source_id = request.GET.get("source_id", "").strip()
    client_id = request.GET.get("client_id", "").strip()
    agent_id = request.GET.get("agent_id", "").strip()
    payload_q = request.GET.get("payload_q", "").strip()
    if source:
        registros_qs = registros_qs.filter(source__icontains=source)
    if source_id:
        registros_qs = registros_qs.filter(source_id__icontains=source_id)
    if client_id:
        registros_qs = registros_qs.filter(client_id__icontains=client_id)
    if agent_id:
        registros_qs = registros_qs.filter(agent_id__icontains=agent_id)
    if payload_q:
        registros_qs = registros_qs.annotate(payload_text=Cast("payload", output_field=TextField())).filter(
            payload_text__icontains=payload_q
        )
    registros_qs = registros_qs.order_by("-created_at")
    source_options = list(
        IngestRecord.objects.exclude(source="").values_list("source", flat=True).distinct().order_by("source")[:200]
    )
    source_id_options = list(
        IngestRecord.objects.exclude(source_id="").values_list("source_id", flat=True).distinct().order_by("source_id")[:200]
    )
    client_id_options = list(
        IngestRecord.objects.exclude(client_id="").values_list("client_id", flat=True).distinct().order_by("client_id")[:200]
    )
    agent_id_options = list(
        IngestRecord.objects.exclude(agent_id="").values_list("agent_id", flat=True).distinct().order_by("agent_id")[:200]
    )
    page_obj = _paginate_queryset(request, registros_qs, per_page=15)
    page_query = request.GET.copy()
    page_query.pop("page", None)
    context = {
        "page_obj": page_obj,
        "page_query": page_query.urlencode(),
        "filters": {
            "source": source,
            "source_id": source_id,
            "client_id": client_id,
            "agent_id": agent_id,
            "payload_q": payload_q,
        },
        "filter_options": {
            "source": source_options,
            "source_id": source_id_options,
            "client_id": client_id_options,
            "agent_id": agent_id_options,
        },
    }
    if _is_partial_request(request):
        return render(request, "core/partials/ingest_records_list.html", context)
    return render(request, "core/ingest_gerenciar.html", context)


def _build_ingest_created_at_range(start_raw, end_raw):
    if not start_raw and not end_raw:
        return None, None, None

    try:
        start_date = date.fromisoformat(start_raw) if start_raw else None
        end_date = date.fromisoformat(end_raw) if end_raw else None
    except ValueError:
        return None, None, "Informe datas validas para filtrar a limpeza."

    if start_date is None:
        start_date = end_date
    if end_date is None:
        end_date = start_date
    if end_date < start_date:
        return None, None, "A data final nao pode ser anterior a data inicial."

    tz = timezone.get_current_timezone()
    start_dt = timezone.make_aware(datetime.combine(start_date, datetime.min.time()), tz)
    end_dt = timezone.make_aware(datetime.combine(end_date + timedelta(days=1), datetime.min.time()), tz)
    return start_dt, end_dt, None


@login_required
def ingest_limpar(request):
    if not _is_admin_user(request.user):
        return HttpResponseForbidden("Sem permissao.")

    message = None
    message_level = "info"
    removed_count = None
    preview_limit = 50
    selected = {
        "client_id": (request.POST.get("client_id") if request.method == "POST" else request.GET.get("client_id") or "").strip(),
        "agent_id": (request.POST.get("agent_id") if request.method == "POST" else request.GET.get("agent_id") or "").strip(),
        "source": (request.POST.get("source") if request.method == "POST" else request.GET.get("source") or "").strip(),
        "data_inicial": (request.POST.get("data_inicial") if request.method == "POST" else request.GET.get("data_inicial") or "").strip(),
        "data_final": (request.POST.get("data_final") if request.method == "POST" else request.GET.get("data_final") or "").strip(),
    }
    created_at_start, created_at_end, date_error = _build_ingest_created_at_range(
        selected["data_inicial"],
        selected["data_final"],
    )

    if request.method == "POST":
        action = (request.POST.get("action") or "").strip()
        if action == "delete_filtered_ingest":
            if not selected["client_id"] or not selected["agent_id"] or not selected["source"]:
                message = "Informe client_id, agent_id e source para limpar."
                message_level = "error"
            elif date_error:
                message = date_error
                message_level = "error"
            else:
                qs = IngestRecord.objects.filter(
                    client_id=selected["client_id"],
                    agent_id=selected["agent_id"],
                    source=selected["source"],
                )
                if created_at_start and created_at_end:
                    qs = qs.filter(created_at__gte=created_at_start, created_at__lt=created_at_end)
                removed_count, _ = qs.delete()
                message = f"{removed_count} registro(s) removido(s) para o filtro informado."
                message_level = "success"

    preview_qs = IngestRecord.objects.all()
    if selected["client_id"]:
        preview_qs = preview_qs.filter(client_id=selected["client_id"])
    if selected["agent_id"]:
        preview_qs = preview_qs.filter(agent_id=selected["agent_id"])
    if selected["source"]:
        preview_qs = preview_qs.filter(source=selected["source"])
    if created_at_start and created_at_end:
        preview_qs = preview_qs.filter(created_at__gte=created_at_start, created_at__lt=created_at_end)
    preview_count = preview_qs.count() if any(selected.values()) and not date_error else 0
    preview_requested = request.method == "GET" and request.GET.get("preview") == "1"
    preview_records = []
    if preview_requested and preview_count:
        preview_records = list(preview_qs.order_by("-created_at")[:preview_limit])

    client_id_options = list(
        IngestRecord.objects.exclude(client_id="").values_list("client_id", flat=True).distinct().order_by("client_id")[:400]
    )
    agent_id_options = list(
        IngestRecord.objects.exclude(agent_id="").values_list("agent_id", flat=True).distinct().order_by("agent_id")[:400]
    )
    source_options = list(
        IngestRecord.objects.exclude(source="").values_list("source", flat=True).distinct().order_by("source")[:400]
    )

    return render(
        request,
        "core/ingest_limpar.html",
        {
            "message": message,
            "message_level": message_level,
            "removed_count": removed_count,
            "selected": selected,
            "preview_count": preview_count,
            "preview_requested": preview_requested,
            "preview_records": preview_records,
            "preview_limit": preview_limit,
            "client_id_options": client_id_options,
            "agent_id_options": agent_id_options,
            "source_options": source_options,
        },
    )


@login_required
def ingest_error_logs(request):
    if not _is_admin_user(request.user):
        return HttpResponseForbidden("Sem permissao.")
    logs_qs = IngestErrorLog.objects.all()
    source = request.GET.get("source", "").strip()
    source_id = request.GET.get("source_id", "").strip()
    client_id = request.GET.get("client_id", "").strip()
    agent_id = request.GET.get("agent_id", "").strip()
    status = request.GET.get("status", "").strip()
    if source:
        logs_qs = logs_qs.filter(source__icontains=source)
    if source_id:
        logs_qs = logs_qs.filter(source_id__icontains=source_id)
    if client_id:
        logs_qs = logs_qs.filter(client_id__icontains=client_id)
    if agent_id:
        logs_qs = logs_qs.filter(agent_id__icontains=agent_id)
    if status == "pending":
        logs_qs = logs_qs.filter(resolved=False)
    if status == "resolved":
        logs_qs = logs_qs.filter(resolved=True)
    logs_qs = logs_qs.order_by("-created_at")
    source_options = list(
        IngestErrorLog.objects.exclude(source="").values_list("source", flat=True).distinct().order_by("source")[:200]
    )
    source_id_options = list(
        IngestErrorLog.objects.exclude(source_id="").values_list("source_id", flat=True).distinct().order_by("source_id")[:200]
    )
    client_id_options = list(
        IngestErrorLog.objects.exclude(client_id="").values_list("client_id", flat=True).distinct().order_by("client_id")[:200]
    )
    agent_id_options = list(
        IngestErrorLog.objects.exclude(agent_id="").values_list("agent_id", flat=True).distinct().order_by("agent_id")[:200]
    )
    page_obj = _paginate_queryset(request, logs_qs, per_page=15)
    pending_count = IngestErrorLog.objects.filter(resolved=False).count()
    page_query = request.GET.copy()
    page_query.pop("page", None)
    context = {
        "page_obj": page_obj,
        "pending_count": pending_count,
        "page_query": page_query.urlencode(),
        "filters": {
            "source": source,
            "source_id": source_id,
            "client_id": client_id,
            "agent_id": agent_id,
            "status": status,
        },
        "filter_options": {
            "source": source_options,
            "source_id": source_id_options,
            "client_id": client_id_options,
            "agent_id": agent_id_options,
        },
    }
    if _is_partial_request(request):
        return render(request, "core/partials/ingest_errors_list.html", context)
    return render(request, "core/ingest_erros.html", context)


@login_required
def ingest_sources(request):
    if not _is_admin_user(request.user):
        return HttpResponseForbidden("Sem permissao.")
    if request.method == "POST":
        action = request.POST.get("action")
        if action == "save_ingest_rule":
            source = request.POST.get("source", "").strip().lower()
            required_raw = request.POST.get("required_fields", "").strip()
            required_fields = []
            if required_raw:
                try:
                    data = json.loads(required_raw)
                    if isinstance(data, list):
                        required_fields = [str(item).strip() for item in data if str(item).strip()]
                except json.JSONDecodeError:
                    required_fields = []
            if source:
                required_fields = _normalize_required_fields(required_fields)
                IngestRule.objects.update_or_create(
                    source=source,
                    defaults={"required_fields": required_fields},
                )
            return redirect("ingest_sources")
        if action == "update_ingest_rule":
            rule_id = request.POST.get("rule_id")
            new_source = request.POST.get("source", "").strip().lower()
            required_raw = request.POST.get("required_fields", "").strip()
            required_fields = []
            if required_raw:
                try:
                    data = json.loads(required_raw)
                    if isinstance(data, list):
                        required_fields = [str(item).strip() for item in data if str(item).strip()]
                except json.JSONDecodeError:
                    required_fields = []
            if rule_id:
                required_fields = _normalize_required_fields(required_fields)
                updates = {"required_fields": required_fields}
                if new_source:
                    updates["source"] = new_source
                IngestRule.objects.filter(pk=rule_id).update(**updates)
            return redirect("ingest_sources")
        if action == "delete_ingest_rule":
            rule_id = request.POST.get("rule_id")
            if rule_id:
                IngestRule.objects.filter(pk=rule_id).delete()
            return redirect("ingest_sources")
    source_q = request.GET.get("source", "").strip()
    rules_qs = IngestRule.objects.all().order_by("source")
    if source_q:
        rules_qs = rules_qs.filter(source__icontains=source_q)
    page_obj = _paginate_queryset(request, rules_qs, per_page=15)
    page_query = request.GET.copy()
    page_query.pop("page", None)
    source_options = list(
        IngestRule.objects.exclude(source="").values_list("source", flat=True).distinct().order_by("source")[:200]
    )
    for rule in page_obj:
        rule.required_fields_json = json.dumps(rule.required_fields or [])
    return render(
        request,
        "core/ingest_sources.html",
        {
            "page_obj": page_obj,
            "page_query": page_query.urlencode(),
            "filters": {"source": source_q},
            "filter_options": {"source": source_options},
        },
    )


@login_required
def ingest_error_detail(request, pk):
    if not _is_admin_user(request.user):
        return HttpResponseForbidden("Sem permissao.")
    log = get_object_or_404(IngestErrorLog, pk=pk)
    pending_count = IngestErrorLog.objects.filter(resolved=False).count()
    payload = log.raw_payload if isinstance(log.raw_payload, dict) else {}
    payload_data = payload.get("payload") if isinstance(payload, dict) else None
    payload_error = None
    if isinstance(payload_data, str):
        try:
            payload_data = json.loads(payload_data)
        except json.JSONDecodeError:
            payload_error = "Payload e uma string JSON invalida."
            payload_data = None
    elif payload_data is not None and not isinstance(payload_data, dict):
        payload_error = "Payload nao e um objeto JSON."
    if request.method == "POST" and request.POST.get("action") == "create_ingest_rule":
        source = str(log.source or "").strip().lower()
        if source and isinstance(payload_data, dict):
            required_fields = _normalize_required_fields(list(payload_data.keys()))
            IngestRule.objects.update_or_create(
                source=source,
                defaults={"required_fields": required_fields},
            )
        return redirect("ingest_erro_detail", pk=log.pk)
    return render(
        request,
        "core/ingest_error_detail.html",
        {
            "log": log,
            "pending_count": pending_count,
            "payload_keys": list(payload_data.keys()) if isinstance(payload_data, dict) else [],
            "payload_error": payload_error,
        },
    )


@login_required
def ingest_detail(request, pk):
    if not _is_admin_user(request.user):
        return HttpResponseForbidden("Sem permissao.")
    registro = get_object_or_404(IngestRecord, pk=pk)
    payload = registro.payload if isinstance(registro.payload, dict) else {}
    return render(
        request,
        "core/ingest_detail.html",
        {
            "registro": registro,
            "payload": payload,
        },
    )


@login_required
def planta_conectada_redirect(request):
    return redirect("ingest_gerenciar")


@login_required
def app_home(request, slug):
    app = _get_app_by_slug_for_user(slug, request.user)
    if not _user_has_app_access(request.user, app):
        return HttpResponseForbidden("Sem permissao.")
    if app.slug == "appmilhaobla":
        return redirect("app_milhao_bla_dashboard")
    if app.slug == "approtas":
        return redirect("app_rotas_dashboard")
    return render(request, "core/app_home.html", {"app": app})


@login_required
def apps_gerenciar(request):
    if not _is_admin_user(request.user):
        return HttpResponseForbidden("Sem permissao.")
    message = None
    message_level = "info"
    if request.method == "POST":
        action = request.POST.get("action")
        if action == "create_app":
            nome = request.POST.get("nome", "").strip()
            slug_raw = request.POST.get("slug", "").strip()
            descricao = request.POST.get("descricao", "").strip()
            icon = request.POST.get("icon", "").strip()
            theme_color = request.POST.get("theme_color", "").strip()
            ingest_fields = _extract_app_ingest_fields(request.POST)
            slug = _clean_app_slug(slug_raw or nome)
            if not nome or not slug:
                message = "Informe nome e slug valido."
                message_level = "error"
            elif slug == "approtas" and (not ingest_fields["ingest_client_id"] or not ingest_fields["ingest_agent_id"]):
                message = "Para o app Rotas, informe client_id e agent_id da ingest."
                message_level = "error"
            else:
                app, created = App.objects.get_or_create(
                    slug=slug,
                    defaults={
                        "nome": nome,
                        "descricao": descricao,
                        "icon": icon,
                        "theme_color": theme_color,
                        "ingest_client_id": ingest_fields["ingest_client_id"],
                        "ingest_agent_id": ingest_fields["ingest_agent_id"],
                        "ingest_source": ingest_fields["ingest_source"],
                        "ativo": True,
                    },
                )
                if not created:
                    app.nome = nome
                    app.descricao = descricao
                    app.icon = icon
                    app.theme_color = theme_color
                    app.ingest_client_id = ingest_fields["ingest_client_id"]
                    app.ingest_agent_id = ingest_fields["ingest_agent_id"]
                    app.ingest_source = ingest_fields["ingest_source"]
                    app.save(
                        update_fields=[
                            "nome",
                            "descricao",
                            "icon",
                            "theme_color",
                            "ingest_client_id",
                            "ingest_agent_id",
                            "ingest_source",
                        ]
                    )
                return redirect("apps_gerenciar")
        if action == "update_app":
            app_id = request.POST.get("app_id")
            app = App.objects.filter(pk=app_id).first()
            if app:
                nome = request.POST.get("nome", "").strip()
                descricao = request.POST.get("descricao", "").strip()
                icon = request.POST.get("icon", "").strip()
                theme_color = request.POST.get("theme_color", "").strip()
                ingest_fields = _extract_app_ingest_fields(request.POST)
                if app.slug == "approtas" and (not ingest_fields["ingest_client_id"] or not ingest_fields["ingest_agent_id"]):
                    message = "Para o app Rotas, informe client_id e agent_id da ingest."
                    message_level = "error"
                    apps = App.objects.all().order_by("nome")
                    return render(
                        request,
                        "core/apps_gerenciar.html",
                        {
                            "apps": apps,
                            "message": message,
                            "message_level": message_level,
                        },
                    )
                if nome:
                    app.nome = nome
                app.descricao = descricao
                app.icon = icon
                app.theme_color = theme_color
                app.ingest_client_id = ingest_fields["ingest_client_id"]
                app.ingest_agent_id = ingest_fields["ingest_agent_id"]
                app.ingest_source = ingest_fields["ingest_source"]
                app.save(
                    update_fields=[
                        "nome",
                        "descricao",
                        "icon",
                        "theme_color",
                        "ingest_client_id",
                        "ingest_agent_id",
                        "ingest_source",
                    ]
                )
                return redirect("apps_gerenciar")
        if action == "toggle_app":
            app_id = request.POST.get("app_id")
            app = App.objects.filter(pk=app_id).first()
            if app:
                app.ativo = not app.ativo
                app.save(update_fields=["ativo"])
                return redirect("apps_gerenciar")
        if action == "delete_app":
            app_id = request.POST.get("app_id")
            app = App.objects.filter(pk=app_id).first()
            if app:
                app.delete()
                return redirect("apps_gerenciar")
    apps = App.objects.all().order_by("nome")
    return render(
        request,
        "core/apps_gerenciar.html",
        {
            "apps": apps,
            "message": message,
            "message_level": message_level,
        },
    )


@login_required
def colaboradores_gerenciar(request):
    if not _is_dev_user(request.user):
        return HttpResponseForbidden("Sem permissao.")
    cliente = _get_cliente(request.user)
    if not cliente:
        return HttpResponseForbidden("Sem cadastro de cliente.")

    message = request.GET.get("msg", "").strip() or None
    message_level = request.GET.get("level", "").strip() or "info"

    if request.method == "POST":
        action = (request.POST.get("action") or "").strip()
        if action == "create_colaborador":
            nome = " ".join((request.POST.get("nome", "") or "").strip().split())[:120]
            cargo = " ".join((request.POST.get("cargo", "") or "").strip().split())[:120]
            if not nome:
                message = "Informe o nome do colaborador."
                message_level = "error"
            elif RadarColaborador.objects.filter(perfil=cliente, nome__iexact=nome).exists():
                message = "Colaborador ja cadastrado para este perfil."
                message_level = "warning"
            else:
                RadarColaborador.objects.create(
                    perfil=cliente,
                    nome=nome,
                    cargo=cargo,
                    ativo=True,
                )
                return redirect(
                    f"{reverse('colaboradores_gerenciar')}?{urlencode({'msg': 'Colaborador cadastrado.', 'level': 'success'})}"
                )
        if action == "update_colaborador":
            colaborador_id = request.POST.get("colaborador_id")
            colaborador = RadarColaborador.objects.filter(pk=colaborador_id, perfil=cliente).first()
            if not colaborador:
                message = "Colaborador nao encontrado."
                message_level = "error"
            else:
                nome = " ".join((request.POST.get("nome", "") or "").strip().split())[:120]
                cargo = " ".join((request.POST.get("cargo", "") or "").strip().split())[:120]
                if not nome:
                    message = "Informe o nome do colaborador."
                    message_level = "error"
                else:
                    duplicate_qs = RadarColaborador.objects.filter(perfil=cliente, nome__iexact=nome).exclude(pk=colaborador.pk)
                    if duplicate_qs.exists():
                        message = "Ja existe colaborador com esse nome."
                        message_level = "warning"
                    else:
                        colaborador.nome = nome
                        colaborador.cargo = cargo
                        colaborador.save(update_fields=["nome", "cargo", "atualizado_em"])
                        return redirect(
                            f"{reverse('colaboradores_gerenciar')}?{urlencode({'msg': 'Colaborador atualizado.', 'level': 'success'})}"
                        )
        if action == "toggle_colaborador":
            colaborador_id = request.POST.get("colaborador_id")
            colaborador = RadarColaborador.objects.filter(pk=colaborador_id, perfil=cliente).first()
            if colaborador:
                colaborador.ativo = not colaborador.ativo
                colaborador.save(update_fields=["ativo", "atualizado_em"])
                status_msg = "ativado" if colaborador.ativo else "desativado"
                return redirect(
                    f"{reverse('colaboradores_gerenciar')}?{urlencode({'msg': f'Colaborador {status_msg}.', 'level': 'success'})}"
                )
            message = "Colaborador nao encontrado."
            message_level = "error"
        if action == "delete_colaborador":
            colaborador_id = request.POST.get("colaborador_id")
            colaborador = RadarColaborador.objects.filter(pk=colaborador_id, perfil=cliente).first()
            if colaborador:
                colaborador.delete()
                return redirect(
                    f"{reverse('colaboradores_gerenciar')}?{urlencode({'msg': 'Colaborador removido.', 'level': 'success'})}"
                )
            message = "Colaborador nao encontrado."
            message_level = "error"

    colaboradores = RadarColaborador.objects.filter(perfil=cliente).order_by("nome", "id")
    return render(
        request,
        "core/colaboradores_gerenciar.html",
        {
            "colaboradores": colaboradores,
            "message": message,
            "message_level": message_level,
        },
    )


def register(request):
    if request.user.is_authenticated:
        return redirect(_get_safe_next_url(request))
    message = None
    next_url = _get_safe_next_url(request)
    form = RegisterForm()
    if request.method == "POST":
        form = RegisterForm(request.POST)
        next_url = _get_safe_next_url(request)
        if form.is_valid():
            user = form.save()
            nome = form.cleaned_data["nome"].strip()
            empresa = form.cleaned_data.get("empresa", "").strip()
            perfil = PerfilUsuario.objects.create(
                nome=nome,
                email=user.email,
                usuario=user,
                ativo=True,
                empresa=empresa,
            )
            _ensure_default_cadernos(perfil)
            authenticated = authenticate(request, username=user.username, password=form.cleaned_data["senha"])
            if authenticated:
                login(request, authenticated)
                return redirect(next_url)
            return redirect("login")
        message = "Revise os campos e tente novamente."
    return render(
        request,
        "core/register.html",
        {
            "form": form,
            "message": message,
            "next_url": next_url if next_url != reverse("painel") else "",
        },
    )


def produto_documentacao_tecnica(request):
    produto = _documentacao_tecnica_product()
    state = (request.GET.get("state") or "").strip().lower()
    requested_next = _get_safe_next_url(request, fallback="ios_list")
    entry_url = _documentacao_tecnica_entry_url()
    plans_url = _documentacao_tecnica_plans_url(next_url=requested_next, state=state or None)
    activate_url = reverse("produto_documentacao_tecnica_ativar")
    if requested_next:
        activate_url = f"{activate_url}?{urlencode({'next': requested_next})}"
    login_url = f"{reverse('login')}?{urlencode({'next': activate_url})}"
    register_url = f"{reverse('register')}?{urlencode({'next': activate_url})}"

    access_state = "disponivel"
    access = None
    entitlement = _resolve_documentacao_entitlement_safe(request.user if request.user.is_authenticated else None)
    if request.user.is_authenticated:
        access_state, access = _user_documentacao_access_state(request.user)
        if not state:
            state = {
                "starter_bloqueado": "starter_bloqueado",
            }.get(access_state, access_state)
    state = state or "disponivel"

    if state in {"required", "expirado", "bloqueado", "starter_bloqueado"}:
        return redirect(_documentacao_tecnica_plans_url(next_url=requested_next, state=state))

    message = None
    message_level = "info"
    checkout_notice = (request.COOKIES.get("product_checkout_notice") or "").strip()
    if state == "required":
        message = "Ative o trial para liberar IOs e Listas de IP."
    elif state == "expirado":
        message = "Seu trial terminou. Escolha um plano para continuar usando IOs e Listas de IP."
        message_level = "warning"
    elif state == "bloqueado":
        message = "Seu acesso a este produto esta bloqueado no momento."
        message_level = "warning"
    elif state == "starter_bloqueado":
        message = _starter_limit_error_message(request.user)
        message_level = "warning"
    if checkout_notice:
        message = checkout_notice
        message_level = "warning"

    return render(
        request,
        "core/produto_documentacao_tecnica.html",
        {
            "produto": produto,
            "access": access,
            "access_state": access_state,
            "entitlement": entitlement,
            "entry_url": entry_url,
            "plans_url": plans_url,
            "activate_url": activate_url,
            "login_url": login_url,
            "register_url": register_url,
            "requested_next": requested_next,
            "message": message,
            "message_level": message_level,
        },
    )


def produto_documentacao_tecnica_planos(request):
    produto = _documentacao_tecnica_product()
    state = (request.GET.get("state") or "").strip().lower()
    requested_next = _get_safe_next_url(request, fallback="ios_list")
    entry_url = _documentacao_tecnica_entry_url()
    activate_url = reverse("produto_documentacao_tecnica_ativar")
    if requested_next:
        activate_url = f"{activate_url}?{urlencode({'next': requested_next})}"
    login_url = f"{reverse('login')}?{urlencode({'next': activate_url})}"
    register_url = f"{reverse('register')}?{urlencode({'next': activate_url})}"

    access_state = "disponivel"
    access = None
    entitlement = _resolve_documentacao_entitlement_safe(request.user if request.user.is_authenticated else None)
    if request.user.is_authenticated:
        access_state, access = _user_documentacao_access_state(request.user)
        if not state:
            state = {
                "starter_bloqueado": "starter_bloqueado",
            }.get(access_state, access_state)
    state = state or "disponivel"

    message = None
    message_level = "info"
    checkout_notice = (request.COOKIES.get("product_checkout_notice") or "").strip()
    if state == "required":
        message = "Ative o trial para liberar IOs e Listas de IP."
    elif state == "expirado":
        message = "Seu trial terminou. Escolha um plano para continuar usando IOs e Listas de IP."
        message_level = "warning"
    elif state == "bloqueado":
        message = "Seu acesso a este produto esta bloqueado no momento."
        message_level = "warning"
    elif state == "starter_bloqueado":
        message = _starter_limit_error_message(request.user)
        message_level = "warning"
    if checkout_notice:
        message = checkout_notice
        message_level = "warning"

    return render(
        request,
        "core/produto_documentacao_tecnica_planos.html",
        {
            "produto": produto,
            "access": access,
            "access_state": access_state,
            "entitlement": entitlement,
            "entry_url": entry_url,
            "landing_url": reverse("produto_documentacao_tecnica"),
            "activate_url": activate_url,
            "login_url": login_url,
            "register_url": register_url,
            "requested_next": requested_next,
            "message": message,
            "message_level": message_level,
        },
    )


def produto_documentacao_tecnica_planos_legacy(request):
    requested_next = _get_safe_next_url(request, fallback="ios_list")
    state = (request.GET.get("state") or "").strip().lower() or None
    return redirect(_platform_plans_url(next_url=requested_next, state=state))


@login_required
def produto_documentacao_tecnica_ativar(request):
    entry_url = _get_safe_next_url(request, fallback="ios_list")
    if not _documentacao_tecnica_product():
        return _redirect_documentacao_tecnica_billing_unavailable(entry_url)
    access_state, _ = _user_documentacao_access_state(request.user)
    if access_state in {"trial_ativo", "ativo"}:
        return redirect(entry_url)
    if access_state in {"expirado", "bloqueado", "starter_bloqueado"}:
        return redirect(_documentacao_tecnica_plans_url(next_url=entry_url, state=access_state))
    activate_trial(request.user, DOCUMENTATION_PRODUCT_CODE)
    return redirect(entry_url)


@login_required
@require_POST
def produto_documentacao_tecnica_ativar_starter(request):
    next_url = _get_safe_next_url(request, fallback="ios_list")
    if not _documentacao_tecnica_product():
        return _redirect_documentacao_tecnica_billing_unavailable(next_url)
    subscription, error = activate_starter_plan(request.user, DOCUMENTATION_PRODUCT_CODE)
    if error:
        return redirect(_documentacao_tecnica_plans_url(next_url=next_url, state="starter_bloqueado"))
    if subscription:
        return redirect(next_url)
    return redirect("produtos_planos")


@login_required
@require_POST
def produto_documentacao_tecnica_checkout_professional(request):
    next_url = _get_safe_next_url(request, fallback="ios_list")
    if not _documentacao_tecnica_product():
        return _redirect_documentacao_tecnica_billing_unavailable(next_url)
    interval = (request.POST.get("interval") or request.GET.get("interval") or AssinaturaUsuario.BillingInterval.MONTHLY).strip().upper()
    if interval not in AssinaturaUsuario.BillingInterval.values:
        interval = AssinaturaUsuario.BillingInterval.MONTHLY
    subscription, error = start_professional_checkout(
        request.user,
        billing_interval=interval,
        product_code=DOCUMENTATION_PRODUCT_CODE,
    )
    if error:
        response = redirect(_documentacao_tecnica_plans_url(next_url=next_url, state="expirado"))
        response.set_cookie("product_checkout_notice", error, max_age=20)
        return response
    if subscription and subscription.checkout_url:
        return redirect(subscription.checkout_url)
    response = redirect(_documentacao_tecnica_plans_url(next_url=next_url, state="expirado"))
    response.set_cookie(
        "product_checkout_notice",
        "Assinatura profissional iniciada. Finalize o checkout quando a integracao estiver habilitada.",
        max_age=20,
    )
    return response


@csrf_exempt
def mercado_pago_webhook(request):
    if request.method != "POST":
        return HttpResponseNotAllowed(["POST"])
    payload = {}
    try:
        payload = json.loads((request.body or b"{}").decode("utf-8"))
    except (TypeError, ValueError, UnicodeDecodeError):
        payload = {}
    external_id = str(payload.get("id") or payload.get("data", {}).get("id") or timezone.now().timestamp()).strip()
    event_type = str(payload.get("type") or payload.get("action") or "unknown").strip()
    event, created = EventoPagamentoWebhook.objects.get_or_create(
        provider=ConfiguracaoPagamento.Provider.MERCADO_PAGO,
        external_id=external_id,
        defaults={
            "event_type": event_type,
            "raw_payload": payload,
            "processed": False,
        },
    )
    if not created:
        event.event_type = event_type or event.event_type
        event.raw_payload = payload
        event.save(update_fields=["event_type", "raw_payload"])
    event.processed = True
    event.processed_at = timezone.now()
    event.processing_error = ""
    event.save(update_fields=["processed", "processed_at", "processing_error"])
    return JsonResponse({"ok": True})


def pagamento_checkout_sucesso(request):
    return render(
        request,
        "core/pagamento_checkout_status.html",
        {
            "status_code": "sucesso",
            "status_title": "Pagamento confirmado",
            "status_message": "Sua assinatura foi confirmada com sucesso. Se a conciliacao automatica ainda estiver em processamento, aguarde alguns instantes e tente abrir os modulos novamente.",
            "status_note": "Se o acesso nao refletir de imediato, o webhook ou a sincronizacao do provider ainda pode estar finalizando a atualizacao.",
        },
    )


def pagamento_checkout_falha(request):
    return render(
        request,
        "core/pagamento_checkout_status.html",
        {
            "status_code": "falha",
            "status_title": "Pagamento nao concluido",
            "status_message": "O checkout retornou uma falha ou cancelamento. Voce pode revisar os dados do pagamento e tentar novamente quando quiser.",
            "status_note": "Nenhuma liberacao adicional sera aplicada enquanto a cobranca nao for confirmada pelo provider.",
        },
    )


def pagamento_checkout_pendente(request):
    return render(
        request,
        "core/pagamento_checkout_status.html",
        {
            "status_code": "pendente",
            "status_title": "Pagamento em analise",
            "status_message": "O provider informou que a cobranca ainda esta pendente. Isso pode acontecer em meios de pagamento que dependem de confirmacao posterior.",
            "status_note": "Assim que houver confirmacao, a assinatura podera ser atualizada automaticamente pelo webhook.",
        },
    )


@login_required
def ios_list(request):
    denied_response = _require_internal_module_access(request, "IOS")
    if denied_response:
        return denied_response
    cliente = _get_cliente(request.user)
    if not cliente and not _is_admin_user(request.user):
        return HttpResponseForbidden("Sem cadastro de cliente.")

    inventarios_qs = _ios_inventarios_queryset(request.user, cliente)
    locais, grupos = _ios_locais_grupos(cliente)
    message = None
    message_level = "info"
    if request.method == "POST":
        action = request.POST.get("action")
        if action == "create_rack":
            if not cliente:
                return HttpResponseForbidden("Sem cadastro de cliente.")
            nome = request.POST.get("nome", "").strip()
            descricao = request.POST.get("descricao", "").strip()
            local_id = request.POST.get("local")
            grupo_id = request.POST.get("grupo")
            id_planta_raw = request.POST.get("id_planta", "").strip()
            inventario_id = request.POST.get("inventario")
            slots_raw = request.POST.get("slots_total", "").strip()
            try:
                slots_total = int(slots_raw)
            except (TypeError, ValueError):
                slots_total = None
            if slots_total is not None:
                slots_total = max(1, min(60, slots_total))
            if nome and slots_total:
                allowed, quota_message = _can_create_more_racks(request.user, increment=1)
                if not allowed:
                    message = quota_message
                    message_level = "warning"
                else:
                    planta = None
                    if id_planta_raw:
                        planta, _ = PlantaIO.objects.get_or_create(codigo=id_planta_raw.upper())
                    inventario = None
                    if inventario_id and inventarios_qs.filter(pk=inventario_id).exists():
                        inventario = inventarios_qs.filter(pk=inventario_id).first()
                    local = None
                    if local_id and cliente:
                        local = LocalRackIO.objects.filter(pk=local_id, cliente=cliente).first()
                    grupo = None
                    if grupo_id and cliente:
                        grupo = GrupoRackIO.objects.filter(pk=grupo_id, cliente=cliente).first()
                    rack = RackIO.objects.create(
                        cliente=cliente,
                        nome=nome,
                        descricao=descricao,
                        local=local,
                        grupo=grupo,
                        id_planta=planta,
                        inventario=inventario,
                        slots_total=slots_total,
                    )
                    slots = [RackSlotIO(rack=rack, posicao=index) for index in range(1, slots_total + 1)]
                    RackSlotIO.objects.bulk_create(slots)
                    return redirect("ios_rack_detail", pk=rack.pk)
            elif not message:
                return redirect("ios_list")
        if action == "create_local":
            if not cliente:
                return HttpResponseForbidden("Sem cadastro de cliente.")
            nome = request.POST.get("local_nome", "").strip()
            if not nome:
                msg = "Informe um nome de local."
                level = "error"
                created = False
            else:
                local, created = LocalRackIO.objects.get_or_create(nome=nome, cliente=cliente)
                if created:
                    msg = "Local criado."
                    level = "success"
                else:
                    msg = "Local ja existe."
                    level = "warning"
            if request.headers.get("x-requested-with") == "XMLHttpRequest":
                return JsonResponse(
                    {
                        "ok": bool(nome),
                        "created": created,
                        "id": local.id if nome and "local" in locals() else None,
                        "nome": local.nome if nome and "local" in locals() else None,
                        "message": msg,
                        "level": level,
                    }
                )
            return redirect("ios_list")
        if action == "create_grupo":
            payload = _create_grupo_payload(request, cliente)
            if request.headers.get("x-requested-with") == "XMLHttpRequest":
                return JsonResponse(payload)
            return redirect("ios_list")
        if action == "create_channel_type":
            nome = request.POST.get("nome", "").strip().upper()
            if nome:
                TipoCanalIO.objects.get_or_create(nome=nome, defaults={"ativo": True})
            return redirect("ios_list")

    racks = _ios_racks_queryset(request.user, cliente)
    rack_groups = _ios_build_rack_groups(racks, locais=locais)
    total_grupos = sum(len(group["groups"]) for group in rack_groups)
    return render(
        request,
        "core/ios_list.html",
        {
            "rack_groups": rack_groups,
            "can_manage": bool(cliente),
            "total_locais": len(rack_groups),
            "total_grupos": total_grupos,
            "total_racks": racks.count(),
            "inventarios": inventarios_qs.order_by("nome"),
            "locais": locais,
            "grupos": grupos,
            "message": message,
            "message_level": message_level,
            "commercial_status": _documentacao_tecnica_status_context(request.user),
            "commercial_plans_url": _documentacao_tecnica_plans_url(next_url=request.path),
            **_io_import_upload_context(request, cliente),
        },
    )


@login_required
def ios_search(request):
    denied_response = _require_internal_module_access(request, "IOS")
    if denied_response:
        return denied_response
    cliente = _get_cliente(request.user)
    if not cliente and not _is_admin_user(request.user):
        return HttpResponseForbidden("Sem cadastro de cliente.")

    inventarios_qs = _ios_inventarios_queryset(request.user, cliente)
    message = None
    message_level = "info"
    if request.method == "POST":
        action = request.POST.get("action")
        if action == "create_rack":
            if not cliente:
                return HttpResponseForbidden("Sem cadastro de cliente.")
            nome = request.POST.get("nome", "").strip()
            descricao = request.POST.get("descricao", "").strip()
            local_id = request.POST.get("local")
            grupo_id = request.POST.get("grupo")
            id_planta_raw = request.POST.get("id_planta", "").strip()
            inventario_id = request.POST.get("inventario")
            slots_raw = request.POST.get("slots_total", "").strip()
            try:
                slots_total = int(slots_raw)
            except (TypeError, ValueError):
                slots_total = None
            if slots_total is not None:
                slots_total = max(1, min(60, slots_total))
            if nome and slots_total:
                allowed, quota_message = _can_create_more_racks(request.user, increment=1)
                if not allowed:
                    message = quota_message
                    message_level = "warning"
                else:
                    planta = None
                    if id_planta_raw:
                        planta, _ = PlantaIO.objects.get_or_create(codigo=id_planta_raw.upper())
                    inventario = None
                    if inventario_id and inventarios_qs.filter(pk=inventario_id).exists():
                        inventario = inventarios_qs.filter(pk=inventario_id).first()
                    local = None
                    if local_id and cliente:
                        local = LocalRackIO.objects.filter(pk=local_id, cliente=cliente).first()
                    grupo = None
                    if grupo_id and cliente:
                        grupo = GrupoRackIO.objects.filter(pk=grupo_id, cliente=cliente).first()
                    rack = RackIO.objects.create(
                        cliente=cliente,
                        nome=nome,
                        descricao=descricao,
                        local=local,
                        grupo=grupo,
                        id_planta=planta,
                        inventario=inventario,
                        slots_total=slots_total,
                    )
                    slots = [RackSlotIO(rack=rack, posicao=index) for index in range(1, slots_total + 1)]
                    RackSlotIO.objects.bulk_create(slots)
                    return redirect("ios_rack_detail", pk=rack.pk)
            elif not message:
                return redirect("ios_search")
        if action == "create_local":
            if not cliente:
                return HttpResponseForbidden("Sem cadastro de cliente.")
            nome = request.POST.get("local_nome", "").strip()
            if not nome:
                msg = "Informe um nome de local."
                level = "error"
                created = False
            else:
                local, created = LocalRackIO.objects.get_or_create(nome=nome, cliente=cliente)
                if created:
                    msg = "Local criado."
                    level = "success"
                else:
                    msg = "Local ja existe."
                    level = "warning"
            if request.headers.get("x-requested-with") == "XMLHttpRequest":
                return JsonResponse(
                    {
                        "ok": bool(nome),
                        "created": created,
                        "id": local.id if nome and "local" in locals() else None,
                        "nome": local.nome if nome and "local" in locals() else None,
                        "message": msg,
                        "level": level,
                    }
                )
            return redirect("ios_search")
        if action == "create_grupo":
            payload = _create_grupo_payload(request, cliente)
            if request.headers.get("x-requested-with") == "XMLHttpRequest":
                return JsonResponse(payload)
            return redirect("ios_search")

    racks = _ios_racks_queryset(request.user, cliente)
    locais, grupos = _ios_locais_grupos(cliente)
    search_term = request.GET.get("q", "").strip()
    rack_filter = request.GET.get("rack", "").strip()
    local_filter = request.GET.get("local", "").strip()
    grupo_filter = request.GET.get("grupo", "").strip()
    search_results = _ios_search_channels(
        racks,
        search_term=search_term,
        rack_filter=rack_filter,
        local_filter=local_filter,
        grupo_filter=grupo_filter,
    )
    search_count = len(search_results)
    if request.headers.get("x-requested-with") == "XMLHttpRequest":
        return JsonResponse(
            {
                "count": search_count,
                "results": _ios_search_payload(search_results),
            }
        )

    return render(
        request,
        "core/ios_search.html",
        {
            "can_manage": bool(cliente),
            "racks": racks.order_by("nome"),
            "locais": locais,
            "grupos": grupos,
            "search_term": search_term,
            "rack_filter": rack_filter,
            "local_filter": local_filter,
            "grupo_filter": grupo_filter,
            "search_results": search_results,
            "search_count": search_count,
            "message": message,
            "message_level": message_level,
            "commercial_status": _documentacao_tecnica_status_context(request.user),
            **_io_import_upload_context(request, cliente),
        },
    )


@login_required
def ios_rack_new(request):
    denied_response = _require_internal_module_access(request, "IOS")
    if denied_response:
        return denied_response
    cliente = _get_cliente(request.user)
    if not cliente and not _is_admin_user(request.user):
        return HttpResponseForbidden("Sem cadastro de cliente.")

    inventarios_qs = _ios_inventarios_queryset(request.user, cliente)
    locais, grupos = _ios_locais_grupos(cliente)
    message = None
    message_level = "info"

    if request.method == "POST":
        action = request.POST.get("action")
        if action == "create_rack":
            if not cliente:
                return HttpResponseForbidden("Sem cadastro de cliente.")
            nome = request.POST.get("nome", "").strip()
            descricao = request.POST.get("descricao", "").strip()
            local_id = request.POST.get("local")
            grupo_id = request.POST.get("grupo")
            id_planta_raw = request.POST.get("id_planta", "").strip()
            inventario_id = request.POST.get("inventario")
            slots_raw = request.POST.get("slots_total", "").strip()
            try:
                slots_total = int(slots_raw)
            except (TypeError, ValueError):
                slots_total = None
            if slots_total is not None:
                slots_total = max(1, min(60, slots_total))
            if nome and slots_total:
                allowed, quota_message = _can_create_more_racks(request.user, increment=1)
                if not allowed:
                    message = quota_message
                    message_level = "warning"
                else:
                    planta = None
                    if id_planta_raw:
                        planta, _ = PlantaIO.objects.get_or_create(codigo=id_planta_raw.upper())
                    inventario = None
                    if inventario_id and inventarios_qs.filter(pk=inventario_id).exists():
                        inventario = inventarios_qs.filter(pk=inventario_id).first()
                    local = None
                    if local_id and cliente:
                        local = LocalRackIO.objects.filter(pk=local_id, cliente=cliente).first()
                    grupo = None
                    if grupo_id and cliente:
                        grupo = GrupoRackIO.objects.filter(pk=grupo_id, cliente=cliente).first()
                    rack = RackIO.objects.create(
                        cliente=cliente,
                        nome=nome,
                        descricao=descricao,
                        local=local,
                        grupo=grupo,
                        id_planta=planta,
                        inventario=inventario,
                        slots_total=slots_total,
                    )
                    slots = [RackSlotIO(rack=rack, posicao=index) for index in range(1, slots_total + 1)]
                    RackSlotIO.objects.bulk_create(slots)
                    return redirect("ios_rack_detail", pk=rack.pk)
        if action == "create_local":
            if not cliente:
                return HttpResponseForbidden("Sem cadastro de cliente.")
            nome = request.POST.get("local_nome", "").strip()
            if not nome:
                msg = "Informe um nome de local."
                level = "error"
                created = False
            else:
                local, created = LocalRackIO.objects.get_or_create(nome=nome, cliente=cliente)
                if created:
                    msg = "Local criado."
                    level = "success"
                else:
                    msg = "Local ja existe."
                    level = "warning"
            if request.headers.get("x-requested-with") == "XMLHttpRequest":
                return JsonResponse(
                    {
                        "ok": bool(nome),
                        "created": created,
                        "id": local.id if nome and "local" in locals() else None,
                        "nome": local.nome if nome and "local" in locals() else None,
                        "message": msg,
                        "level": level,
                    }
                )
            return redirect("ios_rack_new")
        if action == "create_grupo":
            payload = _create_grupo_payload(request, cliente)
            if request.headers.get("x-requested-with") == "XMLHttpRequest":
                return JsonResponse(payload)
            return redirect("ios_rack_new")

    return render(
        request,
        "core/ios_rack_new.html",
        {
            "can_manage": bool(cliente),
            "inventarios": inventarios_qs.order_by("nome"),
            "locais": locais,
            "grupos": grupos,
            "message": message,
            "message_level": message_level,
            "commercial_status": _documentacao_tecnica_status_context(request.user),
        },
    )


@login_required
def ios_rack_detail(request, pk):
    denied_response = _require_internal_module_access(request, "IOS")
    if denied_response:
        return denied_response
    cliente = _get_cliente(request.user)
    if not cliente and not _is_admin_user(request.user):
        return HttpResponseForbidden("Sem cadastro de cliente.")
    if _is_admin_user(request.user) and not cliente:
        inventarios_qs = Inventario.objects.all()
    else:
        inventarios_qs = Inventario.objects.filter(
            Q(cliente=cliente) | Q(id_inventario__in=cliente.inventarios.all())
        )
    locais = LocalRackIO.objects.none()
    grupos = GrupoRackIO.objects.none()
    if cliente:
        locais = LocalRackIO.objects.filter(cliente=cliente).order_by("nome")
        grupos = GrupoRackIO.objects.filter(cliente=cliente).order_by("nome")
    if cliente:
        rack = get_object_or_404(
            RackIO,
            Q(pk=pk),
            Q(cliente=cliente) | Q(id_planta__in=cliente.plantas.all()),
        )
    else:
        rack = get_object_or_404(RackIO, pk=pk)
    can_manage = bool(
        _is_admin_user(request.user)
        or (
            cliente
            and (
                rack.cliente_id == cliente.id
                or (rack.id_planta_id and cliente.plantas.filter(pk=rack.id_planta_id).exists())
            )
        )
    )
    message = None
    selected_module_id = ""

    def get_rack_module_or_404(module_id):
        return get_object_or_404(
            ModuloRackIO.objects.select_related("modulo_modelo", "rack", "modulo_modelo__tipo_base"),
            pk=module_id,
            rack=rack,
        )

    def redirect_to_selected_module(module_id):
        return redirect(_ios_module_panel_url(rack.pk, module_id))

    if request.method == "POST":
        action = request.POST.get("action")
        if action in {
            "update_rack",
            "delete_rack",
            "add_first",
            "add_to_slot",
            "assign_modules",
            "remove_from_slot",
            "move_left",
            "move_right",
            "update_selected_module",
            "delete_selected_module",
            "bulk_update_channels",
            "inline_update_channel",
        } and not can_manage:
            return HttpResponseForbidden("Sem permissao.")
        if action == "create_local":
            if not cliente:
                return HttpResponseForbidden("Sem cadastro de cliente.")
            nome = request.POST.get("local_nome", "").strip()
            if not nome:
                msg = "Informe um nome de local."
                level = "error"
                created = False
            else:
                local, created = LocalRackIO.objects.get_or_create(nome=nome, cliente=cliente)
                if created:
                    msg = "Local criado."
                    level = "success"
                else:
                    msg = "Local ja existe."
                    level = "warning"
            if request.headers.get("x-requested-with") == "XMLHttpRequest":
                return JsonResponse(
                    {
                        "ok": bool(nome),
                        "created": created,
                        "id": local.id if nome and "local" in locals() else None,
                        "nome": local.nome if nome and "local" in locals() else None,
                        "message": msg,
                        "level": level,
                    }
                )
            return redirect("ios_rack_detail", pk=rack.pk)
        if action == "create_grupo":
            payload = _create_grupo_payload(request, cliente)
            if request.headers.get("x-requested-with") == "XMLHttpRequest":
                return JsonResponse(payload)
            return redirect("ios_rack_detail", pk=rack.pk)
        if action == "update_selected_module":
            module_id = request.POST.get("module_id")
            module = get_rack_module_or_404(module_id)
            current_slot = RackSlotIO.objects.filter(rack=rack, modulo=module).first()
            target_slot_id = request.POST.get("slot_id")
            moved = False
            if target_slot_id:
                target_slot = get_object_or_404(RackSlotIO, pk=target_slot_id, rack=rack)
                if not target_slot.modulo_id:
                    if current_slot:
                        current_slot.modulo = None
                        current_slot.save(update_fields=["modulo"])
                    target_slot.modulo = module
                    target_slot.save(update_fields=["modulo"])
                    moved = True
            if request.headers.get("x-requested-with") == "XMLHttpRequest":
                current_slot = RackSlotIO.objects.filter(rack=rack, modulo=module).first()
                return JsonResponse(
                    {
                        "ok": True,
                        "module_id": module.id,
                        "name": module.modulo_modelo.modelo or module.modulo_modelo.nome,
                        "slot_pos": current_slot.posicao if current_slot else None,
                        "moved": moved,
                    }
                )
            return redirect_to_selected_module(module.id)
        if action == "delete_selected_module":
            module_id = request.POST.get("module_id")
            module = get_rack_module_or_404(module_id)
            module.delete()
            if request.headers.get("x-requested-with") == "XMLHttpRequest":
                return JsonResponse({"ok": True, "deleted_module_id": int(module_id)})
            return redirect("ios_rack_detail", pk=rack.pk)
        if action == "bulk_update_channels":
            module_id = request.POST.get("module_id")
            module = get_rack_module_or_404(module_id)
            channel_ids = request.POST.getlist("channel_id")
            channels_qs = module.canais.filter(id__in=channel_ids)
            channels_map = {str(channel.id): channel for channel in channels_qs}
            for channel_id in channel_ids:
                channel = channels_map.get(channel_id)
                if not channel:
                    continue
                tag_raw = request.POST.get(f"tag_{channel_id}", "")
                descricao_raw = request.POST.get(f"descricao_{channel_id}", "")
                tipo_id = request.POST.get(f"tipo_{channel_id}")
                comissionado = request.POST.get(f"comissionado_{channel_id}") == "on"
                channel.tag = _normalize_channel_tag(tag_raw)
                channel.descricao = (descricao_raw or "").strip()
                if tipo_id:
                    channel.tipo_id = tipo_id
                channel.comissionado = comissionado
            if channels_map:
                CanalRackIO.objects.bulk_update(
                    channels_map.values(),
                    ["tag", "descricao", "tipo_id", "comissionado"],
                )
            if request.headers.get("x-requested-with") == "XMLHttpRequest":
                return JsonResponse({"ok": True, "updated": len(channels_map), "module_id": module.id})
            return redirect_to_selected_module(module.id)
        if action == "inline_update_channel":
            module_id = request.POST.get("module_id")
            module = get_rack_module_or_404(module_id)
            channel_id = request.POST.get("channel_id")
            channel = get_object_or_404(module.canais, pk=channel_id)
            tag_raw = request.POST.get("tag", "")
            descricao_raw = request.POST.get("descricao", "")
            tipo_id = request.POST.get("tipo")
            comissionado = request.POST.get("comissionado") == "on"
            channel.tag = _normalize_channel_tag(tag_raw)
            channel.descricao = (descricao_raw or "").strip()
            if tipo_id:
                channel.tipo_id = tipo_id
            channel.comissionado = comissionado
            channel.save(update_fields=["tag", "descricao", "tipo_id", "comissionado"])
            if request.headers.get("x-requested-with") == "XMLHttpRequest":
                return JsonResponse({"ok": True, "module_id": module.id, "channel_id": channel.id})
            return redirect_to_selected_module(module.id)
        if action == "update_rack":
            nome = request.POST.get("nome", "").strip()
            descricao = request.POST.get("descricao", "").strip()
            local_id = request.POST.get("local")
            grupo_id = request.POST.get("grupo")
            id_planta_raw = request.POST.get("id_planta", "").strip()
            inventario_id = request.POST.get("inventario") if "inventario" in request.POST else None
            slots_raw = request.POST.get("slots_total", "").strip()
            try:
                slots_total = int(slots_raw)
            except (TypeError, ValueError):
                slots_total = None
            if slots_total is not None:
                slots_total = max(1, min(60, slots_total))
            if nome:
                rack.nome = nome
            rack.descricao = descricao
            if local_id and cliente:
                rack.local = LocalRackIO.objects.filter(pk=local_id, cliente=cliente).first()
            else:
                rack.local = None
            if grupo_id and cliente:
                rack.grupo = GrupoRackIO.objects.filter(pk=grupo_id, cliente=cliente).first()
            else:
                rack.grupo = None
            if id_planta_raw:
                planta, _ = PlantaIO.objects.get_or_create(codigo=id_planta_raw.upper())
                rack.id_planta = planta
            else:
                rack.id_planta = None
            if "inventario" in request.POST:
                if inventario_id and inventarios_qs.filter(pk=inventario_id).exists():
                    rack.inventario = inventarios_qs.filter(pk=inventario_id).first()
                else:
                    rack.inventario = None
            if slots_total is not None and slots_total != rack.slots_total:
                if slots_total > rack.slots_total:
                    novos = [
                        RackSlotIO(rack=rack, posicao=index)
                        for index in range(rack.slots_total + 1, slots_total + 1)
                    ]
                    RackSlotIO.objects.bulk_create(novos)
                else:
                    slots_para_remover = rack.slots.filter(posicao__gt=slots_total).order_by("posicao")
                    if slots_para_remover.filter(modulo__isnull=False).exists():
                        message = "Nao foi possivel reduzir: existem slots ocupados acima do novo limite."
                        slots = rack.slots.select_related("modulo", "modulo__modulo_modelo").order_by("posicao")
                        modules = (
                            ModuloIO.objects.filter(Q(cliente=rack.cliente) | Q(is_default=True))
                            .select_related("tipo_base")
                            .order_by("nome")
                        )
                        channel_types = list(TipoCanalIO.objects.filter(ativo=True).order_by("nome"))
                        channel_types_data = [
                            {"id": channel_type.id, "nome": channel_type.nome} for channel_type in channel_types
                        ]
                        vacant_slots = list(
                            RackSlotIO.objects.filter(rack=rack, modulo__isnull=True)
                            .order_by("posicao")
                            .values("id", "posicao")
                        )
                        module_editor_data = _ios_build_module_editor_data(slots, channel_types)
                        module_channels = _ios_build_module_channels_summary(
                            module_editor_data,
                            channel_types_data,
                        )
                        ocupados = rack.slots.filter(modulo__isnull=False).count()
                        slots_livres = max(rack.slots_total - ocupados, 0)
                        return render(
                            request,
                            "core/ios_rack_detail.html",
                            {
                                "rack": rack,
                                "slots": slots,
                                "modules": modules,
                                "ocupados": ocupados,
                                "slots_livres": slots_livres,
                                "message": message,
                                "can_manage": can_manage,
                                "inventarios": inventarios_qs.order_by("nome"),
                                "locais": locais,
                                "grupos": grupos,
                                "channel_types_data": channel_types_data,
                                "vacant_slots": vacant_slots,
                                "module_editor_data": module_editor_data,
                                "module_channels": module_channels,
                                "selected_module_id": "",
                            },
                        )
                    slots_para_remover.delete()
                rack.slots_total = slots_total
            update_fields = ["nome", "descricao", "local", "grupo", "id_planta", "slots_total"]
            if "inventario" in request.POST:
                update_fields.append("inventario")
            rack.save(update_fields=update_fields)
            return redirect("ios_rack_detail", pk=rack.pk)
        if action == "delete_rack":
            rack.delete()
            return redirect("ios_list")
        if action in ["add_first", "add_to_slot"]:
            module_id = request.POST.get("module_id")
            module_modelo = get_object_or_404(
                ModuloIO.objects.filter(Q(cliente=rack.cliente) | Q(is_default=True)),
                pk=module_id,
            )
            slot = None
            if action == "add_to_slot":
                slot_id = request.POST.get("slot_id")
                slot = get_object_or_404(RackSlotIO, pk=slot_id, rack=rack)
                if slot.modulo_id:
                    return redirect("ios_rack_detail", pk=rack.pk)
            else:
                slot = RackSlotIO.objects.filter(rack=rack, modulo__isnull=True).order_by("posicao").first()
            if slot:
                modulo = ModuloRackIO.objects.create(
                    rack=rack,
                    modulo_modelo=module_modelo,
                )
                canais = [
                    CanalRackIO(
                        modulo=modulo,
                        indice=index,
                        descricao="",
                        tipo=module_modelo.tipo_base,
                    )
                    for index in range(1, module_modelo.quantidade_canais + 1)
                ]
                CanalRackIO.objects.bulk_create(canais)
                slot.modulo = modulo
                slot.save(update_fields=["modulo"])
            return redirect("ios_rack_detail", pk=rack.pk)
        if action == "assign_modules":
            modules_qs = ModuloIO.objects.filter(Q(cliente=rack.cliente) | Q(is_default=True))
            for key, value in request.POST.items():
                if not key.startswith("slot_"):
                    continue
                if not value:
                    continue
                try:
                    slot_id = int(key.split("_", 1)[1])
                except (TypeError, ValueError):
                    continue
                slot = RackSlotIO.objects.filter(pk=slot_id, rack=rack, modulo__isnull=True).first()
                if not slot:
                    continue
                module_modelo = modules_qs.filter(pk=value).first()
                if not module_modelo:
                    continue
                modulo = ModuloRackIO.objects.create(
                    rack=rack,
                    modulo_modelo=module_modelo,
                )
                canais = [
                    CanalRackIO(
                        modulo=modulo,
                        indice=index,
                        descricao="",
                        tipo=module_modelo.tipo_base,
                    )
                    for index in range(1, module_modelo.quantidade_canais + 1)
                ]
                CanalRackIO.objects.bulk_create(canais)
                slot.modulo = modulo
                slot.save(update_fields=["modulo"])
            return redirect("ios_rack_detail", pk=rack.pk)
        if action == "remove_from_slot":
            slot_id = request.POST.get("slot_id")
            slot = get_object_or_404(RackSlotIO, pk=slot_id, rack=rack)
            if slot.modulo_id:
                slot.modulo.delete()
                slot.modulo = None
                slot.save(update_fields=["modulo"])
            return redirect("ios_rack_detail", pk=rack.pk)
        if action in ["move_left", "move_right"]:
            slot_id = request.POST.get("slot_id")
            slot = get_object_or_404(RackSlotIO, pk=slot_id, rack=rack)
            delta = -1 if action == "move_left" else 1
            neighbor = RackSlotIO.objects.filter(rack=rack, posicao=slot.posicao + delta).first()
            if neighbor:
                slot.modulo, neighbor.modulo = neighbor.modulo, slot.modulo
                slot.save(update_fields=["modulo"])
                neighbor.save(update_fields=["modulo"])
            return redirect("ios_rack_detail", pk=rack.pk)

    slots = (
        rack.slots.select_related("modulo", "modulo__modulo_modelo")
        .prefetch_related("modulo__canais__tipo")
        .order_by("posicao")
    )
    modulo_ids = [slot.modulo_id for slot in slots if slot.modulo_id]
    modulo_status = {}
    if modulo_ids:
        channel_counts = (
            CanalRackIO.objects.filter(modulo_id__in=modulo_ids)
            .values("modulo_id")
            .annotate(
                total=Count("id"),
                comissionados=Count("id", filter=Q(comissionado=True)),
            )
        )
        modulo_status = {
            row["modulo_id"]: (row["total"] > 0 and row["total"] == row["comissionados"])
            for row in channel_counts
        }
    for slot in slots:
        if slot.modulo_id:
            slot.modulo.all_canais_comissionados = modulo_status.get(slot.modulo_id, False)
    channel_types = list(TipoCanalIO.objects.filter(ativo=True).order_by("nome"))
    channel_types_data = [{"id": channel_type.id, "nome": channel_type.nome} for channel_type in channel_types]
    module_editor_data = _ios_build_module_editor_data(slots, channel_types)
    module_channels = _ios_build_module_channels_summary(
        module_editor_data,
        channel_types_data,
    )
    vacant_slots = list(
        RackSlotIO.objects.filter(rack=rack, modulo__isnull=True)
        .order_by("posicao")
        .values("id", "posicao")
    )
    selected_module_id_raw = request.GET.get("module", "").strip()
    if selected_module_id_raw and selected_module_id_raw in module_editor_data:
        selected_module_id = selected_module_id_raw
    modules = (
        ModuloIO.objects.filter(Q(cliente=rack.cliente) | Q(is_default=True))
        .select_related("tipo_base")
        .order_by("modelo", "id")
    )
    available_qs = (
            CanalRackIO.objects.filter(modulo__rack=rack)
        .filter(Q(descricao__isnull=True) | Q(descricao__exact=""))
        .values("tipo__nome")
        .annotate(total=Count("id"))
        .order_by("tipo__nome")
    )
    canais_disponiveis = [
        {"tipo": row["tipo__nome"], "total": row["total"]} for row in available_qs
    ]
    ocupados = rack.slots.filter(modulo__isnull=False).count()
    slots_livres = max(rack.slots_total - ocupados, 0)
    return render(
        request,
        "core/ios_rack_detail.html",
        {
            "rack": rack,
            "slots": slots,
            "modules": modules,
            "ocupados": ocupados,
            "slots_livres": slots_livres,
            "canais_disponiveis": canais_disponiveis,
            "message": message,
            "can_manage": can_manage,
            "inventarios": inventarios_qs.order_by("nome"),
            "locais": locais,
            "grupos": grupos,
            "channel_types_data": channel_types_data,
            "vacant_slots": vacant_slots,
            "module_editor_data": module_editor_data,
            "module_channels": module_channels,
            "selected_module_id": selected_module_id,
            **_io_import_upload_context(request, cliente, target_rack=rack),
        },
    )


def _render_rack_io_pdf(rack, canais):
    try:
        from reportlab.lib import colors
        from reportlab.lib.pagesizes import A4
        from reportlab.lib.utils import ImageReader
        from reportlab.pdfgen import canvas
    except ImportError:
        return None

    buffer = BytesIO()
    pdf = canvas.Canvas(buffer, pagesize=A4)
    width, height = A4

    left = 40
    right = width - 40
    y = height - 42

    pdf.setTitle(f"IOs - {rack.nome}")
    icon_path = os.path.join(os.path.dirname(__file__), "static", "core", "FAVICON_PRETO.png")

    def draw_header():
        nonlocal y
        y = height - 42
        if os.path.exists(icon_path):
            try:
                icon = ImageReader(icon_path)
                pdf.drawImage(
                    icon,
                    right - 38,
                    y - 6,
                    width=36,
                    height=36,
                    preserveAspectRatio=True,
                    mask="auto",
                )
            except Exception:
                pass
        pdf.setFont("Helvetica-Bold", 16)
        pdf.drawString(left, y, rack.nome)
        y -= 16
        pdf.setFont("Helvetica", 10)
        subtitle = "Lista completa de canais do rack"
        if rack.descricao:
            subtitle = f"{subtitle} - {rack.descricao}"
        pdf.drawString(left, y, subtitle[:100])
        y -= 14
        local_nome = rack.local.nome if getattr(rack, "local_id", None) and rack.local else "-"
        grupo_nome = rack.grupo.nome if getattr(rack, "grupo_id", None) and rack.grupo else "-"
        planta = rack.id_planta.codigo if rack.id_planta_id else "-"
        rack_info = f"Local: {local_nome} | Grupo: {grupo_nome} | ID_PLANTA: {planta} | Slots: {rack.slots_total}"
        pdf.setFont("Helvetica", 9)
        pdf.drawString(left, y, rack_info[:130])
        y -= 18

    draw_header()

    def ensure_space(required=40):
        nonlocal y
        if y < required:
            pdf.showPage()
            draw_header()

    current_slot = None
    for canal in canais:
        slot_label = f"Slot {canal.slot_pos:02d}" if canal.slot_pos else "Sem slot"
        if slot_label != current_slot:
            ensure_space(70)
            current_slot = slot_label
            pdf.setFillColorRGB(1, 0.93, 0.87)
            pdf.roundRect(left, y - 11, right - left, 18, 4, fill=1, stroke=0)
            pdf.setFillColor(colors.black)
            pdf.setFont("Helvetica-Bold", 11)
            pdf.drawString(left + 8, y - 1, current_slot)
            y -= 24
            pdf.setFont("Helvetica-Bold", 9)
            pdf.drawString(left + 8, y, "Canal")
            pdf.drawString(left + 80, y, "TAG")
            pdf.drawString(left + 210, y, "Descricao")
            pdf.drawString(left + 450, y, "Tipo")
            y -= 12

        ensure_space(30)
        descricao = canal.descricao or "-"
        tag = canal.tag or "-"
        tipo = canal.tipo.nome if canal.tipo_id else "-"
        pdf.setFont("Helvetica", 9)
        pdf.drawString(left + 8, y, f"CH{canal.indice:02d}")
        pdf.drawString(left + 80, y, str(tag)[:20])
        pdf.drawString(left + 210, y, str(descricao)[:46])
        pdf.drawString(left + 450, y, str(tipo)[:16])
        y -= 12

    pdf.save()
    buffer.seek(0)
    return buffer


def _proposta_status_label(proposta):
    if proposta.finalizada:
        return "Finalizada"
    if proposta.andamento == "EXECUTANDO":
        return "Executando"
    if proposta.aprovada is True:
        return "Aprovada"
    if proposta.aprovada is False:
        return "Reprovada"
    if proposta.valor is None or proposta.valor == 0:
        return "Levantamento"
    return "Pendente"


def _format_brl_currency(value):
    if value is None:
        return "A definir"
    number = f"{value:,.2f}"
    number = number.replace(",", "X").replace(".", ",").replace("X", ".")
    return f"R$ {number}"


def _format_ptbr_datetime(value):
    if not value:
        return ""
    return timezone.localtime(value).strftime("%d/%m/%Y %H:%M")


def _format_ptbr_date(value):
    if not value:
        return ""
    return timezone.localtime(value).strftime("%d/%m/%Y")


def _atividade_response_payload(atividade):
    agenda_dias = _atividade_agenda_dias_iso(atividade)
    colaboradores_nomes = _atividade_colaboradores_nomes(atividade)
    horas_label = ""
    if atividade.horas_trabalho is not None:
        horas_label = str(atividade.horas_trabalho)
    return {
        "ok": True,
        "id": atividade.id,
        "nome": atividade.nome,
        "descricao": atividade.descricao,
        "status": atividade.status,
        "status_label": atividade.get_status_display(),
        "horas_trabalho": horas_label,
        "inicio_execucao_display": _format_ptbr_date(atividade.inicio_execucao_em),
        "finalizada_display": _format_ptbr_date(atividade.finalizada_em),
        "agenda_dias": agenda_dias,
        "agenda_total_dias": len(agenda_dias),
        "colaborador_ids": _atividade_colaboradores_ids(atividade),
        "colaboradores": colaboradores_nomes,
        "colaboradores_label": ", ".join(colaboradores_nomes),
        "total_colaboradores": len(colaboradores_nomes),
        "ordem": atividade.ordem or 0,
    }


def _status_badge_class(status_label):
    normalized = (status_label or "").strip().lower()
    return {
        "pendente": "status-pendente",
        "executando": "status-executando",
        "aprovada": "status-aprovada",
        "reprovada": "status-reprovada",
        "finalizada": "status-finalizada",
        "levantamento": "status-levantamento",
    }.get(normalized, "status-pendente")


def _clean_text(value):
    if value is None:
        return ""
    return str(value).strip()


def _append_pdf_row(rows, label, value, mono=False, highlight=False):
    text = _clean_text(value)
    if not text:
        return
    rows.append(
        {
            "label": label,
            "value": text,
            "mono": bool(mono),
            "highlight": bool(highlight),
        }
    )


def _descricao_blocks(text):
    raw = _clean_text(text)
    if not raw:
        return [{"type": "p", "text": "Sem descricao informada."}]

    blocks = []
    bullet_items = []
    paragraph_lines = []

    def flush_paragraph():
        nonlocal paragraph_lines
        if paragraph_lines:
            merged = " ".join(line.strip() for line in paragraph_lines if line.strip()).strip()
            if merged:
                blocks.append({"type": "p", "text": merged})
            paragraph_lines = []

    def flush_bullets():
        nonlocal bullet_items
        if bullet_items:
            blocks.append({"type": "ul", "items": bullet_items[:]})
            bullet_items = []

    for line in raw.splitlines():
        item = line.strip()
        if not item:
            flush_paragraph()
            flush_bullets()
            continue
        if item.startswith("> "):
            flush_paragraph()
            bullet_text = item[2:].strip()
            if bullet_text:
                bullet_items.append(bullet_text)
            continue
        flush_bullets()
        paragraph_lines.append(item)

    flush_paragraph()
    flush_bullets()

    return blocks or [{"type": "p", "text": "Sem descricao informada."}]


def _sanitize_proposta_descricao(text):
    raw = _clean_text(text)
    if not raw:
        return ""

    lines = raw.splitlines()
    normalized = [line.strip().lower() for line in lines]
    has_old_block = (
        any(item == "origem tecnica" for item in normalized)
        and any(item.startswith("radar:") for item in normalized)
        and any(item.startswith("trabalho:") for item in normalized)
    )
    if not has_old_block:
        return raw

    prefixes = (
        "origem tecnica",
        "origem do trabalho",
        "radar:",
        "trabalho:",
        "descricao do trabalho:",
        "resumo de atividades",
        "setor:",
        "solicitante:",
        "responsavel:",
        "colaboradores:",
        "contrato:",
        "classificacao:",
        "data de registro:",
        "resumo das atividades",
    )
    cleaned_lines = []
    for line in lines:
        stripped = line.strip()
        lowered = stripped.lower()
        if not stripped:
            cleaned_lines.append("")
            continue
        if lowered.startswith(prefixes):
            continue
        if lowered.startswith("- ") and cleaned_lines and not cleaned_lines[-1].strip():
            # evita trazer lista antiga de atividades quando veio do prefill tecnico.
            continue
        cleaned_lines.append(line)

    cleaned = "\n".join(cleaned_lines).strip()
    return cleaned


def _resolve_proposta_trabalho(proposta, user):
    if not proposta.trabalho_id:
        return None, False
    trabalho = _get_radar_trabalho_acessivel(user, proposta.trabalho_id)
    if trabalho:
        return trabalho, False
    return None, True


def _first_attr(obj, names, default=None):
    for name in names:
        if hasattr(obj, name):
            value = getattr(obj, name)
            if value not in (None, ""):
                return value
    return default


def _proposta_condicoes_comerciais(proposta):
    validade = _first_attr(proposta, ("validade_dias", "validade_proposta_dias"), 10)
    prazo_ddl = _first_attr(proposta, ("prazo_pagamento_ddl", "prazo_pagamento_dias"), 30)
    condicao_pagamento = _first_attr(
        proposta,
        ("condicoes_pagamento", "condicao_pagamento"),
        "Deposito em conta nominal da contratada.",
    )

    validade_txt = f"{int(validade)} dias" if str(validade).isdigit() else str(validade)
    prazo_txt = f"{int(prazo_ddl)} DDL" if str(prazo_ddl).isdigit() else str(prazo_ddl)

    return [
        {
            "titulo": "A) Obrigacoes da Contratada (SET)",
            "itens": [
                "Fornecer mao de obra tecnica compativel com o escopo contratado.",
                "Disponibilizar ferramental basico e EPIs necessarios a execucao.",
                "Cumprir normas internas, tecnicas, eticas e legislacao vigente.",
                "Manter organizacao e limpeza nos locais de trabalho.",
                "Disponibilizar responsavel tecnico para coordenacao e acompanhamento quando aplicavel.",
                "Emitir registros/relatorios basicos de atendimento quando aplicavel.",
                "Substituir profissional cuja permanencia seja considerada inadequada pela contratante.",
            ],
        },
        {
            "titulo": "B) Obrigacoes da Contratante (Cliente)",
            "itens": [
                "Disponibilizar acesso, local e infraestrutura para execucao dos servicos.",
                "Fornecer informacoes necessarias e apoio para bom andamento do trabalho.",
                "Disponibilizar local seguro para guarda de ferramentas, materiais e instrumentos (quando aplicavel).",
                "Validar e assinar relatorios/termos de aceite quando aplicavel.",
                "Comunicar imediatamente problemas referentes a equipe e/ou condicoes de execucao.",
            ],
        },
        {
            "titulo": "C) Exclusoes (itens nao inclusos)",
            "intro": "Salvo quando descrito expressamente no escopo:",
            "itens": [
                "Licenciamento de softwares de engenharia e desenvolvimento.",
                "Fornecimento de materiais, equipamentos, laudos, projetos e paineis.",
                "Servicos civis, mecanicos ou estruturais (alvenaria, suportacoes, pipe rack, etc.).",
                "Andaimes, plataformas elevatorias, guindastes ou caminhao munck.",
                "Energia eletrica, utilidades, rede e infraestrutura predial.",
                "Ferramental especial e equipamentos de diagnostico nao citados no escopo.",
                "Expansoes estruturais de logica/arquitetura nao descritas no escopo.",
            ],
        },
        {
            "titulo": "D) Direitos sobre Softwares",
            "itens": [
                "Os softwares desenvolvidos pela SET sao cedidos a contratante exclusivamente para uso no ambiente de instalacao acordado, com a finalidade de operacao e manutencao local ou descentralizada.",
                "E vedada a reproducao, distribuicao, compartilhamento, ou utilizacao dos softwares em outros ambientes/finalidades, salvo autorizacao formal da SET.",
                "Sistemas supervisorios (SCADA) e interfaces homem-maquina (IHMs) sao fornecidos no modo Run-Time, nao incluindo licencas de desenvolvimento/edicao, salvo se especificado em contrato.",
            ],
        },
        {
            "titulo": "E) Garantia de Software",
            "itens": [
                "Garantia de 360 dias para correcoes relacionadas a falhas de programacao dentro do escopo contratado.",
                "Alteracoes por terceiros, mudancas de processo/planta, mudancas de hardware nao previstas ou uso indevido invalidam a garantia.",
            ],
        },
        {
            "titulo": "F) Tributacao",
            "itens": [
                "A contratada e optante pelo Simples Nacional, estando tributos federais e previdenciarios inclusos no preco.",
                "O ISS sera destacado conforme legislacao vigente.",
                "Nao ha necessidade de retencao de outros encargos.",
                "Se materiais forem faturados separadamente dos servicos, a contratante deve sinalizar previamente para incidencia de ICMS.",
            ],
        },
        {
            "titulo": "G) Validade e Pagamento",
            "itens": [
                f"Prazo de validade da proposta comercial: {validade_txt}.",
                f"Prazo de pagamento: {prazo_txt}.",
                f"Condicao padrao: {condicao_pagamento}",
            ],
        },
    ]


def _build_proposta_pdf_context(
    proposta,
    status_label,
    include_origem=True,
    trabalho=None,
    trabalho_indisponivel=False,
):
    origem = (trabalho if trabalho is not None else proposta.trabalho) if include_origem else None
    has_trabalho_vinculado = bool(include_origem and (proposta.trabalho_id or trabalho_indisponivel))
    origem_rows = []
    atividades = []
    if origem:
        if origem.radar and origem.radar.nome:
            origem_rows.append(("Radar", origem.radar.nome))
        if origem.nome:
            origem_rows.append(("Trabalho", origem.nome))
        if origem.descricao:
            origem_rows.append(("Descricao do trabalho", origem.descricao))
        if origem.setor:
            origem_rows.append(("Setor", origem.setor))
        if origem.solicitante:
            origem_rows.append(("Solicitante", origem.solicitante))
        if origem.responsavel:
            origem_rows.append(("Responsavel", origem.responsavel))
        colaboradores = ", ".join(_trabalho_colaboradores_nomes(origem))
        if colaboradores:
            origem_rows.append(("Colaboradores", colaboradores))
        if origem.contrato and origem.contrato.nome:
            origem_rows.append(("Contrato", origem.contrato.nome))
        if origem.classificacao and origem.classificacao.nome:
            origem_rows.append(("Classificacao", origem.classificacao.nome))
        if origem.data_registro:
            origem_rows.append(("Data de registro", origem.data_registro.strftime("%d/%m/%Y")))
        atividades = [
            {
                "nome": atividade.nome,
                "descricao": atividade.descricao or "Sem descricao",
            }
            for atividade in origem.atividades.order_by("criado_em", "id")
        ]
    if has_trabalho_vinculado and not atividades and not trabalho_indisponivel:
        atividades = [{"nome": "Sem atividades vinculadas", "descricao": ""}]
    anexos = [
        {
            "tipo": anexo.get_tipo_display(),
            "nome": os.path.basename(anexo.arquivo.name) if anexo.arquivo else "-",
        }
        for anexo in proposta.anexos.all()
    ]
    identificacao_esquerda = []
    _append_pdf_row(identificacao_esquerda, "Codigo", proposta.codigo or f"ID {proposta.id}", mono=True)
    _append_pdf_row(identificacao_esquerda, "Proposta", proposta.nome)
    _append_pdf_row(identificacao_esquerda, "Prioridade", proposta.prioridade)
    _append_pdf_row(identificacao_esquerda, "Criada em", _format_ptbr_date(proposta.criado_em))
    if proposta.decidido_em:
        _append_pdf_row(identificacao_esquerda, "Decidida em", _format_ptbr_date(proposta.decidido_em))
    if proposta.finalizada_em:
        _append_pdf_row(identificacao_esquerda, "Finalizada em", _format_ptbr_date(proposta.finalizada_em))

    identificacao_direita = []
    if proposta.cliente and proposta.cliente.nome:
        _append_pdf_row(identificacao_direita, "Para", proposta.cliente.nome)
    if proposta.cliente and proposta.cliente.email:
        _append_pdf_row(identificacao_direita, "Email (para)", proposta.cliente.email, mono=True)
    de_contato = ""
    if proposta.criada_por:
        de_contato = proposta.criada_por.email or proposta.criada_por.username
    _append_pdf_row(identificacao_direita, "De", de_contato, mono=True)
    _append_pdf_row(identificacao_direita, "Status", status_label)
    if proposta.aprovado_por:
        _append_pdf_row(identificacao_direita, "Decidida por", proposta.aprovado_por.username)

    origem_rows_fmt = [{"label": label, "value": value} for label, value in origem_rows if _clean_text(value)]
    logo_path = finders.find("core/logoset.png") or finders.find("core/FAVICON_PRETO.png")
    logo_uri = Path(logo_path).as_uri() if logo_path else ""
    descricao_limpa = _sanitize_proposta_descricao(proposta.descricao)
    return {
        "proposta": proposta,
        "status_label": status_label,
        "status_badge_class": _status_badge_class(status_label),
        "codigo": proposta.codigo or f"ID {proposta.id}",
        "valor_display": _format_brl_currency(proposta.valor),
        "criada_em_display": _format_ptbr_date(proposta.criado_em),
        "de_display": proposta.criada_por.username if proposta.criada_por else "Sistema",
        "para_nome": proposta.cliente.nome if proposta.cliente else "-",
        "para_email": proposta.cliente.email if proposta.cliente else "-",
        "identificacao_esquerda": identificacao_esquerda,
        "identificacao_direita": identificacao_direita,
        "origem_rows": origem_rows_fmt,
        "has_trabalho_vinculado": has_trabalho_vinculado,
        "trabalho_indisponivel": bool(trabalho_indisponivel),
        "atividades": atividades,
        "descricao_blocks": _descricao_blocks(descricao_limpa),
        "anexos": anexos,
        "has_observacao": bool((proposta.observacao_cliente or "").strip()),
        "observacao": (proposta.observacao_cliente or "").strip(),
        "condicoes_comerciais": _proposta_condicoes_comerciais(proposta),
        "gerado_em_display": _format_ptbr_date(timezone.now()),
        "logo_uri": logo_uri,
    }


def _render_proposta_pdf(
    proposta,
    status_label,
    include_origem=True,
    trabalho=None,
    trabalho_indisponivel=False,
):
    try:
        from weasyprint import CSS, HTML
    except ImportError:
        return None

    context = _build_proposta_pdf_context(
        proposta,
        status_label,
        include_origem=include_origem,
        trabalho=trabalho,
        trabalho_indisponivel=trabalho_indisponivel,
    )
    html = render_to_string("propostas/proposta_pdf.html", context)
    css_path = finders.find("css/proposta_pdf.css")
    stylesheets = [CSS(filename=css_path)] if css_path else None
    pdf_content = HTML(string=html, base_url=str(settings.BASE_DIR)).write_pdf(stylesheets=stylesheets)
    return BytesIO(pdf_content)


@login_required
def ios_rack_io_list(request, pk):
    denied_response = _require_internal_module_access(request, "IOS")
    if denied_response:
        return denied_response
    cliente = _get_cliente(request.user)
    if not cliente and not _is_admin_user(request.user):
        return HttpResponseForbidden("Sem cadastro de cliente.")
    if cliente:
        rack = get_object_or_404(
            RackIO,
            Q(pk=pk),
            Q(cliente=cliente) | Q(id_planta__in=cliente.plantas.all()),
        )
    else:
        rack = get_object_or_404(RackIO, pk=pk)
    slot_pos_subquery = RackSlotIO.objects.filter(modulo_id=OuterRef("modulo_id")).values("posicao")[:1]
    canais = (
        CanalRackIO.objects.filter(modulo__rack=rack)
        .select_related("modulo", "modulo__modulo_modelo", "tipo")
        .annotate(slot_pos=Subquery(slot_pos_subquery))
        .order_by("slot_pos", "indice")
    )
    if request.GET.get("format") == "pdf":
        pdf_buffer = _render_rack_io_pdf(rack, canais)
        if not pdf_buffer:
            return HttpResponse("Biblioteca de PDF indisponivel (reportlab).", status=500)
        filename = f"ios_{rack.nome}".strip().replace(" ", "_")
        response = HttpResponse(pdf_buffer.getvalue(), content_type="application/pdf")
        response["Content-Disposition"] = f'attachment; filename="{filename}.pdf"'
        return response
    return render(
        request,
        "core/ios_rack_io_list.html",
        {
            "rack": rack,
            "canais": canais,
        },
    )


@login_required
def ios_import_create(request):
    ajax_request = _is_ajax_request(request)
    denied_response = _require_internal_module_access(request, "IOS")
    if denied_response:
        if ajax_request:
            return _json_error_response(
                _io_import_user_message(
                    request.user,
                    "Seu acesso atual nao permite importar planilhas de IO.",
                    "Seu acesso atual nao permite usar a importacao de IO.",
                ),
                status=403,
            )
        return denied_response
    if request.method != "POST":
        if ajax_request:
            return _json_error_response("Metodo nao permitido para esta operacao.", status=405)
        return HttpResponseNotAllowed(["POST"])
    cliente = _get_cliente(request.user)
    if not _io_import_can_manage(request, cliente):
        if ajax_request:
            return _json_error_response(
                _io_import_user_message(
                    request.user,
                    "Sem permissao para importar planilhas de IO.",
                    "Voce nao possui permissao para usar a importacao de IO.",
                ),
                status=403,
            )
        return HttpResponseForbidden("Sem permissao.")

    arquivo = request.FILES.get("arquivo")
    if not arquivo:
        if ajax_request:
            return _json_error_response("Selecione um arquivo para enviar.", status=400)
        return HttpResponse("Arquivo obrigatorio.", status=400)

    job_cliente = cliente
    if not job_cliente:
        if ajax_request:
            return _json_error_response("Conta sem cliente associado para importar IO.", status=400)
        return HttpResponse("Conta sem cliente associado para importar IO.", status=400)

    raw_bytes = arquivo.read()
    arquivo.seek(0)

    job = None
    redirect_url = None
    try:
        job = IOImportJob.objects.create(
            created_by=request.user,
            cliente=job_cliente,
            target_rack=None,
            requested_local=None,
            requested_grupo=None,
            requested_inventario=None,
            requested_rack_name="",
            requested_planta_code="",
            mode=IOImportJob.Mode.CREATE_RACK,
            original_filename=arquivo.name,
            source_file=arquivo,
            file_sha256=build_file_sha256(raw_bytes),
            progress_payload=_initial_io_import_progress_payload(arquivo.name),
        )
        redirect_url = reverse("ios_import_detail", kwargs={"pk": job.pk})
        if ajax_request:
            transaction.on_commit(lambda job_id=job.pk: _spawn_io_import_job_processor_safe(job_id))
            return JsonResponse(
                {
                    "ok": True,
                    "job_id": job.pk,
                    "status_url": reverse("ios_import_status", kwargs={"pk": job.pk}),
                    "redirect_url": redirect_url,
                }
            )
        _reprocess_io_import_job(job)
    except IOImportError as exc:
        if not job:
            logger.warning(
                "IO import failed before job persistence with validation error",
                extra={"user_id": request.user.id, "upload_name": arquivo.name},
            )
            if ajax_request:
                return _json_error_response(
                    _io_import_user_message(
                        request.user,
                        str(exc),
                        "Nao foi possivel analisar o arquivo enviado. Revise o arquivo e tente novamente.",
                    ),
                    status=400,
                )
            return HttpResponse(str(exc), status=400)
        job.status = IOImportJob.Status.FAILED
        job.ai_status = IOImportJob.AIStatus.FAILED
        job.ai_error = str(exc)
        job.warnings = [str(exc)]
        job.progress_payload = _failed_io_import_progress_payload(str(exc), job.progress_payload)
        job.save(update_fields=["status", "ai_status", "ai_error", "warnings", "progress_payload", "updated_at"])
    except Exception as exc:
        logger.exception(
            "Unhandled IO import error while creating import job",
            extra={"user_id": request.user.id, "job_id": job.pk if job else None, "upload_name": arquivo.name},
        )
        if job:
            job.status = IOImportJob.Status.FAILED
            job.ai_status = IOImportJob.AIStatus.FAILED
            job.ai_error = str(exc)
            job.warnings = [f"Falha interna ao analisar a planilha: {exc}"]
            job.progress_payload = _failed_io_import_progress_payload(str(exc), job.progress_payload)
            job.save(update_fields=["status", "ai_status", "ai_error", "warnings", "progress_payload", "updated_at"])
        if ajax_request:
            return _json_error_response(
                _io_import_user_message(
                    request.user,
                    "Falha interna ao analisar a planilha. Verifique o log do servidor.",
                    "Nao foi possivel concluir a analise da planilha agora. Tente novamente em instantes.",
                ),
                status=500,
                job_id=job.pk if job else None,
                redirect_url=redirect_url,
            )
        if job:
            return redirect("ios_import_detail", pk=job.pk)
        return HttpResponse("Falha interna ao criar a importacao.", status=500)
    redirect_url = redirect_url or reverse("ios_import_detail", kwargs={"pk": job.pk})
    if ajax_request:
        return JsonResponse({"ok": True, "redirect_url": redirect_url})
    return redirect("ios_import_detail", pk=job.pk)


@login_required
def ios_import_status(request, pk):
    denied_response = _require_internal_module_access(request, "IOS")
    if denied_response:
        return _json_error_response("Seu acesso atual nao permite consultar esta importacao.", status=403)
    cliente = _get_cliente(request.user)
    job = get_object_or_404(_build_io_import_job_queryset(request, cliente), pk=pk)
    processing = job.status == IOImportJob.Status.UPLOADED
    failed = job.status == IOImportJob.Status.FAILED
    message = ""
    progress = _build_io_import_status_progress(job)
    if failed and not _is_dev_user(request.user):
        progress["title"] = "Analise interrompida"
        progress["message"] = "Nao foi possivel concluir a analise da planilha. Revise o arquivo e tente novamente."
    if failed:
        technical_message = (job.warnings or [job.ai_error or "Falha ao processar a importacao."])[0]
        message = _io_import_user_message(
            request.user,
            technical_message,
            "Nao foi possivel concluir a analise da planilha. Revise o arquivo e tente novamente.",
        )
    elif processing:
        message = progress.get("message") or "A planilha esta sendo analisada em segundo plano."
    return JsonResponse(
        {
            "ok": True,
            "job_id": job.pk,
            "status": job.status,
            "status_display": job.get_status_display(),
            "processing": processing,
            "complete": not processing,
            "failed": failed,
            "redirect_url": reverse("ios_import_detail", kwargs={"pk": job.pk}),
            "message": message,
            "warnings_count": len(job.warnings or []) if _is_dev_user(request.user) else 0,
            "ai_status": job.ai_status if _is_dev_user(request.user) else "",
            "progress": progress,
        }
    )


@login_required
def ios_import_detail(request, pk):
    denied_response = _require_internal_module_access(request, "IOS")
    if denied_response:
        return denied_response
    cliente = _get_cliente(request.user)
    job = get_object_or_404(_build_io_import_job_queryset(request, cliente), pk=pk)

    if request.method == "POST":
        action = request.POST.get("action")
        if action == "reanalyze":
            _reprocess_io_import_job(job)
            return redirect("ios_import_detail", pk=job.pk)
        if action == "apply_import_rack":
            rack_key = (request.POST.get("rack_key") or "").strip()
            if not rack_key:
                warnings = list(job.warnings or [])
                warnings.append("Nenhum rack foi selecionado para aplicacao.")
                job.warnings = warnings
                job.save(update_fields=["warnings", "updated_at"])
                return redirect("ios_import_detail", pk=job.pk)
            applied_rack_key_map = dict((job.apply_log or {}).get("applied_rack_keys") or {})
            increment = 0 if rack_key in applied_rack_key_map else 1
            allowed, quota_message = _can_create_more_racks(request.user, increment=increment)
            if not allowed:
                warnings = list(job.warnings or [])
                warnings.append(quota_message)
                job.warnings = warnings
                job.save(update_fields=["warnings", "updated_at"])
                return redirect("ios_import_detail", pk=job.pk)
            try:
                apply_import_job(
                    job=job,
                    user=request.user,
                    rack_model=RackIO,
                    rack_slot_model=RackSlotIO,
                    rack_module_model=ModuloRackIO,
                    channel_model=CanalRackIO,
                    module_qs=_io_import_module_catalog(job.cliente),
                    plant_model=PlantaIO,
                    selected_rack_keys=[rack_key],
                )
                return redirect("ios_import_detail", pk=job.pk)
            except IOImportError as exc:
                warnings = list(job.warnings or [])
                warnings.append(str(exc))
                job.warnings = warnings
                job.save(update_fields=["warnings", "updated_at"])
                return redirect("ios_import_detail", pk=job.pk)
        if action == "apply_import":
            preview_racks, _preview_payload = _build_io_import_preview_racks(
                job.proposal_payload or {},
                applied_rack_key_map=dict((job.apply_log or {}).get("applied_rack_keys") or {}),
            )
            pending_count = sum(1 for rack in preview_racks if not rack.get("is_applied"))
            allowed, quota_message = _can_create_more_racks(request.user, increment=pending_count)
            if not allowed:
                warnings = list(job.warnings or [])
                warnings.append(quota_message)
                job.warnings = warnings
                job.save(update_fields=["warnings", "updated_at"])
                return redirect("ios_import_detail", pk=job.pk)
            try:
                applied_racks = apply_import_job(
                    job=job,
                    user=request.user,
                    rack_model=RackIO,
                    rack_slot_model=RackSlotIO,
                    rack_module_model=ModuloRackIO,
                    channel_model=CanalRackIO,
                    module_qs=_io_import_module_catalog(job.cliente),
                    plant_model=PlantaIO,
                )
                if len(applied_racks) == 1:
                    return redirect("ios_rack_detail", pk=applied_racks[0].pk)
                return redirect("ios_import_detail", pk=job.pk)
            except IOImportError as exc:
                warnings = list(job.warnings or [])
                warnings.append(str(exc))
                job.warnings = warnings
                job.status = IOImportJob.Status.FAILED
                job.save(update_fields=["warnings", "status", "updated_at"])
                return redirect("ios_import_detail", pk=job.pk)

    proposal = job.proposal_payload or {}
    extracted = job.extracted_payload or {}
    rows = extracted.get("rows") or []
    sheet_summaries = extracted.get("sheets") or []
    applied_rack_key_map = dict((job.apply_log or {}).get("applied_rack_keys") or {})
    applied_rack_ids = list((job.apply_log or {}).get("applied_rack_ids") or [])
    applied_racks = list(RackIO.objects.filter(id__in=applied_rack_ids)) if applied_rack_ids else []
    preview_racks, preview_payload = _build_io_import_preview_racks(
        proposal,
        applied_rack_key_map=applied_rack_key_map,
    )
    pending_racks_count = sum(1 for rack in preview_racks if not rack.get("is_applied"))
    rows_preview = rows[:120]
    status_url = reverse("ios_import_status", kwargs={"pk": job.pk})
    user_facing_failure_message = ""
    if job.status == IOImportJob.Status.FAILED:
        user_facing_failure_message = _io_import_user_message(
            request.user,
            (job.warnings or [job.ai_error or "Falha ao processar a importacao."])[0],
            "Nao foi possivel concluir a analise da planilha. Revise o arquivo e tente novamente. Se o problema continuar, contate o suporte.",
        )
    return render(
        request,
        "core/io_import_detail.html",
        {
            "job": job,
            "proposal": proposal,
            "sheet_summaries": sheet_summaries,
            "applied_racks": applied_racks,
            "preview_racks": preview_racks,
            "preview_payload": preview_payload,
            "pending_racks_count": pending_racks_count,
            "rows_preview": rows_preview,
            "total_rows_preview": len(rows_preview),
            "has_more_rows": len(rows) > len(rows_preview),
            "io_import_is_dev": _is_dev_user(request.user),
            "status_url": status_url,
            "user_facing_failure_message": user_facing_failure_message,
        },
    )


@login_required
def ios_import_admin(request):
    if not _is_admin_user(request.user):
        return HttpResponseForbidden("Sem permissao.")
    message = None
    settings_obj = _io_import_settings()
    if request.method == "POST":
        action = request.POST.get("action")
        if action == "save_settings":
            settings_obj.enabled = request.POST.get("enabled") == "on"
            provider = request.POST.get("provider", IOImportSettings.Provider.OPENAI)
            if provider in dict(IOImportSettings.Provider.choices):
                settings_obj.provider = provider
            api_key = request.POST.get("api_key", "").strip()
            if api_key:
                settings_obj.api_key = api_key
            settings_obj.api_base_url = request.POST.get("api_base_url", "").strip()
            settings_obj.model = request.POST.get("model", "").strip()
            settings_obj.reasoning_effort = request.POST.get("reasoning_effort", "").strip() or "medium"
            try:
                settings_obj.max_rows_for_ai = max(20, min(int(request.POST.get("max_rows_for_ai", "150")), 500))
            except (TypeError, ValueError):
                settings_obj.max_rows_for_ai = 150
            settings_obj.header_prompt = request.POST.get("header_prompt", "").strip() or DEFAULT_HEADER_PROMPT
            settings_obj.grouping_prompt = request.POST.get("grouping_prompt", "").strip() or DEFAULT_GROUPING_PROMPT
            settings_obj.updated_by = request.user
            settings_obj.save()
            message = "Configuracoes de importacao atualizadas."
        elif action == "reanalyze_job":
            job_id = request.POST.get("job_id")
            job = IOImportJob.objects.filter(pk=job_id).first()
            if job:
                _reprocess_io_import_job(job)
                return redirect("ios_import_admin")

    jobs = IOImportJob.objects.select_related("cliente", "created_by", "target_rack", "applied_rack").order_by("-created_at")[:30]
    return render(
        request,
        "core/io_import_admin.html",
        {
            "message": message,
            "settings_obj": settings_obj,
            "jobs": jobs,
        },
    )


@login_required
def ios_modulos(request):
    denied_response = _require_internal_module_access(request, "IOS")
    if denied_response:
        return denied_response
    cliente = _get_cliente(request.user)
    if not cliente and not _is_admin_user(request.user):
        return HttpResponseForbidden("Sem cadastro de cliente.")
    if request.method == "POST":
        action = request.POST.get("action")
        if action == "create_module":
            if not cliente:
                return HttpResponseForbidden("Sem cadastro de cliente.")
            modelo = request.POST.get("modelo", "").strip()
            marca = request.POST.get("marca", "").strip()
            canais_raw = request.POST.get("quantidade_canais", "").strip()
            tipo_id = request.POST.get("tipo_base")
            try:
                quantidade_canais = int(canais_raw)
            except (TypeError, ValueError):
                quantidade_canais = None
            if quantidade_canais is not None:
                quantidade_canais = max(1, min(512, quantidade_canais))
            if modelo and quantidade_canais and tipo_id:
                tipo_base = get_object_or_404(TipoCanalIO, pk=tipo_id)
                ModuloIO.objects.create(
                    cliente=cliente,
                    nome="",
                    modelo=modelo,
                    marca=marca,
                    quantidade_canais=quantidade_canais,
                    tipo_base=tipo_base,
                )
            return redirect("ios_modulos")

    if not cliente:
        modules = ModuloIO.objects.none()
    else:
        modules = ModuloIO.objects.filter(cliente=cliente, is_default=False).select_related("tipo_base")
    channel_types = TipoCanalIO.objects.filter(ativo=True).order_by("nome")
    return render(
        request,
        "core/ios_modulos.html",
        {
            "modules": modules,
            "channel_types": channel_types,
            "can_manage": bool(cliente),
        },
    )


@login_required
def ios_modulo_modelo_detail(request, pk):
    denied_response = _require_internal_module_access(request, "IOS")
    if denied_response:
        return denied_response
    cliente = _get_cliente(request.user)
    if not cliente and not _is_admin_user(request.user):
        return HttpResponseForbidden("Sem cadastro de cliente.")
    module_qs = ModuloIO.objects.select_related("tipo_base")
    module = get_object_or_404(module_qs, pk=pk, cliente=cliente) if cliente else get_object_or_404(module_qs, pk=pk)
    if module.is_default and not _is_admin_user(request.user):
        return HttpResponseForbidden("Sem permissao.")
    if request.method == "POST":
        action = request.POST.get("action")
        if action == "update_model":
            modelo = request.POST.get("modelo", "").strip()
            marca = request.POST.get("marca", "").strip()
            canais_raw = request.POST.get("quantidade_canais", "").strip()
            tipo_id = request.POST.get("tipo_base")
            try:
                quantidade_canais = int(canais_raw)
            except (TypeError, ValueError):
                quantidade_canais = module.quantidade_canais
            quantidade_canais = max(1, min(512, quantidade_canais))
            module.modelo = modelo
            module.marca = marca
            if tipo_id:
                module.tipo_base_id = tipo_id
            module.quantidade_canais = quantidade_canais
            module.save(update_fields=["modelo", "marca", "tipo_base", "quantidade_canais"])
            return redirect("ios_modulo_modelo_detail", pk=module.pk)
        if action == "delete_model":
            if not module.instancias.exists():
                module.delete()
                return redirect("ios_modulos")
            return redirect("ios_modulo_modelo_detail", pk=module.pk)

    channel_types = TipoCanalIO.objects.filter(ativo=True).order_by("nome")
    return render(
        request,
        "core/ios_modulo_modelo_detail.html",
        {
            "module": module,
            "channel_types": channel_types,
        },
    )


@login_required
def inventarios_list(request):
    denied_response = _require_internal_module_access(request, "INVENTARIO")
    if denied_response:
        return denied_response
    cliente = _get_cliente(request.user)
    if not cliente and not _is_admin_user(request.user):
        return HttpResponseForbidden("Sem cadastro de cliente.")
    if request.method == "POST":
        action = request.POST.get("action")
        if action == "create_inventario":
            if not cliente:
                return HttpResponseForbidden("Sem cadastro de cliente.")
            nome = request.POST.get("nome", "").strip()
            descricao = request.POST.get("descricao", "").strip()
            responsavel = request.POST.get("responsavel", "").strip()
            cidade = request.POST.get("cidade", "").strip()
            estado = request.POST.get("estado", "").strip()
            pais = request.POST.get("pais", "").strip()
            id_inventario_raw = request.POST.get("id_inventario", "").strip()
            tagset_pattern = request.POST.get("tagset_pattern", "").strip()
            if tagset_pattern not in dict(Inventario.TagsetPattern.choices):
                tagset_pattern = Inventario.TagsetPattern.TIPO_SEQ
            id_inventario = None
            if id_inventario_raw:
                id_inventario, _ = InventarioID.objects.get_or_create(codigo=id_inventario_raw.upper())
            if nome:
                Inventario.objects.create(
                    cliente=cliente,
                    nome=nome,
                    descricao=descricao,
                    responsavel=responsavel,
                    cidade=cidade,
                    estado=estado,
                    pais=pais,
                    id_inventario=id_inventario,
                    tagset_pattern=tagset_pattern,
                    criador=request.user,
                )
            return redirect("inventarios_list")

    if _is_admin_user(request.user) and not cliente:
        inventarios = Inventario.objects.all()
    else:
        inventarios = Inventario.objects.filter(
            Q(cliente=cliente) | Q(id_inventario__in=cliente.inventarios.all())
        )
    inventarios = inventarios.annotate(total_ativos=Count("ativos", filter=Q(ativos__pai__isnull=True)))
    return render(
        request,
        "core/inventarios_list.html",
        {
            "inventarios": inventarios,
            "can_manage": bool(cliente),
            "tagset_choices": Inventario.TagsetPattern.choices,
        },
    )


@login_required
def inventario_detail(request, pk):
    denied_response = _require_internal_module_access(request, "INVENTARIO")
    if denied_response:
        return denied_response
    cliente = _get_cliente(request.user)
    if not cliente and not _is_admin_user(request.user):
        return HttpResponseForbidden("Sem cadastro de cliente.")
    if cliente:
        inventario = get_object_or_404(
            Inventario,
            Q(pk=pk),
            Q(cliente=cliente) | Q(id_inventario__in=cliente.inventarios.all()),
        )
    else:
        inventario = get_object_or_404(Inventario, pk=pk)
    message = None
    tipos_ativos = TipoAtivo.objects.filter(ativo=True).order_by("nome")
    if request.method == "POST":
        action = request.POST.get("action")
        if action == "create_ativo":
            nome = request.POST.get("nome", "").strip()
            setor = request.POST.get("setor", "").strip()
            tipo_id = request.POST.get("tipo", "").strip()
            identificacao = request.POST.get("identificacao", "").strip()
            tag_interna = request.POST.get("tag_interna", "").strip()
            if nome:
                tipo = None
                if tipo_id:
                    tipo = TipoAtivo.objects.filter(pk=tipo_id, ativo=True).first()
                ativo = Ativo.objects.create(
                    inventario=inventario,
                    setor=setor,
                    nome=nome,
                    tipo=tipo,
                    identificacao=identificacao,
                    tag_interna=tag_interna,
                )
                ativo.tag_set = _generate_tagset(inventario, tipo, setor, "ativo")
                ativo.save(update_fields=["tag_set"])
                total_items_raw = request.POST.get("total_items", "").strip()
                try:
                    total_items = int(total_items_raw)
                except (TypeError, ValueError):
                    total_items = 0
                total_items = max(0, min(50, total_items))
                itens_para_criar = []
                for index in range(total_items):
                    item_nome = request.POST.get(f"item_nome_{index}", "").strip()
                    item_tipo_id = request.POST.get(f"item_tipo_{index}", "").strip()
                    item_identificacao = request.POST.get(f"item_identificacao_{index}", "").strip()
                    item_tag_interna = request.POST.get(f"item_tag_interna_{index}", "").strip()
                    item_comissionado = request.POST.get(f"item_comissionado_{index}") == "on"
                    item_em_manutencao = request.POST.get(f"item_em_manutencao_{index}") == "on"
                    if not item_nome:
                        continue
                    item_tipo = None
                    if item_tipo_id:
                        item_tipo = TipoAtivo.objects.filter(pk=item_tipo_id, ativo=True).first()
                    item_tag_base = _generate_tagset(
                        inventario,
                        item_tipo,
                        ativo.setor,
                        "item",
                        fallback_tipo=ativo.tipo,
                        ativo=ativo,
                    )
                    item_tag_set = f"{ativo.tag_set}-{item_tag_base}" if ativo.tag_set else item_tag_base
                    itens_para_criar.append(
                        AtivoItem(
                            ativo=ativo,
                            nome=item_nome,
                            tipo=item_tipo,
                            identificacao=item_identificacao,
                            tag_interna=item_tag_interna,
                            tag_set=item_tag_set,
                            comissionado=item_comissionado,
                            em_manutencao=item_em_manutencao,
                            comissionado_em=timezone.now() if item_comissionado else None,
                            comissionado_por=request.user if item_comissionado else None,
                            manutencao_em=timezone.now() if item_em_manutencao else None,
                            manutencao_por=request.user if item_em_manutencao else None,
                        )
                    )
                if itens_para_criar:
                    AtivoItem.objects.bulk_create(itens_para_criar)
                    _sync_ativo_status(ativo)
            return redirect("inventario_detail", pk=inventario.pk)
        if action == "toggle_comissionado":
            ativo_id = request.POST.get("ativo_id")
            ativo = get_object_or_404(Ativo, pk=ativo_id, inventario=inventario)
            if ativo.comissionado:
                ativo.comissionado = False
                ativo.comissionado_em = None
                ativo.comissionado_por = None
            else:
                ativo.comissionado = True
                ativo.comissionado_em = timezone.now()
                ativo.comissionado_por = request.user
            ativo.save(update_fields=["comissionado", "comissionado_em", "comissionado_por"])
            return redirect("inventario_detail", pk=inventario.pk)
        if action == "toggle_manutencao":
            ativo_id = request.POST.get("ativo_id")
            ativo = get_object_or_404(Ativo, pk=ativo_id, inventario=inventario)
            if ativo.em_manutencao:
                ativo.em_manutencao = False
                ativo.manutencao_em = None
                ativo.manutencao_por = None
            else:
                ativo.em_manutencao = True
                ativo.manutencao_em = timezone.now()
                ativo.manutencao_por = request.user
            ativo.save(update_fields=["em_manutencao", "manutencao_em", "manutencao_por"])
            return redirect("inventario_detail", pk=inventario.pk)
        if action == "update_inventario":
            if not cliente and not _is_admin_user(request.user):
                return HttpResponseForbidden("Sem cadastro de cliente.")
            nome = request.POST.get("nome", "").strip()
            descricao = request.POST.get("descricao", "").strip()
            responsavel = request.POST.get("responsavel", "").strip()
            cidade = request.POST.get("cidade", "").strip()
            estado = request.POST.get("estado", "").strip()
            pais = request.POST.get("pais", "").strip()
            id_inventario_raw = request.POST.get("id_inventario", "").strip()
            tagset_pattern = request.POST.get("tagset_pattern", "").strip()
            if tagset_pattern not in dict(Inventario.TagsetPattern.choices):
                tagset_pattern = Inventario.TagsetPattern.TIPO_SEQ
            id_inventario = None
            if id_inventario_raw:
                id_inventario, _ = InventarioID.objects.get_or_create(codigo=id_inventario_raw.upper())
            if nome:
                inventario.nome = nome
            inventario.descricao = descricao
            inventario.responsavel = responsavel
            inventario.cidade = cidade
            inventario.estado = estado
            inventario.pais = pais
            inventario.id_inventario = id_inventario
            inventario.tagset_pattern = tagset_pattern
            inventario.save(
                update_fields=[
                    "nome",
                    "descricao",
                    "responsavel",
                    "cidade",
                    "estado",
                    "pais",
                    "id_inventario",
                    "tagset_pattern",
                ]
            )
            return redirect("inventario_detail", pk=inventario.pk)
        if action == "delete_inventario":
            if not cliente and not _is_admin_user(request.user):
                return HttpResponseForbidden("Sem cadastro de cliente.")
            inventario.delete()
            return redirect("inventarios_list")

    ativos = (
        Ativo.objects.filter(inventario=inventario, pai__isnull=True)
        .select_related("pai", "comissionado_por", "manutencao_por")
        .annotate(itens_total=Count("itens"))
        .order_by("nome")
    )
    total_ativos = Ativo.objects.filter(inventario=inventario, pai__isnull=True).count()
    return render(
        request,
        "core/inventario_detail.html",
        {
            "inventario": inventario,
            "ativos": ativos,
            "total_ativos": total_ativos,
            "message": message,
            "tipos_ativos": tipos_ativos,
            "tagset_choices": Inventario.TagsetPattern.choices,
        },
    )


@login_required
def inventario_tagset_preview(request, pk):
    denied_response = _require_internal_module_access(request, "INVENTARIO")
    if denied_response:
        return denied_response
    cliente = _get_cliente(request.user)
    if not cliente and not _is_admin_user(request.user):
        return HttpResponseForbidden("Sem cadastro de cliente.")
    if cliente:
        inventario = get_object_or_404(
            Inventario,
            Q(pk=pk),
            Q(cliente=cliente) | Q(id_inventario__in=cliente.inventarios.all()),
        )
    else:
        inventario = get_object_or_404(Inventario, pk=pk)
    target = (request.GET.get("target") or "").strip().lower()
    setor = request.GET.get("setor", "").strip()
    tipo_id = request.GET.get("tipo_id")
    fallback_id = request.GET.get("fallback_tipo_id")
    ativo_id = request.GET.get("ativo_id")
    ativo_tagset = request.GET.get("ativo_tagset", "").strip()

    tipo = TipoAtivo.objects.filter(pk=tipo_id).first() if tipo_id else None
    fallback_tipo = TipoAtivo.objects.filter(pk=fallback_id).first() if fallback_id else None
    ativo = Ativo.objects.filter(pk=ativo_id, inventario=inventario).first() if ativo_id else None

    if target == "item":
        item_tag_base = _generate_tagset(
            inventario,
            tipo,
            setor or (ativo.setor if ativo else ""),
            "item",
            fallback_tipo=fallback_tipo or (ativo.tipo if ativo else None),
            ativo=ativo,
        )
        if ativo and ativo.tag_set:
            tag_set = f"{ativo.tag_set}-{item_tag_base}"
        elif ativo_tagset:
            tag_set = f"{ativo_tagset}-{item_tag_base}"
        else:
            tag_set = item_tag_base
    else:
        tag_set = _generate_tagset(inventario, tipo, setor, "ativo")
    return JsonResponse({"tag_set": tag_set})


@login_required
def inventario_ativo_detail(request, inventario_pk, pk):
    denied_response = _require_internal_module_access(request, "INVENTARIO")
    if denied_response:
        return denied_response
    cliente = _get_cliente(request.user)
    if not cliente and not _is_admin_user(request.user):
        return HttpResponseForbidden("Sem cadastro de cliente.")
    if cliente:
        inventario = get_object_or_404(
            Inventario,
            Q(pk=inventario_pk),
            Q(cliente=cliente) | Q(id_inventario__in=cliente.inventarios.all()),
        )
    else:
        inventario = get_object_or_404(Inventario, pk=inventario_pk)
    ativo = get_object_or_404(
        Ativo.objects.select_related("inventario", "comissionado_por", "manutencao_por"),
        pk=pk,
        inventario=inventario,
    )
    tipos_ativos = TipoAtivo.objects.filter(ativo=True).order_by("nome")
    if request.method == "POST":
        action = request.POST.get("action")
        if action == "create_item":
            nome = request.POST.get("nome", "").strip()
            tipo_id = request.POST.get("tipo", "").strip()
            identificacao = request.POST.get("identificacao", "").strip()
            tag_interna = request.POST.get("tag_interna", "").strip()
            comissionado = request.POST.get("comissionado") == "on"
            em_manutencao = request.POST.get("em_manutencao") == "on"
            if nome:
                if em_manutencao:
                    comissionado = False
                tipo = None
                if tipo_id:
                    tipo = TipoAtivo.objects.filter(pk=tipo_id, ativo=True).first()
                if not ativo.tag_set:
                    ativo.tag_set = _generate_tagset(inventario, ativo.tipo, ativo.setor, "ativo")
                    ativo.save(update_fields=["tag_set"])
                item_tag_base = _generate_tagset(
                    inventario,
                    tipo,
                    ativo.setor,
                    "item",
                    fallback_tipo=ativo.tipo,
                    ativo=ativo,
                )
                AtivoItem.objects.create(
                    ativo=ativo,
                    nome=nome,
                    tipo=tipo,
                    identificacao=identificacao,
                    tag_interna=tag_interna,
                    tag_set=f"{ativo.tag_set}-{item_tag_base}" if ativo.tag_set else item_tag_base,
                    comissionado=comissionado,
                    em_manutencao=em_manutencao,
                    comissionado_em=timezone.now() if comissionado else None,
                    comissionado_por=request.user if comissionado else None,
                    manutencao_em=timezone.now() if em_manutencao else None,
                    manutencao_por=request.user if em_manutencao else None,
                )
                _sync_ativo_status(ativo)
            return redirect("inventario_ativo_detail", inventario_pk=inventario.pk, pk=ativo.pk)
        if action == "toggle_item_comissionado":
            item_id = request.POST.get("item_id")
            alvo = get_object_or_404(AtivoItem, pk=item_id, ativo=ativo)
            if alvo.comissionado:
                alvo.comissionado = False
                alvo.comissionado_em = None
                alvo.comissionado_por = None
            else:
                if alvo.em_manutencao:
                    alvo.em_manutencao = False
                    alvo.manutencao_em = None
                    alvo.manutencao_por = None
                alvo.comissionado = True
                alvo.comissionado_em = timezone.now()
                alvo.comissionado_por = request.user
            alvo.save(
                update_fields=[
                    "comissionado",
                    "comissionado_em",
                    "comissionado_por",
                    "em_manutencao",
                    "manutencao_em",
                    "manutencao_por",
                ]
            )
            _sync_ativo_status(ativo)
            return redirect("inventario_ativo_detail", inventario_pk=inventario.pk, pk=ativo.pk)
        if action == "toggle_item_manutencao":
            item_id = request.POST.get("item_id")
            alvo = get_object_or_404(AtivoItem, pk=item_id, ativo=ativo)
            if alvo.em_manutencao:
                alvo.em_manutencao = False
                alvo.manutencao_em = None
                alvo.manutencao_por = None
            else:
                if alvo.comissionado:
                    alvo.comissionado = False
                    alvo.comissionado_em = None
                    alvo.comissionado_por = None
                alvo.em_manutencao = True
                alvo.manutencao_em = timezone.now()
                alvo.manutencao_por = request.user
            alvo.save(
                update_fields=[
                    "comissionado",
                    "comissionado_em",
                    "comissionado_por",
                    "em_manutencao",
                    "manutencao_em",
                    "manutencao_por",
                ]
            )
            _sync_ativo_status(ativo)
            return redirect("inventario_ativo_detail", inventario_pk=inventario.pk, pk=ativo.pk)
        if action == "update_ativo":
            if not cliente and not _is_admin_user(request.user):
                return HttpResponseForbidden("Sem cadastro de cliente.")
            nome = request.POST.get("nome", "").strip()
            setor = request.POST.get("setor", "").strip()
            tipo_id = request.POST.get("tipo", "").strip()
            identificacao = request.POST.get("identificacao", "").strip()
            tag_interna = request.POST.get("tag_interna", "").strip()
            if nome:
                ativo.nome = nome
            ativo.setor = setor
            ativo.tipo = TipoAtivo.objects.filter(pk=tipo_id).first() if tipo_id else None
            ativo.identificacao = identificacao
            ativo.tag_interna = tag_interna
            ativo.save(
                update_fields=[
                    "nome",
                    "setor",
                    "tipo",
                    "identificacao",
                    "tag_interna",
                ]
            )
            return redirect("inventario_ativo_detail", inventario_pk=inventario.pk, pk=ativo.pk)
        if action == "delete_ativo":
            if not cliente and not _is_admin_user(request.user):
                return HttpResponseForbidden("Sem cadastro de cliente.")
            ativo.delete()
            return redirect("inventario_detail", pk=inventario.pk)

    itens = (
        AtivoItem.objects.filter(ativo=ativo)
        .select_related("comissionado_por", "manutencao_por")
        .order_by("nome")
    )
    return render(
        request,
        "core/inventario_ativo_detail.html",
        {
            "inventario": inventario,
            "ativo": ativo,
            "itens": itens,
            "tipos_ativos": tipos_ativos,
        },
    )


@login_required
def inventario_item_detail(request, inventario_pk, ativo_pk, pk):
    denied_response = _require_internal_module_access(request, "INVENTARIO")
    if denied_response:
        return denied_response
    cliente = _get_cliente(request.user)
    if not cliente and not _is_admin_user(request.user):
        return HttpResponseForbidden("Sem cadastro de cliente.")
    if cliente:
        inventario = get_object_or_404(
            Inventario,
            Q(pk=inventario_pk),
            Q(cliente=cliente) | Q(id_inventario__in=cliente.inventarios.all()),
        )
    else:
        inventario = get_object_or_404(Inventario, pk=inventario_pk)
    ativo = get_object_or_404(Ativo, pk=ativo_pk, inventario=inventario)
    item = get_object_or_404(AtivoItem, pk=pk, ativo=ativo)
    tipos_ativos = TipoAtivo.objects.filter(ativo=True).order_by("nome")
    if request.method == "POST":
        action = request.POST.get("action")
        if action == "update_item":
            if not cliente and not _is_admin_user(request.user):
                return HttpResponseForbidden("Sem cadastro de cliente.")
            nome = request.POST.get("nome", "").strip()
            tipo_id = request.POST.get("tipo", "").strip()
            identificacao = request.POST.get("identificacao", "").strip()
            tag_interna = request.POST.get("tag_interna", "").strip()
            comissionado = request.POST.get("comissionado") == "on"
            em_manutencao = request.POST.get("em_manutencao") == "on"
            if em_manutencao:
                comissionado = False
            if nome:
                item.nome = nome
            item.tipo = TipoAtivo.objects.filter(pk=tipo_id).first() if tipo_id else None
            item.identificacao = identificacao
            item.tag_interna = tag_interna
            if comissionado and not item.comissionado:
                item.comissionado_em = timezone.now()
                item.comissionado_por = request.user
            if not comissionado:
                item.comissionado_em = None
                item.comissionado_por = None
            if em_manutencao and not item.em_manutencao:
                item.manutencao_em = timezone.now()
                item.manutencao_por = request.user
            if not em_manutencao:
                item.manutencao_em = None
                item.manutencao_por = None
            item.comissionado = comissionado
            item.em_manutencao = em_manutencao
            item.save(
                update_fields=[
                    "nome",
                    "tipo",
                    "identificacao",
                    "tag_interna",
                    "comissionado",
                    "comissionado_em",
                    "comissionado_por",
                    "em_manutencao",
                    "manutencao_em",
                    "manutencao_por",
                ]
            )
            _sync_ativo_status(ativo)
            return redirect(
                "inventario_item_detail",
                inventario_pk=inventario.pk,
                ativo_pk=ativo.pk,
                pk=item.pk,
            )
        if action == "delete_item":
            if not cliente and not _is_admin_user(request.user):
                return HttpResponseForbidden("Sem cadastro de cliente.")
            item.delete()
            _sync_ativo_status(ativo)
            return redirect("inventario_ativo_detail", inventario_pk=inventario.pk, pk=ativo.pk)
    return render(
        request,
        "core/inventario_item_detail.html",
        {
            "inventario": inventario,
            "ativo": ativo,
            "item": item,
            "tipos_ativos": tipos_ativos,
        },
    )


@login_required
def listas_ip_import_create(request):
    denied_response = _require_internal_module_access(request, "LISTA_IP")
    if denied_response:
        if _is_ajax_request(request):
            return _json_error_response("Seu acesso atual nao permite importar planilhas de IP.", status=403)
        return denied_response
    if request.method != "POST":
        return HttpResponseNotAllowed(["POST"])
    cliente = _get_cliente(request.user)
    if not _ip_import_can_manage(request, cliente):
        if _is_ajax_request(request):
            return _json_error_response("Sem permissao para importar planilhas de IP.", status=403)
        return HttpResponseForbidden("Sem permissao.")

    ajax_request = _is_ajax_request(request)

    arquivo = request.FILES.get("arquivo")
    if not arquivo:
        if ajax_request:
            return _json_error_response("Selecione um arquivo para enviar.", status=400)
        return HttpResponse("Arquivo obrigatorio.", status=400)

    job_cliente = cliente
    if not job_cliente:
        if ajax_request:
            return _json_error_response("Conta sem cliente associado para importar listas de IP.", status=400)
        return HttpResponse("Conta sem cliente associado para importar listas de IP.", status=400)

    raw_bytes = arquivo.read()
    arquivo.seek(0)

    job = None
    redirect_url = None
    try:
        job = IPImportJob.objects.create(
            created_by=request.user,
            cliente=job_cliente,
            original_filename=arquivo.name,
            source_file=arquivo,
            file_sha256=build_ip_file_sha256(raw_bytes),
            progress_payload=_initial_ip_import_progress_payload(arquivo.name),
        )
        redirect_url = reverse("listas_ip_import_detail", kwargs={"pk": job.pk})
        transaction.on_commit(lambda job_id=job.pk: _spawn_ip_import_job_processor_safe(job_id))
        if ajax_request:
            return JsonResponse(
                {
                    "ok": True,
                    "job_id": job.pk,
                    "status_url": reverse("listas_ip_import_status", kwargs={"pk": job.pk}),
                    "redirect_url": redirect_url,
                }
            )
    except IPImportError as exc:
        if not job:
            if ajax_request:
                return _json_error_response(
                    _ip_import_user_message(
                        request.user,
                        str(exc),
                        "Nao foi possivel analisar o arquivo enviado. Revise o arquivo e tente novamente.",
                    ),
                    status=400,
                )
            return HttpResponse(str(exc), status=400)
        job.status = IPImportJob.Status.FAILED
        job.ai_status = IPImportJob.AIStatus.FAILED
        job.ai_error = str(exc)
        job.warnings = [str(exc)]
        job.progress_payload = _failed_ip_import_progress_payload(str(exc), job.progress_payload)
        job.save(update_fields=["status", "ai_status", "ai_error", "warnings", "progress_payload", "updated_at"])
    except Exception as exc:
        logger.exception(
            "Unhandled IP import error while creating import job",
            extra={"user_id": request.user.id, "job_id": job.pk if job else None, "upload_name": arquivo.name},
        )
        if job:
            job.status = IPImportJob.Status.FAILED
            job.ai_status = IPImportJob.AIStatus.FAILED
            job.ai_error = str(exc)
            job.warnings = [f"Falha interna ao analisar a planilha: {exc}"]
            job.progress_payload = _failed_ip_import_progress_payload(str(exc), job.progress_payload)
            job.save(update_fields=["status", "ai_status", "ai_error", "warnings", "progress_payload", "updated_at"])
        if ajax_request:
            return _json_error_response(
                _ip_import_user_message(
                    request.user,
                    "Falha interna ao analisar a planilha. Verifique o log do servidor.",
                    "Nao foi possivel concluir a analise da planilha agora. Tente novamente em instantes.",
                ),
                status=500,
                job_id=job.pk if job else None,
                redirect_url=redirect_url,
            )
        if job:
            return redirect("listas_ip_import_detail", pk=job.pk)
        return HttpResponse("Falha interna ao criar a importacao.", status=500)

    redirect_url = redirect_url or reverse("listas_ip_import_detail", kwargs={"pk": job.pk})
    if ajax_request:
        return JsonResponse({"ok": True, "redirect_url": redirect_url})
    return redirect("listas_ip_import_detail", pk=job.pk)


@login_required
def listas_ip_import_status(request, pk):
    denied_response = _require_internal_module_access(request, "LISTA_IP")
    if denied_response:
        return _json_error_response("Seu acesso atual nao permite consultar esta importacao.", status=403)
    cliente = _get_cliente(request.user)
    job = get_object_or_404(_build_ip_import_job_queryset(request, cliente), pk=pk)
    processing = job.status == IPImportJob.Status.UPLOADED
    failed = job.status == IPImportJob.Status.FAILED
    message = ""
    progress = _build_ip_import_status_progress(job)
    if failed and not _is_dev_user(request.user):
        progress["title"] = "Analise interrompida"
        progress["message"] = "Nao foi possivel concluir a analise da planilha. Revise o arquivo e tente novamente."
    if failed:
        technical_message = (job.warnings or [job.ai_error or "Falha ao processar a importacao."])[0]
        message = _ip_import_user_message(
            request.user,
            technical_message,
            "Nao foi possivel concluir a analise da planilha. Revise o arquivo e tente novamente.",
        )
    elif processing:
        message = progress.get("message") or "A planilha esta sendo analisada em segundo plano."
    return JsonResponse(
        {
            "ok": True,
            "job_id": job.pk,
            "status": job.status,
            "status_display": job.get_status_display(),
            "processing": processing,
            "complete": not processing,
            "failed": failed,
            "redirect_url": reverse("listas_ip_import_detail", kwargs={"pk": job.pk}),
            "message": message,
            "warnings_count": len(job.warnings or []) if _is_dev_user(request.user) else 0,
            "ai_status": job.ai_status if _is_dev_user(request.user) else "",
            "progress": progress,
        }
    )


@login_required
def listas_ip_import_detail(request, pk):
    denied_response = _require_internal_module_access(request, "LISTA_IP")
    if denied_response:
        return denied_response
    cliente = _get_cliente(request.user)
    job = get_object_or_404(_build_ip_import_job_queryset(request, cliente), pk=pk)

    if request.method == "POST":
        action = request.POST.get("action")
        if action == "reanalyze":
            job.status = IPImportJob.Status.UPLOADED
            job.ai_status = IPImportJob.AIStatus.SKIPPED
            job.ai_error = ""
            job.warnings = []
            job.progress_payload = _initial_ip_import_progress_payload(job.original_filename)
            job.save(update_fields=["status", "ai_status", "ai_error", "warnings", "progress_payload", "updated_at"])
            transaction.on_commit(lambda job_id=job.pk: _spawn_ip_import_job_processor_safe(job_id))
            return redirect("listas_ip_import_detail", pk=job.pk)
        if action == "apply_import_list":
            list_key = (request.POST.get("list_key") or "").strip()
            if not list_key:
                warnings = list(job.warnings or [])
                warnings.append("Nenhuma lista foi selecionada para aplicacao.")
                job.warnings = warnings
                job.save(update_fields=["warnings", "updated_at"])
                return redirect("listas_ip_import_detail", pk=job.pk)
            try:
                apply_ip_import_job(job=job, user=request.user, selected_list_keys=[list_key])
                return redirect("listas_ip_import_detail", pk=job.pk)
            except IPImportError as exc:
                warnings = list(job.warnings or [])
                warnings.append(str(exc))
                job.warnings = warnings
                job.save(update_fields=["warnings", "updated_at"])
                return redirect("listas_ip_import_detail", pk=job.pk)
        if action == "apply_import":
            try:
                apply_ip_import_job(job=job, user=request.user)
                return redirect("listas_ip_import_detail", pk=job.pk)
            except IPImportError as exc:
                warnings = list(job.warnings or [])
                warnings.append(str(exc))
                job.warnings = warnings
                job.status = IPImportJob.Status.FAILED
                job.save(update_fields=["warnings", "status", "updated_at"])
                return redirect("listas_ip_import_detail", pk=job.pk)

    proposal = job.proposal_payload or {}
    extracted = job.extracted_payload or {}
    sheet_summaries = extracted.get("sheets") or []
    applied_list_key_map = dict((job.apply_log or {}).get("applied_list_keys") or {})
    applied_list_ids = list((job.apply_log or {}).get("applied_list_ids") or [])
    applied_lists = list(ListaIP.objects.filter(id__in=applied_list_ids)) if applied_list_ids else []
    preview_lists = _build_ip_import_preview_lists(proposal, applied_list_key_map=applied_list_key_map)
    pending_lists_count = sum(1 for item in preview_lists if not item.get("is_applied"))

    return render(
        request,
        "core/ip_import_detail.html",
        {
            "job": job,
            "proposal": proposal,
            "sheet_summaries": sheet_summaries,
            "preview_lists": preview_lists,
            "pending_lists_count": pending_lists_count,
            "applied_lists": applied_lists,
            "ip_import_is_dev": _is_dev_user(request.user),
            "status_url": reverse("listas_ip_import_status", kwargs={"pk": job.pk}),
            "progress": _build_ip_import_status_progress(job),
        },
    )


@login_required
def listas_ip_import_admin(request):
    if not _is_admin_user(request.user):
        return HttpResponseForbidden("Sem permissao.")
    message = None
    settings_obj = _ip_import_settings()
    if request.method == "POST":
        action = request.POST.get("action")
        if action == "save_settings":
            settings_obj.enabled = request.POST.get("enabled") == "on"
            provider = request.POST.get("provider", IPImportSettings.Provider.OPENAI)
            if provider in dict(IPImportSettings.Provider.choices):
                settings_obj.provider = provider
            api_key = request.POST.get("api_key", "").strip()
            if api_key:
                settings_obj.api_key = api_key
            settings_obj.api_base_url = request.POST.get("api_base_url", "").strip()
            settings_obj.model = request.POST.get("model", "").strip()
            settings_obj.reasoning_effort = request.POST.get("reasoning_effort", "").strip() or "medium"
            try:
                settings_obj.max_rows_for_ai = max(20, min(int(request.POST.get("max_rows_for_ai", "180")), 500))
            except (TypeError, ValueError):
                settings_obj.max_rows_for_ai = 180
            settings_obj.header_prompt = request.POST.get("header_prompt", "").strip() or DEFAULT_IP_HEADER_PROMPT
            settings_obj.grouping_prompt = request.POST.get("grouping_prompt", "").strip() or DEFAULT_IP_HEADER_GROUPING_PROMPT
            settings_obj.updated_by = request.user
            settings_obj.save()
            message = "Configuracoes de importacao atualizadas."
        elif action == "reanalyze_job":
            job_id = request.POST.get("job_id")
            job = IPImportJob.objects.filter(pk=job_id).first()
            if job:
                job.status = IPImportJob.Status.UPLOADED
                job.ai_status = IPImportJob.AIStatus.SKIPPED
                job.ai_error = ""
                job.warnings = []
                job.progress_payload = _initial_ip_import_progress_payload(job.original_filename)
                job.save(update_fields=["status", "ai_status", "ai_error", "warnings", "progress_payload", "updated_at"])
                transaction.on_commit(lambda job_id=job.pk: _spawn_ip_import_job_processor_safe(job_id))
                return redirect("listas_ip_import_admin")

    jobs = IPImportJob.objects.select_related("cliente", "created_by", "applied_lista").order_by("-created_at")[:30]
    return render(
        request,
        "core/ip_import_admin.html",
        {
            "message": message,
            "settings_obj": settings_obj,
            "jobs": jobs,
        },
    )


@login_required
def listas_ip_list(request):
    denied_response = _require_internal_module_access(request, "LISTA_IP")
    if denied_response:
        return denied_response
    cliente = _get_cliente(request.user)
    if not cliente and not _is_admin_user(request.user):
        return HttpResponseForbidden("Sem cadastro de cliente.")

    message = None
    message_level = "info"
    if request.method == "POST":
        action = request.POST.get("action")
        if action == "create_lista":
            if not cliente:
                return HttpResponseForbidden("Sem cadastro de cliente.")
            nome = request.POST.get("nome", "").strip()
            descricao = request.POST.get("descricao", "").strip()
            faixa_inicio = request.POST.get("faixa_inicio", "").strip()
            faixa_fim = request.POST.get("faixa_fim", "").strip()
            protocolo_padrao = request.POST.get("protocolo_padrao", "").strip()
            id_listaip_raw = request.POST.get("id_listaip", "").strip()
            if not nome:
                message = "Informe um nome para a lista."
                message_level = "error"
            else:
                ip_values, error = _ip_range_values(faixa_inicio, faixa_fim)
                if error:
                    message = error
                    message_level = "error"
                else:
                    id_listaip = None
                    if id_listaip_raw:
                        id_listaip, _ = ListaIPID.objects.get_or_create(codigo=id_listaip_raw.upper())
                    lista = ListaIP.objects.create(
                        cliente=cliente,
                        id_listaip=id_listaip,
                        nome=nome,
                        descricao=descricao,
                        faixa_inicio=faixa_inicio,
                        faixa_fim=faixa_fim,
                        protocolo_padrao=protocolo_padrao,
                    )
                    _sync_lista_ip_items(lista, ip_values)
                    return redirect("lista_ip_detail", pk=lista.pk)

    if _is_admin_user(request.user) and not cliente:
        listas = ListaIP.objects.all()
        can_manage = True
    else:
        listas = ListaIP.objects.filter(Q(cliente=cliente) | Q(id_listaip__in=cliente.listas_ip.all()))
        can_manage = bool(cliente)

    listas = listas.annotate(total_ips=Count("ips")).order_by("nome")
    return render(
        request,
        "core/listas_ip_list.html",
        {
            "listas": listas,
            "can_manage": can_manage,
            "message": message,
            "message_level": message_level,
            "ip_import_can_upload": _ip_import_can_manage(request, cliente),
            "ip_import_is_admin": _is_admin_user(request.user),
            "commercial_status": _documentacao_tecnica_status_context(request.user),
        },
    )


@login_required
def lista_ip_detail(request, pk):
    denied_response = _require_internal_module_access(request, "LISTA_IP")
    if denied_response:
        return denied_response
    cliente = _get_cliente(request.user)
    if not cliente and not _is_admin_user(request.user):
        return HttpResponseForbidden("Sem cadastro de cliente.")

    if _is_admin_user(request.user) and not cliente:
        lista = get_object_or_404(ListaIP, pk=pk)
        can_manage = True
    else:
        lista = get_object_or_404(
            ListaIP,
            Q(pk=pk),
            Q(cliente=cliente) | Q(id_listaip__in=cliente.listas_ip.all()),
        )
        can_manage = bool(cliente) and lista.cliente_id == cliente.id

    message = request.GET.get("msg", "").strip()
    message_level = request.GET.get("level", "").strip() or "info"

    if request.method == "POST":
        action = request.POST.get("action")
        if action in {"update_lista", "regenerate_range", "delete_lista", "update_item", "apply_default_protocol"}:
            if not can_manage and not _is_admin_user(request.user):
                return HttpResponseForbidden("Sem permissao.")
        if action == "update_lista":
            nome = request.POST.get("nome", "").strip()
            descricao = request.POST.get("descricao", "").strip()
            faixa_inicio = request.POST.get("faixa_inicio", "").strip()
            faixa_fim = request.POST.get("faixa_fim", "").strip()
            protocolo_padrao = request.POST.get("protocolo_padrao", "").strip()
            id_listaip_raw = request.POST.get("id_listaip", "").strip()
            if not nome:
                message = "Informe um nome para a lista."
                message_level = "error"
            else:
                ip_values, error = _ip_range_values(faixa_inicio, faixa_fim)
                if error:
                    message = error
                    message_level = "error"
                else:
                    id_listaip = None
                    if id_listaip_raw:
                        id_listaip, _ = ListaIPID.objects.get_or_create(codigo=id_listaip_raw.upper())
                    lista.nome = nome
                    lista.descricao = descricao
                    lista.faixa_inicio = faixa_inicio
                    lista.faixa_fim = faixa_fim
                    lista.protocolo_padrao = protocolo_padrao
                    lista.id_listaip = id_listaip
                    lista.save(
                        update_fields=[
                            "nome",
                            "descricao",
                            "faixa_inicio",
                            "faixa_fim",
                            "protocolo_padrao",
                            "id_listaip",
                        ]
                    )
                    _sync_lista_ip_items(lista, ip_values)
                    return redirect("lista_ip_detail", pk=lista.pk)
        if action == "regenerate_range":
            ip_values, error = _ip_range_values(lista.faixa_inicio, lista.faixa_fim)
            if error:
                message = error
                message_level = "error"
            else:
                _sync_lista_ip_items(lista, ip_values)
                message = "Faixa atualizada."
                message_level = "success"
        if action == "apply_default_protocol":
            if lista.protocolo_padrao:
                ListaIPItem.objects.filter(lista=lista, protocolo="").update(protocolo=lista.protocolo_padrao)
                message = "Protocolo aplicado nos IPs sem valor."
                message_level = "success"
            else:
                message = "Defina um protocolo padrao antes."
                message_level = "error"
        if action == "delete_lista":
            lista.delete()
            return redirect("listas_ip_list")
        if action == "bulk_update_items":
            item_ids = request.POST.getlist("item_id")
            items_qs = ListaIPItem.objects.filter(lista=lista, id__in=item_ids)
            items_map = {str(item.id): item for item in items_qs}
            for item_id in item_ids:
                item = items_map.get(item_id)
                if not item:
                    continue
                nome_raw = request.POST.get(f"nome_equipamento_{item_id}", "").strip()
                descricao_raw = request.POST.get(f"descricao_{item_id}", "").strip()
                mac_raw = request.POST.get(f"mac_{item_id}", "").strip()
                protocolo_raw = request.POST.get(f"protocolo_{item_id}", "").strip()
                item.nome_equipamento = nome_raw
                item.descricao = descricao_raw
                item.mac = mac_raw
                item.protocolo = protocolo_raw
            if items_map:
                ListaIPItem.objects.bulk_update(
                    items_map.values(),
                    ["nome_equipamento", "descricao", "mac", "protocolo"],
                )
            if request.headers.get("x-requested-with") == "XMLHttpRequest":
                return JsonResponse({"ok": True, "updated": len(items_map)})
            return redirect("lista_ip_detail", pk=lista.pk)
        if action == "inline_update_item":
            item_id = request.POST.get("item_id")
            item = get_object_or_404(ListaIPItem, pk=item_id, lista=lista)
            item.nome_equipamento = request.POST.get("nome_equipamento", "").strip()
            item.descricao = request.POST.get("descricao", "").strip()
            item.mac = request.POST.get("mac", "").strip()
            item.protocolo = request.POST.get("protocolo", "").strip()
            item.save(update_fields=["nome_equipamento", "descricao", "mac", "protocolo"])
            if request.headers.get("x-requested-with") == "XMLHttpRequest":
                return JsonResponse({"ok": True})
            return redirect("lista_ip_detail", pk=lista.pk)

    search_term = request.GET.get("q", "").strip()
    items = ListaIPItem.objects.filter(lista=lista)
    if search_term:
        items = items.filter(
            Q(ip__icontains=search_term)
            | Q(nome_equipamento__icontains=search_term)
            | Q(descricao__icontains=search_term)
            | Q(mac__icontains=search_term)
            | Q(protocolo__icontains=search_term)
        )
    items = list(items)
    items.sort(key=lambda item: ipaddress.ip_address(item.ip))
    nome_counts = {}
    mac_counts = {}
    for item in items:
        nome_key = (item.nome_equipamento or "").strip().upper()
        mac_key = (item.mac or "").strip().upper()
        if nome_key:
            nome_counts[nome_key] = nome_counts.get(nome_key, 0) + 1
        if mac_key:
            mac_counts[mac_key] = mac_counts.get(mac_key, 0) + 1
    nomes_repetidos = {key for key, count in nome_counts.items() if count > 1}
    macs_repetidos = {key for key, count in mac_counts.items() if count > 1}
    page_obj = _paginate_queryset(request, items, per_page=50)
    items = list(page_obj.object_list)
    page_params = request.GET.copy()
    if "page" in page_params:
        page_params.pop("page")
    page_query = page_params.urlencode()
    total_ips = ListaIPItem.objects.filter(lista=lista).count()
    total_preenchidos = ListaIPItem.objects.filter(
        lista=lista,
    ).filter(
        Q(nome_equipamento__gt="")
        | Q(mac__gt="")
        | Q(protocolo__gt="")
    ).count()
    return render(
        request,
        "core/lista_ip_detail.html",
        {
            "lista": lista,
            "items": items,
            "total_ips": total_ips,
            "total_preenchidos": total_preenchidos,
            "search_term": search_term,
            "can_manage": can_manage,
            "message": message,
            "message_level": message_level,
            "page_obj": page_obj,
            "page_query": page_query,
            "nomes_repetidos": nomes_repetidos,
            "macs_repetidos": macs_repetidos,
        },
    )


@login_required
def radar_list(request):
    denied_response = _require_internal_module_access(request, "RADAR")
    if denied_response:
        return denied_response
    cliente = _get_cliente(request.user)
    if not cliente and not _is_admin_user(request.user):
        return HttpResponseForbidden("Sem cadastro de cliente.")

    message = None
    message_level = "info"
    if request.method == "POST":
        action = request.POST.get("action")
        if action == "create_radar":
            if not cliente:
                return HttpResponseForbidden("Sem cadastro de cliente.")
            nome = request.POST.get("nome", "").strip()
            descricao = request.POST.get("descricao", "").strip()
            local = request.POST.get("local", "").strip()
            id_radar_raw = request.POST.get("id_radar", "").strip()
            if not nome:
                message = "Informe um nome para o radar."
                message_level = "error"
            else:
                id_radar = None
                if id_radar_raw:
                    id_radar, _ = RadarID.objects.get_or_create(codigo=id_radar_raw.upper())
                Radar.objects.create(
                    cliente=cliente,
                    nome=nome,
                    descricao=descricao,
                    local=local,
                    id_radar=id_radar,
                    criador=request.user,
                )
                return redirect("radar_list")

    if _is_admin_user(request.user) and not cliente:
        radars = Radar.objects.all()
        can_manage = True
    else:
        radars = Radar.objects.filter(Q(cliente=cliente) | Q(id_radar__in=cliente.radares.all()))
        can_manage = bool(cliente)

    radars = radars.annotate(
        total_trabalhos=Count("trabalhos", distinct=True),
        total_atividades=Count("trabalhos__atividades", distinct=True),
    ).order_by("nome")
    return render(
        request,
        "core/radar_list.html",
        {
            "radars": radars,
            "can_manage": can_manage,
            "message": message,
            "message_level": message_level,
        },
    )


@login_required
def radar_detail(request, pk):
    denied_response = _require_internal_module_access(request, "RADAR")
    if denied_response:
        return denied_response
    cliente = _get_cliente(request.user)
    if not cliente and not _is_admin_user(request.user):
        return HttpResponseForbidden("Sem cadastro de cliente.")

    if _is_admin_user(request.user) and not cliente:
        radar = get_object_or_404(Radar, pk=pk)
        is_creator = False
        has_id_radar_access = False
        can_manage = False
    else:
        radar = get_object_or_404(
            Radar,
            Q(pk=pk),
            Q(cliente=cliente) | Q(id_radar__in=cliente.radares.all()),
        )
        is_creator = _is_radar_creator_user(request.user, radar)
        has_id_radar_access = bool(cliente) and (
            radar.id_radar_id and cliente.radares.filter(pk=radar.id_radar_id).exists()
        )
        can_manage = is_creator

    message = None
    message_level = "info"
    classificacoes = RadarClassificacao.objects.order_by("nome")
    classificacao_filter = request.GET.get("classificacao", "").strip()
    if request.method == "POST":
        action = request.POST.get("action")
        if action in {
            "create_trabalho",
            "update_radar",
            "delete_radar",
            "create_classificacao",
            "create_contrato",
        }:
            if not can_manage:
                return HttpResponseForbidden("Somente quem criou o radar pode alterar.")
        if action == "create_classificacao":
            nome = request.POST.get("classificacao_nome", "").strip()
            if not nome:
                msg = "Informe um nome de classificacao."
                level = "error"
                created = False
            else:
                classificacao, created = RadarClassificacao.objects.get_or_create(nome=nome)
                if created:
                    msg = "Classificacao criada."
                    level = "success"
                else:
                    msg = "Classificacao ja existe."
                    level = "warning"
            if request.headers.get("x-requested-with") == "XMLHttpRequest":
                return JsonResponse(
                    {
                        "ok": bool(nome),
                        "created": created,
                        "id": classificacao.id if nome and "classificacao" in locals() else None,
                        "nome": classificacao.nome if nome and "classificacao" in locals() else None,
                        "message": msg,
                        "level": level,
                    }
                )
            params = {"cadastro": "classificacao", "msg": msg, "level": level}
            return redirect(f"{reverse('radar_detail', args=[radar.pk])}?{urlencode(params)}")
        if action == "create_contrato":
            nome = request.POST.get("contrato_nome", "").strip()
            if not nome:
                msg = "Informe um nome de contrato."
                level = "error"
                created = False
            else:
                contrato, created = RadarContrato.objects.get_or_create(nome=nome)
                if created:
                    msg = "Contrato criado."
                    level = "success"
                else:
                    msg = "Contrato ja existe."
                    level = "warning"
            if request.headers.get("x-requested-with") == "XMLHttpRequest":
                return JsonResponse(
                    {
                        "ok": bool(nome),
                        "created": created,
                        "id": contrato.id if nome and "contrato" in locals() else None,
                        "nome": contrato.nome if nome and "contrato" in locals() else None,
                        "message": msg,
                        "level": level,
                    }
                )
            params = {"cadastro": "contrato", "msg": msg, "level": level}
            return redirect(f"{reverse('radar_detail', args=[radar.pk])}?{urlencode(params)}")
        if action == "create_trabalho":
            nome = request.POST.get("nome", "").strip()
            descricao = request.POST.get("descricao", "").strip()
            setor = request.POST.get("setor", "").strip()
            solicitante = request.POST.get("solicitante", "").strip()
            responsavel = request.POST.get("responsavel", "").strip()
            colaboradores_ids = _parse_colaborador_ids_input(request.POST.getlist("colaborador_ids"))
            colaboradores_nomes_legacy = _parse_colaboradores_input(request.POST.get("colaboradores", ""))
            horas_dia, horas_dia_error = _parse_horas_dia_input(request.POST.get("horas_dia", ""))
            contrato_id = request.POST.get("contrato")
            data_raw = request.POST.get("data_registro", "").strip()
            classificacao_id = request.POST.get("classificacao")
            data_registro = None
            if data_raw:
                try:
                    data_registro = datetime.strptime(data_raw, "%Y-%m-%d").date()
                except ValueError:
                    data_registro = None
            if not nome:
                if request.headers.get("x-requested-with") == "XMLHttpRequest":
                    return JsonResponse({"ok": False, "message": "Informe um nome para o trabalho.", "level": "error"}, status=400)
                message = "Informe um nome para o trabalho."
                message_level = "error"
            elif horas_dia_error:
                if request.headers.get("x-requested-with") == "XMLHttpRequest":
                    return JsonResponse({"ok": False, "message": horas_dia_error, "level": "error"}, status=400)
                message = horas_dia_error
                message_level = "error"
            else:
                contrato = None
                if contrato_id:
                    contrato = RadarContrato.objects.filter(pk=contrato_id).first()
                classificacao = None
                if classificacao_id:
                    classificacao = RadarClassificacao.objects.filter(pk=classificacao_id).first()
                novo_trabalho = RadarTrabalho.objects.create(
                    radar=radar,
                    nome=nome,
                    descricao=descricao,
                    setor=setor,
                    solicitante=solicitante,
                    responsavel=responsavel,
                    contrato=contrato,
                    data_registro=data_registro or timezone.localdate(),
                    classificacao=classificacao,
                    horas_dia=horas_dia,
                    ultimo_status_evento_em=timezone.now(),
                    criado_por=request.user,
                )
                colaboradores_nomes = _sync_trabalho_colaboradores(
                    novo_trabalho,
                    colaboradores_nomes=colaboradores_nomes_legacy,
                    colaboradores_ids=colaboradores_ids,
                )
                if request.headers.get("x-requested-with") == "XMLHttpRequest":
                    return JsonResponse(
                        {
                            "ok": True,
                            "message": "Trabalho criado.",
                            "level": "success",
                            "row": {
                                "id": novo_trabalho.id,
                                "nome": novo_trabalho.nome or "",
                                "descricao": novo_trabalho.descricao or "",
                                "status": novo_trabalho.status,
                                "status_label": novo_trabalho.get_status_display(),
                                "classificacao": novo_trabalho.classificacao.nome if novo_trabalho.classificacao else "",
                                "contrato": novo_trabalho.contrato.nome if novo_trabalho.contrato else "",
                                "data_registro": novo_trabalho.data_registro.isoformat() if novo_trabalho.data_registro else "",
                                "data_registro_label": novo_trabalho.data_registro.strftime("%d/%m/%Y") if novo_trabalho.data_registro else "",
                                "ultimo_status_evento_em": (
                                    novo_trabalho.ultimo_status_evento_em.isoformat()
                                    if novo_trabalho.ultimo_status_evento_em
                                    else ""
                                ),
                                "setor": novo_trabalho.setor or "",
                                "solicitante": novo_trabalho.solicitante or "",
                                "responsavel": novo_trabalho.responsavel or "",
                                "horas_dia": str(novo_trabalho.horas_dia) if novo_trabalho.horas_dia is not None else "",
                                "colaboradores": ", ".join(colaboradores_nomes),
                                "total_colaboradores": len(colaboradores_nomes),
                                "total_atividades": 0,
                                "total_horas": "0.00",
                                "detalhe_url": reverse("radar_trabalho_detail", args=[radar.pk, novo_trabalho.pk]),
                            },
                        }
                    )
                return redirect("radar_detail", pk=radar.pk)
        if action == "quick_status_trabalho":
            trabalho_id = request.POST.get("trabalho_id")
            trabalho = get_object_or_404(RadarTrabalho, pk=trabalho_id, radar=radar)
            _sync_trabalho_status(trabalho)
            if request.headers.get("x-requested-with") == "XMLHttpRequest":
                return JsonResponse(
                    {
                        "ok": False,
                        "id": trabalho.id,
                        "status": trabalho.status,
                        "status_label": trabalho.get_status_display(),
                        "message": "Status do trabalho e automatico com base nas atividades.",
                        "level": "warning",
                    },
                    status=400,
                )
            return redirect("radar_detail", pk=radar.pk)
        if action == "update_radar":
            nome = request.POST.get("nome", "").strip()
            descricao = request.POST.get("descricao", "").strip()
            local = request.POST.get("local", "").strip()
            id_radar_raw = request.POST.get("id_radar", "").strip()
            if not nome:
                message = "Informe um nome para o radar."
                message_level = "error"
            else:
                id_radar = None
                if id_radar_raw:
                    id_radar, _ = RadarID.objects.get_or_create(codigo=id_radar_raw.upper())
                radar.nome = nome
                radar.descricao = descricao
                radar.local = local
                radar.id_radar = id_radar
                radar.save(update_fields=["nome", "descricao", "local", "id_radar"])
                return redirect("radar_detail", pk=radar.pk)
        if action == "delete_radar":
            radar.delete()
            return redirect("radar_list")

    trabalhos_base = (
        radar.trabalhos.annotate(
            total_atividades=Count("atividades"),
            total_horas=Coalesce(
                Sum("atividades__horas_trabalho"),
                Value(Decimal("0.00")),
            ),
        )
        .select_related(
            "classificacao",
            "contrato",
        )
        .prefetch_related("colaboradores")
    )
    if classificacao_filter:
        trabalhos_base = trabalhos_base.filter(classificacao_id=classificacao_filter)
    total_trabalhos = trabalhos_base.count()
    trabalhos_tabela = trabalhos_base

    trabalhos_tabela = trabalhos_tabela.order_by(
        F("ultimo_status_evento_em").desc(nulls_last=True),
        "-data_registro",
        "nome",
    )

    trabalhos_table_data = []
    for trabalho in trabalhos_tabela:
        colaboradores_nomes = _trabalho_colaboradores_nomes(trabalho)
        trabalhos_table_data.append(
            {
                "id": trabalho.id,
                "nome": trabalho.nome or "",
                "descricao": trabalho.descricao or "",
                "status": trabalho.status,
                "status_label": trabalho.get_status_display(),
                "classificacao": trabalho.classificacao.nome if trabalho.classificacao else "",
                "contrato": trabalho.contrato.nome if trabalho.contrato else "",
                "data_registro": trabalho.data_registro.isoformat() if trabalho.data_registro else "",
                "data_registro_label": trabalho.data_registro.strftime("%d/%m/%Y") if trabalho.data_registro else "",
                "ultimo_status_evento_em": (
                    trabalho.ultimo_status_evento_em.isoformat()
                    if trabalho.ultimo_status_evento_em
                    else ""
                ),
                "setor": trabalho.setor or "",
                "solicitante": trabalho.solicitante or "",
                "responsavel": trabalho.responsavel or "",
                "horas_dia": str(trabalho.horas_dia) if trabalho.horas_dia is not None else "",
                "colaboradores": ", ".join(colaboradores_nomes),
                "total_colaboradores": len(colaboradores_nomes),
                "total_atividades": trabalho.total_atividades or 0,
                "total_horas": str((trabalho.total_horas or Decimal("0.00")).quantize(Decimal("0.01"))),
                "detalhe_url": reverse("radar_trabalho_detail", args=[radar.pk, trabalho.pk]),
            }
        )
    return render(
        request,
        "core/radar_detail.html",
        {
            "radar": radar,
            "trabalhos_table_data": trabalhos_table_data,
            "total_trabalhos": total_trabalhos,
            "classificacoes": classificacoes,
            "contratos": RadarContrato.objects.order_by("nome"),
            "classificacao_filter": classificacao_filter,
            "colaboradores_catalogo": list(_radar_colaboradores_catalogo(radar)),
            "can_manage": can_manage,
            "is_radar_creator": is_creator,
            "has_id_radar_access": has_id_radar_access,
            "message": message,
            "message_level": message_level,
            "open_cadastro": request.GET.get("cadastro", "").strip(),
            "export_month_default": timezone.localdate().strftime("%Y-%m"),
            "radar_export_pdf_url": reverse("radar_export_pdf", args=[radar.pk]),
        },
    )


@login_required
def radar_agenda(request, pk):
    denied_response = _require_internal_module_access(request, "RADAR")
    if denied_response:
        return denied_response
    cliente = _get_cliente(request.user)
    if not cliente and not _is_admin_user(request.user):
        return HttpResponseForbidden("Sem cadastro de cliente.")

    if _is_admin_user(request.user) and not cliente:
        radar = get_object_or_404(Radar, pk=pk)
        is_creator = False
        has_id_radar_access = False
        can_manage = False
    else:
        radar = get_object_or_404(
            Radar,
            Q(pk=pk),
            Q(cliente=cliente) | Q(id_radar__in=cliente.radares.all()),
        )
        is_creator = _is_radar_creator_user(request.user, radar)
        has_id_radar_access = bool(cliente) and (
            radar.id_radar_id and cliente.radares.filter(pk=radar.id_radar_id).exists()
        )
        can_manage = is_creator

    today = timezone.localdate()
    dia_raw = request.GET.get("dia", "").strip()
    if dia_raw:
        try:
            selected_day = datetime.strptime(dia_raw, "%Y-%m-%d").date()
        except ValueError:
            selected_day = today
    else:
        selected_day = today

    month_start = selected_day.replace(day=1)
    month_end = date(
        selected_day.year,
        selected_day.month,
        calendar.monthrange(selected_day.year, selected_day.month)[1],
    )

    month_exec_rows = list(
        RadarAtividadeDiaExecucao.objects.filter(
            atividade__trabalho__radar=radar,
            data_execucao__gte=month_start,
            data_execucao__lte=month_end,
        )
        .select_related("atividade", "atividade__trabalho")
        .order_by("data_execucao")
    )
    month_observation_rows = list(
        RadarTrabalhoObservacao.objects.filter(
            trabalho__radar=radar,
            data_observacao__gte=month_start,
            data_observacao__lte=month_end,
        )
        .select_related("trabalho")
        .order_by("data_observacao", "trabalho__nome", "id")
    )
    month_day_counts = {}
    month_observation_counts = {}
    daily_rows = []
    daily_observation_rows = []
    atividade_ids = {
        row.atividade_id
        for row in month_exec_rows
        if row.atividade_id
    }
    colaboradores_por_atividade = _atividade_colaboradores_count_map(atividade_ids)
    month_summary_map = {}
    status_map = dict(RadarTrabalho.Status.choices)
    month_total_horas = Decimal("0.00")

    for row in month_exec_rows:
        month_day_counts[row.data_execucao] = month_day_counts.get(row.data_execucao, 0) + 1
        if row.data_execucao == selected_day:
            daily_rows.append(row)
        atividade = row.atividade
        trabalho = atividade.trabalho if atividade else None
        if not atividade or not trabalho:
            continue
        total_colaboradores = colaboradores_por_atividade.get(atividade.id, 0)
        multiplier = Decimal(total_colaboradores if total_colaboradores > 0 else 1)
        horas_dia = trabalho.horas_dia if trabalho.horas_dia is not None else Decimal("8.00")
        horas_atividade_dia = (horas_dia * multiplier).quantize(Decimal("0.01"))
        month_total_horas += horas_atividade_dia

        summary = month_summary_map.get(trabalho.id)
        if summary is None:
            summary = {
                "trabalho_id": trabalho.id,
                "trabalho_nome": trabalho.nome or "-",
                "trabalho_url": reverse("radar_trabalho_detail", args=[radar.pk, trabalho.id]),
                "inicio": row.data_execucao,
                "fim": row.data_execucao,
                "status": trabalho.status,
                "status_label": status_map.get(trabalho.status, trabalho.status),
                "total_slots": 0,
                "total_horas": Decimal("0.00"),
            }
            month_summary_map[trabalho.id] = summary
        if row.data_execucao < summary["inicio"]:
            summary["inicio"] = row.data_execucao
        if row.data_execucao > summary["fim"]:
            summary["fim"] = row.data_execucao
        summary["total_slots"] += 1
        summary["total_horas"] += horas_atividade_dia

    for observation in month_observation_rows:
        month_observation_counts[observation.data_observacao] = (
            month_observation_counts.get(observation.data_observacao, 0) + 1
        )
        if observation.data_observacao == selected_day:
            daily_observation_rows.append(observation)

    month_total_horas = month_total_horas.quantize(Decimal("0.01"))
    month_summary = []
    for summary in sorted(
        month_summary_map.values(),
        key=lambda item: (item["inicio"], item["fim"], (item["trabalho_nome"] or "").casefold()),
    ):
        summary["inicio_display"] = summary["inicio"].strftime("%d/%m/%Y") if summary["inicio"] else "-"
        summary["fim_display"] = summary["fim"].strftime("%d/%m/%Y") if summary["fim"] else "-"
        summary["total_horas"] = summary["total_horas"].quantize(Decimal("0.01"))
        month_summary.append(summary)

    daily_activity_ids = {
        row.atividade_id
        for row in daily_rows
        if row.atividade_id
    }
    colaboradores_rows_por_atividade = _atividade_colaboradores_rows_map(daily_activity_ids)

    daily_groups = []
    daily_groups_map = {}
    daily_total_atividades = 0
    daily_total_observacoes = 0
    daily_total_horas = Decimal("0.00")
    for row in daily_rows:
        atividade = row.atividade
        trabalho = atividade.trabalho
        total_colaboradores = colaboradores_por_atividade.get(atividade.id, 0)
        multiplier = Decimal(total_colaboradores if total_colaboradores > 0 else 1)
        horas_dia = trabalho.horas_dia if trabalho.horas_dia is not None else Decimal("8.00")
        horas_atividade_dia = (horas_dia * multiplier).quantize(Decimal("0.01"))

        group = daily_groups_map.get(trabalho.id)
        if group is None:
            group = {
                "trabalho_id": trabalho.id,
                "trabalho_nome": trabalho.nome or "-",
                "trabalho_status": trabalho.status,
                "trabalho_status_label": trabalho.get_status_display(),
                "trabalho_url": reverse("radar_trabalho_detail", args=[radar.pk, trabalho.id]),
                "total_colaboradores": 0,
                "total_atividades": 0,
                "total_observacoes": 0,
                "total_horas_dia": Decimal("0.00"),
                "atividades": [],
                "observacoes": [],
                "_colaborador_keys": set(),
            }
            daily_groups_map[trabalho.id] = group
            daily_groups.append(group)

        for colaborador_row in colaboradores_rows_por_atividade.get(atividade.id, []):
            nome = " ".join((_radar_colaborador_nome(colaborador_row) or "").strip().split())
            if not nome:
                continue
            key = (
                f"id:{colaborador_row.colaborador_id}"
                if colaborador_row.colaborador_id
                else f"nome:{nome.casefold()}"
            )
            group["_colaborador_keys"].add(key)

        group["atividades"].append(
            {
                "id": atividade.id,
                "nome": atividade.nome or "-",
                "status": atividade.status,
                "status_label": atividade.get_status_display(),
                "horas_dia": horas_atividade_dia,
            }
        )
        group["total_atividades"] += 1
        group["total_horas_dia"] += horas_atividade_dia
        daily_total_atividades += 1
        daily_total_horas += horas_atividade_dia

    for observation in daily_observation_rows:
        trabalho = observation.trabalho
        if not trabalho:
            continue

        group = daily_groups_map.get(trabalho.id)
        if group is None:
            group = {
                "trabalho_id": trabalho.id,
                "trabalho_nome": trabalho.nome or "-",
                "trabalho_status": trabalho.status,
                "trabalho_status_label": trabalho.get_status_display(),
                "trabalho_url": reverse("radar_trabalho_detail", args=[radar.pk, trabalho.id]),
                "total_colaboradores": len(_trabalho_colaboradores_nomes(trabalho)),
                "total_atividades": 0,
                "total_observacoes": 0,
                "total_horas_dia": Decimal("0.00"),
                "atividades": [],
                "observacoes": [],
                "_colaborador_keys": set(),
            }
            daily_groups_map[trabalho.id] = group
            daily_groups.append(group)

        texto = (observation.texto or "").strip()
        if not texto:
            continue

        group["observacoes"].append(
            {
                "id": observation.id,
                "texto": texto,
                "data_display": observation.data_observacao.strftime("%d/%m/%Y"),
            }
        )
        group["total_observacoes"] += 1
        daily_total_observacoes += 1

    for group in daily_groups:
        group["total_horas_dia"] = group["total_horas_dia"].quantize(Decimal("0.01"))
        colaborador_keys = group.pop("_colaborador_keys")
        if colaborador_keys:
            group["total_colaboradores"] = len(colaborador_keys)
    daily_groups.sort(key=lambda item: (item["trabalho_nome"] or "").casefold())
    daily_total_horas = daily_total_horas.quantize(Decimal("0.01"))

    calendar_weeks = []
    # Semana visual da agenda: domingo a sabado.
    month_calendar = calendar.Calendar(firstweekday=6).monthdatescalendar(selected_day.year, selected_day.month)
    for week in month_calendar:
        week_cells = []
        for cell_day in week:
            is_current_month = cell_day.month == selected_day.month
            activity_count = month_day_counts.get(cell_day, 0) if is_current_month else 0
            observation_count = month_observation_counts.get(cell_day, 0) if is_current_month else 0
            week_cells.append(
                {
                    "iso": cell_day.isoformat(),
                    "day": cell_day.day,
                    "is_current_month": is_current_month,
                    "is_selected": cell_day == selected_day,
                    "is_today": cell_day == today,
                    "activity_count": activity_count,
                    "observation_count": observation_count,
                    "has_observation": observation_count > 0,
                }
            )
        calendar_weeks.append(week_cells)

    month_names = [
        "Janeiro",
        "Fevereiro",
        "Marco",
        "Abril",
        "Maio",
        "Junho",
        "Julho",
        "Agosto",
        "Setembro",
        "Outubro",
        "Novembro",
        "Dezembro",
    ]
    month_label = f"{month_names[selected_day.month - 1]} de {selected_day.year}"

    context = {
        "radar": radar,
        "can_manage": can_manage or _is_admin_user(request.user),
        "is_radar_creator": is_creator,
        "has_id_radar_access": has_id_radar_access,
        "selected_day": selected_day,
        "selected_day_iso": selected_day.isoformat(),
        "selected_day_display": selected_day.strftime("%d/%m/%Y"),
        "today_iso": today.isoformat(),
        "prev_day_iso": (selected_day - timedelta(days=1)).isoformat(),
        "next_day_iso": (selected_day + timedelta(days=1)).isoformat(),
        "prev_month_iso": _add_months(selected_day, -1).isoformat(),
        "next_month_iso": _add_months(selected_day, 1).isoformat(),
        "month_label": month_label,
        "weekday_labels": ["Dom", "Seg", "Ter", "Qua", "Qui", "Sex", "Sab"],
        "calendar_weeks": calendar_weeks,
        "daily_groups": daily_groups,
        "daily_total_trabalhos": len(daily_groups),
        "daily_total_atividades": daily_total_atividades,
        "daily_total_observacoes": daily_total_observacoes,
        "daily_total_horas": daily_total_horas,
        "month_summary": month_summary,
        "month_total_trabalhos": len(month_summary),
        "month_total_horas": month_total_horas,
    }

    if _is_partial_request(request) and request.GET.get("section") == "daily":
        return render(request, "core/partials/radar_agenda_daily_section.html", context)

    return render(
        request,
        "core/radar_agenda.html",
        context,
    )


def _radar_month_summary_snapshot(radar, month_start, month_end):
    month_exec_rows = list(
        RadarAtividadeDiaExecucao.objects.filter(
            atividade__trabalho__radar=radar,
            data_execucao__gte=month_start,
            data_execucao__lte=month_end,
        )
        .select_related("atividade", "atividade__trabalho")
        .order_by(
            "data_execucao",
            "atividade__trabalho__nome",
            "atividade__ordem",
            "atividade__nome",
            "id",
        )
    )
    atividade_ids = {
        row.atividade_id
        for row in month_exec_rows
        if row.atividade_id
    }
    colaboradores_por_atividade = _atividade_colaboradores_count_map(atividade_ids)
    status_map = dict(RadarTrabalho.Status.choices)
    month_summary_map = {}
    month_total_horas = Decimal("0.00")
    for row in month_exec_rows:
        atividade = row.atividade
        trabalho = atividade.trabalho if atividade else None
        if not atividade or not trabalho:
            continue
        trabalho_id = trabalho.id
        total_colaboradores = colaboradores_por_atividade.get(atividade.id, 0)
        multiplier = Decimal(total_colaboradores if total_colaboradores > 0 else 1)
        horas_dia = trabalho.horas_dia or Decimal("8.00")
        horas_atividade_dia = (horas_dia * multiplier).quantize(Decimal("0.01"))
        month_total_horas += horas_atividade_dia
        summary = month_summary_map.get(trabalho_id)
        if summary is None:
            summary = {
                "trabalho_id": trabalho_id,
                "trabalho_nome": trabalho.nome or "-",
                "inicio": row.data_execucao,
                "fim": row.data_execucao,
                "status": trabalho.status,
                "status_label": status_map.get(trabalho.status, trabalho.status),
                "total_slots": 0,
                "total_horas": Decimal("0.00"),
            }
            month_summary_map[trabalho_id] = summary
        if row.data_execucao < summary["inicio"]:
            summary["inicio"] = row.data_execucao
        if row.data_execucao > summary["fim"]:
            summary["fim"] = row.data_execucao
        summary["total_slots"] += 1
        summary["total_horas"] += horas_atividade_dia

    month_summary = []
    for summary in sorted(
        month_summary_map.values(),
        key=lambda item: (item["inicio"], item["fim"], (item["trabalho_nome"] or "").casefold()),
    ):
        summary["inicio_display"] = summary["inicio"].strftime("%d/%m/%Y") if summary["inicio"] else "-"
        summary["fim_display"] = summary["fim"].strftime("%d/%m/%Y") if summary["fim"] else "-"
        summary["total_horas"] = summary["total_horas"].quantize(Decimal("0.01"))
        month_summary.append(summary)

    month_total_horas = month_total_horas.quantize(Decimal("0.01"))
    return {
        "month_summary": month_summary,
        "month_total_horas": month_total_horas,
        "trabalhos_ids": {item["trabalho_id"] for item in month_summary},
    }


def _build_radar_relatorio_pdf_context(radar, month_start, month_end):
    month_snapshot = _radar_month_summary_snapshot(radar, month_start, month_end)
    month_summary = month_snapshot["month_summary"]
    trabalhos_ids = month_snapshot["trabalhos_ids"]
    month_total_horas = month_snapshot["month_total_horas"]
    if not month_summary:
        return {
            "radar": radar,
            "month_summary": [],
        }

    trabalhos_qs = (
        RadarTrabalho.objects.filter(pk__in=trabalhos_ids)
        .select_related("classificacao", "contrato")
        .prefetch_related("atividades", "observacoes")
    )
    trabalhos_map = {trabalho.id: trabalho for trabalho in trabalhos_qs}

    exec_rows = list(
        RadarAtividadeDiaExecucao.objects.filter(
            atividade__trabalho__radar=radar,
            data_execucao__gte=month_start,
            data_execucao__lte=month_end,
        )
        .select_related(
            "atividade",
            "atividade__trabalho",
        )
        .order_by(
            "atividade__trabalho_id",
            "data_execucao",
            "atividade__ordem",
            "atividade__nome",
            "id",
        )
    )
    atividade_ids = {
        exec_row.atividade_id
        for exec_row in exec_rows
        if exec_row.atividade_id
    }
    colaboradores_por_atividade = _atividade_colaboradores_rows_map(atividade_ids)
    exec_por_trabalho = {}
    for exec_row in exec_rows:
        trabalho_id = exec_row.atividade.trabalho_id if exec_row.atividade else None
        if not trabalho_id:
            continue
        exec_por_trabalho.setdefault(trabalho_id, []).append(exec_row)

    trabalho_pages = []
    for summary_row in month_summary:
        trabalho_id = summary_row.get("trabalho_id")
        trabalho = trabalhos_map.get(trabalho_id)
        if not trabalho:
            continue

        horas_dia = trabalho.horas_dia if trabalho.horas_dia is not None else Decimal("8.00")
        horas_dia = horas_dia.quantize(Decimal("0.01"))
        execucoes = exec_por_trabalho.get(trabalho_id, [])
        colaborador_tables_map = {}
        for execucao in execucoes:
            atividade = execucao.atividade
            atividade_nome = atividade.nome if atividade and atividade.nome else "-"
            row_payload = {
                "data_display": execucao.data_execucao.strftime("%d/%m/%Y"),
                "atividade_nome": atividade_nome,
                "hxh": horas_dia,
            }
            atividade_colaboradores = colaboradores_por_atividade.get(getattr(atividade, "id", None), [])
            if not atividade_colaboradores:
                table = colaborador_tables_map.setdefault(
                    "sem-colaborador",
                    {
                        "nome": "Nao informado",
                        "rows": [],
                        "total_horas": Decimal("0.00"),
                    },
                )
                table["rows"].append(row_payload)
                table["total_horas"] += horas_dia
                continue
            seen_keys = set()
            for colaborador_row in atividade_colaboradores:
                nome = " ".join((_radar_colaborador_nome(colaborador_row) or "").strip().split()) or "Nao informado"
                key = (
                    f"id:{colaborador_row.colaborador_id}"
                    if colaborador_row.colaborador_id
                    else f"nome:{nome.casefold()}"
                )
                if key in seen_keys:
                    continue
                seen_keys.add(key)
                table = colaborador_tables_map.setdefault(
                    key,
                    {
                        "nome": nome,
                        "rows": [],
                        "total_horas": Decimal("0.00"),
                    },
                )
                table["rows"].append(row_payload)
                table["total_horas"] += horas_dia

        colaborador_tables = []
        for item in sorted(
            colaborador_tables_map.values(),
            key=lambda current: ((current["nome"] or "").casefold(), current["nome"]),
        ):
            item["total_horas"] = item["total_horas"].quantize(Decimal("0.01"))
            colaborador_tables.append(item)

        total_execucoes = len(execucoes)
        total_horas_trabalho = summary_row["total_horas"]
        prefetched = getattr(trabalho, "_prefetched_objects_cache", {})
        atividades_base = prefetched.get("atividades")
        if atividades_base is None:
            atividades_base = list(trabalho.atividades.all())
        atividades_ordenadas = sorted(
            atividades_base,
            key=lambda item: (item.ordem or 0, item.criado_em, item.id),
        )
        atividades_resumo = [
            {
                "nome": atividade.nome or "-",
                "descricao": atividade.descricao or "",
                "ordem": atividade.ordem or 0,
            }
            for atividade in atividades_ordenadas
        ]
        observacoes_base = prefetched.get("observacoes")
        if observacoes_base is None:
            observacoes_base = list(trabalho.observacoes.all())
        observacoes_ordenadas = sorted(
            observacoes_base,
            key=lambda item: (item.data_observacao, item.id),
            reverse=True,
        )
        observacoes_resumo = [
            {
                "data_display": observacao.data_observacao.strftime("%d/%m/%Y"),
                "texto": observacao.texto or "",
            }
            for observacao in observacoes_ordenadas
            if (observacao.texto or "").strip()
        ]

        trabalho_pages.append(
            {
                "id": trabalho.id,
                "nome": trabalho.nome or "-",
                "descricao": trabalho.descricao or "Sem descricao.",
                "setor": trabalho.setor or "-",
                "solicitante": trabalho.solicitante or "-",
                "responsavel": trabalho.responsavel or "-",
                "classificacao": trabalho.classificacao.nome if trabalho.classificacao else "-",
                "contrato": trabalho.contrato.nome if trabalho.contrato else "-",
                "status_label": trabalho.get_status_display(),
                "data_registro_display": trabalho.data_registro.strftime("%d/%m/%Y") if trabalho.data_registro else "-",
                "inicio_display": summary_row["inicio_display"],
                "fim_display": summary_row["fim_display"],
                "horas_dia": horas_dia,
                "total_execucoes": total_execucoes,
                "total_colaboradores": len(
                    [item for key, item in colaborador_tables_map.items() if key != "sem-colaborador"]
                ),
                "total_horas": total_horas_trabalho,
                "atividades_resumo": atividades_resumo,
                "observacoes_resumo": observacoes_resumo,
                "colaborador_tables": colaborador_tables,
            }
        )

    month_names = [
        "Janeiro",
        "Fevereiro",
        "Marco",
        "Abril",
        "Maio",
        "Junho",
        "Julho",
        "Agosto",
        "Setembro",
        "Outubro",
        "Novembro",
        "Dezembro",
    ]
    month_label = f"{month_names[month_start.month - 1]} de {month_start.year}"
    logo_path = finders.find("core/logoset.png") or finders.find("core/FAVICON_PRETO.png")
    logo_uri = Path(logo_path).as_uri() if logo_path else ""
    return {
        "radar": radar,
        "month_label": month_label,
        "month_iso": month_start.strftime("%Y-%m"),
        "month_summary": month_summary,
        "month_total_trabalhos": len(month_summary),
        "month_total_horas": month_total_horas,
        "trabalho_pages": trabalho_pages,
        "gerado_em_display": timezone.localdate().strftime("%d/%m/%Y"),
        "logo_uri": logo_uri,
    }


def _render_radar_relatorio_pdf(context):
    try:
        from weasyprint import CSS, HTML
    except ImportError:
        return None
    html = render_to_string("core/radar_relatorio_pdf.html", context)
    css_path = finders.find("css/radar_relatorio_pdf.css")
    stylesheets = [CSS(filename=css_path)] if css_path else None
    pdf_content = HTML(string=html, base_url=str(settings.BASE_DIR)).write_pdf(stylesheets=stylesheets)
    return BytesIO(pdf_content)


def _radar_export_error_response(request, message, status=400):
    if _is_partial_request(request):
        return JsonResponse({"ok": False, "message": message}, status=status)
    return HttpResponse(message, status=status)


@login_required
def radar_export_pdf(request, pk):
    denied_response = _require_internal_module_access(request, "RADAR")
    if denied_response:
        return denied_response
    if request.method != "GET":
        return HttpResponseNotAllowed(["GET"])

    cliente = _get_cliente(request.user)
    if not cliente:
        return HttpResponseForbidden("Sem cadastro de cliente.")
    radar = get_object_or_404(
        Radar,
        Q(pk=pk),
        Q(cliente=cliente) | Q(id_radar__in=cliente.radares.all()),
    )
    if not _is_radar_creator_user(request.user, radar):
        return _radar_export_error_response(request, "Somente quem criou o radar pode exportar.", status=403)

    mes_raw = request.GET.get("mes", "").strip()
    if not re.match(r"^\d{4}-\d{2}$", mes_raw):
        return _radar_export_error_response(request, "Informe um mes valido no formato YYYY-MM.", status=400)
    try:
        month_start = datetime.strptime(f"{mes_raw}-01", "%Y-%m-%d").date()
    except ValueError:
        return _radar_export_error_response(request, "Mes invalido.", status=400)
    month_end = date(
        month_start.year,
        month_start.month,
        calendar.monthrange(month_start.year, month_start.month)[1],
    )

    context = _build_radar_relatorio_pdf_context(radar, month_start, month_end)
    if not context.get("month_summary"):
        return _radar_export_error_response(
            request,
            "Sem atividades executadas no mes selecionado. Relatorio bloqueado.",
            status=400,
        )

    pdf_buffer = _render_radar_relatorio_pdf(context)
    if not pdf_buffer:
        return _radar_export_error_response(request, "Biblioteca de PDF indisponivel (WeasyPrint).", status=500)

    radar_nome = re.sub(r"[^A-Za-z0-9_-]+", "_", str(radar.nome or f"radar_{radar.id}")).strip("_")
    if not radar_nome:
        radar_nome = f"radar_{radar.id}"
    filename = f"relatorio_{radar_nome}_{month_start.strftime('%Y-%m')}.pdf"
    response = HttpResponse(pdf_buffer.getvalue(), content_type="application/pdf")
    response["Content-Disposition"] = f'attachment; filename="{filename}"'
    return response


@login_required
def radar_trabalho_detail(request, radar_pk, pk):
    denied_response = _require_internal_module_access(request, "RADAR")
    if denied_response:
        return denied_response
    cliente = _get_cliente(request.user)
    if not cliente and not _is_admin_user(request.user):
        return HttpResponseForbidden("Sem cadastro de cliente.")

    if _is_admin_user(request.user) and not cliente:
        radar = get_object_or_404(Radar, pk=radar_pk)
        trabalho = get_object_or_404(RadarTrabalho, pk=pk, radar=radar)
        is_creator = False
        has_id_radar_access = False
        can_manage = False
    else:
        radar = get_object_or_404(
            Radar,
            Q(pk=radar_pk),
            Q(cliente=cliente) | Q(id_radar__in=cliente.radares.all()),
        )
        trabalho = get_object_or_404(RadarTrabalho, pk=pk, radar=radar)
        is_creator = _is_radar_creator_user(request.user, radar)
        has_id_radar_access = bool(cliente) and (
            radar.id_radar_id and cliente.radares.filter(pk=radar.id_radar_id).exists()
        )
        can_manage = is_creator

    message = request.GET.get("msg", "").strip()
    message_level = request.GET.get("level", "").strip() or "info"
    classificacoes = RadarClassificacao.objects.order_by("nome")

    can_edit_trabalho_by_creator = can_manage

    if request.method == "POST":
        action = request.POST.get("action")
        if action in {
            "create_atividade",
            "update_trabalho",
            "delete_trabalho",
            "duplicate_trabalho",
            "update_atividade",
            "quick_status_atividade",
            "set_agenda_atividade",
            "delete_atividade",
            "move_atividade",
            "move_atividade_to",
            "create_contrato",
            "create_classificacao",
            "create_observacao",
            "update_observacao",
            "delete_observacao",
        }:
            if not can_manage:
                return HttpResponseForbidden("Somente quem criou o radar pode alterar.")
        if action == "create_classificacao":
            nome = request.POST.get("classificacao_nome", "").strip()
            if not nome:
                msg = "Informe um nome de classificacao."
                level = "error"
                created = False
            else:
                classificacao, created = RadarClassificacao.objects.get_or_create(nome=nome)
                if created:
                    msg = "Classificacao criada."
                    level = "success"
                else:
                    msg = "Classificacao ja existe."
                    level = "warning"
            if request.headers.get("x-requested-with") == "XMLHttpRequest":
                return JsonResponse(
                    {
                        "ok": bool(nome),
                        "created": created,
                        "id": classificacao.id if nome and "classificacao" in locals() else None,
                        "nome": classificacao.nome if nome and "classificacao" in locals() else None,
                        "message": msg,
                        "level": level,
                    }
                )
            params = {"cadastro": "classificacao", "msg": msg, "level": level}
            return redirect(f"{reverse('radar_trabalho_detail', args=[radar.pk, trabalho.pk])}?{urlencode(params)}")
        if action == "create_contrato":
            nome = request.POST.get("contrato_nome", "").strip()
            if not nome:
                msg = "Informe um nome de contrato."
                level = "error"
                created = False
            else:
                contrato, created = RadarContrato.objects.get_or_create(nome=nome)
                if created:
                    msg = "Contrato criado."
                    level = "success"
                else:
                    msg = "Contrato ja existe."
                    level = "warning"
            if request.headers.get("x-requested-with") == "XMLHttpRequest":
                return JsonResponse(
                    {
                        "ok": bool(nome),
                        "created": created,
                        "id": contrato.id if nome and "contrato" in locals() else None,
                        "nome": contrato.nome if nome and "contrato" in locals() else None,
                        "message": msg,
                        "level": level,
                    }
                )
            params = {"cadastro": "contrato", "msg": msg, "level": level}
            return redirect(f"{reverse('radar_trabalho_detail', args=[radar.pk, trabalho.pk])}?{urlencode(params)}")
        if action == "update_trabalho":
            if not can_edit_trabalho_by_creator:
                return HttpResponseForbidden("Somente quem criou o radar pode editar.")
            nome = request.POST.get("nome", "").strip()
            descricao = request.POST.get("descricao", "").strip()
            setor = request.POST.get("setor", "").strip()
            solicitante = request.POST.get("solicitante", "").strip()
            responsavel = request.POST.get("responsavel", "").strip()
            colaboradores_ids = _parse_colaborador_ids_input(request.POST.getlist("colaborador_ids"))
            colaboradores_nomes_legacy = _parse_colaboradores_input(request.POST.get("colaboradores", ""))
            horas_dia, horas_dia_error = _parse_horas_dia_input(
                request.POST.get("horas_dia", ""),
                default=trabalho.horas_dia if trabalho.horas_dia is not None else Decimal("8.00"),
            )
            data_raw = request.POST.get("data_registro", "").strip()
            classificacao_id = request.POST.get("classificacao")
            contrato_id = request.POST.get("contrato")
            if not nome:
                message = "Informe um nome para o trabalho."
                message_level = "error"
            elif horas_dia_error:
                message = horas_dia_error
                message_level = "error"
            else:
                with transaction.atomic():
                    horas_dia_alterado = trabalho.horas_dia != horas_dia
                    if data_raw:
                        try:
                            trabalho.data_registro = datetime.strptime(data_raw, "%Y-%m-%d").date()
                        except ValueError:
                            pass
                    if classificacao_id:
                        trabalho.classificacao = RadarClassificacao.objects.filter(pk=classificacao_id).first()
                    else:
                        trabalho.classificacao = None
                    if contrato_id:
                        trabalho.contrato = RadarContrato.objects.filter(pk=contrato_id).first()
                    else:
                        trabalho.contrato = None
                    if not trabalho.criado_por_id and is_creator:
                        trabalho.criado_por = request.user
                    trabalho.nome = nome
                    trabalho.descricao = descricao
                    trabalho.setor = setor
                    trabalho.solicitante = solicitante
                    trabalho.responsavel = responsavel
                    trabalho.horas_dia = horas_dia
                    trabalho.save(
                        update_fields=[
                            "criado_por",
                            "nome",
                            "descricao",
                            "data_registro",
                            "classificacao",
                            "contrato",
                            "setor",
                            "solicitante",
                            "responsavel",
                            "horas_dia",
                        ]
                    )
                    _sync_trabalho_colaboradores(
                        trabalho,
                        colaboradores_nomes=colaboradores_nomes_legacy,
                        colaboradores_ids=colaboradores_ids,
                    )
                    if horas_dia_alterado:
                        _recalcular_horas_atividades_trabalho(trabalho)
                _sync_trabalho_status(trabalho)
                return redirect("radar_trabalho_detail", radar_pk=radar.pk, pk=trabalho.pk)
        if action == "delete_trabalho":
            if not can_edit_trabalho_by_creator:
                return HttpResponseForbidden("Somente quem criou o radar pode excluir.")
            trabalho.delete()
            return redirect("radar_detail", pk=radar.pk)
        if action == "duplicate_trabalho":
            if not can_edit_trabalho_by_creator:
                return HttpResponseForbidden("Somente quem criou o radar pode duplicar.")
            nome_copia = f"{trabalho.nome} - COPIA"
            novo_trabalho = RadarTrabalho.objects.create(
                radar=radar,
                nome=nome_copia,
                descricao=trabalho.descricao,
                data_registro=trabalho.data_registro,
                classificacao=trabalho.classificacao,
                contrato=trabalho.contrato,
                setor=trabalho.setor,
                solicitante=trabalho.solicitante,
                responsavel=trabalho.responsavel,
                horas_dia=trabalho.horas_dia,
                ultimo_status_evento_em=timezone.now(),
                criado_por=request.user,
            )
            colaboradores_origem = _trabalho_colaboradores_nomes(trabalho)
            if colaboradores_origem:
                _sync_trabalho_colaboradores(novo_trabalho, colaboradores_nomes=colaboradores_origem)
            atividades = list(trabalho.atividades.prefetch_related("colaboradores").all())
            if atividades:
                for atividade in atividades:
                    nova_atividade = RadarAtividade.objects.create(
                        trabalho=novo_trabalho,
                        nome=atividade.nome,
                        descricao=atividade.descricao,
                        horas_trabalho=Decimal("0.00"),
                        status=atividade.status,
                        inicio_execucao_em=atividade.inicio_execucao_em,
                        finalizada_em=atividade.finalizada_em,
                        ordem=atividade.ordem,
                    )
                    _sync_atividade_colaboradores(
                        nova_atividade,
                        colaboradores_nomes=_atividade_colaboradores_nomes(atividade),
                        colaboradores_ids=_atividade_colaboradores_ids(atividade),
                    )
            _sync_trabalho_status(novo_trabalho)
            return redirect("radar_trabalho_detail", radar_pk=radar.pk, pk=novo_trabalho.pk)
        if action == "create_observacao":
            texto = request.POST.get("observacao_texto", "").strip()
            data_raw = request.POST.get("observacao_data", "").strip()
            data_observacao = timezone.localdate()
            create_error = False
            if data_raw:
                try:
                    data_observacao = datetime.strptime(data_raw, "%Y-%m-%d").date()
                except ValueError:
                    message = "Data invalida para observacao."
                    message_level = "error"
                    create_error = True
            if not texto:
                message = "Informe o texto da observacao."
                message_level = "error"
                create_error = True
            if not create_error:
                RadarTrabalhoObservacao.objects.create(
                    trabalho=trabalho,
                    texto=texto,
                    data_observacao=data_observacao,
                )
                return redirect("radar_trabalho_detail", radar_pk=radar.pk, pk=trabalho.pk)
        if action == "update_observacao":
            observacao_id = request.POST.get("observacao_id")
            observacao = get_object_or_404(RadarTrabalhoObservacao, pk=observacao_id, trabalho=trabalho)
            texto = request.POST.get("observacao_texto", "").strip()
            data_raw = request.POST.get("observacao_data", "").strip()
            update_error = False
            if not texto:
                message = "Informe o texto da observacao."
                message_level = "error"
                update_error = True
            else:
                data_observacao = observacao.data_observacao
                if data_raw:
                    try:
                        data_observacao = datetime.strptime(data_raw, "%Y-%m-%d").date()
                    except ValueError:
                        message = "Data invalida para observacao."
                        message_level = "error"
                        update_error = True
                if not update_error:
                    observacao.texto = texto
                    observacao.data_observacao = data_observacao
                    observacao.save(update_fields=["texto", "data_observacao", "atualizado_em"])
                    return redirect("radar_trabalho_detail", radar_pk=radar.pk, pk=trabalho.pk)
        if action == "delete_observacao":
            observacao_id = request.POST.get("observacao_id")
            observacao = get_object_or_404(RadarTrabalhoObservacao, pk=observacao_id, trabalho=trabalho)
            observacao.delete()
            return redirect("radar_trabalho_detail", radar_pk=radar.pk, pk=trabalho.pk)
        if action == "create_atividade":
            nome = request.POST.get("nome", "").strip()
            descricao = request.POST.get("descricao", "").strip()
            status_raw = request.POST.get("status", "").strip()
            if status_raw not in dict(RadarAtividade.Status.choices):
                status_raw = RadarAtividade.Status.PENDENTE
            if not nome:
                if request.headers.get("x-requested-with") == "XMLHttpRequest":
                    return JsonResponse({"ok": False, "message": "Informe um nome para a atividade.", "level": "error"}, status=400)
                message = "Informe um nome para a atividade."
                message_level = "error"
            else:
                proxima_ordem = (
                    RadarAtividade.objects.filter(trabalho=trabalho).aggregate(max_ordem=Max("ordem"))["max_ordem"] or 0
                ) + 1
                nova_atividade = RadarAtividade.objects.create(
                    trabalho=trabalho,
                    nome=nome,
                    descricao=descricao,
                    horas_trabalho=Decimal("0.00"),
                    status=status_raw,
                    inicio_execucao_em=None,
                    finalizada_em=None,
                    ordem=proxima_ordem,
                )
                _sync_trabalho_status(trabalho)
                if request.headers.get("x-requested-with") == "XMLHttpRequest":
                    row = _atividade_response_payload(nova_atividade)
                    row.pop("ok", None)
                    return JsonResponse(
                        {
                            "ok": True,
                            "message": "Atividade criada.",
                            "level": "success",
                            "row": row,
                        }
                    )
                return redirect("radar_trabalho_detail", radar_pk=radar.pk, pk=trabalho.pk)
        if action == "update_atividade":
            atividade_id = request.POST.get("atividade_id")
            atividade = get_object_or_404(RadarAtividade, pk=atividade_id, trabalho=trabalho)
            nome_atividade = request.POST.get("nome", "").strip()
            colaboradores_ids = _parse_colaborador_ids_input(request.POST.getlist("colaborador_ids"))
            colaboradores_ids_permitidos = set(_trabalho_colaboradores_ids(trabalho)) | set(
                _atividade_colaboradores_ids(atividade)
            )
            colaboradores_ids = [
                colaborador_id
                for colaborador_id in colaboradores_ids
                if colaborador_id in colaboradores_ids_permitidos
            ]
            if not nome_atividade:
                if request.headers.get("x-requested-with") == "XMLHttpRequest":
                    return JsonResponse({"ok": False, "message": "Informe um nome para a atividade."}, status=400)
                return redirect("radar_trabalho_detail", radar_pk=radar.pk, pk=trabalho.pk)
            with transaction.atomic():
                atividade.nome = nome_atividade
                atividade.descricao = request.POST.get("descricao", "").strip()
                status_raw = request.POST.get("status", "").strip()
                if status_raw in dict(RadarAtividade.Status.choices):
                    atividade.status = status_raw
                atividade.save(
                    update_fields=[
                        "nome",
                        "descricao",
                        "status",
                    ]
                )
                _sync_atividade_colaboradores(
                    atividade,
                    colaboradores_ids=colaboradores_ids,
                )
                mudou_metricas = _sync_atividade_execucao_metrics_from_agenda(atividade)
                if mudou_metricas:
                    atividade.save(update_fields=["inicio_execucao_em", "finalizada_em", "horas_trabalho"])
            _sync_trabalho_status(trabalho)
            if request.headers.get("x-requested-with") == "XMLHttpRequest":
                return JsonResponse(_atividade_response_payload(atividade))
            return redirect("radar_trabalho_detail", radar_pk=radar.pk, pk=trabalho.pk)
        if action == "quick_status_atividade":
            atividade_id = request.POST.get("atividade_id")
            atividade = get_object_or_404(RadarAtividade, pk=atividade_id, trabalho=trabalho)
            status_raw = request.POST.get("status", "").strip()
            if status_raw not in dict(RadarAtividade.Status.choices):
                status_raw = RadarAtividade.Status.PENDENTE
            atividade.status = status_raw
            atividade.save(update_fields=["status"])
            _sync_trabalho_status(trabalho)
            if request.headers.get("x-requested-with") == "XMLHttpRequest":
                return JsonResponse(_atividade_response_payload(atividade))
            return redirect("radar_trabalho_detail", radar_pk=radar.pk, pk=trabalho.pk)
        if action == "set_agenda_atividade":
            atividade_id = request.POST.get("atividade_id")
            atividade = get_object_or_404(RadarAtividade, pk=atividade_id, trabalho=trabalho)
            agenda_raw = request.POST.get("dias_execucao", "")
            agenda_datas, agenda_error = _parse_agenda_execucao_input(agenda_raw)
            if agenda_error:
                return JsonResponse(
                    {
                        "ok": False,
                        "message": agenda_error,
                        "level": "error",
                    },
                    status=400,
                )
            with transaction.atomic():
                atuais = set(atividade.dias_execucao.values_list("data_execucao", flat=True))
                novos = set(agenda_datas or [])
                remover = atuais - novos
                adicionar = novos - atuais
                if remover:
                    atividade.dias_execucao.filter(data_execucao__in=remover).delete()
                if adicionar:
                    RadarAtividadeDiaExecucao.objects.bulk_create(
                        [
                            RadarAtividadeDiaExecucao(atividade=atividade, data_execucao=data_execucao)
                            for data_execucao in sorted(adicionar)
                        ],
                        ignore_conflicts=True,
                    )
                mudou_datas = _sync_atividade_execucao_metrics_from_agenda(atividade, agenda_datas=sorted(novos))
                if mudou_datas:
                    atividade.save(update_fields=["inicio_execucao_em", "finalizada_em", "horas_trabalho"])
            if request.headers.get("x-requested-with") == "XMLHttpRequest":
                atividade.refresh_from_db()
                return JsonResponse(_atividade_response_payload(atividade))
            return redirect("radar_trabalho_detail", radar_pk=radar.pk, pk=trabalho.pk)
        if action == "delete_atividade":
            atividade_id = request.POST.get("atividade_id")
            atividade = get_object_or_404(RadarAtividade, pk=atividade_id, trabalho=trabalho)
            atividade.delete()
            _normalizar_ordem_atividades(trabalho)
            _sync_trabalho_status(trabalho)
            if request.headers.get("x-requested-with") == "XMLHttpRequest":
                return JsonResponse(
                    {
                        "ok": True,
                        "id": atividade_id,
                    }
                )
            return redirect("radar_trabalho_detail", radar_pk=radar.pk, pk=trabalho.pk)
        if action == "move_atividade":
            atividade_id = request.POST.get("atividade_id")
            direcao = request.POST.get("direcao", "").strip().lower()
            atividade = get_object_or_404(RadarAtividade, pk=atividade_id, trabalho=trabalho)
            moved = False
            swap_with_id = None
            if direcao in {"up", "down"}:
                with transaction.atomic():
                    _normalizar_ordem_atividades(trabalho)
                    atividades_status = list(
                        RadarAtividade.objects.select_for_update()
                        .filter(trabalho=trabalho)
                        .order_by("ordem", "criado_em", "id")
                    )
                    ids = [item.id for item in atividades_status]
                    try:
                        idx = ids.index(atividade.id)
                    except ValueError:
                        idx = -1
                    offset = -1 if direcao == "up" else 1
                    neighbor_idx = idx + offset
                    if 0 <= idx < len(atividades_status) and 0 <= neighbor_idx < len(atividades_status):
                        atual = atividades_status[idx]
                        vizinho = atividades_status[neighbor_idx]
                        atual.ordem, vizinho.ordem = vizinho.ordem, atual.ordem
                        RadarAtividade.objects.bulk_update([atual, vizinho], ["ordem"])
                        moved = True
                        swap_with_id = vizinho.id
            if request.headers.get("x-requested-with") == "XMLHttpRequest":
                return JsonResponse(
                    {
                        "ok": True,
                        "id": atividade.id,
                        "direcao": direcao,
                        "moved": moved,
                        "swap_with_id": swap_with_id,
                    }
                )
            return redirect("radar_trabalho_detail", radar_pk=radar.pk, pk=trabalho.pk)
        if action == "move_atividade_to":
            atividade_id = request.POST.get("atividade_id")
            target_id = request.POST.get("target_atividade_id")
            atividade = get_object_or_404(RadarAtividade, pk=atividade_id, trabalho=trabalho)
            target = get_object_or_404(RadarAtividade, pk=target_id, trabalho=trabalho)

            moved = False
            if atividade.id != target.id:
                with transaction.atomic():
                    _normalizar_ordem_atividades(trabalho)
                    atividades_status = list(
                        RadarAtividade.objects.select_for_update()
                        .filter(trabalho=trabalho)
                        .order_by("ordem", "criado_em", "id")
                    )
                    ids = [item.id for item in atividades_status]
                    try:
                        idx_from = ids.index(atividade.id)
                        idx_to = ids.index(target.id)
                    except ValueError:
                        idx_from = -1
                        idx_to = -1
                    if 0 <= idx_from < len(atividades_status) and 0 <= idx_to < len(atividades_status):
                        item = atividades_status.pop(idx_from)
                        if idx_from < idx_to:
                            idx_to -= 1
                        atividades_status.insert(idx_to, item)
                        changed = []
                        for idx, atividade_item in enumerate(atividades_status, start=1):
                            if atividade_item.ordem != idx:
                                atividade_item.ordem = idx
                                changed.append(atividade_item)
                        if changed:
                            RadarAtividade.objects.bulk_update(changed, ["ordem"])
                        moved = True

            if request.headers.get("x-requested-with") == "XMLHttpRequest":
                return JsonResponse(
                    {
                        "ok": True,
                        "id": atividade.id,
                        "target_id": target.id,
                        "moved": moved,
                    }
                )
            return redirect("radar_trabalho_detail", radar_pk=radar.pk, pk=trabalho.pk)

    # Garante consistencia em horas historicas antes de montar a tabela.
    _recalcular_horas_atividades_trabalho(trabalho)

    contratos = RadarContrato.objects.order_by("nome")
    atividades_base = trabalho.atividades.prefetch_related("dias_execucao", "colaboradores").all()
    _normalizar_ordem_atividades(trabalho)
    atividades_ordenadas = atividades_base.order_by("ordem", "criado_em", "id")
    atividades_table_data = []
    for atividade in atividades_ordenadas:
        row = _atividade_response_payload(atividade)
        row.pop("ok", None)
        atividades_table_data.append(row)
    edit_atividade = None
    edit_atividade_id = request.GET.get("editar", "").strip()
    if edit_atividade_id:
        edit_atividade = RadarAtividade.objects.filter(pk=edit_atividade_id, trabalho=trabalho).prefetch_related("colaboradores").first()
    total_atividades = atividades_base.count()
    observacoes_trabalho = list(trabalho.observacoes.order_by("-data_observacao", "-id"))
    can_create_proposta_from_trabalho = can_edit_trabalho_by_creator
    can_duplicate_trabalho = can_edit_trabalho_by_creator
    return render(
        request,
        "core/radar_trabalho_detail.html",
        {
            "radar": radar,
            "trabalho": trabalho,
            "atividades_ordenadas": atividades_ordenadas,
            "atividades_table_data": atividades_table_data,
            "total_atividades": total_atividades,
            "contratos": contratos,
            "classificacoes": classificacoes,
            "status_choices": RadarAtividade.Status.choices,
            "can_manage": can_manage,
            "is_radar_creator": is_creator,
            "has_id_radar_access": has_id_radar_access,
            "message": message,
            "message_level": message_level,
            "open_cadastro": request.GET.get("cadastro", "").strip(),
            "edit_atividade": edit_atividade,
            "can_create_proposta_from_trabalho": can_create_proposta_from_trabalho,
            "can_duplicate_trabalho": can_duplicate_trabalho,
            "can_edit_trabalho_by_creator": can_edit_trabalho_by_creator,
            "trabalho_colaboradores": ", ".join(_trabalho_colaboradores_nomes(trabalho)),
            "trabalho_colaborador_ids": _trabalho_colaboradores_ids(trabalho),
            "colaboradores_catalogo": list(_radar_colaboradores_catalogo(radar)),
            "atividade_colaboradores_catalogo": _atividade_editor_colaboradores_catalogo(trabalho),
            "edit_atividade_colaborador_ids": _atividade_colaboradores_ids(edit_atividade) if edit_atividade else [],
            "observacoes_trabalho": observacoes_trabalho,
            "observacao_data_default": timezone.localdate().isoformat(),
        },
    )


@login_required
def ios_rack_modulo_detail(request, pk):
    denied_response = _require_internal_module_access(request, "IOS")
    if denied_response:
        return denied_response
    cliente = _get_cliente(request.user)
    if not cliente and not _is_admin_user(request.user):
        return HttpResponseForbidden("Sem cadastro de cliente.")
    module_qs = ModuloRackIO.objects.select_related("modulo_modelo", "rack")
    if cliente:
        module = get_object_or_404(
            module_qs,
            Q(pk=pk),
            Q(rack__cliente=cliente) | Q(rack__id_planta__in=cliente.plantas.all()),
        )
    else:
        module = get_object_or_404(module_qs, pk=pk)
    can_manage = bool(
        _is_admin_user(request.user)
        or (
            cliente
            and (
                module.rack.cliente_id == cliente.id
                or (
                    module.rack.id_planta_id
                    and cliente.plantas.filter(pk=module.rack.id_planta_id).exists()
                )
            )
        )
    )
    if request.method == "GET":
        return redirect(_ios_module_panel_url(module.rack_id, module.id))
    slot = RackSlotIO.objects.filter(modulo=module).select_related("rack").first()
    prev_slot = None
    next_slot = None
    if slot:
        prev_slot = (
            RackSlotIO.objects.filter(rack=module.rack, modulo__isnull=False, posicao__lt=slot.posicao)
            .select_related("modulo")
            .order_by("-posicao")
            .first()
        )
        next_slot = (
            RackSlotIO.objects.filter(rack=module.rack, modulo__isnull=False, posicao__gt=slot.posicao)
            .select_related("modulo")
            .order_by("posicao")
            .first()
        )
    if request.method == "POST":
        action = request.POST.get("action")
        if action in {
            "update_module_name",
            "update_module",
            "move_to_slot",
            "delete_module",
            "update_channels",
        } and not can_manage:
            return HttpResponseForbidden("Sem permissao.")
        if action == "update_module_name":
            return redirect("ios_rack_modulo_detail", pk=module.pk)
        if action == "update_module":
            if request.POST.get("delete_module") == "on":
                rack_id = module.rack_id
                module.delete()
                return redirect("ios_rack_detail", pk=rack_id)
            target_slot_id = request.POST.get("slot_id")
            if target_slot_id:
                target_slot = get_object_or_404(RackSlotIO, pk=target_slot_id, rack=module.rack)
                if not target_slot.modulo_id:
                    if slot:
                        slot.modulo = None
                        slot.save(update_fields=["modulo"])
                    target_slot.modulo = module
                    target_slot.save(update_fields=["modulo"])
            return redirect("ios_rack_modulo_detail", pk=module.pk)
        if action == "move_to_slot":
            target_slot_id = request.POST.get("slot_id")
            target_slot = get_object_or_404(RackSlotIO, pk=target_slot_id, rack=module.rack)
            if target_slot.modulo_id:
                return redirect("ios_rack_modulo_detail", pk=module.pk)
            if slot:
                slot.modulo = None
                slot.save(update_fields=["modulo"])
            target_slot.modulo = module
            target_slot.save(update_fields=["modulo"])
            return redirect("ios_rack_modulo_detail", pk=module.pk)
        if action == "delete_module":
            rack_id = module.rack_id
            module.delete()
            return redirect("ios_rack_detail", pk=rack_id)
        if action == "update_channels":
            for channel in module.canais.all():
                tag_raw = request.POST.get(f"tag_{channel.id}", "")
                descricao_raw = request.POST.get(f"descricao_{channel.id}")
                tipo_id = request.POST.get(f"tipo_{channel.id}")
                comissionado = request.POST.get(f"comissionado_{channel.id}") == "on"
                if descricao_raw is None:
                    continue
                if tag_raw is not None:
                    channel.tag = _normalize_channel_tag(tag_raw)
                channel.descricao = descricao_raw.strip()
                if tipo_id:
                    channel.tipo_id = tipo_id
                channel.comissionado = comissionado
                channel.save(update_fields=["tag", "descricao", "tipo_id", "comissionado"])
            return redirect("ios_rack_modulo_detail", pk=module.pk)
        if action == "bulk_update_channels":
            channel_ids = request.POST.getlist("channel_id")
            channels_qs = module.canais.filter(id__in=channel_ids)
            channels_map = {str(channel.id): channel for channel in channels_qs}
            for channel_id in channel_ids:
                channel = channels_map.get(channel_id)
                if not channel:
                    continue
                tag_raw = request.POST.get(f"tag_{channel_id}", "")
                descricao_raw = request.POST.get(f"descricao_{channel_id}", "")
                tipo_id = request.POST.get(f"tipo_{channel_id}")
                comissionado = request.POST.get(f"comissionado_{channel_id}") == "on"
                channel.tag = _normalize_channel_tag(tag_raw)
                channel.descricao = (descricao_raw or "").strip()
                if tipo_id:
                    channel.tipo_id = tipo_id
                channel.comissionado = comissionado
            if channels_map:
                CanalRackIO.objects.bulk_update(
                    channels_map.values(),
                    ["tag", "descricao", "tipo_id", "comissionado"],
                )
            if request.headers.get("x-requested-with") == "XMLHttpRequest":
                return JsonResponse({"ok": True, "updated": len(channels_map)})
            return redirect("ios_rack_modulo_detail", pk=module.pk)
        if action == "inline_update_channel":
            channel_id = request.POST.get("channel_id")
            channel = get_object_or_404(module.canais, pk=channel_id)
            tag_raw = request.POST.get("tag", "")
            descricao_raw = request.POST.get("descricao", "")
            tipo_id = request.POST.get("tipo")
            comissionado = request.POST.get("comissionado") == "on"
            channel.tag = _normalize_channel_tag(tag_raw)
            channel.descricao = (descricao_raw or "").strip()
            if tipo_id:
                channel.tipo_id = tipo_id
            channel.comissionado = comissionado
            channel.save(update_fields=["tag", "descricao", "tipo_id", "comissionado"])
            if request.headers.get("x-requested-with") == "XMLHttpRequest":
                return JsonResponse({"ok": True})
            return redirect("ios_rack_modulo_detail", pk=module.pk)
    channels = list(module.canais.select_related("tipo", "ativo", "ativo_item").order_by("indice"))
    tag_counts = {}
    for channel in channels:
        tag_key = (channel.tag or "").strip().upper()
        if tag_key:
            tag_counts[tag_key] = tag_counts.get(tag_key, 0) + 1
    tags_repetidas = {key for key, count in tag_counts.items() if count > 1}
    channel_types = TipoCanalIO.objects.filter(ativo=True).order_by("nome")
    vacant_slots = RackSlotIO.objects.filter(rack=module.rack, modulo__isnull=True).order_by("posicao")
    rack_slots = (
        RackSlotIO.objects.filter(rack=module.rack)
        .select_related("modulo")
        .order_by("posicao")
    )
    return render(
        request,
        "core/ios_modulo_detail.html",
        {
            "module": module,
            "channels": channels,
            "channel_types": channel_types,
            "tags_repetidas": tags_repetidas,
            "rack": module.rack,
            "slot": slot,
            "vacant_slots": vacant_slots,
            "has_vacant_slots": vacant_slots.exists(),
            "prev_slot": prev_slot,
            "next_slot": next_slot,
            "rack_slots": rack_slots,
        },
    )


def _normalize_proposta_tipo(raw_tipo):
    tipo = (raw_tipo or "").strip().lower()
    if tipo in {"enviadas", "enviada", "sent"}:
        return "enviadas"
    if tipo in {"recebidas", "recebida", "received"}:
        return "recebidas"
    return "recebidas"


def _proposta_status_annotations(queryset):
    return (
        queryset.annotate(
            status_order=Case(
                When(finalizada=True, then=Value(6)),
                When(andamento="EXECUTANDO", then=Value(4)),
                When(aprovada=True, then=Value(3)),
                When(aprovada=False, then=Value(5)),
                When(aprovada__isnull=True, valor__isnull=True, then=Value(1)),
                When(aprovada__isnull=True, valor=0, then=Value(1)),
                default=Value(2),
                output_field=IntegerField(),
            )
        )
        .annotate(
            status_label=Case(
                When(finalizada=True, then=Value("Finalizada")),
                When(andamento="EXECUTANDO", then=Value("Executando")),
                When(aprovada__isnull=True, valor__isnull=True, then=Value("Levantamento")),
                When(aprovada__isnull=True, valor=0, then=Value("Levantamento")),
                When(aprovada=True, then=Value("Aprovada")),
                When(aprovada=False, then=Value("Reprovada")),
                default=Value("Pendente"),
            )
        )
    )


def _proposta_base_qs(user, cliente):
    base = Proposta.objects.select_related("cliente", "criada_por", "trabalho", "trabalho__radar")
    if cliente:
        return base.filter(Q(criada_por=user) | Q(cliente=cliente)).distinct()
    return base.filter(criada_por=user)


def _proposta_tipo_qs(user, cliente, tipo):
    base = _proposta_base_qs(user, cliente)
    if tipo == "enviadas":
        return base.filter(criada_por=user)
    if not cliente:
        return base.none()
    return base.filter(cliente=cliente).exclude(criada_por=user)


def _pendencias_total(user, cliente):
    if not cliente:
        return 0
    return (
        Proposta.objects.filter(cliente=cliente, aprovada__isnull=True, valor__gt=0, finalizada=False)
        .filter(cliente__usuario=user)
        .count()
    )


def _apply_status_filter(queryset, status_filter):
    if status_filter == "pendente":
        return queryset.filter(aprovada__isnull=True, valor__isnull=False).exclude(valor=0)
    if status_filter == "levantamento":
        return queryset.filter(aprovada__isnull=True).filter(Q(valor=0) | Q(valor__isnull=True))
    if status_filter == "aprovada":
        return queryset.filter(aprovada=True)
    if status_filter == "reprovada":
        return queryset.filter(aprovada=False)
    if status_filter == "finalizada":
        return queryset.filter(finalizada=True)
    return queryset


def _apply_search_filter(queryset, search_term):
    term = (search_term or "").strip()
    if not term:
        return queryset
    return queryset.filter(
        Q(codigo__icontains=term)
        | Q(nome__icontains=term)
        | Q(descricao__icontains=term)
        | Q(cliente__nome__icontains=term)
        | Q(cliente__email__icontains=term)
        | Q(criada_por__username__icontains=term)
        | Q(criada_por__email__icontains=term)
    )


def _group_propostas(propostas, tipo):
    agrupadas = {}
    for proposta in propostas:
        if tipo == "enviadas":
            destino = proposta.cliente.nome if proposta.cliente else ""
            if not destino:
                destino = proposta.cliente.email if proposta.cliente else "Destino"
            chave = destino
        else:
            chave = proposta.criada_por.username if proposta.criada_por else "Sistema"
        agrupadas.setdefault(chave, []).append(proposta)
    return [{"nome": key, "propostas": value} for key, value in agrupadas.items()]


def _build_proposta_sections(user, cliente, tipo, status_filter, search_term=None):
    cutoff = timezone.now() - timedelta(days=90)
    tipo_qs = _proposta_status_annotations(_proposta_tipo_qs(user, cliente, tipo))
    tipo_qs = _apply_search_filter(tipo_qs, search_term)
    tipo_qs = tipo_qs.order_by("-criado_em")
    para_aprovar = Proposta.objects.none()
    aguardando_aprovacao = Proposta.objects.none()
    executando = Proposta.objects.none()
    levantamento = Proposta.objects.none()
    if cliente:
        para_aprovar = tipo_qs.filter(
            aprovada__isnull=True,
            valor__gt=0,
            cliente__usuario=user,
            finalizada=False,
        ).order_by("-criado_em")
    aguardando_aprovacao = tipo_qs.filter(
        aprovada__isnull=True,
        valor__gt=0,
        finalizada=False,
    ).order_by("-criado_em")
    executando = tipo_qs.filter(andamento="EXECUTANDO", finalizada=False).order_by("-criado_em")
    levantamento = tipo_qs.filter(aprovada__isnull=True, finalizada=False).filter(
        Q(valor=0) | Q(valor__isnull=True)
    ).order_by("-criado_em")
    todas = _apply_status_filter(tipo_qs, status_filter)
    todas = todas.exclude(
        Q(finalizada=True) & Q(finalizada_em__lt=cutoff)
    )
    finalizadas_90 = tipo_qs.filter(finalizada=True, finalizada_em__gte=cutoff).order_by("-finalizada_em")
    finalizadas_lista = list(finalizadas_90)
    return {
        "para_aprovar": para_aprovar,
        "aguardando_aprovacao": aguardando_aprovacao,
        "executando": executando,
        "levantamento": levantamento,
        "todas": _group_propostas(todas, tipo),
        "finalizadas_90": finalizadas_lista,
    }


def _proposta_quick_stats(user, cliente, tipo):
    cutoff_30 = timezone.now() - timedelta(days=30)
    base = _proposta_tipo_qs(user, cliente, tipo)
    pendentes = base.filter(
        aprovada__isnull=True,
        valor__gt=0,
        finalizada=False,
    ).count()
    levantamento = base.filter(aprovada__isnull=True).filter(Q(valor=0) | Q(valor__isnull=True)).filter(
        finalizada=False
    ).count()
    if tipo == "enviadas":
        executando = base.filter(aprovada=True, andamento="EXECUTANDO", finalizada=False).count()
        decididas_30 = base.filter(aprovada__isnull=False, decidido_em__gte=cutoff_30).count()
        aprovadas_30 = base.filter(aprovada=True, decidido_em__gte=cutoff_30).count()
        taxa_aprovacao = int(round((aprovadas_30 / decididas_30) * 100)) if decididas_30 else 0
        return {
            "pendentes": pendentes,
            "em_execucao": levantamento,
            "total": executando,
            "aprovadas_execucao": 0,
            "finalizadas_90": f"{taxa_aprovacao}%",
        }
    aprovadas_para_execucao = base.filter(aprovada=True, finalizada=False).exclude(andamento="EXECUTANDO").count()
    aprovadas_em_execucao = base.filter(aprovada=True, andamento="EXECUTANDO", finalizada=False).count()
    concluidas_30 = base.filter(finalizada=True, finalizada_em__gte=cutoff_30).count()
    return {
        "pendentes": pendentes,
        "em_execucao": levantamento,
        "total": aprovadas_para_execucao,
        "aprovadas_execucao": aprovadas_em_execucao,
        "finalizadas_90": concluidas_30,
    }


@login_required
def proposta_list(request):
    denied_response = _require_internal_module_access(request, "PROPOSTAS")
    if denied_response:
        return denied_response
    cliente = _get_cliente(request.user)
    tipo_ativo = _normalize_proposta_tipo(request.GET.get("mode") or request.GET.get("tipo") or "recebidas")
    status_filter = None
    search_term = ""
    pendencias_total = _pendencias_total(request.user, cliente)
    sections = _build_proposta_sections(request.user, cliente, tipo_ativo, status_filter, search_term=search_term)
    status_label_map = {
        "pendente": "Pendentes",
        "levantamento": "Levantamento",
        "aprovada": "Aprovadas",
        "reprovada": "Reprovadas",
        "finalizada": "Finalizadas (90 dias)",
    }
    status_filter_label = status_label_map.get(status_filter)
    quick_stats = _proposta_quick_stats(request.user, cliente, tipo_ativo)
    return render(
        request,
        "core/proposta_list.html",
        {
            "cliente": cliente,
            "propostas_para_aprovar": sections["para_aprovar"],
            "propostas_aguardando": sections["aguardando_aprovacao"],
            "propostas_executando": sections["executando"],
            "propostas_levantamento": sections["levantamento"],
            "propostas_todas": sections["todas"],
            "propostas_finalizadas": sections["finalizadas_90"],
            "pendencias_total": pendencias_total,
            "status_filter": status_filter,
            "status_filter_label": status_filter_label,
            "tipo_ativo": tipo_ativo,
            "search_term": search_term,
            "quick_stats": quick_stats,
            "is_vendedor": True,
            "current_user_id": request.user.id,
        },
    )


@login_required
def proposta_data(request):
    denied_response = _require_internal_module_access(request, "PROPOSTAS")
    if denied_response:
        return JsonResponse({"ok": False, "error": "forbidden"}, status=403)
    if request.method != "GET":
        return HttpResponseNotAllowed(["GET"])
    cliente = _get_cliente(request.user)
    tipo_ativo = _normalize_proposta_tipo(request.GET.get("mode") or request.GET.get("tipo") or "recebidas")
    status_filter = None
    search_term = ""
    sections = _build_proposta_sections(request.user, cliente, tipo_ativo, status_filter, search_term=search_term)
    status_label_map = {
        "pendente": "Pendentes",
        "levantamento": "Levantamento",
        "aprovada": "Aprovadas",
        "reprovada": "Reprovadas",
        "finalizada": "Finalizadas (90 dias)",
    }
    html = render_to_string(
        "core/propostas/_sections.html",
        {
            "cliente": cliente,
            "propostas_para_aprovar": sections["para_aprovar"],
            "propostas_aguardando": sections["aguardando_aprovacao"],
            "propostas_executando": sections["executando"],
            "propostas_levantamento": sections["levantamento"],
            "propostas_todas": sections["todas"],
            "propostas_finalizadas": sections["finalizadas_90"],
            "status_filter": status_filter,
            "status_filter_label": status_label_map.get(status_filter),
            "tipo_ativo": tipo_ativo,
            "search_term": search_term,
            "current_user_id": request.user.id,
        },
        request=request,
    )
    summary = _proposta_quick_stats(request.user, cliente, tipo_ativo)
    return JsonResponse({"ok": True, "mode": tipo_ativo, "summary": summary, "html": html})


@login_required
def proposta_finalizadas_arquivo(request):
    denied_response = _require_internal_module_access(request, "PROPOSTAS")
    if denied_response:
        return denied_response
    cliente = _get_cliente(request.user)
    tipo_ativo = _normalize_proposta_tipo(request.GET.get("mode") or request.GET.get("tipo") or "recebidas")
    cutoff = timezone.now() - timedelta(days=90)
    base = _proposta_tipo_qs(request.user, cliente, tipo_ativo)
    finalizadas = (
        _proposta_status_annotations(base.filter(finalizada=True, finalizada_em__lt=cutoff))
        .order_by("-finalizada_em")
    )
    paginator = Paginator(finalizadas, 20)
    page_obj = paginator.get_page(request.GET.get("page"))
    return render(
        request,
        "core/proposta_finalizadas_arquivo.html",
        {
            "cliente": cliente,
            "page_obj": page_obj,
            "cutoff": cutoff,
            "tipo_ativo": tipo_ativo,
        },
    )


@login_required
def proposta_busca(request):
    denied_response = _require_internal_module_access(request, "PROPOSTAS")
    if denied_response:
        return denied_response
    cliente = _get_cliente(request.user)
    tipo_ativo = _normalize_proposta_tipo(request.GET.get("mode") or request.GET.get("tipo") or "recebidas")
    status_filter = request.GET.get("status", "").strip().lower() or None
    search_term = request.GET.get("q", "").strip()
    base = _proposta_status_annotations(_proposta_tipo_qs(request.user, cliente, tipo_ativo))
    base = _apply_search_filter(base, search_term)
    resultados = _apply_status_filter(base, status_filter).order_by("-criado_em")
    paginator = Paginator(resultados, 25)
    page_obj = paginator.get_page(request.GET.get("page"))
    return render(
        request,
        "core/proposta_busca.html",
        {
            "cliente": cliente,
            "tipo_ativo": tipo_ativo,
            "status_filter": status_filter or "",
            "search_term": search_term,
            "page_obj": page_obj,
        },
    )


@login_required
def proposta_detail(request, pk):
    denied_response = _require_internal_module_access(request, "PROPOSTAS")
    if denied_response:
        return denied_response
    cliente = _get_cliente(request.user)
    proposta_qs_base = Proposta.objects.select_related(
        "cliente",
        "criada_por",
        "aprovado_por",
        "trabalho",
        "trabalho__radar",
        "trabalho__classificacao",
        "trabalho__contrato",
    ).prefetch_related("anexos", "trabalho__atividades")
    if cliente:
        proposta_qs = proposta_qs_base.filter(Q(criada_por=request.user) | Q(cliente=cliente))
        proposta = get_object_or_404(proposta_qs, pk=pk)
    else:
        proposta = get_object_or_404(proposta_qs_base, pk=pk, criada_por=request.user)
    message = None
    if request.method == "POST":
        action = request.POST.get("action")
        if action == "update_value":
            if proposta.criada_por_id != request.user.id:
                return HttpResponseForbidden("Sem permissao.")
            if proposta.aprovada is not None:
                message = "Nao e possivel alterar o valor apos aprovacao."
            else:
                valor_raw = request.POST.get("valor", "").replace(",", ".").strip()
                valor = None
                if valor_raw:
                    try:
                        valor = Decimal(valor_raw)
                    except (InvalidOperation, ValueError):
                        valor = None
                if valor_raw and valor is None:
                    message = "Informe um valor valido."
                else:
                    proposta.valor = valor
                    proposta.save(update_fields=["valor"])
                    return redirect("proposta_detail", pk=proposta.pk)
        if action == "update_details":
            if proposta.criada_por_id != request.user.id:
                return HttpResponseForbidden("Sem permissao.")
            nome = request.POST.get("nome", "").strip()
            descricao = _sanitize_proposta_descricao(request.POST.get("descricao", "").strip())
            codigo = request.POST.get("codigo", "").strip()
            update_fields = []
            if nome:
                proposta.nome = nome
                update_fields.append("nome")
            if descricao:
                proposta.descricao = descricao
                update_fields.append("descricao")
            if codigo:
                proposta.codigo = codigo
                update_fields.append("codigo")
            if update_fields:
                proposta.save(update_fields=update_fields)
            return redirect("proposta_detail", pk=proposta.pk)
        if action == "set_finalizada":
            if proposta.criada_por_id != request.user.id:
                return HttpResponseForbidden("Sem permissao.")
            if proposta.aprovada is None:
                message = "Aguardando aprovacao. Finalizacao so e possivel apos aprovacao."
            else:
                proposta.finalizada = True
                if not proposta.finalizada_em:
                    proposta.finalizada_em = timezone.now()
                proposta.save(update_fields=["finalizada", "finalizada_em"])
                return redirect("proposta_detail", pk=proposta.pk)
        if action == "set_executando":
            if proposta.criada_por_id != request.user.id:
                return HttpResponseForbidden("Sem permissao.")
            if proposta.aprovada is not True:
                message = "Somente propostas aprovadas podem ir para Executando."
            else:
                proposta.andamento = "EXECUTANDO"
                proposta.save(update_fields=["andamento"])
                return redirect("proposta_detail", pk=proposta.pk)
        if action == "remove_aprovacao":
            if proposta.criada_por_id != request.user.id:
                return HttpResponseForbidden("Sem permissao.")
            if proposta.finalizada or proposta.andamento == "EXECUTANDO":
                message = "Nao e possivel remover a aprovacao apos iniciar a execucao."
            else:
                proposta.aprovada = None
                proposta.aprovado_por = None
                proposta.decidido_em = None
                proposta.save(update_fields=["aprovada", "aprovado_por", "decidido_em"])
                return redirect("proposta_detail", pk=proposta.pk)
        if action == "add_anexo":
            arquivo = request.FILES.get("arquivo")
            tipo = request.POST.get("tipo") or PropostaAnexo.Tipo.OUTROS
            if arquivo:
                PropostaAnexo.objects.create(proposta=proposta, arquivo=arquivo, tipo=tipo)
            return redirect("proposta_detail", pk=proposta.pk)
        if action == "delete_anexo":
            anexo_id = request.POST.get("anexo_id")
            anexo = get_object_or_404(PropostaAnexo, pk=anexo_id, proposta=proposta)
            anexo.delete()
            return redirect("proposta_detail", pk=proposta.pk)
        if action == "delete_proposta":
            if proposta.criada_por_id != request.user.id:
                return HttpResponseForbidden("Sem permissao.")
            if proposta.aprovada is not None:
                message = "Nao e possivel excluir apos aprovacao."
            else:
                proposta.delete()
                return redirect("propostas")
    trabalho_vinculado, trabalho_indisponivel = _resolve_proposta_trabalho(proposta, request.user)
    status_label = _proposta_status_label(proposta)
    return render(
        request,
        "core/proposta_detail.html",
        {
            "cliente": cliente,
            "proposta": proposta,
            "message": message,
            "status_label": status_label,
            "descricao_comercial": _sanitize_proposta_descricao(proposta.descricao),
            "trabalho_vinculado": trabalho_vinculado,
            "trabalho_indisponivel": trabalho_indisponivel,
        },
    )


@login_required
def proposta_export_pdf(request, pk):
    denied_response = _require_internal_module_access(request, "PROPOSTAS")
    if denied_response:
        return denied_response
    cliente = _get_cliente(request.user)
    select_related_fields = [
        "cliente",
        "criada_por",
        "trabalho",
        "trabalho__radar",
        "trabalho__classificacao",
        "trabalho__contrato",
    ]
    prefetch_related_fields = ["anexos", "trabalho__atividades"]
    if cliente:
        proposta_qs = Proposta.objects.select_related(*select_related_fields).prefetch_related(
            *prefetch_related_fields
        ).filter(
            Q(criada_por=request.user) | Q(cliente=cliente)
        )
        proposta = get_object_or_404(proposta_qs, pk=pk)
    else:
        proposta = get_object_or_404(
            Proposta.objects.select_related(*select_related_fields).prefetch_related(*prefetch_related_fields),
            pk=pk,
            criada_por=request.user,
        )
    status_label = _proposta_status_label(proposta)
    trabalho_vinculado, trabalho_indisponivel = _resolve_proposta_trabalho(proposta, request.user)
    pdf_buffer = _render_proposta_pdf(
        proposta,
        status_label,
        include_origem=True,
        trabalho=trabalho_vinculado,
        trabalho_indisponivel=trabalho_indisponivel,
    )
    if not pdf_buffer:
        return HttpResponse("Biblioteca de PDF indisponivel (WeasyPrint).", status=500)
    base_name = proposta.codigo or f"proposta_{proposta.id}"
    filename = re.sub(r"[^A-Za-z0-9_-]+", "_", str(base_name)).strip("_") or f"proposta_{proposta.id}"
    response = HttpResponse(pdf_buffer.getvalue(), content_type="application/pdf")
    response["Content-Disposition"] = f'attachment; filename="{filename}.pdf"'
    return response


@login_required
def proposta_nova_vendedor(request):
    denied_response = _require_internal_module_access(request, "PROPOSTAS")
    if denied_response:
        return denied_response
    message = None
    form_data = {
        "email": "",
        "nome": "",
        "descricao": "",
        "valor": "",
        "prioridade": "50",
        "codigo": "",
        "observacao": "",
        "trabalho_id": "",
    }
    source_trabalho = None
    if request.method == "POST":
        email = request.POST.get("email", "").strip().lower()
        nome = request.POST.get("nome", "").strip()
        descricao = _sanitize_proposta_descricao(request.POST.get("descricao", "").strip())
        valor_raw = request.POST.get("valor", "").replace(",", ".").strip()
        prioridade_raw = request.POST.get("prioridade", "").strip()
        codigo = request.POST.get("codigo", "").strip()
        observacao = request.POST.get("observacao", "").strip()
        trabalho_id = request.POST.get("trabalho_id", "").strip() or request.POST.get("origem_trabalho_id", "").strip()
        anexo_tipo = request.POST.get("anexo_tipo") or PropostaAnexo.Tipo.OUTROS
        anexo_arquivo = request.FILES.get("anexo_arquivo")
        form_data = {
            "email": email,
            "nome": nome,
            "descricao": descricao,
            "valor": valor_raw,
            "prioridade": prioridade_raw or "50",
            "codigo": codigo,
            "observacao": observacao,
            "trabalho_id": trabalho_id,
        }

        trabalho = None
        if trabalho_id:
            trabalho = _get_radar_trabalho_acessivel(request.user, trabalho_id)
            if not trabalho:
                message = "Origem de trabalho invalida para o seu acesso."
            elif not _is_radar_creator_user(request.user, trabalho.radar):
                message = "Somente quem criou o radar pode gerar proposta a partir do trabalho."
            else:
                source_trabalho = trabalho

        destinatario = PerfilUsuario.objects.filter(email__iexact=email).first() if email else None
        if not message and not destinatario:
            message = "Usuario nao encontrado para este email."
        elif not message:
            valor = None
            if valor_raw:
                try:
                    valor = Decimal(valor_raw)
                except (InvalidOperation, ValueError):
                    valor = None
            try:
                prioridade = int(prioridade_raw) if prioridade_raw else 50
            except ValueError:
                prioridade = 50
            prioridade = max(1, min(99, prioridade))
            if not nome or not descricao:
                message = "Preencha nome e descricao."
            elif valor_raw and valor is None:
                message = "Informe um valor valido ou deixe em branco para levantamento."
            else:
                proposta = Proposta.objects.create(
                    cliente=destinatario,
                    criada_por=request.user,
                    nome=nome,
                    descricao=descricao,
                    valor=valor,
                    prioridade=prioridade,
                    codigo=codigo,
                    observacao_cliente=observacao,
                    trabalho=trabalho,
                )
                if anexo_arquivo:
                    PropostaAnexo.objects.create(
                        proposta=proposta,
                        arquivo=anexo_arquivo,
                        tipo=anexo_tipo,
                    )
                return redirect("proposta_detail", pk=proposta.pk)

    return render(
        request,
        "core/proposta_nova.html",
        {
            "message": message,
            "form_data": form_data,
            "tipos_anexo": PropostaAnexo.Tipo.choices,
            "source_trabalho": source_trabalho,
        },
    )


@login_required
def proposta_nova_de_trabalho(request, trabalho_pk):
    denied_response = _require_internal_module_access(request, "PROPOSTAS")
    if denied_response:
        return denied_response
    trabalho = _get_radar_trabalho_acessivel(request.user, trabalho_pk)
    if not trabalho:
        return HttpResponseForbidden("Sem permissao.")
    can_manage_trabalho = _is_radar_creator_user(request.user, trabalho.radar)
    if not can_manage_trabalho:
        return HttpResponseForbidden("Somente quem criou o radar pode gerar proposta a partir do trabalho.")
    form_data = {
        "email": "",
        "nome": trabalho.nome,
        "descricao": "",
        "valor": "",
        "prioridade": "50",
        "codigo": "",
        "observacao": f"Origem: Radar {trabalho.radar.nome} / Trabalho {trabalho.nome}",
        "trabalho_id": str(trabalho.id),
    }
    return render(
        request,
        "core/proposta_nova.html",
        {
            "message": None,
            "form_data": form_data,
            "tipos_anexo": PropostaAnexo.Tipo.choices,
            "source_trabalho": trabalho,
        },
    )


@login_required
@require_POST
def aprovar_proposta(request, pk):
    denied_response = _require_internal_module_access(request, "PROPOSTAS")
    if denied_response:
        return denied_response
    cliente = _get_cliente(request.user)
    proposta = get_object_or_404(Proposta, pk=pk, cliente=cliente)
    if proposta.cliente.usuario_id != request.user.id:
        return HttpResponseForbidden("Somente o destinatario pode aprovar.")
    if proposta.valor is None or proposta.valor == 0:
        return HttpResponseForbidden("Proposta em levantamento.")
    if proposta.aprovada is None:
        proposta.aprovada = True
        proposta.decidido_em = timezone.now()
        proposta.aprovado_por = request.user
        proposta.save(update_fields=["aprovada", "decidido_em", "aprovado_por"])
    return redirect("propostas")


@login_required
@require_POST
def reprovar_proposta(request, pk):
    denied_response = _require_internal_module_access(request, "PROPOSTAS")
    if denied_response:
        return denied_response
    cliente = _get_cliente(request.user)
    proposta = get_object_or_404(Proposta, pk=pk, cliente=cliente)
    if proposta.cliente.usuario_id != request.user.id:
        return HttpResponseForbidden("Somente o destinatario pode reprovar.")
    if proposta.aprovada is None:
        proposta.aprovada = False
        proposta.decidido_em = timezone.now()
        proposta.aprovado_por = request.user
        proposta.save(update_fields=["aprovada", "decidido_em", "aprovado_por"])
    return redirect("propostas")


@login_required
@require_POST
def salvar_observacao(request, pk):
    denied_response = _require_internal_module_access(request, "PROPOSTAS")
    if denied_response:
        return denied_response
    cliente = _get_cliente(request.user)
    if cliente:
        proposta_qs = Proposta.objects.filter(Q(criada_por=request.user) | Q(cliente=cliente))
        proposta = get_object_or_404(proposta_qs, pk=pk)
    else:
        proposta = get_object_or_404(Proposta, pk=pk, criada_por=request.user)
    observacao = request.POST.get("observacao", "").strip()
    prioridade_raw = request.POST.get("prioridade", "").strip()
    proposta.observacao_cliente = observacao
    if prioridade_raw:
        try:
            prioridade = int(prioridade_raw)
        except ValueError:
            prioridade = None
        if prioridade is not None:
            prioridade = max(1, min(99, prioridade))
            proposta.prioridade = prioridade
    proposta.save(update_fields=["observacao_cliente", "prioridade"])
    return redirect("proposta_detail", pk=proposta.pk)


@login_required
def user_management(request):
    if not _is_admin_user(request.user):
        return HttpResponseForbidden("Sem permissao.")
    message = None
    form = UserCreateForm()
    tipo_form = TipoPerfilCreateForm()
    if request.method == "POST":
        if request.POST.get("create_tipo") == "1":
            tipo_form = TipoPerfilCreateForm(request.POST)
            if tipo_form.is_valid():
                tipo_form.save()
                return redirect("usuarios")
        elif request.POST.get("update_tipo") == "1":
            tipo_id = request.POST.get("tipo_id")
            novo_nome = request.POST.get("novo_nome", "").strip()
            tipo = get_object_or_404(TipoPerfil, pk=tipo_id)
            if novo_nome:
                if TipoPerfil.objects.exclude(pk=tipo.id).filter(nome__iexact=novo_nome).exists():
                    message = "Tipo ja existe."
                else:
                    tipo.nome = novo_nome
                    tipo.save(update_fields=["nome"])
                    return redirect("usuarios")
            else:
                message = "Informe um nome valido."
        elif request.POST.get("delete_tipo") == "1":
            tipo_id = request.POST.get("tipo_id")
            tipo = get_object_or_404(TipoPerfil, pk=tipo_id)
            tipo.delete()
            return redirect("usuarios")
        if request.POST.get("create_user") == "1":
            form = UserCreateForm(request.POST)
            if form.is_valid():
                user = form.save()
                tipo_ids = request.POST.getlist("tipos")
                tipos = TipoPerfil.objects.filter(id__in=tipo_ids) if tipo_ids else TipoPerfil.objects.none()
                nome = user.username.split("@")[0]
                cliente = PerfilUsuario.objects.create(
                    nome=nome,
                    email=user.username,
                    usuario=user,
                    ativo=True,
                )
                if tipos:
                    cliente.tipos.set(tipos)
                _ensure_default_cadernos(cliente)
                return redirect("usuarios")
    else:
        form = UserCreateForm()
    user_query = request.GET.get("q", "").strip()
    users_qs = User.objects.order_by("username")
    if user_query:
        users_qs = users_qs.filter(username__icontains=user_query)
    users = Paginator(users_qs, 15).get_page(request.GET.get("page"))
    return render(
        request,
        "core/usuarios.html",
        {
            "form": form,
            "users": users,
            "user_query": user_query,
            "tipos": TipoPerfil.objects.order_by("nome"),
            "tipo_form": tipo_form,
            "message": message,
        },
    )


@login_required
def usuarios_gerenciar_usuario(request, pk):
    if not _is_admin_user(request.user):
        return HttpResponseForbidden("Sem permissao.")
    user = get_object_or_404(User, pk=pk)
    perfil = _get_cliente(user)
    message = None
    if request.method == "POST":
        action = request.POST.get("action")
        if action == "update_user":
            email = request.POST.get("email", "").strip().lower()
            is_staff = request.POST.get("is_staff") == "on"
            is_active = request.POST.get("is_active") == "on"
            if not email:
                message = "Informe um email valido."
            else:
                existing = User.objects.filter(username=email).exclude(pk=user.pk)
                if existing.exists():
                    message = "Email ja cadastrado."
                else:
                    user.username = email
                    user.email = email
                    user.is_staff = is_staff
                    user.is_active = is_active
                    user.save(update_fields=["username", "email", "is_staff", "is_active"])
                    if perfil:
                        perfil.email = email
                        perfil.save(update_fields=["email"])
                    return redirect("usuarios_gerenciar_usuario", pk=user.pk)
        if action == "update_perfil":
            nome = request.POST.get("nome", "").strip()
            empresa = request.POST.get("empresa", "").strip()
            sigla_cidade = request.POST.get("sigla_cidade", "").strip()
            tipo_ids = request.POST.getlist("tipos")
            plantas_raw = request.POST.get("plantas", "")
            financeiros_raw = request.POST.get("financeiros", "")
            inventarios_raw = request.POST.get("inventarios", "")
            listas_ip_raw = request.POST.get("listas_ip", "")
            radares_raw = request.POST.get("radares", "")
            apps_raw = request.POST.get("apps", "")
            if not perfil:
                perfil = PerfilUsuario.objects.create(
                    nome=nome or user.username.split("@")[0],
                    email=user.email or user.username,
                    usuario=user,
                    ativo=True,
                    empresa=empresa,
                    sigla_cidade=sigla_cidade,
                )
                _ensure_default_cadernos(perfil)
            else:
                if nome:
                    perfil.nome = nome
                perfil.empresa = empresa
                perfil.sigla_cidade = sigla_cidade
                perfil.save(update_fields=["nome", "empresa", "sigla_cidade"])
            tipos = TipoPerfil.objects.filter(id__in=tipo_ids)
            perfil.tipos.set(tipos)
            cleaned = plantas_raw
            for sep in [";", "\n", "\r", "\t"]:
                cleaned = cleaned.replace(sep, ",")
            codes = [code.strip().upper() for code in cleaned.split(",") if code.strip()]
            plantas = [PlantaIO.objects.get_or_create(codigo=code)[0] for code in codes]
            perfil.plantas.set(plantas)
            cleaned_fin = financeiros_raw
            for sep in [";", "\n", "\r", "\t"]:
                cleaned_fin = cleaned_fin.replace(sep, ",")
            fin_codes = [code.strip().upper() for code in cleaned_fin.split(",") if code.strip()]
            financeiros = [FinanceiroID.objects.get_or_create(codigo=code)[0] for code in fin_codes]
            perfil.financeiros.set(financeiros)
            cleaned_inv = inventarios_raw
            for sep in [";", "\n", "\r", "\t"]:
                cleaned_inv = cleaned_inv.replace(sep, ",")
            inv_codes = [code.strip().upper() for code in cleaned_inv.split(",") if code.strip()]
            inventarios = [InventarioID.objects.get_or_create(codigo=code)[0] for code in inv_codes]
            perfil.inventarios.set(inventarios)
            cleaned_ip = listas_ip_raw
            for sep in [";", "\n", "\r", "\t"]:
                cleaned_ip = cleaned_ip.replace(sep, ",")
            ip_codes = [code.strip().upper() for code in cleaned_ip.split(",") if code.strip()]
            listas_ip = [ListaIPID.objects.get_or_create(codigo=code)[0] for code in ip_codes]
            perfil.listas_ip.set(listas_ip)
            cleaned_radar = radares_raw
            for sep in [";", "\n", "\r", "\t"]:
                cleaned_radar = cleaned_radar.replace(sep, ",")
            radar_codes = [code.strip().upper() for code in cleaned_radar.split(",") if code.strip()]
            radares = [RadarID.objects.get_or_create(codigo=code)[0] for code in radar_codes]
            perfil.radares.set(radares)
            cleaned_apps = apps_raw
            for sep in [";", "\n", "\r", "\t"]:
                cleaned_apps = cleaned_apps.replace(sep, ",")
            app_slugs = [_clean_app_slug(code) for code in cleaned_apps.split(",") if code.strip()]
            apps = []
            for slug in app_slugs:
                if not slug:
                    continue
                app, created = App.objects.get_or_create(slug=slug, defaults={"nome": slug})
                if created and not app.nome:
                    app.nome = slug
                    app.save(update_fields=["nome"])
                apps.append(app)
            perfil.apps.set(apps)
            now = timezone.now()
            for product in ProdutoPlataforma.objects.order_by("nome"):
                access_mode = (request.POST.get(f"produto_mode_{product.id}") or "").strip().upper()
                existing_access = AcessoProdutoUsuario.objects.filter(usuario=user, produto=product).first()
                if access_mode != "ON":
                    if existing_access:
                        existing_access.delete()
                    continue
                status = (
                    request.POST.get(f"produto_status_{product.id}") or AcessoProdutoUsuario.Status.ATIVO
                ).strip().upper()
                origem = (
                    request.POST.get(f"produto_origem_{product.id}") or AcessoProdutoUsuario.Origem.MANUAL
                ).strip().upper()
                trial_fim = _parse_local_date_boundary(request.POST.get(f"produto_trial_fim_{product.id}"), end=True)
                acesso_fim = _parse_local_date_boundary(request.POST.get(f"produto_acesso_fim_{product.id}"), end=True)
                observacao = (request.POST.get(f"produto_observacao_{product.id}") or "").strip()
                defaults = {
                    "origem": origem if origem in AcessoProdutoUsuario.Origem.values else AcessoProdutoUsuario.Origem.MANUAL,
                    "status": status if status in AcessoProdutoUsuario.Status.values else AcessoProdutoUsuario.Status.ATIVO,
                    "observacao": observacao,
                    "trial_fim": None,
                    "acesso_fim": None,
                }
                if existing_access:
                    defaults["acesso_inicio"] = existing_access.acesso_inicio or now
                    defaults["trial_inicio"] = existing_access.trial_inicio
                else:
                    defaults["acesso_inicio"] = now
                    defaults["trial_inicio"] = None
                if defaults["status"] == AcessoProdutoUsuario.Status.TRIAL_ATIVO:
                    defaults["trial_inicio"] = defaults["trial_inicio"] or now
                    if trial_fim:
                        defaults["trial_fim"] = trial_fim
                    elif existing_access and existing_access.trial_fim:
                        defaults["trial_fim"] = existing_access.trial_fim
                    else:
                        defaults["trial_fim"] = now + timedelta(days=TRIAL_DURATION_DAYS)
                    defaults["acesso_fim"] = None
                else:
                    defaults["acesso_fim"] = acesso_fim
                    if defaults["status"] != AcessoProdutoUsuario.Status.EXPIRADO:
                        defaults["trial_inicio"] = None
                        defaults["trial_fim"] = None
                AcessoProdutoUsuario.objects.update_or_create(
                    usuario=user,
                    produto=product,
                    defaults=defaults,
                )
            return redirect("usuarios_gerenciar_usuario", pk=user.pk)
        if action == "set_password":
            new_password = request.POST.get("new_password", "").strip()
            if new_password:
                user.set_password(new_password)
                user.save(update_fields=["password"])
                message = "Senha atualizada."
            else:
                message = "Informe uma senha valida."
    product_access_rows = _build_product_access_rows(user)
    return render(
        request,
        "core/usuarios_gerenciar_usuario.html",
        {
            "user_item": user,
            "perfil": perfil,
            "tipos": TipoPerfil.objects.order_by("nome"),
            "product_access_rows": product_access_rows,
            "product_access_count": sum(1 for row in product_access_rows if row["access"]),
            "product_status_choices": AcessoProdutoUsuario.Status.choices,
            "product_origin_choices": AcessoProdutoUsuario.Origem.choices,
            "message": message,
        },
    )


@login_required
def meu_perfil(request):
    user = request.user
    perfil = _get_cliente(user)
    message = None
    if request.method == "POST":
        action = request.POST.get("action")
        if action == "update_user":
            email = request.POST.get("email", "").strip().lower()
            if not email:
                message = "Informe um email valido."
            else:
                existing = User.objects.filter(username=email).exclude(pk=user.pk)
                if existing.exists():
                    message = "Email ja cadastrado."
                else:
                    user.username = email
                    user.email = email
                    user.save(update_fields=["username", "email"])
                    if perfil:
                        perfil.email = email
                        perfil.save(update_fields=["email"])
                    return redirect("meu_perfil")
        if action == "update_profile":
            nome = request.POST.get("nome", "").strip()
            empresa = request.POST.get("empresa", "").strip()
            sigla_cidade = request.POST.get("sigla_cidade", "").strip()
            plantas_raw = request.POST.get("plantas", "")
            financeiros_raw = request.POST.get("financeiros", "")
            inventarios_raw = request.POST.get("inventarios", "")
            listas_ip_raw = request.POST.get("listas_ip", "")
            radares_raw = request.POST.get("radares", "")
            if not perfil:
                perfil = PerfilUsuario.objects.create(
                    nome=nome or (user.username.split("@")[0] if user.username else "Usuario"),
                    email=user.email or user.username,
                    usuario=user,
                    ativo=True,
                    empresa=empresa,
                    sigla_cidade=sigla_cidade,
                )
                _ensure_default_cadernos(perfil)
            else:
                if nome:
                    perfil.nome = nome
                perfil.empresa = empresa
                perfil.sigla_cidade = sigla_cidade
                perfil.save(update_fields=["nome", "empresa", "sigla_cidade"])
            cleaned = plantas_raw
            for sep in [";", "\n", "\r", "\t"]:
                cleaned = cleaned.replace(sep, ",")
            codes = [code.strip().upper() for code in cleaned.split(",") if code.strip()]
            plantas = [PlantaIO.objects.get_or_create(codigo=code)[0] for code in codes]
            perfil.plantas.set(plantas)
            cleaned_fin = financeiros_raw
            for sep in [";", "\n", "\r", "\t"]:
                cleaned_fin = cleaned_fin.replace(sep, ",")
            fin_codes = [code.strip().upper() for code in cleaned_fin.split(",") if code.strip()]
            financeiros = [FinanceiroID.objects.get_or_create(codigo=code)[0] for code in fin_codes]
            perfil.financeiros.set(financeiros)
            cleaned_inv = inventarios_raw
            for sep in [";", "\n", "\r", "\t"]:
                cleaned_inv = cleaned_inv.replace(sep, ",")
            inv_codes = [code.strip().upper() for code in cleaned_inv.split(",") if code.strip()]
            inventarios = [InventarioID.objects.get_or_create(codigo=code)[0] for code in inv_codes]
            perfil.inventarios.set(inventarios)
            cleaned_ip = listas_ip_raw
            for sep in [";", "\n", "\r", "\t"]:
                cleaned_ip = cleaned_ip.replace(sep, ",")
            ip_codes = [code.strip().upper() for code in cleaned_ip.split(",") if code.strip()]
            listas_ip = [ListaIPID.objects.get_or_create(codigo=code)[0] for code in ip_codes]
            perfil.listas_ip.set(listas_ip)
            cleaned_radar = radares_raw
            for sep in [";", "\n", "\r", "\t"]:
                cleaned_radar = cleaned_radar.replace(sep, ",")
            radar_codes = [code.strip().upper() for code in cleaned_radar.split(",") if code.strip()]
            radares = [RadarID.objects.get_or_create(codigo=code)[0] for code in radar_codes]
            perfil.radares.set(radares)
            return redirect("meu_perfil")
        if action == "set_password":
            new_password = request.POST.get("new_password", "").strip()
            if new_password:
                user.set_password(new_password)
                user.save(update_fields=["password"])
                message = "Senha atualizada."
            else:
                message = "Informe uma senha valida."
    return render(
        request,
        "core/meu_perfil.html",
        {
            "perfil": perfil,
            "message": message,
        },
    )


@login_required
def financeiro_overview(request):
    denied_response = _require_internal_module_access(request, "FINANCEIRO")
    if denied_response:
        return denied_response
    cliente = _get_cliente(request.user)
    if not cliente and not _is_admin_user(request.user):
        return HttpResponseForbidden("Sem cadastro de cliente.")
    cadernos = _financeiro_allowed_cadernos_qs(request.user, cliente)
    total_expr = ExpressionWrapper(
        F("compras__itens__valor") * F("compras__itens__quantidade"),
        output_field=DecimalField(max_digits=12, decimal_places=2),
    )
    today = timezone.localdate()
    start_date = date(today.year, today.month, 1)
    if today.month == 12:
        end_date = date(today.year + 1, 1, 1)
    else:
        end_date = date(today.year, today.month + 1, 1)
    item_expr = ExpressionWrapper(
        F("itens__valor") * F("itens__quantidade"),
        output_field=DecimalField(max_digits=12, decimal_places=2),
    )
    cadernos = cadernos.annotate(
        total_mes=Sum(
            total_expr,
            filter=Q(compras__data__gte=start_date, compras__data__lt=end_date),
        )
    ).order_by("nome")
    compras_qs = _financeiro_allowed_compras_qs(request.user, cliente)
    total_geral = compras_qs.aggregate(total=Sum(item_expr)).get("total")
    ultimas_compras = compras_qs.prefetch_related("itens").order_by("-data")[:6]

    caderno_id = request.GET.get("caderno_id")
    compras = Compra.objects.none()
    if caderno_id:
        compras = _financeiro_allowed_compras_qs(request.user, cliente).filter(caderno_id=caderno_id).order_by("-data")
        compras = compras.prefetch_related("itens")

    return render(
        request,
        "core/financeiro_overview.html",
        {
            "cliente": cliente,
            "cadernos": cadernos,
            "total_geral": total_geral or 0,
            "compras": compras,
            "caderno_id": caderno_id,
            "ultimas_compras": ultimas_compras,
        },
    )


@login_required
def financeiro_nova(request):
    denied_response = _require_internal_module_access(request, "FINANCEIRO")
    if denied_response:
        return denied_response
    cliente = _get_cliente(request.user)
    if not cliente and not _is_admin_user(request.user):
        return HttpResponseForbidden("Sem cadastro de cliente.")

    from_compra_id = request.GET.get("from_compra")
    message = request.GET.get("msg", "").strip()
    message_level = request.GET.get("level", "").strip() or "info"
    open_cadastro = request.GET.get("cadastro", "").strip()
    if request.method == "POST":
        action = request.POST.get("action")
        if action == "create_categoria":
            nome = request.POST.get("categoria_nome", "").strip()
            next_caderno_id = request.POST.get("next_caderno_id", "").strip()
            if not nome:
                msg = "Informe um nome de categoria."
                level = "error"
                created = False
            else:
                categoria, created = CategoriaCompra.objects.get_or_create(nome=nome)
                if created:
                    msg = "Categoria criada."
                    level = "success"
                else:
                    msg = "Categoria ja existe."
                    level = "warning"
            if request.headers.get("x-requested-with") == "XMLHttpRequest":
                return JsonResponse(
                    {
                        "ok": bool(nome),
                        "created": created,
                        "id": categoria.id if nome and "categoria" in locals() else None,
                        "nome": categoria.nome if nome and "categoria" in locals() else None,
                        "message": msg,
                        "level": level,
                    }
                )
            params = {"cadastro": "categoria", "msg": msg, "level": level}
            if next_caderno_id:
                params["caderno_id"] = next_caderno_id
            return redirect(f"{reverse('financeiro_nova')}?{urlencode(params)}")
        if action == "create_centro":
            nome = request.POST.get("centro_nome", "").strip()
            next_caderno_id = request.POST.get("next_caderno_id", "").strip()
            if not nome:
                msg = "Informe um nome de centro de custo."
                level = "error"
                created = False
            else:
                centro, created = CentroCusto.objects.get_or_create(nome=nome)
                if created:
                    msg = "Centro de custo criado."
                    level = "success"
                else:
                    msg = "Centro de custo ja existe."
                    level = "warning"
            if request.headers.get("x-requested-with") == "XMLHttpRequest":
                return JsonResponse(
                    {
                        "ok": bool(nome),
                        "created": created,
                        "id": centro.id if nome and "centro" in locals() else None,
                        "nome": centro.nome if nome and "centro" in locals() else None,
                        "message": msg,
                        "level": level,
                    }
                )
            params = {"cadastro": "centro", "msg": msg, "level": level}
            if next_caderno_id:
                params["caderno_id"] = next_caderno_id
            return redirect(f"{reverse('financeiro_nova')}?{urlencode(params)}")
        if action == "create_compra":
            caderno_id = request.POST.get("caderno")
            nome = request.POST.get("nome", "").strip()
            descricao = request.POST.get("descricao", "").strip()
            data_raw = request.POST.get("data", "").strip()
            categoria_id = request.POST.get("categoria")
            centro_id = request.POST.get("centro_custo")
            itens_payload = []
            total_items_raw = request.POST.get("total_items", "1").strip()
            try:
                total_items = int(total_items_raw)
            except ValueError:
                total_items = 1
            total_items = max(1, min(total_items, 200))
            for idx in range(total_items):
                item_nome = request.POST.get(f"item_nome_{idx}", "").strip()
                item_valor = request.POST.get(f"item_valor_{idx}", "").replace(",", ".").strip()
                item_quantidade = request.POST.get(f"item_quantidade_{idx}", "").strip()
                item_parcela = request.POST.get(f"item_parcela_{idx}", "").strip()
                item_tipo = request.POST.get(f"item_tipo_{idx}")
                item_pago = request.POST.get(f"item_pago_{idx}") == "on"
                if item_nome:
                    if item_parcela and not _is_parcela_valid(item_parcela):
                        msg = "Parcela invalida. Use 01/36 ou 1/-."
                        params = {"msg": msg, "level": "error"}
                        if caderno_id:
                            params["caderno_id"] = caderno_id
                        if from_compra_id:
                            params["from_compra"] = from_compra_id
                        return redirect(f"{reverse('financeiro_nova')}?{urlencode(params)}")
                    itens_payload.append(
                        {
                            "nome": item_nome,
                            "valor": item_valor,
                            "quantidade": item_quantidade,
                            "parcela": item_parcela,
                            "tipo_id": item_tipo,
                            "pago": item_pago,
                        }
                    )
            try:
                data = datetime.strptime(data_raw, "%Y-%m-%d").date()
            except ValueError:
                data = None
            allowed_cadernos = _financeiro_allowed_cadernos_qs(request.user, cliente)
            has_any = any(
                [
                    caderno_id,
                    nome,
                    descricao,
                    data is not None,
                    categoria_id,
                    centro_id,
                    itens_payload,
                ]
            )
            if has_any:
                if caderno_id and not allowed_cadernos.filter(id=caderno_id).exists():
                    return redirect("financeiro")
                compra = Compra.objects.create(
                    caderno_id=caderno_id or None,
                    nome=nome,
                    descricao=descricao,
                    data=data,
                    categoria_id=categoria_id or None,
                    centro_custo_id=centro_id or None,
                )
                for item in itens_payload:
                    valor_raw = item["valor"]
                    quantidade_raw = item["quantidade"]
                    try:
                        valor = Decimal(valor_raw) if valor_raw else None
                    except (InvalidOperation, ValueError):
                        valor = None
                    try:
                        quantidade = int(quantidade_raw) if quantidade_raw else 1
                    except ValueError:
                        quantidade = 1
                    quantidade = max(1, quantidade)
                    parcela = _normalize_parcela(item["parcela"], "1/1")
                    CompraItem.objects.create(
                        compra=compra,
                        nome=item["nome"],
                        valor=valor,
                        quantidade=quantidade,
                        parcela=parcela,
                        tipo_id=item["tipo_id"] or None,
                        pago=item["pago"],
                    )
                return redirect("financeiro_compra_detail", pk=compra.pk)
            return redirect("financeiro")

    cadernos = _financeiro_allowed_cadernos_qs(request.user, cliente)
    categorias = CategoriaCompra.objects.order_by("nome")
    centros = CentroCusto.objects.order_by("nome")
    tipos = TipoCompra.objects.order_by("nome")
    selected_caderno_id = request.GET.get("caderno_id") or ""
    initial = {
        "nome": "",
        "descricao": "",
        "data": "",
        "categoria_id": "",
        "centro_id": "",
        "itens": [],
    }
    if from_compra_id:
        compra_qs = Compra.objects.filter(pk=from_compra_id)
        if not _is_admin_user(request.user):
            compra_qs = compra_qs.filter(
                Q(caderno__criador=cliente) | Q(caderno__id_financeiro__in=cliente.financeiros.all())
            )
        compra_ref = (
            compra_qs.select_related("categoria", "centro_custo", "caderno")
            .prefetch_related("itens")
            .first()
        )
        if compra_ref:
            selected_caderno_id = str(compra_ref.caderno_id or selected_caderno_id)
            base_nome = compra_ref.nome or compra_ref.descricao or "Compra"
            initial["nome"] = f"COPIA {base_nome}".strip()
            initial["descricao"] = compra_ref.descricao or ""
            initial["data"] = compra_ref.data.strftime("%Y-%m-%d") if compra_ref.data else ""
            initial["categoria_id"] = str(compra_ref.categoria_id or "")
            initial["centro_id"] = str(compra_ref.centro_custo_id or "")
            initial["itens"] = list(compra_ref.itens.all())

    return render(
        request,
        "core/financeiro_nova.html",
        {
            "cliente": cliente,
            "cadernos": cadernos,
            "categorias": categorias,
            "centros": centros,
            "tipos": tipos,
            "selected_caderno_id": str(selected_caderno_id),
            "initial": initial,
            "message": message,
            "message_level": message_level,
            "open_cadastro": open_cadastro,
        },
    )


@login_required
def financeiro_cadernos(request):
    denied_response = _require_internal_module_access(request, "FINANCEIRO")
    if denied_response:
        return denied_response
    cliente = _get_cliente(request.user)
    if not cliente and not _is_admin_user(request.user):
        return HttpResponseForbidden("Sem cadastro de cliente.")

    message = request.GET.get("msg", "").strip()
    message_level = request.GET.get("level", "").strip() or "info"
    open_cadastro = request.GET.get("cadastro", "").strip()
    if request.method == "POST":
        action = request.POST.get("action")
        if action == "create_caderno":
            nome = request.POST.get("nome", "").strip()
            id_financeiro_raw = request.POST.get("id_financeiro", "").strip()
            if nome:
                financeiro = None
                if id_financeiro_raw:
                    financeiro, _ = FinanceiroID.objects.get_or_create(codigo=id_financeiro_raw.upper())
                Caderno.objects.create(nome=nome, ativo=True, id_financeiro=financeiro, criador=cliente)
            return redirect("financeiro_cadernos")
        if action == "toggle_caderno":
            caderno_id = request.POST.get("caderno_id")
            caderno = get_object_or_404(
                _financeiro_allowed_cadernos_qs(request.user, cliente),
                pk=caderno_id,
            )
            caderno.ativo = not caderno.ativo
            caderno.save(update_fields=["ativo"])
            return redirect("financeiro_cadernos")
        if action == "delete_caderno":
            caderno_id = request.POST.get("caderno_id")
            caderno = get_object_or_404(
                _financeiro_allowed_cadernos_qs(request.user, cliente),
                pk=caderno_id,
            )
            caderno.delete()
            return redirect("financeiro_cadernos")
        if action == "create_categoria":
            nome = request.POST.get("categoria_nome", "").strip()
            if not nome:
                msg = "Informe um nome de categoria."
                level = "error"
                created = False
            else:
                categoria, created = CategoriaCompra.objects.get_or_create(nome=nome)
                if created:
                    msg = "Categoria criada."
                    level = "success"
                else:
                    msg = "Categoria ja existe."
                    level = "warning"
            if request.headers.get("x-requested-with") == "XMLHttpRequest":
                return JsonResponse(
                    {
                        "ok": bool(nome),
                        "created": created,
                        "id": categoria.id if nome and "categoria" in locals() else None,
                        "nome": categoria.nome if nome and "categoria" in locals() else None,
                        "message": msg,
                        "level": level,
                    }
                )
            params = {"cadastro": "categoria", "msg": msg, "level": level}
            return redirect(f"{reverse('financeiro_cadernos')}?{urlencode(params)}")
        if action == "create_centro":
            nome = request.POST.get("centro_nome", "").strip()
            if not nome:
                msg = "Informe um nome de centro de custo."
                level = "error"
                created = False
            else:
                centro, created = CentroCusto.objects.get_or_create(nome=nome)
                if created:
                    msg = "Centro de custo criado."
                    level = "success"
                else:
                    msg = "Centro de custo ja existe."
                    level = "warning"
            if request.headers.get("x-requested-with") == "XMLHttpRequest":
                return JsonResponse(
                    {
                        "ok": bool(nome),
                        "created": created,
                        "id": centro.id if nome and "centro" in locals() else None,
                        "nome": centro.nome if nome and "centro" in locals() else None,
                        "message": msg,
                        "level": level,
                    }
                )
            params = {"cadastro": "centro", "msg": msg, "level": level}
            return redirect(f"{reverse('financeiro_cadernos')}?{urlencode(params)}")

    total_expr = ExpressionWrapper(
        F("compras__itens__valor") * F("compras__itens__quantidade"),
        output_field=DecimalField(max_digits=12, decimal_places=2),
    )
    cadernos = (
        _financeiro_allowed_cadernos_qs(request.user, cliente)
        .annotate(total=Sum(total_expr))
        .order_by("nome")
    )
    return render(
        request,
        "core/financeiro_cadernos.html",
        {
            "cadernos": cadernos,
            "message": message,
            "message_level": message_level,
            "open_cadastro": open_cadastro,
        },
    )


@login_required
def financeiro_caderno_detail(request, pk):
    denied_response = _require_internal_module_access(request, "FINANCEIRO")
    if denied_response:
        return denied_response
    cliente = _get_cliente(request.user)
    if not cliente and not _is_admin_user(request.user):
        return HttpResponseForbidden("Sem cadastro de cliente.")
    caderno = get_object_or_404(_financeiro_allowed_cadernos_qs(request.user, cliente), pk=pk)
    today = timezone.localdate()
    selected_month = request.GET.get("mes", "").strip()
    if selected_month:
        try:
            selected_dt = datetime.strptime(selected_month, "%Y-%m")
        except ValueError:
            selected_dt = datetime(today.year, today.month, 1)
            selected_month = selected_dt.strftime("%Y-%m")
    else:
        selected_dt = datetime(today.year, today.month, 1)
        selected_month = selected_dt.strftime("%Y-%m")
    if selected_dt.month == 1:
        prev_dt = datetime(selected_dt.year - 1, 12, 1)
    else:
        prev_dt = datetime(selected_dt.year, selected_dt.month - 1, 1)
    if selected_dt.month == 12:
        next_dt = datetime(selected_dt.year + 1, 1, 1)
    else:
        next_dt = datetime(selected_dt.year, selected_dt.month + 1, 1)
    prev_month = prev_dt.strftime("%Y-%m")
    next_month = next_dt.strftime("%Y-%m")
    current_month = today.strftime("%Y-%m")
    start_date = date(selected_dt.year, selected_dt.month, 1)
    if selected_dt.month == 12:
        end_date = date(selected_dt.year + 1, 1, 1)
    else:
        end_date = date(selected_dt.year, selected_dt.month + 1, 1)
    zero_money = Decimal("0.00")

    def build_compra_row(compra_obj):
        itens = list(compra_obj.itens.all())
        status_label = _compra_status_label(compra_obj)
        status_key = status_label.lower()
        total_itens = sum(
            ((item.valor or zero_money) * (item.quantidade or 0) for item in itens),
            zero_money,
        )
        total_pago = sum(
            ((item.valor or zero_money) * (item.quantidade or 0) for item in itens if item.pago),
            zero_money,
        )
        total_pendente = total_itens - total_pago
        compra_obj.status_label = status_label
        compra_obj.total_itens = total_itens
        compra_obj.total_pago = total_pago
        compra_obj.total_pendente = total_pendente
        compra_obj.itens_count = len(itens)
        return {
            "id": compra_obj.id,
            "nome": (compra_obj.nome or "").strip(),
            "descricao": (compra_obj.descricao or "").strip(),
            "status": status_key,
            "status_label": status_label,
            "data": compra_obj.data.isoformat() if compra_obj.data else "",
            "data_label": compra_obj.data.strftime("%d/%m/%Y") if compra_obj.data else "-",
            "itens_count": compra_obj.itens_count,
            "total_itens": str(total_itens.quantize(Decimal("0.01"))),
            "total_pago": str(total_pago.quantize(Decimal("0.01"))),
            "total_pendente": str(total_pendente.quantize(Decimal("0.01"))),
            "categoria": compra_obj.categoria.nome if compra_obj.categoria else "",
            "centro": compra_obj.centro_custo.nome if compra_obj.centro_custo else "",
            "detalhe_url": reverse("financeiro_compra_detail", args=[compra_obj.pk]),
        }

    def build_month_summary_payload():
        month_compras = (
            Compra.objects.filter(caderno=caderno, data__gte=start_date, data__lt=end_date)
            .prefetch_related("itens")
            .order_by("id")
        )
        summary_total_mes = Decimal("0.00")
        summary_total_pago = Decimal("0.00")
        summary_total_pendente = Decimal("0.00")
        summary_total_compras = 0
        for compra_mes in month_compras:
            row = build_compra_row(compra_mes)
            summary_total_mes += Decimal(row["total_itens"])
            summary_total_pago += Decimal(row["total_pago"])
            summary_total_pendente += Decimal(row["total_pendente"])
            summary_total_compras += 1
        return {
            "total_mes": str(summary_total_mes.quantize(Decimal("0.01"))),
            "total_pago": str(summary_total_pago.quantize(Decimal("0.01"))),
            "total_pendente": str(summary_total_pendente.quantize(Decimal("0.01"))),
            "total_compras": summary_total_compras,
        }

    def build_month_snapshot():
        month_compras = (
            Compra.objects.filter(caderno=caderno, data__gte=start_date, data__lt=end_date)
            .select_related("categoria", "centro_custo")
            .prefetch_related("itens")
            .order_by("-data", "-id")
        )
        month_rows = []
        total_mes = Decimal("0.00")
        total_pago = Decimal("0.00")
        total_pendente = Decimal("0.00")
        total_compras = 0
        total_pagas = 0
        total_pendentes = 0

        for compra_mes in month_compras:
            row = build_compra_row(compra_mes)
            month_rows.append(row)
            total_mes += Decimal(row["total_itens"])
            total_pago += Decimal(row["total_pago"])
            total_pendente += Decimal(row["total_pendente"])
            total_compras += 1
            if row["status"] == "pago":
                total_pagas += 1
            else:
                total_pendentes += 1

        ticket_medio = total_mes / total_compras if total_compras else Decimal("0.00")
        return {
            "rows": month_rows,
            "summary": {
                "total_mes": total_mes,
                "total_pago": total_pago,
                "total_pendente": total_pendente,
                "total_compras": total_compras,
                "total_pagas": total_pagas,
                "total_pendentes": total_pendentes,
                "ticket_medio": ticket_medio,
            },
        }

    def build_month_navigation_payload():
        return {
            "selected_month": selected_month,
            "prev_month": prev_month,
            "next_month": next_month,
            "current_month": current_month,
            "quick_create_date": today.strftime("%Y-%m-%d") if selected_month == current_month else start_date.strftime("%Y-%m-%d"),
        }

    if request.method == "POST" and request.POST.get("action") == "create_quick_compra":
        nome = request.POST.get("nome", "").strip()
        descricao = request.POST.get("descricao", "").strip()
        data_raw = request.POST.get("data", "").strip()
        categoria_id = request.POST.get("categoria", "").strip()
        centro_id = request.POST.get("centro_custo", "").strip()
        item_nome = request.POST.get("item_nome", "").strip()
        item_valor_raw = request.POST.get("item_valor", "").replace(",", ".").strip()
        reference_month = request.POST.get("selected_month", "").strip() or selected_month

        if not nome:
            return JsonResponse(
                {
                    "ok": False,
                    "message": "Informe o nome da compra.",
                    "level": "error",
                },
                status=400,
            )

        if data_raw:
            try:
                data_compra = datetime.strptime(data_raw, "%Y-%m-%d").date()
            except ValueError:
                return JsonResponse(
                    {
                        "ok": False,
                        "message": "Data invalida.",
                        "level": "error",
                    },
                    status=400,
                )
        else:
            data_compra = None

        if item_valor_raw and not item_nome:
            return JsonResponse(
                {
                    "ok": False,
                    "message": "Informe o nome do item.",
                    "level": "error",
                },
                status=400,
            )

        try:
            item_valor = Decimal(item_valor_raw) if item_valor_raw else None
        except (InvalidOperation, ValueError):
            return JsonResponse(
                {
                    "ok": False,
                    "message": "Valor do item invalido.",
                    "level": "error",
                },
                status=400,
            )

        compra = Compra.objects.create(
            caderno=caderno,
            nome=nome,
            descricao=descricao,
            data=data_compra,
            categoria_id=categoria_id or None,
            centro_custo_id=centro_id or None,
        )
        if item_nome:
            CompraItem.objects.create(
                compra=compra,
                nome=item_nome,
                valor=item_valor,
                quantidade=1,
                parcela="1/1",
                pago=False,
            )
        compra = (
            Compra.objects.filter(pk=compra.pk)
            .select_related("categoria", "centro_custo")
            .prefetch_related("itens")
            .get()
        )
        row_payload = build_compra_row(compra)
        in_selected_month = bool(data_compra and start_date <= data_compra < end_date)
        month_summary = build_month_summary_payload() if in_selected_month else None

        if request.headers.get("x-requested-with") == "XMLHttpRequest":
            message = "Compra criada."
            if item_nome:
                message = "Compra criada com item inicial."
            if reference_month and not in_selected_month:
                message = "Compra criada fora do mes selecionado."
            return JsonResponse(
                {
                    "ok": True,
                    "row": row_payload if in_selected_month else None,
                    "message": message,
                    "level": "success",
                    "in_selected_month": in_selected_month,
                    "summary": month_summary,
                }
            )
        return redirect("financeiro_compra_detail", pk=compra.pk)

    if request.headers.get("x-requested-with") == "XMLHttpRequest":
        month_snapshot = build_month_snapshot()
        navigation_payload = build_month_navigation_payload()
        return JsonResponse(
            {
                "ok": True,
                "rows": month_snapshot["rows"],
                "summary": {
                    "total_mes": str(month_snapshot["summary"]["total_mes"].quantize(Decimal("0.01"))),
                    "total_pago": str(month_snapshot["summary"]["total_pago"].quantize(Decimal("0.01"))),
                    "total_pendente": str(month_snapshot["summary"]["total_pendente"].quantize(Decimal("0.01"))),
                    "total_compras": month_snapshot["summary"]["total_compras"],
                },
                **navigation_payload,
            }
        )

    compras_base_qs = (
        Compra.objects.filter(caderno=caderno)
        .select_related("categoria", "centro_custo")
        .prefetch_related("itens")
        .order_by("-data", "-id")
    )
    compras_sem_data_qs = compras_base_qs.filter(data__isnull=True)

    month_snapshot = build_month_snapshot()
    compras_table_data = month_snapshot["rows"]
    resumo = month_snapshot["summary"]
    compras_sem_data = []

    for compra in compras_sem_data_qs:
        itens = list(compra.itens.all())
        compra.status_label = _compra_status_label(compra)
        compra.total_itens = sum(
            ((item.valor or zero_money) * (item.quantidade or 0) for item in itens),
            zero_money,
        )
        compra.total_pago = sum(
            ((item.valor or zero_money) * (item.quantidade or 0) for item in itens if item.pago),
            zero_money,
        )
        compra.total_pendente = compra.total_itens - compra.total_pago
        compra.itens_count = len(itens)
        compras_sem_data.append(compra)
    return render(
        request,
        "core/financeiro_caderno_detail.html",
        {
            "caderno": caderno,
            "compras_table_data": compras_table_data,
            "compras_sem_data": compras_sem_data,
            "selected_month": selected_month,
            "mes_referencia": start_date,
            "prev_month": prev_month,
            "next_month": next_month,
            "current_month": current_month,
            "quick_create_date": build_month_navigation_payload()["quick_create_date"],
            "categorias": CategoriaCompra.objects.order_by("nome"),
            "centros": CentroCusto.objects.order_by("nome"),
            "resumo": resumo,
        },
    )


@login_required
def financeiro_compra_detail(request, pk):
    denied_response = _require_internal_module_access(request, "FINANCEIRO")
    if denied_response:
        return denied_response
    cliente = _get_cliente(request.user)
    if not cliente and not _is_admin_user(request.user):
        return HttpResponseForbidden("Sem cadastro de cliente.")
    compra = get_object_or_404(_financeiro_allowed_compras_qs(request.user, cliente), pk=pk)
    zero_money = Decimal("0.00")

    def serialize_item_payload(item):
        total_valor = (item.valor or zero_money) * (item.quantidade or 0)
        return {
            "id": item.id,
            "nome": item.nome,
            "quantidade": item.quantidade,
            "valor": str((item.valor or zero_money).quantize(Decimal("0.01"))),
            "parcela": item.parcela or "1/1",
            "total": str(total_valor.quantize(Decimal("0.01"))),
            "tipo": item.tipo.nome if item.tipo else "",
            "pago_status": "pago" if item.pago else "pendente",
            "pago_label": "Pago" if item.pago else "Pendente",
        }

    def serialize_compra_summary(compra_obj):
        itens_compra = list(compra_obj.itens.all())
        total_itens = sum(
            ((item.valor or zero_money) * (item.quantidade or 0) for item in itens_compra),
            zero_money,
        )
        total_pago = sum(
            ((item.valor or zero_money) * (item.quantidade or 0) for item in itens_compra if item.pago),
            zero_money,
        )
        total_pendente = total_itens - total_pago
        status_label = _compra_status_label(compra_obj)
        return {
            "id": compra_obj.id,
            "status": status_label.lower(),
            "status_label": status_label,
            "itens_count": len(itens_compra),
            "total_itens": str(total_itens.quantize(Decimal("0.01"))),
            "total_pago": str(total_pago.quantize(Decimal("0.01"))),
            "total_pendente": str(total_pendente.quantize(Decimal("0.01"))),
        }

    message = request.GET.get("msg", "").strip()
    message_level = request.GET.get("level", "").strip() or "info"
    open_cadastro = request.GET.get("cadastro", "").strip()
    if request.method == "POST":
        action = request.POST.get("action")
        if action == "create_categoria":
            nome = request.POST.get("categoria_nome", "").strip()
            if not nome:
                msg = "Informe um nome de categoria."
                level = "error"
                created = False
            else:
                categoria, created = CategoriaCompra.objects.get_or_create(nome=nome)
                if created:
                    msg = "Categoria criada."
                    level = "success"
                else:
                    msg = "Categoria ja existe."
                    level = "warning"
            if request.headers.get("x-requested-with") == "XMLHttpRequest":
                return JsonResponse(
                    {
                        "ok": bool(nome),
                        "created": created,
                        "id": categoria.id if nome and "categoria" in locals() else None,
                        "nome": categoria.nome if nome and "categoria" in locals() else None,
                        "message": msg,
                        "level": level,
                    }
                )
            params = {"cadastro": "categoria", "msg": msg, "level": level}
            return redirect(f"{reverse('financeiro_compra_detail', kwargs={'pk': compra.pk})}?{urlencode(params)}")
        if action == "create_centro":
            nome = request.POST.get("centro_nome", "").strip()
            if not nome:
                msg = "Informe um nome de centro de custo."
                level = "error"
                created = False
            else:
                centro, created = CentroCusto.objects.get_or_create(nome=nome)
                if created:
                    msg = "Centro de custo criado."
                    level = "success"
                else:
                    msg = "Centro de custo ja existe."
                    level = "warning"
            if request.headers.get("x-requested-with") == "XMLHttpRequest":
                return JsonResponse(
                    {
                        "ok": bool(nome),
                        "created": created,
                        "id": centro.id if nome and "centro" in locals() else None,
                        "nome": centro.nome if nome and "centro" in locals() else None,
                        "message": msg,
                        "level": level,
                    }
                )
            params = {"cadastro": "centro", "msg": msg, "level": level}
            return redirect(f"{reverse('financeiro_compra_detail', kwargs={'pk': compra.pk})}?{urlencode(params)}")
        if action == "delete_compra":
            caderno_id = compra.caderno_id
            compra.delete()
            if caderno_id:
                return redirect("financeiro_caderno_detail", pk=caderno_id)
            return redirect("financeiro")
        if action == "update_compra":
            nome = request.POST.get("nome", "").strip()
            descricao = request.POST.get("descricao", "").strip()
            categoria_id = request.POST.get("categoria")
            centro_id = request.POST.get("centro_custo")
            caderno_id = request.POST.get("caderno")
            data_raw = request.POST.get("data", "").strip()
            allowed_cadernos = _financeiro_allowed_cadernos_qs(request.user, cliente)
            if caderno_id and not allowed_cadernos.filter(id=caderno_id).exists():
                return redirect("financeiro_compra_detail", pk=compra.pk)
            if data_raw:
                try:
                    data = datetime.strptime(data_raw, "%Y-%m-%d").date()
                except ValueError:
                    data = None
            else:
                data = None
            compra.nome = nome
            compra.descricao = descricao
            compra.categoria_id = categoria_id or None
            compra.centro_custo_id = centro_id or None
            compra.data = data
            if caderno_id:
                compra.caderno_id = caderno_id
            compra.save(update_fields=["nome", "descricao", "categoria", "centro_custo", "caderno", "data"])
            return redirect("financeiro_compra_detail", pk=compra.pk)
        if action == "copy_next_months":
            meses_raw = request.POST.get("meses", "").strip()
            try:
                meses = int(meses_raw)
            except (TypeError, ValueError):
                meses = 0
            meses = max(0, min(24, meses))
            if meses <= 0:
                msg = "Informe a quantidade de meses para copiar."
                params = {"msg": msg, "level": "error"}
                return redirect(
                    f"{reverse('financeiro_compra_detail', kwargs={'pk': compra.pk})}?{urlencode(params)}"
                )
            if not compra.data:
                msg = "Defina uma data para copiar para os proximos meses."
                params = {"msg": msg, "level": "error"}
                return redirect(
                    f"{reverse('financeiro_compra_detail', kwargs={'pk': compra.pk})}?{urlencode(params)}"
                )
            itens_origem = list(compra.itens.all())
            copied_months = 0
            skipped_months = 0
            for offset in range(1, meses + 1):
                target_date = _add_months(compra.data, offset)
                itens_payload = []
                for item in itens_origem:
                    parcela = _parcela_for_copy(item.parcela, offset)
                    if not parcela:
                        continue
                    itens_payload.append(
                        {
                            "nome": item.nome,
                            "valor": item.valor,
                            "quantidade": item.quantidade,
                            "tipo_id": item.tipo_id,
                            "parcela": parcela,
                        }
                    )
                if not itens_payload:
                    skipped_months += 1
                    continue
                existing = Compra.objects.filter(
                    caderno_id=compra.caderno_id,
                    nome=compra.nome,
                    categoria_id=compra.categoria_id,
                    centro_custo_id=compra.centro_custo_id,
                    valor=compra.valor,
                    data=target_date,
                ).first()
                if existing:
                    existing.descricao = compra.descricao
                    existing.valor = compra.valor
                    existing.caderno_id = compra.caderno_id
                    existing.categoria_id = compra.categoria_id
                    existing.centro_custo_id = compra.centro_custo_id
                    existing.data = target_date
                    existing.save(
                        update_fields=[
                            "descricao",
                            "valor",
                            "caderno",
                            "categoria",
                            "centro_custo",
                            "data",
                        ]
                    )
                    existing.itens.all().delete()
                    alvo = existing
                else:
                    alvo = Compra.objects.create(
                        caderno_id=compra.caderno_id,
                        nome=compra.nome,
                        descricao=compra.descricao,
                        valor=compra.valor,
                        data=target_date,
                        categoria_id=compra.categoria_id,
                        centro_custo_id=compra.centro_custo_id,
                    )
                itens_novos = [
                    CompraItem(
                        compra=alvo,
                        nome=payload["nome"],
                        valor=payload["valor"],
                        quantidade=payload["quantidade"],
                        tipo_id=payload["tipo_id"],
                        parcela=payload["parcela"],
                        pago=False,
                    )
                    for payload in itens_payload
                ]
                CompraItem.objects.bulk_create(itens_novos)
                copied_months += 1
            if copied_months == 0:
                msg = "Nenhuma compra copiada: itens com parcela 1/1 ou parcelas finalizadas."
                params = {"msg": msg, "level": "warning"}
            elif skipped_months:
                msg = f"Compras copiadas. {skipped_months} mes(es) sem itens para copiar."
                params = {"msg": msg, "level": "warning"}
            else:
                msg = "Compra copiada para os proximos meses."
                params = {"msg": msg, "level": "success"}
            return redirect(
                f"{reverse('financeiro_compra_detail', kwargs={'pk': compra.pk})}?{urlencode(params)}"
            )
        if action == "add_item":
            nome = request.POST.get("nome", "").strip()
            valor_raw = request.POST.get("valor", "").replace(",", ".").strip()
            quantidade_raw = request.POST.get("quantidade", "").strip()
            parcela_raw = request.POST.get("parcela", "")
            tipo_id = request.POST.get("tipo")
            pago = request.POST.get("pago") == "on"
            try:
                valor = Decimal(valor_raw)
            except (InvalidOperation, ValueError):
                valor = None
            try:
                quantidade = int(quantidade_raw) if quantidade_raw else 1
            except ValueError:
                quantidade = 1
            quantidade = max(1, quantidade)
            if parcela_raw and not _is_parcela_valid(parcela_raw):
                msg = "Parcela invalida. Use 01/36 ou 1/-."
                params = {"msg": msg, "level": "error"}
                return redirect(
                    f"{reverse('financeiro_compra_detail', kwargs={'pk': compra.pk})}?{urlencode(params)}"
                )
            if nome:
                parcela = _normalize_parcela(parcela_raw, "1/1")
                CompraItem.objects.create(
                    compra=compra,
                    nome=nome,
                    valor=valor,
                    quantidade=quantidade,
                    parcela=parcela,
                    tipo_id=tipo_id or None,
                    pago=pago,
                )
            return redirect("financeiro_compra_detail", pk=compra.pk)
        if action == "toggle_item_pago":
            item_id = request.POST.get("item_id")
            item = get_object_or_404(CompraItem.objects.select_related("tipo"), pk=item_id, compra=compra)
            item.pago = not item.pago
            item.save(update_fields=["pago"])
            if request.headers.get("x-requested-with") == "XMLHttpRequest":
                return JsonResponse(
                    {
                        "ok": True,
                        "row": serialize_item_payload(item),
                        "compra": serialize_compra_summary(compra),
                        "message": "Status do item atualizado.",
                        "level": "success",
                    }
                )
            return redirect("financeiro_compra_detail", pk=compra.pk)
        if action == "update_item_valor":
            item_id = request.POST.get("item_id")
            item = get_object_or_404(CompraItem.objects.select_related("tipo"), pk=item_id, compra=compra)
            valor_raw = request.POST.get("valor", "").replace(",", ".").strip()
            try:
                valor = Decimal(valor_raw) if valor_raw else None
            except (InvalidOperation, ValueError):
                if request.headers.get("x-requested-with") == "XMLHttpRequest":
                    return JsonResponse(
                        {
                            "ok": False,
                            "message": "Informe um valor valido.",
                            "level": "error",
                        },
                        status=400,
                    )
                params = {"msg": "Informe um valor valido.", "level": "error"}
                return redirect(
                    f"{reverse('financeiro_compra_detail', kwargs={'pk': compra.pk})}?{urlencode(params)}"
                )
            item.valor = valor
            item.save(update_fields=["valor"])
            if request.headers.get("x-requested-with") == "XMLHttpRequest":
                return JsonResponse(
                    {
                        "ok": True,
                        "row": serialize_item_payload(item),
                        "compra": serialize_compra_summary(compra),
                        "message": "Valor do item atualizado.",
                        "level": "success",
                    }
                )
            return redirect("financeiro_compra_detail", pk=compra.pk)
        if action == "update_item":
            item_id = request.POST.get("item_id")
            item = get_object_or_404(CompraItem, pk=item_id, compra=compra)
            nome = request.POST.get("nome", "").strip()
            valor_raw = request.POST.get("valor", "").replace(",", ".").strip()
            quantidade_raw = request.POST.get("quantidade", "").strip()
            parcela_raw = request.POST.get("parcela", "")
            tipo_id = request.POST.get("tipo")
            pago = request.POST.get("pago") == "on"
            try:
                valor = Decimal(valor_raw)
            except (InvalidOperation, ValueError):
                valor = None
            try:
                quantidade = int(quantidade_raw) if quantidade_raw else item.quantidade
            except ValueError:
                quantidade = item.quantidade
            quantidade = max(1, quantidade)
            if parcela_raw and not _is_parcela_valid(parcela_raw):
                msg = "Parcela invalida. Use 01/36 ou 1/-."
                params = {"msg": msg, "level": "error"}
                return redirect(
                    f"{reverse('financeiro_compra_detail', kwargs={'pk': compra.pk})}?{urlencode(params)}"
                )
            if nome:
                item.nome = nome
            item.valor = valor
            item.quantidade = quantidade
            item.parcela = _normalize_parcela(parcela_raw, item.parcela)
            item.tipo_id = tipo_id or None
            item.pago = pago
            item.save(update_fields=["nome", "valor", "quantidade", "parcela", "tipo", "pago"])
            return redirect("financeiro_compra_detail", pk=compra.pk)
        if action == "delete_item":
            item_id = request.POST.get("item_id")
            item = get_object_or_404(CompraItem, pk=item_id, compra=compra)
            item.delete()
            return redirect("financeiro_compra_detail", pk=compra.pk)
    compra.status_label = _compra_status_label(compra)
    itens = list(compra.itens.select_related("tipo").order_by("id"))
    itens_table_data = []
    for item in itens:
        item.total_valor = (item.valor or 0) * (item.quantidade or 0)
        itens_table_data.append(serialize_item_payload(item))
    compra.total_itens = sum(item.total_valor for item in itens)
    tipos = TipoCompra.objects.order_by("nome")
    categorias = CategoriaCompra.objects.order_by("nome")
    centros = CentroCusto.objects.order_by("nome")
    cadernos = _financeiro_allowed_cadernos_qs(request.user, cliente).order_by("nome")
    return render(
        request,
        "core/financeiro_compra_detail.html",
        {
            "compra": compra,
            "itens": itens,
            "itens_table_data": itens_table_data,
            "tipos": tipos,
            "categorias": categorias,
            "centros": centros,
            "cadernos": cadernos,
            "message": message,
            "message_level": message_level,
            "open_cadastro": open_cadastro,
        },
    )


@login_required
def admin_explorar(request):
    if not _is_admin_user(request.user):
        return HttpResponseForbidden("Sem permissao.")
    cliente_id = request.GET.get("cliente_id")
    cliente_q = request.GET.get("cliente_q", "").strip()
    cliente_sort = request.GET.get("cliente_sort", "nome")
    proposta_status = request.GET.get("proposta_status", "").strip()
    proposta_sort = request.GET.get("proposta_sort", "-criado_em")

    clientes = PerfilUsuario.objects.all()
    if cliente_q:
        clientes = clientes.filter(nome__icontains=cliente_q)
    if cliente_sort == "empresa":
        clientes = clientes.order_by("empresa", "nome")
    elif cliente_sort == "email":
        clientes = clientes.order_by("email", "nome")
    else:
        clientes = clientes.order_by("nome")

    cliente = None
    propostas = Proposta.objects.none()
    if cliente_id:
        cliente = get_object_or_404(PerfilUsuario, pk=cliente_id)
        propostas = Proposta.objects.filter(cliente=cliente)
        if proposta_status == "pendente":
            propostas = propostas.filter(aprovada__isnull=True)
        elif proposta_status == "aprovada":
            propostas = propostas.filter(aprovada=True)
        elif proposta_status == "reprovada":
            propostas = propostas.filter(aprovada=False)
        elif proposta_status == "finalizada":
            propostas = propostas.filter(finalizada=True)
        if proposta_sort == "prioridade":
            propostas = propostas.order_by("prioridade", "-criado_em")
        elif proposta_sort == "valor":
            propostas = propostas.order_by("-valor", "-criado_em")
        else:
            propostas = propostas.order_by("-criado_em")
    return render(
        request,
        "admin/explorar.html",
        {
            "clientes": clientes,
            "cliente": cliente,
            "propostas": propostas,
            "cliente_q": cliente_q,
            "cliente_sort": cliente_sort,
            "proposta_status": proposta_status,
            "proposta_sort": proposta_sort,
        },
    )


@login_required
def modulos_acesso_gerenciar(request):
    if not _is_admin_user(request.user):
        return HttpResponseForbidden("Sem permissao.")
    message = None
    if request.method == "POST":
        action = request.POST.get("action")
        if action == "update_module":
            module = get_object_or_404(ModuloAcesso, pk=request.POST.get("module_id"))
            tipo_ids = request.POST.getlist("tipos")
            ativo = request.POST.get("ativo") == "on"
            module.ativo = ativo
            if module.tipo == ModuloAcesso.Tipo.CORE:
                tipos = TipoPerfil.objects.filter(id__in=tipo_ids)
                module.save(update_fields=["ativo"])
                module.tipos.set(tipos)
            else:
                module.save(update_fields=["ativo"])
            return redirect("modulos_acesso_gerenciar")

    modules = ModuloAcesso.objects.prefetch_related("tipos").order_by("nome")
    core_modules = [module for module in modules if module.tipo == ModuloAcesso.Tipo.CORE]
    app_modules = [module for module in modules if module.tipo == ModuloAcesso.Tipo.APP]
    return render(
        request,
        "core/modulos_acesso.html",
        {
            "core_modules": core_modules,
            "app_modules": app_modules,
            "tipos": TipoPerfil.objects.order_by("nome"),
            "message": message,
        },
    )


@login_required
def produtos_gerenciar(request):
    if not _is_admin_user(request.user):
        return HttpResponseForbidden("Sem permissao.")
    ensure_billing_catalog()
    message = None
    if request.method == "POST":
        action = request.POST.get("action")
        if action == "create_product":
            codigo = (request.POST.get("codigo") or "").strip().upper()
            nome = (request.POST.get("nome") or "").strip()
            descricao = (request.POST.get("descricao") or "").strip()
            ativo = request.POST.get("ativo") == "on"
            if not nome:
                message = "Informe um nome para o produto."
            else:
                product = ProdutoPlataforma(
                    codigo=codigo or nome,
                    nome=nome,
                    descricao=descricao,
                    ativo=ativo,
                )
                try:
                    product.save()
                    return redirect("produtos_gerenciar")
                except Exception:
                    message = "Nao foi possivel salvar o produto. Verifique se o codigo ja existe."
        if action == "update_product":
            product = get_object_or_404(ProdutoPlataforma, pk=request.POST.get("product_id"))
            codigo = (request.POST.get("codigo") or "").strip().upper()
            nome = (request.POST.get("nome") or "").strip()
            descricao = (request.POST.get("descricao") or "").strip()
            ativo = request.POST.get("ativo") == "on"
            if not nome:
                message = "Informe um nome para o produto."
            else:
                product.codigo = codigo or product.codigo
                product.nome = nome
                product.descricao = descricao
                product.ativo = ativo
                try:
                    product.save()
                    return redirect("produtos_gerenciar")
                except Exception:
                    message = "Nao foi possivel atualizar o produto. Verifique se o codigo ja existe."

    products = list(
        ProdutoPlataforma.objects.annotate(
            total_acessos=Count("acessos_usuario"),
            acessos_ativos=Count(
                "acessos_usuario",
                filter=Q(acessos_usuario__status__in=[
                    AcessoProdutoUsuario.Status.ATIVO,
                    AcessoProdutoUsuario.Status.TRIAL_ATIVO,
                ]),
            ),
        ).order_by("nome")
    )
    total_product_accesses = sum(product.total_acessos for product in products)
    return render(
        request,
        "core/produtos.html",
        {
            "products": products,
            "total_product_accesses": total_product_accesses,
            "message": message,
        },
    )


@login_required
def pagamentos_planos_gerenciar(request):
    if not _is_admin_user(request.user):
        return HttpResponseForbidden("Sem permissao.")
    product = ensure_billing_catalog()
    settings_obj = payment_config()
    provider_defaults = _ensure_payment_checkout_urls(request, settings_obj)
    message = None
    if request.method == "POST":
        action = request.POST.get("action")
        if action == "update_payment_config":
            settings_obj.enabled = request.POST.get("enabled") == "on"
            settings_obj.sandbox_mode = request.POST.get("sandbox_mode") == "on"
            settings_obj.mercado_pago_public_key = (request.POST.get("mercado_pago_public_key") or "").strip()
            access_token = (request.POST.get("mercado_pago_access_token") or "").strip()
            webhook_secret = (request.POST.get("mercado_pago_webhook_secret") or "").strip()
            if access_token:
                settings_obj.mercado_pago_access_token = access_token
            if webhook_secret:
                settings_obj.mercado_pago_webhook_secret = webhook_secret
            settings_obj.checkout_success_url = (request.POST.get("checkout_success_url") or "").strip() or provider_defaults["success"]
            settings_obj.checkout_failure_url = (request.POST.get("checkout_failure_url") or "").strip() or provider_defaults["failure"]
            settings_obj.checkout_pending_url = (request.POST.get("checkout_pending_url") or "").strip() or provider_defaults["pending"]
            try:
                settings_obj.trial_duration_days = max(1, min(int(request.POST.get("trial_duration_days") or "30"), 120))
            except (TypeError, ValueError):
                settings_obj.trial_duration_days = 30
            settings_obj.updated_by = request.user
            settings_obj.save()
            return redirect("pagamentos_planos_gerenciar")
        if action == "update_plan":
            plan = get_object_or_404(PlanoComercial, pk=request.POST.get("plan_id"), produto=product)
            plan.nome = (request.POST.get("nome") or "").strip() or plan.nome
            plan.descricao = (request.POST.get("descricao") or "").strip()
            plan.ativo = request.POST.get("ativo") == "on"
            plan.is_free = request.POST.get("is_free") == "on"
            try:
                plan.ordem = max(0, min(int(request.POST.get("ordem") or plan.ordem), 999))
            except (TypeError, ValueError):
                pass
            rack_limit_raw = (request.POST.get("rack_limit_simultaneous") or "").strip()
            if rack_limit_raw:
                try:
                    plan.rack_limit_simultaneous = max(1, min(int(rack_limit_raw), 999))
                except (TypeError, ValueError):
                    pass
            else:
                plan.rack_limit_simultaneous = None
            for field_name in ("preco_mensal", "preco_anual"):
                raw = (request.POST.get(field_name) or "").replace(",", ".").strip()
                try:
                    setattr(plan, field_name, Decimal(raw) if raw else None)
                except (InvalidOperation, ValueError):
                    setattr(plan, field_name, None)
            plan.provider_plan_code_mensal = (request.POST.get("provider_plan_code_mensal") or "").strip()
            plan.provider_plan_code_anual = (request.POST.get("provider_plan_code_anual") or "").strip()
            plan.save()
            return redirect("pagamentos_planos_gerenciar")

    plans = list(PlanoComercial.objects.filter(produto=product).order_by("ordem", "nome"))
    subscriptions = list(
        AssinaturaUsuario.objects.select_related("usuario", "plano")
        .filter(produto=product)
        .order_by("-updated_at", "-created_at")[:40]
    )
    webhook_events = list(EventoPagamentoWebhook.objects.order_by("-received_at")[:25])
    return render(
        request,
        "core/billing_admin.html",
        {
            "message": message,
            "product": product,
            "settings_obj": settings_obj,
            "provider_defaults": provider_defaults,
            "plans": plans,
            "subscriptions": subscriptions,
            "webhook_events": webhook_events,
        },
    )


@login_required
def admin_logs(request):
    if not _is_admin_user(request.user):
        return HttpResponseForbidden("Sem permissao.")
    user_q = request.GET.get("user", "").strip()
    module_q = request.GET.get("module", "").strip()
    logs_qs = AdminAccessLog.objects.select_related("user").all()
    if user_q:
        logs_qs = logs_qs.filter(user__username__icontains=user_q)
    if module_q:
        logs_qs = logs_qs.filter(module__icontains=module_q)
    logs = logs_qs.order_by("-created_at")[:500]
    return render(
        request,
        "core/admin_logs.html",
        {
            "logs": logs,
            "user_q": user_q,
            "module_q": module_q,
        },
    )


@login_required
def admin_db_monitor(request):
    if not _is_admin_user(request.user):
        return HttpResponseForbidden("Sem permissao.")

    started_at = timezone.localtime(timezone.now())
    db_ok = False
    db_error = ""
    db_info = {
        "name": "",
        "user": "",
        "server_time": None,
        "version": "",
        "in_recovery": None,
        "postmaster_start": None,
        "active_connections": None,
    }
    cfg = connections["default"].settings_dict

    try:
        with connections["default"].cursor() as cursor:
            cursor.execute("SELECT 1")
            db_ok = cursor.fetchone()[0] == 1

            cursor.execute("SELECT current_database(), current_user, now(), version()")
            row = cursor.fetchone()
            if row:
                db_info["name"] = row[0] or ""
                db_info["user"] = row[1] or ""
                db_info["server_time"] = row[2]
                db_info["version"] = row[3] or ""

            try:
                cursor.execute("SELECT pg_is_in_recovery(), pg_postmaster_start_time()")
                row = cursor.fetchone()
                if row:
                    db_info["in_recovery"] = row[0]
                    db_info["postmaster_start"] = row[1]
            except DatabaseError:
                pass

            try:
                cursor.execute("SELECT count(*) FROM pg_stat_activity")
                row = cursor.fetchone()
                if row:
                    db_info["active_connections"] = row[0]
            except DatabaseError:
                pass
    except Exception as exc:
        db_ok = False
        db_error = str(exc)

    context = {
        "started_at": started_at,
        "db_ok": db_ok,
        "db_error": db_error,
        "db_info": db_info,
        "db_config": {
            "engine": cfg.get("ENGINE", ""),
            "host": cfg.get("HOST", ""),
            "port": cfg.get("PORT", ""),
            "name": cfg.get("NAME", ""),
        },
    }
    return render(request, "core/admin_db_monitor.html", context)


def _admin_db_public_tables(ingest_only=False):
    with connections["default"].cursor() as cursor:
        params = []
        ingest_clause = ""
        if ingest_only:
            ingest_clause = " AND t.table_name ILIKE %s"
            params.append("%ingest%")
        cursor.execute(
            f"""
            SELECT t.table_name, COALESCE(s.n_live_tup::bigint, 0)
            FROM information_schema.tables t
            LEFT JOIN pg_stat_user_tables s ON s.relname = t.table_name
            WHERE t.table_schema = 'public'
              AND t.table_type = 'BASE TABLE'
              {ingest_clause}
            ORDER BY t.table_name
            """,
            params,
        )
        rows = cursor.fetchall() or []
    return [{"name": row[0], "estimated_rows": int(row[1] or 0)} for row in rows]


def _admin_db_table_columns(table_name):
    with connections["default"].cursor() as cursor:
        cursor.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = %s
            ORDER BY ordinal_position
            """,
            [table_name],
        )
        rows = cursor.fetchall() or []
    return [row[0] for row in rows]


def _admin_db_to_int(value, default_value, min_value=None, max_value=None):
    try:
        number = int(value)
    except (TypeError, ValueError):
        number = default_value
    if min_value is not None and number < min_value:
        number = min_value
    if max_value is not None and number > max_value:
        number = max_value
    return number


def _admin_db_ingest_payload_keys(table_name, max_keys=80):
    qn = connections["default"].ops.quote_name
    table_sql = qn(table_name)
    payload_sql = qn("payload")
    try:
        with connections["default"].cursor() as cursor:
            cursor.execute(
                f"""
                SELECT key
                FROM (
                    SELECT DISTINCT jsonb_object_keys({payload_sql}) AS key
                    FROM {table_sql}
                    WHERE {payload_sql} IS NOT NULL
                      AND jsonb_typeof({payload_sql}) = 'object'
                ) payload_keys
                ORDER BY key
                LIMIT %s
                """,
                [max_keys],
            )
            rows = cursor.fetchall() or []
    except DatabaseError:
        return []
    return [row[0] for row in rows if row and row[0]]


@login_required
def admin_db_table(request):
    if not _is_admin_user(request.user):
        return HttpResponseForbidden("Sem permissao.")

    tables = _admin_db_public_tables(ingest_only=True)
    selected_table = request.GET.get("table", "").strip()
    available_names = {item["name"] for item in tables}
    if not selected_table or selected_table not in available_names:
        selected_table = tables[0]["name"] if tables else ""

    return render(
        request,
        "core/admin_db_table.html",
        {
            "tables": tables,
            "selected_table": selected_table,
            "started_at": timezone.localtime(timezone.now()),
        },
    )


@login_required
def admin_db_table_data(request):
    if not _is_admin_user(request.user):
        return JsonResponse({"ok": False, "error": "forbidden"}, status=403)

    table_name = request.GET.get("table", "").strip()
    page = _admin_db_to_int(request.GET.get("page"), 1, min_value=1, max_value=100000)
    page_size = _admin_db_to_int(request.GET.get("page_size"), 50, min_value=10, max_value=200)
    sort_by = request.GET.get("sort_by", "").strip()
    sort_dir = request.GET.get("sort_dir", "asc").strip().lower()
    if sort_dir not in {"asc", "desc"}:
        sort_dir = "asc"

    tables = _admin_db_public_tables(ingest_only=True)
    available_names = {item["name"] for item in tables}
    if table_name not in available_names:
        return JsonResponse({"ok": False, "error": "invalid_table"}, status=400)

    base_columns = _admin_db_table_columns(table_name)
    if not base_columns:
        return JsonResponse({"ok": False, "error": "no_columns"}, status=400)

    is_ingest_record = table_name == "core_ingestrecord"
    payload_keys = []
    virtual_columns = []
    if is_ingest_record and "payload" in base_columns:
        payload_keys = _admin_db_ingest_payload_keys(table_name, max_keys=120)
        virtual_columns = ["client", *[f"payload.{key}" for key in payload_keys]]
    columns = [*base_columns, *virtual_columns]
    qn = connections["default"].ops.quote_name

    recent_sort_sql = ""
    if "updated_at" in base_columns and "created_at" in base_columns:
        recent_sort_sql = f"COALESCE({qn('updated_at')}, {qn('created_at')})"
    elif "updated_at" in base_columns:
        recent_sort_sql = qn("updated_at")
    elif "created_at" in base_columns:
        recent_sort_sql = qn("created_at")
    elif "id" in base_columns:
        recent_sort_sql = qn("id")

    allowed_sort_columns = set(columns)
    if recent_sort_sql:
        allowed_sort_columns.add("__recent__")
    if sort_by not in allowed_sort_columns:
        sort_by = "__recent__" if recent_sort_sql else ("id" if "id" in columns else columns[0])

    raw_filters = request.GET.get("filters", "").strip()
    parsed_filters = {}
    if raw_filters:
        try:
            payload = json.loads(raw_filters)
            if isinstance(payload, dict):
                parsed_filters = payload
        except json.JSONDecodeError:
            return JsonResponse({"ok": False, "error": "invalid_filters"}, status=400)

    where_parts = []
    where_params = []

    def resolve_column_sql(col_name):
        if col_name == "__recent__" and recent_sort_sql:
            return recent_sort_sql, []
        if col_name in base_columns:
            return qn(col_name), []
        if is_ingest_record and col_name == "client":
            client_id_sql = qn("client_id")
            source_id_sql = qn("source_id")
            return f"COALESCE(NULLIF({client_id_sql}, ''), split_part({source_id_sql}, ':', 1))", []
        if is_ingest_record and col_name.startswith("payload."):
            payload_key = col_name.split(".", 1)[1]
            if payload_key in payload_keys:
                return f"{qn('payload')} ->> %s", [payload_key]
        return "", []

    for col_name, values in parsed_filters.items():
        if not isinstance(values, list):
            continue
        col_sql, col_expr_params = resolve_column_sql(col_name)
        if not col_sql:
            continue
        clean_values = []
        include_null = False
        for value in values[:500]:
            if value == "__NULL__":
                include_null = True
                continue
            clean_values.append(value)
        if not clean_values and not include_null:
            continue
        parts = []
        clause_params = []
        if clean_values:
            placeholders = ", ".join(["%s"] * len(clean_values))
            parts.append(f"({col_sql}) IN ({placeholders})")
            clause_params.extend(col_expr_params)
            clause_params.extend(clean_values)
        if include_null:
            parts.append(f"({col_sql}) IS NULL")
            clause_params.extend(col_expr_params)
        where_parts.append("(" + " OR ".join(parts) + ")")
        where_params.extend(clause_params)

    where_sql = ""
    if where_parts:
        where_sql = " WHERE " + " AND ".join(where_parts)

    offset = (page - 1) * page_size
    table_sql = qn(table_name)
    sort_sql, sort_params = resolve_column_sql(sort_by)
    if not sort_sql:
        sort_by = "id" if "id" in columns else columns[0]
        sort_sql, sort_params = resolve_column_sql(sort_by)
    order_sql = "DESC" if sort_dir == "desc" else "ASC"

    try:
        with connections["default"].cursor() as cursor:
            cursor.execute(
                f"SELECT COUNT(*) FROM {table_sql}{where_sql}",
                where_params,
            )
            total_rows = int((cursor.fetchone() or [0])[0] or 0)

            cursor.execute(
                f"""
                SELECT *
                FROM {table_sql}
                {where_sql}
                ORDER BY ({sort_sql}) {order_sql} NULLS LAST
                LIMIT %s OFFSET %s
                """,
                [*where_params, *sort_params, page_size, offset],
            )
            db_rows = cursor.fetchall() or []
    except DatabaseError as exc:
        return JsonResponse(
            {"ok": False, "error": "query_failed", "detail": str(exc)},
            status=400,
        )

    total_pages = max(1, (total_rows + page_size - 1) // page_size)
    normalized_page = page if page <= total_pages else total_pages
    if normalized_page != page:
        return JsonResponse(
            {
                "ok": True,
                "table": table_name,
                "columns": columns,
                "rows": [],
                "page": normalized_page,
                "page_size": page_size,
                "total_rows": total_rows,
                "total_pages": total_pages,
                "sort_by": sort_by,
                "sort_dir": sort_dir,
                "filters": parsed_filters,
            }
        )

    rows = []
    for raw_row in db_rows:
        row_dict = {}
        for index, col_name in enumerate(base_columns):
            row_dict[col_name] = raw_row[index]
        if is_ingest_record:
            source_id_value = row_dict.get("source_id")
            client_id_value = row_dict.get("client_id")
            client_value = client_id_value if client_id_value else ""
            if not client_value and isinstance(source_id_value, str) and ":" in source_id_value:
                client_value = source_id_value.split(":", 1)[0]
            row_dict["client"] = client_value
            payload_value = row_dict.get("payload")
            payload_obj = {}
            if isinstance(payload_value, dict):
                payload_obj = payload_value
            elif isinstance(payload_value, (str, bytes, bytearray)):
                try:
                    payload_obj = json.loads(payload_value)
                except (TypeError, ValueError, json.JSONDecodeError):
                    payload_obj = {}
            if not isinstance(payload_obj, dict):
                payload_obj = {}
            for payload_key in payload_keys:
                row_dict[f"payload.{payload_key}"] = payload_obj.get(payload_key)
        rows.append(row_dict)

    return JsonResponse(
        {
            "ok": True,
            "table": table_name,
            "columns": columns,
            "rows": rows,
            "page": normalized_page,
            "page_size": page_size,
            "total_rows": total_rows,
            "total_pages": total_pages,
            "sort_by": sort_by,
            "sort_dir": sort_dir,
            "filters": parsed_filters,
        }
    )


@login_required
def admin_db_table_values(request):
    if not _is_admin_user(request.user):
        return JsonResponse({"ok": False, "error": "forbidden"}, status=403)

    table_name = request.GET.get("table", "").strip()
    column_name = request.GET.get("column", "").strip()
    q = request.GET.get("q", "").strip()
    limit = _admin_db_to_int(request.GET.get("limit"), 200, min_value=20, max_value=500)

    tables = _admin_db_public_tables(ingest_only=True)
    available_names = {item["name"] for item in tables}
    if table_name not in available_names:
        return JsonResponse({"ok": False, "error": "invalid_table"}, status=400)

    base_columns = _admin_db_table_columns(table_name)
    is_ingest_record = table_name == "core_ingestrecord"
    payload_keys = []
    virtual_columns = []
    if is_ingest_record and "payload" in base_columns:
        payload_keys = _admin_db_ingest_payload_keys(table_name, max_keys=120)
        virtual_columns = ["client", *[f"payload.{key}" for key in payload_keys]]
    columns = [*base_columns, *virtual_columns]
    if column_name not in columns:
        return JsonResponse({"ok": False, "error": "invalid_column"}, status=400)

    qn = connections["default"].ops.quote_name
    table_sql = qn(table_name)

    if column_name in base_columns:
        col_sql = qn(column_name)
        col_params = []
    elif column_name == "client":
        col_sql = f"COALESCE(NULLIF({qn('client_id')}, ''), split_part({qn('source_id')}, ':', 1))"
        col_params = []
    elif column_name.startswith("payload.") and is_ingest_record:
        payload_key = column_name.split(".", 1)[1]
        if payload_key not in payload_keys:
            return JsonResponse({"ok": False, "error": "invalid_column"}, status=400)
        col_sql = f"{qn('payload')} ->> %s"
        col_params = [payload_key]
    else:
        return JsonResponse({"ok": False, "error": "invalid_column"}, status=400)

    values = []
    try:
        with connections["default"].cursor() as cursor:
            cursor.execute(
                f"""
                WITH base AS (
                    SELECT ({col_sql}) AS col_value
                    FROM {table_sql}
                )
                SELECT col_value, COUNT(*)
                FROM base
                WHERE col_value IS NOT NULL
                {"AND CAST(col_value AS TEXT) ILIKE %s" if q else ""}
                GROUP BY col_value
                ORDER BY COUNT(*) DESC, CAST(col_value AS TEXT) ASC
                LIMIT %s
                """,
                [*col_params, *([f"%{q}%"] if q else []), limit],
            )
            rows = cursor.fetchall() or []
            for value, count in rows:
                values.append(
                    {
                        "value": value,
                        "label": str(value),
                        "count": int(count or 0),
                    }
                )

            if not q:
                cursor.execute(
                    f"""
                    WITH base AS (
                        SELECT ({col_sql}) AS col_value
                        FROM {table_sql}
                    )
                    SELECT COUNT(*)
                    FROM base
                    WHERE col_value IS NULL
                    """,
                    col_params,
                )
                null_count = int((cursor.fetchone() or [0])[0] or 0)
                if null_count > 0:
                    values.insert(
                        0,
                        {
                            "value": "__NULL__",
                            "label": "(vazio)",
                            "count": null_count,
                        },
                    )
    except DatabaseError as exc:
        return JsonResponse(
            {"ok": False, "error": "query_failed", "detail": str(exc)},
            status=400,
        )

    return JsonResponse({"ok": True, "table": table_name, "column": column_name, "values": values})


@login_required
def ajustes_sistema(request):
    if not _is_admin_user(request.user):
        return HttpResponseForbidden("Sem permissao.")
    message = None
    system_config = SystemConfiguration.load()
    if request.method == "POST":
        action = request.POST.get("action")
        if action == "create_channel_type":
            nome = request.POST.get("nome", "").strip().upper()
            if nome:
                TipoCanalIO.objects.get_or_create(nome=nome, defaults={"ativo": True})
        if action == "create_tipo_ativo":
            nome = request.POST.get("nome", "").strip()
            codigo = request.POST.get("codigo", "").strip().upper()
            if nome and codigo:
                TipoAtivo.objects.get_or_create(
                    nome=nome,
                    defaults={"codigo": codigo, "ativo": True},
                )
        if action == "toggle_tipo_ativo":
            tipo_id = request.POST.get("tipo_id")
            tipo = TipoAtivo.objects.filter(pk=tipo_id).first()
            if tipo:
                tipo.ativo = not tipo.ativo
                tipo.save(update_fields=["ativo"])
        if action == "update_maintenance_mode":
            system_config.maintenance_mode_enabled = request.POST.get("maintenance_mode_enabled") == "on"
            system_config.maintenance_message = request.POST.get("maintenance_message", "").strip()
            system_config.updated_by = request.user
            system_config.save(update_fields=["maintenance_mode_enabled", "maintenance_message", "updated_by", "updated_at"])
            message = "Modo manutencao atualizado."
    channel_types = TipoCanalIO.objects.filter(ativo=True).order_by("nome")
    tipos_ativos = TipoAtivo.objects.order_by("nome")
    return render(
        request,
        "core/ajustes.html",
        {
            "message": message,
            "channel_types": channel_types,
            "tipos_ativos": tipos_ativos,
            "system_config": system_config,
        },
    )

