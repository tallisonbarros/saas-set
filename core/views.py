import calendar
import hmac
import json
import logging
import os
import ipaddress
import re
from io import BytesIO
from datetime import date, datetime, timedelta
from decimal import Decimal, InvalidOperation

from django.contrib.auth import authenticate, login, logout
from django.contrib.auth.decorators import login_required
from django.core.paginator import Paginator
from django.http import HttpResponse, HttpResponseForbidden, HttpResponseNotAllowed, JsonResponse
from django.shortcuts import get_object_or_404, redirect, render
from django.template.loader import render_to_string
from django.urls import reverse
from urllib.parse import urlencode
from django.utils import timezone
from django.views.decorators.http import require_POST
from django.views.decorators.csrf import csrf_exempt
from django.utils import timezone

from django.contrib.auth.models import User
from django.db import DatabaseError, connections
from django.db.models import Case, Count, DecimalField, F, IntegerField, OuterRef, Q, Subquery, Sum, TextField, Value, When
from django.db.models.expressions import ExpressionWrapper
from django.db.models.functions import Cast

from .forms import RegisterForm, TipoPerfilCreateForm, UserCreateForm
from .models import (
    CanalRackIO,
    CategoriaCompra,
    Caderno,
    CentroCusto,
    PerfilUsuario,
    Compra,
    CompraItem,
    GrupoRackIO,
    LocalRackIO,
    ModuloIO,
    ModuloRackIO,
    FinanceiroID,
    Inventario,
    InventarioID,
    ListaIP,
    ListaIPID,
    ListaIPItem,
    App,
    IngestRecord,
    IngestErrorLog,
    IngestRule,
    AdminAccessLog,
    Radar,
    RadarAtividade,
    RadarClassificacao,
    RadarContrato,
    RadarID,
    RadarTrabalho,
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

logger = logging.getLogger(__name__)

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


def _user_role(user):
    if user.is_superuser or user.is_staff:
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
        trabalho.save(update_fields=["status"])


def home(request):
    if request.user.is_authenticated:
        logout(request)
    return render(request, "core/home.html")


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
    if request.user.is_staff and not cliente:
        apps = App.objects.filter(ativo=True).order_by("nome")
    elif cliente:
        apps = cliente.apps.filter(ativo=True).order_by("nome")
    else:
        apps = App.objects.none()
    return render(
        request,
        "core/painel.html",
        {
            "display_name": display_name,
            "role": role,
            "is_financeiro": True,
            "is_cliente": True,
            "is_vendedor": True,
            "apps": apps,
        },
    )


@login_required
def planta_conectada(request):
    if not request.user.is_staff:
        return HttpResponseForbidden("Sem permissao.")
    if request.method == "POST":
        action = request.POST.get("action")
        if action == "clear_ingest":
            IngestRecord.objects.all().delete()
            return redirect("planta_conectada")
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


@login_required
def ingest_error_logs(request):
    if not request.user.is_staff:
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
    if not request.user.is_staff:
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
    if not request.user.is_staff:
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
    if not request.user.is_staff:
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
    cliente = _get_cliente(request.user)
    app = get_object_or_404(App, slug=slug, ativo=True)
    if request.user.is_staff:
        allowed = True
    else:
        allowed = bool(cliente) and cliente.apps.filter(pk=app.pk).exists()
    if not allowed:
        return HttpResponseForbidden("Sem permissao.")
    if app.slug == "appmilhaobla":
        return redirect("app_milhao_bla_dashboard")
    if app.slug == "approtas":
        return redirect("app_rotas_dashboard")
    return render(request, "core/app_home.html", {"app": app})


@login_required
def apps_gerenciar(request):
    if not request.user.is_staff:
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


def register(request):
    if request.user.is_authenticated:
        return redirect("painel")
    message = None
    form = RegisterForm()
    if request.method == "POST":
        form = RegisterForm(request.POST)
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
                return redirect("painel")
            return redirect("login")
        message = "Revise os campos e tente novamente."
    return render(
        request,
        "core/register.html",
        {
            "form": form,
            "message": message,
        },
    )


@login_required
def ios_list(request):
    cliente = _get_cliente(request.user)
    if not cliente and not request.user.is_staff:
        return HttpResponseForbidden("Sem cadastro de cliente.")

    if request.user.is_staff and not cliente:
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
    message = None
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

    if request.user.is_staff and not cliente:
        racks = RackIO.objects.all()
    else:
        racks = RackIO.objects.filter(Q(cliente=cliente) | Q(id_planta__in=cliente.plantas.all()))
    racks = racks.select_related("inventario", "local", "grupo").annotate(
        ocupados=Count("slots", filter=Q(slots__modulo__isnull=False)),
        canais_total=Count("slots__modulo__canais", distinct=True),
        canais_comissionados=Count(
            "slots__modulo__canais",
            filter=Q(slots__modulo__canais__comissionado=True),
            distinct=True,
        ),
    )
    racks_ordered = racks.order_by("local__nome", "grupo__nome", "inventario__nome", "nome")
    rack_groups = []
    grouped = {}
    for rack in racks_ordered:
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
            },
        )
        group_bucket = local_bucket["groups"].setdefault(
            grupo_key,
            {
                "grupo": rack.grupo if rack.grupo_id else None,
                "racks": [],
            },
        )
        group_bucket["racks"].append(rack)

    for _, local_data in grouped.items():
        group_rows = list(local_data["groups"].values())
        rack_groups.append(
            {
                "local": local_data["local"],
                "groups": group_rows,
            }
        )
    channel_types = TipoCanalIO.objects.filter(ativo=True).order_by("nome")
    locais = LocalRackIO.objects.none()
    grupos = GrupoRackIO.objects.none()
    if cliente:
        locais = LocalRackIO.objects.filter(cliente=cliente).order_by("nome")
        grupos = GrupoRackIO.objects.filter(cliente=cliente).order_by("nome")
    search_term = request.GET.get("q", "").strip()
    rack_filter = request.GET.get("rack", "").strip()
    local_filter = request.GET.get("local", "").strip()
    grupo_filter = request.GET.get("grupo", "").strip()
    search_results = []
    search_count = 0
    if search_term or rack_filter or local_filter or grupo_filter:
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
        if rack_filter and rack_filter.isdigit():
            channels = channels.filter(modulo__rack_id=int(rack_filter))
        if local_filter and local_filter.isdigit():
            channels = channels.filter(modulo__rack__local_id=int(local_filter))
        if grupo_filter and grupo_filter.isdigit():
            channels = channels.filter(modulo__rack__grupo_id=int(grupo_filter))
        if search_term:
            channels = channels.filter(search_filter)
        channels = (
            channels.select_related("modulo", "modulo__rack", "modulo__modulo_modelo", "tipo")
            .annotate(slot_pos=Subquery(slot_pos_subquery))
            .order_by("modulo__rack__nome", "slot_pos", "indice")[:200]
        )
        search_results = list(channels)
        search_count = len(search_results)
    if request.headers.get("x-requested-with") == "XMLHttpRequest":
        payload = []
        for channel in search_results:
            payload.append(
                {
                    "rack": channel.modulo.rack.nome,
                    "slot": f"S{channel.slot_pos}" if channel.slot_pos else "-",
                    "modulo": channel.modulo.nome or channel.modulo.modulo_modelo.nome,
                    "canal": f"CH{channel.indice:02d}",
                    "canal_tag": channel.tag or "-",
                    "tipo": channel.tipo.nome,
                    "local": channel.modulo.rack.local.nome if channel.modulo.rack.local_id else "-",
                    "grupo": channel.modulo.rack.grupo.nome if channel.modulo.rack.grupo_id else "-",
                    "url": reverse("ios_rack_modulo_detail", kwargs={"pk": channel.modulo.id}),
                }
            )
        return JsonResponse(
            {
                "count": search_count,
                "results": payload,
            }
        )
    return render(
        request,
        "core/ios_list.html",
        {
            "racks": racks,
            "rack_groups": rack_groups,
            "channel_types": channel_types,
            "can_manage": bool(cliente),
            "search_term": search_term,
            "rack_filter": rack_filter,
            "local_filter": local_filter,
            "grupo_filter": grupo_filter,
            "search_results": search_results,
            "search_count": search_count,
            "inventarios": inventarios_qs.order_by("nome"),
            "locais": locais,
            "grupos": grupos,
        },
    )


@login_required
def ios_rack_detail(request, pk):
    cliente = _get_cliente(request.user)
    if not cliente and not request.user.is_staff:
        return HttpResponseForbidden("Sem cadastro de cliente.")
    if request.user.is_staff and not cliente:
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
        request.user.is_staff
        or (
            cliente
            and (
                rack.cliente_id == cliente.id
                or (rack.id_planta_id and cliente.plantas.filter(pk=rack.id_planta_id).exists())
            )
        )
    )
    message = None
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
                        modulo_ids = [slot.modulo_id for slot in slots if slot.modulo_id]
                        module_channels = {}
                        if modulo_ids:
                            channels = (
                                CanalRackIO.objects.filter(modulo_id__in=modulo_ids)
                                .select_related("tipo")
                                .order_by("modulo_id", "indice")
                            )
                            for channel in channels:
                                module_channels.setdefault(channel.modulo_id, []).append(
                                    {
                                        "canal": f"{channel.indice:02d}",
                                        "tag": channel.tag or "-",
                                        "tipo": channel.tipo.nome if channel.tipo_id else "-",
                                    }
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
                                "module_channels": module_channels,
                                "message": message,
                                "inventarios": inventarios_qs.order_by("nome"),
                                "locais": locais,
                                "grupos": grupos,
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
                    nome=module_modelo.nome,
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
                    nome=module_modelo.nome,
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
    module_channels = {}
    if modulo_ids:
        channels = (
            CanalRackIO.objects.filter(modulo_id__in=modulo_ids)
            .select_related("tipo")
            .order_by("modulo_id", "indice")
        )
        for channel in channels:
            module_channels.setdefault(channel.modulo_id, []).append(
                {
                    "canal": f"{channel.indice:02d}",
                    "tag": channel.tag or "-",
                    "tipo": channel.tipo.nome if channel.tipo_id else "-",
                }
            )
    modules = (
        ModuloIO.objects.filter(Q(cliente=rack.cliente) | Q(is_default=True))
        .select_related("tipo_base")
        .order_by("nome")
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
            "module_channels": module_channels,
            "message": message,
            "inventarios": inventarios_qs.order_by("nome"),
            "locais": locais,
            "grupos": grupos,
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


@login_required
def ios_rack_io_list(request, pk):
    cliente = _get_cliente(request.user)
    if not cliente and not request.user.is_staff:
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
def ios_modulos(request):
    cliente = _get_cliente(request.user)
    if not cliente and not request.user.is_staff:
        return HttpResponseForbidden("Sem cadastro de cliente.")
    if request.method == "POST":
        action = request.POST.get("action")
        if action == "create_module":
            if not cliente:
                return HttpResponseForbidden("Sem cadastro de cliente.")
            nome = request.POST.get("nome", "").strip()
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
            if nome and quantidade_canais and tipo_id:
                tipo_base = get_object_or_404(TipoCanalIO, pk=tipo_id)
                modulo = ModuloIO.objects.create(
                    cliente=cliente,
                    nome=nome,
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
    cliente = _get_cliente(request.user)
    if not cliente and not request.user.is_staff:
        return HttpResponseForbidden("Sem cadastro de cliente.")
    module_qs = ModuloIO.objects.select_related("tipo_base")
    module = get_object_or_404(module_qs, pk=pk, cliente=cliente) if cliente else get_object_or_404(module_qs, pk=pk)
    if module.is_default and not request.user.is_staff:
        return HttpResponseForbidden("Sem permissao.")
    if request.method == "POST":
        action = request.POST.get("action")
        if action == "update_model":
            nome = request.POST.get("nome", "").strip()
            modelo = request.POST.get("modelo", "").strip()
            marca = request.POST.get("marca", "").strip()
            canais_raw = request.POST.get("quantidade_canais", "").strip()
            tipo_id = request.POST.get("tipo_base")
            try:
                quantidade_canais = int(canais_raw)
            except (TypeError, ValueError):
                quantidade_canais = module.quantidade_canais
            quantidade_canais = max(1, min(512, quantidade_canais))
            if nome:
                module.nome = nome
            module.modelo = modelo
            module.marca = marca
            if tipo_id:
                module.tipo_base_id = tipo_id
            module.quantidade_canais = quantidade_canais
            module.save(update_fields=["nome", "modelo", "marca", "tipo_base", "quantidade_canais"])
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
    cliente = _get_cliente(request.user)
    if not cliente and not request.user.is_staff:
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

    if request.user.is_staff and not cliente:
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
    cliente = _get_cliente(request.user)
    if not cliente and not request.user.is_staff:
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
            if not cliente and not request.user.is_staff:
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
            if not cliente and not request.user.is_staff:
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
    cliente = _get_cliente(request.user)
    if not cliente and not request.user.is_staff:
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
    cliente = _get_cliente(request.user)
    if not cliente and not request.user.is_staff:
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
            if not cliente and not request.user.is_staff:
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
            if not cliente and not request.user.is_staff:
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
    cliente = _get_cliente(request.user)
    if not cliente and not request.user.is_staff:
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
            if not cliente and not request.user.is_staff:
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
            if not cliente and not request.user.is_staff:
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
def listas_ip_list(request):
    cliente = _get_cliente(request.user)
    if not cliente and not request.user.is_staff:
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
                    return redirect("listas_ip_list")

    if request.user.is_staff and not cliente:
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
        },
    )


@login_required
def lista_ip_detail(request, pk):
    cliente = _get_cliente(request.user)
    if not cliente and not request.user.is_staff:
        return HttpResponseForbidden("Sem cadastro de cliente.")

    if request.user.is_staff and not cliente:
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
            if not can_manage and not request.user.is_staff:
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
            "can_manage": can_manage or request.user.is_staff,
            "message": message,
            "message_level": message_level,
            "nomes_repetidos": nomes_repetidos,
            "macs_repetidos": macs_repetidos,
        },
    )


@login_required
def radar_list(request):
    cliente = _get_cliente(request.user)
    if not cliente and not request.user.is_staff:
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

    if request.user.is_staff and not cliente:
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
    cliente = _get_cliente(request.user)
    if not cliente and not request.user.is_staff:
        return HttpResponseForbidden("Sem cadastro de cliente.")

    if request.user.is_staff and not cliente:
        radar = get_object_or_404(Radar, pk=pk)
        is_creator = False
        has_id_radar_access = False
        can_manage = True
    else:
        radar = get_object_or_404(
            Radar,
            Q(pk=pk),
            Q(cliente=cliente) | Q(id_radar__in=cliente.radares.all()),
        )
        is_creator = bool(cliente) and radar.cliente_id == cliente.id
        has_id_radar_access = bool(cliente) and (
            radar.id_radar_id and cliente.radares.filter(pk=radar.id_radar_id).exists()
        )
        can_manage = bool(cliente)

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
            if not can_manage and not request.user.is_staff:
                return HttpResponseForbidden("Sem permissao.")
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
                message = "Informe um nome para o trabalho."
                message_level = "error"
            else:
                contrato = None
                if contrato_id:
                    contrato = RadarContrato.objects.filter(pk=contrato_id).first()
                classificacao = None
                if classificacao_id:
                    classificacao = RadarClassificacao.objects.filter(pk=classificacao_id).first()
                RadarTrabalho.objects.create(
                    radar=radar,
                    nome=nome,
                    descricao=descricao,
                    setor=setor,
                    solicitante=solicitante,
                    responsavel=responsavel,
                    contrato=contrato,
                    data_registro=data_registro or timezone.localdate(),
                    classificacao=classificacao,
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

    trabalhos_base = radar.trabalhos.annotate(total_atividades=Count("atividades")).select_related(
        "classificacao",
        "contrato",
    )
    if classificacao_filter:
        trabalhos_base = trabalhos_base.filter(classificacao_id=classificacao_filter)
    today = timezone.localdate()
    show_all_finalizados = request.GET.get("finalizados") == "all"
    base_params = request.GET.copy()
    base_params.pop("finalizados", None)
    toggle_params = base_params.copy()
    toggle_params["finalizados"] = "all"
    total_trabalhos = trabalhos_base.count()
    trabalhos_execucao = trabalhos_base.filter(status=RadarTrabalho.Status.EXECUTANDO)
    trabalhos_pendentes = trabalhos_base.filter(status=RadarTrabalho.Status.PENDENTE)
    trabalhos_finalizados = trabalhos_base.filter(status=RadarTrabalho.Status.FINALIZADA)
    trabalhos_finalizados_mes = trabalhos_finalizados.filter(
        criado_em__year=today.year,
        criado_em__month=today.month,
    )
    trabalhos_finalizados_antigos = trabalhos_finalizados.exclude(
        criado_em__year=today.year,
        criado_em__month=today.month,
    )
    if show_all_finalizados:
        trabalhos_finalizados_mes = trabalhos_finalizados
    trabalhos_execucao = trabalhos_execucao.order_by("-data_registro", "nome")
    trabalhos_pendentes = trabalhos_pendentes.order_by("-data_registro", "nome")
    trabalhos_finalizados_mes = trabalhos_finalizados_mes.order_by("-data_registro", "nome")
    has_finalizados_antigos = trabalhos_finalizados_antigos.exists()
    return render(
        request,
        "core/radar_detail.html",
        {
            "radar": radar,
            "trabalhos_execucao": trabalhos_execucao,
            "trabalhos_pendentes": trabalhos_pendentes,
            "trabalhos_finalizados": trabalhos_finalizados_mes,
            "show_all_finalizados": show_all_finalizados,
            "has_finalizados_antigos": has_finalizados_antigos,
            "total_trabalhos": total_trabalhos,
            "classificacoes": classificacoes,
            "contratos": RadarContrato.objects.order_by("nome"),
            "classificacao_filter": classificacao_filter,
            "can_manage": can_manage or request.user.is_staff,
            "is_radar_creator": is_creator,
            "has_id_radar_access": has_id_radar_access,
            "message": message,
            "message_level": message_level,
            "open_cadastro": request.GET.get("cadastro", "").strip(),
            "finalizados_toggle_query": toggle_params.urlencode() if toggle_params else "finalizados=all",
            "finalizados_reset_query": base_params.urlencode() if base_params else "",
        },
    )


@login_required
def radar_trabalho_detail(request, radar_pk, pk):
    cliente = _get_cliente(request.user)
    if not cliente and not request.user.is_staff:
        return HttpResponseForbidden("Sem cadastro de cliente.")

    if request.user.is_staff and not cliente:
        radar = get_object_or_404(Radar, pk=radar_pk)
        trabalho = get_object_or_404(RadarTrabalho, pk=pk, radar=radar)
        is_creator = False
        has_id_radar_access = False
        can_manage = True
    else:
        radar = get_object_or_404(
            Radar,
            Q(pk=radar_pk),
            Q(cliente=cliente) | Q(id_radar__in=cliente.radares.all()),
        )
        trabalho = get_object_or_404(RadarTrabalho, pk=pk, radar=radar)
        is_creator = bool(cliente) and radar.cliente_id == cliente.id
        has_id_radar_access = bool(cliente) and (
            radar.id_radar_id and cliente.radares.filter(pk=radar.id_radar_id).exists()
        )
        can_manage = bool(cliente)

    message = request.GET.get("msg", "").strip()
    message_level = request.GET.get("level", "").strip() or "info"
    classificacoes = RadarClassificacao.objects.order_by("nome")
    classificacao_filter = request.GET.get("classificacao", "").strip()

    if request.method == "POST":
        action = request.POST.get("action")
        if action in {
            "create_atividade",
            "update_trabalho",
            "delete_trabalho",
            "duplicate_trabalho",
            "update_atividade",
            "delete_atividade",
            "create_contrato",
            "create_classificacao",
        }:
            if not can_manage and not request.user.is_staff:
                return HttpResponseForbidden("Sem permissao.")
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
            nome = request.POST.get("nome", "").strip()
            descricao = request.POST.get("descricao", "").strip()
            setor = request.POST.get("setor", "").strip()
            solicitante = request.POST.get("solicitante", "").strip()
            responsavel = request.POST.get("responsavel", "").strip()
            data_raw = request.POST.get("data_registro", "").strip()
            classificacao_id = request.POST.get("classificacao")
            contrato_id = request.POST.get("contrato")
            if not nome:
                message = "Informe um nome para o trabalho."
                message_level = "error"
            else:
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
                trabalho.nome = nome
                trabalho.descricao = descricao
                trabalho.setor = setor
                trabalho.solicitante = solicitante
                trabalho.responsavel = responsavel
                trabalho.save(
                    update_fields=[
                        "nome",
                        "descricao",
                        "data_registro",
                        "classificacao",
                        "contrato",
                        "setor",
                        "solicitante",
                        "responsavel",
                    ]
                )
                _sync_trabalho_status(trabalho)
                return redirect("radar_trabalho_detail", radar_pk=radar.pk, pk=trabalho.pk)
        if action == "delete_trabalho":
            trabalho.delete()
            return redirect("radar_detail", pk=radar.pk)
        if action == "duplicate_trabalho":
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
            )
            atividades = list(trabalho.atividades.all())
            if atividades:
                RadarAtividade.objects.bulk_create(
                    [
                        RadarAtividade(
                            trabalho=novo_trabalho,
                            nome=atividade.nome,
                            descricao=atividade.descricao,
                            horas_trabalho=atividade.horas_trabalho,
                            status=atividade.status,
                        )
                        for atividade in atividades
                    ]
                )
            _sync_trabalho_status(novo_trabalho)
            return redirect("radar_trabalho_detail", radar_pk=radar.pk, pk=novo_trabalho.pk)
        if action == "create_atividade":
            nome = request.POST.get("nome", "").strip()
            descricao = request.POST.get("descricao", "").strip()
            horas_raw = request.POST.get("horas_trabalho", "").replace(",", ".").strip()
            status_raw = request.POST.get("status", "").strip()
            if status_raw not in dict(RadarAtividade.Status.choices):
                status_raw = RadarAtividade.Status.PENDENTE
            horas = None
            if horas_raw:
                try:
                    horas = Decimal(horas_raw)
                except InvalidOperation:
                    horas = None
            if not nome:
                message = "Informe um nome para a atividade."
                message_level = "error"
            else:
                RadarAtividade.objects.create(
                    trabalho=trabalho,
                    nome=nome,
                    descricao=descricao,
                    horas_trabalho=horas,
                    status=status_raw,
                )
                _sync_trabalho_status(trabalho)
                return redirect("radar_trabalho_detail", radar_pk=radar.pk, pk=trabalho.pk)
        if action == "update_atividade":
            atividade_id = request.POST.get("atividade_id")
            atividade = get_object_or_404(RadarAtividade, pk=atividade_id, trabalho=trabalho)
            atividade.nome = request.POST.get("nome", "").strip()
            atividade.descricao = request.POST.get("descricao", "").strip()
            horas_raw = request.POST.get("horas_trabalho", "").replace(",", ".").strip()
            if horas_raw:
                try:
                    atividade.horas_trabalho = Decimal(horas_raw)
                except InvalidOperation:
                    atividade.horas_trabalho = None
            else:
                atividade.horas_trabalho = None
            status_raw = request.POST.get("status", "").strip()
            if status_raw in dict(RadarAtividade.Status.choices):
                atividade.status = status_raw
            atividade.save(
                update_fields=[
                    "nome",
                    "descricao",
                    "horas_trabalho",
                    "status",
                ]
            )
            _sync_trabalho_status(trabalho)
            if request.headers.get("x-requested-with") == "XMLHttpRequest":
                return JsonResponse(
                    {
                        "ok": True,
                        "id": atividade.id,
                        "nome": atividade.nome,
                        "descricao": atividade.descricao,
                        "status": atividade.status,
                        "status_label": atividade.get_status_display(),
                        "horas_trabalho": str(atividade.horas_trabalho) if atividade.horas_trabalho else "",
                    }
                )
            return redirect("radar_trabalho_detail", radar_pk=radar.pk, pk=trabalho.pk)
        if action == "delete_atividade":
            atividade_id = request.POST.get("atividade_id")
            atividade = get_object_or_404(RadarAtividade, pk=atividade_id, trabalho=trabalho)
            atividade.delete()
            _sync_trabalho_status(trabalho)
            if request.headers.get("x-requested-with") == "XMLHttpRequest":
                return JsonResponse(
                    {
                        "ok": True,
                        "id": atividade_id,
                    }
                )
            return redirect("radar_trabalho_detail", radar_pk=radar.pk, pk=trabalho.pk)

    contratos = RadarContrato.objects.order_by("nome")
    atividades_base = trabalho.atividades.all()
    today = timezone.localdate()
    show_all_finalizados = request.GET.get("finalizados") == "all"
    base_params = request.GET.copy()
    base_params.pop("finalizados", None)
    toggle_params = base_params.copy()
    toggle_params["finalizados"] = "all"
    atividades_execucao = atividades_base.filter(status=RadarAtividade.Status.EXECUTANDO).order_by("-criado_em")
    atividades_pendentes = atividades_base.filter(status=RadarAtividade.Status.PENDENTE).order_by("-criado_em")
    atividades_finalizadas = atividades_base.filter(status=RadarAtividade.Status.FINALIZADA)
    atividades_finalizadas_mes = atividades_finalizadas.filter(
        criado_em__year=today.year,
        criado_em__month=today.month,
    )
    atividades_finalizadas_antigas = atividades_finalizadas.exclude(
        criado_em__year=today.year,
        criado_em__month=today.month,
    )
    if show_all_finalizados:
        atividades_finalizadas_mes = atividades_finalizadas
    atividades_finalizadas_mes = atividades_finalizadas_mes.order_by("-criado_em")
    has_finalizadas_antigas = atividades_finalizadas_antigas.exists()
    edit_atividade = None
    edit_atividade_id = request.GET.get("editar", "").strip()
    if edit_atividade_id:
        edit_atividade = RadarAtividade.objects.filter(pk=edit_atividade_id, trabalho=trabalho).first()
    total_atividades = atividades_base.count()
    return render(
        request,
        "core/radar_trabalho_detail.html",
        {
            "radar": radar,
            "trabalho": trabalho,
            "atividades_execucao": atividades_execucao,
            "atividades_pendentes": atividades_pendentes,
            "atividades_finalizadas": atividades_finalizadas_mes,
            "show_all_finalizados": show_all_finalizados,
            "has_finalizadas_antigas": has_finalizadas_antigas,
            "total_atividades": total_atividades,
            "contratos": contratos,
            "classificacoes": classificacoes,
            "status_choices": RadarAtividade.Status.choices,
            "can_manage": can_manage or request.user.is_staff,
            "is_radar_creator": is_creator,
            "has_id_radar_access": has_id_radar_access,
            "message": message,
            "message_level": message_level,
            "open_cadastro": request.GET.get("cadastro", "").strip(),
            "edit_atividade": edit_atividade,
            "finalizados_toggle_query": toggle_params.urlencode() if toggle_params else "finalizados=all",
            "finalizados_reset_query": base_params.urlencode() if base_params else "",
        },
    )


@login_required
def ios_rack_modulo_detail(request, pk):
    cliente = _get_cliente(request.user)
    if not cliente and not request.user.is_staff:
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
        request.user.is_staff
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
            nome = request.POST.get("nome", "").strip()
            module.nome = nome
            module.save(update_fields=["nome"])
            return redirect("ios_rack_modulo_detail", pk=module.pk)
        if action == "update_module":
            nome = request.POST.get("nome", "").strip()
            if nome:
                module.nome = nome
                module.save(update_fields=["nome"])
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
    base = Proposta.objects.select_related("cliente", "criada_por")
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
    if tipo == "recebidas":
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
    cutoff = timezone.now() - timedelta(days=90)
    base = _proposta_tipo_qs(user, cliente, tipo)
    return {
        "pendentes": base.filter(
            aprovada__isnull=True,
            valor__gt=0,
            finalizada=False,
        ).count(),
        "em_execucao": base.filter(andamento="EXECUTANDO", finalizada=False).count(),
        "finalizadas_90": base.filter(finalizada=True, finalizada_em__gte=cutoff).count(),
        "total": base.count(),
    }


@login_required
def proposta_list(request):
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
    cliente = _get_cliente(request.user)
    if cliente:
        proposta_qs = Proposta.objects.filter(Q(criada_por=request.user) | Q(cliente=cliente))
        proposta = get_object_or_404(proposta_qs, pk=pk)
    else:
        proposta = get_object_or_404(Proposta, pk=pk, criada_por=request.user)
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
            descricao = request.POST.get("descricao", "").strip()
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
    if proposta.finalizada:
        status_label = "Finalizada"
    elif proposta.andamento == "EXECUTANDO":
        status_label = "Executando"
    elif proposta.aprovada is True:
        status_label = "Aprovada"
    elif proposta.aprovada is False:
        status_label = "Reprovada"
    elif proposta.valor is None or proposta.valor == 0:
        status_label = "Levantamento"
    else:
        status_label = "Pendente"
    return render(
        request,
        "core/proposta_detail.html",
        {
            "cliente": cliente,
            "proposta": proposta,
            "message": message,
            "status_label": status_label,
        },
    )


@login_required
def proposta_nova_vendedor(request):
    message = None
    form_data = {
        "email": "",
        "nome": "",
        "descricao": "",
        "valor": "",
        "prioridade": "50",
        "codigo": "",
        "observacao": "",
    }
    if request.method == "POST":
        email = request.POST.get("email", "").strip().lower()
        nome = request.POST.get("nome", "").strip()
        descricao = request.POST.get("descricao", "").strip()
        valor_raw = request.POST.get("valor", "").replace(",", ".").strip()
        prioridade_raw = request.POST.get("prioridade", "").strip()
        codigo = request.POST.get("codigo", "").strip()
        observacao = request.POST.get("observacao", "").strip()
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
        }

        destinatario = PerfilUsuario.objects.filter(email__iexact=email).first() if email else None
        if not destinatario:
            message = "Usuario nao encontrado para este email."
        else:
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
        },
    )


@login_required
@require_POST
def aprovar_proposta(request, pk):
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
    if not request.user.is_staff:
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
    if not request.user.is_staff:
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
            return redirect("usuarios_gerenciar_usuario", pk=user.pk)
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
        "core/usuarios_gerenciar_usuario.html",
        {
            "user_item": user,
            "perfil": perfil,
            "tipos": TipoPerfil.objects.order_by("nome"),
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
    cliente = _get_cliente(request.user)
    if not cliente and not request.user.is_staff:
        return HttpResponseForbidden("Sem cadastro de cliente.")
    if cliente:
        cadernos = Caderno.objects.filter(Q(criador=cliente) | Q(id_financeiro__in=cliente.financeiros.all()))
    else:
        cadernos = Caderno.objects.none()
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
    if cliente:
        compras_qs = Compra.objects.filter(
            Q(caderno__criador=cliente) | Q(caderno__id_financeiro__in=cliente.financeiros.all())
        )
        total_geral = compras_qs.aggregate(total=Sum(item_expr)).get("total")
        ultimas_compras = compras_qs.prefetch_related("itens").order_by("-data")[:6]
    else:
        total_geral = None
        ultimas_compras = Compra.objects.none()

    caderno_id = request.GET.get("caderno_id")
    compras = Compra.objects.none()
    if cliente and caderno_id:
        compras = Compra.objects.filter(
            Q(caderno_id=caderno_id),
            Q(caderno__criador=cliente) | Q(caderno__id_financeiro__in=cliente.financeiros.all()),
        ).order_by("-data")
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
    cliente = _get_cliente(request.user)
    if not cliente and not request.user.is_staff:
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
            if not cliente:
                return HttpResponseForbidden("Sem cadastro de cliente.")
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
            allowed_cadernos = Caderno.objects.filter(
                Q(criador=cliente) | Q(id_financeiro__in=cliente.financeiros.all())
            )
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

    if cliente:
        cadernos = Caderno.objects.filter(Q(criador=cliente) | Q(id_financeiro__in=cliente.financeiros.all()))
    else:
        cadernos = Caderno.objects.none()
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
        if not request.user.is_staff:
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
    cliente = _get_cliente(request.user)
    if not cliente and not request.user.is_staff:
        return HttpResponseForbidden("Sem cadastro de cliente.")

    message = request.GET.get("msg", "").strip()
    message_level = request.GET.get("level", "").strip() or "info"
    open_cadastro = request.GET.get("cadastro", "").strip()
    if request.method == "POST":
        action = request.POST.get("action")
        if action == "create_caderno":
            nome = request.POST.get("nome", "").strip()
            id_financeiro_raw = request.POST.get("id_financeiro", "").strip()
            if nome and cliente:
                financeiro = None
                if id_financeiro_raw:
                    financeiro, _ = FinanceiroID.objects.get_or_create(codigo=id_financeiro_raw.upper())
                Caderno.objects.create(nome=nome, ativo=True, id_financeiro=financeiro, criador=cliente)
            return redirect("financeiro_cadernos")
        if action == "toggle_caderno":
            caderno_id = request.POST.get("caderno_id")
            caderno = get_object_or_404(
                Caderno,
                Q(pk=caderno_id),
                Q(criador=cliente) | Q(id_financeiro__in=cliente.financeiros.all()),
            )
            caderno.ativo = not caderno.ativo
            caderno.save(update_fields=["ativo"])
            return redirect("financeiro_cadernos")
        if action == "delete_caderno":
            caderno_id = request.POST.get("caderno_id")
            caderno = get_object_or_404(
                Caderno,
                Q(pk=caderno_id),
                Q(criador=cliente) | Q(id_financeiro__in=cliente.financeiros.all()),
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
        Caderno.objects.filter(Q(criador=cliente) | Q(id_financeiro__in=cliente.financeiros.all()))
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
    cliente = _get_cliente(request.user)
    if not cliente and not request.user.is_staff:
        return HttpResponseForbidden("Sem cadastro de cliente.")
    caderno = get_object_or_404(
        Caderno,
        Q(pk=pk),
        Q(criador=cliente) | Q(id_financeiro__in=cliente.financeiros.all()),
    )
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

    status_filter = request.GET.get("status", "").strip().lower()
    categoria_filter = request.GET.get("categoria", "").strip()
    centro_filter = request.GET.get("centro", "").strip()
    search_query = request.GET.get("q", "").strip()
    query_params = {}
    if status_filter:
        query_params["status"] = status_filter
    if categoria_filter:
        query_params["categoria"] = categoria_filter
    if centro_filter:
        query_params["centro"] = centro_filter
    if search_query:
        query_params["q"] = search_query
    month_query = urlencode(query_params)

    compras_base_qs = (
        Compra.objects.filter(caderno=caderno)
        .select_related("categoria", "centro_custo")
        .prefetch_related("itens")
        .order_by("-data", "-id")
    )
    if categoria_filter:
        compras_base_qs = compras_base_qs.filter(categoria_id=categoria_filter)
    if centro_filter:
        compras_base_qs = compras_base_qs.filter(centro_custo_id=centro_filter)

    compras_qs = compras_base_qs.filter(data__gte=start_date, data__lt=end_date)
    compras_sem_data_qs = compras_base_qs.filter(data__isnull=True)

    compras = []
    compras_sem_data = []
    total_mes = Decimal(0)
    total_pago = Decimal(0)
    total_pendente = Decimal(0)
    total_compras = 0
    total_pagas = 0
    total_pendentes = 0

    for compra in compras_qs:
        itens = list(compra.itens.all())
        compra.status_label = _compra_status_label(compra)
        compra.total_itens = sum(
            (item.valor or Decimal(0)) * (item.quantidade or 0) for item in itens
        )
        compra.total_pago = sum(
            (item.valor or Decimal(0)) * (item.quantidade or 0) for item in itens if item.pago
        )
        compra.total_pendente = compra.total_itens - compra.total_pago
        compra.itens_count = len(itens)
        if status_filter == "pago" and compra.status_label.lower() != "pago":
            continue
        if status_filter == "pendente" and compra.status_label.lower() != "pendente":
            continue
        compras.append(compra)
        total_mes += compra.total_itens
        total_pago += compra.total_pago
        total_pendente += compra.total_pendente
        total_compras += 1
        if compra.status_label.lower() == "pago":
            total_pagas += 1
        else:
            total_pendentes += 1
    ticket_medio = total_mes / total_compras if total_compras else Decimal(0)

    for compra in compras_sem_data_qs:
        itens = list(compra.itens.all())
        compra.status_label = _compra_status_label(compra)
        if status_filter == "pago" and compra.status_label.lower() != "pago":
            continue
        if status_filter == "pendente" and compra.status_label.lower() != "pendente":
            continue
        compra.total_itens = sum(
            (item.valor or Decimal(0)) * (item.quantidade or 0) for item in itens
        )
        compra.total_pago = sum(
            (item.valor or Decimal(0)) * (item.quantidade or 0) for item in itens if item.pago
        )
        compra.total_pendente = compra.total_itens - compra.total_pago
        compra.itens_count = len(itens)
        compras_sem_data.append(compra)

    categorias = CategoriaCompra.objects.order_by("nome")
    centros = CentroCusto.objects.order_by("nome")
    if search_query:
        compras = [
            compra
            for compra in compras
            if search_query.lower()
            in (
                (compra.nome or compra.descricao or "")
                .strip()
                .lower()
            )
        ]
        compras_sem_data = [
            compra
            for compra in compras_sem_data
            if search_query.lower()
            in (
                (compra.nome or compra.descricao or "")
                .strip()
                .lower()
            )
        ]
    return render(
        request,
        "core/financeiro_caderno_detail.html",
        {
            "caderno": caderno,
            "compras": compras,
            "compras_sem_data": compras_sem_data,
            "selected_month": selected_month,
            "mes_referencia": start_date,
            "prev_month": prev_month,
            "next_month": next_month,
            "current_month": current_month,
            "month_query": month_query,
            "status_filter": status_filter,
            "categoria_filter": categoria_filter,
            "centro_filter": centro_filter,
            "search_query": search_query,
            "categorias": categorias,
            "centros": centros,
            "resumo": {
                "total_mes": total_mes,
                "total_pago": total_pago,
                "total_pendente": total_pendente,
                "total_compras": total_compras,
                "total_pagas": total_pagas,
                "total_pendentes": total_pendentes,
                "ticket_medio": ticket_medio,
            },
        },
    )


@login_required
def financeiro_compra_detail(request, pk):
    cliente = _get_cliente(request.user)
    if not cliente and not request.user.is_staff:
        return HttpResponseForbidden("Sem cadastro de cliente.")
    compra = get_object_or_404(
        Compra,
        Q(pk=pk),
        Q(caderno__criador=cliente) | Q(caderno__id_financeiro__in=cliente.financeiros.all()),
    )
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
            allowed_cadernos = Caderno.objects.filter(
                Q(criador=cliente) | Q(id_financeiro__in=cliente.financeiros.all())
            )
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
            item = get_object_or_404(CompraItem, pk=item_id, compra=compra)
            item.pago = not item.pago
            item.save(update_fields=["pago"])
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
    for item in itens:
        item.total_valor = (item.valor or 0) * (item.quantidade or 0)
    compra.total_itens = sum(item.total_valor for item in itens)
    tipos = TipoCompra.objects.order_by("nome")
    categorias = CategoriaCompra.objects.order_by("nome")
    centros = CentroCusto.objects.order_by("nome")
    cadernos = Caderno.objects.filter(Q(criador=cliente) | Q(id_financeiro__in=cliente.financeiros.all())).order_by("nome")
    return render(
        request,
        "core/financeiro_compra_detail.html",
        {
            "compra": compra,
            "itens": itens,
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
    if not request.user.is_staff:
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
def admin_logs(request):
    if not request.user.is_staff:
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
    if not request.user.is_staff:
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
    if not request.user.is_staff:
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
    if not request.user.is_staff:
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

    if sort_by not in columns:
        sort_by = "id" if "id" in columns else columns[0]

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
    qn = connections["default"].ops.quote_name

    def resolve_column_sql(col_name):
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
            payload_obj = payload_value if isinstance(payload_value, dict) else {}
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
    if not request.user.is_staff:
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
    if not request.user.is_staff:
        return HttpResponseForbidden("Sem permissao.")
    message = None
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
    channel_types = TipoCanalIO.objects.filter(ativo=True).order_by("nome")
    tipos_ativos = TipoAtivo.objects.order_by("nome")
    return render(
        request,
        "core/ajustes.html",
        {
            "message": message,
            "channel_types": channel_types,
            "tipos_ativos": tipos_ativos,
        },
    )
