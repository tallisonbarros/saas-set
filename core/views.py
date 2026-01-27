import calendar
import json
import os
import ipaddress
import re
from datetime import date, datetime
from decimal import Decimal, InvalidOperation

from django.contrib.auth import authenticate, login, logout
from django.contrib.auth.decorators import login_required
from django.http import HttpResponseForbidden, HttpResponseNotAllowed, JsonResponse
from django.shortcuts import get_object_or_404, redirect, render
from django.urls import reverse
from urllib.parse import urlencode
from django.utils import timezone
from django.views.decorators.http import require_POST
from django.views.decorators.csrf import csrf_exempt

from django.contrib.auth.models import User
from django.db.models import Case, Count, DecimalField, F, IntegerField, OuterRef, Q, Subquery, Sum, Value, When
from django.db.models.expressions import ExpressionWrapper

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


def _clean_tag_prefix(value):
    value = re.sub(r"[^0-9A-Za-z]", "", (value or "").strip().upper())
    return value[:3] if value else ""


def _clean_app_slug(value):
    value = re.sub(r"[^0-9A-Za-z_-]", "", (value or "").strip().lower())
    value = value.replace(" ", "_")
    return value[:60]


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


def _create_grupo_payload(request):
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
    grupo, created = GrupoRackIO.objects.get_or_create(nome=nome)
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
    auth_header = request.headers.get("Authorization", "").strip()
    if not expected_token or auth_header != f"Bearer {expected_token}":
        return JsonResponse({"ok": False, "error": "unauthorized"}, status=401)
    try:
        raw_body = request.body.decode("utf-8") if request.body else ""
        payload = json.loads(raw_body or "[]")
    except json.JSONDecodeError:
        return JsonResponse({"ok": False, "error": "invalid_json"}, status=400)
    if not isinstance(payload, list):
        return JsonResponse({"ok": False, "error": "invalid_payload"}, status=400)
    items_by_source = {}
    for item in payload:
        if not isinstance(item, dict):
            return JsonResponse({"ok": False, "error": "invalid_payload"}, status=400)
        source_id = str(item.get("source_id", "")).strip()
        client_id = str(item.get("client_id", "")).strip()
        agent_id = str(item.get("agent_id", "")).strip()
        source = str(item.get("source", "")).strip()
        if not source_id or not client_id or not agent_id or not source:
            return JsonResponse({"ok": False, "error": "invalid_payload"}, status=400)
        payload_data = item.get("payload", None)
        if isinstance(payload_data, str):
            try:
                payload_data = json.loads(payload_data)
            except json.JSONDecodeError:
                return JsonResponse({"ok": False, "error": "invalid_payload"}, status=400)
        if payload_data is None:
            return JsonResponse({"ok": False, "error": "invalid_payload"}, status=400)
        items_by_source[source_id] = {
            "client_id": client_id,
            "agent_id": agent_id,
            "source": source,
            "payload": payload_data,
        }
    if items_by_source:
        existing_records = IngestRecord.objects.filter(source_id__in=items_by_source.keys())
        existing_by_source = {record.source_id: record for record in existing_records}
        to_update = []
        to_create = []
        for source_id, data in items_by_source.items():
            existing = existing_by_source.get(source_id)
            if existing:
                existing.client_id = data["client_id"]
                existing.agent_id = data["agent_id"]
                existing.source = data["source"]
                existing.payload = data["payload"]
                to_update.append(existing)
            else:
                to_create.append(IngestRecord(source_id=source_id, **data))
        if to_update:
            IngestRecord.objects.bulk_update(to_update, ["client_id", "agent_id", "source", "payload"])
        if to_create:
            IngestRecord.objects.bulk_create(to_create)
    return JsonResponse({"ok": True, "count": len(payload)})


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
    if request.method == "POST" and request.POST.get("action") == "clear_ingest":
        IngestRecord.objects.all().delete()
        return redirect("planta_conectada")
    registros = IngestRecord.objects.all().order_by("-created_at")[:200]
    return render(
        request,
        "core/planta_conectada.html",
        {
            "registros": registros,
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
            slug = _clean_app_slug(slug_raw or nome)
            if not nome or not slug:
                message = "Informe nome e slug valido."
                message_level = "error"
            else:
                app, created = App.objects.get_or_create(
                    slug=slug,
                    defaults={
                        "nome": nome,
                        "descricao": descricao,
                        "icon": icon,
                        "theme_color": theme_color,
                        "ativo": True,
                    },
                )
                if not created:
                    app.nome = nome
                    app.descricao = descricao
                    app.icon = icon
                    app.theme_color = theme_color
                    app.save(update_fields=["nome", "descricao", "icon", "theme_color"])
                return redirect("apps_gerenciar")
        if action == "update_app":
            app_id = request.POST.get("app_id")
            app = App.objects.filter(pk=app_id).first()
            if app:
                nome = request.POST.get("nome", "").strip()
                descricao = request.POST.get("descricao", "").strip()
                icon = request.POST.get("icon", "").strip()
                theme_color = request.POST.get("theme_color", "").strip()
                if nome:
                    app.nome = nome
                app.descricao = descricao
                app.icon = icon
                app.theme_color = theme_color
                app.save(update_fields=["nome", "descricao", "icon", "theme_color"])
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
    locais = LocalRackIO.objects.order_by("nome")
    grupos = GrupoRackIO.objects.order_by("nome")
    grupos = GrupoRackIO.objects.order_by("nome")
    grupos = GrupoRackIO.objects.order_by("nome")
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
                if local_id and LocalRackIO.objects.filter(pk=local_id).exists():
                    local = LocalRackIO.objects.filter(pk=local_id).first()
                grupo = None
                if grupo_id and GrupoRackIO.objects.filter(pk=grupo_id).exists():
                    grupo = GrupoRackIO.objects.filter(pk=grupo_id).first()
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
            nome = request.POST.get("local_nome", "").strip()
            if not nome:
                msg = "Informe um nome de local."
                level = "error"
                created = False
            else:
                local, created = LocalRackIO.objects.get_or_create(nome=nome)
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
            payload = _create_grupo_payload(request)
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
        local_key = rack.local_id or 0
        grupo_key = rack.grupo_id or 0
        grouped.setdefault(local_key, {}).setdefault(grupo_key, []).append(rack)
    for local_key, groups in grouped.items():
        local = None
        if local_key:
            local = next(iter(groups.values()))[0].local
        group_rows = []
        for grupo_key, items in groups.items():
            grupo = None
            if grupo_key:
                grupo = items[0].grupo
            group_rows.append(
                {
                    "grupo": grupo,
                    "racks": items,
                }
            )
        rack_groups.append(
            {
                "local": local,
                "groups": group_rows,
            }
        )
    channel_types = TipoCanalIO.objects.filter(ativo=True).order_by("nome")
    locais = LocalRackIO.objects.order_by("nome")
    grupos = GrupoRackIO.objects.order_by("nome")
    grupos = GrupoRackIO.objects.order_by("nome")
    search_term = request.GET.get("q", "").strip()
    search_results = []
    search_count = 0
    if search_term:
        slot_pos_subquery = RackSlotIO.objects.filter(modulo_id=OuterRef("modulo_id")).values("posicao")[:1]
        search_filter = (
            Q(nome__icontains=search_term)
            | Q(modulo__nome__icontains=search_term)
            | Q(modulo__modulo_modelo__nome__icontains=search_term)
            | Q(modulo__rack__nome__icontains=search_term)
        )
        channels = (
            CanalRackIO.objects.filter(modulo__rack__in=racks)
            .filter(search_filter)
            .select_related("modulo", "modulo__rack", "modulo__modulo_modelo", "tipo")
            .annotate(slot_pos=Subquery(slot_pos_subquery))
            .order_by("modulo__rack__nome", "slot_pos", "indice")[:200]
        )
        search_results = list(channels)
        search_count = len(search_results)
    return render(
        request,
        "core/ios_list.html",
        {
            "racks": racks,
            "rack_groups": rack_groups,
            "channel_types": channel_types,
            "can_manage": bool(cliente),
            "search_term": search_term,
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
    locais = LocalRackIO.objects.order_by("nome")
    grupos = GrupoRackIO.objects.order_by("nome")
    if cliente:
        rack = get_object_or_404(
            RackIO,
            Q(pk=pk),
            Q(cliente=cliente) | Q(id_planta__in=cliente.plantas.all()),
        )
    else:
        rack = get_object_or_404(RackIO, pk=pk)
    message = None
    if request.method == "POST":
        action = request.POST.get("action")
        if action == "create_local":
            nome = request.POST.get("local_nome", "").strip()
            if not nome:
                msg = "Informe um nome de local."
                level = "error"
                created = False
            else:
                local, created = LocalRackIO.objects.get_or_create(nome=nome)
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
            payload = _create_grupo_payload(request)
            if request.headers.get("x-requested-with") == "XMLHttpRequest":
                return JsonResponse(payload)
            return redirect("ios_rack_detail", pk=rack.pk)
        if action == "update_rack":
            if not request.user.is_staff and rack.cliente != cliente:
                return HttpResponseForbidden("Sem permissao.")
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
            if nome:
                rack.nome = nome
            rack.descricao = descricao
            if local_id and LocalRackIO.objects.filter(pk=local_id).exists():
                rack.local = LocalRackIO.objects.filter(pk=local_id).first()
            else:
                rack.local = None
            if grupo_id and GrupoRackIO.objects.filter(pk=grupo_id).exists():
                rack.grupo = GrupoRackIO.objects.filter(pk=grupo_id).first()
            else:
                rack.grupo = None
            if id_planta_raw:
                planta, _ = PlantaIO.objects.get_or_create(codigo=id_planta_raw.upper())
                rack.id_planta = planta
            else:
                rack.id_planta = None
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
                                        "nome": channel.nome or "-",
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
            rack.save(update_fields=["nome", "descricao", "local", "grupo", "id_planta", "slots_total", "inventario"])
            return redirect("ios_rack_detail", pk=rack.pk)
        if action == "delete_rack":
            if not request.user.is_staff and rack.cliente != cliente:
                return HttpResponseForbidden("Sem permissao.")
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
                        nome="",
                        tipo=module_modelo.tipo_base,
                    )
                    for index in range(1, module_modelo.quantidade_canais + 1)
                ]
                CanalRackIO.objects.bulk_create(canais)
                slot.modulo = modulo
                slot.save(update_fields=["modulo"])
            return redirect("ios_rack_detail", pk=rack.pk)
        if action == "assign_modules":
            if not request.user.is_staff and rack.cliente != cliente:
                return HttpResponseForbidden("Sem permissao.")
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
                        nome="",
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
                    "nome": channel.nome or "-",
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
        .filter(Q(nome__isnull=True) | Q(nome__exact=""))
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
        if action == "update_item":
            item_id = request.POST.get("item_id")
            item = get_object_or_404(ListaIPItem, pk=item_id, lista=lista)
            item.nome_equipamento = request.POST.get("nome_equipamento", "").strip()
            item.mac = request.POST.get("mac", "").strip()
            item.protocolo = request.POST.get("protocolo", "").strip()
            item.save(update_fields=["nome_equipamento", "mac", "protocolo"])
            return redirect("lista_ip_detail", pk=lista.pk)

    search_term = request.GET.get("q", "").strip()
    items = ListaIPItem.objects.filter(lista=lista)
    if search_term:
        items = items.filter(
            Q(ip__icontains=search_term)
            | Q(nome_equipamento__icontains=search_term)
            | Q(mac__icontains=search_term)
            | Q(protocolo__icontains=search_term)
        )
    items = list(items)
    items.sort(key=lambda item: ipaddress.ip_address(item.ip))
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
        if action in {"create_trabalho", "update_radar", "delete_radar", "create_classificacao"}:
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
        if action == "create_trabalho":
            nome = request.POST.get("nome", "").strip()
            descricao = request.POST.get("descricao", "").strip()
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
                classificacao = None
                if classificacao_id:
                    classificacao = RadarClassificacao.objects.filter(pk=classificacao_id).first()
                RadarTrabalho.objects.create(
                    radar=radar,
                    nome=nome,
                    descricao=descricao,
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

    trabalhos_execucao = radar.trabalhos.filter(status=RadarTrabalho.Status.EXECUTANDO)
    trabalhos_execucao = trabalhos_execucao.annotate(total_atividades=Count("atividades")).select_related(
        "classificacao"
    )
    trabalhos = radar.trabalhos.annotate(total_atividades=Count("atividades")).select_related("classificacao")
    if classificacao_filter:
        trabalhos_execucao = trabalhos_execucao.filter(classificacao_id=classificacao_filter)
        trabalhos = trabalhos.filter(classificacao_id=classificacao_filter)
    trabalhos = trabalhos.order_by("-data_registro", "nome")
    trabalhos_execucao = trabalhos_execucao.order_by("-data_registro", "nome")
    return render(
        request,
        "core/radar_detail.html",
        {
            "radar": radar,
            "trabalhos": trabalhos,
            "trabalhos_execucao": trabalhos_execucao,
            "classificacoes": classificacoes,
            "classificacao_filter": classificacao_filter,
            "can_manage": can_manage or request.user.is_staff,
            "is_radar_creator": is_creator,
            "has_id_radar_access": has_id_radar_access,
            "message": message,
            "message_level": message_level,
            "open_cadastro": request.GET.get("cadastro", "").strip(),
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
            data_raw = request.POST.get("data_registro", "").strip()
            classificacao_id = request.POST.get("classificacao")
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
                trabalho.nome = nome
                trabalho.descricao = descricao
                trabalho.save(update_fields=["nome", "descricao", "data_registro", "classificacao"])
                _sync_trabalho_status(trabalho)
                return redirect("radar_trabalho_detail", radar_pk=radar.pk, pk=trabalho.pk)
        if action == "delete_trabalho":
            trabalho.delete()
            return redirect("radar_detail", pk=radar.pk)
        if action == "create_atividade":
            nome = request.POST.get("nome", "").strip()
            descricao = request.POST.get("descricao", "").strip()
            setor = request.POST.get("setor", "").strip()
            solicitante = request.POST.get("solicitante", "").strip()
            responsavel = request.POST.get("responsavel", "").strip()
            contrato_id = request.POST.get("contrato")
            classificacao_id = request.POST.get("classificacao")
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
                contrato = None
                if contrato_id:
                    contrato = RadarContrato.objects.filter(pk=contrato_id).first()
                classificacao = None
                if classificacao_id:
                    classificacao = RadarClassificacao.objects.filter(pk=classificacao_id).first()
                RadarAtividade.objects.create(
                    trabalho=trabalho,
                    nome=nome,
                    descricao=descricao,
                    setor=setor,
                    solicitante=solicitante,
                    responsavel=responsavel,
                    contrato=contrato,
                    classificacao=classificacao,
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
            atividade.setor = request.POST.get("setor", "").strip()
            atividade.solicitante = request.POST.get("solicitante", "").strip()
            atividade.responsavel = request.POST.get("responsavel", "").strip()
            contrato_id = request.POST.get("contrato")
            atividade.contrato = RadarContrato.objects.filter(pk=contrato_id).first() if contrato_id else None
            classificacao_id = request.POST.get("classificacao")
            atividade.classificacao = (
                RadarClassificacao.objects.filter(pk=classificacao_id).first() if classificacao_id else None
            )
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
                    "setor",
                    "solicitante",
                    "responsavel",
                    "contrato",
                    "classificacao",
                    "horas_trabalho",
                    "status",
                ]
            )
            _sync_trabalho_status(trabalho)
            return redirect("radar_trabalho_detail", radar_pk=radar.pk, pk=trabalho.pk)
        if action == "delete_atividade":
            atividade_id = request.POST.get("atividade_id")
            atividade = get_object_or_404(RadarAtividade, pk=atividade_id, trabalho=trabalho)
            atividade.delete()
            _sync_trabalho_status(trabalho)
            return redirect("radar_trabalho_detail", radar_pk=radar.pk, pk=trabalho.pk)

    contratos = RadarContrato.objects.order_by("nome")
    atividades = trabalho.atividades.select_related("contrato", "classificacao")
    if classificacao_filter:
        atividades = atividades.filter(classificacao_id=classificacao_filter)
    atividades = atividades.order_by("-criado_em")
    total_atividades = atividades.count()
    return render(
        request,
        "core/radar_trabalho_detail.html",
        {
            "radar": radar,
            "trabalho": trabalho,
            "atividades": atividades,
            "total_atividades": total_atividades,
            "contratos": contratos,
            "classificacoes": classificacoes,
            "classificacao_filter": classificacao_filter,
            "status_choices": RadarAtividade.Status.choices,
            "can_manage": can_manage or request.user.is_staff,
            "is_radar_creator": is_creator,
            "has_id_radar_access": has_id_radar_access,
            "message": message,
            "message_level": message_level,
            "open_cadastro": request.GET.get("cadastro", "").strip(),
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
                nome_raw = request.POST.get(f"nome_{channel.id}")
                tipo_id = request.POST.get(f"tipo_{channel.id}")
                comissionado = request.POST.get(f"comissionado_{channel.id}") == "on"
                vinculo_raw = request.POST.get(f"vinculo_{channel.id}", "").strip()
                if nome_raw is None:
                    continue
                channel.nome = nome_raw.strip()
                if tipo_id:
                    channel.tipo_id = tipo_id
                channel.comissionado = comissionado
                channel.ativo_id = None
                channel.ativo_item_id = None
                if vinculo_raw:
                    ativo_match = Ativo.objects.filter(tag_set__iexact=vinculo_raw).first()
                    item_match = AtivoItem.objects.filter(tag_set__iexact=vinculo_raw).first()
                    if item_match:
                        channel.ativo_item_id = item_match.id
                        channel.ativo_id = item_match.ativo_id
                    elif ativo_match:
                        channel.ativo_id = ativo_match.id
                channel.save(update_fields=["nome", "tipo_id", "comissionado", "ativo_id", "ativo_item_id"])
            return redirect("ios_rack_modulo_detail", pk=module.pk)
    channels = module.canais.select_related("tipo", "ativo", "ativo_item").order_by("indice")
    channel_types = TipoCanalIO.objects.filter(ativo=True).order_by("nome")
    cliente = _get_cliente(request.user)
    if request.user.is_staff and not cliente:
        inventarios_qs = Inventario.objects.all()
    else:
        inventarios_qs = Inventario.objects.filter(
            Q(cliente=cliente) | Q(id_inventario__in=cliente.inventarios.all())
        )
    if module.rack.inventario_id:
        ativos_qs = Ativo.objects.filter(inventario=module.rack.inventario)
    else:
        ativos_qs = Ativo.objects.filter(inventario__in=inventarios_qs)
    itens_qs = AtivoItem.objects.filter(ativo__in=ativos_qs).select_related("ativo")
    vacant_slots = RackSlotIO.objects.filter(rack=module.rack, modulo__isnull=True).order_by("posicao")
    return render(
        request,
        "core/ios_modulo_detail.html",
        {
            "module": module,
            "channels": channels,
            "channel_types": channel_types,
            "ativos": ativos_qs.order_by("nome"),
            "itens": itens_qs.order_by("ativo__nome", "nome"),
            "rack": module.rack,
            "slot": slot,
            "vacant_slots": vacant_slots,
            "has_vacant_slots": vacant_slots.exists(),
            "prev_slot": prev_slot,
            "next_slot": next_slot,
        },
    )


@login_required
def proposta_list(request):
    cliente = _get_cliente(request.user)
    propostas = Proposta.objects.none()
    if cliente:
        propostas = (
            Proposta.objects.filter(Q(criada_por=request.user) | Q(cliente=cliente))
            .distinct()
            .order_by("-criado_em")
        )
    else:
        propostas = Proposta.objects.filter(criada_por=request.user).order_by("-criado_em")
    status = request.GET.get("status")
    if status == "pendente":
        propostas = propostas.filter(aprovada__isnull=True).exclude(valor=0)
    elif status == "levantamento":
        propostas = propostas.filter(aprovada__isnull=True, valor=0)
    elif status == "aprovada":
        propostas = propostas.filter(aprovada=True)
    elif status == "reprovada":
        propostas = propostas.filter(aprovada=False)
    elif status == "finalizada":
        propostas = propostas.filter(finalizada=True)
    else:
        propostas = propostas.exclude(aprovada=False).exclude(finalizada=True)
    propostas = propostas.annotate(
        status_order=Case(
            When(finalizada=True, then=Value(6)),
            When(andamento="EXECUTANDO", then=Value(4)),
            When(aprovada=True, then=Value(3)),
            When(aprovada=False, then=Value(5)),
            When(aprovada__isnull=True, valor=0, then=Value(1)),
            default=Value(2),
            output_field=IntegerField(),
        )
    ).order_by("status_order", "-criado_em")
    propostas = propostas.annotate(
        status_label=Case(
            When(andamento="EXECUTANDO", then=Value("Executando")),
            When(aprovada__isnull=True, valor=0, then=Value("Levantamento")),
            When(aprovada=True, then=Value("Aprovada")),
            When(aprovada=False, then=Value("Reprovada")),
            default=Value("Pendente"),
        )
    )
    propostas_para_aprovar = Proposta.objects.none()
    propostas_restantes = propostas
    if not status:
        propostas_para_aprovar = propostas.filter(
            aprovada__isnull=True,
            valor__gt=0,
            cliente__usuario=request.user,
        )
        propostas_restantes = propostas.exclude(pk__in=propostas_para_aprovar.values("pk"))
    pendencias_total = propostas_para_aprovar.count()
    propostas_recebidas = []
    propostas_enviadas = []
    recebidas_map = {}
    enviadas_map = {}
    for proposta in propostas_restantes:
        if proposta.criada_por_id == request.user.id:
            destinatario = proposta.cliente.nome if proposta.cliente else ""
            if not destinatario:
                destinatario = proposta.cliente.email if proposta.cliente else "Destino"
            if destinatario not in enviadas_map:
                enviadas_map[destinatario] = []
            enviadas_map[destinatario].append(proposta)
        else:
            remetente = proposta.criada_por.username if proposta.criada_por else "Sistema"
            if remetente not in recebidas_map:
                recebidas_map[remetente] = []
            recebidas_map[remetente].append(proposta)
    propostas_recebidas = [{"nome": key, "propostas": value} for key, value in recebidas_map.items()]
    propostas_enviadas = [{"nome": key, "propostas": value} for key, value in enviadas_map.items()]
    return render(
        request,
        "core/proposta_list.html",
        {
            "cliente": cliente,
            "propostas": propostas,
            "propostas_para_aprovar": propostas_para_aprovar,
            "propostas_restantes": propostas_restantes,
            "propostas_recebidas": propostas_recebidas,
            "propostas_enviadas": propostas_enviadas,
            "pendencias_total": pendencias_total,
            "status_filter": status,
            "is_vendedor": True,
            "current_user_id": request.user.id,
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
                try:
                    valor = Decimal(valor_raw)
                except (InvalidOperation, ValueError):
                    valor = None
                if valor is None:
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
                proposta.save(update_fields=["finalizada"])
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
    elif proposta.valor == 0:
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
    form_data = {"email": "", "nome": "", "descricao": "", "valor": "", "prioridade": "50"}
    if request.method == "POST":
        email = request.POST.get("email", "").strip().lower()
        nome = request.POST.get("nome", "").strip()
        descricao = request.POST.get("descricao", "").strip()
        valor_raw = request.POST.get("valor", "").replace(",", ".").strip()
        prioridade_raw = request.POST.get("prioridade", "").strip()
        form_data = {
            "email": email,
            "nome": nome,
            "descricao": descricao,
            "valor": valor_raw,
            "prioridade": prioridade_raw or "50",
        }

        destinatario = PerfilUsuario.objects.filter(email__iexact=email).first() if email else None
        if not destinatario:
            message = "Usuario nao encontrado para este email."
        else:
            try:
                valor = Decimal(valor_raw)
            except (InvalidOperation, ValueError):
                valor = None
            try:
                prioridade = int(prioridade_raw) if prioridade_raw else 50
            except ValueError:
                prioridade = 50
            prioridade = max(1, min(99, prioridade))
            if not nome or not descricao or valor is None:
                message = "Preencha nome, descricao e valor valido."
            else:
                proposta = Proposta.objects.create(
                    cliente=destinatario,
                    criada_por=request.user,
                    nome=nome,
                    descricao=descricao,
                    valor=valor,
                    prioridade=prioridade,
                )
                return redirect("proposta_detail", pk=proposta.pk)

    return render(
        request,
        "core/proposta_nova.html",
        {
            "message": message,
            "form_data": form_data,
        },
    )


@login_required
@require_POST
def aprovar_proposta(request, pk):
    cliente = _get_cliente(request.user)
    proposta = get_object_or_404(Proposta, pk=pk, cliente=cliente)
    if proposta.cliente.usuario_id != request.user.id:
        return HttpResponseForbidden("Somente o destinatario pode aprovar.")
    if proposta.valor == 0:
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
    users = User.objects.order_by("username")
    return render(
        request,
        "core/usuarios.html",
        {
            "form": form,
            "users": users,
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
                item_tipo = request.POST.get(f"item_tipo_{idx}")
                item_pago = request.POST.get(f"item_pago_{idx}") == "on"
                if item_nome:
                    itens_payload.append(
                        {
                            "nome": item_nome,
                            "valor": item_valor,
                            "quantidade": item_quantidade,
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
                    CompraItem.objects.create(
                        compra=compra,
                        nome=item["nome"],
                        valor=valor,
                        quantidade=quantidade,
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
