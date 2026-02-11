import json
from datetime import timedelta, timezone as dt_timezone

from django.contrib.auth.decorators import login_required
from django.db import IntegrityError
from django.http import HttpResponseForbidden
from django.shortcuts import get_object_or_404, redirect, render
from django.utils import timezone
from django.utils.dateparse import parse_datetime

from core.models import App, AppRotasMap, IngestRecord
from core.views import _get_cliente

TAG_KEYS = ("Name", "TagName", "tagname", "tag", "nome_tag")
VALUE_KEYS = ("Value", "value", "valor", "status")
TIMESTAMP_KEYS = ("TimestampUtc", "Hora", "DataHoraBase", "datahora", "timestamp")
ROTA_SUFFIXES = (
    ("_DESLIGAR", "DESLIGAR"),
    ("_LIGADA", "LIGADA"),
    ("_LIGAR", "LIGAR"),
    ("_ORIGEM", "ORIGEM"),
    ("_DESTINO", "DESTINO"),
    ("_DESTIN", "DESTINO"),
)
MAX_DASHBOARD_RECORDS = 6000
MAX_ROUTE_RECORDS = 12000
GLOBAL_TIMELINE_LIMIT = 180
ROUTE_TIMELINE_LIMIT = 240


def _get_rotas_app():
    return get_object_or_404(App, slug="approtas", ativo=True)


def _has_access(user, app):
    if user.is_staff:
        return True
    cliente = _get_cliente(user)
    return bool(cliente) and cliente.apps.filter(pk=app.pk).exists()


def _parse_query_datetime(value):
    text = (value or "").strip()
    if not text:
        return None
    parsed = parse_datetime(text)
    if not parsed:
        return None
    if timezone.is_naive(parsed):
        return timezone.make_aware(parsed, timezone.get_current_timezone())
    return parsed


def _fmt_input_datetime(value):
    if not value:
        return ""
    localized = timezone.localtime(value)
    return localized.strftime("%Y-%m-%dT%H:%M")


def _coerce_value(value):
    if value is None:
        return None
    if isinstance(value, (int, float, bool)):
        return value
    text = str(value).strip()
    if not text:
        return None
    lower = text.lower()
    if lower in ("true", "on", "sim", "ligado"):
        return 1
    if lower in ("false", "off", "nao", "não", "desligado"):
        return 0
    try:
        if "." in text:
            return float(text)
        return int(text)
    except ValueError:
        return text


def _is_active(value):
    if value is None:
        return False
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value > 0
    text = str(value).strip().lower()
    if text in ("1", "true", "on", "sim", "ligado"):
        return True
    if text in ("0", "false", "off", "nao", "não", "desligado", ""):
        return False
    return True


def _value_to_int(value):
    if value is None:
        return None
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def _extract_tag(payload):
    for key in TAG_KEYS:
        value = payload.get(key)
        if value:
            return str(value).strip()
    return ""


def _extract_value(payload):
    for key in VALUE_KEYS:
        if key in payload:
            return _coerce_value(payload.get(key))
    return None


def _extract_timestamp(payload, record):
    for key in TIMESTAMP_KEYS:
        raw = payload.get(key)
        if not raw:
            continue
        parsed = parse_datetime(str(raw).strip())
        if not parsed:
            continue
        if timezone.is_naive(parsed):
            if key == "TimestampUtc":
                return parsed.replace(tzinfo=dt_timezone.utc)
            return timezone.make_aware(parsed, timezone.get_current_timezone())
        return parsed
    return record.updated_at or record.created_at


def _classify_tag(tag_name):
    tag = str(tag_name or "").strip().upper()
    if not tag:
        return None, None
    for suffix, attr in ROTA_SUFFIXES:
        if not tag.endswith(suffix):
            continue
        prefix = tag[: -len(suffix)].strip("_")
        if not prefix:
            return None, None
        return prefix, attr
    return None, None


def _build_event(record):
    payload = record.payload if isinstance(record.payload, dict) else {}
    tag_name = _extract_tag(payload)
    prefix, attr = _classify_tag(tag_name)
    if not prefix:
        return None
    timestamp = _extract_timestamp(payload, record)
    if not timestamp:
        return None
    if timezone.is_naive(timestamp):
        timestamp = timezone.make_aware(timestamp, timezone.get_current_timezone())
    return {
        "prefixo": prefix,
        "atributo": attr,
        "tag": tag_name,
        "valor": _extract_value(payload),
        "timestamp": timestamp,
        "ingest_timestamp": record.updated_at or record.created_at,
        "source_id": record.source_id,
    }


def _timeline_points(events, limit):
    by_dt = {}
    for event in events:
        dt = event["timestamp"]
        by_dt[dt] = by_dt.get(dt, 0) + 1
    points = [
        {
            "timestamp": dt,
            "iso": dt.isoformat(),
            "label": timezone.localtime(dt).strftime("%d/%m/%Y %H:%M:%S"),
            "count": count,
        }
        for dt, count in sorted(by_dt.items(), key=lambda item: item[0])
    ]
    if len(points) <= limit:
        return points
    step = max(1, len(points) // limit)
    sampled = points[::step]
    if sampled[-1]["iso"] != points[-1]["iso"]:
        sampled.append(points[-1])
    return sampled


def _selected_timeline_point(points, selected_at):
    if not points:
        return None, -1
    best_index = 0
    for idx, point in enumerate(points):
        if point["timestamp"] <= selected_at:
            best_index = idx
        else:
            break
    return points[best_index], best_index


def _build_route_cards(events, selected_at, origem_maps, destino_maps, search="", show_inactive=False):
    states = {}
    known_prefixes = sorted({event["prefixo"] for event in events})
    for event in events:
        if event["timestamp"] > selected_at:
            break
        prefixo = event["prefixo"]
        state = states.setdefault(
            prefixo,
            {
                "prefixo": prefixo,
                "attrs": {"LIGAR": None, "DESLIGAR": None, "LIGADA": None, "ORIGEM": None, "DESTINO": None},
                "last_update": None,
            },
        )
        state["attrs"][event["atributo"]] = event["valor"]
        state["last_update"] = event["timestamp"]

    cards = []
    search_norm = (search or "").strip().lower()
    for prefixo in known_prefixes:
        state = states.get(
            prefixo,
            {
                "prefixo": prefixo,
                "attrs": {"LIGAR": None, "DESLIGAR": None, "LIGADA": None, "ORIGEM": None, "DESTINO": None},
                "last_update": None,
            },
        )
        attrs = state["attrs"]
        ligar_on = _is_active(attrs.get("LIGAR"))
        desligar_on = _is_active(attrs.get("DESLIGAR"))
        ligada_on = _is_active(attrs.get("LIGADA"))
        play_blink = ligar_on and not ligada_on
        play_on = ligar_on and ligada_on
        pause_on = desligar_on
        origem_codigo = _value_to_int(attrs.get("ORIGEM"))
        destino_codigo = _value_to_int(attrs.get("DESTINO"))
        origem_nome = origem_maps.get(origem_codigo) if origem_codigo is not None else None
        destino_nome = destino_maps.get(destino_codigo) if destino_codigo is not None else None
        origem_display = origem_nome or (str(origem_codigo) if origem_codigo is not None else "--")
        destino_display = destino_nome or (str(destino_codigo) if destino_codigo is not None else "--")
        is_inactive = not (play_blink or play_on or pause_on)

        haystack = " ".join([prefixo, origem_display, destino_display]).lower()
        if search_norm and search_norm not in haystack:
            continue
        if is_inactive and not show_inactive:
            continue
        cards.append(
            {
                "prefixo": prefixo,
                "origem_display": origem_display,
                "destino_display": destino_display,
                "origem_codigo": origem_codigo,
                "destino_codigo": destino_codigo,
                "play_blink": play_blink,
                "play_on": play_on,
                "pause_on": pause_on,
                "is_inactive": is_inactive,
                "last_update": state["last_update"],
                "last_update_display": (
                    timezone.localtime(state["last_update"]).strftime("%d/%m %H:%M:%S") if state["last_update"] else "-"
                ),
            }
        )
    cards.sort(key=lambda item: item["prefixo"])
    return cards


def _query_window(request, default_hours=24):
    now = timezone.now()
    start = _parse_query_datetime(request.GET.get("inicio"))
    end = _parse_query_datetime(request.GET.get("fim"))
    if not end:
        end = now
    if not start:
        start = end - timedelta(hours=default_hours)
    if start > end:
        start, end = end, start
    return start, end


def _base_records_queryset(app, start, end):
    qs = IngestRecord.objects.filter(
        client_id=app.ingest_client_id,
        agent_id=app.ingest_agent_id,
        created_at__gte=start,
        created_at__lte=end,
    )
    if app.ingest_source:
        qs = qs.filter(source=app.ingest_source)
    return qs


@login_required
def dashboard(request):
    app = _get_rotas_app()
    if not _has_access(request.user, app):
        return HttpResponseForbidden("Sem permissao.")

    start, end = _query_window(request, default_hours=24)
    show_inactive = request.GET.get("mostrar_inativas") == "1"
    search = (request.GET.get("busca") or "").strip()
    config_missing = not app.ingest_client_id or not app.ingest_agent_id

    events = []
    if not config_missing:
        records = (
            _base_records_queryset(app, start, end)
            .only("source_id", "payload", "created_at", "updated_at")
            .order_by("-created_at")[:MAX_DASHBOARD_RECORDS]
        )
        for record in records:
            event = _build_event(record)
            if event:
                events.append(event)
        events.sort(key=lambda item: (item["timestamp"], item["prefixo"], item["atributo"]))

    timeline = _timeline_points(events, GLOBAL_TIMELINE_LIMIT)
    selected_at = _parse_query_datetime(request.GET.get("at"))
    if not selected_at:
        selected_at = timeline[-1]["timestamp"] if timeline else end
    selected_point, selected_index = _selected_timeline_point(timeline, selected_at)
    if selected_point:
        selected_at = selected_point["timestamp"]

    maps_qs = AppRotasMap.objects.filter(app=app, ativo=True).order_by("tipo", "codigo")
    origem_maps = {item.codigo: item.nome for item in maps_qs if item.tipo == AppRotasMap.Tipo.ORIGEM}
    destino_maps = {item.codigo: item.nome for item in maps_qs if item.tipo == AppRotasMap.Tipo.DESTINO}
    cards = _build_route_cards(events, selected_at, origem_maps, destino_maps, search=search, show_inactive=show_inactive)

    recent_events = [
        event
        for event in reversed(events)
        if event["timestamp"] <= selected_at
    ][:40]
    for event in recent_events:
        event["timestamp_display"] = timezone.localtime(event["timestamp"]).strftime("%d/%m %H:%M:%S")
        value_display = event["valor"]
        if isinstance(value_display, float):
            value_display = f"{value_display:.3f}".rstrip("0").rstrip(".")
        event["valor_display"] = value_display

    return render(
        request,
        "core/apps/app_rotas/dashboard.html",
        {
            "app": app,
            "cards": cards,
            "inicio": _fmt_input_datetime(start),
            "fim": _fmt_input_datetime(end),
            "busca": search,
            "mostrar_inativas": show_inactive,
            "timeline": timeline,
            "timeline_total": len(timeline),
            "timeline_json": json.dumps([{"iso": point["iso"], "label": point["label"]} for point in timeline]),
            "selected_index": selected_index,
            "selected_point": selected_point,
            "selected_at_iso": selected_at.isoformat() if selected_at else "",
            "selected_at_label": timezone.localtime(selected_at).strftime("%d/%m/%Y %H:%M:%S") if selected_at else "-",
            "eventos_recentes": recent_events,
            "config_missing": config_missing,
            "total_events": len(events),
            "max_records": MAX_DASHBOARD_RECORDS,
        },
    )


@login_required
def rota_detalhe(request, prefixo):
    app = _get_rotas_app()
    if not _has_access(request.user, app):
        return HttpResponseForbidden("Sem permissao.")

    start, end = _query_window(request, default_hours=24 * 7)
    config_missing = not app.ingest_client_id or not app.ingest_agent_id

    events = []
    if not config_missing:
        records = (
            _base_records_queryset(app, start, end)
            .only("source_id", "payload", "created_at", "updated_at")
            .order_by("-created_at")[:MAX_ROUTE_RECORDS]
        )
        prefix_norm = (prefixo or "").strip().upper()
        for record in records:
            event = _build_event(record)
            if event and event["prefixo"] == prefix_norm:
                events.append(event)
        events.sort(key=lambda item: (item["timestamp"], item["atributo"]))
    else:
        prefix_norm = (prefixo or "").strip().upper()

    timeline = _timeline_points(events, ROUTE_TIMELINE_LIMIT)
    selected_at = _parse_query_datetime(request.GET.get("at"))
    if not selected_at:
        selected_at = timeline[-1]["timestamp"] if timeline else end
    selected_point, selected_index = _selected_timeline_point(timeline, selected_at)
    if selected_point:
        selected_at = selected_point["timestamp"]

    maps_qs = AppRotasMap.objects.filter(app=app, ativo=True).order_by("tipo", "codigo")
    origem_maps = {item.codigo: item.nome for item in maps_qs if item.tipo == AppRotasMap.Tipo.ORIGEM}
    destino_maps = {item.codigo: item.nome for item in maps_qs if item.tipo == AppRotasMap.Tipo.DESTINO}

    attrs = {"LIGAR": None, "DESLIGAR": None, "LIGADA": None, "ORIGEM": None, "DESTINO": None}
    for event in events:
        if event["timestamp"] > selected_at:
            break
        attrs[event["atributo"]] = event["valor"]

    ligar_on = _is_active(attrs.get("LIGAR"))
    desligar_on = _is_active(attrs.get("DESLIGAR"))
    ligada_on = _is_active(attrs.get("LIGADA"))
    status = {
        "play_blink": ligar_on and not ligada_on,
        "play_on": ligar_on and ligada_on,
        "pause_on": desligar_on,
    }

    origem_codigo = _value_to_int(attrs.get("ORIGEM"))
    destino_codigo = _value_to_int(attrs.get("DESTINO"))
    origem_nome = origem_maps.get(origem_codigo) if origem_codigo is not None else None
    destino_nome = destino_maps.get(destino_codigo) if destino_codigo is not None else None

    timeline_events = []
    previous_values = {}
    for event in reversed(events):
        if event["timestamp"] > selected_at:
            continue
        attr = event["atributo"]
        changed = previous_values.get(attr) != event["valor"]
        previous_values[attr] = event["valor"]
        value_display = event["valor"]
        if attr in ("ORIGEM", "DESTINO"):
            code = _value_to_int(event["valor"])
            mapped = origem_maps.get(code) if attr == "ORIGEM" else destino_maps.get(code)
            if mapped:
                value_display = f"{mapped} ({code})"
            elif code is not None:
                value_display = str(code)
        elif isinstance(value_display, float):
            value_display = f"{value_display:.3f}".rstrip("0").rstrip(".")
        elif value_display is None:
            value_display = "-"
        timeline_events.append(
            {
                "timestamp_display": timezone.localtime(event["timestamp"]).strftime("%d/%m/%Y %H:%M:%S"),
                "atributo": attr,
                "valor_display": value_display,
                "changed": changed,
                "is_command": attr in ("LIGAR", "DESLIGAR", "LIGADA"),
            }
        )
        if len(timeline_events) >= 120:
            break

    return render(
        request,
        "core/apps/app_rotas/rota_detalhe.html",
        {
            "app": app,
            "prefixo": prefix_norm,
            "inicio": _fmt_input_datetime(start),
            "fim": _fmt_input_datetime(end),
            "timeline": timeline,
            "timeline_json": json.dumps([{"iso": point["iso"], "label": point["label"]} for point in timeline]),
            "selected_index": selected_index,
            "selected_point": selected_point,
            "selected_at_iso": selected_at.isoformat() if selected_at else "",
            "selected_at_label": timezone.localtime(selected_at).strftime("%d/%m/%Y %H:%M:%S") if selected_at else "-",
            "attrs": attrs,
            "status": status,
            "origem_codigo": origem_codigo,
            "destino_codigo": destino_codigo,
            "origem_display": origem_nome or (str(origem_codigo) if origem_codigo is not None else "--"),
            "destino_display": destino_nome or (str(destino_codigo) if destino_codigo is not None else "--"),
            "timeline_events": timeline_events,
            "config_missing": config_missing,
        },
    )


@login_required
def mapeamentos(request):
    app = _get_rotas_app()
    if not _has_access(request.user, app):
        return HttpResponseForbidden("Sem permissao.")

    message = None
    message_level = "info"
    tipo_filtro = (request.GET.get("tipo") or "").strip().upper()
    if tipo_filtro not in ("ORIGEM", "DESTINO"):
        tipo_filtro = ""

    if request.method == "POST":
        action = request.POST.get("action")
        if action == "save_map":
            map_id = request.POST.get("map_id")
            tipo = (request.POST.get("tipo") or "").strip().upper()
            codigo_raw = (request.POST.get("codigo") or "").strip()
            nome = (request.POST.get("nome") or "").strip()
            ativo = request.POST.get("ativo") == "on"
            if tipo not in ("ORIGEM", "DESTINO"):
                message = "Selecione um tipo valido."
                message_level = "error"
            elif not codigo_raw:
                message = "Informe o codigo numerico."
                message_level = "error"
            elif not nome:
                message = "Informe o nome amigavel."
                message_level = "error"
            else:
                try:
                    codigo = int(codigo_raw)
                except ValueError:
                    codigo = None
                if codigo is None:
                    message = "Codigo invalido."
                    message_level = "error"
                else:
                    try:
                        if map_id:
                            mapa = AppRotasMap.objects.filter(app=app, pk=map_id).first()
                            if mapa:
                                mapa.tipo = tipo
                                mapa.codigo = codigo
                                mapa.nome = nome
                                mapa.ativo = ativo
                                mapa.save()
                        else:
                            AppRotasMap.objects.create(
                                app=app,
                                tipo=tipo,
                                codigo=codigo,
                                nome=nome,
                                ativo=ativo,
                            )
                        return redirect("app_rotas_mapeamentos")
                    except IntegrityError:
                        message = "Ja existe mapeamento com esse app/tipo/codigo."
                        message_level = "error"
        if action == "delete_map":
            map_id = request.POST.get("map_id")
            mapa = AppRotasMap.objects.filter(app=app, pk=map_id).first()
            if mapa:
                mapa.delete()
                return redirect("app_rotas_mapeamentos")

    edit_id = request.GET.get("edit")
    edit_item = AppRotasMap.objects.filter(app=app, pk=edit_id).first() if edit_id else None
    maps = AppRotasMap.objects.filter(app=app).order_by("tipo", "codigo")
    if tipo_filtro:
        maps = maps.filter(tipo=tipo_filtro)

    return render(
        request,
        "core/apps/app_rotas/mapeamentos.html",
        {
            "app": app,
            "maps": maps,
            "edit_item": edit_item,
            "tipo_filtro": tipo_filtro,
            "message": message,
            "message_level": message_level,
        },
    )
