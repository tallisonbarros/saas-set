import json
from datetime import datetime, time, timedelta, timezone as dt_timezone

from django.contrib.auth.decorators import login_required
from django.core.paginator import Paginator
from django.db import IntegrityError
from django.db.models import Q
from django.db.models.functions import TruncDate
from django.http import HttpResponseForbidden, JsonResponse
from django.shortcuts import get_object_or_404, redirect, render
from django.template.loader import render_to_string
from django.utils import timezone
from django.utils.dateparse import parse_datetime

from core.models import App, AppRotaConfig, AppRotasMap, IngestRecord
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
MAX_DASHBOARD_RECORDS = 8000
MAX_ROUTE_RECORDS = 16000
BASELINE_RECORDS_LIMIT = 12000
RECENT_EVENTS_PAGE_SIZE = 10
ROUTE_EVENTS_PAGE_SIZE = 12
TIMELINE_STEP_MINUTES = 5
AVAILABLE_DAYS_LIMIT = 45
LIFEBIT_TAG_NAME = "LIFEBIT"
LIFEBIT_TIMEOUT_SECONDS = 30


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


def _parse_query_date(value):
    text = (value or "").strip()
    if not text:
        return None
    try:
        return datetime.strptime(text, "%Y-%m-%d").date()
    except ValueError:
        return None


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


def _binary_state(value):
    if value is None:
        return None
    return 1 if _is_active(value) else 0


def _context_status_label(ligar_value, desligar_value, ligada_value):
    key = (_binary_state(ligar_value), _binary_state(desligar_value), _binary_state(ligada_value))
    mapping = {
        (0, 0, 0): "Linha parada",
        (1, 0, 0): "Linha ligando",
        (1, 0, 1): "Linha ligada",
        (1, 1, 0): "Linha desligando",
    }
    return mapping.get(key, "Estado indefinido")


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
        "timestamp": timezone.localtime(timestamp),
        "ingest_timestamp": record.updated_at or record.created_at,
        "source_id": record.source_id,
    }


def _day_bounds(day):
    tz = timezone.get_current_timezone()
    start = timezone.make_aware(datetime.combine(day, time.min), tz)
    end = start + timedelta(days=1)
    return start, end


def _clamp_datetime(value, start, end_exclusive):
    if value is None:
        return None
    if value < start:
        return start
    max_value = end_exclusive - timedelta(seconds=1)
    if value > max_value:
        return max_value
    return value


def _build_fixed_timeline(day_start, day_end):
    points = []
    current = day_start
    idx = 0
    while current <= day_end:
        points.append(
            {
                "idx": idx,
                "timestamp": current,
                "iso": current.isoformat(),
                "label": timezone.localtime(current).strftime("%d/%m/%Y %H:%M:%S"),
                "hour_label": timezone.localtime(current).strftime("%H:%M"),
            }
        )
        current = current + timedelta(minutes=TIMELINE_STEP_MINUTES)
        idx += 1
    if points[-1]["timestamp"] != day_end:
        points.append(
            {
                "idx": idx,
                "timestamp": day_end,
                "iso": day_end.isoformat(),
                "label": timezone.localtime(day_end).strftime("%d/%m/%Y %H:%M:%S"),
                "hour_label": timezone.localtime(day_end).strftime("%H:%M"),
            }
        )
    return points


def _build_timeline_with_events(day_start, day_end, events):
    points_by_iso = {}
    for point in _build_fixed_timeline(day_start, day_end):
        points_by_iso[point["iso"]] = point

    for event in events:
        ts = event["timestamp"]
        if ts < day_start:
            ts = day_start
        if ts > day_end:
            ts = day_end
        iso = ts.isoformat()
        if iso in points_by_iso:
            continue
        points_by_iso[iso] = {
            "timestamp": ts,
            "iso": iso,
            "label": timezone.localtime(ts).strftime("%d/%m/%Y %H:%M:%S"),
            "hour_label": timezone.localtime(ts).strftime("%H:%M"),
        }

    timeline = sorted(points_by_iso.values(), key=lambda item: item["timestamp"])
    for idx, point in enumerate(timeline):
        point["idx"] = idx
    return timeline


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


def _base_records_queryset(app):
    qs = IngestRecord.objects.filter(
        client_id=app.ingest_client_id,
        agent_id=app.ingest_agent_id,
    )
    if app.ingest_source:
        qs = qs.filter(source=app.ingest_source)
    return qs


def _records_in_window(app, start, end_exclusive, limit):
    qs = _base_records_queryset(app).filter(
        Q(updated_at__gte=start, updated_at__lt=end_exclusive)
        | Q(updated_at__isnull=True, created_at__gte=start, created_at__lt=end_exclusive)
    )
    return qs.only("source_id", "payload", "created_at", "updated_at").order_by("-updated_at", "-created_at")[:limit]


def _records_before(app, cutoff, limit):
    qs = _base_records_queryset(app).filter(
        Q(updated_at__lt=cutoff) | Q(updated_at__isnull=True, created_at__lt=cutoff)
    )
    return qs.only("source_id", "payload", "created_at", "updated_at").order_by("-updated_at", "-created_at")[:limit]


def _lifebit_lookup_q():
    lookup = Q()
    for key in TAG_KEYS:
        lookup |= Q(**{f"payload__{key}__iexact": LIFEBIT_TAG_NAME})
    return lookup


def _lifebit_status(app):
    record = (
        _base_records_queryset(app)
        .filter(_lifebit_lookup_q())
        .only("payload", "created_at", "updated_at")
        .order_by("-updated_at", "-created_at")
        .first()
    )
    if not record:
        return False, None
    last_seen = record.updated_at or record.created_at
    if not last_seen:
        return False, None
    now_local = timezone.localtime(timezone.now())
    last_seen_local = timezone.localtime(last_seen)
    delta = (now_local - last_seen_local).total_seconds()
    return delta <= LIFEBIT_TIMEOUT_SECONDS, last_seen_local


def _events_from_records(records, start=None, end_exclusive=None, prefix=None):
    events = []
    prefix_upper = (prefix or "").strip().upper()
    for record in records:
        event = _build_event(record)
        if not event:
            continue
        if prefix_upper and event["prefixo"] != prefix_upper:
            continue
        if start and event["timestamp"] < start:
            continue
        if end_exclusive and event["timestamp"] >= end_exclusive:
            continue
        events.append(event)
    events.sort(key=lambda item: (item["timestamp"], item["prefixo"], item["atributo"]))
    return events


def _seed_states_from_events(events_before):
    states = {}
    for event in events_before:
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
    return states


def _clone_state(state):
    return {
        "prefixo": state["prefixo"],
        "attrs": dict(state["attrs"]),
        "last_update": state.get("last_update"),
    }


def _build_route_cards(
    events,
    selected_at,
    origem_maps,
    destino_maps,
    initial_states=None,
    known_prefixes=None,
    route_configs=None,
):
    states = {}
    for prefixo, state in (initial_states or {}).items():
        states[prefixo] = _clone_state(state)

    prefixes_from_events = {event["prefixo"] for event in events}
    active_prefixes = sorted(set(known_prefixes or set()) | prefixes_from_events)

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
    route_configs = route_configs or {}
    for prefixo in active_prefixes:
        cfg = route_configs.get(prefixo)
        if cfg and not cfg.ativo:
            continue
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
        play_blink = ligar_on and not ligada_on and not desligar_on
        play_on = ligar_on and ligada_on and not desligar_on
        pause_on = desligar_on
        origem_codigo = _value_to_int(attrs.get("ORIGEM"))
        destino_codigo = _value_to_int(attrs.get("DESTINO"))
        origem_nome = origem_maps.get(origem_codigo) if origem_codigo is not None else None
        destino_nome = destino_maps.get(destino_codigo) if destino_codigo is not None else None
        origem_display = origem_nome or (str(origem_codigo) if origem_codigo is not None else "--")
        destino_display = destino_nome or (str(destino_codigo) if destino_codigo is not None else "--")
        is_inactive = not (play_blink or play_on or pause_on)
        nome_exibicao = (cfg.nome_exibicao or "").strip() if cfg else ""
        ordem = cfg.ordem if cfg else 0
        cards.append(
            {
                "prefixo": prefixo,
                "nome_exibicao": nome_exibicao,
                "titulo": nome_exibicao or prefixo,
                "ordem": ordem,
                "origem_display": origem_display,
                "destino_display": destino_display,
                "origem_codigo": origem_codigo,
                "destino_codigo": destino_codigo,
                "play_blink": play_blink,
                "play_on": play_on,
                "pause_on": pause_on,
                "context_status": _context_status_label(attrs.get("LIGAR"), attrs.get("DESLIGAR"), attrs.get("LIGADA")),
                "is_inactive": is_inactive,
                "last_update": state["last_update"],
                "last_update_display": (
                    timezone.localtime(state["last_update"]).strftime("%d/%m %H:%M:%S") if state["last_update"] else "-"
                ),
            }
        )
    cards.sort(key=lambda item: ((item["ordem"] if item["ordem"] > 0 else 999999), item["prefixo"]))
    return cards


def _available_days(app):
    days = list(
        _base_records_queryset(app)
        .exclude(updated_at__isnull=True)
        .annotate(day=TruncDate("updated_at"))
        .values_list("day", flat=True)
        .distinct()
        .order_by("-day")[:AVAILABLE_DAYS_LIMIT]
    )
    return [day for day in days if day]


def _day_navigation(available_days, selected_day):
    prev_day = None
    next_day = None
    if selected_day in available_days:
        idx = available_days.index(selected_day)
        if idx < len(available_days) - 1:
            prev_day = available_days[idx + 1]
        if idx > 0:
            next_day = available_days[idx - 1]
    return prev_day, next_day


def _timeline_now_state(selected_day, selected_at, day_start, day_end_exclusive):
    now_local = timezone.localtime(timezone.now())
    now_target = _clamp_datetime(now_local, day_start, day_end_exclusive)
    tolerance_seconds = TIMELINE_STEP_MINUTES * 60 + 1
    showing_now = selected_day == timezone.localdate() and abs((selected_at - now_target).total_seconds()) <= tolerance_seconds
    return showing_now, now_target, timezone.localdate()


def _timeline_end_for_day(selected_day, day_start, day_end_exclusive):
    day_end_point = day_end_exclusive - timedelta(seconds=1)
    if selected_day != timezone.localdate():
        return day_end_point
    now_local = timezone.localtime(timezone.now())
    now_clamped = _clamp_datetime(now_local, day_start, day_end_exclusive)
    if now_clamped is None:
        return day_end_point
    return min(day_end_point, now_clamped)


def _build_ligada_intervals(day_events, day_start, day_end, initial_ligada_on):
    intervals = []
    ligada_on = bool(initial_ligada_on)
    current_start = day_start if ligada_on else None
    for event in day_events:
        if event["atributo"] != "LIGADA":
            continue
        ev_time = event["timestamp"]
        if ev_time < day_start or ev_time > day_end:
            continue
        new_on = _is_active(event["valor"])
        if ligada_on and not new_on and current_start is not None:
            intervals.append((current_start, ev_time))
            current_start = None
        elif not ligada_on and new_on:
            current_start = ev_time
        ligada_on = new_on
    if ligada_on and current_start is not None and current_start < day_end:
        intervals.append((current_start, day_end))
    return intervals


def _build_global_ligada_intervals(day_events, day_start, day_end, initial_ligada_prefixes=None):
    intervals = []
    ligada_prefixes = set(initial_ligada_prefixes or set())
    current_start = day_start if ligada_prefixes else None

    for event in day_events:
        if event["atributo"] != "LIGADA":
            continue
        ev_time = event["timestamp"]
        if ev_time < day_start or ev_time > day_end:
            continue
        prefixo = event["prefixo"]
        was_any = bool(ligada_prefixes)
        new_on = _is_active(event["valor"])
        if new_on:
            ligada_prefixes.add(prefixo)
        else:
            ligada_prefixes.discard(prefixo)
        is_any = bool(ligada_prefixes)
        if not was_any and is_any:
            current_start = ev_time
        elif was_any and not is_any and current_start is not None:
            intervals.append((current_start, ev_time))
            current_start = None

    if ligada_prefixes and current_start is not None and current_start < day_end:
        intervals.append((current_start, day_end))
    return intervals


def _ligada_gradient(intervals, day_start, day_end):
    total_seconds = (day_end - day_start).total_seconds()
    if total_seconds <= 0:
        return "linear-gradient(to right, rgba(148,163,184,0.28) 0%, rgba(148,163,184,0.28) 100%)"

    def pct(dt):
        return max(0.0, min(100.0, ((dt - day_start).total_seconds() / total_seconds) * 100.0))

    parts = []
    cursor = 0.0
    for start, end in intervals:
        start_pct = pct(start)
        end_pct = pct(end)
        if start_pct > cursor:
            parts.append(f"rgba(148,163,184,0.28) {cursor:.3f}% {start_pct:.3f}%")
        if end_pct > start_pct:
            parts.append(f"rgba(34,197,94,0.65) {start_pct:.3f}% {end_pct:.3f}%")
        cursor = max(cursor, end_pct)
    if cursor < 100.0:
        parts.append(f"rgba(148,163,184,0.28) {cursor:.3f}% 100%")
    if not parts:
        parts = ["rgba(148,163,184,0.28) 0% 100%"]
    return "linear-gradient(to right, " + ", ".join(parts) + ")"


@login_required
def dashboard(request):
    app = _get_rotas_app()
    if not _has_access(request.user, app):
        return HttpResponseForbidden("Sem permissao.")

    config_missing = not app.ingest_client_id or not app.ingest_agent_id
    lifebit_connected = False
    lifebit_last_seen = None
    if not config_missing:
        lifebit_connected, lifebit_last_seen = _lifebit_status(app)
    available_days = [] if config_missing else _available_days(app)
    selected_day = _parse_query_date(request.GET.get("dia"))
    if not selected_day:
        today = timezone.localdate()
        selected_day = today if today in available_days else (available_days[0] if available_days else today)

    day_start, day_end_exclusive = _day_bounds(selected_day)
    timeline_end_point = _timeline_end_for_day(selected_day, day_start, day_end_exclusive)

    events_today = []
    seed_states = {}
    day_prefixes = set()
    if not config_missing:
        today_records = _records_in_window(app, day_start, day_end_exclusive, MAX_DASHBOARD_RECORDS)
        events_today = _events_from_records(today_records)
        day_prefixes = {event["prefixo"] for event in events_today}
        if day_prefixes:
            baseline_records = _records_before(app, day_start, BASELINE_RECORDS_LIMIT)
            baseline_events = _events_from_records(baseline_records)
            baseline_events = [event for event in baseline_events if event["prefixo"] in day_prefixes]
            seed_states = _seed_states_from_events(baseline_events)

    timeline = _build_timeline_with_events(day_start, timeline_end_point, events_today)
    selected_at = _parse_query_datetime(request.GET.get("at"))
    if not selected_at:
        now = timezone.localtime(timezone.now())
        selected_at = now if selected_day == timezone.localdate() else timeline_end_point
    selected_at = _clamp_datetime(selected_at, day_start, day_end_exclusive)
    if selected_at and selected_at > timeline_end_point:
        selected_at = timeline_end_point
    selected_point, selected_index = _selected_timeline_point(timeline, selected_at)
    if selected_point:
        selected_at = selected_point["timestamp"]

    maps_qs = AppRotasMap.objects.filter(app=app, ativo=True).order_by("tipo", "codigo")
    origem_maps = {item.codigo: item.nome for item in maps_qs if item.tipo == AppRotasMap.Tipo.ORIGEM}
    destino_maps = {item.codigo: item.nome for item in maps_qs if item.tipo == AppRotasMap.Tipo.DESTINO}
    configs_qs = AppRotaConfig.objects.filter(app=app)
    route_configs = {item.prefixo.strip().upper(): item for item in configs_qs}
    cards = _build_route_cards(
        events_today,
        selected_at,
        origem_maps,
        destino_maps,
        initial_states=seed_states,
        known_prefixes=day_prefixes,
        route_configs=route_configs,
    )
    initial_ligada_prefixes = {
        prefixo for prefixo, state in seed_states.items() if _is_active(state["attrs"].get("LIGADA"))
    }
    global_ligada_intervals = _build_global_ligada_intervals(
        events_today,
        day_start,
        timeline_end_point,
        initial_ligada_prefixes=initial_ligada_prefixes,
    )
    global_ligada_gradient = _ligada_gradient(global_ligada_intervals, day_start, timeline_end_point)

    recent_events = [event for event in reversed(events_today) if event["timestamp"] <= selected_at][:200]
    events_page_num = request.GET.get("events_page", "1")
    recent_events_paginator = Paginator(recent_events, RECENT_EVENTS_PAGE_SIZE)
    recent_events_page = recent_events_paginator.get_page(events_page_num)
    for event in recent_events_page.object_list:
        event["timestamp_display"] = timezone.localtime(event["timestamp"]).strftime("%d/%m %H:%M:%S")
        value_display = event["valor"]
        if isinstance(value_display, float):
            value_display = f"{value_display:.3f}".rstrip("0").rstrip(".")
        event["valor_display"] = value_display

    if request.GET.get("partial") == "recent_events" and request.headers.get("X-Requested-With") == "XMLHttpRequest":
        return render(
            request,
            "core/apps/app_rotas/_eventos_recentes.html",
            {
                "eventos_recentes": recent_events_page.object_list,
                "recent_events_page": recent_events_page,
            },
        )
    if request.GET.get("partial") == "timeline" and request.headers.get("X-Requested-With") == "XMLHttpRequest":
        showing_now, now_target, now_day = _timeline_now_state(selected_day, selected_at, day_start, day_end_exclusive)
        cards_html = render_to_string(
            "core/apps/app_rotas/_rotas_cards.html",
            {
                "cards": cards,
                "selected_day": selected_day,
                "selected_at_iso": selected_at.isoformat() if selected_at else "",
                "lifebit_connected": lifebit_connected,
                "lifebit_last_seen": (
                    timezone.localtime(lifebit_last_seen).strftime("%d/%m/%Y %H:%M:%S") if lifebit_last_seen else "-"
                ),
            },
            request=request,
        )
        events_html = render_to_string(
            "core/apps/app_rotas/_eventos_recentes.html",
            {
                "eventos_recentes": recent_events_page.object_list,
                "recent_events_page": recent_events_page,
            },
            request=request,
        )
        return JsonResponse(
            {
                "ok": True,
                "selected_at_label": timezone.localtime(selected_at).strftime("%d/%m/%Y %H:%M:%S") if selected_at else "-",
                "showing_now": showing_now,
                "now_day": now_day.strftime("%Y-%m-%d"),
                "now_at_iso": now_target.isoformat() if now_target else "",
                "lifebit_connected": lifebit_connected,
                "lifebit_label": "Conectado" if lifebit_connected else "Desconectado",
                "lifebit_last_seen": (
                    timezone.localtime(lifebit_last_seen).strftime("%d/%m/%Y %H:%M:%S") if lifebit_last_seen else "-"
                ),
                "cards_html": cards_html,
                "events_html": events_html,
            }
        )

    prev_day, next_day = _day_navigation(available_days, selected_day)
    showing_now, now_target, now_day = _timeline_now_state(selected_day, selected_at, day_start, day_end_exclusive)

    return render(
        request,
        "core/apps/app_rotas/dashboard.html",
        {
            "app": app,
            "cards": cards,
            "selected_day": selected_day,
            "available_days": available_days,
            "prev_day": prev_day,
            "next_day": next_day,
            "timeline": timeline,
            "timeline_total": len(timeline),
            "timeline_json": json.dumps([{"iso": point["iso"], "label": point["label"]} for point in timeline]),
            "selected_index": selected_index,
            "selected_point": selected_point,
            "selected_at_iso": selected_at.isoformat() if selected_at else "",
            "selected_at_label": timezone.localtime(selected_at).strftime("%d/%m/%Y %H:%M:%S") if selected_at else "-",
            "showing_now": showing_now,
            "now_day": now_day,
            "now_at_iso": now_target.isoformat() if now_target else "",
            "eventos_recentes": recent_events_page.object_list,
            "recent_events_page": recent_events_page,
            "config_missing": config_missing,
            "total_events": len(events_today),
            "max_records": MAX_DASHBOARD_RECORDS,
            "global_ligada_gradient": global_ligada_gradient,
            "lifebit_connected": lifebit_connected,
            "lifebit_label": "Conectado" if lifebit_connected else "Desconectado",
            "lifebit_last_seen": (
                timezone.localtime(lifebit_last_seen).strftime("%d/%m/%Y %H:%M:%S") if lifebit_last_seen else "-"
            ),
        },
    )


@login_required
def rota_detalhe(request, prefixo):
    app = _get_rotas_app()
    if not _has_access(request.user, app):
        return HttpResponseForbidden("Sem permissao.")

    config_missing = not app.ingest_client_id or not app.ingest_agent_id
    prefix_norm = (prefixo or "").strip().upper()

    if request.method == "POST":
        action = request.POST.get("action")
        if action == "save_rota_config":
            nome_exibicao = (request.POST.get("nome_exibicao") or "").strip()
            ordem_raw = (request.POST.get("ordem") or "").strip()
            ativo = request.POST.get("ativo") == "on"
            try:
                ordem = int(ordem_raw) if ordem_raw else 0
            except ValueError:
                ordem = 0
            config, _ = AppRotaConfig.objects.get_or_create(
                app=app,
                prefixo=prefix_norm,
                defaults={
                    "nome_exibicao": nome_exibicao,
                    "ordem": ordem,
                    "ativo": ativo,
                },
            )
            if config.nome_exibicao != nome_exibicao or config.ordem != ordem or config.ativo != ativo:
                config.nome_exibicao = nome_exibicao
                config.ordem = ordem
                config.ativo = ativo
                config.save(update_fields=["nome_exibicao", "ordem", "ativo", "atualizado_em"])
            dia = (request.GET.get("dia") or request.POST.get("dia") or "").strip()
            at = (request.GET.get("at") or request.POST.get("at") or "").strip()
            query = []
            if dia:
                query.append(f"dia={dia}")
            if at:
                query.append(f"at={at}")
            suffix = f"?{'&'.join(query)}" if query else ""
            return redirect(f"{request.path}{suffix}")

    available_days = [] if config_missing else _available_days(app)
    selected_day = _parse_query_date(request.GET.get("dia"))
    if not selected_day:
        today = timezone.localdate()
        selected_day = today if today in available_days else (available_days[0] if available_days else today)

    day_start, day_end_exclusive = _day_bounds(selected_day)
    timeline_end_point = _timeline_end_for_day(selected_day, day_start, day_end_exclusive)

    day_events = []
    baseline_seed = {}
    if not config_missing:
        records_today = _records_in_window(app, day_start, day_end_exclusive, MAX_ROUTE_RECORDS)
        day_events = _events_from_records(records_today, prefix=prefix_norm)

        records_before = _records_before(app, day_start, BASELINE_RECORDS_LIMIT)
        baseline_events = _events_from_records(records_before, prefix=prefix_norm)
        baseline_seed = _seed_states_from_events(baseline_events)

    timeline = _build_timeline_with_events(day_start, timeline_end_point, day_events)
    selected_at = _parse_query_datetime(request.GET.get("at"))
    if not selected_at:
        now = timezone.localtime(timezone.now())
        selected_at = now if selected_day == timezone.localdate() else timeline_end_point
    selected_at = _clamp_datetime(selected_at, day_start, day_end_exclusive)
    if selected_at and selected_at > timeline_end_point:
        selected_at = timeline_end_point
    selected_point, selected_index = _selected_timeline_point(timeline, selected_at)
    if selected_point:
        selected_at = selected_point["timestamp"]

    seed_attrs = baseline_seed.get(
        prefix_norm,
        {
            "attrs": {"LIGAR": None, "DESLIGAR": None, "LIGADA": None, "ORIGEM": None, "DESTINO": None},
            "last_update": None,
        },
    )["attrs"]
    attrs = {
        "LIGAR": seed_attrs.get("LIGAR"),
        "DESLIGAR": seed_attrs.get("DESLIGAR"),
        "LIGADA": seed_attrs.get("LIGADA"),
        "ORIGEM": seed_attrs.get("ORIGEM"),
        "DESTINO": seed_attrs.get("DESTINO"),
    }
    for event in day_events:
        if event["timestamp"] > selected_at:
            break
        attrs[event["atributo"]] = event["valor"]

    ligar_on = _is_active(attrs.get("LIGAR"))
    desligar_on = _is_active(attrs.get("DESLIGAR"))
    ligada_on = _is_active(attrs.get("LIGADA"))
    status = {
        "play_blink": ligar_on and not ligada_on and not desligar_on,
        "play_on": ligar_on and ligada_on and not desligar_on,
        "pause_on": desligar_on,
        "context_label": _context_status_label(attrs.get("LIGAR"), attrs.get("DESLIGAR"), attrs.get("LIGADA")),
    }

    maps_qs = AppRotasMap.objects.filter(app=app, ativo=True).order_by("tipo", "codigo")
    origem_maps = {item.codigo: item.nome for item in maps_qs if item.tipo == AppRotasMap.Tipo.ORIGEM}
    destino_maps = {item.codigo: item.nome for item in maps_qs if item.tipo == AppRotasMap.Tipo.DESTINO}
    origem_codigo = _value_to_int(attrs.get("ORIGEM"))
    destino_codigo = _value_to_int(attrs.get("DESTINO"))
    origem_nome = origem_maps.get(origem_codigo) if origem_codigo is not None else None
    destino_nome = destino_maps.get(destino_codigo) if destino_codigo is not None else None

    timeline_events = []
    previous_values = {}
    for event in reversed(day_events):
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

    detail_events_page_num = request.GET.get("detail_events_page", "1")
    detail_events_paginator = Paginator(timeline_events, ROUTE_EVENTS_PAGE_SIZE)
    detail_events_page = detail_events_paginator.get_page(detail_events_page_num)

    initial_ligada_on = _is_active(seed_attrs.get("LIGADA"))
    ligada_intervals = _build_ligada_intervals(day_events, day_start, timeline_end_point, initial_ligada_on)
    ligada_gradient = _ligada_gradient(ligada_intervals, day_start, timeline_end_point)

    prev_day, next_day = _day_navigation(available_days, selected_day)
    route_config = AppRotaConfig.objects.filter(app=app, prefixo=prefix_norm).first()
    route_display_name = (route_config.nome_exibicao.strip() if route_config and route_config.nome_exibicao else "") or prefix_norm

    if request.GET.get("partial") == "timeline" and request.headers.get("X-Requested-With") == "XMLHttpRequest":
        showing_now, now_target, now_day = _timeline_now_state(selected_day, selected_at, day_start, day_end_exclusive)
        attrs_html = render_to_string(
            "core/apps/app_rotas/_rota_detalhe_attrs.html",
            {
                "attrs": attrs,
                "status": status,
                "origem_display": origem_nome or (str(origem_codigo) if origem_codigo is not None else "--"),
                "destino_display": destino_nome or (str(destino_codigo) if destino_codigo is not None else "--"),
            },
            request=request,
        )
        status_html = render_to_string(
            "core/apps/app_rotas/_rota_detalhe_status.html",
            {
                "status": status,
            },
            request=request,
        )
        events_html = render_to_string(
            "core/apps/app_rotas/_rota_detalhe_eventos.html",
            {
                "timeline_events": detail_events_page.object_list,
                "detail_events_page": detail_events_page,
                "selected_day": selected_day,
                "selected_at_iso": selected_at.isoformat() if selected_at else "",
            },
            request=request,
        )
        return JsonResponse(
            {
                "ok": True,
                "selected_at_label": timezone.localtime(selected_at).strftime("%d/%m/%Y %H:%M:%S") if selected_at else "-",
                "showing_now": showing_now,
                "now_day": now_day.strftime("%Y-%m-%d"),
                "now_at_iso": now_target.isoformat() if now_target else "",
                "attrs_html": attrs_html,
                "status_html": status_html,
                "events_html": events_html,
            }
        )
    if request.GET.get("partial") == "detail_events" and request.headers.get("X-Requested-With") == "XMLHttpRequest":
        return render(
            request,
            "core/apps/app_rotas/_rota_detalhe_eventos.html",
            {
                "timeline_events": detail_events_page.object_list,
                "detail_events_page": detail_events_page,
                "selected_day": selected_day,
                "selected_at_iso": selected_at.isoformat() if selected_at else "",
            },
        )

    showing_now, now_target, now_day = _timeline_now_state(selected_day, selected_at, day_start, day_end_exclusive)

    return render(
        request,
        "core/apps/app_rotas/rota_detalhe.html",
        {
            "app": app,
            "prefixo": prefix_norm,
            "route_config": route_config,
            "route_display_name": route_display_name,
            "selected_day": selected_day,
            "available_days": available_days,
            "prev_day": prev_day,
            "next_day": next_day,
            "timeline": timeline,
            "timeline_json": json.dumps([{"iso": point["iso"], "label": point["label"]} for point in timeline]),
            "selected_index": selected_index,
            "selected_point": selected_point,
            "selected_at_iso": selected_at.isoformat() if selected_at else "",
            "selected_at_label": timezone.localtime(selected_at).strftime("%d/%m/%Y %H:%M:%S") if selected_at else "-",
            "showing_now": showing_now,
            "now_day": now_day,
            "now_at_iso": now_target.isoformat() if now_target else "",
            "attrs": attrs,
            "status": status,
            "origem_codigo": origem_codigo,
            "destino_codigo": destino_codigo,
            "origem_display": origem_nome or (str(origem_codigo) if origem_codigo is not None else "--"),
            "destino_display": destino_nome or (str(destino_codigo) if destino_codigo is not None else "--"),
            "timeline_events": detail_events_page.object_list,
            "detail_events_page": detail_events_page,
            "ligada_gradient": ligada_gradient,
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


@login_required
def conexao(request):
    app = _get_rotas_app()
    if not _has_access(request.user, app):
        return HttpResponseForbidden("Sem permissao.")

    config_missing = not app.ingest_client_id or not app.ingest_agent_id
    lifebit_connected = False
    lifebit_last_seen = None
    eventos = []
    if not config_missing:
        lifebit_connected, lifebit_last_seen = _lifebit_status(app)
        rows = (
            _base_records_queryset(app)
            .filter(_lifebit_lookup_q())
            .only("payload", "created_at", "updated_at")
            .order_by("-updated_at", "-created_at")[:30]
        )
        for row in rows:
            payload = row.payload if isinstance(row.payload, dict) else {}
            ts = _extract_timestamp(payload, row)
            if ts and timezone.is_naive(ts):
                ts = timezone.make_aware(ts, timezone.get_current_timezone())
            val = _extract_value(payload)
            eventos.append(
                {
                    "timestamp_display": timezone.localtime(ts).strftime("%d/%m/%Y %H:%M:%S") if ts else "-",
                    "valor_display": str(val if val is not None else "-"),
                }
            )

    return render(
        request,
        "core/apps/app_rotas/conexao.html",
        {
            "app": app,
            "config_missing": config_missing,
            "lifebit_connected": lifebit_connected,
            "lifebit_label": "Conectado" if lifebit_connected else "Desconectado",
            "lifebit_last_seen": (
                timezone.localtime(lifebit_last_seen).strftime("%d/%m/%Y %H:%M:%S") if lifebit_last_seen else "-"
            ),
            "lifebit_timeout_seconds": LIFEBIT_TIMEOUT_SECONDS,
            "eventos": eventos,
        },
    )


@login_required
def dados(request):
    app = _get_rotas_app()
    if not _has_access(request.user, app):
        return HttpResponseForbidden("Sem permissao.")

    config_missing = not app.ingest_client_id or not app.ingest_agent_id
    base_qs = IngestRecord.objects.none()
    total_client_agent = 0
    total_with_source = 0
    sample_size = 0
    sample_parse_ok = 0
    page_obj = None
    rows = []
    source_q = (request.GET.get("source") or "").strip()
    source_id_q = (request.GET.get("source_id") or "").strip()
    tag_q = (request.GET.get("tag") or "").strip()
    valor_q = (request.GET.get("valor") or "").strip()
    prefixo_q = (request.GET.get("prefixo") or "").strip().upper()
    atributo_q = (request.GET.get("atributo") or "").strip().upper()

    if not config_missing:
        base_qs = IngestRecord.objects.filter(
            client_id=app.ingest_client_id,
            agent_id=app.ingest_agent_id,
        ).order_by("-updated_at", "-created_at")
        filtered_qs = base_qs
        if source_q:
            filtered_qs = filtered_qs.filter(source__icontains=source_q)
        if source_id_q:
            filtered_qs = filtered_qs.filter(source_id__icontains=source_id_q)
        if tag_q:
            tag_lookup = Q()
            for key in TAG_KEYS:
                tag_lookup |= Q(**{f"payload__{key}__icontains": tag_q})
            filtered_qs = filtered_qs.filter(tag_lookup)
        if valor_q:
            value_lookup = Q()
            for key in VALUE_KEYS:
                value_lookup |= Q(**{f"payload__{key}__icontains": valor_q})
            filtered_qs = filtered_qs.filter(value_lookup)
        if prefixo_q:
            prefix_lookup = Q()
            for key in TAG_KEYS:
                prefix_lookup |= Q(**{f"payload__{key}__istartswith": f"{prefixo_q}_"})
            filtered_qs = filtered_qs.filter(prefix_lookup)
        if atributo_q in {"LIGAR", "DESLIGAR", "LIGADA", "ORIGEM", "DESTINO"}:
            if atributo_q == "DESTINO":
                suffixes = ["_DESTINO", "_DESTIN"]
            else:
                suffixes = [f"_{atributo_q}"]
            attr_lookup = Q()
            for key in TAG_KEYS:
                for suffix in suffixes:
                    attr_lookup |= Q(**{f"payload__{key}__iendswith": suffix})
            filtered_qs = filtered_qs.filter(attr_lookup)

        total_client_agent = base_qs.count()
        if app.ingest_source:
            total_with_source = base_qs.filter(source=app.ingest_source).count()
        else:
            total_with_source = total_client_agent

        sample_records = list(filtered_qs.only("payload", "created_at", "updated_at", "source", "source_id")[:1200])
        sample_size = len(sample_records)
        for rec in sample_records:
            if _build_event(rec):
                sample_parse_ok += 1

        paginator = Paginator(filtered_qs.only("payload", "created_at", "updated_at", "source", "source_id"), 50)
        page_obj = paginator.get_page(request.GET.get("page", "1"))

        for rec in page_obj.object_list:
            payload = rec.payload if isinstance(rec.payload, dict) else {}
            event = _build_event(rec)
            timestamp = rec.updated_at or rec.created_at
            rows.append(
                {
                    "timestamp_display": timezone.localtime(timestamp).strftime("%d/%m/%Y %H:%M:%S") if timestamp else "-",
                    "source": rec.source,
                    "source_id": rec.source_id,
                    "tag": _extract_tag(payload),
                    "value": payload.get("Value", payload.get("value", payload.get("valor", payload.get("status", "-")))),
                    "prefixo": event["prefixo"] if event else "-",
                    "atributo": event["atributo"] if event else "-",
                }
            )

    return render(
        request,
        "core/apps/app_rotas/dados.html",
        {
            "app": app,
            "config_missing": config_missing,
            "total_client_agent": total_client_agent,
            "total_with_source": total_with_source,
            "sample_size": sample_size,
            "sample_parse_ok": sample_parse_ok,
            "page_obj": page_obj,
            "rows": rows,
            "filters": {
                "source": source_q,
                "source_id": source_id_q,
                "tag": tag_q,
                "valor": valor_q,
                "prefixo": prefixo_q,
                "atributo": atributo_q,
            },
        },
    )


@login_required
def ordenar_rotas(request):
    if request.method != "POST":
        return JsonResponse({"ok": False, "error": "method_not_allowed"}, status=405)
    app = _get_rotas_app()
    if not _has_access(request.user, app):
        return JsonResponse({"ok": False, "error": "forbidden"}, status=403)
    try:
        payload = json.loads(request.body.decode("utf-8"))
    except (ValueError, UnicodeDecodeError):
        return JsonResponse({"ok": False, "error": "invalid_json"}, status=400)
    prefixos = payload.get("prefixos", [])
    if not isinstance(prefixos, list):
        return JsonResponse({"ok": False, "error": "invalid_prefix_list"}, status=400)

    cleaned = []
    seen = set()
    for item in prefixos:
        prefixo = str(item or "").strip().upper()
        if not prefixo or prefixo in seen:
            continue
        seen.add(prefixo)
        cleaned.append(prefixo)
    if not cleaned:
        return JsonResponse({"ok": False, "error": "empty_prefix_list"}, status=400)

    existing = {cfg.prefixo: cfg for cfg in AppRotaConfig.objects.filter(app=app, prefixo__in=cleaned)}
    changed = 0
    for idx, prefixo in enumerate(cleaned, start=1):
        cfg = existing.get(prefixo)
        if not cfg:
            AppRotaConfig.objects.create(app=app, prefixo=prefixo, ordem=idx, ativo=True)
            changed += 1
            continue
        if cfg.ordem != idx:
            cfg.ordem = idx
            cfg.save(update_fields=["ordem", "atualizado_em"])
            changed += 1

    return JsonResponse({"ok": True, "updated": changed})
