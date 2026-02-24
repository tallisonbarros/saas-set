from datetime import datetime

from django.contrib.auth.decorators import login_required
from django.http import HttpResponseForbidden, JsonResponse
from django.shortcuts import get_object_or_404, render
from django.utils import timezone

from core.models import App, IngestRecord
from core.views import _get_cliente


DEFAULT_CLIENT_ID = "clienteA"
DEFAULT_AGENT_ID = "agente01"
DEFAULT_SOURCES = ("balanca_acumulado_hora", "balanca_acumulado")


def _normalize_sources(raw_source):
    text = str(raw_source or "").strip()
    if not text:
        return list(DEFAULT_SOURCES)
    parts = [item.strip() for item in text.replace(";", ",").split(",") if item.strip()]
    return parts or list(DEFAULT_SOURCES)


def _parse_iso_datetime(value):
    if not value:
        return None
    if isinstance(value, datetime):
        return value
    text = str(value).strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None


def _extract_balance_name(tag_name):
    if not tag_name:
        return None
    tag_upper = str(tag_name).upper()
    for name in ("LIMBL01", "CLABL01", "CLABL02", "SECBL01", "SECBL02"):
        if name in tag_upper:
            return name
    return None


def _format_kg(value):
    if value is None:
        return None
    try:
        rounded = round(float(value))
    except (TypeError, ValueError):
        return None
    return f"{rounded:,.0f}".replace(",", ".")


def _get_app_if_allowed(request):
    app = get_object_or_404(App, slug="appmilhaobla", ativo=True)
    cliente = _get_cliente(request.user)
    if request.user.is_staff:
        return app
    if not cliente or not cliente.apps.filter(pk=app.pk).exists():
        return None
    return app


def _build_dashboard_context(request, app):
    balance_labels = {
        "LIMBL01": "MILHO",
        "SECBL01": "GERMEN",
        "SECBL02": "RESIDUO",
        "CLABL01": "MIUDO",
        "CLABL02": "GRAUDO",
    }

    ingest_client_id = (app.ingest_client_id or "").strip() or DEFAULT_CLIENT_ID
    ingest_agent_id = (app.ingest_agent_id or "").strip() or DEFAULT_AGENT_ID
    ingest_sources = _normalize_sources(app.ingest_source)
    records = IngestRecord.objects.filter(
        client_id=ingest_client_id,
        agent_id=ingest_agent_id,
        source__in=ingest_sources,
    ).order_by("-created_at")[:2000]
    entries = []
    for record in records:
        payload = record.payload if isinstance(record.payload, dict) else {}
        tag_name = payload.get("TagName") or payload.get("tagname")
        balance_name = _extract_balance_name(tag_name)
        if not balance_name:
            continue
        hora = payload.get("Hora") or payload.get("DataHoraBase") or payload.get("datahora")
        dt = _parse_iso_datetime(hora)
        if not dt:
            continue
        value = payload.get("ProducaoHora")
        if value is None:
            value = payload.get("Delta")
        try:
            value = float(value) if value is not None else None
        except (TypeError, ValueError):
            value = None
        ingest_dt = record.updated_at or record.created_at
        if ingest_dt and timezone.is_aware(ingest_dt):
            ingest_dt = timezone.localtime(ingest_dt)
        entries.append(
            {
                "balance": balance_name,
                "label": balance_labels.get(balance_name, balance_name),
                "datetime": dt,
                "date": dt.date(),
                "hour": dt.strftime("%H:%M"),
                "ingest_datetime": ingest_dt,
                "ingest_time": ingest_dt.strftime("%H:%M") if ingest_dt else None,
                "value": value,
                "value_display": _format_kg(value),
            }
        )

    entries.sort(key=lambda item: (item["date"], item["hour"]))
    dates = sorted({item["date"] for item in entries})
    balances = sorted({item["balance"] for item in entries})

    selected_date_raw = request.GET.get("date", "")
    selected_date = None
    if selected_date_raw:
        try:
            selected_date = datetime.strptime(selected_date_raw, "%Y-%m-%d").date()
        except ValueError:
            selected_date = None
    if not selected_date and dates:
        selected_date = dates[-1]

    # Sem seletor de balanca: sempre exibimos todas as balancas disponiveis.
    selected_balances = list(balances)

    filtered = [
        item
        for item in entries
        if (not selected_date or item["date"] == selected_date)
    ]

    last_by_balance = {}
    for item in filtered:
        balance = item["balance"]
        ingest_dt = item.get("ingest_datetime") or item["datetime"]
        current = last_by_balance.get(balance)
        if ingest_dt and (not current or ingest_dt > current):
            last_by_balance[balance] = ingest_dt
    last_ingests = [
        {
            "balance": balance,
            "label": balance_labels.get(balance, balance),
            "time": last_by_balance[balance].strftime("%H:%M"),
        }
        for balance in sorted(last_by_balance.keys())
    ]

    prev_date = None
    next_date = None
    if selected_date and dates:
        try:
            idx = dates.index(selected_date)
            if idx > 0:
                prev_date = dates[idx - 1]
            if idx < len(dates) - 1:
                next_date = dates[idx + 1]
        except ValueError:
            pass
    milho_total = sum((item["value"] or 0) for item in filtered if item["balance"] == "LIMBL01") if filtered else 0
    total_sem_milho = sum((item["value"] or 0) for item in filtered if item["balance"] != "LIMBL01") if filtered else 0
    total_value = milho_total
    total_value_display = _format_kg(milho_total)
    totals_by_balance = {}
    for item in filtered:
        balance = item["balance"]
        totals_by_balance.setdefault(balance, 0)
        totals_by_balance[balance] += item["value"] or 0
    totals_by_balance_items = [
        {
            "balance": balance,
            "label": balance_labels.get(balance, balance),
            "total": totals_by_balance[balance],
            "total_display": _format_kg(totals_by_balance[balance]),
        }
        for balance in sorted(totals_by_balance.keys())
        if balance != "LIMBL01"
    ]
    totals_by_balance = totals_by_balance_items + [
        {
            "balance": "TOTAL",
            "label": "TOTAL",
            "total": total_sem_milho,
            "total_display": _format_kg(total_sem_milho),
        }
    ]

    composition_source = [
        item
        for item in entries
        if (not selected_date or item["date"] == selected_date)
    ]
    composition_totals = {}
    for item in composition_source:
        balance = item["balance"]
        if balance == "LIMBL01":
            continue
        composition_totals.setdefault(balance, 0)
        composition_totals[balance] += item["value"] or 0
    composition_items = [
        {
            "balance": balance,
            "label": balance_labels.get(balance, balance),
            "total": composition_totals[balance],
        }
        for balance in sorted(composition_totals.keys())
    ]
    composition_total = sum(item["total"] for item in composition_items)
    composition = []
    if composition_items and composition_total > 0:
        running = 0.0
        for idx, item in enumerate(composition_items):
            if idx == len(composition_items) - 1:
                percent = round(100.0 - running, 1)
            else:
                percent = round((item["total"] / composition_total) * 100.0, 1)
                running += percent
            composition.append(
                {
                    "balance": item["balance"],
                    "label": item["label"],
                    "percent": percent,
                    "percent_str": f"{percent:.1f}",
                }
            )

    return {
        "entries": filtered,
        "dates": dates,
        "balances": balances,
        "selected_date": selected_date,
        "selected_balances": selected_balances,
        "total_value": total_value,
        "total_value_display": total_value_display,
        "total_sem_milho": total_sem_milho,
        "total_sem_milho_display": _format_kg(total_sem_milho),
        "totals_by_balance": totals_by_balance,
        "composition": composition,
        "prev_date": prev_date,
        "next_date": next_date,
        "last_ingests": last_ingests,
        "ingest_client_id": ingest_client_id,
        "ingest_agent_id": ingest_agent_id,
        "ingest_sources_display": ", ".join(ingest_sources),
    }


@login_required
def dashboard(request):
    app = _get_app_if_allowed(request)
    if not app:
        return HttpResponseForbidden("Sem permissao.")
    context = _build_dashboard_context(request, app)
    context.update(
        {
            "app": app,
            "theme_color": app.theme_color,
            "icon": app.icon,
        }
    )
    return render(request, "core/apps/app_milhao_bla/dashboard.html", context)


@login_required
def dashboard_cards_data(request):
    app = _get_app_if_allowed(request)
    if not app:
        return JsonResponse({"ok": False, "error": "forbidden"}, status=403)
    context = _build_dashboard_context(request, app)
    return JsonResponse(
        {
            "ok": True,
            "updated_at": timezone.localtime(timezone.now()).strftime("%H:%M:%S"),
            "total_value_display": context["total_value_display"],
            "totals_by_balance": [
                {
                    "balance": item["balance"],
                    "label": item["label"],
                    "total_display": item["total_display"],
                }
                for item in context["totals_by_balance"]
            ],
            "composition": [
                {
                    "balance": item["balance"],
                    "label": item["label"],
                    "percent_str": item["percent_str"],
                }
                for item in context["composition"]
            ],
            "last_ingests": [
                {
                    "balance": item["balance"],
                    "label": item["label"],
                    "time": item["time"],
                }
                for item in context["last_ingests"]
            ],
        }
    )
