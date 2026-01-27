from datetime import datetime, timedelta

from django.contrib.auth.decorators import login_required
from django.http import HttpResponseForbidden
from django.shortcuts import get_object_or_404, render

from core.models import App, IngestRecord
from core.views import _get_cliente


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


@login_required
def dashboard(request):
    app = get_object_or_404(App, slug="appmilhaobla", ativo=True)
    cliente = _get_cliente(request.user)
    if not request.user.is_staff:
        if not cliente or not cliente.apps.filter(pk=app.pk).exists():
            return HttpResponseForbidden("Sem permissao.")

    records = IngestRecord.objects.filter(
        client_id="clienteA",
        agent_id="agente01",
        source__in=["balanca_acumulado_hora", "balanca_acumulado"],
    ).order_by("-created_at")[:2000]

    balance_labels = {
        "LIMBL01": "MILHO",
        "SECBL01": "GERMEN",
        "SECBL02": "RESIDUO",
        "CLABL01": "MIUDO",
        "CLABL02": "GRAUDO",
    }
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
        entries.append(
            {
                "balance": balance_name,
                "label": balance_labels.get(balance_name, balance_name),
                "datetime": dt,
                "date": dt.date(),
                "hour": dt.strftime("%H:%M"),
                "value": value,
            }
        )

    entries.sort(key=lambda item: (item["date"], item["hour"]))
    dates = sorted({item["date"] for item in entries})
    balances = sorted({item["balance"] for item in entries})

    selected_date_raw = request.GET.get("date", "")
    selected_balance_raw = request.GET.getlist("balance")
    if not selected_balance_raw:
        selected_balance_raw = request.GET.get("balance", "").split(",")
    selected_balances = [item.strip() for item in selected_balance_raw if item.strip()]

    selected_date = None
    if selected_date_raw:
        try:
            selected_date = datetime.strptime(selected_date_raw, "%Y-%m-%d").date()
        except ValueError:
            selected_date = None
    if not selected_date and dates:
        selected_date = dates[-1]

    valid_balances = {balance for balance in balances}
    selected_balances = [bal for bal in selected_balances if bal in valid_balances]
    if not selected_balances and balances:
        if "LIMBL01" in balances:
            selected_balances = ["LIMBL01"]
        else:
            selected_balances = [balances[0]]

    filtered = [
        item
        for item in entries
        if (not selected_date or item["date"] == selected_date)
        and (not selected_balances or item["balance"] in selected_balances)
    ]
    total_value = sum(item["value"] or 0 for item in filtered) if filtered else 0
    totals_by_balance = {}
    for item in filtered:
        balance = item["balance"]
        totals_by_balance.setdefault(balance, 0)
        totals_by_balance[balance] += item["value"] or 0
    totals_by_balance = [
        {
            "balance": balance,
            "label": balance_labels.get(balance, balance),
            "total": totals_by_balance[balance],
        }
        for balance in sorted(totals_by_balance.keys())
    ]
    latest_value = filtered[-1]["value"] if filtered else None
    latest_datetime = filtered[-1]["datetime"] if filtered else None
    latest_by_balance_map = {}
    for item in filtered:
        latest_by_balance_map[item["balance"]] = item
    latest_by_balance = [
        {
            "balance": balance,
            "label": balance_labels.get(balance, balance),
            "value": latest_by_balance_map[balance]["value"],
            "datetime": latest_by_balance_map[balance]["datetime"],
        }
        for balance in sorted(latest_by_balance_map.keys())
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

    avg_total_14 = None
    avg_by_balance = {}
    if selected_date and selected_balances:
        window_end = selected_date
        window_start = selected_date - timedelta(days=13)
        window_dates = [window_start + timedelta(days=offset) for offset in range(14)]
        window_set = set(window_dates)
        daily_total = {day: 0.0 for day in window_dates}
        daily_by_balance = {balance: {day: 0.0 for day in window_dates} for balance in selected_balances}
        for item in entries:
            if item["date"] not in window_set:
                continue
            if item["balance"] not in selected_balances:
                continue
            value = item["value"] or 0
            daily_total[item["date"]] += value
            daily_by_balance[item["balance"]][item["date"]] += value
        total_days = [value for value in daily_total.values() if value > 0]
        if total_days:
            avg_total_14 = sum(total_days) / len(total_days)
        avg_by_balance = {}
        for balance, totals in daily_by_balance.items():
            balance_days = [value for value in totals.values() if value > 0]
            if balance_days:
                avg_by_balance[balance] = sum(balance_days) / len(balance_days)

    totals_by_balance = [
        {
            "balance": item["balance"],
            "label": item["label"],
            "total": item["total"],
            "avg_14": avg_by_balance.get(item["balance"]),
        }
        for item in totals_by_balance
    ]

    return render(
        request,
        "core/apps/app_milhao_bla/dashboard.html",
        {
            "app": app,
            "theme_color": app.theme_color,
            "icon": app.icon,
            "entries": filtered,
            "dates": dates,
            "balances": balances,
            "selected_date": selected_date,
            "selected_balances": selected_balances,
            "total_value": total_value,
            "totals_by_balance": totals_by_balance,
            "latest_value": latest_value,
            "latest_datetime": latest_datetime,
            "latest_by_balance": latest_by_balance,
            "avg_total_14": avg_total_14,
            "composition": composition,
        },
    )
