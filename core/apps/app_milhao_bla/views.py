from datetime import datetime
from pathlib import Path

from django.conf import settings
from django.contrib.auth.decorators import login_required
from django.db.models import Q
from django.http import HttpResponse, HttpResponseForbidden, JsonResponse
from django.template.loader import render_to_string
from django.shortcuts import get_object_or_404, render
from django.urls import reverse
from django.utils import timezone
from django.views.decorators.http import require_POST

from core.models import AdminAccessLog, App, AppMilhaoBlaMuralDia, AppMilhaoBlaMuralDiaLeitura, IngestRecord
from core.views import _get_cliente, _is_admin_user, _is_dev_user
from .export_excel import build_milhao_excel_export


DEFAULT_CLIENT_ID = "clienteA"
DEFAULT_AGENT_ID = "agente01"
DEFAULT_SOURCES = ("balanca_acumulado_hora", "balanca_acumulado")
MAX_EXPORT_RANGE_DAYS = 93
MURAL_ACCESS_AUDIT_MODULE = "apps:appmilhaobla:mural_dia"
EXPORT_ACCESS_AUDIT_MODULE = "apps:appmilhaobla:export_excel"
BALANCE_LABELS = {
    "LIMBL01": "MILHO",
    "SECBL01": "GERMEN",
    "SECBL02": "RESIDUO",
    "CLABL01": "MIUDO",
    "CLABL02": "GRAUDO",
}


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


def _parse_yyyy_mm_dd(value):
    if not value:
        return None
    try:
        return datetime.strptime(str(value).strip(), "%Y-%m-%d").date()
    except ValueError:
        return None


def _format_kg(value):
    if value is None:
        return None
    try:
        rounded = round(float(value))
    except (TypeError, ValueError):
        return None
    return f"{rounded:,.0f}".replace(",", ".")


def _normalize_mural_visibility(raw_value):
    value = str(raw_value or "").strip().upper()
    if value not in dict(AppMilhaoBlaMuralDia.Visibilidade.choices):
        return AppMilhaoBlaMuralDia.Visibilidade.PUBLICA
    return value


def _mural_visibility_label(value):
    if value == AppMilhaoBlaMuralDia.Visibilidade.PRIVADA:
        return "Apenas para mim"
    return "Publicar para todos"


def _mural_visibility_badge_label(value):
    if value == AppMilhaoBlaMuralDia.Visibilidade.PRIVADA:
        return "so eu"
    return "para todos"


def _visible_mural_notes_qs(user):
    visibility_public = AppMilhaoBlaMuralDia.Visibilidade.PUBLICA
    base_qs = AppMilhaoBlaMuralDia.objects.select_related("autor", "autor__perfilusuario")
    if not user or not user.is_authenticated:
        return base_qs.filter(visibilidade=visibility_public)
    if _is_dev_user(user):
        return base_qs
    return base_qs.filter(Q(visibilidade=visibility_public) | Q(autor=user))


def _register_app_access_event(user, module):
    if not user or not user.is_authenticated or not module:
        return None
    return AdminAccessLog.objects.create(user=user, module=module)


def _get_mural_author_label(user):
    perfil = getattr(user, "perfilusuario", None)
    if perfil and (perfil.nome or "").strip():
        return perfil.nome.strip()
    full_name = (user.get_full_name() or "").strip()
    return full_name or user.username


def _serialize_mural_note(note, request_user):
    created_at = note.criado_em
    if created_at and timezone.is_aware(created_at):
        created_at = timezone.localtime(created_at)
    return {
        "id": note.id,
        "author_label": _get_mural_author_label(note.autor),
        "visibility": note.visibilidade,
        "visibility_label": _mural_visibility_label(note.visibilidade),
        "visibility_badge_label": _mural_visibility_badge_label(note.visibilidade),
        "text": (note.texto or "").strip(),
        "created_at": created_at,
        "created_at_label": created_at.strftime("%d/%m/%Y %H:%M") if created_at else "",
        "created_at_date_label": created_at.strftime("%d/%m/%Y") if created_at else "",
        "time_label": created_at.strftime("%H:%M") if created_at else "",
        "is_own": bool(request_user and request_user.is_authenticated and note.autor_id == request_user.id),
        "can_delete": bool(request_user and request_user.is_authenticated and note.autor_id == request_user.id),
        "delete_url": "",
    }


def _load_mural_notes_for_day(selected_date, request_user):
    if not selected_date:
        return []
    rows = _visible_mural_notes_qs(request_user).filter(data_referencia=selected_date).order_by("criado_em", "id")
    notes = [_serialize_mural_note(row, request_user) for row in rows]
    for note in notes:
        note["delete_url"] = reverse("app_milhao_bla_mural_day_delete", args=[note["id"]])
    return notes


def _latest_visible_mural_note(selected_date, request_user):
    if not selected_date:
        return None
    return (
        _visible_mural_notes_qs(request_user)
        .filter(data_referencia=selected_date)
        .order_by("-criado_em", "-id")
        .first()
    )


def _mural_day_has_unread(selected_date, request_user):
    if not selected_date or not request_user or not request_user.is_authenticated:
        return False
    latest_note = _latest_visible_mural_note(selected_date, request_user)
    if not latest_note or not latest_note.criado_em:
        return False
    leitura = (
        AppMilhaoBlaMuralDiaLeitura.objects.filter(
            usuario=request_user,
            data_referencia=selected_date,
        )
        .only("visualizado_em")
        .first()
    )
    if not leitura or not leitura.visualizado_em:
        return True
    visualizado_em = leitura.visualizado_em
    if timezone.is_aware(visualizado_em):
        visualizado_em = timezone.localtime(visualizado_em)
    latest_created_at = latest_note.criado_em
    if timezone.is_aware(latest_created_at):
        latest_created_at = timezone.localtime(latest_created_at)
    return latest_created_at > visualizado_em


def _mark_mural_day_viewed(request_user, selected_date):
    if not selected_date or not request_user or not request_user.is_authenticated:
        return None
    leitura, _ = AppMilhaoBlaMuralDiaLeitura.objects.update_or_create(
        usuario=request_user,
        data_referencia=selected_date,
        defaults={"visualizado_em": timezone.now()},
    )
    return leitura


def _render_mural_notes_html(request, *, selected_date, mural_notes):
    return render_to_string(
        "core/apps/app_milhao_bla/_mural_day_notes.html",
        {
            "selected_date": selected_date,
            "mural_notes": mural_notes,
        },
        request=request,
    )


def _resolve_ingest_config(app):
    ingest_client_id = (app.ingest_client_id or "").strip() or DEFAULT_CLIENT_ID
    ingest_agent_id = (app.ingest_agent_id or "").strip() or DEFAULT_AGENT_ID
    ingest_sources = _normalize_sources(app.ingest_source)
    return ingest_client_id, ingest_agent_id, ingest_sources


def _load_entries_for_app(app, *, limit=2000, start_date=None, end_date=None):
    ingest_client_id, ingest_agent_id, ingest_sources = _resolve_ingest_config(app)
    records_qs = IngestRecord.objects.filter(
        client_id=ingest_client_id,
        agent_id=ingest_agent_id,
        source__in=ingest_sources,
    ).order_by("-created_at")
    if limit is not None:
        records_iter = records_qs[:limit]
    else:
        records_iter = records_qs.iterator(chunk_size=2000)

    entries = []
    for record in records_iter:
        payload = record.payload if isinstance(record.payload, dict) else {}
        tag_name = payload.get("TagName") or payload.get("tagname")
        balance_name = _extract_balance_name(tag_name)
        if not balance_name:
            continue
        hora = payload.get("Hora") or payload.get("DataHoraBase") or payload.get("datahora")
        dt = _parse_iso_datetime(hora)
        if not dt:
            continue

        item_date = dt.date()
        if start_date and item_date < start_date:
            continue
        if end_date and item_date > end_date:
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
                "label": BALANCE_LABELS.get(balance_name, balance_name),
                "datetime": dt,
                "date": item_date,
                "hour": dt.strftime("%H:%M"),
                "ingest_datetime": ingest_dt,
                "ingest_time": ingest_dt.strftime("%H:%M") if ingest_dt else None,
                "value": value,
                "value_display": _format_kg(value),
            }
        )

    entries.sort(key=lambda item: (item["date"], item["hour"], item["balance"]))
    return entries, ingest_client_id, ingest_agent_id, ingest_sources


def _get_app_if_allowed(request):
    app = get_object_or_404(App, slug="appmilhaobla", ativo=True)
    cliente = _get_cliente(request.user)
    if _is_admin_user(request.user):
        return app
    if not cliente or not cliente.apps.filter(pk=app.pk).exists():
        return None
    return app


def _build_dashboard_context(request, app):
    entries, ingest_client_id, ingest_agent_id, ingest_sources = _load_entries_for_app(app, limit=2000)
    mural_dates = set(
        _visible_mural_notes_qs(request.user)
        .order_by("data_referencia")
        .values_list("data_referencia", flat=True)
        .distinct()
    )
    dates = sorted({item["date"] for item in entries} | mural_dates)
    balances = sorted({item["balance"] for item in entries})

    selected_date = _parse_yyyy_mm_dd(request.GET.get("date", ""))
    if not selected_date and dates:
        selected_date = dates[-1]
    if not selected_date:
        selected_date = timezone.localdate()

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
            "label": BALANCE_LABELS.get(balance, balance),
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
            "label": BALANCE_LABELS.get(balance, balance),
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
            "label": BALANCE_LABELS.get(balance, balance),
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

    mural_notes = _load_mural_notes_for_day(selected_date, request.user)
    mural_has_unread = _mural_day_has_unread(selected_date, request.user)

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
        "mural_notes": mural_notes,
        "mural_notes_count": len(mural_notes),
        "mural_has_unread": mural_has_unread,
        "mural_visibility_options": [
            {
                "value": AppMilhaoBlaMuralDia.Visibilidade.PUBLICA,
                "label": _mural_visibility_label(AppMilhaoBlaMuralDia.Visibilidade.PUBLICA),
            },
            {
                "value": AppMilhaoBlaMuralDia.Visibilidade.PRIVADA,
                "label": _mural_visibility_label(AppMilhaoBlaMuralDia.Visibilidade.PRIVADA),
            },
        ],
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
            "export_max_days": MAX_EXPORT_RANGE_DAYS,
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


@login_required
@require_POST
def mural_day_access(request):
    app = _get_app_if_allowed(request)
    if not app:
        return JsonResponse({"ok": False, "error": "forbidden"}, status=403)
    _register_app_access_event(request.user, MURAL_ACCESS_AUDIT_MODULE)
    return JsonResponse(
        {
            "ok": True,
        }
    )


@login_required
@require_POST
def export_excel_access(request):
    app = _get_app_if_allowed(request)
    if not app:
        return JsonResponse({"ok": False, "error": "forbidden"}, status=403)
    _register_app_access_event(request.user, EXPORT_ACCESS_AUDIT_MODULE)
    return JsonResponse({"ok": True})


@login_required
@require_POST
def mural_day_create(request):
    app = _get_app_if_allowed(request)
    if not app:
        return JsonResponse({"ok": False, "error": "forbidden"}, status=403)

    selected_date = _parse_yyyy_mm_dd(request.POST.get("data_referencia", "")) or timezone.localdate()
    text = request.POST.get("texto", "").strip()
    visibility = _normalize_mural_visibility(request.POST.get("visibilidade"))

    if not text:
        return JsonResponse({"ok": False, "error": "Informe o texto da nota."}, status=400)

    note = AppMilhaoBlaMuralDia.objects.create(
        data_referencia=selected_date,
        texto=text,
        visibilidade=visibility,
        autor=request.user,
    )
    _mark_mural_day_viewed(request.user, selected_date)
    mural_notes = _load_mural_notes_for_day(selected_date, request.user)
    return JsonResponse(
        {
            "ok": True,
            "note_id": note.id,
            "notes_count": len(mural_notes),
            "has_unread": False,
            "list_html": _render_mural_notes_html(
                request,
                selected_date=selected_date,
                mural_notes=mural_notes,
            ),
        }
    )


@login_required
@require_POST
def mural_day_delete(request, note_id):
    app = _get_app_if_allowed(request)
    if not app:
        return JsonResponse({"ok": False, "error": "forbidden"}, status=403)

    note = get_object_or_404(AppMilhaoBlaMuralDia, pk=note_id)
    if note.autor_id != request.user.id:
        return JsonResponse({"ok": False, "error": "Somente o autor pode excluir esta nota."}, status=403)

    selected_date = note.data_referencia
    note.delete()
    mural_notes = _load_mural_notes_for_day(selected_date, request.user)
    return JsonResponse(
        {
            "ok": True,
            "deleted_id": note_id,
            "notes_count": len(mural_notes),
            "has_unread": _mural_day_has_unread(selected_date, request.user),
            "list_html": _render_mural_notes_html(
                request,
                selected_date=selected_date,
                mural_notes=mural_notes,
            ),
        }
    )


@login_required
@require_POST
def mural_day_mark_viewed(request):
    app = _get_app_if_allowed(request)
    if not app:
        return JsonResponse({"ok": False, "error": "forbidden"}, status=403)

    selected_date = _parse_yyyy_mm_dd(request.POST.get("data_referencia", "")) or timezone.localdate()
    _mark_mural_day_viewed(request.user, selected_date)
    return JsonResponse({"ok": True, "has_unread": False})


@login_required
def mural_day_live(request):
    app = _get_app_if_allowed(request)
    if not app:
        return JsonResponse({"ok": False, "error": "forbidden"}, status=403)

    selected_date = _parse_yyyy_mm_dd(request.GET.get("date", "")) or timezone.localdate()
    current_latest_note_id_raw = request.GET.get("latest_note_id", "").strip()
    try:
        current_latest_note_id = int(current_latest_note_id_raw) if current_latest_note_id_raw else None
    except ValueError:
        current_latest_note_id = None

    latest_note = _latest_visible_mural_note(selected_date, request.user)
    latest_note_id = latest_note.id if latest_note else None
    has_changed = latest_note_id != current_latest_note_id
    has_unread = _mural_day_has_unread(selected_date, request.user)

    payload = {
        "ok": True,
        "has_changed": has_changed,
        "has_unread": has_unread,
        "latest_note_id": latest_note_id,
    }
    if has_changed:
        mural_notes = _load_mural_notes_for_day(selected_date, request.user)
        payload.update(
            {
                "notes_count": len(mural_notes),
                "list_html": _render_mural_notes_html(
                    request,
                    selected_date=selected_date,
                    mural_notes=mural_notes,
                ),
            }
        )
    return JsonResponse(payload)


@login_required
@require_POST
def export_excel(request):
    app = _get_app_if_allowed(request)
    if not app:
        return JsonResponse({"ok": False, "error": "forbidden"}, status=403)

    start_date = _parse_yyyy_mm_dd(request.POST.get("start_date"))
    end_date = _parse_yyyy_mm_dd(request.POST.get("end_date"))
    if not start_date or not end_date:
        return JsonResponse({"ok": False, "error": "Intervalo invalido."}, status=400)
    if start_date > end_date:
        return JsonResponse({"ok": False, "error": "Data inicial maior que data final."}, status=400)

    selected_days = (end_date - start_date).days + 1
    if selected_days > MAX_EXPORT_RANGE_DAYS:
        return JsonResponse(
            {
                "ok": False,
                "error": f"Intervalo maximo: {MAX_EXPORT_RANGE_DAYS} dias.",
            },
            status=400,
        )

    entries, _, _, _ = _load_entries_for_app(
        app,
        limit=None,
        start_date=start_date,
        end_date=end_date,
    )
    filename = f"milhao_bla_{start_date.strftime('%Y%m%d')}_a_{end_date.strftime('%Y%m%d')}.xlsx"
    export_dt = timezone.localtime(timezone.now())

    base_dir = Path(settings.BASE_DIR)
    logo_set_path = base_dir / "core" / "static" / "core" / "logoset.png"
    logo_milhao_path = base_dir / "core" / "apps" / "app_milhao_bla" / "static" / "app_milhao_bla" / "milhao_logo.png"
    try:
        file_content = build_milhao_excel_export(
            filename=filename,
            start_date=start_date,
            end_date=end_date,
            export_dt=export_dt,
            entries=entries,
            logo_set_path=logo_set_path,
            logo_milhao_path=logo_milhao_path,
        )
    except Exception:
        return JsonResponse({"ok": False, "error": "Falha ao gerar arquivo Excel."}, status=500)

    response = HttpResponse(
        file_content,
        content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
    response["Content-Disposition"] = f'attachment; filename="{filename}"'
    response["Cache-Control"] = "no-store"
    return response
