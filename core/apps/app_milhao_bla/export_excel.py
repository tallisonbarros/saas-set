from collections import defaultdict
from io import BytesIO
from pathlib import Path

from openpyxl import Workbook
from openpyxl.drawing.image import Image as XLImage
from openpyxl.styles import Alignment, Font, PatternFill


BALANCE_ORDER = ("LIMBL01", "CLABL01", "CLABL02", "SECBL01", "SECBL02")
BALANCE_LABELS = {
    "LIMBL01": "MILHO",
    "SECBL01": "GERMEN",
    "SECBL02": "RESIDUO",
    "CLABL01": "MIUDO",
    "CLABL02": "GRAUDO",
}
HEADER_FILL = PatternFill(fill_type="solid", fgColor="1F2937")
HEADER_FONT = Font(color="FFFFFF", bold=True)


def _format_footer(export_dt):
    return f"exportado dia {export_dt.strftime('%d/%m/%Y %H:%M')} de setbrasil.club"


def _style_header(ws, row_idx=1):
    for cell in ws[row_idx]:
        cell.fill = HEADER_FILL
        cell.font = HEADER_FONT
        cell.alignment = Alignment(horizontal="center", vertical="center")


def _autosize_columns(ws, min_width=12, max_width=42):
    for column_cells in ws.columns:
        first_cell = column_cells[0]
        column_letter = getattr(first_cell, "column_letter", None)
        if not column_letter:
            continue
        content_width = 0
        for cell in column_cells:
            value = cell.value
            if value is None:
                continue
            content_width = max(content_width, len(str(value)))
        ws.column_dimensions[column_letter].width = max(min_width, min(max_width, content_width + 2))


def _append_footer(ws, footer_text):
    footer_row = ws.max_row + 2
    max_col = max(1, ws.max_column)
    ws.merge_cells(start_row=footer_row, start_column=1, end_row=footer_row, end_column=max_col)
    footer_cell = ws.cell(row=footer_row, column=1, value=footer_text)
    footer_cell.font = Font(italic=True, size=10, color="475569")
    footer_cell.alignment = Alignment(horizontal="left", vertical="center")


def _add_logo(ws, logo_path, anchor, width=None):
    if not logo_path:
        return
    path = Path(logo_path)
    if not path.exists():
        return
    try:
        image = XLImage(str(path))
    except Exception:
        return
    if width and image.width:
        ratio = float(width) / float(image.width)
        image.width = width
        image.height = int(image.height * ratio)
    ws.add_image(image, anchor)


def _build_summary_sheet(wb, *, filename, start_date, end_date, export_dt, logo_set_path, logo_milhao_path, footer_text):
    ws = wb.active
    ws.title = "Resumo"

    ws["A1"] = "Exportacao App Milhao BLA"
    ws["A1"].font = Font(size=16, bold=True)
    ws["A2"] = f"Arquivo: {filename}"
    ws["A2"].font = Font(size=12, bold=True)

    ws["A4"] = "Periodo inicial"
    ws["B4"] = start_date.strftime("%d/%m/%Y")
    ws["A5"] = "Periodo final"
    ws["B5"] = end_date.strftime("%d/%m/%Y")
    ws["A6"] = "Exportado em"
    ws["B6"] = export_dt.strftime("%d/%m/%Y %H:%M")

    ws.column_dimensions["A"].width = 28
    ws.column_dimensions["B"].width = 24
    ws.column_dimensions["D"].width = 8
    ws.column_dimensions["E"].width = 20
    ws.column_dimensions["F"].width = 8
    ws.column_dimensions["G"].width = 20

    _add_logo(ws, logo_set_path, "E1", width=130)
    _add_logo(ws, logo_milhao_path, "G1", width=130)

    _append_footer(ws, footer_text)


def _build_totals_by_balance_sheet(wb, *, entries, footer_text):
    ws = wb.create_sheet("Totais por balanca")
    ws.append(["Balanca", "Descricao", "Total_kg", "% do total (sem LIMBL01)"])
    _style_header(ws, 1)

    totals_map = defaultdict(float)
    for item in entries:
        balance = item.get("balance")
        totals_map[balance] += item.get("value") or 0.0

    total_without_milho = sum(value for balance, value in totals_map.items() if balance != "LIMBL01")
    for balance in BALANCE_ORDER:
        total_value = totals_map.get(balance, 0.0)
        percent = None
        if balance != "LIMBL01" and total_without_milho > 0:
            percent = (total_value / total_without_milho) * 100.0
        ws.append(
            [
                balance,
                BALANCE_LABELS.get(balance, balance),
                round(total_value, 2),
                round(percent, 2) if percent is not None else None,
            ]
        )

    ws.auto_filter.ref = f"A1:D{ws.max_row}"
    for row_idx in range(2, ws.max_row + 1):
        ws.cell(row=row_idx, column=3).number_format = "#,##0.00"
        ws.cell(row=row_idx, column=4).number_format = "0.00\\%"
    _autosize_columns(ws)
    _append_footer(ws, footer_text)


def _build_hourly_readings_sheet(wb, *, entries, footer_text):
    ws = wb.create_sheet("Leituras por hora")
    ws.append(["Data", "Hora", "Balanca", "Descricao", "Valor_kg", "Ultima_leitura_hora"])
    _style_header(ws, 1)

    if not entries:
        ws.append(["", "", "", "Sem dados no periodo selecionado.", "", ""])
        _autosize_columns(ws)
        _append_footer(ws, footer_text)
        return

    last_ingest_by_date_balance = {}
    for item in entries:
        key = (item.get("date"), item.get("balance"))
        ingest_dt = item.get("ingest_datetime") or item.get("datetime")
        current = last_ingest_by_date_balance.get(key)
        if ingest_dt and (not current or ingest_dt > current):
            last_ingest_by_date_balance[key] = ingest_dt

    for item in entries:
        item_date = item.get("date")
        balance = item.get("balance")
        last_ingest = last_ingest_by_date_balance.get((item_date, balance))
        ws.append(
            [
                item_date.strftime("%d/%m/%Y") if item_date else "",
                item.get("hour") or "",
                balance,
                BALANCE_LABELS.get(balance, balance),
                item.get("value"),
                last_ingest.strftime("%H:%M") if last_ingest else "",
            ]
        )

    ws.auto_filter.ref = f"A1:F{ws.max_row}"
    for row_idx in range(2, ws.max_row + 1):
        ws.cell(row=row_idx, column=5).number_format = "#,##0.00"
    _autosize_columns(ws)
    _append_footer(ws, footer_text)


def _build_daily_totals_sheet(wb, *, entries, footer_text):
    ws = wb.create_sheet("Totais por dia")
    ws.append(
        [
            "Data",
            "LIMBL01_kg",
            "CLABL01_kg",
            "CLABL02_kg",
            "SECBL01_kg",
            "SECBL02_kg",
            "TOTAL_sem_milho_kg",
        ]
    )
    _style_header(ws, 1)

    if not entries:
        ws.append(["Sem dados no periodo selecionado.", 0, 0, 0, 0, 0, 0])
        _autosize_columns(ws)
        _append_footer(ws, footer_text)
        return

    daily_totals = defaultdict(lambda: defaultdict(float))
    for item in entries:
        day = item.get("date")
        balance = item.get("balance")
        daily_totals[day][balance] += item.get("value") or 0.0

    for day in sorted(daily_totals.keys()):
        day_values = daily_totals[day]
        total_without_milho = (
            day_values.get("CLABL01", 0.0)
            + day_values.get("CLABL02", 0.0)
            + day_values.get("SECBL01", 0.0)
            + day_values.get("SECBL02", 0.0)
        )
        ws.append(
            [
                day.strftime("%d/%m/%Y"),
                round(day_values.get("LIMBL01", 0.0), 2),
                round(day_values.get("CLABL01", 0.0), 2),
                round(day_values.get("CLABL02", 0.0), 2),
                round(day_values.get("SECBL01", 0.0), 2),
                round(day_values.get("SECBL02", 0.0), 2),
                round(total_without_milho, 2),
            ]
        )

    ws.auto_filter.ref = f"A1:G{ws.max_row}"
    for row_idx in range(2, ws.max_row + 1):
        for column_idx in range(2, 8):
            ws.cell(row=row_idx, column=column_idx).number_format = "#,##0.00"
    _autosize_columns(ws)
    _append_footer(ws, footer_text)


def build_milhao_excel_export(
    *,
    filename,
    start_date,
    end_date,
    export_dt,
    entries,
    logo_set_path=None,
    logo_milhao_path=None,
):
    workbook = Workbook()
    footer_text = _format_footer(export_dt)

    _build_summary_sheet(
        workbook,
        filename=filename,
        start_date=start_date,
        end_date=end_date,
        export_dt=export_dt,
        logo_set_path=logo_set_path,
        logo_milhao_path=logo_milhao_path,
        footer_text=footer_text,
    )
    _build_totals_by_balance_sheet(workbook, entries=entries, footer_text=footer_text)
    _build_hourly_readings_sheet(workbook, entries=entries, footer_text=footer_text)
    _build_daily_totals_sheet(workbook, entries=entries, footer_text=footer_text)

    output = BytesIO()
    workbook.save(output)
    return output.getvalue()
