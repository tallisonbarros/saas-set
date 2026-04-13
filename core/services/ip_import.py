import csv
import hashlib
import ipaddress
import json
import os
import re
import socket
import time
import unicodedata
from collections import Counter, defaultdict
from dataclasses import dataclass
from io import BytesIO, StringIO
from pathlib import Path
from urllib import error as urlerror
from urllib import request as urlrequest

from openpyxl import load_workbook

from core.models import ListaIP, ListaIPID, ListaIPItem


DEFAULT_AI_REQUEST_TIMEOUT_SECONDS = 25.0
DEFAULT_AI_TOTAL_BUDGET_SECONDS = 180.0
DEFAULT_AI_MAX_SHEETS_PER_JOB = 24


HEADER_ALIASES = {
    "list_name": [
        "LISTA",
        "LIST NAME",
        "LISTA IP",
        "NOME LISTA",
        "NETWORK",
        "SUBNET",
        "SEGMENT",
        "AREA",
        "PANEL",
    ],
    "list_code": ["ID_LISTAIP", "LIST ID", "LISTA ID", "LIST CODE", "CODIGO", "CODE"],
    "ip": ["IP", "IP ADDRESS", "ENDERECO IP", "ENDERECO", "HOST IP", "HOST"],
    "device_name": [
        "DEVICE",
        "DEVICE NAME",
        "NOME EQUIPAMENTO",
        "EQUIPAMENTO",
        "EQP",
        "EQUIP",
        "EQUIPAMENTO TAG",
        "HOSTNAME",
        "NODE",
        "TAG",
        "TAG NAME",
    ],
    "description": ["DESCRIPTION", "DESCRICAO", "DESC", "SERVICE", "SERVICO"],
    "mac": ["MAC", "MAC ADDRESS", "ENDERECO MAC"],
    "protocol": ["PROTOCOL", "PROTOCOLO", "SERVICE TYPE"],
    "controller": ["PLC", "CONTROLADOR", "CPU", "CLP"],
    "drive": ["DRIVE", "INVERSOR", "ACIONAMENTO"],
    "status_note": ["STATUS", "STATE", "SITUACAO"],
    "novelty": ["NOVO", "NEW"],
    "range_start": ["FAIXA INICIO", "IP INICIAL", "START IP", "RANGE START", "IP START"],
    "range_end": ["FAIXA FIM", "IP FINAL", "END IP", "RANGE END", "IP END"],
}

DEFAULT_HEADER_PROMPT = (
    "Map the spreadsheet headers to the SAAS-SET IP import schema. "
    "Prefer explicit engineering columns such as list_name, list_code, ip, device_name, description, mac, protocol, range_start and range_end. "
    "Only return mappings that are strongly supported by the sheet."
)

DEFAULT_GROUPING_PROMPT = (
    "Given normalized IP rows, suggest the best list grouping and a stable default list name when the spreadsheet omits it. "
    "Do not invent IP addresses or override explicit values."
)


class IPImportError(Exception):
    pass


@dataclass
class ParsedSpreadsheet:
    file_format: str
    sheet_name: str
    header_row_index: int
    rows_total: int
    headers: list
    raw_rows: list
    column_map: dict
    warnings: list
    suggested_list_name: str


def _ascii_upper(value):
    normalized = unicodedata.normalize("NFKD", str(value or ""))
    normalized = normalized.encode("ascii", "ignore").decode("ascii")
    return normalized.strip().upper()


def _cell_to_text(value):
    if value is None:
        return ""
    if isinstance(value, bool):
        return "SIM" if value else "NAO"
    text = str(value).strip()
    if not text:
        return ""
    return re.sub(r"\s+", " ", text)


def _read_positive_float_env(name, default):
    raw_value = str(os.environ.get(name, "")).strip()
    if not raw_value:
        return default
    try:
        parsed = float(raw_value)
    except (TypeError, ValueError):
        return default
    return parsed if parsed > 0 else default


def _read_positive_int_env(name, default):
    raw_value = str(os.environ.get(name, "")).strip()
    if not raw_value:
        return default
    try:
        parsed = int(raw_value)
    except (TypeError, ValueError):
        return default
    return parsed if parsed > 0 else default


def _ai_request_timeout_seconds():
    return _read_positive_float_env("IP_IMPORT_AI_TIMEOUT_SECONDS", DEFAULT_AI_REQUEST_TIMEOUT_SECONDS)


def _ai_total_budget_seconds():
    return _read_positive_float_env("IP_IMPORT_AI_TOTAL_BUDGET_SECONDS", DEFAULT_AI_TOTAL_BUDGET_SECONDS)


def _ai_max_sheets_per_job():
    return _read_positive_int_env("IP_IMPORT_AI_MAX_SHEETS_PER_JOB", DEFAULT_AI_MAX_SHEETS_PER_JOB)


def _compact_token(value):
    return re.sub(r"[^A-Z0-9]+", "", _ascii_upper(value))


def _header_tokens(value):
    return [token for token in re.split(r"[^A-Z0-9]+", _ascii_upper(value)) if token]


def _alias_match_score(header_value, alias):
    header_compact = _compact_token(header_value)
    alias_token = _compact_token(alias)
    if not header_compact or not alias_token:
        return 0
    header_words = set(_header_tokens(header_value))
    if header_compact == alias_token:
        return 100
    if alias_token in header_words:
        return 80
    if len(alias_token) >= 4 and alias_token in header_compact:
        return 60
    return 0


def _non_empty_cells(row):
    return sum(1 for item in row if _cell_to_text(item))


def _score_header_row(row):
    score = 0
    for cell in row:
        text = _cell_to_text(cell)
        if not text:
            continue
        for aliases in HEADER_ALIASES.values():
            score += max(_alias_match_score(text, alias) for alias in aliases)
    return score


def _detect_header_row(rows):
    candidates = []
    for index, row in enumerate(rows[:20]):
        candidates.append((_score_header_row(row), _non_empty_cells(row), -index, index))
    best = max(candidates, default=(0, 0, 0, 0))
    if best[0] <= 0:
        return 0
    return best[3]


def _resolve_file_format(original_filename):
    suffix = Path(original_filename or "").suffix.lower()
    if suffix in {".xlsx", ".xlsm"}:
        return "xlsx"
    if suffix == ".csv":
        return "csv"
    if suffix == ".tsv":
        return "tsv"
    return "unknown"


def _guess_delimiter(sample_text):
    try:
        dialect = csv.Sniffer().sniff(sample_text, delimiters=",;\t|")
        return dialect.delimiter
    except csv.Error:
        if sample_text.count(";") > sample_text.count(","):
            return ";"
        if sample_text.count("\t") > sample_text.count(","):
            return "\t"
        return ","


def _read_csv_rows(raw_bytes, file_format):
    decoded = None
    for encoding in ("utf-8-sig", "utf-8", "latin-1"):
        try:
            decoded = raw_bytes.decode(encoding)
            break
        except UnicodeDecodeError:
            continue
    if decoded is None:
        raise IPImportError("Nao foi possivel decodificar o arquivo CSV.")
    delimiter = "\t" if file_format == "tsv" else _guess_delimiter(decoded[:4000])
    reader = csv.reader(StringIO(decoded), delimiter=delimiter)
    return [[_cell_to_text(cell) for cell in row] for row in reader]


def _read_xlsx_rows(raw_bytes):
    workbook = load_workbook(filename=BytesIO(raw_bytes), data_only=True, read_only=True)
    sheets = []
    for worksheet in workbook.worksheets:
        rows = []
        for row in worksheet.iter_rows(values_only=True):
            rows.append([_cell_to_text(cell) for cell in row])
        sheets.append({"name": worksheet.title, "rows": rows})
    return sheets


def _clean_list_name(value):
    text = _cell_to_text(value)
    if not text:
        return ""
    text = re.sub(r"\.(xlsx|xlsm|csv|tsv)$", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\b(?:ABA|SHEET)\s*\d+\b", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\s+", " ", text).strip(" -_/")
    return text[:120]


def _suggest_list_name(sheet_name, original_filename):
    sheet_label = _clean_list_name(sheet_name)
    if sheet_label and not re.fullmatch(r"(sheet|planilha|arquivo)\s*\d*", _ascii_upper(sheet_label)):
        return sheet_label
    stem = _clean_list_name(Path(original_filename or "").stem)
    return stem or "Lista importada"


def _detect_column_map(headers, ai_result=None):
    detected = {}
    confidence = {}
    ai_headers = (ai_result or {}).get("column_map") or {}
    for field_name, aliases in HEADER_ALIASES.items():
        best = None
        for index, header in enumerate(headers):
            header_text = _cell_to_text(header)
            if not header_text:
                continue
            score = max(_alias_match_score(header_text, alias) for alias in aliases)
            ai_header = _cell_to_text(ai_headers.get(field_name))
            if ai_header and _compact_token(ai_header) == _compact_token(header_text):
                score = max(score, 115)
            if score <= 0:
                continue
            candidate = {"index": index, "header": header_text}
            if best is None or score > best[0]:
                best = (score, candidate)
        if best:
            detected[field_name] = best[1]
            confidence[field_name] = best[0]
    return detected, confidence


def _build_parsed_sheet(sheet_payload, file_format, original_filename, ai_result=None):
    rows = sheet_payload.get("rows") or []
    if not rows or not any(_non_empty_cells(row) for row in rows):
        raise IPImportError("A planilha nao possui linhas validas para importacao.")
    header_row_index = _detect_header_row(rows)
    header_row_index = min(header_row_index, max(len(rows) - 1, 0))
    headers = rows[header_row_index]
    column_map, confidence = _detect_column_map(headers, ai_result=ai_result)
    warnings = []
    if "ip" not in column_map and not {"range_start", "range_end"} <= set(column_map.keys()):
        warnings.append("Nenhuma coluna clara de IP ou faixa de IP foi encontrada.")
    return ParsedSpreadsheet(
        file_format=file_format,
        sheet_name=sheet_payload.get("name") or "Arquivo",
        header_row_index=header_row_index,
        rows_total=len(rows),
        headers=headers,
        raw_rows=rows,
        column_map={
            key: {
                "index": value["index"],
                "header": value["header"],
                "confidence": confidence.get(key, 0),
            }
            for key, value in column_map.items()
        },
        warnings=warnings,
        suggested_list_name=_suggest_list_name(sheet_payload.get("name"), original_filename),
    )


def parse_workbook(raw_bytes, original_filename):
    file_format = _resolve_file_format(original_filename)
    if file_format == "unknown":
        raise IPImportError("Formato nao suportado. Use arquivos .xlsx, .xlsm, .csv ou .tsv.")
    if file_format in {"csv", "tsv"}:
        return [
            _build_parsed_sheet(
                {"name": "Arquivo", "rows": _read_csv_rows(raw_bytes, file_format)},
                file_format=file_format,
                original_filename=original_filename,
            )
        ]

    sheets = _read_xlsx_rows(raw_bytes)
    parsed = []
    for sheet_payload in sheets:
        rows = sheet_payload.get("rows") or []
        if not rows or not any(_non_empty_cells(row) for row in rows):
            continue
        parsed.append(_build_parsed_sheet(sheet_payload, file_format=file_format, original_filename=original_filename))
    if not parsed:
        raise IPImportError("Nenhuma aba valida foi encontrada no arquivo.")
    return parsed


def _extract_row_value(row, column_map, field_name):
    mapping = column_map.get(field_name) or {}
    index = mapping.get("index")
    if index is None or index >= len(row):
        return ""
    return _cell_to_text(row[index])


def _normalize_mac(value):
    raw = re.sub(r"[^0-9A-F]", "", _ascii_upper(value))
    if not raw:
        return "", False
    if len(raw) != 12:
        return _cell_to_text(value), False
    return ":".join(raw[index:index + 2] for index in range(0, 12, 2)), True


def _expand_ip_range(start, end, limit=4096):
    try:
        start_ip = ipaddress.ip_address((start or "").strip())
        end_ip = ipaddress.ip_address((end or "").strip())
    except ValueError:
        raise IPImportError("Faixa de IP invalida na planilha.")
    if start_ip.version != end_ip.version:
        raise IPImportError("Faixa de IP mistura versoes diferentes.")
    start_int = int(start_ip)
    end_int = int(end_ip)
    if end_int < start_int:
        raise IPImportError("Faixa de IP invalida: o fim e menor que o inicio.")
    total = end_int - start_int + 1
    if total > limit:
        raise IPImportError(f"Faixa de IP excede o limite de {limit} enderecos.")
    return [str(ipaddress.ip_address(value)) for value in range(start_int, end_int + 1)]


def _validate_ip(value):
    text = _cell_to_text(value)
    if not text:
        return ""
    try:
        return str(ipaddress.ip_address(text))
    except ValueError:
        return ""


def build_file_sha256(raw_bytes):
    digest = hashlib.sha256()
    digest.update(raw_bytes or b"")
    return digest.hexdigest()


def _most_common(values, default=""):
    filtered = [_cell_to_text(value) for value in values if _cell_to_text(value)]
    if not filtered:
        return default
    return Counter(filtered).most_common(1)[0][0]


def _is_sparse_ip_set(ip_values):
    if len(ip_values) <= 1:
        return False
    ordered = sorted(ipaddress.ip_address(ip_value) for ip_value in ip_values)
    return int(ordered[-1]) - int(ordered[0]) + 1 != len(ordered)


def _build_sheet_warning(sheet_name, warning, multi_sheet):
    text = _cell_to_text(warning)
    if not text:
        return ""
    if multi_sheet and sheet_name:
        return f"[{sheet_name}] {text}"
    return text


def _dedupe_ip_items(rows):
    deduped = {}
    duplicates = 0
    for row in rows:
        key = row["ip"]
        if key in deduped:
            duplicates += 1
            existing = deduped[key]
            for field_name in ("device_name", "description", "mac", "protocol", "list_code"):
                if not existing.get(field_name) and row.get(field_name):
                    existing[field_name] = row[field_name]
            continue
        deduped[key] = dict(row)
    return list(deduped.values()), duplicates


def _compose_description(explicit_description, controller, drive, status_note, novelty):
    explicit = _cell_to_text(explicit_description)
    if explicit:
        return explicit[:200]

    parts = []
    controller_text = _cell_to_text(controller)
    drive_text = _cell_to_text(drive)
    status_text = _cell_to_text(status_note)
    novelty_text = _cell_to_text(novelty)

    if controller_text:
        parts.append(f"PLC {controller_text}")
    if drive_text:
        parts.append(f"Drive {drive_text}")
    if status_text:
        parts.append(f"Status {status_text}")
    if novelty_text:
        parts.append(novelty_text)
    return " | ".join(parts)[:200]


def normalize_rows(parsed, ai_result=None):
    column_map, _ = _detect_column_map(parsed.headers, ai_result=ai_result)
    if "ip" not in column_map and not {"range_start", "range_end"} <= set(column_map.keys()):
        raise IPImportError("Nao foi possivel identificar uma coluna de IP ou uma faixa valida.")

    default_list_name = _clean_list_name((ai_result or {}).get("default_list_name")) or parsed.suggested_list_name
    warnings = list(parsed.warnings or [])
    normalized_rows = []

    for row_index, row in enumerate(parsed.raw_rows[parsed.header_row_index + 1 :], start=parsed.header_row_index + 2):
        if not any(_cell_to_text(cell) for cell in row):
            continue

        ip_text = _validate_ip(_extract_row_value(row, column_map, "ip"))
        range_start = _extract_row_value(row, column_map, "range_start")
        range_end = _extract_row_value(row, column_map, "range_end")
        device_name = _extract_row_value(row, column_map, "device_name")
        description = _extract_row_value(row, column_map, "description")
        protocol = _extract_row_value(row, column_map, "protocol")
        controller = _extract_row_value(row, column_map, "controller")
        drive = _extract_row_value(row, column_map, "drive")
        status_note = _extract_row_value(row, column_map, "status_note")
        novelty = _extract_row_value(row, column_map, "novelty")
        list_name = _clean_list_name(_extract_row_value(row, column_map, "list_name")) or default_list_name
        list_code = _cell_to_text(_extract_row_value(row, column_map, "list_code")).upper()
        mac_value, mac_ok = _normalize_mac(_extract_row_value(row, column_map, "mac"))
        description_value = _compose_description(description, controller, drive, status_note, novelty)

        if not ip_text and not (range_start and range_end):
            if any((device_name, description_value, protocol, mac_value)):
                warnings.append(f"Linha {row_index}: ignorada porque nao possui IP ou faixa.")
            continue

        if mac_value and not mac_ok:
            warnings.append(f"Linha {row_index}: MAC mantido como texto bruto porque o formato parece invalido.")

        if ip_text:
            ip_values = [ip_text]
        else:
            try:
                ip_values = _expand_ip_range(range_start, range_end)
            except IPImportError as exc:
                warnings.append(f"Linha {row_index}: {exc}")
                continue

        for ip_value in ip_values:
            normalized_rows.append(
                {
                    "source_sheet": parsed.sheet_name,
                    "source_row": row_index,
                    "list_name": list_name[:120],
                    "list_code": list_code[:60],
                    "ip": ip_value,
                    "device_name": device_name[:120],
                    "description": description_value[:200],
                    "mac": mac_value[:30],
                    "protocol": protocol[:30],
                }
            )

    if not normalized_rows:
        raise IPImportError("Nenhuma linha com IP valido foi encontrada depois do cabecalho.")

    effective_map = {field_name: payload.get("header", "") for field_name, payload in column_map.items()}
    return normalized_rows, effective_map, warnings


def build_import_proposal(original_filename, normalized_rows):
    grouped = defaultdict(list)
    for row in normalized_rows:
        group_key = _compact_token(row.get("list_code") or row.get("list_name") or row.get("source_sheet")) or f"LIST_{len(grouped) + 1}"
        grouped[group_key].append(row)

    proposal_lists = []
    warnings = []
    total_items = 0

    for list_index, (list_token, group_rows) in enumerate(grouped.items(), start=1):
        deduped_rows, duplicate_count = _dedupe_ip_items(group_rows)
        if duplicate_count:
            warnings.append(f"Lista {list_index}: {duplicate_count} IP(s) duplicado(s) foram consolidados na preview.")

        sorted_rows = sorted(deduped_rows, key=lambda item: ipaddress.ip_address(item["ip"]))
        ip_values = [item["ip"] for item in sorted_rows]
        sparse = _is_sparse_ip_set(ip_values)
        list_name = _clean_list_name(_most_common(item.get("list_name") for item in sorted_rows)) or _suggest_list_name(
            sorted_rows[0].get("source_sheet"),
            original_filename,
        )
        list_code = _most_common(item.get("list_code") for item in sorted_rows)
        protocol = _most_common(item.get("protocol") for item in sorted_rows)
        description = _most_common(item.get("description") for item in sorted_rows)
        source_sheets = sorted({item.get("source_sheet") for item in sorted_rows if item.get("source_sheet")})
        total_items += len(sorted_rows)
        if sparse:
            warnings.append(f"Lista {list_name or list_index}: a faixa resume IPs nao contiguos; os itens individuais foram preservados.")
        proposal_lists.append(
            {
                "list_key": f"list_{list_index}_{list_token.lower()}",
                "name": list_name or f"Lista importada {list_index}",
                "id_listaip": list_code,
                "description": description,
                "faixa_inicio": ip_values[0],
                "faixa_fim": ip_values[-1],
                "protocolo_padrao": protocol,
                "total_ips": len(sorted_rows),
                "filled_devices": sum(1 for item in sorted_rows if item.get("device_name")),
                "source_sheets": source_sheets,
                "is_sparse": sparse,
                "items": sorted_rows,
                "preview_items": sorted_rows[:18],
            }
        )

    return {
        "lists": proposal_lists,
        "warnings": warnings,
        "conflicts": [],
        "summary": {
            "lists": len(proposal_lists),
            "items": total_items,
            "rows": len(normalized_rows),
        },
    }


def _extract_response_text(response_payload):
    if not isinstance(response_payload, dict):
        return ""
    output = response_payload.get("output") or []
    for item in output:
        for content in item.get("content") or []:
            if content.get("type") in {"output_text", "text"} and content.get("text"):
                return content.get("text")
    if response_payload.get("output_text"):
        return response_payload.get("output_text")
    return ""


def _call_openai_responses(settings_obj, schema_name, schema, system_prompt, user_prompt, request_timeout_seconds=None):
    if not settings_obj.enabled:
        raise IPImportError("Agente de importacao desativado.")
    if settings_obj.provider != settings_obj.Provider.OPENAI:
        raise IPImportError("Provider de agente nao suportado nesta versao.")
    if not settings_obj.api_key:
        raise IPImportError("API key do agente nao configurada.")

    base_url = (settings_obj.api_base_url or "").strip().rstrip("/") or "https://api.openai.com/v1"
    payload = {
        "model": settings_obj.model,
        "input": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        "text": {
            "format": {
                "type": "json_schema",
                "name": schema_name,
                "strict": True,
                "schema": schema,
            }
        },
    }
    if settings_obj.reasoning_effort and settings_obj.reasoning_effort != "none":
        payload["reasoning"] = {"effort": settings_obj.reasoning_effort}

    body = json.dumps(payload).encode("utf-8")
    http_request = urlrequest.Request(
        url=f"{base_url}/responses",
        data=body,
        method="POST",
        headers={
            "Authorization": f"Bearer {settings_obj.api_key}",
            "Content-Type": "application/json",
        },
    )
    request_timeout_seconds = request_timeout_seconds or _ai_request_timeout_seconds()
    try:
        with urlrequest.urlopen(http_request, timeout=request_timeout_seconds) as response:
            response_payload = json.loads(response.read().decode("utf-8"))
    except urlerror.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise IPImportError(f"Falha na chamada do agente: HTTP {exc.code} - {detail[:400]}")
    except urlerror.URLError as exc:
        raise IPImportError(f"Falha na chamada do agente: {exc.reason}")
    except (TimeoutError, socket.timeout):
        raise IPImportError(f"Falha na chamada do agente: timeout apos {request_timeout_seconds:.1f}s.")
    except OSError as exc:
        raise IPImportError(f"Falha na chamada do agente: {exc}")

    response_text = _extract_response_text(response_payload)
    if not response_text:
        raise IPImportError("O agente nao retornou texto utilizavel.")
    try:
        return json.loads(response_text)
    except json.JSONDecodeError as exc:
        raise IPImportError(f"O agente retornou JSON invalido: {exc}")


def run_ai_analysis(settings_obj, parsed, normalized_rows, request_timeout_seconds=None):
    sample_rows = []
    for row in normalized_rows[: min(len(normalized_rows), settings_obj.max_rows_for_ai)]:
        sample_rows.append(
            {
                "source_row": row["source_row"],
                "list_name": row["list_name"],
                "list_code": row["list_code"],
                "ip": row["ip"],
                "device_name": row["device_name"],
                "description": row["description"],
                "mac": row["mac"],
                "protocol": row["protocol"],
            }
        )

    schema = {
        "type": "object",
        "properties": {
            "default_list_name": {"type": "string"},
            "column_map": {
                "type": "object",
                "properties": {
                    "list_name": {"type": "string"},
                    "list_code": {"type": "string"},
                    "ip": {"type": "string"},
                    "device_name": {"type": "string"},
                    "description": {"type": "string"},
                    "mac": {"type": "string"},
                    "protocol": {"type": "string"},
                    "range_start": {"type": "string"},
                    "range_end": {"type": "string"},
                },
                "required": [
                    "list_name",
                    "list_code",
                    "ip",
                    "device_name",
                    "description",
                    "mac",
                    "protocol",
                    "range_start",
                    "range_end",
                ],
                "additionalProperties": False,
            },
            "warnings": {"type": "array", "items": {"type": "string"}},
            "notes": {"type": "string"},
        },
        "required": ["default_list_name", "column_map", "warnings", "notes"],
        "additionalProperties": False,
    }
    user_prompt = json.dumps(
        {
            "headers": parsed.headers,
            "detected_column_map": parsed.column_map,
            "sample_rows": sample_rows,
            "sheet_name": parsed.sheet_name,
        },
        ensure_ascii=True,
    )
    return _call_openai_responses(
        settings_obj=settings_obj,
        schema_name="ip_import_analysis",
        schema=schema,
        system_prompt=f"{settings_obj.header_prompt}\n\n{settings_obj.grouping_prompt}",
        user_prompt=user_prompt,
        request_timeout_seconds=request_timeout_seconds,
    )


def reprocess_import_job(job, settings_obj=None):
    if not job.source_file:
        raise IPImportError("Arquivo fonte da importacao nao encontrado.")
    job.source_file.open("rb")
    raw_bytes = job.source_file.read()
    job.source_file.close()

    parsed_sheets = parse_workbook(raw_bytes=raw_bytes, original_filename=job.original_filename)
    multi_sheet = len(parsed_sheets) > 1
    normalized_rows = []
    warnings = []
    effective_map = {}
    sheet_summaries = []
    ai_status = job.AIStatus.SKIPPED
    ai_payload = {"sheets": {}}
    ai_errors = []
    ai_model = settings_obj.model if settings_obj and settings_obj.enabled else ""
    ai_success = 0
    ai_attempts = 0
    processed_sheets = []
    ai_started_at = time.monotonic()
    ai_request_timeout = _ai_request_timeout_seconds()
    ai_total_budget = _ai_total_budget_seconds()
    ai_max_sheets = _ai_max_sheets_per_job()

    for parsed in parsed_sheets:
        try:
            sheet_rows, sheet_map, sheet_warnings = normalize_rows(parsed=parsed, ai_result=None)
        except IPImportError as exc:
            warnings.append(_build_sheet_warning(parsed.sheet_name, str(exc), multi_sheet))
            sheet_summaries.append(
                {
                    "sheet_name": parsed.sheet_name,
                    "header_row_index": parsed.header_row_index + 1,
                    "rows_total": parsed.rows_total,
                    "rows_parsed": 0,
                    "column_map": {},
                    "skipped": True,
                }
            )
            continue

        sheet_ai_payload = {}
        if settings_obj and settings_obj.enabled:
            remaining_budget = ai_total_budget - (time.monotonic() - ai_started_at)
            if ai_attempts >= ai_max_sheets:
                sheet_warnings.append(
                    "Analise com IA pulada nesta aba para respeitar o limite operacional da importacao web."
                )
            elif remaining_budget <= 1:
                sheet_warnings.append(
                    "Analise com IA pulada nesta aba porque o tempo maximo da importacao web foi atingido."
                )
            else:
                request_timeout_seconds = min(ai_request_timeout, remaining_budget)
                try:
                    ai_attempts += 1
                    sheet_ai_payload = run_ai_analysis(
                        settings_obj=settings_obj,
                        parsed=parsed,
                        normalized_rows=sheet_rows,
                        request_timeout_seconds=request_timeout_seconds,
                    )
                    sheet_rows, sheet_map, sheet_warnings = normalize_rows(parsed=parsed, ai_result=sheet_ai_payload)
                    ai_success += 1
                except IPImportError as exc:
                    ai_errors.append(_build_sheet_warning(parsed.sheet_name, str(exc), multi_sheet))
                    sheet_warnings.append(str(exc))

        normalized_rows.extend(sheet_rows)
        effective_map[parsed.sheet_name] = sheet_map
        ai_payload["sheets"][parsed.sheet_name] = sheet_ai_payload
        sheet_summaries.append(
            {
                "sheet_name": parsed.sheet_name,
                "header_row_index": parsed.header_row_index + 1,
                "rows_total": parsed.rows_total,
                "rows_parsed": len(sheet_rows),
                "column_map": sheet_map,
            }
        )
        processed_sheets.append(parsed)
        for warning in sheet_warnings:
            formatted = _build_sheet_warning(parsed.sheet_name, warning, multi_sheet)
            if formatted:
                warnings.append(formatted)

    if not normalized_rows:
        raise IPImportError("Nenhuma aba util gerou linhas normalizadas para importacao.")

    if settings_obj and settings_obj.enabled:
        ai_status = job.AIStatus.SUCCESS if ai_success == len(processed_sheets) else job.AIStatus.FAILED
        ai_error = " | ".join(error for error in ai_errors if error)
        if ai_error:
            warnings.extend(error for error in ai_errors if error)
    else:
        ai_error = ""

    proposal = build_import_proposal(original_filename=job.original_filename, normalized_rows=normalized_rows)
    for warning in proposal.get("warnings") or []:
        warnings.append(warning)

    primary_sheet = processed_sheets[0]
    sheet_label = primary_sheet.sheet_name if len(processed_sheets) == 1 else f"{primary_sheet.sheet_name} +{len(processed_sheets) - 1}"
    return {
        "file_format": primary_sheet.file_format,
        "sheet_name": sheet_label,
        "header_row_index": primary_sheet.header_row_index + 1 if len(processed_sheets) == 1 else None,
        "rows_total": sum(item.rows_total for item in processed_sheets),
        "rows_parsed": len(normalized_rows),
        "column_map": effective_map,
        "warnings": warnings,
        "normalized_rows": normalized_rows,
        "sheet_summaries": sheet_summaries,
        "proposal": proposal,
        "ai_status": ai_status,
        "ai_payload": ai_payload,
        "ai_error": ai_error,
        "ai_model": ai_model,
    }


def apply_import_job(job, user, selected_list_keys=None):
    proposal = job.proposal_payload or {}
    list_payloads = proposal.get("lists") or []
    if not list_payloads:
        raise IPImportError("A importacao nao possui listas prontas para aplicacao.")
    if not job.cliente:
        raise IPImportError("A importacao nao possui cliente associado.")

    apply_log = dict(job.apply_log or {})
    applied_list_keys = dict(apply_log.get("applied_list_keys") or {})
    applied_list_ids = list(apply_log.get("applied_list_ids") or [])
    requested_keys = {str(item) for item in (selected_list_keys or [payload.get("list_key") for payload in list_payloads]) if str(item)}
    applied = []

    for payload in list_payloads:
        list_key = str(payload.get("list_key") or "")
        if not list_key or list_key not in requested_keys:
            continue

        existing_id = applied_list_keys.get(list_key)
        if existing_id:
            existing_lista = ListaIP.objects.filter(pk=existing_id).first()
            if existing_lista:
                applied.append(existing_lista)
                continue

        id_listaip_obj = None
        if payload.get("id_listaip"):
            id_listaip_obj, _ = ListaIPID.objects.get_or_create(codigo=_cell_to_text(payload.get("id_listaip")).upper())

        descricao = _cell_to_text(payload.get("description"))
        if payload.get("is_sparse"):
            sparse_note = "Importado de planilha com IPs nao contiguos."
            descricao = f"{descricao} {sparse_note}".strip() if descricao else sparse_note

        lista = ListaIP.objects.create(
            cliente=job.cliente,
            id_listaip=id_listaip_obj,
            nome=_cell_to_text(payload.get("name"))[:120] or f"Lista importada {len(applied_list_keys) + 1}",
            descricao=descricao[:500],
            faixa_inicio=_cell_to_text(payload.get("faixa_inicio")),
            faixa_fim=_cell_to_text(payload.get("faixa_fim")),
            protocolo_padrao=_cell_to_text(payload.get("protocolo_padrao"))[:30],
        )

        item_payloads, _ = _dedupe_ip_items(payload.get("items") or [])
        ListaIPItem.objects.bulk_create(
            [
                ListaIPItem(
                    lista=lista,
                    ip=item["ip"],
                    nome_equipamento=_cell_to_text(item.get("device_name"))[:120],
                    descricao=_cell_to_text(item.get("description"))[:200],
                    mac=_cell_to_text(item.get("mac"))[:30],
                    protocolo=_cell_to_text(item.get("protocol"))[:30],
                )
                for item in item_payloads
            ]
        )

        applied.append(lista)
        applied_list_keys[list_key] = lista.pk
        if lista.pk not in applied_list_ids:
            applied_list_ids.append(lista.pk)
        job.applied_lista = lista

    if not applied:
        raise IPImportError("Nenhuma lista foi aplicada. Revise a selecao da preview.")

    all_keys = {str(payload.get("list_key") or "") for payload in list_payloads if payload.get("list_key")}
    applied_now = set(applied_list_keys.keys())
    apply_log["applied_list_keys"] = applied_list_keys
    apply_log["applied_list_ids"] = applied_list_ids
    apply_log["lists_applied"] = len(applied_now)
    apply_log["items_applied"] = ListaIPItem.objects.filter(lista_id__in=applied_list_ids).count()
    job.apply_log = apply_log
    job.status = job.Status.APPLIED if all_keys and all_keys <= applied_now else job.Status.REVIEW
    job.save(update_fields=["applied_lista", "apply_log", "status", "updated_at"])
    return applied
