import csv
import hashlib
import json
import re
import socket
import unicodedata
from collections import defaultdict
from dataclasses import dataclass
from io import BytesIO, StringIO
from pathlib import Path
from urllib import error as urlerror
from urllib import request as urlrequest
from zipfile import BadZipFile

from openpyxl import load_workbook
from openpyxl.utils.exceptions import InvalidFileException


TYPE_ALIASES = {
    "DI": ["DI", "DIGITAL INPUT", "DISCRETE INPUT", "ENTRADA DIGITAL", "DIGITAL_IN"],
    "DO": ["DO", "DQ", "DIGITAL OUTPUT", "DISCRETE OUTPUT", "SAIDA DIGITAL", "DIGITAL_OUT"],
    "AI": ["AI", "ANALOG INPUT", "ENTRADA ANALOGICA", "ANALOG_IN"],
    "AO": ["AO", "AQ", "ANALOG OUTPUT", "SAIDA ANALOGICA", "ANALOG_OUT"],
}

HEADER_ALIASES = {
    "panel": ["PANEL", "PAINEL", "PNL", "PAINEL IO", "PANEL NAME", "DRIVER", "PLC"],
    "rack": ["RACK", "RK", "NOME RACK", "RACK NAME"],
    "slot": ["SLOT", "SL", "POSICAO SLOT", "CARD SLOT", "POSICAO", "CARTAO", "MODULO SLOT"],
    "module_model": [
        "MODULO",
        "MOD",
        "MODELO MODULO",
        "MODEL",
        "MODULE",
        "MODULE MODEL",
        "MODULE TYPE",
        "CARD",
        "CARD MODEL",
        "CARD TYPE",
        "CARD GROUP",
    ],
    "channel": ["CANAL", "CHANNEL", "CHAN", "CH", "CHANNEL NUMBER"],
    "location": [
        "LOCATION",
        "LOCALIZACAO",
        "LOCALIZACAO STRING",
        "LOC",
        "CARD POSITION",
        "CARD POS",
        "PANEL / CARD POS",
        "PANEL / CARD",
        "PANEL/CARD",
        "RACK/SLOT",
        "R/S/C",
        "FIELD WIRING",
        "TERM",
        "TERMINAL",
        "JB/TB",
        "JB TB",
    ],
    "address": ["ADDR", "ADDRESS"],
    "fieldbus": ["FIELDBUS", "BUS", "STATION", "REMOTE NODE", "NODE"],
    "point_ref": ["POINT REF", "LOOP REF", "POINT UID"],
    "tag": [
        "TAG",
        "TAGSET",
        "TAG NAME",
        "TAGNAME",
        "NOME TAG",
        "TAG IO",
        "POINT",
        "PT",
        "OBJECT",
        "OBJECT NAME",
    ],
    "description": [
        "DESCRICAO",
        "DESCRICAO SINAL",
        "DESCRIPTION",
        "SIGNAL DESCRIPTION",
        "SERVICE",
        "SERVICO",
        "SVC",
        "DESC",
        "NOME",
        "VISIBLE TEXT",
        "ALARM TEXT",
    ],
    "signal_hint": ["SIGNAL", "SINAL", "TYPE / SIG", "TYPE / SIGNAL", "SCAN"],
    "type": [
        "TIPO",
        "TYPE",
        "TIPO IO",
        "IO TYPE",
        "I/O",
        "SIGNAL TYPE",
        "TIPO SINAL",
        "TYPE / SIGNAL",
        "CLASS",
        "CODE",
    ],
}

DEFAULT_HEADER_PROMPT = (
    "Map the spreadsheet headers to the SAAS-SET IO import schema. "
    "Prefer stable engineering columns such as rack, slot, module_model, channel, tag, description and type. "
    "Only return explicit mappings that are strongly supported by the sheet content."
)

DEFAULT_GROUPING_PROMPT = (
    "Given normalized IO rows, suggest rack name, slot grouping, module models, channel ordering, and warnings. "
    "Do not invent unsupported fields. Respect explicit slot and channel values when present."
)


class IOImportError(Exception):
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
    layout: str = "tabular"


def _ascii_upper(value):
    normalized = unicodedata.normalize("NFKD", str(value or ""))
    normalized = normalized.encode("ascii", "ignore").decode("ascii")
    return normalized.strip().upper()


def _compact_token(value):
    return re.sub(r"[^A-Z0-9]+", "", _ascii_upper(value))


def _tokenize_identifier(value):
    return [token for token in re.split(r"[^A-Z0-9]+", _ascii_upper(value)) if token]


def _header_word_tokens(value):
    return [token for token in re.split(r"[^A-Z0-9]+", _ascii_upper(value)) if token]


def _alias_match_score(header_value, alias):
    header_compact = _compact_token(header_value)
    alias_token = _compact_token(alias)
    if not header_compact or not alias_token:
        return 0
    header_words = set(_header_word_tokens(header_value))
    if header_compact == alias_token:
        return 100
    if alias_token in header_words:
        return 80
    if len(alias_token) >= 4 and alias_token in header_compact:
        return 60
    return 0


def _best_alias_match(header_value, aliases):
    best_score = 0
    best_rank = 999
    for rank, alias in enumerate(aliases):
        score = _alias_match_score(header_value, alias)
        if score > best_score or (score and score == best_score and rank < best_rank):
            best_score = score
            best_rank = rank
    return best_score, best_rank


def _sample_column_values(rows, header_row_index, index, limit=12):
    if not rows:
        return []
    values = []
    for row in rows[header_row_index + 1 :]:
        if index >= len(row):
            continue
        value = _cell_to_text(row[index])
        if not value:
            continue
        values.append(value)
        if len(values) >= limit:
            break
    return values


def _looks_like_tag_value(value):
    token = _cell_to_text(value).strip()
    if not token:
        return False
    if " " in token:
        return False
    ascii_token = _ascii_upper(token)
    if len(ascii_token) < 3 or len(ascii_token) > 40:
        return False
    if not any(char.isalpha() for char in ascii_token):
        return False
    if not any(char.isdigit() for char in ascii_token):
        return False
    return bool(re.fullmatch(r"[A-Z0-9_.:/-]+", ascii_token))


def _score_type_values(values):
    if not values:
        return 0
    return sum(1 for value in values if normalize_type(value)) * 15


def _score_tag_values(values):
    if not values:
        return 0
    return sum(1 for value in values if _looks_like_tag_value(value)) * 8


def _cell_to_text(value):
    if value is None:
        return ""
    if isinstance(value, bool):
        return "SIM" if value else "NAO"
    text = str(value).strip()
    if not text:
        return ""
    return re.sub(r"\s+", " ", text)


def normalize_tag(value):
    value = _cell_to_text(value)
    if not value:
        return ""
    normalized = _ascii_upper(value)
    normalized = re.sub(r"[^A-Z0-9]+", "_", normalized)
    return normalized.strip("_")


def normalize_module_alias(value):
    token = _ascii_upper(value)
    token = token.replace("MODULO", "")
    token = token.replace("MODEL", "")
    return re.sub(r"[^A-Z0-9]+", "", token)


def normalize_type(raw_value, fallbacks=None):
    candidates = [_ascii_upper(raw_value)]
    for fallback in fallbacks or []:
        candidates.append(_ascii_upper(fallback))
    compact_candidates = [_compact_token(item) for item in candidates if item]
    combined_text = " | ".join(candidate for candidate in candidates if candidate)
    combined_compact = " ".join(compact_candidates)

    if "SPAREAO" in combined_compact or "RESERVAAO" in combined_compact:
        return "AO"
    if re.search(r"\b(SPARE|RESERVA)[_. -]?(AO)\b", combined_text) or (
        "RESERVA" in combined_text and "SAIDA" in combined_text and "ANALOG" in combined_text
    ):
        return "AO"
    if "SPAREAI" in combined_compact or "RESERVAAI" in combined_compact or "RESERVAANALOG" in combined_compact:
        return "AI"
    if re.search(r"\b(SPARE|RESERVA)[_. -]?(AI|ANALOG)\b", combined_text):
        return "AI"
    if "SPAREDO" in combined_compact or "RESERVADO" in combined_compact:
        return "DO"
    if re.search(r"\b(SPARE|RESERVA)[_. -]?(DO)\b", combined_text) or (
        "RESERVA" in combined_text and "SAIDA" in combined_text and "DIGITAL" in combined_text
    ):
        return "DO"
    if "SPAREDI" in combined_compact or "RESERVADI" in combined_compact:
        return "DI"
    if re.search(r"\b(SPARE|RESERVA)[_. -]?(DI|DIGITAL)\b", combined_text) and "SAIDA" not in combined_text:
        return "DI"

    for candidate in candidates:
        if not candidate:
            continue
        if re.search(r"%IW\d+\.\d+\.\d+\b", candidate):
            return "AI"
        if re.search(r"%QW\d+\.\d+\.\d+\b", candidate):
            return "AO"
        if re.search(r"%I\d+\.\d+\.\d+\b", candidate):
            return "DI"
        if re.search(r"%Q\d+\.\d+\.\d+\b", candidate):
            return "DO"
        compact_candidate = _compact_token(candidate)
        if re.fullmatch(r"DI\d+", compact_candidate):
            return "DI"
        if re.fullmatch(r"(DO|DQ)\d+", compact_candidate):
            return "DO"
        if re.fullmatch(r"AI\d+", compact_candidate):
            return "AI"
        if re.fullmatch(r"(AO|AQ)\d+", compact_candidate):
            return "AO"

    for canonical, aliases in TYPE_ALIASES.items():
        alias_tokens = {_compact_token(alias) for alias in aliases}
        for token in compact_candidates:
            if not token:
                continue
            if token in alias_tokens:
                return canonical

    for canonical, aliases in TYPE_ALIASES.items():
        for alias in aliases:
            alias_text = _ascii_upper(alias)
            alias_compact = _compact_token(alias)
            if len(alias_compact) < 4:
                continue
            if alias_text in combined_text or alias_compact in combined_compact:
                return canonical

    if re.search(r"\b(AIT|FIT|LIT|PIT|TIT|WIT|PHIT)[_.-]?\d+", combined_text):
        return "AI"
    if "_SPD_FBK" in combined_text or "REFERENCIA REAL DE VELOCIDADE" in combined_text:
        return "AI"
    if "_SPD_REF" in combined_compact or "_POS_REF" in combined_compact or "_OUT" in combined_compact or "_REF" in combined_compact:
        return "AO"
    if "SETPOINT" in combined_text:
        return "AO"
    if "TRANSMISSOR" in combined_text or "TRANSMITTER" in combined_text:
        return "AI"
    if re.search(r"\b(ZS|LS|PS|TS|FS)[_.-]?\d+.*_(OPEN|CLOSE)\b", combined_text):
        return "DI"

    analog_markers = ("4-20 MA", "420MA", "0-10 V", "010V", "1-5 V", "15V", "PT100", "RTD", "THERMOCOUPLE")
    analog_hint = any(marker in combined_text or marker.replace(" ", "") in combined_compact for marker in analog_markers)
    analog_output_markers = (
        "SETPOINT",
        "SPD_REF",
        "POS_REF",
        "_REF",
        "_OUT",
        " COMANDO ",
        " COMMAND ",
        "_CMD",
    )
    analog_input_markers = (
        "TRANSMISSOR",
        "TRANSMITTER",
        "INDICADOR",
        "MEASUREMENT",
        "PRESSAO",
        "PRESSURE",
        "TEMPERATURA",
        "TEMPERATURE",
        "VAZAO",
        "FLOW",
        "NIVEL",
        "LEVEL",
        "CONDUTIVIDADE",
        "WEIGHT",
        "PIT_",
        "FIT_",
        "AIT_",
        "TIT_",
        "WIT_",
        "LIT_",
        "PHIT_",
    )
    if analog_hint:
        if any(marker in combined_text or marker in combined_compact for marker in analog_output_markers):
            return "AO"
        if any(marker in combined_text or marker in combined_compact for marker in analog_input_markers):
            return "AI"
        return "AI"

    discrete_output_markers = (
        "COMANDO",
        "COMMAND",
        "_CMD",
        "_START",
        "_STOP",
        "PARTIDA",
        "SOLENOID",
        "BEACON",
        "HORN",
        "STARTER",
        "ENABLE",
    )
    if any(marker in combined_text or marker in combined_compact for marker in discrete_output_markers):
        return "DO"

    discrete_input_markers = (
        "RETORNO",
        "FEEDBACK",
        "HEALTHY",
        "_FB",
        "_FBK",
        "_RUN",
        "_FLT",
        "FAULT",
        "STATUS",
        "AUX CONTACT",
        "DRY CONTACT",
        "LIMIT SWITCH",
        "CHAVE",
        "SENSOR",
        "DETECTOR",
        "FIM DE CURSO",
    )
    if any(marker in combined_text or marker in combined_compact for marker in discrete_input_markers):
        return "DI"

    if "LOCAL:I." in combined_text:
        return "AI" if "ANALOG" in combined_text or "AI" in combined_compact else "DI"
    if "LOCAL:O." in combined_text:
        return "AO" if "ANALOG" in combined_text or "AO" in combined_compact else "DO"
    return ""


def _normalize_int(value):
    text = _cell_to_text(value)
    if not text:
        return None
    match = re.search(r"(\d+)", text)
    if not match:
        return None
    try:
        return int(match.group(1))
    except ValueError:
        return None


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
        raise IOImportError("Nao foi possivel decodificar o arquivo CSV.")
    delimiter = "\t" if file_format == "tsv" else _guess_delimiter(decoded[:4000])
    reader = csv.reader(StringIO(decoded), delimiter=delimiter)
    rows = []
    for row in reader:
        rows.append([_cell_to_text(cell) for cell in row])
    return rows


def _read_xlsx_rows(raw_bytes):
    try:
        workbook = load_workbook(filename=BytesIO(raw_bytes), data_only=True, read_only=True)
    except (BadZipFile, InvalidFileException, OSError, ValueError) as exc:
        raise IOImportError(
            "Nao foi possivel abrir a planilha Excel. Confirme que o arquivo nao esta corrompido "
            "e que voce selecionou a planilha real, nao um arquivo temporario do Excel."
        ) from exc
    sheets = []
    for worksheet in workbook.worksheets:
        rows = []
        for row in worksheet.iter_rows(values_only=True):
            rows.append([_cell_to_text(cell) for cell in row])
        sheets.append({"name": worksheet.title, "rows": rows})
    return sheets


def _non_empty_cells(row):
    return sum(1 for item in row if _cell_to_text(item))


def _row_text(row):
    return " ".join(_cell_to_text(item) for item in row if _cell_to_text(item))


def _is_mostly_numeric_token(token):
    token = _compact_token(token)
    if not token:
        return False
    letters = sum(1 for char in token if char.isalpha())
    digits = sum(1 for char in token if char.isdigit())
    return digits > letters and digits >= 2


def _is_probable_data_row(row):
    non_empty = [cell for cell in row if _cell_to_text(cell)]
    if len(non_empty) < 4:
        return False
    numeric_tokens = sum(1 for cell in non_empty if _is_mostly_numeric_token(cell))
    type_hits = sum(1 for cell in non_empty if normalize_type(cell))
    tag_hits = sum(1 for cell in non_empty if normalize_tag(cell) and "_" in normalize_tag(cell))
    return numeric_tokens >= 2 or type_hits >= 2 or tag_hits >= 1


def _header_signature(row):
    alias_score = _score_header_row(row)
    non_empty = _non_empty_cells(row)
    data_penalty = 1 if _is_probable_data_row(row) else 0
    return alias_score, non_empty - data_penalty, -data_penalty


def _extract_filename_sheet_hints(original_filename):
    stem = Path(original_filename or "").stem
    tokens = [token for token in _tokenize_identifier(stem) if len(token) >= 3]
    informative_tokens = [
        token
        for token in tokens
        if token not in {"PLANILHA", "LISTA", "NUTRIEN", "REV", "REVISAO", "SHEET", "ABA"}
        and not re.fullmatch(r"REV\d+", token)
    ]
    marker_tokens = [token for token in informative_tokens if re.search(r"[A-Z]+\d+", token)]
    preferred_token = marker_tokens[-1] if marker_tokens else ""
    return informative_tokens, preferred_token


def _parse_slot_block_title(row_or_text):
    if isinstance(row_or_text, str):
        text = _cell_to_text(row_or_text)
    else:
        non_empty = [cell for cell in row_or_text if _cell_to_text(cell)]
        if len(non_empty) == 0 or len(non_empty) > 3:
            return None
        if _score_header_row(row_or_text) >= 6:
            return None
        text = _row_text(row_or_text)
    match = re.search(r"\bSLOT\s*0*(?P<slot>\d+)\b(?P<rest>.*)$", text, flags=re.IGNORECASE)
    if not match:
        return None

    rest = _cell_to_text(match.group("rest"))
    module_text = rest
    declared_channels = None
    declared_type = ""
    trailer = re.search(
        r"(?P<module>.*?)(?:\s*[:\-]\s*(?P<count>\d+)\s*(?P<type>[A-Z]{2}))\s*$",
        rest,
        flags=re.IGNORECASE,
    )
    if trailer:
        module_text = _cell_to_text(trailer.group("module"))
        declared_channels = _normalize_int(trailer.group("count"))
        declared_type = normalize_type(trailer.group("type"))

    return {
        "slot_index": _normalize_int(match.group("slot")),
        "module_raw": module_text.strip(" :-"),
        "declared_channels": declared_channels,
        "declared_type": declared_type,
        "title": text,
    }


def _looks_like_block_subheader(row):
    if _score_header_row(row) >= 6:
        tokens = {_compact_token(cell) for cell in row if _cell_to_text(cell)}
        has_tag = any(token in {"TAG", "TAGS"} or "TAG" in token for token in tokens)
        has_description = any(token in {"DESCRICAO", "DESCRIPTION", "SERVICE", "DESC"} or "DESCR" in token for token in tokens)
        has_structural = any(
            token in {"IO", "PANEL", "CARDPOS", "CARDGROUP", "CHANNEL", "CH", "RACK", "SLOT"}
            for token in tokens
        )
        if has_tag and (has_description or has_structural):
            return True
    tokens = {_compact_token(cell) for cell in row if _cell_to_text(cell)}
    if not tokens:
        return False
    has_io = "IO" in tokens
    has_tag = any(token in {"TAG", "TAGS", "TAGS"} or "TAG" in token for token in tokens)
    has_description = any(token in {"DESCRICAO", "DESCRIPTION"} or "DESCR" in token for token in tokens)
    return has_io and (has_tag or has_description)


def _looks_like_section_title(row):
    if _non_empty_cells(row) != 1:
        return False
    text = _ascii_upper(_row_text(row))
    if not text:
        return False
    explicit_sections = {
        "DISCRETE INPUTS",
        "DISCRETE OUTPUTS",
        "ANALOG INPUTS",
        "ANALOG OUTPUTS",
        "SPARES",
        "SPARES",
        "INPUTS",
        "OUTPUTS",
    }
    if text in explicit_sections:
        return True
    if text.startswith("JUNCTION BOX "):
        return True
    if text.startswith("AREA_"):
        return True
    return False


def _parse_rack_section_title(row):
    if _non_empty_cells(row) == 0:
        return ""
    if _parse_slot_block_title(row) or _looks_like_block_subheader(row):
        return ""
    if _non_empty_cells(row) > 2:
        return ""

    text = _row_text(row)
    upper_text = _ascii_upper(text)
    if not upper_text:
        return ""

    generic_markers = [
        "LISTA DE IO",
        "RESPONSAVEL",
        "RESPONSAVEL TECNICO",
        "NUTRIEN",
        "UNIDADE",
        "REV",
        "REVISAO",
        "FOLHA",
        "SHEET",
    ]
    if any(marker in upper_text for marker in generic_markers):
        return ""

    tokens = _tokenize_identifier(upper_text)
    if not tokens:
        return ""
    if not any(re.search(r"[A-Z]+\d+", token) for token in tokens):
        return ""

    rack_name = _infer_rack_name_from_sheet_name(text)
    return rack_name


def _parse_module_section_title(row):
    if _non_empty_cells(row) == 0 or _non_empty_cells(row) > 3:
        return None
    text = _row_text(row)
    upper_text = _ascii_upper(text)
    if not upper_text or "PANEL" not in upper_text or "SLOT" not in upper_text:
        return None
    if "|" not in text and " / " not in text:
        return None

    panel = _parse_panel_token(upper_text)
    rack_match = re.search(r"\bRACK\s*0*(\d+)\b", upper_text)
    slot_match = re.search(r"\bSLOT\s*0*(\d+)\b", upper_text)
    if not rack_match or not slot_match:
        return None

    module_text = ""
    if "|" in text:
        module_text = _cell_to_text(text.split("|")[-1])
    module_text = re.sub(r"^\s*SLOT\s*\d+\s*", "", module_text, flags=re.IGNORECASE).strip()
    try:
        rack_index = int(rack_match.group(1))
        slot_index = int(slot_match.group(1))
    except ValueError:
        return None
    return {
        "panel": panel,
        "rack": rack_index,
        "slot": slot_index,
        "module_model": module_text,
    }


def _count_slot_block_rows(rows):
    return sum(1 for row in rows if _parse_slot_block_title(row))


def _detect_sheet_header_row(rows):
    best_signature = None
    best_row_index = 0
    for index, row in enumerate((rows or [])[:20]):
        signature = _header_signature(row)
        if best_signature is None or signature > best_signature:
            best_signature = signature
            best_row_index = index
    return best_row_index, best_signature[0] if best_signature else 0


def _score_header_row(row):
    score = 0
    matched_fields = set()
    for cell in row:
        if not _compact_token(cell):
            continue
        for field_name, aliases in HEADER_ALIASES.items():
            if field_name in matched_fields:
                continue
            best_score = max((_alias_match_score(cell, alias) for alias in aliases), default=0)
            if best_score >= 100:
                score += 3
                matched_fields.add(field_name)
                break
            if best_score >= 60:
                score += 1
                matched_fields.add(field_name)
                break
    return score


def _choose_best_sheet(sheet_payloads, original_filename=""):
    best_sheet = None
    best_signature = None
    filename_tokens, preferred_token = _extract_filename_sheet_hints(original_filename)
    for sheet in sheet_payloads:
        rows = sheet.get("rows") or []
        best_row_index, best_row_score = _detect_sheet_header_row(rows)
        sheet_name_token = _compact_token(sheet.get("name") or "")
        preferred_bonus = 1 if preferred_token and preferred_token in sheet_name_token else 0
        overlap_score = sum(1 for token in filename_tokens if token and token in sheet_name_token)
        block_score = _count_slot_block_rows(rows[:200])
        signature = (preferred_bonus, overlap_score, block_score, best_row_score, len(rows), -best_row_index)
        if best_signature is None or signature > best_signature:
            best_signature = signature
            best_sheet = {
                "name": sheet.get("name") or "Sheet1",
                "rows": rows,
                "header_row_index": best_row_index,
            }
    return best_sheet


def _detect_column_map(headers, rows=None, header_row_index=0):
    used_indexes = set()
    used_fields = set()
    mapping = {}
    confidence = {}
    normalized_headers = [(_compact_token(header), header) for header in headers]
    candidates = []
    for field_name, aliases in HEADER_ALIASES.items():
        for index, (token, raw_header) in enumerate(normalized_headers):
            if not token:
                continue
            score, alias_rank = _best_alias_match(raw_header, aliases)
            if field_name == "description" and token in {"DESCRICAO", "DESCRIPTION", "NOME", "SIGNAL"}:
                score = 50
            elif field_name == "module_model" and token in {"MODULO", "MODEL", "MODELO"}:
                score = 50
            elif field_name == "channel" and token == "IO":
                score = max(score, 45)
            sample_values = _sample_column_values(rows or [], header_row_index, index) if rows else []
            semantic_bonus = 0
            if field_name == "type":
                semantic_bonus = _score_type_values(sample_values)
            elif field_name == "tag":
                semantic_bonus = _score_tag_values(sample_values)
            if score > 0:
                candidates.append(
                    (
                        score + semantic_bonus,
                        score,
                        -alias_rank,
                        -len(token),
                        -index,
                        field_name,
                        index,
                        raw_header,
                    )
                )
    for score, base_score, _, _, _, field_name, index, raw_header in sorted(candidates, reverse=True):
        if field_name in used_fields or index in used_indexes:
            continue
        mapping[field_name] = {"index": index, "header": raw_header}
        confidence[field_name] = max(score, base_score)
        used_fields.add(field_name)
        used_indexes.add(index)
    return mapping, confidence


def _looks_like_summary_headers(headers):
    tokens = {_compact_token(header) for header in headers if _cell_to_text(header)}
    aggregate_hits = {"POINTS", "DI", "DO", "AI", "AO", "SPARE"} & tokens
    if "POINTS" in tokens and len(aggregate_hits) >= 4:
        return True
    if "PRIMARYSHEET" in tokens or "CONTEUDO" in tokens:
        return True
    if tokens and tokens.issubset({"FIELD", "VALUE", "REVISION", "DATE", "AUTHOR", "SCOPE"}):
        return True
    return False


def _count_non_empty_data_rows(rows, header_row_index):
    start_index = min(header_row_index + 1, len(rows))
    count = 0
    for row in rows[start_index:]:
        if _non_empty_cells(row) == 0:
            continue
        if _parse_module_section_title(row) or _parse_slot_block_title(row) or _looks_like_block_subheader(row):
            continue
        count += 1
    return count


def _should_skip_parsed_sheet(parsed):
    sheet_token = _compact_token(parsed.sheet_name)
    helper_markers = (
        "INDEX",
        "INDICE",
        "LEGEND",
        "LEGENDA",
        "SUMMARY",
        "RESUMO",
        "RACKSUMMARY",
        "RACKMAP",
        "RACKXREF",
        "REVISION",
        "REVLOG",
        "CAPA",
        "COVER",
        "CHKINDEX",
        "STARTHERE",
        "START",
        "ALARMS",
        "SLOTSUMMARY",
        "JBSUMMARY",
        "PANELSUMMARY",
        "AREASUMMARY",
    )
    if _looks_like_summary_headers(parsed.headers):
        return True

    core_fields = {"tag", "description"} & set(parsed.column_map.keys())
    structural_fields = {"slot", "channel", "module_model", "type", "location", "point_ref"} & set(parsed.column_map.keys())
    data_rows = _count_non_empty_data_rows(parsed.raw_rows, parsed.header_row_index)
    block_layout = parsed.layout == "slot_blocks" or _has_slot_block_context(parsed.raw_rows)

    if any(marker in sheet_token for marker in helper_markers):
        if sheet_token in {"ALARMS", "START", "STARTHERE", "INDEX", "INDICE"}:
            return True
        if not block_layout and (not core_fields or not structural_fields):
            return True
        if data_rows < 2:
            return True

    if not block_layout and not core_fields:
        return True
    return False


def _extract_sheet_tag_set(parsed):
    mapping = parsed.column_map.get("tag")
    if not mapping:
        return set()
    tag_index = mapping.get("index")
    if tag_index is None:
        return set()
    tags = set()
    for row in parsed.raw_rows[parsed.header_row_index + 1 :]:
        if tag_index >= len(row):
            continue
        if _looks_like_repeated_tabular_header(row) or _looks_like_section_title(row):
            continue
        tag = normalize_tag(row[tag_index])
        if tag:
            tags.add(tag)
    return tags


def _sheet_quality_signature(parsed):
    structural = len(
        {"panel", "rack", "slot", "channel", "module_model", "location", "address", "fieldbus", "type"}
        & set(parsed.column_map.keys())
    )
    semantic = len({"tag", "description", "point_ref"} & set(parsed.column_map.keys()))
    return (
        structural,
        semantic,
        _count_non_empty_data_rows(parsed.raw_rows, parsed.header_row_index),
        len(parsed.column_map),
    )


def _dedupe_parsed_sheets(parsed_sheets):
    if len(parsed_sheets) <= 1:
        return parsed_sheets
    kept = []
    kept_meta = []
    for position, parsed in enumerate(parsed_sheets):
        tag_set = _extract_sheet_tag_set(parsed)
        duplicate_of = None
        for kept_index, meta in enumerate(kept_meta):
            kept_tags = meta["tags"]
            if not tag_set or not kept_tags:
                continue
            overlap = len(tag_set & kept_tags)
            min_size = min(len(tag_set), len(kept_tags))
            if min_size < 20:
                continue
            if overlap >= int(min_size * 0.9):
                current_quality = _sheet_quality_signature(parsed)
                kept_quality = meta["quality"]
                if current_quality > kept_quality:
                    kept[kept_index] = parsed
                    kept_meta[kept_index] = {
                        "tags": tag_set,
                        "quality": current_quality,
                        "position": position,
                    }
                duplicate_of = kept_index
                break
        if duplicate_of is None:
            kept.append(parsed)
            kept_meta.append(
                {"tags": tag_set, "quality": _sheet_quality_signature(parsed), "position": position}
            )
    return [item for _, item in sorted(zip((meta["position"] for meta in kept_meta), kept), key=lambda pair: pair[0])]


def _resolve_file_format(filename):
    suffix = Path(filename or "").suffix.lower()
    if suffix in {".xlsx", ".xlsm"}:
        return "xlsx"
    if suffix == ".csv":
        return "csv"
    if suffix == ".tsv":
        return "tsv"
    return "unknown"


def build_file_sha256(raw_bytes):
    digest = hashlib.sha256()
    digest.update(raw_bytes or b"")
    return digest.hexdigest()


def _build_parsed_sheet(sheet_payload, file_format):
    rows = sheet_payload.get("rows") or []
    if not rows:
        raise IOImportError("A planilha nao possui linhas para importacao.")

    header_row_index, _ = _detect_sheet_header_row(rows)
    header_row_index = min(header_row_index, max(len(rows) - 1, 0))
    headers = rows[header_row_index]
    column_map, confidence = _detect_column_map(headers, rows=rows, header_row_index=header_row_index)
    explicit_tabular_columns = {"tag"} <= set(column_map.keys()) and {"rack", "slot", "channel"} & set(column_map.keys())
    layout = "slot_blocks" if _count_slot_block_rows(rows) and not explicit_tabular_columns else "tabular"
    warnings = []
    if "tag" not in column_map and "description" not in column_map:
        warnings.append("Nao foi possivel mapear claramente as colunas principais de TAG/descricao.")
    if "type" not in column_map:
        warnings.append("Tipo de IO nao encontrado de forma explicita; sera inferido quando possivel.")
    if layout == "slot_blocks":
        warnings.append("Layout agrupado por SLOT detectado; slot, modulo e tipo serao inferidos a partir dos titulos dos blocos.")

    return ParsedSpreadsheet(
        file_format=file_format,
        sheet_name=sheet_payload.get("name") or "Sheet1",
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
        layout=layout,
    )


def parse_workbook(raw_bytes, original_filename):
    file_format = _resolve_file_format(original_filename)
    if file_format == "unknown":
        raise IOImportError("Formato nao suportado. Use arquivos .xlsx, .xlsm, .csv ou .tsv.")

    if file_format in {"csv", "tsv"}:
        sheet_payload = {"name": "Arquivo", "rows": _read_csv_rows(raw_bytes, file_format)}
        return [_build_parsed_sheet(sheet_payload, file_format=file_format)]

    sheets = _read_xlsx_rows(raw_bytes)
    if not sheets:
        raise IOImportError("Nenhuma planilha foi encontrada no arquivo.")

    parsed_sheets = []
    for sheet_payload in sheets:
        rows = sheet_payload.get("rows") or []
        if not rows or not any(_non_empty_cells(row) for row in rows):
            continue
        parsed_sheets.append(_build_parsed_sheet(sheet_payload, file_format=file_format))
    if not parsed_sheets:
        raise IOImportError("Nao foi possivel detectar abas validas para importacao.")
    filtered_sheets = [parsed for parsed in parsed_sheets if not _should_skip_parsed_sheet(parsed)]
    return _dedupe_parsed_sheets(filtered_sheets or parsed_sheets)


def parse_spreadsheet(raw_bytes, original_filename):
    file_format = _resolve_file_format(original_filename)
    if file_format == "unknown":
        raise IOImportError("Formato nao suportado. Use arquivos .xlsx, .xlsm, .csv ou .tsv.")

    if file_format in {"csv", "tsv"}:
        sheet_payload = {"name": "Arquivo", "rows": _read_csv_rows(raw_bytes, file_format)}
    else:
        sheets = _read_xlsx_rows(raw_bytes)
        if not sheets:
            raise IOImportError("Nenhuma planilha foi encontrada no arquivo.")
        sheet_payload = _choose_best_sheet(sheets, original_filename=original_filename)
        if not sheet_payload:
            raise IOImportError("Nao foi possivel detectar uma aba valida para importacao.")
    return _build_parsed_sheet(sheet_payload, file_format=file_format)


def _extract_row_value(row, column_map, field_name):
    mapping = column_map.get(field_name)
    if not mapping:
        return ""
    index = mapping.get("index")
    if index is None or index >= len(row):
        return ""
    return _cell_to_text(row[index])


def _find_first_value(*values):
    for value in values:
        text = _cell_to_text(value)
        if text:
            return text
    return ""


def _expand_panel_name(token):
    token = _clean_rack_name(token.replace("_", "-"))
    if not token:
        return ""
    upper_token = _ascii_upper(token)
    upper_token = re.sub(r"-(?:R|RACK)\d{1,2}$", "", upper_token, flags=re.IGNORECASE)
    if upper_token.startswith(("MCC-", "PNL-", "REM-", "UBS-")):
        return _clean_rack_name(upper_token)
    upper_token = re.sub(r"^(?:ET200|FB|PLC)-", "", upper_token, flags=re.IGNORECASE)
    if re.fullmatch(r"[A-Z]{3}-\d{2}", upper_token):
        if upper_token.startswith("TRN-"):
            return f"MCC-{upper_token}"
        return f"PNL-{upper_token}"
    return _clean_rack_name(upper_token)


def _parse_panel_token(text):
    upper_text = _ascii_upper(text)
    if not upper_text:
        return ""
    match = re.search(r"\b(?:MCC|PNL|PLC|ET200|REM|UBS|FB)[-_][A-Z0-9]+(?:[-_][A-Z0-9]+)*\b", upper_text)
    if match:
        return _expand_panel_name(match.group(0))
    short_match = re.fullmatch(r"[A-Z]{3}-\d{2}", upper_text.strip())
    if short_match:
        return _expand_panel_name(short_match.group(0))
    return ""


def _parse_hardware_context(*values):
    text = " ".join(_cell_to_text(value) for value in values if _cell_to_text(value))
    upper_text = _ascii_upper(text)
    if not upper_text:
        return {}

    panel = _parse_panel_token(upper_text)
    rack = None
    slot = None
    channel = None

    rack_match = re.search(r"\bRACK\s*0*(\d+)\b", upper_text)
    if not rack_match:
        rack_match = re.search(r"(?:^|[\s/.\-])R\s*0*(\d+)\b", upper_text)
    if not rack_match:
        rack_match = re.search(r"[-_/]R0*(\d+)\b", upper_text)
    if rack_match:
        try:
            rack = int(rack_match.group(1))
        except ValueError:
            rack = None

    slot_match = re.search(r"\bSLOT\s*0*(\d+)\b", upper_text)
    if not slot_match:
        slot_match = re.search(r"(?:^|[\s/.\-])S\s*0*(\d+)\b", upper_text)
    if not slot_match:
        slot_match = re.search(r"[-_/]S0*(\d+)\b", upper_text)
    if slot_match:
        try:
            slot = int(slot_match.group(1))
        except ValueError:
            slot = None

    channel_match = re.search(r"\bCH(?:ANNEL)?\s*0*(\d+)\b", upper_text)
    if not channel_match:
        channel_match = re.search(r"(?:^|[\s/.\-])C\s*0*(\d+)\b", upper_text)
    if not channel_match:
        channel_match = re.search(r"[-_/]CH?0*(\d+)\b", upper_text)
    if channel_match:
        try:
            channel = int(channel_match.group(1))
        except ValueError:
            channel = None

    combined_match = re.search(r"\bR0*(\d+)\s*[/\-]\s*S0*(\d+)(?:\s*[/\-]\s*CH?0*(\d+))?\b", upper_text)
    if combined_match:
        rack = rack or int(combined_match.group(1))
        slot = slot or int(combined_match.group(2))
        if combined_match.group(3):
            channel = channel or int(combined_match.group(3))

    std_addr_match = re.search(r"\b(\d+)\.(\d+)\.(\d+)\b", upper_text)
    if std_addr_match:
        rack = rack or int(std_addr_match.group(1))
        slot = slot or int(std_addr_match.group(2))
        channel = channel or int(std_addr_match.group(3))

    unity_match = re.search(r"%[IQ](?:W)?(\d+)\.(\d+)\.(\d+)\b", upper_text)
    if unity_match:
        rack = rack or int(unity_match.group(1))
        slot = slot or int(unity_match.group(2))
        channel = channel or (int(unity_match.group(3)) + 1)

    logix_match = re.search(r"\bSLOT\s*0*(\d+)\.DATA\.(\d+)\b", upper_text)
    if logix_match:
        slot = slot or int(logix_match.group(1))
        channel = channel or (int(logix_match.group(2)) + 1)

    bit_match = re.search(r"\bBIT\s*0*(\d+)\b", upper_text)
    if bit_match:
        channel = channel or (int(bit_match.group(1)) + 1)

    return {
        "panel": panel,
        "rack": rack,
        "slot": slot,
        "channel": channel,
    }


def _parse_symbolic_channel(value):
    text = _ascii_upper(value)
    if not text:
        return None
    match = re.search(r"\b(?P<type>[A-Z]{1,3})\s*[\.\-_/ ]\s*(?P<index>\d+)\b", text)
    if not match:
        return None
    channel_type = normalize_type(match.group("type"))
    if not channel_type:
        return None
    try:
        channel_index = int(match.group("index")) + 1
    except ValueError:
        return None
    return {"type": channel_type, "index": channel_index}


def _normalize_channel_index(value):
    symbolic = _parse_symbolic_channel(value)
    if symbolic:
        return symbolic["index"]
    return _normalize_int(value)


def serialize_module_catalog(modules_qs):
    return [
        {
            "id": module.id,
            "modelo": (module.modelo or module.nome or f"Modulo {module.id}").strip(),
            "marca": (module.marca or "").strip(),
            "tipo": module.tipo_base.nome if module.tipo_base_id else "",
            "quantidade_canais": int(module.quantidade_canais),
        }
        for module in modules_qs
    ]


def _has_slot_block_context(rows):
    titles = 0
    headers = 0
    for row in rows:
        if _parse_slot_block_title(row):
            titles += 1
        elif _looks_like_block_subheader(row):
            headers += 1
    return titles >= 1 and headers >= 1


def _looks_like_repeated_tabular_header(row):
    return _score_header_row(row) >= 6 and not _is_probable_data_row(row)


def _row_repeats_headers(row, headers):
    if not headers:
        return False
    comparisons = []
    for index, header in enumerate(headers):
        header_token = _compact_token(header)
        if not header_token or index >= len(row):
            continue
        row_token = _compact_token(row[index])
        if not row_token:
            continue
        comparisons.append((row_token, header_token))
    if len(comparisons) < 3:
        return False
    matches = sum(1 for row_token, header_token in comparisons if row_token == header_token)
    return matches >= 3 and matches >= int(len(comparisons) * 0.6)


def _normalize_slot_block_rows(parsed, active_map):
    normalized_rows = []
    current_block = None
    current_rack_context = ""
    for row_offset, row in enumerate(parsed.raw_rows, start=1):
        if _non_empty_cells(row) == 0:
            continue
        if _looks_like_section_title(row):
            continue
        rack_section_title = _parse_rack_section_title(row)
        if rack_section_title:
            current_rack_context = rack_section_title
            current_block = None
            continue
        slot_block = _parse_slot_block_title(row)
        if slot_block:
            current_block = slot_block
            continue
        if _looks_like_block_subheader(row):
            continue
        if _row_repeats_headers(row, parsed.headers):
            continue
        if current_block is None:
            continue

        channel_raw = _extract_row_value(row, active_map, "channel")
        address_raw = _extract_row_value(row, active_map, "address")
        fieldbus_raw = _extract_row_value(row, active_map, "fieldbus")
        tag_raw = _extract_row_value(row, active_map, "tag")
        description_raw = _extract_row_value(row, active_map, "description")
        type_raw = _extract_row_value(row, active_map, "type")
        if not channel_raw and _parse_symbolic_channel(type_raw):
            channel_raw = type_raw
            type_raw = ""
        panel_raw = _extract_row_value(row, active_map, "panel")
        location_raw = _extract_row_value(row, active_map, "location")
        rack_raw = _extract_row_value(row, active_map, "rack") or current_rack_context
        hardware_context = _parse_hardware_context(
            panel_raw,
            location_raw,
            address_raw,
            fieldbus_raw,
            rack_raw,
            channel_raw,
            current_rack_context,
        )
        if not panel_raw:
            panel_raw = hardware_context.get("panel") or ""
        if not rack_raw and hardware_context.get("rack"):
            rack_raw = str(hardware_context["rack"])
        symbolic_channel = _parse_symbolic_channel(channel_raw)
        inferred_type = normalize_type(
            type_raw,
            fallbacks=[
                current_block.get("declared_type"),
                channel_raw,
                address_raw,
                tag_raw,
                description_raw,
                current_block.get("module_raw"),
            ],
        )

        if not any([channel_raw, tag_raw, description_raw]):
            continue

        normalized_rows.append(
            {
                "source_row": row_offset,
                "source_sheet": parsed.sheet_name,
                "panel_raw": panel_raw,
                "location_raw": location_raw,
                "rack_raw": rack_raw,
                "slot_raw": str(current_block.get("slot_index") or hardware_context.get("slot") or ""),
                "module_raw": current_block.get("module_raw") or "",
                "channel_raw": channel_raw,
                "tag": normalize_tag(tag_raw),
                "description": description_raw,
                "type": inferred_type,
                "slot_index": current_block.get("slot_index") or hardware_context.get("slot"),
                "channel_index": (symbolic_channel or {}).get("index")
                or hardware_context.get("channel")
                or _normalize_channel_index(channel_raw),
                "issues": [],
            }
        )
    return normalized_rows


def normalize_rows(parsed, module_catalog, ai_result=None):
    normalized_rows = []
    warnings = list(parsed.warnings)
    ai_mapping = (ai_result or {}).get("column_map") or {}
    active_map = dict(parsed.column_map)
    for field_name, header_name in ai_mapping.items():
        if field_name in active_map:
            continue
        for index, header in enumerate(parsed.headers):
            if _compact_token(header) == _compact_token(header_name):
                active_map[field_name] = {
                    "index": index,
                    "header": header,
                    "confidence": 80,
                    "source": "ai",
                }
                break

    block_layout_active = parsed.layout == "slot_blocks" or (
        "slot" not in active_map and "module_model" not in active_map and _has_slot_block_context(parsed.raw_rows)
    )
    if block_layout_active:
        normalized_rows = _normalize_slot_block_rows(parsed, active_map)
    else:
        current_section_context = {}
        for previous_row in reversed(parsed.raw_rows[: parsed.header_row_index + 1]):
            previous_section_context = _parse_module_section_title(previous_row)
            if previous_section_context:
                current_section_context = previous_section_context
                break
        for row_offset, row in enumerate(parsed.raw_rows[parsed.header_row_index + 1 :], start=parsed.header_row_index + 2):
            if _non_empty_cells(row) == 0:
                continue
            if _looks_like_section_title(row):
                continue

            section_context = _parse_module_section_title(row)
            if section_context:
                current_section_context = section_context
                continue
            if _looks_like_repeated_tabular_header(row):
                continue
            if _row_repeats_headers(row, parsed.headers):
                continue

            panel_raw = _extract_row_value(row, active_map, "panel") or current_section_context.get("panel") or ""
            rack_raw = _extract_row_value(row, active_map, "rack")
            slot_raw = _extract_row_value(row, active_map, "slot")
            module_raw = _extract_row_value(row, active_map, "module_model") or current_section_context.get("module_model") or ""
            channel_raw = _extract_row_value(row, active_map, "channel")
            address_raw = _extract_row_value(row, active_map, "address")
            fieldbus_raw = _extract_row_value(row, active_map, "fieldbus")
            signal_raw = _extract_row_value(row, active_map, "signal_hint")
            tag_raw = _extract_row_value(row, active_map, "tag")
            description_raw = _extract_row_value(row, active_map, "description")
            type_raw = _extract_row_value(row, active_map, "type")
            location_raw = _find_first_value(
                _extract_row_value(row, active_map, "location"),
                fieldbus_raw,
                address_raw,
                _extract_row_value(row, active_map, "point_ref"),
            )
            hardware_context = _parse_hardware_context(
                panel_raw,
                location_raw,
                address_raw,
                fieldbus_raw,
                rack_raw,
                slot_raw,
                channel_raw,
                _row_text(row),
            )
            if not panel_raw:
                panel_raw = hardware_context.get("panel") or ""
            if not rack_raw and current_section_context.get("rack"):
                rack_raw = str(current_section_context["rack"])
            if not rack_raw and hardware_context.get("rack"):
                rack_raw = str(hardware_context["rack"])
            if not slot_raw and current_section_context.get("slot"):
                slot_raw = str(current_section_context["slot"])
            if not slot_raw and hardware_context.get("slot"):
                slot_raw = str(hardware_context["slot"])
            if not channel_raw and hardware_context.get("channel"):
                channel_raw = str(hardware_context["channel"])

            if not any([rack_raw, slot_raw, channel_raw, tag_raw, description_raw, location_raw, panel_raw]):
                continue
            if not tag_raw and not description_raw:
                continue

            inferred_type = normalize_type(
                type_raw,
                fallbacks=[signal_raw, channel_raw, address_raw, tag_raw, description_raw, module_raw, location_raw],
            )
            normalized_rows.append(
                {
                    "source_row": row_offset,
                    "source_sheet": parsed.sheet_name,
                    "panel_raw": panel_raw,
                    "location_raw": location_raw,
                    "rack_raw": rack_raw,
                    "slot_raw": slot_raw,
                    "module_raw": module_raw,
                    "channel_raw": channel_raw,
                    "tag": normalize_tag(tag_raw),
                    "description": description_raw,
                    "type": inferred_type,
                    "slot_index": _normalize_int(slot_raw) or hardware_context.get("slot"),
                    "channel_index": _normalize_channel_index(channel_raw) or hardware_context.get("channel"),
                    "issues": [],
                }
            )

    if not normalized_rows:
        raise IOImportError("A planilha nao possui linhas uteis abaixo do cabecalho detectado.")

    known_models = {normalize_module_alias(item["modelo"]): item for item in module_catalog}
    for row in normalized_rows:
        module_alias = normalize_module_alias(row.get("module_raw"))
        row["matched_module"] = known_models.get(module_alias) if module_alias else None
        if not row["type"]:
            row["issues"].append("tipo_nao_inferido")
        if not row["tag"] and not row["description"]:
            row["issues"].append("linha_sem_tag_e_descricao")

    warnings.extend((ai_result or {}).get("warnings") or [])
    return normalized_rows, active_map, warnings


def _best_module_for_type(module_catalog, channel_type, needed_channels, explicit_model=None):
    type_modules = [item for item in module_catalog if item["tipo"] == channel_type]
    if explicit_model:
        alias = normalize_module_alias(explicit_model)
        for item in type_modules:
            if normalize_module_alias(item["modelo"]) == alias:
                return item
    exact = [item for item in type_modules if item["quantidade_canais"] == needed_channels]
    if exact:
        return exact[0]
    larger = sorted(
        [item for item in type_modules if item["quantidade_canais"] >= needed_channels],
        key=lambda item: (item["quantidade_canais"], item["modelo"]),
    )
    if larger:
        return larger[0]
    smaller = sorted(
        [item for item in type_modules if item["quantidade_canais"] < needed_channels],
        key=lambda item: (-item["quantidade_canais"], item["modelo"]),
    )
    if smaller:
        return smaller[0]
    if channel_type:
        capacity = max(int(needed_channels or 0), 1)
        model_name = _cell_to_text(explicit_model)[:80] or f"{channel_type}-{capacity:02d} CUSTOM"
        display_name = _cell_to_text(explicit_model)[:120] or f"Modulo {channel_type} {capacity} canais"
        return {
            "id": None,
            "modelo": model_name,
            "nome": display_name,
            "marca": "IMPORTADO",
            "tipo": channel_type,
            "quantidade_canais": capacity,
            "source": "custom",
        }
    return None


def _pick_default_rack_name(original_filename, requested_rack_name, ai_result, normalized_rows, target_rack=None):
    if target_rack:
        return target_rack.nome
    if requested_rack_name:
        return requested_rack_name.strip()
    ai_name = (ai_result or {}).get("rack_name") or ""
    if ai_name:
        return ai_name.strip()
    for row in normalized_rows:
        rack_raw = (row.get("rack_raw") or "").strip()
        if rack_raw:
            return rack_raw[:120]
    stem = Path(original_filename or "rack_importado").stem
    stem = re.sub(r"[_-]+", " ", stem).strip()
    return (stem or "Rack importado")[:120]


def _clean_rack_name(value):
    text = _cell_to_text(value)
    if not text:
        return ""
    text = re.sub(r"\s+", " ", text).strip(" -_/")
    return text[:120]


def _infer_rack_name_from_sheet_name(sheet_name, original_filename=""):
    sheet_name = _clean_rack_name(sheet_name)
    if not sheet_name:
        return _pick_default_rack_name(original_filename, "", None, [])

    tokens = _tokenize_identifier(sheet_name)
    generic_tokens = {"SHEET", "PLANILHA", "LISTA", "ABA", "PAINEL", "RACK"}
    technical_prefixes = ("6ES", "ET200", "CPU", "IM", "DI", "DQ", "AI", "AQ")
    meaningful = []
    for token in tokens:
        if token in generic_tokens:
            continue
        if any(token.startswith(prefix) for prefix in technical_prefixes):
            break
        meaningful.append(token)
    if meaningful:
        if len(meaningful) >= 2 and meaningful[0].isalpha() and meaningful[1].isdigit():
            meaningful = [f"{meaningful[0]}{meaningful[1]}"] + meaningful[2:]
        return _clean_rack_name("-".join(meaningful))
    return sheet_name[:120]


def _compose_rack_name(panel_raw, rack_raw, source_sheet, original_filename):
    panel_name = _parse_panel_token(panel_raw) or _clean_rack_name(panel_raw)
    rack_name = _clean_rack_name(rack_raw)
    sheet_name = _infer_rack_name_from_sheet_name(source_sheet, original_filename=original_filename)

    if panel_name and rack_name:
        if re.fullmatch(r"\d+", rack_name):
            return f"{panel_name} - Rack {int(rack_name):02d}"
        if normalize_tag(panel_name) == normalize_tag(rack_name):
            return panel_name
        return f"{panel_name} - {rack_name}"
    if panel_name:
        return panel_name
    if rack_name:
        if re.fullmatch(r"\d+", rack_name):
            base_name = sheet_name or _pick_default_rack_name(original_filename, "", None, [])
            return f"{base_name} - Rack {int(rack_name):02d}"
        return rack_name
    return sheet_name


def _resolve_rack_partition(row, original_filename):
    panel_raw = row.get("panel_raw") or ""
    rack_raw = row.get("rack_raw") or ""
    rack_name = _compose_rack_name(
        panel_raw=panel_raw,
        rack_raw=rack_raw,
        source_sheet=row.get("source_sheet"),
        original_filename=original_filename,
    )
    rack_name = rack_name or _pick_default_rack_name(original_filename, "", None, [row])
    rack_key = normalize_tag(rack_name) or "RACK_IMPORTADO"
    return rack_key, rack_name


def _group_rows_by_slot(normalized_rows):
    by_slot = defaultdict(list)
    without_slot = []
    for row in normalized_rows:
        if row.get("slot_index"):
            by_slot[row["slot_index"]].append(row)
        else:
            without_slot.append(row)
    return by_slot, without_slot


def _pack_rows_without_slot(rows, module_catalog, starting_slot):
    slot_cursor = starting_slot
    module_groups = []
    by_type = defaultdict(list)
    for row in rows:
        key = row.get("type") or "SEM_TIPO"
        by_type[key].append(row)

    for channel_type in sorted(by_type.keys()):
        grouped_rows = sorted(by_type[channel_type], key=lambda item: item["source_row"])
        while grouped_rows:
            desired = len(grouped_rows)
            module_model = _best_module_for_type(module_catalog, channel_type, desired)
            if not module_model:
                take = min(16, len(grouped_rows))
                selected_rows = grouped_rows[:take]
                grouped_rows = grouped_rows[take:]
                module_groups.append(
                    {"slot_index": slot_cursor, "module_model": None, "rows": selected_rows, "source": "packed"}
                )
                slot_cursor += 1
                continue
            capacity = max(1, module_model["quantidade_canais"])
            if len(grouped_rows) <= capacity:
                take = len(grouped_rows)
            else:
                smaller_sizes = [
                    item["quantidade_canais"]
                    for item in module_catalog
                    if item["tipo"] == channel_type and item["quantidade_canais"] < len(grouped_rows)
                ]
                take = max(smaller_sizes) if smaller_sizes else capacity
            selected_rows = grouped_rows[:take]
            grouped_rows = grouped_rows[take:]
            module_groups.append(
                {
                    "slot_index": slot_cursor,
                    "module_model": _best_module_for_type(module_catalog, channel_type, len(selected_rows)),
                    "rows": selected_rows,
                    "source": "packed",
                }
            )
            slot_cursor += 1
    return module_groups


def _build_channel_payloads(group_rows, module_model):
    capacity = module_model["quantidade_canais"] if module_model else len(group_rows)
    rows_by_channel = {}
    next_channel = 1
    for row in group_rows:
        channel_index = row.get("channel_index")
        if channel_index and channel_index not in rows_by_channel:
            rows_by_channel[channel_index] = row
            continue
        while next_channel in rows_by_channel:
            next_channel += 1
        rows_by_channel[next_channel] = row
        next_channel += 1

    channels = []
    for index in range(1, capacity + 1):
        row = rows_by_channel.get(index)
        if row:
            channels.append(
                {
                    "index": index,
                    "tag": row.get("tag") or "",
                    "description": row.get("description") or "",
                    "type": row.get("type") or (module_model or {}).get("tipo") or "",
                    "source_row": row.get("source_row"),
                    "issues": list(row.get("issues") or []),
                }
            )
        else:
            channels.append(
                {
                    "index": index,
                    "tag": "",
                    "description": "",
                    "type": (module_model or {}).get("tipo") or "",
                    "source_row": None,
                    "issues": ["canal_sem_linha_de_origem"],
                }
            )
    return channels


def _build_single_rack_proposal(rack_name, normalized_rows, module_catalog, target_rack=None):
    proposal_warnings = []
    by_slot, rows_without_slot = _group_rows_by_slot(normalized_rows)
    module_groups = []
    for slot_index in sorted(by_slot.keys()):
        slot_rows = sorted(by_slot[slot_index], key=lambda item: (item.get("channel_index") or 999999, item["source_row"]))
        explicit_model = next((item["module_raw"] for item in slot_rows if item.get("module_raw")), "")
        channel_type = next((item["type"] for item in slot_rows if item.get("type")), "")
        module_groups.append(
            {
                "slot_index": slot_index,
                "module_model": _best_module_for_type(module_catalog, channel_type, len(slot_rows), explicit_model=explicit_model),
                "rows": slot_rows,
                "source": "slot",
            }
        )

    next_slot = (max((group["slot_index"] for group in module_groups), default=0) or 0) + 1
    if rows_without_slot:
        module_groups.extend(_pack_rows_without_slot(rows_without_slot, module_catalog, next_slot))
        proposal_warnings.append(
            f"{len(rows_without_slot)} linhas nao traziam slot explicito e foram agrupadas automaticamente."
        )

    modules = []
    max_slot = 0
    conflicts = []
    for group in sorted(module_groups, key=lambda item: item["slot_index"]):
        slot_index = group["slot_index"]
        max_slot = max(max_slot, slot_index)
        module_model = group.get("module_model")
        custom_module = None
        if module_model and module_model.get("id") is None and module_model.get("tipo"):
            custom_module = {
                "modelo": module_model.get("modelo") or "",
                "nome": module_model.get("nome") or module_model.get("modelo") or "",
                "marca": module_model.get("marca") or "IMPORTADO",
                "tipo": module_model.get("tipo") or "",
                "quantidade_canais": int(module_model.get("quantidade_canais") or len(group["rows"]) or 1),
            }
        if not module_model:
            conflicts.append(
                {
                    "slot_index": slot_index,
                    "message": "Nao foi possivel resolver um modelo de modulo para este agrupamento.",
                }
            )
        modules.append(
            {
                "slot_index": slot_index,
                "module_model_id": module_model["id"] if module_model else None,
                "module_model_name": module_model["modelo"] if module_model else "",
                "module_type": module_model["tipo"] if module_model else "",
                "channel_capacity": module_model["quantidade_canais"] if module_model else len(group["rows"]),
                "module_model_source": "custom" if custom_module else "catalog",
                "custom_module": custom_module,
                "channels": _build_channel_payloads(group["rows"], module_model),
                "source": group["source"],
            }
        )

    return {
        "rack_key": normalize_tag(rack_name) or "RACK_IMPORTADO",
        "name": rack_name,
        "slots_total": max(max_slot, len(modules), 1),
        "target_rack_id": target_rack.id if target_rack else None,
        "source_sheets": sorted({row.get("source_sheet") for row in normalized_rows if row.get("source_sheet")}),
        "modules": modules,
        "warnings": proposal_warnings,
        "conflicts": conflicts,
        "summary": {
            "rows": len(normalized_rows),
            "modules": len(modules),
            "slots": max(max_slot, len(modules), 1),
            "with_conflicts": len(conflicts),
        },
    }


def build_import_proposal(
    original_filename,
    normalized_rows,
    module_catalog,
    requested_rack_name="",
    target_rack=None,
    ai_result=None,
):
    if not normalized_rows:
        raise IOImportError("Nao ha linhas normalizadas para montar a proposta.")

    racks_by_key = defaultdict(list)
    for row in normalized_rows:
        rack_key, rack_name = _resolve_rack_partition(row, original_filename=original_filename)
        row["resolved_rack_key"] = rack_key
        row["resolved_rack_name"] = rack_name
        racks_by_key[rack_key].append(row)

    rack_items = sorted(
        racks_by_key.items(),
        key=lambda item: (item[1][0].get("resolved_rack_name") or item[0], item[0]),
    )
    is_single_rack = len(rack_items) == 1

    racks = []
    global_warnings = []
    global_conflicts = []
    total_modules = 0
    total_slots = 0
    for rack_key, rack_rows in rack_items:
        rack_name = rack_rows[0].get("resolved_rack_name") or rack_key
        if is_single_rack:
            rack_name = _pick_default_rack_name(
                original_filename=original_filename,
                requested_rack_name=requested_rack_name,
                ai_result=ai_result,
                normalized_rows=rack_rows,
                target_rack=target_rack,
            )
        rack_payload = _build_single_rack_proposal(
            rack_name=rack_name,
            normalized_rows=rack_rows,
            module_catalog=module_catalog,
            target_rack=target_rack if is_single_rack else None,
        )
        racks.append(rack_payload)
        total_modules += rack_payload["summary"]["modules"]
        total_slots += rack_payload["summary"]["slots"]
        for warning in rack_payload.get("warnings") or []:
            global_warnings.append(f"[{rack_name}] {warning}")
        for conflict in rack_payload.get("conflicts") or []:
            global_conflicts.append(
                {
                    "rack_key": rack_payload["rack_key"],
                    "rack_name": rack_name,
                    "slot_index": conflict.get("slot_index"),
                    "message": conflict.get("message"),
                }
            )

    if target_rack and len(racks) > 1:
        global_conflicts.append(
            {
                "rack_key": normalize_tag(target_rack.nome) or "RACK_DESTINO",
                "rack_name": target_rack.nome,
                "slot_index": None,
                "message": "A proposta gerou multiplos racks; nao e possivel aplicar tudo em um unico rack destino.",
            }
        )

    proposal = {
        "racks": racks,
        "warnings": global_warnings,
        "conflicts": global_conflicts,
        "summary": {
            "rows": len(normalized_rows),
            "modules": total_modules,
            "slots": total_slots,
            "racks": len(racks),
            "with_conflicts": len(global_conflicts),
        },
    }
    if len(racks) == 1:
        proposal["rack"] = {
            "name": racks[0]["name"],
            "slots_total": racks[0]["slots_total"],
            "target_rack_id": racks[0]["target_rack_id"],
        }
        proposal["modules"] = racks[0]["modules"]
    else:
        proposal["rack"] = {}
        proposal["modules"] = []
    return proposal


def _extract_response_text(payload):
    output = payload.get("output") or []
    for item in output:
        content = item.get("content") or []
        for content_item in content:
            if content_item.get("type") == "output_text":
                return content_item.get("text") or ""
    return payload.get("output_text") or ""


def _call_openai_responses(settings_obj, schema_name, schema, system_prompt, user_prompt):
    if not settings_obj.enabled:
        raise IOImportError("Agente de importacao desativado.")
    if settings_obj.provider != settings_obj.Provider.OPENAI:
        raise IOImportError("Provider de agente nao suportado nesta versao.")
    if not settings_obj.api_key:
        raise IOImportError("API key do agente nao configurada.")

    base_url = (settings_obj.api_base_url or "").strip().rstrip("/")
    if not base_url:
        base_url = "https://api.openai.com/v1"
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
    try:
        with urlrequest.urlopen(http_request, timeout=40) as response:
            response_payload = json.loads(response.read().decode("utf-8"))
    except urlerror.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise IOImportError(f"Falha na chamada do agente: HTTP {exc.code} - {detail[:400]}")
    except urlerror.URLError as exc:
        raise IOImportError(f"Falha na chamada do agente: {exc.reason}")
    except (TimeoutError, socket.timeout):
        raise IOImportError("Falha na chamada do agente: timeout apos 40s.")
    except OSError as exc:
        raise IOImportError(f"Falha na chamada do agente: {exc}")

    response_text = _extract_response_text(response_payload)
    if not response_text:
        raise IOImportError("O agente nao retornou texto utilizavel.")
    try:
        return json.loads(response_text)
    except json.JSONDecodeError as exc:
        raise IOImportError(f"O agente retornou JSON invalido: {exc}")


def run_ai_analysis(settings_obj, parsed, normalized_rows, module_catalog):
    sample_rows = []
    for row in normalized_rows[: min(len(normalized_rows), settings_obj.max_rows_for_ai)]:
        sample_rows.append(
            {
                "source_row": row["source_row"],
                "rack_raw": row["rack_raw"],
                "slot_raw": row["slot_raw"],
                "module_raw": row["module_raw"],
                "channel_raw": row["channel_raw"],
                "tag": row["tag"],
                "description": row["description"],
                "type": row["type"],
            }
        )

    schema = {
        "type": "object",
        "properties": {
            "rack_name": {"type": "string"},
            "column_map": {
                "type": "object",
                "properties": {
                    "rack": {"type": "string"},
                    "slot": {"type": "string"},
                    "module_model": {"type": "string"},
                    "channel": {"type": "string"},
                    "tag": {"type": "string"},
                    "description": {"type": "string"},
                    "type": {"type": "string"},
                },
                "required": ["rack", "slot", "module_model", "channel", "tag", "description", "type"],
                "additionalProperties": False,
            },
            "warnings": {"type": "array", "items": {"type": "string"}},
            "notes": {"type": "string"},
        },
        "required": ["rack_name", "column_map", "warnings", "notes"],
        "additionalProperties": False,
    }
    user_prompt = json.dumps(
        {
            "headers": parsed.headers,
            "detected_column_map": parsed.column_map,
            "sample_rows": sample_rows,
            "module_catalog": module_catalog,
        },
        ensure_ascii=True,
    )
    return _call_openai_responses(
        settings_obj=settings_obj,
        schema_name="io_import_analysis",
        schema=schema,
        system_prompt=f"{settings_obj.header_prompt}\n\n{settings_obj.grouping_prompt}",
        user_prompt=user_prompt,
    )


def _format_sheet_warning(sheet_name, warning, multi_sheet):
    warning = _cell_to_text(warning)
    if not warning:
        return ""
    if multi_sheet and sheet_name:
        return f"[{sheet_name}] {warning}"
    return warning


def reprocess_import_job(job, module_catalog, settings_obj=None):
    if not job.source_file:
        raise IOImportError("Arquivo fonte da importacao nao encontrado.")
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
    processed_sheets = []

    for parsed in parsed_sheets:
        try:
            sheet_rows, sheet_map, sheet_warnings = normalize_rows(parsed=parsed, module_catalog=module_catalog, ai_result=None)
        except IOImportError as exc:
            warnings.append(_format_sheet_warning(parsed.sheet_name, str(exc), multi_sheet))
            sheet_summaries.append(
                {
                    "sheet_name": parsed.sheet_name,
                    "header_row_index": parsed.header_row_index + 1,
                    "rows_total": parsed.rows_total,
                    "rows_parsed": 0,
                    "layout": parsed.layout,
                    "column_map": {},
                    "skipped": True,
                }
            )
            continue
        sheet_ai_payload = {}
        if settings_obj and settings_obj.enabled:
            try:
                sheet_ai_payload = run_ai_analysis(
                    settings_obj=settings_obj,
                    parsed=parsed,
                    normalized_rows=sheet_rows,
                    module_catalog=module_catalog,
                )
                sheet_rows, sheet_map, sheet_warnings = normalize_rows(
                    parsed=parsed,
                    module_catalog=module_catalog,
                    ai_result=sheet_ai_payload,
                )
                ai_success += 1
            except IOImportError as exc:
                ai_errors.append(_format_sheet_warning(parsed.sheet_name, str(exc), multi_sheet))
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
                "layout": parsed.layout,
                "column_map": sheet_map,
            }
        )
        processed_sheets.append(parsed)
        for warning in sheet_warnings:
            formatted = _format_sheet_warning(parsed.sheet_name, warning, multi_sheet)
            if formatted:
                warnings.append(formatted)

    if not normalized_rows:
        raise IOImportError("Nenhuma aba util gerou linhas normalizadas para importacao.")

    if settings_obj and settings_obj.enabled:
        if ai_success == len(processed_sheets):
            ai_status = job.AIStatus.SUCCESS
        elif ai_success == 0:
            ai_status = job.AIStatus.FAILED
        else:
            ai_status = job.AIStatus.FAILED
        ai_error = " | ".join(error for error in ai_errors if error)
        if ai_error:
            warnings.extend(error for error in ai_errors if error)
    else:
        ai_error = ""

    proposal = build_import_proposal(
        original_filename=job.original_filename,
        normalized_rows=normalized_rows,
        module_catalog=module_catalog,
        requested_rack_name=job.requested_rack_name,
        target_rack=job.target_rack,
        ai_result=ai_payload,
    )
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


def _apply_single_rack_payload(
    job,
    rack_payload,
    target_rack,
    module_map,
    module_qs,
    rack_model,
    rack_slot_model,
    rack_module_model,
    channel_model,
    plant_model,
):
    type_model = module_qs.model._meta.get_field("tipo_base").remote_field.model
    modules_payload = rack_payload.get("modules") or []
    if not modules_payload:
        raise IOImportError("Um dos racks propostos nao possui modulos para aplicar.")

    rack = target_rack
    if rack is None:
        rack = rack_model.objects.create(
            cliente=job.cliente,
            inventario=job.requested_inventario,
            local=job.requested_local,
            grupo=job.requested_grupo,
            id_planta=(
                plant_model.objects.get_or_create(codigo=job.requested_planta_code.upper())[0]
                if job.requested_planta_code
                else None
            ),
            nome=(rack_payload.get("name") or job.requested_rack_name or "Rack importado")[:120],
            descricao=f"Importado de planilha: {job.original_filename}"[:500],
            slots_total=max(int(rack_payload.get("slots_total") or len(modules_payload) or 1), 1),
        )
        rack_slot_model.objects.bulk_create(
            [rack_slot_model(rack=rack, posicao=index) for index in range(1, rack.slots_total + 1)]
        )
    else:
        max_slot = max(int(module["slot_index"]) for module in modules_payload)
        if max_slot > rack.slots_total:
            rack_slot_model.objects.bulk_create(
                [rack_slot_model(rack=rack, posicao=index) for index in range(rack.slots_total + 1, max_slot + 1)]
            )
            rack.slots_total = max_slot
            rack.save(update_fields=["slots_total"])

    slot_map = {
        slot.posicao: slot
        for slot in rack.slots.select_related("modulo", "modulo__modulo_modelo").all()
    }

    for module_payload in modules_payload:
        slot_index = int(module_payload["slot_index"])
        slot = slot_map.get(slot_index)
        if not slot:
            raise IOImportError(f"Slot {slot_index} nao encontrado no rack destino.")
        module_model_id = module_payload.get("module_model_id")
        module_model = module_map.get(str(module_model_id)) if module_model_id else None
        if not module_model:
            custom_module = module_payload.get("custom_module") or {}
            module_type = normalize_type(custom_module.get("tipo") or module_payload.get("module_type"))
            capacity = int(
                custom_module.get("quantidade_canais")
                or module_payload.get("channel_capacity")
                or len(module_payload.get("channels") or [])
                or 1
            )
            if not module_type:
                raise IOImportError(f"Modulo do slot {slot_index} nao esta catalogado e o tipo nao pode ser inferido.")
            tipo_base = type_model.objects.filter(nome=module_type).first()
            if not tipo_base:
                raise IOImportError(f"Tipo base {module_type} nao encontrado para criar modulo do slot {slot_index}.")
            modelo = (
                _cell_to_text(custom_module.get("modelo"))
                or _cell_to_text(module_payload.get("module_model_name"))
                or f"{module_type}-{capacity:02d} CUSTOM"
            )[:80]
            nome = (_cell_to_text(custom_module.get("nome")) or modelo or f"Modulo {module_type}")[:120]
            marca = (_cell_to_text(custom_module.get("marca")) or "IMPORTADO")[:80]
            module_model = module_qs.model.objects.filter(
                cliente=job.cliente,
                modelo=modelo,
                tipo_base=tipo_base,
                quantidade_canais=capacity,
            ).first()
            if not module_model:
                module_model = module_qs.model.objects.create(
                    cliente=job.cliente,
                    nome=nome,
                    modelo=modelo,
                    marca=marca,
                    quantidade_canais=capacity,
                    tipo_base=tipo_base,
                    is_default=False,
                )
            module_map[str(module_model.id)] = module_model

        rack_module = slot.modulo
        if rack_module:
            if rack_module.modulo_modelo_id != module_model.id:
                raise IOImportError(
                    f"Conflito no slot {slot_index}: ja existe um modulo diferente no rack destino."
                )
        else:
            rack_module = rack_module_model.objects.create(rack=rack, modulo_modelo=module_model)
            channel_model.objects.bulk_create(
                [
                    channel_model(modulo=rack_module, indice=index, descricao="", tipo=module_model.tipo_base)
                    for index in range(1, module_model.quantidade_canais + 1)
                ]
            )
            slot.modulo = rack_module
            slot.save(update_fields=["modulo"])
            slot_map[slot_index] = slot

        channel_lookup = {channel.indice: channel for channel in rack_module.canais.select_related("tipo").all()}
        to_update = []
        for channel_payload in module_payload.get("channels") or []:
            channel = channel_lookup.get(int(channel_payload["index"]))
            if not channel:
                continue
            channel.tag = normalize_tag(channel_payload.get("tag"))
            channel.descricao = (channel_payload.get("description") or "").strip()
            channel_type = normalize_type(channel_payload.get("type"))
            if channel_type:
                matching_type = next(
                    (module.tipo_base for module in module_qs if module.tipo_base.nome == channel_type),
                    None,
                )
                if matching_type:
                    channel.tipo = matching_type
            to_update.append(channel)
        if to_update:
            channel_model.objects.bulk_update(to_update, ["tag", "descricao", "tipo"])
    return rack


def apply_import_job(
    job,
    user,
    rack_model,
    rack_slot_model,
    rack_module_model,
    channel_model,
    module_qs,
    plant_model,
    selected_rack_keys=None,
):
    from django.db import transaction

    proposal = job.proposal_payload or {}
    proposal_racks = proposal.get("racks") or []
    if not proposal_racks and proposal.get("modules"):
        proposal_racks = [
            {
                "name": (proposal.get("rack") or {}).get("name") or job.requested_rack_name or "Rack importado",
                "slots_total": (proposal.get("rack") or {}).get("slots_total") or len(proposal.get("modules") or []),
                "modules": proposal.get("modules") or [],
                "summary": proposal.get("summary") or {},
            }
        ]

    conflicts = proposal.get("conflicts") or []
    if conflicts:
        raise IOImportError("A proposta possui conflitos e nao pode ser aplicada.")
    if not proposal_racks:
        raise IOImportError("A proposta nao possui racks para aplicar.")
    if job.target_rack and len(proposal_racks) > 1:
        raise IOImportError("Esta importacao gerou multiplos racks e nao pode ser aplicada em um rack destino unico.")

    module_map = {str(module.id): module for module in module_qs}
    existing_apply_log = dict(job.apply_log or {})
    applied_rack_key_map = dict(existing_apply_log.get("applied_rack_keys") or {})
    if selected_rack_keys:
        selected_set = {str(item) for item in selected_rack_keys if str(item).strip()}
        proposal_racks = [
            rack_payload
            for rack_payload in proposal_racks
            if str(rack_payload.get("rack_key") or "").strip() in selected_set
        ]
        if not proposal_racks:
            raise IOImportError("Nenhum rack selecionado para aplicacao.")
    proposal_racks = [
        rack_payload
        for rack_payload in proposal_racks
        if str(rack_payload.get("rack_key") or "").strip() not in applied_rack_key_map
    ]
    if not proposal_racks:
        selected_ids = [
            rack_id
            for rack_key, rack_id in applied_rack_key_map.items()
            if not selected_rack_keys or rack_key in {str(item) for item in selected_rack_keys}
        ]
        return list(rack_model.objects.filter(id__in=selected_ids))

    applied_racks = []

    with transaction.atomic():
        for index, rack_payload in enumerate(proposal_racks):
            rack = _apply_single_rack_payload(
                job=job,
                rack_payload=rack_payload,
                target_rack=job.target_rack if index == 0 else None,
                module_map=module_map,
                module_qs=module_qs,
                rack_model=rack_model,
                rack_slot_model=rack_slot_model,
                rack_module_model=rack_module_model,
                channel_model=channel_model,
                plant_model=plant_model,
            )
            applied_racks.append(rack)
            rack_key = str(rack_payload.get("rack_key") or "").strip()
            if rack_key:
                applied_rack_key_map[rack_key] = rack.id

        all_proposal_keys = {
            str(rack_payload.get("rack_key") or "").strip()
            for rack_payload in (proposal.get("racks") or [])
            if str(rack_payload.get("rack_key") or "").strip()
        }
        all_applied_ids = list(dict.fromkeys(list(applied_rack_key_map.values())))
        job.applied_rack = rack_model.objects.filter(id__in=all_applied_ids).order_by("id").first()
        job.status = job.Status.APPLIED if all_proposal_keys and all_proposal_keys.issubset(applied_rack_key_map.keys()) else job.Status.REVIEW
        job.apply_log = {
            "applied_by": getattr(user, "username", "") or "",
            "racks_applied": len(all_applied_ids),
            "modules_applied": sum(
                len(rack_payload.get("modules") or [])
                for rack_payload in (proposal.get("racks") or [])
                if str(rack_payload.get("rack_key") or "").strip() in applied_rack_key_map
            ),
            "applied_rack_ids": all_applied_ids,
            "applied_rack_keys": applied_rack_key_map,
        }
        job.save(update_fields=["applied_rack", "status", "apply_log"])
    return applied_racks
