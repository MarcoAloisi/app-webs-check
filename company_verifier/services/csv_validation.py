"""CSV ingestion and validation."""

from __future__ import annotations

import csv
import io
from pathlib import Path
from collections.abc import Iterable

import pandas as pd

from company_verifier.models import CompanyInput, ProcessingStatus, UploadValidationResult, ValidationIssue
from company_verifier.utils.web import build_record_hash, extract_domain, is_probably_valid_url, normalize_column_name, normalize_url

REQUIRED_COLUMNS = {"nombre_empresa", "web"}
RESULT_COLUMNS = {
    "web_verificada",
    "existe",
    "operativa",
    "absorbida_adquirida",
    "rebranded",
    "legitima",
    "riesgo_fraude",
    "tipologia_riesgo",
    "score_confianza",
    "pasos_verificados",
    "justificacion_detallada",
    "fuentes",
    "banderas_rojas",
    "banderas_verdes",
    "requiere_revision_manual",
    "prompt_enviado",
    "respuesta_llm_cruda",
    "processing_status",
}
CHECKPOINT_MARKER_COLUMNS = {
    "web_verificada",
    "existe",
    "operativa",
    "legitima",
    "riesgo_fraude",
    "score_confianza",
    "pasos_verificados",
    "justificacion_detallada",
    "processing_status",
}


def _decode_bytes(raw: bytes) -> tuple[str, str]:
    for encoding in ("utf-8-sig", "utf-8", "latin-1"):
        try:
            return raw.decode(encoding), encoding
        except UnicodeDecodeError:
            continue
    return raw.decode("utf-8", errors="replace"), "utf-8-replace"


def _drop_empty_rows(frame: pd.DataFrame) -> pd.DataFrame:
    cleaned = frame.fillna("")
    mask = cleaned.apply(lambda row: any(str(value).strip() for value in row), axis=1)
    return cleaned[mask].copy()


def _normalize_dataframe(frame: pd.DataFrame) -> pd.DataFrame:
    frame = frame.copy()
    frame.columns = [normalize_column_name(column) for column in frame.columns]
    return frame


def _build_issues(row_numbers: Iterable[int], message: str, severity: str = "warning") -> ValidationIssue:
    return ValidationIssue(severity=severity, message=message, row_numbers=list(row_numbers))


def _read_csv_with_fallbacks(decoded: str) -> pd.DataFrame:
    candidates: list[str | None] = [None, ",", ";", "\t", "|"]
    best_frame: pd.DataFrame | None = None
    best_score = -1

    for separator in candidates:
        try:
            common_kwargs = {
                "dtype": str,
                "keep_default_na": False,
                "skipinitialspace": True,
            }
            if separator is None:
                frame = pd.read_csv(io.StringIO(decoded), sep=None, engine="python", **common_kwargs)
            else:
                frame = pd.read_csv(io.StringIO(decoded), sep=separator, **common_kwargs)
        except (pd.errors.ParserError, csv.Error, UnicodeError):
            continue

        normalized_columns = {normalize_column_name(column) for column in frame.columns}
        score = len(REQUIRED_COLUMNS.intersection(normalized_columns))
        if score > best_score:
            best_score = score
            best_frame = frame
        if score == len(REQUIRED_COLUMNS):
            return frame

    if best_frame is not None:
        return best_frame
    return pd.read_csv(io.StringIO(decoded), dtype=str, keep_default_na=False)


def _excel_engine(filename: str) -> str:
    suffix = Path(filename).suffix.lower()
    if suffix == ".xls":
        return "xlrd"
    return "openpyxl"


def list_sheet_names(raw: bytes, filename: str) -> list[str]:
    """Return workbook sheet names for Excel uploads."""
    suffix = Path(filename).suffix.lower()
    if suffix not in {".xlsx", ".xls"}:
        return []
    with pd.ExcelFile(io.BytesIO(raw), engine=_excel_engine(filename)) as workbook:
        return list(workbook.sheet_names)


def _load_frame(raw: bytes, filename: str, sheet_name: str | None = None) -> tuple[pd.DataFrame, str]:
    suffix = Path(filename).suffix.lower()
    if suffix == ".csv":
        decoded, encoding = _decode_bytes(raw)
        return _read_csv_with_fallbacks(decoded), encoding

    if suffix in {".xlsx", ".xls"}:
        with pd.ExcelFile(io.BytesIO(raw), engine=_excel_engine(filename)) as workbook:
            selected_sheet = sheet_name or workbook.sheet_names[0]
            frame = pd.read_excel(workbook, sheet_name=selected_sheet, dtype=str, keep_default_na=False)
        return frame, "binary"

    raise ValueError("Formato no soportado. Sube un CSV, XLSX o XLS.")


def _validate_frame(frame: pd.DataFrame, *, encoding: str) -> tuple[pd.DataFrame, UploadValidationResult]:
    frame = _drop_empty_rows(_normalize_dataframe(frame))
    missing = REQUIRED_COLUMNS - set(frame.columns)
    if missing:
        raise ValueError(
            "Faltan columnas requeridas: "
            f"{', '.join(sorted(missing))}. "
            "Asegúrate de subir un archivo con cabecera y columnas nombre_empresa y web."
        )

    frame["nombre_empresa"] = frame["nombre_empresa"].astype(str).str.strip()
    frame["web"] = frame["web"].astype(str).str.strip()
    frame["web_normalized"] = frame["web"].map(normalize_url)
    frame["domain_normalized"] = frame["web_normalized"].map(extract_domain)
    frame["row_number"] = list(range(2, len(frame) + 2))
    frame["record_hash"] = [
        build_record_hash(name, url)
        for name, url in zip(frame["nombre_empresa"], frame["web_normalized"], strict=False)
    ]
    if "processing_status" not in frame.columns:
        frame["processing_status"] = ProcessingStatus.PENDING.value

    issues: list[ValidationIssue] = []
    empty_company_rows = frame.index[frame["nombre_empresa"].eq("")].tolist()
    if empty_company_rows:
        issues.append(_build_issues((idx + 2 for idx in empty_company_rows), "Nombre de empresa vacío.", severity="error"))

    invalid_url_rows = [int(row.row_number) for row in frame.itertuples() if not is_probably_valid_url(row.web_normalized)]
    if invalid_url_rows:
        issues.append(_build_issues(invalid_url_rows, "URL malformada o sin dominio válido.", severity="error"))

    duplicate_mask = frame.duplicated(subset=["record_hash"], keep="first")
    duplicates_removed = int(duplicate_mask.sum())
    if duplicates_removed:
        duplicate_rows = frame.loc[duplicate_mask, "row_number"].astype(int).tolist()
        issues.append(_build_issues(duplicate_rows, "Duplicados detectados y descartados."))
        frame = frame.loc[~duplicate_mask].copy()

    valid_mask = frame["row_number"].astype(int).isin(invalid_url_rows)
    if invalid_url_rows:
        frame = frame.loc[~valid_mask].copy()

    rows = [
        CompanyInput(
            row_number=int(row.row_number),
            nombre_empresa=str(row.nombre_empresa),
            web=str(row.web),
            web_normalized=str(row.web_normalized),
            domain_normalized=str(row.domain_normalized),
            record_hash=str(row.record_hash),
            processing_status=ProcessingStatus(str(row.processing_status)),
        )
        for row in frame.itertuples()
    ]
    validation = UploadValidationResult(
        rows=rows,
        issues=issues,
        duplicates_removed=duplicates_removed,
        encoding_used=encoding,
        is_checkpoint_file=CHECKPOINT_MARKER_COLUMNS.issubset(set(frame.columns)),
    )
    return frame.reset_index(drop=True), validation


def load_tabular_bytes(
    raw: bytes,
    filename: str,
    *,
    sheet_name: str | None = None,
) -> tuple[pd.DataFrame, UploadValidationResult]:
    """Load and validate an uploaded CSV, XLSX or XLS file."""
    frame, encoding = _load_frame(raw, filename, sheet_name)
    return _validate_frame(frame, encoding=encoding)


def load_csv_bytes(raw: bytes) -> tuple[pd.DataFrame, UploadValidationResult]:
    """Load and validate an uploaded CSV or checkpoint file."""
    return load_tabular_bytes(raw, "upload.csv")


def extract_completed_results(frame: pd.DataFrame) -> list[dict[str, str]]:
    """Extract flattened results already present inside a checkpoint CSV."""
    available = RESULT_COLUMNS.intersection(frame.columns)
    if "processing_status" not in available:
        return []
    completed = frame.loc[frame["processing_status"].eq(ProcessingStatus.COMPLETED.value)].copy()
    if completed.empty:
        return []
    base_columns = [column for column in ("record_hash", "nombre_empresa", "web", "web_input") if column in completed.columns]
    result_columns = base_columns + sorted(available - set(base_columns))
    return completed[result_columns].to_dict(orient="records")
