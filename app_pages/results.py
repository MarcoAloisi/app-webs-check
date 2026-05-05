from __future__ import annotations

import io
import json
from pathlib import Path

import pandas as pd
import streamlit as st

from company_verifier.models import CompanyVerificationResult
from company_verifier.run_controller import drain_worker_events, worker_is_running
from company_verifier.services.csv_validation import extract_completed_results, list_sheet_names
from company_verifier.services.export_service import ExportService
from company_verifier.session import get_results_view_source, set_results_view_source

_export_service = ExportService()


@st.cache_data(show_spinner=False)
def _build_frame(results_data: tuple[str, ...]) -> pd.DataFrame:
    results = [CompanyVerificationResult.model_validate_json(item) for item in results_data]
    return _export_service.to_dataframe(results)


@st.cache_data(show_spinner=False)
def _build_exports(results_data: tuple[str, ...]) -> tuple[bytes, bytes]:
    results = [CompanyVerificationResult.model_validate_json(item) for item in results_data]
    return _export_service.to_csv_bytes(results), _export_service.to_excel_bytes(results)


@st.cache_data(show_spinner=False)
def _sheet_names(file_name: str, raw_bytes: bytes) -> list[str]:
    return list_sheet_names(raw_bytes, file_name)


@st.cache_data(show_spinner=False)
def _parse_results_upload(file_name: str, raw_bytes: bytes, sheet_name: str | None) -> tuple[str, ...]:
    suffix = Path(file_name).suffix.lower()
    if suffix == ".csv":
        frame = pd.read_csv(io.BytesIO(raw_bytes), dtype=str, keep_default_na=False)
    elif suffix in {".xlsx", ".xls"}:
        with pd.ExcelFile(io.BytesIO(raw_bytes), engine="xlrd" if suffix == ".xls" else "openpyxl") as workbook:
            selected_sheet = sheet_name or workbook.sheet_names[0]
            frame = pd.read_excel(workbook, sheet_name=selected_sheet, dtype=str, keep_default_na=False)
    else:
        raise ValueError("Formato no soportado. Sube un CSV, XLSX o XLS.")

    frame = frame.fillna("")
    records = extract_completed_results(frame)
    if not records:
        records = frame.to_dict(orient="records")
    results = _export_service.from_flat_records(records)
    return tuple(result.model_dump_json() for result in results)


def _serialized_results() -> tuple[str, ...]:
    return tuple(CompanyVerificationResult.model_validate(item).model_dump_json() for item in st.session_state.get("results", []))


def _resolve_results_source() -> tuple[tuple[str, ...], str | None]:
    stored_source, stored_serialized_results, stored_message = get_results_view_source()
    source = st.radio(
        "Origen de resultados",
        options=["Sesión actual", "Archivo externo"],
        index=0 if stored_source == "session" else 1,
        horizontal=True,
    )
    if source == "Sesión actual":
        serialized_results = _serialized_results()
        message = "Mostrando resultados de la sesión actual."
        set_results_view_source("session", serialized_results=serialized_results, message=message)
        return serialized_results, message

    uploaded_file = st.file_uploader(
        "Subir resultados exportados o checkpoint",
        type=["csv", "xlsx", "xls"],
        key="results_upload_file",
    )
    if uploaded_file is None:
        if stored_source == "external" and stored_serialized_results:
            return stored_serialized_results, stored_message
        st.info("Sube un archivo de resultados para visualizarlo aquí sin reemplazar la sesión actual.")
        return (), None

    raw = uploaded_file.getvalue()
    selected_sheet: str | None = None
    sheet_names = _sheet_names(uploaded_file.name, raw)
    if len(sheet_names) > 1:
        selected_sheet = st.selectbox("Hoja", options=sheet_names, key="results_upload_sheet")

    try:
        serialized_results = _parse_results_upload(uploaded_file.name, raw, selected_sheet)
    except ValueError as exc:
        st.warning(str(exc))
        return (), None

    if not serialized_results:
        st.warning("El archivo no contiene resultados procesados para mostrar.")
        return (), None
    message = f"Mostrando resultados cargados desde {uploaded_file.name}."
    set_results_view_source("external", serialized_results=serialized_results, message=message)
    return serialized_results, message


def _render_results_page() -> None:
    st.subheader("Resultados y filtros")
    serialized_results, source_message = _resolve_results_source()
    if not serialized_results:
        if source_message is not None:
            st.info("Todavía no hay resultados para mostrar.")
        return

    if source_message:
        st.caption(source_message)

    frame = _build_frame(serialized_results)
    status_options = ["todos", "si", "no", "indeterminado"]
    risk_options = ["todos", "bajo", "medio", "alto"]
    review_options = ["todos", "sí", "no"]

    col1, col2, col3, col4 = st.columns(4)
    exists_filter = col1.selectbox("Existe", status_options)
    operative_filter = col2.selectbox("Operativa", status_options)
    risk_filter = col3.selectbox("Riesgo", risk_options)
    review_filter = col4.selectbox("Revisión manual", review_options)
    score_range = st.slider("Rango score", min_value=0, max_value=100, value=(0, 100))

    filtered = frame.copy()
    if exists_filter != "todos":
        filtered = filtered.loc[filtered["existe"] == exists_filter]
    if operative_filter != "todos":
        filtered = filtered.loc[filtered["operativa"] == operative_filter]
    if risk_filter != "todos":
        filtered = filtered.loc[filtered["riesgo_fraude"] == risk_filter]
    if review_filter != "todos":
        expected = review_filter == "sí"
        filtered = filtered.loc[filtered["requiere_revision_manual"] == expected]
    filtered = filtered.loc[filtered["score_confianza"].between(score_range[0], score_range[1])]

    summary_cols = st.columns(4)
    summary_cols[0].metric("% activas", f"{(frame['operativa'].eq('si').mean() * 100):.1f}%")
    summary_cols[1].metric("% sospechosas", f"{(frame['legitima'].eq('sospechosa').mean() * 100):.1f}%")
    summary_cols[2].metric("% revisión manual", f"{(frame['requiere_revision_manual'].mean() * 100):.1f}%")
    summary_cols[3].metric("Score medio", f"{frame['score_confianza'].mean():.1f}")

    visible_columns = [
        "nombre_empresa",
        "web_input",
        "web_verificada",
        "existe",
        "operativa",
        "legitima",
        "riesgo_fraude",
        "score_confianza",
        "requiere_revision_manual",
    ]
    st.dataframe(filtered[visible_columns], width="stretch", hide_index=True)

    csv_bytes, excel_bytes = _build_exports(serialized_results)
    export_cols = st.columns(2)
    export_cols[0].download_button("Exportar CSV completo", data=csv_bytes, file_name="resultados_completos.csv", mime="text/csv", width="stretch")
    export_cols[1].download_button("Exportar Excel completo", data=excel_bytes, file_name="resultados_completos.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", width="stretch")

    st.markdown("### Detalle expandible")
    for record in filtered.head(50).to_dict(orient="records"):
        label = f"{record['nombre_empresa']} · score {record['score_confianza']} · riesgo {record['riesgo_fraude']}"
        with st.expander(label):
            st.write(record["justificacion_detallada"])
            try:
                pasos = json.loads(record["pasos_verificados"])
            except json.JSONDecodeError:
                pasos = []
            st.json({"pasos_verificados": pasos, "banderas_rojas": json.loads(record["banderas_rojas"]), "banderas_verdes": json.loads(record["banderas_verdes"]), "fuentes": json.loads(record["fuentes"])})

    if len(filtered) > 50:
        st.caption("Se muestran los primeros 50 expandibles para mantener la interfaz fluida.")


if worker_is_running():
    @st.fragment(run_every="1s")
    def _render_live_results() -> None:
        drain_worker_events()
        _render_results_page()

    _render_live_results()
else:
    drain_worker_events()
    _render_results_page()
