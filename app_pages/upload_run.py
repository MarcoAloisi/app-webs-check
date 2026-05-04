from __future__ import annotations

import time

import pandas as pd
import streamlit as st

from company_verifier.models import CompanyInput, CompanyVerificationResult, VerificationRunMetrics
from company_verifier.services.cost_estimator import CostEstimatorService
from company_verifier.services.csv_validation import extract_completed_results, load_csv_bytes
from company_verifier.services.export_service import ExportService
from company_verifier.services.verification_orchestrator import VerificationOrchestrator
from company_verifier.session import append_log, get_metrics, get_settings, reset_run_state, update_metrics
from company_verifier.storage.checkpoint_store import CheckpointStore

_export_service = ExportService()
_checkpoint_store = CheckpointStore()
_cost_service = CostEstimatorService()


@st.cache_data(show_spinner=False)
def _parse_upload(raw_bytes: bytes) -> tuple[pd.DataFrame, dict, list[dict[str, str]]]:
    frame, validation = load_csv_bytes(raw_bytes)
    completed = extract_completed_results(frame)
    return frame, validation.model_dump(mode="json"), completed



def _get_api_key() -> str | None:
    try:
        return st.secrets.get("OPENROUTER_API_KEY", "") or None
    except Exception:  # noqa: BLE001
        return None



def _current_rows() -> list[CompanyInput]:
    return [CompanyInput.model_validate(item) for item in st.session_state.get("upload_rows", [])]



def _current_results() -> list[CompanyVerificationResult]:
    return [CompanyVerificationResult.model_validate(item) for item in st.session_state.get("results", [])]



def _load_upload(uploaded_file: st.runtime.uploaded_file_manager.UploadedFile) -> None:
    raw = uploaded_file.getvalue()
    frame, validation_data, completed_records = _parse_upload(raw)
    validation_rows = validation_data.pop("rows")
    reset_run_state(keep_upload=False)
    st.session_state["uploaded_filename"] = uploaded_file.name
    st.session_state["source_dataframe"] = frame
    st.session_state["upload_rows"] = validation_rows
    st.session_state["validation_summary"] = validation_data
    st.session_state["upload_issues"] = validation_data.get("issues", [])

    restored_results = _export_service.from_flat_records(completed_records)
    st.session_state["results"] = [item.model_dump(mode="json") for item in restored_results]
    st.session_state["results_by_hash"] = {
        record["record_hash"]: model.model_dump(mode="json")
        for record, model in zip(completed_records, restored_results, strict=False)
    }
    restored_count = len(restored_results)
    update_metrics(
        total_rows=len(validation_rows),
        processed_rows=restored_count,
        completed_rows=restored_count,
        batches_completed=0,
    )
    if restored_count:
        append_log(f"Checkpoint cargado con {restored_count} resultados previos.")
    else:
        append_log(f"Archivo cargado: {uploaded_file.name}")



def _start_run() -> None:
    if not st.session_state.get("upload_rows"):
        st.warning("Primero carga un CSV válido.")
        return
    metrics = get_metrics()
    if not metrics.started_at:
        update_metrics(started_at=time.time(), total_rows=len(st.session_state["upload_rows"]))
    st.session_state["run_status"] = "running"
    st.session_state["stop_requested"] = False
    append_log("Ejecución iniciada.")



def _request_stop() -> None:
    st.session_state["stop_requested"] = True
    append_log("Se detendrá al terminar el batch actual.")



def _pending_rows() -> list[CompanyInput]:
    done_hashes = set(st.session_state.get("results_by_hash", {}).keys())
    return [row for row in _current_rows() if row.record_hash not in done_hashes]



def _refresh_checkpoint() -> None:
    results = _current_results()
    metrics = get_metrics()
    settings = get_settings()
    source_frame = st.session_state.get("source_dataframe", pd.DataFrame())
    st.session_state["latest_checkpoint_json"] = _checkpoint_store.build_payload(source_frame, results, settings, metrics)
    st.session_state["latest_checkpoint_csv"] = _checkpoint_store.build_checkpoint_csv(source_frame, results)
    st.session_state["checkpoint_ready"] = True
    append_log("Checkpoint actualizado en memoria y listo para descarga.")



def _process_next_batch() -> None:
    try:
        pending = _pending_rows()
        if not pending:
            st.session_state["run_status"] = "completed"
            update_metrics(finished_at=time.time())
            _refresh_checkpoint()
            append_log("No quedan empresas pendientes.")
            return

        settings = get_settings()
        current_row = pending[0]
        metrics = get_metrics()
        current_position = metrics.processed_rows + 1
        batch_number = ((current_position - 1) // settings.batch_size) + 1
        batch_offset = (current_position - 1) % settings.batch_size + 1
        append_log(
            f"Procesando empresa {current_position}/{metrics.total_rows or len(_current_rows())} · "
            f"batch {batch_number} · elemento {batch_offset}/{settings.batch_size}: {current_row.nombre_empresa}"
        )
        estimator = _cost_service.estimate([current_row], settings)
        orchestrator = VerificationOrchestrator(_get_api_key())
        result = orchestrator.process_company(current_row, settings=settings, log_callback=append_log)
        results_by_hash = dict(st.session_state.get("results_by_hash", {}))
        result_list = list(st.session_state.get("results", []))
        serialized = result.model_dump(mode="json")
        results_by_hash[current_row.record_hash] = serialized
        result_list.append(serialized)
        st.session_state["last_processed_hash"] = current_row.record_hash
        st.session_state["results_by_hash"] = results_by_hash
        st.session_state["results"] = result_list

        processed_rows = metrics.processed_rows + 1
        update_metrics(
            processed_rows=processed_rows,
            completed_rows=len(result_list),
            batches_completed=processed_rows // settings.batch_size,
            estimated_cost_usd=round(metrics.estimated_cost_usd + estimator.estimated_cost_usd, 4),
        )
        append_log(f"Empresa completada: {current_row.nombre_empresa}")

        is_checkpoint_turn = processed_rows % settings.checkpoint_interval == 0
        if is_checkpoint_turn or st.session_state.get("stop_requested") or len(_pending_rows()) == 0:
            _refresh_checkpoint()

        if st.session_state.get("stop_requested"):
            st.session_state["run_status"] = "stopped"
            update_metrics(finished_at=time.time())
            append_log("Ejecución detenida por el usuario.")
        elif not _pending_rows():
            st.session_state["run_status"] = "completed"
            update_metrics(finished_at=time.time())
            append_log("Ejecución finalizada.")
    except Exception as exc:  # noqa: BLE001
        st.session_state["run_status"] = "failed"
        update_metrics(finished_at=time.time())
        append_log(f"Error durante el batch: {exc}")
        raise



def _eta_text() -> str:
    metrics = get_metrics()
    if not metrics.started_at or metrics.processed_rows == 0 or metrics.total_rows == 0:
        return "n/d"
    elapsed = max(1.0, time.time() - metrics.started_at)
    rate = metrics.processed_rows / elapsed
    remaining = max(0, metrics.total_rows - metrics.processed_rows)
    eta_seconds = int(remaining / rate) if rate else 0
    minutes, seconds = divmod(eta_seconds, 60)
    hours, minutes = divmod(minutes, 60)
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}"


if st.session_state.get("run_status") == "running":
    _process_next_batch()

st.subheader("Carga del CSV y control de ejecución")
with st.container(border=True):
    uploaded_file = st.file_uploader("CSV de entrada o checkpoint CSV", type=["csv"])
    if uploaded_file is not None and uploaded_file.name != st.session_state.get("uploaded_filename"):
        _load_upload(uploaded_file)

rows = _current_rows()
results = _current_results()
metrics = get_metrics()
settings = get_settings()
pending_rows = _pending_rows() if rows else []

col1, col2, col3, col4 = st.columns(4)
col1.metric("Filas válidas", len(rows))
col2.metric("Resultados generados", len(results))
col3.metric("Pendientes", len(pending_rows))
col4.metric("ETA", _eta_text())

progress_total = metrics.total_rows or len(rows) or 1
st.progress(min(metrics.processed_rows / progress_total, 1.0), text=f"{metrics.processed_rows}/{progress_total} empresas procesadas")

if rows:
    estimate = _cost_service.estimate(rows[: settings.batch_size], settings)
    st.caption(
        f"Estimación rápida por batch: ~{estimate.estimated_total_tokens} tokens y ${estimate.estimated_cost_usd:.4f} con {settings.model}."
    )

control_cols = st.columns([1, 1, 1, 2])
if control_cols[0].button("Iniciar / reanudar", type="primary", disabled=not rows or st.session_state.get("run_status") == "running"):
    _start_run()
    _process_next_batch()
    st.rerun()
if control_cols[1].button("Detener", disabled=st.session_state.get("run_status") != "running"):
    _request_stop()
if control_cols[2].button("Reset sesión", disabled=not rows and not results):
    reset_run_state(keep_upload=False)
    st.rerun()
control_cols[3].write(f"Estado actual: **{st.session_state.get('run_status', 'idle')}**")

validation = st.session_state.get("validation_summary") or {}
issues = validation.get("issues", [])
if issues:
    st.warning("Se detectaron incidencias en la carga.")
    st.dataframe(pd.DataFrame(issues), use_container_width=True, hide_index=True)

if st.session_state.get("checkpoint_ready"):
    st.success("Hay un checkpoint actualizado listo para descarga.")
    download_cols = st.columns(3)
    download_cols[0].download_button(
        "Descargar checkpoint CSV",
        data=st.session_state.get("latest_checkpoint_csv", ""),
        file_name="checkpoint_resultados.csv",
        mime="text/csv",
        use_container_width=True,
    )
    download_cols[1].download_button(
        "Descargar checkpoint JSON",
        data=st.session_state.get("latest_checkpoint_json", ""),
        file_name="checkpoint_resultados.json",
        mime="application/json",
        use_container_width=True,
    )
    download_cols[2].download_button(
        "Descargar resultados JSONL",
        data=_export_service.jsonl_bytes(results),
        file_name="resultados.jsonl",
        mime="application/jsonl",
        use_container_width=True,
    )

with st.expander("Log en vivo", expanded=True):
    logs = st.session_state.get("logs", [])
    st.code("\n".join(logs[-40:]) or "Sin eventos todavía.", language="text")

if not rows:
    st.info("Sube un CSV con columnas nombre_empresa y web para empezar.")

if st.session_state.get("run_status") == "running":
    st.info("Procesando en batches secuenciales. La página se irá refrescando entre batches para permitir detener y descargar checkpoints.")
    time.sleep(0.2)
    st.rerun()
