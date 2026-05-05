from __future__ import annotations

from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
import hashlib
import queue
import threading
import time
from typing import Any

import pandas as pd
import streamlit as st

from company_verifier.models import AppSettings, CompanyInput, CompanyVerificationResult
from company_verifier.run_controller import current_results, drain_worker_events, get_stop_event, pending_rows, refresh_checkpoint, worker_is_running
from company_verifier.services.cost_estimator import CostEstimatorService
from company_verifier.services.csv_validation import extract_completed_results, list_sheet_names, load_tabular_bytes
from company_verifier.services.export_service import ExportService
from company_verifier.session import append_log, get_metrics, get_settings, reset_run_state, update_metrics

_export_service = ExportService()
_cost_service = CostEstimatorService()


@st.cache_data(show_spinner=False)
def _sheet_names(file_name: str, raw_bytes: bytes) -> list[str]:
    return list_sheet_names(raw_bytes, file_name)


@st.cache_data(show_spinner=False)
def _parse_upload(file_name: str, raw_bytes: bytes, sheet_name: str | None) -> tuple[pd.DataFrame, dict, list[dict[str, str]]]:
    frame, validation = load_tabular_bytes(raw_bytes, file_name, sheet_name=sheet_name)
    completed = extract_completed_results(frame)
    return frame, validation.model_dump(mode="json"), completed



def _get_api_key() -> str | None:
    try:
        return st.secrets.get("OPENROUTER_API_KEY", "") or None
    except Exception:  # noqa: BLE001
        return None

def _current_rows() -> list[CompanyInput]:
    return [CompanyInput.model_validate(item) for item in st.session_state.get("upload_rows", [])]


def _load_upload(file_name: str, raw: bytes, *, file_signature: str, sheet_name: str | None) -> None:
    frame, validation_data, completed_records = _parse_upload(file_name, raw, sheet_name)
    validation_rows = validation_data.pop("rows")
    reset_run_state(keep_upload=False)
    st.session_state["uploaded_filename"] = file_name
    st.session_state["uploaded_file_signature"] = file_signature
    st.session_state["uploaded_sheet_name"] = sheet_name
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
        append_log(f"Archivo cargado: {file_name}")


def _enqueue_event(event_queue: queue.Queue[dict[str, Any]], event_type: str, **payload: Any) -> None:
    event_queue.put({"type": event_type, **payload})


def _process_single_row(
    row_data: dict[str, Any],
    settings_data: dict[str, Any],
    api_key: str | None,
    start_position: int,
    total_rows: int,
    relative_index: int,
    event_queue: queue.Queue[dict[str, Any]],
) -> dict[str, Any]:
    from company_verifier.services.verification_orchestrator import VerificationOrchestrator

    settings = AppSettings.model_validate(settings_data)
    current_row = CompanyInput.model_validate(row_data)
    orchestrator = VerificationOrchestrator(api_key)
    estimator = CostEstimatorService()

    current_position = start_position + relative_index
    batch_number = ((current_position - 1) // settings.batch_size) + 1
    batch_offset = ((current_position - 1) % settings.batch_size) + 1
    _enqueue_event(
        event_queue,
        "log",
        message=(
            f"Procesando empresa {current_position}/{total_rows} · "
            f"batch {batch_number} · elemento {batch_offset}/{settings.batch_size}: "
            f"{current_row.nombre_empresa}"
        ),
    )

    started_at = time.perf_counter()
    cost_estimate = estimator.estimate([current_row], settings)
    result = orchestrator.process_company(
        current_row,
        settings=settings,
        log_callback=lambda message: _enqueue_event(event_queue, "log", message=message),
    )
    duration_seconds = time.perf_counter() - started_at
    return {
        "row_hash": current_row.record_hash,
        "company_name": current_row.nombre_empresa,
        "result": result.model_dump(mode="json"),
        "estimated_cost_usd": cost_estimate.estimated_cost_usd,
        "duration_seconds": duration_seconds,
    }


def _process_rows_in_background(
    rows_data: list[dict[str, Any]],
    settings_data: dict[str, Any],
    api_key: str | None,
    start_position: int,
    event_queue: queue.Queue[dict[str, Any]],
    stop_event: threading.Event,
) -> None:
    settings = AppSettings.model_validate(settings_data)
    total_rows = start_position + len(rows_data)
    max_workers = min(settings.parallel_workers, len(rows_data))
    row_iterator = iter(enumerate(rows_data, start=1))
    futures: dict[Future[dict[str, Any]], tuple[int, dict[str, Any]]] = {}

    def _submit_next(executor: ThreadPoolExecutor) -> bool:
        if stop_event.is_set():
            return False
        try:
            relative_index, row_data = next(row_iterator)
        except StopIteration:
            return False
        future = executor.submit(
            _process_single_row,
            row_data,
            settings_data,
            api_key,
            start_position,
            total_rows,
            relative_index,
            event_queue,
        )
        futures[future] = (relative_index, row_data)
        return True

    with ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="verification-row") as executor:
        for _ in range(max_workers):
            if not _submit_next(executor):
                break

        while futures:
            done, _ = wait(futures.keys(), timeout=0.2, return_when=FIRST_COMPLETED)
            if not done:
                if stop_event.is_set():
                    for future in futures:
                        future.cancel()
                continue

            for future in done:
                _, row_data = futures.pop(future)
                try:
                    payload = future.result()
                except Exception as exc:  # noqa: BLE001
                    stop_event.set()
                    for pending_future in futures:
                        pending_future.cancel()
                    current_row = CompanyInput.model_validate(row_data)
                    _enqueue_event(
                        event_queue,
                        "failed",
                        company_name=current_row.nombre_empresa,
                        error=str(exc),
                    )
                    return

                _enqueue_event(event_queue, "company_done", **payload)

                if stop_event.is_set():
                    continue

                while len(futures) < max_workers and _submit_next(executor):
                    pass

    if stop_event.is_set():
        _enqueue_event(event_queue, "stopped")
        return

    _enqueue_event(event_queue, "completed")



def _eta_text() -> str:
    metrics = get_metrics()
    settings = get_settings()
    if not metrics.started_at or metrics.processed_rows == 0 or metrics.total_rows == 0:
        return "n/d"
    remaining = max(0, metrics.total_rows - metrics.processed_rows)
    if remaining == 0:
        return "00:00:00"
    accumulated_seconds = metrics.accumulated_row_seconds
    if accumulated_seconds <= 0:
        accumulated_seconds = max(1.0, time.time() - metrics.started_at)
    avg_row_seconds = accumulated_seconds / metrics.processed_rows
    effective_parallelism = max(1, min(settings.parallel_workers, remaining, metrics.total_rows))
    eta_seconds = int((remaining * avg_row_seconds) / effective_parallelism)
    minutes, seconds = divmod(eta_seconds, 60)
    hours, minutes = divmod(minutes, 60)
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}"


def _elapsed_text() -> str:
    metrics = get_metrics()
    if not metrics.started_at:
        return "00:00:00"
    end_time = metrics.finished_at or time.time()
    elapsed_seconds = max(0, int(end_time - metrics.started_at))
    minutes, seconds = divmod(elapsed_seconds, 60)
    hours, minutes = divmod(minutes, 60)
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}"


def _duration_text(total_seconds: float) -> str:
    rounded_seconds = max(0, int(total_seconds))
    minutes, seconds = divmod(rounded_seconds, 60)
    hours, minutes = divmod(minutes, 60)
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}"


def _avg_row_time_text() -> str:
    metrics = get_metrics()
    if metrics.processed_rows == 0:
        return "00:00:00"
    average_seconds = metrics.accumulated_row_seconds / metrics.processed_rows
    return _duration_text(average_seconds)


st.subheader("Carga del CSV y control de ejecución")
with st.container(border=True):
    uploaded_file = st.file_uploader(
        "Archivo de entrada o checkpoint",
        type=["csv", "xlsx", "xls"],
        disabled=worker_is_running(),
    )
    if uploaded_file is not None:
        raw = uploaded_file.getvalue()
        file_signature = hashlib.sha256(raw).hexdigest()
        sheet_names = _sheet_names(uploaded_file.name, raw)
        st.session_state["uploaded_sheet_names"] = sheet_names
        selected_sheet = sheet_names[0] if len(sheet_names) == 1 else st.session_state.get("uploaded_sheet_name")
        if len(sheet_names) > 1:
            selected_sheet = st.selectbox(
                "Hoja",
                options=sheet_names,
                index=(sheet_names.index(selected_sheet) if selected_sheet in sheet_names else 0),
            )
        should_reload = (
            file_signature != st.session_state.get("uploaded_file_signature")
            or selected_sheet != st.session_state.get("uploaded_sheet_name")
        )
        if should_reload:
            _load_upload(uploaded_file.name, raw, file_signature=file_signature, sheet_name=selected_sheet)
    else:
        st.session_state["uploaded_sheet_names"] = []

drain_worker_events()


def _start_run() -> None:
    if worker_is_running():
        return

    pending = pending_rows()
    if not pending:
        st.warning("No quedan empresas pendientes.")
        return

    metrics = get_metrics()
    if not metrics.started_at:
        started_at = time.time()
    else:
        started_at = metrics.started_at

    update_metrics(started_at=started_at, finished_at=None, total_rows=len(st.session_state["upload_rows"]))
    st.session_state["run_status"] = "running"
    st.session_state["stop_requested"] = False
    append_log("Ejecución iniciada.")

    event_queue: queue.Queue[dict[str, Any]] = queue.Queue()
    stop_event = threading.Event()
    worker = threading.Thread(
        target=_process_rows_in_background,
        kwargs={
            "rows_data": [row.model_dump(mode="json") for row in pending],
            "settings_data": get_settings().model_dump(mode="json"),
            "api_key": _get_api_key(),
            "start_position": metrics.processed_rows,
            "event_queue": event_queue,
            "stop_event": stop_event,
        },
        daemon=True,
        name="verification-worker",
    )
    st.session_state["batch_event_queue"] = event_queue
    st.session_state["batch_stop_event"] = stop_event
    st.session_state["batch_worker"] = worker
    worker.start()


def _request_stop() -> None:
    if not worker_is_running():
        return

    st.session_state["stop_requested"] = True
    stop_event = get_stop_event()
    if stop_event is not None:
        stop_event.set()
    append_log("Se detendrá cuando terminen las empresas ya en curso.")


@st.fragment(run_every="1s")
def _render_live_panel() -> None:
    drain_worker_events()

    rows = _current_rows()
    results = current_results()
    metrics = get_metrics()
    settings = get_settings()
    pending = pending_rows() if rows else []

    col1, col2, col3, col4, col5, col6 = st.columns(6)
    col1.metric("Filas válidas", len(rows))
    col2.metric("Resultados generados", len(results))
    col3.metric("Pendientes", len(pending))
    col4.metric("ETA", _eta_text())
    col5.metric("Transcurrido", _elapsed_text())
    col6.metric("Promedio/empresa", _avg_row_time_text())

    progress_total = metrics.total_rows or len(rows) or 1
    st.progress(
        min(metrics.processed_rows / progress_total, 1.0),
        text=f"{metrics.processed_rows}/{progress_total} empresas procesadas",
    )

    if rows:
        estimate = _cost_service.estimate(rows[: settings.batch_size], settings)
        st.caption(
            f"Estimación rápida por batch: ~{estimate.estimated_total_tokens} tokens y ${estimate.estimated_cost_usd:.4f} con {settings.model}. Paralelismo actual: {settings.parallel_workers}."
        )

    control_cols = st.columns([1, 1, 1, 2])
    if control_cols[0].button("Iniciar / reanudar", type="primary", disabled=not pending or worker_is_running()):
        _start_run()
    if control_cols[1].button("Detener", disabled=not worker_is_running()):
        _request_stop()
    if control_cols[2].button("Reset sesión", disabled=worker_is_running() or (not rows and not results)):
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
        st.info("Sube un CSV o Excel con columnas nombre_empresa y web para empezar.")
    elif worker_is_running():
        st.info("La ejecución corre en segundo plano. Esta vista se actualiza automáticamente para mostrar progreso, logs y checkpoints en tiempo real.")

_render_live_panel()
