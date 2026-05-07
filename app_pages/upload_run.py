from __future__ import annotations

from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
import hashlib
import json
from pathlib import Path
import queue
import threading
import time
from typing import Any

import pandas as pd
import streamlit as st

from company_verifier.models import AppSettings, CompanyInput, CompanyVerificationResult, UploadValidationResult
from company_verifier.run_controller import current_results, drain_worker_events, get_stop_event, pending_rows, refresh_checkpoint, worker_is_running
from company_verifier.services.cost_estimator import CostEstimatorService
from company_verifier.services.csv_validation import extract_completed_results, list_sheet_names, load_tabular_bytes
from company_verifier.services.export_service import ExportService
from company_verifier.session import append_log, get_metrics, get_settings, reset_run_state, update_metrics
from company_verifier.storage.checkpoint_store import CheckpointStore
from company_verifier.utils.web import build_record_hash, extract_domain, normalize_url

_export_service = ExportService()
_cost_service = CostEstimatorService()
_checkpoint_store = CheckpointStore()


@st.cache_data(show_spinner=False)
def _sheet_names(file_name: str, raw_bytes: bytes) -> list[str]:
    return list_sheet_names(raw_bytes, file_name)


@st.cache_data(show_spinner=False)
def _parse_upload(file_name: str, raw_bytes: bytes, sheet_name: str | None) -> tuple[pd.DataFrame, dict, list[dict[str, str]]]:
    suffix = Path(file_name).suffix.lower()
    if suffix in {".csv", ".xlsx", ".xls"}:
        frame, validation = load_tabular_bytes(raw_bytes, file_name, sheet_name=sheet_name)
        completed = extract_completed_results(frame)
        return frame, validation.model_dump(mode="json"), completed
    if suffix == ".json":
        return _parse_json_upload(raw_bytes)
    if suffix == ".jsonl":
        return _parse_jsonl_upload(raw_bytes)
    raise ValueError("Formato no soportado. Sube un CSV, XLSX, XLS, JSON o JSONL.")


def _result_to_row_payload(result: CompanyVerificationResult, row_number: int) -> dict[str, Any]:
    normalized_url = normalize_url(result.web_input) if result.web_input else "https://unknown.invalid"
    record_hash = build_record_hash(result.nombre_empresa, normalized_url)
    return {
        "row_number": row_number,
        "nombre_empresa": result.nombre_empresa,
        "web": result.web_input,
        "web_normalized": normalized_url,
        "domain_normalized": extract_domain(normalized_url),
        "record_hash": record_hash,
        "processing_status": result.processing_status.value,
    }


def _results_to_upload_artifacts(results: list[CompanyVerificationResult]) -> tuple[pd.DataFrame, dict, list[dict[str, Any]]]:
    row_payloads = [_result_to_row_payload(result, row_number=index + 2) for index, result in enumerate(results)]
    frame = pd.DataFrame(row_payloads)
    completed_records = []
    for row_payload, result in zip(row_payloads, results, strict=False):
        completed_records.append({**row_payload, **result.model_dump(mode="json")})
    validation = UploadValidationResult(
        rows=[CompanyInput.model_validate(row_payload) for row_payload in row_payloads],
        issues=[],
        duplicates_removed=0,
        encoding_used="utf-8",
        is_checkpoint_file=True,
    )
    return frame, validation.model_dump(mode="json"), completed_records


def _parse_json_upload(raw_bytes: bytes) -> tuple[pd.DataFrame, dict, list[dict[str, Any]]]:
    payload = json.loads(raw_bytes.decode("utf-8-sig"))
    if isinstance(payload, dict) and isinstance(payload.get("rows"), list) and isinstance(payload.get("results"), list):
        checkpoint = _checkpoint_store.load_payload(raw_bytes.decode("utf-8-sig"))
        frame = pd.DataFrame(checkpoint.rows)
        validation = UploadValidationResult(
            rows=[CompanyInput.model_validate(row) for row in checkpoint.rows],
            issues=[],
            duplicates_removed=0,
            encoding_used="utf-8",
            is_checkpoint_file=True,
        )
        completed_records = [dict(result) for result in checkpoint.results]
        return frame, validation.model_dump(mode="json"), completed_records
    results = _export_service.from_json_bytes(raw_bytes)
    return _results_to_upload_artifacts(results)


def _parse_jsonl_upload(raw_bytes: bytes) -> tuple[pd.DataFrame, dict, list[dict[str, Any]]]:
    results = _export_service.from_jsonl_bytes(raw_bytes)
    return _results_to_upload_artifacts(results)



def _get_api_key() -> str | None:
    try:
        return st.secrets.get("OPENROUTER_API_KEY", "") or None
    except Exception:  # noqa: BLE001
        return None

def _current_rows() -> list[CompanyInput]:
    return [CompanyInput.model_validate(item) for item in st.session_state.get("upload_rows", [])]


def _build_manual_company_input(company_name: str, website: str) -> CompanyInput:
    normalized_name = company_name.strip()
    normalized_input_url = normalize_url(website.strip()) if website.strip() else ""

    if not normalized_name and not normalized_input_url:
        raise ValueError("Debes indicar al menos el nombre de empresa o la web.")

    if not normalized_name:
        normalized_name = extract_domain(normalized_input_url) or normalized_input_url

    raw_web = website.strip() or "No proporcionada"
    if not normalized_input_url:
        normalized_input_url = "https://unknown.invalid"

    normalized_domain = extract_domain(normalized_input_url) or "unknown.invalid"
    return CompanyInput(
        row_number=2,
        nombre_empresa=normalized_name,
        web=raw_web,
        web_normalized=normalized_input_url,
        domain_normalized=normalized_domain,
        record_hash=build_record_hash(normalized_name, normalized_input_url),
    )


def _load_manual_input(company_name: str, website: str) -> None:
    row = _build_manual_company_input(company_name, website)
    reset_run_state(keep_upload=False)
    frame = pd.DataFrame(
        [
            {
                "row_number": row.row_number,
                "nombre_empresa": row.nombre_empresa,
                "web": row.web,
                "web_normalized": row.web_normalized,
                "domain_normalized": row.domain_normalized,
                "record_hash": row.record_hash,
                "processing_status": row.processing_status.value,
            }
        ]
    )
    st.session_state["uploaded_filename"] = "entrada_manual"
    st.session_state["uploaded_file_signature"] = None
    st.session_state["uploaded_sheet_name"] = None
    st.session_state["uploaded_sheet_names"] = []
    st.session_state["source_dataframe"] = frame
    st.session_state["upload_rows"] = [row.model_dump(mode="json")]
    st.session_state["validation_summary"] = {"issues": []}
    st.session_state["upload_issues"] = []
    update_metrics(
        total_rows=1,
        processed_rows=0,
        completed_rows=0,
        batches_completed=0,
        failed_rows=0,
        estimated_cost_usd=0.0,
        accumulated_row_seconds=0.0,
        started_at=None,
        finished_at=None,
    )
    append_log(f"Entrada manual preparada para {row.nombre_empresa}.")


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


st.subheader("Carga del CSV y control de ejecución")
with st.container(border=True):
    upload_tab, manual_tab = st.tabs(["Carga de archivo", "Empresa individual"])

    with upload_tab:
        uploaded_file = st.file_uploader(
            "Archivo de entrada o checkpoint",
            type=["csv", "xlsx", "xls", "json", "jsonl"],
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

    with manual_tab:
        with st.form("manual_company_form", border=False):
            company_name = st.text_input("Nombre de empresa", disabled=worker_is_running())
            website = st.text_input("Web", placeholder="https://empresa.com", disabled=worker_is_running())
            submitted = st.form_submit_button("Evaluar empresa", type="primary", disabled=worker_is_running())

        st.caption("Puedes informar nombre y web, solo nombre o solo web. Si falta uno de los dos, la app inferirá un valor mínimo para ejecutar la verificación.")

        if submitted:
            try:
                _load_manual_input(company_name, website)
            except ValueError as exc:
                st.warning(str(exc))
            else:
                st.session_state["manual_run_requested"] = True
                st.rerun()

drain_worker_events()

if st.session_state.pop("manual_run_requested", False):
    _start_run()
    st.rerun()


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
        st.dataframe(pd.DataFrame(issues), width="stretch", hide_index=True)

    if st.session_state.get("checkpoint_ready"):
        st.success("Hay un checkpoint actualizado listo para descarga.")
        download_cols = st.columns(3)
        download_cols[0].download_button(
            "Descargar checkpoint CSV",
            data=st.session_state.get("latest_checkpoint_csv", ""),
            file_name="checkpoint_resultados.csv",
            mime="text/csv",
            width="stretch",
        )
        download_cols[1].download_button(
            "Descargar checkpoint JSON",
            data=st.session_state.get("latest_checkpoint_json", ""),
            file_name="checkpoint_resultados.json",
            mime="application/json",
            width="stretch",
        )
        download_cols[2].download_button(
            "Descargar resultados JSONL",
            data=_export_service.jsonl_bytes(results),
            file_name="resultados.jsonl",
            mime="application/jsonl",
            width="stretch",
        )

    with st.expander("Log en vivo", expanded=True):
        logs = st.session_state.get("logs", [])
        st.code("\n".join(logs[-40:]) or "Sin eventos todavía.", language="text")

    if not rows:
        st.info("Sube un CSV o Excel con columnas nombre_empresa y web para empezar.")
    elif worker_is_running():
        st.info("La ejecución corre en segundo plano. Esta vista se actualiza automáticamente para mostrar progreso, logs y checkpoints en tiempo real.")

_render_live_panel()
