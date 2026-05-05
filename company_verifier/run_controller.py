from __future__ import annotations

import queue
import threading
import time
from typing import Any

import pandas as pd
import streamlit as st

from company_verifier.models import AppSettings, CompanyInput, CompanyVerificationResult
from company_verifier.services.export_service import ExportService
from company_verifier.session import append_log, get_metrics, get_settings, update_metrics
from company_verifier.storage.checkpoint_store import CheckpointStore

_export_service = ExportService()
_checkpoint_store = CheckpointStore()


def current_rows() -> list[CompanyInput]:
    return [CompanyInput.model_validate(item) for item in st.session_state.get("upload_rows", [])]


def current_results() -> list[CompanyVerificationResult]:
    return [CompanyVerificationResult.model_validate(item) for item in st.session_state.get("results", [])]


def pending_rows() -> list[CompanyInput]:
    done_hashes = set(st.session_state.get("results_by_hash", {}).keys())
    return [row for row in current_rows() if row.record_hash not in done_hashes]


def refresh_checkpoint() -> None:
    results = current_results()
    metrics = get_metrics()
    settings = get_settings()
    source_frame = st.session_state.get("source_dataframe", pd.DataFrame())
    st.session_state["latest_checkpoint_json"] = _checkpoint_store.build_payload(source_frame, results, settings, metrics)
    st.session_state["latest_checkpoint_csv"] = _checkpoint_store.build_checkpoint_csv(source_frame, results)
    st.session_state["checkpoint_ready"] = True
    append_log("Checkpoint actualizado en memoria y listo para descarga.")


def get_event_queue() -> queue.Queue[dict[str, Any]]:
    event_queue = st.session_state.get("batch_event_queue")
    if event_queue is None:
        event_queue = queue.Queue()
        st.session_state["batch_event_queue"] = event_queue
    return event_queue


def get_stop_event() -> threading.Event | None:
    stop_event = st.session_state.get("batch_stop_event")
    if isinstance(stop_event, threading.Event):
        return stop_event
    return None


def worker_is_running() -> bool:
    worker = st.session_state.get("batch_worker")
    return isinstance(worker, threading.Thread) and worker.is_alive()


def _apply_company_result(event: dict[str, Any]) -> None:
    row_hash = str(event["row_hash"])
    results_by_hash = dict(st.session_state.get("results_by_hash", {}))
    if row_hash in results_by_hash:
        return

    serialized = dict(event["result"])
    result_list = list(st.session_state.get("results", []))
    results_by_hash[row_hash] = serialized
    result_list.append(serialized)
    st.session_state["last_processed_hash"] = row_hash
    st.session_state["results_by_hash"] = results_by_hash
    st.session_state["results"] = result_list

    metrics = get_metrics()
    settings = get_settings()
    processed_rows = metrics.processed_rows + 1
    update_metrics(
        processed_rows=processed_rows,
        completed_rows=len(result_list),
        batches_completed=processed_rows // settings.batch_size,
        estimated_cost_usd=round(metrics.estimated_cost_usd + float(event["estimated_cost_usd"]), 4),
    )
    append_log(f"Empresa completada: {event['company_name']}")

    if processed_rows % settings.checkpoint_interval == 0 or not pending_rows():
        refresh_checkpoint()


def _finalize_run(status: str, message: str) -> None:
    if st.session_state.get("run_status") != status:
        st.session_state["run_status"] = status
    update_metrics(finished_at=time.time())
    refresh_checkpoint()
    append_log(message)


def drain_worker_events() -> None:
    event_queue = get_event_queue()
    while True:
        try:
            event = event_queue.get_nowait()
        except queue.Empty:
            break

        event_type = event["type"]
        if event_type == "log":
            append_log(str(event["message"]))
        elif event_type == "company_done":
            _apply_company_result(event)
        elif event_type == "completed":
            _finalize_run("completed", "Ejecución finalizada.")
        elif event_type == "stopped":
            _finalize_run("stopped", "Ejecución detenida por el usuario.")
        elif event_type == "failed":
            _finalize_run(
                "failed",
                f"Error durante el batch ({event['company_name']}): {event['error']}",
            )

    worker = st.session_state.get("batch_worker")
    if isinstance(worker, threading.Thread) and not worker.is_alive():
        if st.session_state.get("run_status") == "running":
            _finalize_run("failed", "La ejecución terminó de forma inesperada.")
        if st.session_state.get("run_status") != "running":
            st.session_state["batch_worker"] = None
            st.session_state["batch_stop_event"] = None
