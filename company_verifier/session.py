"""Session-state helpers for Streamlit pages."""

from __future__ import annotations

import queue
import time
from typing import Any

import pandas as pd
import streamlit as st

from company_verifier.config import (
    CHECKPOINT_INTERVAL,
    DEFAULT_BATCH_SIZE,
    DEFAULT_ENABLE_WEB_SEARCH,
    DEFAULT_FALLBACK_MODEL,
    DEFAULT_PARALLEL_WORKERS,
    DEFAULT_MANUAL_REVIEW_THRESHOLD,
    DEFAULT_MAX_TOKENS,
    DEFAULT_MODEL,
    DEFAULT_TEMPERATURE,
)
from company_verifier.models import AppSettings, VerificationRunMetrics


DEFAULT_SETTINGS = AppSettings(
    model=DEFAULT_MODEL,
    fallback_model=DEFAULT_FALLBACK_MODEL,
    temperature=DEFAULT_TEMPERATURE,
    max_tokens=DEFAULT_MAX_TOKENS,
    batch_size=DEFAULT_BATCH_SIZE,
    parallel_workers=DEFAULT_PARALLEL_WORKERS,
    manual_review_threshold=DEFAULT_MANUAL_REVIEW_THRESHOLD,
    enable_web_search=DEFAULT_ENABLE_WEB_SEARCH,
    checkpoint_interval=CHECKPOINT_INTERVAL,
)


def init_session_state() -> None:
    """Initialize keys required by the application."""
    st.session_state.setdefault("settings", DEFAULT_SETTINGS.model_dump())
    st.session_state.setdefault("upload_rows", [])
    st.session_state.setdefault("upload_issues", [])
    st.session_state.setdefault("source_dataframe", pd.DataFrame())
    st.session_state.setdefault("results", [])
    st.session_state.setdefault("results_by_hash", {})
    st.session_state.setdefault("logs", [])
    st.session_state.setdefault("run_metrics", VerificationRunMetrics().model_dump())
    st.session_state.setdefault("run_status", "idle")
    st.session_state.setdefault("current_batch_index", 0)
    st.session_state.setdefault("stop_requested", False)
    st.session_state.setdefault("latest_checkpoint_json", "")
    st.session_state.setdefault("latest_checkpoint_csv", "")
    st.session_state.setdefault("last_processed_hash", None)
    st.session_state.setdefault("validation_summary", None)
    st.session_state.setdefault("checkpoint_ready", False)
    st.session_state.setdefault("uploaded_filename", None)
    st.session_state.setdefault("uploaded_file_signature", None)
    st.session_state.setdefault("uploaded_sheet_name", None)
    st.session_state.setdefault("uploaded_sheet_names", [])
    st.session_state.setdefault("batch_worker", None)
    st.session_state.setdefault("batch_stop_event", None)
    st.session_state.setdefault("batch_event_queue", queue.Queue())
    st.session_state.setdefault("results_view_source", "session")
    st.session_state.setdefault("results_view_serialized", ())
    st.session_state.setdefault("results_view_message", None)


def get_settings() -> AppSettings:
    return AppSettings.model_validate(st.session_state["settings"])



def update_settings(settings: AppSettings) -> None:
    st.session_state["settings"] = settings.model_dump()



def append_log(message: str) -> None:
    logs = list(st.session_state.get("logs", []))
    timestamp = time.strftime("%H:%M:%S")
    logs.append(f"[{timestamp}] {message}")
    st.session_state["logs"] = logs[-400:]



def reset_run_state(keep_upload: bool = True) -> None:
    existing_rows = st.session_state.get("upload_rows", []) if keep_upload else []
    issues = st.session_state.get("upload_issues", []) if keep_upload else []
    source_df = st.session_state.get("source_dataframe", pd.DataFrame()) if keep_upload else pd.DataFrame()
    uploaded_filename = st.session_state.get("uploaded_filename") if keep_upload else None
    uploaded_file_signature = st.session_state.get("uploaded_file_signature") if keep_upload else None
    uploaded_sheet_name = st.session_state.get("uploaded_sheet_name") if keep_upload else None
    uploaded_sheet_names = st.session_state.get("uploaded_sheet_names", []) if keep_upload else []
    st.session_state["upload_rows"] = existing_rows
    st.session_state["upload_issues"] = issues
    st.session_state["source_dataframe"] = source_df
    st.session_state["uploaded_filename"] = uploaded_filename
    st.session_state["uploaded_file_signature"] = uploaded_file_signature
    st.session_state["uploaded_sheet_name"] = uploaded_sheet_name
    st.session_state["uploaded_sheet_names"] = uploaded_sheet_names
    st.session_state["results"] = []
    st.session_state["results_by_hash"] = {}
    st.session_state["logs"] = []
    st.session_state["run_metrics"] = VerificationRunMetrics().model_dump()
    st.session_state["run_status"] = "idle"
    st.session_state["current_batch_index"] = 0
    st.session_state["stop_requested"] = False
    st.session_state["latest_checkpoint_json"] = ""
    st.session_state["latest_checkpoint_csv"] = ""
    st.session_state["checkpoint_ready"] = False
    st.session_state["last_processed_hash"] = None
    st.session_state["batch_worker"] = None
    st.session_state["batch_stop_event"] = None
    st.session_state["batch_event_queue"] = queue.Queue()



def update_metrics(**changes: Any) -> None:
    metrics = VerificationRunMetrics.model_validate(st.session_state["run_metrics"])
    data = metrics.model_dump()
    data.update(changes)
    st.session_state["run_metrics"] = VerificationRunMetrics.model_validate(data).model_dump()



def get_metrics() -> VerificationRunMetrics:
    return VerificationRunMetrics.model_validate(st.session_state["run_metrics"])


def set_results_view_source(
    source: str,
    *,
    serialized_results: tuple[str, ...] = (),
    message: str | None = None,
) -> None:
    st.session_state["results_view_source"] = source
    st.session_state["results_view_serialized"] = serialized_results
    st.session_state["results_view_message"] = message


def get_results_view_source() -> tuple[str, tuple[str, ...], str | None]:
    return (
        str(st.session_state.get("results_view_source", "session")),
        tuple(st.session_state.get("results_view_serialized", ())),
        st.session_state.get("results_view_message"),
    )
