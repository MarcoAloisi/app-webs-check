from __future__ import annotations

import streamlit as st

from company_verifier.config import APP_TITLE
from company_verifier.run_controller import drain_worker_events
from company_verifier.session import get_metrics, init_session_state

st.set_page_config(page_title=APP_TITLE, page_icon=":material/fact_check:", layout="wide")
init_session_state()

pages = st.navigation(
    {
        "Operación": [
            st.Page("app_pages/upload_run.py", title="Carga y ejecución", icon=":material/upload_file:"),
            st.Page("app_pages/results.py", title="Resultados", icon=":material/table_view:"),
            st.Page("app_pages/audit.py", title="Auditoría", icon=":material/manage_search:"),
        ],
        "Configuración": [
            st.Page("app_pages/settings.py", title="Modelos y parámetros", icon=":material/tune:"),
        ],
    },
    position="top",
)

@st.fragment(run_every="1s")
def _render_run_status() -> None:
    drain_worker_events()
    metrics = get_metrics()
    status = st.session_state.get("run_status", "idle")
    st.caption(
        f"Estado: {status} · Procesadas {metrics.processed_rows}/{metrics.total_rows or 0} empresas · "
        f"Batches completados: {metrics.batches_completed} · Coste estimado acumulado: ${metrics.estimated_cost_usd:.4f}"
    )

current_page = pages
drain_worker_events()

st.title(f"{current_page.icon} {APP_TITLE}")
_render_run_status()

pages.run()
