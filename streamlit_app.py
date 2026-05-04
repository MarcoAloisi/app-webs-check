from __future__ import annotations

import streamlit as st

from company_verifier.config import APP_TITLE
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

current_page = pages
metrics = get_metrics()

st.title(f"{current_page.icon} {APP_TITLE}")
caption = (
    f"Procesadas {metrics.processed_rows}/{metrics.total_rows or 0} empresas · "
    f"Batches completados: {metrics.batches_completed} · "
    f"Coste estimado acumulado: ${metrics.estimated_cost_usd:.4f}"
)
st.caption(caption)

pages.run()
