from __future__ import annotations

import streamlit as st

from company_verifier.models import CompanyVerificationResult
from company_verifier.run_controller import current_results, drain_worker_events, worker_is_running


def _render_audit_page() -> None:
    st.subheader("Auditoría por empresa")
    results = current_results()
    if not results:
        st.info("Todavía no hay resultados auditables.")
        return

    options = {f"{item.nombre_empresa} · {item.web_input}": item for item in results}
    selected_label = st.selectbox("Empresa", options=list(options.keys()))
    selected = options[selected_label]

    cols = st.columns(4)
    cols[0].metric("Existe", selected.existe.value)
    cols[1].metric("Operativa", selected.operativa.value)
    cols[2].metric("Legítima", selected.legitima.value)
    cols[3].metric("Score", selected.score_confianza)

    st.markdown("### Trazabilidad")
    st.write(selected.justificacion_detallada)
    st.json(
        {
            "fuentes": selected.fuentes,
            "banderas_rojas": selected.banderas_rojas,
            "banderas_verdes": selected.banderas_verdes,
            "requiere_revision_manual": selected.requiere_revision_manual,
        }
    )

    st.markdown("### Pasos verificados")
    for step in selected.pasos_verificados:
        with st.expander(f"Paso {step.step_number}: {step.name} · {step.status.value}", expanded=step.step_number <= 2):
            st.write(step.finding)
            st.json({"evidence": step.evidence, "sources": step.sources})

    st.markdown("### Prompt enviado")
    st.code(selected.prompt_enviado or "No se envió prompt; se usó fallback conservador.", language="json")

    st.markdown("### Respuesta cruda del LLM")
    st.code(selected.respuesta_llm_cruda or "Sin respuesta cruda registrada.", language="json")


if worker_is_running():
    @st.fragment(run_every="1s")
    def _render_live_audit() -> None:
        drain_worker_events()
        _render_audit_page()

    _render_live_audit()
else:
    drain_worker_events()
    _render_audit_page()
