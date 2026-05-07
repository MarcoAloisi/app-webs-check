from __future__ import annotations

import streamlit as st

from company_verifier.models import CompanyVerificationResult, LegitimacyAnswer, RiskLevel
from company_verifier.run_controller import current_results, drain_worker_events, worker_is_running
from company_verifier.session import get_results_view_source


def _is_suspicious(result: CompanyVerificationResult) -> bool:
    return (
        result.requiere_revision_manual
        or result.legitima != LegitimacyAnswer.YES
        or result.riesgo_fraude == RiskLevel.HIGH
    )


def _legitimacy_badge(result: CompanyVerificationResult) -> str:
    labels = [result.legitima.value]
    if result.absorbida_adquirida.value == "si":
        labels.append("adquirida")
    if result.rebranded.value == "si":
        labels.append("rebranded")
    return " · ".join(labels)


def _render_audit_page() -> None:
    st.subheader("Auditoría por empresa")
    source, serialized_results, source_message = get_results_view_source()
    if source == "external" and serialized_results:
        results = [CompanyVerificationResult.model_validate_json(item) for item in serialized_results]
        if source_message:
            st.caption(source_message)
    else:
        results = current_results()
    if not results:
        st.info("Todavía no hay resultados auditables.")
        return

    status_options = ["todos", "si", "no", "indeterminado"]
    risk_options = ["todos", "bajo", "medio", "alto"]
    review_options = ["todos", "sí", "no"]

    col1, col2, col3, col4 = st.columns(4)
    exists_filter = col1.selectbox("Existe", status_options, key="audit_exists_filter")
    operative_filter = col2.selectbox("Operativa", status_options, key="audit_operative_filter")
    risk_filter = col3.selectbox("Riesgo", risk_options, key="audit_risk_filter")
    review_filter = col4.selectbox("Revisión manual", review_options, key="audit_review_filter")

    scores = [item.score_confianza for item in results]
    score_range = st.slider(
        "Rango de score",
        min_value=min(scores),
        max_value=max(scores),
        value=(min(scores), max(scores)),
        key="audit_score_range",
    )
    only_suspicious = st.checkbox("Solo sospechosas", value=False, key="audit_only_suspicious")

    filtered_results = list(results)
    if exists_filter != "todos":
        filtered_results = [item for item in filtered_results if item.existe.value == exists_filter]
    if operative_filter != "todos":
        filtered_results = [item for item in filtered_results if item.operativa.value == operative_filter]
    if risk_filter != "todos":
        filtered_results = [item for item in filtered_results if item.riesgo_fraude.value == risk_filter]
    if review_filter != "todos":
        expected_review = review_filter == "sí"
        filtered_results = [item for item in filtered_results if item.requiere_revision_manual == expected_review]
    filtered_results = [item for item in filtered_results if score_range[0] <= item.score_confianza <= score_range[1]]
    if only_suspicious:
        filtered_results = [item for item in filtered_results if _is_suspicious(item)]

    st.caption(f"Empresas visibles: {len(filtered_results)} de {len(results)}")
    if not filtered_results:
        st.warning("No hay empresas que cumplan los filtros seleccionados.")
        return

    options = {f"{item.nombre_empresa} · {item.web_input}": item for item in filtered_results}
    selected_label = st.selectbox("Empresa", options=list(options.keys()))
    selected = options[selected_label]

    cols = st.columns(6)
    cols[0].metric("Existe", selected.existe.value)
    cols[1].metric("Operativa", selected.operativa.value)
    cols[2].metric("Absorbida/adq.", selected.absorbida_adquirida.value)
    cols[3].metric("Rebranded", selected.rebranded.value)
    cols[4].metric("Legítima", selected.legitima.value)
    cols[5].metric("Score", selected.score_confianza)
    st.caption(f"Estado visual: {_legitimacy_badge(selected)}")

    st.markdown("### Trazabilidad")
    st.write(selected.justificacion_detallada)
    st.json(
        {
            "fuentes": selected.fuentes,
            "banderas_rojas": selected.banderas_rojas,
            "banderas_verdes": selected.banderas_verdes,
            "absorbida_adquirida": selected.absorbida_adquirida.value,
            "rebranded": selected.rebranded.value,
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
