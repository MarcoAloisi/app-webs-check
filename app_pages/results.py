from __future__ import annotations

import json

import pandas as pd
import streamlit as st

from company_verifier.models import CompanyVerificationResult
from company_verifier.services.export_service import ExportService

_export_service = ExportService()


def _current_results() -> list[CompanyVerificationResult]:
    return [CompanyVerificationResult.model_validate(item) for item in st.session_state.get("results", [])]


st.subheader("Resultados y filtros")
results = _current_results()
if not results:
    st.info("Todavía no hay resultados para mostrar.")
    st.stop()

frame = _export_service.to_dataframe(results)
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
st.dataframe(filtered[visible_columns], use_container_width=True, hide_index=True)

csv_bytes = _export_service.to_csv_bytes(results)
excel_bytes = _export_service.to_excel_bytes(results)
export_cols = st.columns(2)
export_cols[0].download_button("Exportar CSV completo", data=csv_bytes, file_name="resultados_completos.csv", mime="text/csv", use_container_width=True)
export_cols[1].download_button("Exportar Excel completo", data=excel_bytes, file_name="resultados_completos.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", use_container_width=True)

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
