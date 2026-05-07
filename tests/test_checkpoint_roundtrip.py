from __future__ import annotations

import pandas as pd

from company_verifier.models import AppSettings, CompanyVerificationResult, VerificationRunMetrics
from company_verifier.services.csv_validation import extract_completed_results
from company_verifier.storage.checkpoint_store import CheckpointStore



def test_checkpoint_store_builds_csv_and_json() -> None:
    store = CheckpointStore()
    frame = pd.DataFrame(
        [
            {
                "nombre_empresa": "Acme Corp",
                "web": "https://acme.example",
                "web_normalized": "https://acme.example",
                "domain_normalized": "acme.example",
                "row_number": 2,
                "record_hash": "abc123456789def0",
                "processing_status": "pending",
            }
        ]
    )
    result = CompanyVerificationResult(
        nombre_empresa="Acme Corp",
        web_input="https://acme.example",
        web_verificada="https://acme.example",
        existe="si",
        operativa="si",
        absorbida_adquirida="no",
        rebranded="no",
        legitima="si",
        riesgo_fraude="bajo",
        tipologia_riesgo=[],
        score_confianza=88,
        pasos_verificados=[
            {
                "step_number": idx,
                "name": f"Paso {idx}",
                "status": "completed",
                "finding": "ok",
                "evidence": [],
                "sources": [],
            }
            for idx in range(1, 8)
        ],
        justificacion_detallada="Justificación suficientemente larga para la validación del modelo.",
        fuentes=["https://acme.example"],
        banderas_rojas=[],
        banderas_verdes=["SSL válido"],
        requiere_revision_manual=False,
    )

    csv_payload = store.build_checkpoint_csv(frame, [result])
    json_payload = store.build_payload(frame, [result], AppSettings(model="openai/gpt-4o-mini"), VerificationRunMetrics(total_rows=1, processed_rows=1))

    assert "Acme Corp" in csv_payload
    assert "openai/gpt-4o-mini" in json_payload
    assert "absorbida_adquirida" in json_payload


def test_extract_completed_results_accepts_exported_results_without_record_hash_or_web() -> None:
    frame = pd.DataFrame(
        [
            {
                "nombre_empresa": "Acme Corp",
                "web_input": "https://acme.example",
                "web_verificada": "https://acme.example",
                "existe": "si",
                "operativa": "si",
                "absorbida_adquirida": "no",
                "rebranded": "no",
                "legitima": "si",
                "riesgo_fraude": "bajo",
                "tipologia_riesgo": "[]",
                "score_confianza": "88",
                "pasos_verificados": "[]",
                "justificacion_detallada": "Justificación suficientemente larga para la validación del modelo.",
                "fuentes": "[\"https://acme.example\"]",
                "banderas_rojas": "[]",
                "banderas_verdes": "[\"SSL válido\"]",
                "requiere_revision_manual": False,
                "prompt_enviado": "",
                "respuesta_llm_cruda": "",
                "processing_status": "completed",
            }
        ]
    )

    records = extract_completed_results(frame)

    assert len(records) == 1
    assert records[0]["nombre_empresa"] == "Acme Corp"
    assert records[0]["web_input"] == "https://acme.example"
    assert records[0]["absorbida_adquirida"] == "no"


def test_score_confianza_accepts_float_like_strings() -> None:
    result = CompanyVerificationResult(
        nombre_empresa="Acme Corp",
        web_input="https://acme.example",
        web_verificada="https://acme.example",
        existe="si",
        operativa="si",
        absorbida_adquirida="no",
        rebranded="no",
        legitima="si",
        riesgo_fraude="bajo",
        tipologia_riesgo=[],
        score_confianza="78.0",
        pasos_verificados=[
            {
                "step_number": idx,
                "name": f"Paso {idx}",
                "status": "completed",
                "finding": "ok",
                "evidence": [],
                "sources": [],
            }
            for idx in range(1, 8)
        ],
        justificacion_detallada="Justificación suficientemente larga para la validación del modelo.",
        fuentes=["https://acme.example"],
        banderas_rojas=[],
        banderas_verdes=["SSL válido"],
        requiere_revision_manual=False,
    )

    assert result.score_confianza == 78
