from __future__ import annotations

import pandas as pd

from company_verifier.models import AppSettings, CompanyVerificationResult, VerificationRunMetrics
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
