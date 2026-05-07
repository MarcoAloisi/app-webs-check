from __future__ import annotations

import pandas as pd

from app_pages.upload_run import _apply_result_statuses_to_frame, _attach_checkpoint_row_metadata


def test_attach_checkpoint_row_metadata_matches_results_by_normalized_url() -> None:
    rows = [
        {
            "nombre_empresa": "Original Name",
            "web": "http://www.example.com",
            "web_normalized": "http://example.com",
            "domain_normalized": "example.com",
            "row_number": 2,
            "record_hash": "hash-123",
            "processing_status": "pending",
        }
    ]
    results = [
        {
            "nombre_empresa": "Renamed Company",
            "web_input": "http://example.com/",
            "web_verificada": "http://example.com",
            "existe": "si",
            "operativa": "si",
            "absorbida_adquirida": "no",
            "rebranded": "si",
            "legitima": "si",
            "riesgo_fraude": "bajo",
            "tipologia_riesgo": [],
            "score_confianza": 80,
            "pasos_verificados": [
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
            "justificacion_detallada": "Justificación suficientemente larga para la validación del modelo.",
            "fuentes": ["http://example.com"],
            "banderas_rojas": [],
            "banderas_verdes": [],
            "requiere_revision_manual": False,
            "processing_status": "completed",
        }
    ]

    completed_records = _attach_checkpoint_row_metadata(rows, results)

    assert len(completed_records) == 1
    assert completed_records[0]["record_hash"] == "hash-123"
    assert completed_records[0]["web_normalized"] == "http://example.com"


def test_apply_result_statuses_to_frame_marks_completed_rows() -> None:
    frame = pd.DataFrame(
        [
            {
                "nombre_empresa": "Original Name",
                "web": "http://www.example.com",
                "web_normalized": "http://example.com",
                "domain_normalized": "example.com",
                "row_number": 2,
                "record_hash": "hash-123",
                "processing_status": "pending",
            }
        ]
    )
    completed_records = [{"record_hash": "hash-123"}]

    updated = _apply_result_statuses_to_frame(frame, completed_records)

    assert updated.loc[0, "processing_status"] == "completed"
