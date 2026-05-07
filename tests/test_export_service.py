from __future__ import annotations

import json

from company_verifier.models import CompanyVerificationResult
from company_verifier.services.export_service import ExportService


def _sample_result_payload() -> dict[str, object]:
    return {
        "nombre_empresa": "Acme Corp",
        "web_input": "https://acme.example",
        "web_verificada": "https://acme.example",
        "existe": "si",
        "operativa": "si",
        "absorbida_adquirida": "no",
        "rebranded": "no",
        "legitima": "si",
        "riesgo_fraude": "bajo",
        "tipologia_riesgo": [],
        "score_confianza": "78.0",
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
        "fuentes": ["https://acme.example"],
        "banderas_rojas": [],
        "banderas_verdes": ["SSL válido"],
        "requiere_revision_manual": False,
        "processing_status": "completed",
    }


def test_export_service_loads_checkpoint_json_results() -> None:
    service = ExportService()
    raw_bytes = json.dumps(
        {
            "created_at": "2026-05-07T13:14:00Z",
            "settings": {
                "model": "openai/gpt-oss-120b:free",
                "fallback_model": "minimax/minimax-m2.5:free",
                "temperature": 0.2,
                "max_tokens": 1800,
                "batch_size": 30,
                "parallel_workers": 10,
                "manual_review_threshold": 70,
                "enable_web_search": True,
                "web_search_engine": "auto",
                "web_search_max_results": 5,
                "web_search_max_total_results": None,
                "web_search_context_size": "medium",
                "web_search_allowed_domains": [],
                "web_search_excluded_domains": [],
                "checkpoint_interval": 500,
            },
            "metrics": {
                "processed_rows": 1,
                "total_rows": 1,
                "completed_rows": 1,
                "failed_rows": 0,
                "batches_completed": 0,
                "estimated_cost_usd": 0.0,
                "accumulated_row_seconds": 0.0,
            },
            "rows": [],
            "results": [_sample_result_payload()],
        },
        ensure_ascii=False,
    ).encode("utf-8")

    results = service.from_json_bytes(raw_bytes)

    assert len(results) == 1
    assert isinstance(results[0], CompanyVerificationResult)
    assert results[0].score_confianza == 78


def test_export_service_loads_jsonl_results() -> None:
    service = ExportService()
    raw_bytes = (json.dumps(_sample_result_payload(), ensure_ascii=False) + "\n").encode("utf-8")

    results = service.from_jsonl_bytes(raw_bytes)

    assert len(results) == 1
    assert results[0].nombre_empresa == "Acme Corp"
    assert results[0].score_confianza == 78


def test_export_service_limits_result_export_columns() -> None:
    service = ExportService()
    results = [CompanyVerificationResult.model_validate(_sample_result_payload())]

    frame = service.to_results_export_dataframe(results)

    assert list(frame.columns) == [
        "nombre_empresa",
        "web_input",
        "web_verificada",
        "existe",
        "operativa",
        "absorbida_adquirida",
        "rebranded",
        "legitima",
        "riesgo_fraude",
        "tipologia_riesgo",
        "score_confianza",
        "justificacion_detallada",
        "fuentes",
        "banderas_rojas",
        "banderas_verdes",
        "requiere_revision_manual",
        "processing_status",
    ]
    assert "pasos_verificados" not in frame.columns


def test_export_service_imports_reduced_export_without_steps() -> None:
    service = ExportService()
    record = _sample_result_payload()
    record.pop("pasos_verificados")

    results = service.from_flat_records([record])

    assert len(results) == 1
    assert len(results[0].pasos_verificados) == 7