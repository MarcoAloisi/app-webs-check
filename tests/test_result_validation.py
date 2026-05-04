from __future__ import annotations

from company_verifier.models import CompanyInput
from company_verifier.models import LlmEnvelope
from company_verifier.services.result_validation import ResultValidationService



def test_fallback_creates_seven_steps_and_manual_review() -> None:
    service = ResultValidationService()
    company = CompanyInput(
        row_number=2,
        nombre_empresa="Acme Corp",
        web="https://acme.example",
        web_normalized="https://acme.example",
        domain_normalized="acme.example",
        record_hash="abc123456789def0",
    )
    web_evidence = {
        "status_code": 200,
        "final_url": "https://acme.example",
        "redirect_chain": [],
        "ssl_valid": True,
        "html_title": "Acme Corp",
        "meta_description": "Industrial services",
        "text_snippet": "Acme Corp ofrece servicios industriales con contacto info@acme.example.",
        "contact_emails": ["info@acme.example"],
        "contact_phones": ["+34 555 123 456"],
        "legal_identifiers": ["ESB12345678"],
        "company_name_match": True,
        "parked_domain": False,
        "empty_site": False,
        "error": None,
        "source_urls": ["https://acme.example"],
    }

    result = service.normalize(company, web_evidence, envelope=None, manual_review_threshold=90, web_search_enabled=False)

    assert len(result.pasos_verificados) == 7
    assert result.requiere_revision_manual is True
    assert result.score_confianza > 0
    assert result.web_verificada == "https://acme.example"


def test_llm_result_score_is_capped_for_liquidated_inactive_mismatched_company() -> None:
    service = ResultValidationService()
    company = CompanyInput(
        row_number=7,
        nombre_empresa="Binsight Advising Ltd",
        web="https://binsight.ai",
        web_normalized="https://binsight.ai",
        domain_normalized="binsight.ai",
        record_hash="binsight-12345678",
    )
    envelope = LlmEnvelope(
        model="x-ai/grok-4.1-fast",
        prompt="prompt",
        raw_response="raw",
        parsed_json={
            "nombre_empresa": "Binsight Advising Ltd",
            "web_input": "https://binsight.ai",
            "web_verificada": None,
            "existe": "si",
            "operativa": "no",
            "legitima": "si",
            "riesgo_fraude": "medio",
            "tipologia_riesgo": ["continuidad_ambigua"],
            "score_confianza": 75,
            "pasos_verificados": [
                {"step_number": 1, "name": "Paso 1", "status": "completed", "finding": "Dominio no resuelve.", "evidence": ["Dominio no resuelve"], "sources": ["https://binsight.ai"]},
                {"step_number": 2, "name": "Paso 2", "status": "completed", "finding": "Sin actividad reciente.", "evidence": ["Sin actividad reciente"], "sources": ["https://pitchbook.com/profiles/company/484586-11"]},
                {"step_number": 3, "name": "Paso 3", "status": "completed", "finding": "Desajuste de nombres.", "evidence": ["Mismatch entre entidad y dominio"], "sources": ["https://www.crunchbase.com/organization/binsight-ai"]},
                {"step_number": 4, "name": "Paso 4", "status": "completed", "finding": "No hay LinkedIn corporativo actual fiable.", "evidence": ["Ausencia de presencia digital actual"], "sources": ["https://www.linkedin.com/company/binsight1"]},
                {"step_number": 5, "name": "Paso 5", "status": "completed", "finding": "Sin fraude directo, pero alta ambigüedad.", "evidence": ["Alta ambigüedad"], "sources": ["https://en.checkid.co.il/company/BINSIGHT+ADVISING+LTD-dGYxyNA-515322600"]},
                {"step_number": 6, "name": "Paso 6", "status": "completed", "finding": "Empresa liquidada y no operativa.", "evidence": ["Empresa liquidada"], "sources": ["https://next.obudget.org/i/org/company/515322600"]},
                {"step_number": 7, "name": "Paso 7", "status": "completed", "finding": "Sin dominio sucesor confirmado.", "evidence": ["Sin conexión confirmada"], "sources": ["https://binsight.ai"]},
            ],
            "justificacion_detallada": "Binsight Advising Ltd figura liquidada. El dominio no resuelve y no hay continuidad operativa verificable. Hay desajuste entre la entidad legal y el uso histórico del dominio, además de ausencia de presencia digital actual.",
            "fuentes": [
                "https://en.checkid.co.il/company/BINSIGHT+ADVISING+LTD-dGYxyNA-515322600",
                "https://next.obudget.org/i/org/company/515322600",
                "https://pitchbook.com/profiles/company/484586-11",
                "https://www.crunchbase.com/organization/binsight-ai",
            ],
            "banderas_rojas": [
                "Dominio no resuelve",
                "Empresa liquidada",
                "Desajuste entre nombre empresa y uso histórico del dominio",
                "Ausencia de presencia digital actual",
            ],
            "banderas_verdes": [
                "Entrada verificada en registros mercantiles israelíes",
            ],
            "requiere_revision_manual": True,
        },
    )

    result = service.normalize(company, web_evidence={}, envelope=envelope, manual_review_threshold=70, web_search_enabled=True)

    assert result.score_confianza <= 35
    assert result.legitima == "sospechosa"
    assert result.requiere_revision_manual is True
