from __future__ import annotations

from company_verifier.models import CompanyInput
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
