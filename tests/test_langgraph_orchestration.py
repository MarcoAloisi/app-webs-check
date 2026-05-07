from __future__ import annotations

import re

from company_verifier.models import AppSettings, CompanyInput
from company_verifier.services.openrouter_client import _build_extra_body
from company_verifier.services.prompt_builder import SYSTEM_PROMPT, build_verification_prompt
from company_verifier.services.verification_orchestrator import VerificationOrchestrator, _build_web_search_options


def test_openrouter_web_search_payload_is_enabled() -> None:
    payload = _build_extra_body(
        True,
        {
            "engine": "exa",
            "max_results": 5,
            "max_total_results": 20,
            "search_context_size": "medium",
            "allowed_domains": ["example.com"],
            "excluded_domains": ["reddit.com"],
        },
    )

    assert payload["tools"][0]["type"] == "openrouter:web_search"
    assert payload["tools"][0]["parameters"]["engine"] == "exa"
    assert payload["tools"][0]["parameters"]["allowed_domains"] == ["example.com"]


def test_web_search_options_helper_maps_settings() -> None:
    settings = AppSettings(
        model="x-ai/grok-3-mini",
        enable_web_search=True,
        web_search_engine="native",
        web_search_max_results=4,
        web_search_max_total_results=12,
        web_search_context_size="high",
        web_search_allowed_domains=["arxiv.org"],
        web_search_excluded_domains=["reddit.com"],
    )

    payload = _build_web_search_options(settings)

    assert payload["engine"] == "native"
    assert payload["max_total_results"] == 12
    assert payload["allowed_domains"] == ["arxiv.org"]


def test_langgraph_orchestrator_returns_conservative_result_without_api_key() -> None:
    orchestrator = VerificationOrchestrator(api_key=None)
    company = CompanyInput(
        row_number=2,
        nombre_empresa="Acme Corp",
        web="https://example.com",
        web_normalized="https://example.com",
        domain_normalized="example.com",
        record_hash="hash-example-1234",
    )
    settings = AppSettings(model="openai/gpt-4o-mini", enable_web_search=True)

    result = orchestrator.process_company(company, settings=settings)

    assert len(result.pasos_verificados) == 7
    assert result.requiere_revision_manual is True
    assert result.web_input == "https://example.com"


def test_prompt_requires_deep_osint_and_linkedin_checks() -> None:
    company = CompanyInput(
        row_number=7,
        nombre_empresa="Example Corp",
        web="https://example.com",
        web_normalized="https://example.com",
        domain_normalized="example.com",
        record_hash="hash-example-5678",
    )

    prompt = build_verification_prompt(
        company,
        {"final_url": "https://example.com", "status_code": 200},
        enable_web_search=True,
    )

    assert "LinkedIn corporativo actual" in prompt
    assert "WHOIS" in prompt
    assert "dominio oficial actual, alternativo o sucesor" in prompt
    assert "dominio sucesor o alternativo parece activo, legítimo y coherente" in prompt
    assert "No te limites a comprobar si la web carga" in prompt
    assert "web_verificada" in prompt
    assert "absorbida_adquirida" in prompt
    assert "rebranded" in prompt
    assert "investiga ese nuevo dominio o rebranding para comprobar la legalidad" in prompt


def test_system_prompt_enforces_conservative_domain_change_analysis() -> None:
    assert "cambio de dominio" in SYSTEM_PROMPT
    assert "LinkedIn" in SYSTEM_PROMPT
    assert "absorbida/adquirida" in SYSTEM_PROMPT
    assert "investigar también el nuevo dominio o la nueva marca" in SYSTEM_PROMPT
    assert re.search(r"SOLO JSON válido", SYSTEM_PROMPT) is not None