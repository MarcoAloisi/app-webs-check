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
    assert result.absorbida_adquirida == "no"
    assert result.rebranded == "no"


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
            "absorbida_adquirida": "si",
            "rebranded": "no",
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
    assert result.absorbida_adquirida == "si"
    assert result.rebranded == "no"
    assert result.requiere_revision_manual is True


def test_llm_result_keeps_legitimacy_for_confirmed_acquired_successor() -> None:
    service = ResultValidationService()
    company = CompanyInput(
        row_number=8,
        nombre_empresa="Legacy Corp",
        web="https://legacy.example",
        web_normalized="https://legacy.example",
        domain_normalized="legacy.example",
        record_hash="legacy-12345678",
    )
    envelope = LlmEnvelope(
        model="openai/gpt-oss-120b:free",
        prompt="prompt",
        raw_response="raw",
        parsed_json={
            "nombre_empresa": "Legacy Corp",
            "web_input": "https://legacy.example",
            "web_verificada": "https://successor.example",
            "existe": "si",
            "operativa": "no",
            "absorbida_adquirida": "si",
            "rebranded": "no",
            "legitima": "si",
            "riesgo_fraude": "bajo",
            "tipologia_riesgo": ["empresa_adquirida"],
            "score_confianza": 84,
            "pasos_verificados": [
                {"step_number": 1, "name": "Paso 1", "status": "completed", "finding": "Dominio original redirige al sucesor.", "evidence": ["Redirección al sucesor"], "sources": ["https://legacy.example"]},
                {"step_number": 2, "name": "Paso 2", "status": "completed", "finding": "Contenido reciente en la web sucesora.", "evidence": ["Noticias recientes"], "sources": ["https://successor.example/news"]},
                {"step_number": 3, "name": "Paso 3", "status": "completed", "finding": "La marca histórica coincide con el anuncio de adquisición.", "evidence": ["Adquisición confirmada"], "sources": ["https://successor.example/about"]},
                {"step_number": 4, "name": "Paso 4", "status": "completed", "finding": "LinkedIn y noticias confirman continuidad corporativa.", "evidence": ["Perfil corporativo activo"], "sources": ["https://linkedin.com/company/successor"]},
                {"step_number": 5, "name": "Paso 5", "status": "completed", "finding": "Sin señales de fraude; sucesor legítimo.", "evidence": ["Branding consistente"], "sources": ["https://successor.example"]},
                {"step_number": 6, "name": "Paso 6", "status": "completed", "finding": "La entidad original fue absorbida, pero el negocio sigue operativo bajo el sucesor.", "evidence": ["Continuidad operativa confirmada"], "sources": ["https://successor.example/about"]},
                {"step_number": 7, "name": "Paso 7", "status": "completed", "finding": "El dominio sucesor está activo y es legítimo.", "evidence": ["Dominio sucesor activo"], "sources": ["https://successor.example"]},
            ],
            "justificacion_detallada": "Legacy Corp fue adquirida y su operación continúa bajo Successor Inc. El dominio sucesor https://successor.example está activo, presenta branding coherente y fuentes externas confirman la continuidad corporativa. No se observan señales de fraude ni ruptura operativa relevante.",
            "fuentes": [
                "https://legacy.example",
                "https://successor.example",
                "https://linkedin.com/company/successor",
            ],
            "banderas_rojas": [],
            "banderas_verdes": [
                "Adquisición confirmada por fuentes externas",
                "Dominio sucesor activo y coherente",
            ],
            "requiere_revision_manual": False,
        },
    )

    result = service.normalize(company, web_evidence={}, envelope=envelope, manual_review_threshold=70, web_search_enabled=True)

    assert result.legitima == "si"
    assert result.score_confianza >= 80
    assert result.riesgo_fraude == "bajo"


def test_llm_result_downgrades_hijacked_domain_with_scam_and_stale_linkedin() -> None:
    service = ResultValidationService()
    company = CompanyInput(
        row_number=9,
        nombre_empresa="Tooteko Srls",
        web="https://tooteko.com",
        web_normalized="https://tooteko.com",
        domain_normalized="tooteko.com",
        record_hash="tooteko-12345678",
    )
    envelope = LlmEnvelope(
        model="openai/gpt-oss-120b:free",
        prompt="prompt",
        raw_response="raw",
        parsed_json={
            "nombre_empresa": "Tooteko Srls",
            "web_input": "https://tooteko.com",
            "web_verificada": None,
            "existe": "si",
            "operativa": "si",
            "absorbida_adquirida": "no",
            "rebranded": "no",
            "legitima": "si",
            "riesgo_fraude": "medio",
            "tipologia_riesgo": ["posible_sitio_secuestrado", "phishing_moderado"],
            "score_confianza": 78,
            "pasos_verificados": [
                {"step_number": 1, "name": "Paso 1", "status": "completed", "finding": "El dominio responde pero muestra contenido deportivo no relacionado.", "evidence": ["Contenido no relacionado"], "sources": ["https://tooteko.com"]},
                {"step_number": 2, "name": "Paso 2", "status": "completed", "finding": "Sin señales consistentes de actualización corporativa reciente.", "evidence": ["Contenido ajeno a la actividad histórica"], "sources": ["https://tooteko.com"]},
                {"step_number": 3, "name": "Paso 3", "status": "completed", "finding": "La entidad legal existe, pero el sitio actual no coincide con su actividad.", "evidence": ["Mismatch dominio-entidad"], "sources": ["https://registro.example"]},
                {"step_number": 4, "name": "Paso 4", "status": "completed", "finding": "LinkedIn existe desde 2014, pero sin empleados visibles ni actividad reciente.", "evidence": ["LinkedIn antiguo sin actividad", "Sin empleados"], "sources": ["https://linkedin.com/company/tooteko"]},
                {"step_number": 5, "name": "Paso 5", "status": "completed", "finding": "Hay reportes asociando la web a scam y posible phishing moderado.", "evidence": ["Scam reports"], "sources": ["https://forum.example/report"]},
                {"step_number": 6, "name": "Paso 6", "status": "completed", "finding": "La entidad legal sigue activa en registros italianos.", "evidence": ["Registro mercantil activo"], "sources": ["https://registro.example"]},
                {"step_number": 7, "name": "Paso 7", "status": "completed", "finding": "No se confirmó un dominio alternativo legítimo actual.", "evidence": ["Sin dominio sucesor confirmado"], "sources": ["https://tooteko.com"]},
            ],
            "justificacion_detallada": "Tooteko Srls sigue activa en registros, pero tooteko.com muestra contenido no relacionado con la empresa y esto sugiere posible secuestro o reutilización del sitio. El LinkedIn histórico no aporta continuidad actual porque no muestra empleados visibles ni actividad reciente. Además existen señales reputacionales de scam asociadas a la web, por lo que la legitimidad digital no puede darse por confirmada y el riesgo debe mantenerse al menos moderado.",
            "fuentes": [
                "https://tooteko.com",
                "https://linkedin.com/company/tooteko",
                "https://registro.example",
                "https://forum.example/report",
            ],
            "banderas_rojas": [
                "Contenido no relacionado con la empresa",
                "Posible sitio secuestrado",
                "LinkedIn antiguo sin actividad",
                "Sin empleados visibles",
                "Scam reports asociados al dominio",
            ],
            "banderas_verdes": [
                "Entidad legal activa en registros italianos",
            ],
            "requiere_revision_manual": True,
        },
    )

    result = service.normalize(company, web_evidence={}, envelope=envelope, manual_review_threshold=70, web_search_enabled=True)

    assert result.legitima == "sospechosa"
    assert result.score_confianza <= 50
    assert result.riesgo_fraude == "medio"
    assert result.requiere_revision_manual is True
