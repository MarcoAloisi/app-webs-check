"""Prompt builders for OpenRouter verification."""

from __future__ import annotations

import json
from typing import Any

from company_verifier.models import CompanyInput

SYSTEM_PROMPT = """Eres un analista senior de verificación corporativa. Debes evaluar legitimidad y estado operativo de una empresa con un enfoque conservador, auditable y antifabulación.

Reglas obligatorias:
- Nunca inventes fuentes, datos, registros ni fechas.
- Debes devolver SOLO JSON válido.
- Completa exactamente 7 pasos verificados, uno por cada paso pedido.
- Si una evidencia falta o es débil, usa status 'not_verifiable'.
- Distingue entre ausencia de evidencia y evidencia negativa.
- Si detectas contradicciones o score bajo, requiere_revision_manual debe ser true.
- La justificación debe citar evidencias textuales concretas del sitio o de las fuentes entregadas.
"""


def build_verification_prompt(
    company: CompanyInput,
    web_evidence: dict[str, Any],
    *,
    enable_web_search: bool,
) -> str:
    """Build the user prompt sent to the LLM."""
    prompt_payload = {
        "objetivo": "Verificar rigurosamente una empresa y su sitio web siguiendo 7 pasos obligatorios.",
        "empresa": {
            "nombre_empresa": company.nombre_empresa,
            "web_input": company.web,
            "web_normalized": company.web_normalized,
            "domain_normalized": company.domain_normalized,
        },
        "web_search_habilitado": enable_web_search,
        "evidencia_precolectada": web_evidence,
        "salida_requerida": {
            "nombre_empresa": "str",
            "web_input": "str",
            "web_verificada": "str|null",
            "existe": "si|no|indeterminado",
            "operativa": "si|no|indeterminado",
            "legitima": "si|no|sospechosa",
            "riesgo_fraude": "bajo|medio|alto",
            "tipologia_riesgo": ["str"],
            "score_confianza": "int 0-100",
            "pasos_verificados": [
                {
                    "step_number": 1,
                    "name": "str",
                    "status": "completed|failed|not_verifiable",
                    "finding": "str",
                    "evidence": ["str"],
                    "sources": ["url"],
                }
            ],
            "justificacion_detallada": "3-5 frases con evidencias concretas",
            "fuentes": ["url o referencia textual"],
            "banderas_rojas": ["str"],
            "banderas_verdes": ["str"],
            "requiere_revision_manual": "bool",
        },
        "instrucciones": [
            "No concluyas hasta reflejar los 7 pasos.",
            "El paso 4 solo puede usar búsqueda externa si web_search_habilitado es true; si no, marca not_verifiable.",
            "No añadas markdown, ni comentarios, ni texto fuera del JSON.",
        ],
    }
    return json.dumps(prompt_payload, ensure_ascii=False, indent=2)
