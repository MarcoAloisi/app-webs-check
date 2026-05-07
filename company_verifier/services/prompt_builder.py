"""Prompt builders for OpenRouter verification."""

from __future__ import annotations

import json
from typing import Any

from company_verifier.models import CompanyInput

SYSTEM_PROMPT = """Eres un analista senior de ciberseguridad, OSINT y verificación corporativa. Debes evaluar legitimidad, estado operativo, riesgo de fraude y continuidad digital de una empresa con un enfoque conservador, auditable y antifabulación.

Reglas obligatorias:
- Nunca inventes fuentes, datos, registros, dominios, perfiles, fechas ni conclusiones.
- Debes devolver SOLO JSON válido que respete exactamente la estructura pedida.
- Completa exactamente 7 pasos verificados, uno por cada paso pedido.
- Si una evidencia falta, es ambigua o es insuficiente, usa status 'not_verifiable'.
- Distingue siempre entre ausencia de evidencia, evidencia negativa y evidencia contradictoria.
- Prioriza fuentes oficiales o reputadas: sitio oficial, LinkedIn corporativo, noticias fiables, registros mercantiles, directorios empresariales serios y páginas de empleo oficiales.
- Verifica y explica explícitamente si la empresa fue absorbida/adquirida o si hizo rebranding; si no hay evidencia suficiente, no lo afirmes.
- Cuando web_search_habilitado sea true, debes investigar en profundidad la empresa detrás del dominio, incluyendo señales de continuidad operativa, rebranding, adquisición, cambio de dominio, redirecciones y presencia actual en LinkedIn.
- Si detectas que el dominio actual ya no representa a la empresa original, debes indicarlo explícitamente y tratar de identificar el dominio oficial actual. Si no puedes confirmarlo, indícalo como no identificable.
- La justificación debe citar evidencias textuales concretas del sitio o de las fuentes entregadas.
- Si detectas contradicciones, señales de fraude, cambio de titularidad, score bajo o falta de evidencia crítica, requiere_revision_manual debe ser true.
- El score_confianza debe reflejar continuidad operativa y consistencia global, no solo existencia legal histórica.
- No otorgues scores altos por el mero hecho de encontrar un registro mercantil o una antigua ficha corporativa si la empresa aparece liquidada, cerrada, out of business, sin actividad reciente o con dominio caído.
- Regla de severidad: si la empresa está liquidada, cerrada, inactiva, out of business o sin continuidad operativa verificable, el score_confianza normalmente debe quedar por debajo de 40.
- Regla de severidad: si el dominio no resuelve, no hay presencia digital actual fiable y además hay desajuste entre la entidad legal y el dominio o la marca, el score_confianza normalmente debe quedar por debajo de 30.
- Regla de coherencia: si operativa es 'no', el score_confianza no debe ser alto; evita scores >= 70 salvo evidencia excepcional y muy bien justificada, lo cual será extremadamente raro.
- Penaliza fuertemente señales acumuladas como dominio expirado/no resuelve, empresa liquidada, ausencia de LinkedIn corporativo actual, ausencia de noticias recientes, mismatch de nombres o marca, y falta de dominio sucesor confirmado.
- La etiqueta legitima='si' exige evidencia actual y consistente de identidad corporativa válida; no basta con existencia histórica en registros.
- Si la empresa existió pero está liquidada, cerrada, out of business o sin continuidad digital verificable, evita legitima='si'; usa normalmente 'sospechosa' salvo evidencia excepcional de sucesión o continuidad corporativa clara.
- Si además existe desajuste entre entidad legal, marca y dominio, la legitimidad no debe clasificarse como afirmativa.
"""


def build_verification_prompt(
    company: CompanyInput,
    web_evidence: dict[str, Any],
    *,
    enable_web_search: bool,
) -> str:
    """Build the user prompt sent to the LLM."""
    prompt_payload = {
        "objetivo": (
            "Verificar rigurosamente una empresa y su dominio con enfoque de ciberseguridad y OSINT, "
            "determinando actividad real del dominio, actualidad del contenido, existencia de la empresa, "
            "continuidad operativa, legitimidad, riesgo de fraude y posible migración a dominios nuevos u oficiales."
        ),
        "empresa": {
            "nombre_empresa": company.nombre_empresa,
            "web_input": company.web,
            "web_normalized": company.web_normalized,
            "domain_normalized": company.domain_normalized,
        },
        "web_search_habilitado": enable_web_search,
        "evidencia_precolectada": web_evidence,
        "criterios_de_investigacion": [
            "No te limites a comprobar si la web carga; debes investigar la empresa real detrás del dominio.",
            "Determina si el dominio resuelve, si el sitio sirve contenido real y si existen redirecciones a otro dominio, subdominio, LinkedIn, GitHub o web corporativa matriz.",
            "Busca evidencia visible y fechada de actualización reciente del sitio, idealmente dentro de los últimos 3 a 6 meses.",
            "Identifica el nombre de la empresa, su país o región principal y su sector, usando tanto la web como fuentes reputadas.",
            "Comprueba si la empresa sigue operando mediante LinkedIn corporativo, WHOIS, noticias recientes, ofertas de empleo, directorios empresariales, registros, comunicados oficiales y, cuando aporten señal útil, foros o comunidades públicas.",
            "Verifica expresamente si el dominio cambió de propietario, propósito o marca, o si la empresa migró a otro dominio oficial.",
            "Si detectas un cambio de dominio, rebranding, absorción o adquisición, intenta identificar el dominio principal actual y explica brevemente qué cambió y cuándo pudo ocurrir aproximadamente.",
            "Si no encuentras un dominio nuevo o alternativo fiable para la misma empresa, indícalo explícitamente como no identificado.",
            "Evalúa legitimidad y riesgo con base en señales positivas y red flags: branding consistente, contacto verificable, cobertura reputada, registros fiables, advertencias de phishing, typosquatting, parking o contenido sospechoso.",
            "Si web_search_habilitado es false, no inventes búsquedas externas y limita los pasos externos a la evidencia ya disponible; si es true, úsalo para profundizar especialmente en LinkedIn, noticias y posibles dominios alternativos.",
        ],
        "pasos_obligatorios": [
            {
                "step_number": 1,
                "name": "Actividad real del dominio",
                "debes_verificar": [
                    "Resolución o accesibilidad del dominio/sitio",
                    "Contenido real vs parking, venta, suspensión o placeholder",
                    "Redirecciones relevantes y dominio final",
                ],
            },
            {
                "step_number": 2,
                "name": "Recencia del contenido web",
                "debes_verificar": [
                    "Evidencia fechada visible en noticias, blog, press release, eventos, anuncios o footer",
                    "Si el contenido parece actualizado en los últimos 3 a 6 meses",
                    "Explicación breve si la recencia es negativa o indeterminada",
                ],
            },
            {
                "step_number": 3,
                "name": "Identificación de la empresa",
                "debes_verificar": [
                    "Nombre real de la empresa u organización asociada al dominio",
                    "Coincidencia entre marca, dominio, textos del sitio y empresa declarada",
                    "País o región principal y sector cuando sea posible",
                ],
            },
            {
                "step_number": 4,
                "name": "Validación cruzada externa",
                "debes_verificar": [
                    "LinkedIn corporativo actual",
                    "Noticias recientes, directorios, registros, perfiles oficiales o páginas de empleo",
                    "Evidencia de continuidad operativa, adquisición, rebranding o cierre",
                ],
            },
            {
                "step_number": 5,
                "name": "Legitimidad y riesgo",
                "debes_verificar": [
                    "Consistencia de branding, contacto, dirección y datos legales",
                    "Señales de phishing, malware, scam, impersonación o typosquatting",
                    "Clasificación conservadora del riesgo y tipología detectada",
                ],
            },
            {
                "step_number": 6,
                "name": "Estado operativo actual",
                "debes_verificar": [
                    "Si la empresa existe actualmente y parece operativa",
                    "Si parece defunta, adquirida, renombrada o inactiva",
                    "Distinción entre empresa legítima no operativa y dominio sospechoso",
                ],
            },
            {
                "step_number": 7,
                "name": "Dominio oficial actual o alternativo",
                "debes_verificar": [
                    "Si el dominio original dejó de ser el principal o cambió de propósito",
                    "Cuál sería el dominio oficial actual, alternativo o sucesor de la misma empresa",
                    "Si no se identifica ninguno, indicarlo de forma explícita",
                ],
            },
        ],
        "salida_requerida": {
            "nombre_empresa": "str",
            "web_input": "str",
            "web_verificada": "str|null (dominio o URL oficial actual confirmada; si no puede confirmarse, null)",
            "existe": "si|no|indeterminado",
            "operativa": "si|no|indeterminado",
            "absorbida_adquirida": "si|no",
            "rebranded": "si|no",
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
            "Cuando la evidencia apunte a un dominio nuevo, redirección corporativa, adquisición o rebranding, refléjalo también en web_verificada, fuentes y justificacion_detallada.",
            "Si la empresa fue absorbida o adquirida, marca absorbida_adquirida = si; si hay evidencia de cambio de marca comercial, marca rebranded = si.",
            "Si usas LinkedIn, noticias u otras fuentes externas, cita la evidencia de forma concreta y conservadora sin inventar URLs no observadas.",
            "No afirmes que una empresa está activa solo porque el dominio cargue; busca señales adicionales de actividad real cuando sea posible.",
            "Ajusta el score_confianza de forma conservadora: registro histórico o existencia legal no equivalen a continuidad operativa actual.",
            "Si la evidencia combina empresa liquidada o out of business + dominio no resuelve o presencia digital ausente + mismatch de nombre, el score_confianza debe quedar claramente bajo, normalmente por debajo de 30-40.",
            "Si operativa = no o requiere_revision_manual = true por contradicciones materiales, evita scores altos o cercanos a 75 salvo justificación excepcional basada en evidencia sólida y actual.",
            "No marques legitima = si cuando solo exista evidencia histórica o registral pero no continuidad operativa/digital actual.",
            "Si la empresa está liquidada, out of business o sin continuidad verificable, usa normalmente legitima = sospechosa, especialmente si el dominio no resuelve o hay mismatch con la marca.",
            "No añadas markdown, ni comentarios, ni texto fuera del JSON.",
        ],
    }
    return json.dumps(prompt_payload, ensure_ascii=False, indent=2)
