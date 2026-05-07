"""Validation and conservative fallback for LLM outputs."""

from __future__ import annotations

import json
from typing import Any

from pydantic import ValidationError

from company_verifier.models import (
    BinaryAnswer,
    CompanyInput,
    CompanyVerificationResult,
    LegitimacyAnswer,
    LlmEnvelope,
    ProcessingStatus,
    RiskLevel,
    StepStatus,
    TernaryAnswer,
    VerificationStepResult,
)

STEP_NAMES = {
    1: "Resolución del dominio",
    2: "Análisis del contenido web",
    3: "Coincidencia empresa ↔ web",
    4: "Validación cruzada externa",
    5: "Detección de señales de fraude/scam",
    6: "Estado operativo",
    7: "Web alternativa oficial",
}


class ResultValidationService:
    """Normalize model responses and create safe fallbacks."""

    def normalize(
        self,
        company: CompanyInput,
        web_evidence: dict[str, Any],
        envelope: LlmEnvelope | None,
        *,
        manual_review_threshold: int,
        web_search_enabled: bool,
    ) -> CompanyVerificationResult:
        if envelope and envelope.parsed_json:
            try:
                result = CompanyVerificationResult.model_validate(
                    {
                        **envelope.parsed_json,
                        "nombre_empresa": envelope.parsed_json.get("nombre_empresa") or company.nombre_empresa,
                        "web_input": envelope.parsed_json.get("web_input") or company.web,
                        "prompt_enviado": envelope.prompt,
                        "respuesta_llm_cruda": envelope.raw_response,
                    }
                )
                if len(result.pasos_verificados) == 7:
                    result = self._apply_conservative_legitimacy_guards(result)
                    result = self._apply_conservative_score_caps(result)
                    if result.score_confianza < manual_review_threshold:
                        result.requiere_revision_manual = True
                    return result
            except ValidationError:
                pass
        return self._fallback(company, web_evidence, envelope, manual_review_threshold, web_search_enabled)

    def _extract_conservative_markers(self, result: CompanyVerificationResult) -> dict[str, bool]:
        flags_text = " | ".join(result.banderas_rojas).lower()
        justification_text = result.justificacion_detallada.lower()
        combined_text = f"{flags_text} | {justification_text}"

        severe_discontinuity_markers = (
            "liquid",
            "disuelt",
            "cerrad",
            "out of business",
            "inactiv",
            "discontinu",
        )
        domain_failure_markers = (
            "no resuelve",
            "dominio caído",
            "dominio expir",
            "expirad",
            "parking",
            "parquead",
        )
        mismatch_markers = (
            "desajuste",
            "mismatch",
            "sin conexión confirmada",
            "cambio de titular",
            "marca distinta",
        )
        absent_presence_markers = (
            "ausencia de presencia digital",
            "sin linkedin",
            "sin actividad reciente",
            "sin noticias recientes",
            "sin continuidad operativa",
        )
        return {
            "severe_discontinuity": any(marker in combined_text for marker in severe_discontinuity_markers),
            "domain_failure": any(marker in combined_text for marker in domain_failure_markers),
            "mismatch": any(marker in combined_text for marker in mismatch_markers),
            "absent_presence": any(marker in combined_text for marker in absent_presence_markers),
        }

    def _apply_conservative_legitimacy_guards(self, result: CompanyVerificationResult) -> CompanyVerificationResult:
        markers = self._extract_conservative_markers(result)
        should_downgrade_legitimacy = False

        if result.legitima == LegitimacyAnswer.YES:
            if markers["severe_discontinuity"] and (markers["domain_failure"] or markers["absent_presence"]):
                should_downgrade_legitimacy = True
            elif result.operativa == TernaryAnswer.NO and (markers["mismatch"] or markers["absent_presence"]):
                should_downgrade_legitimacy = True
            elif result.riesgo_fraude == RiskLevel.HIGH:
                should_downgrade_legitimacy = True

        if should_downgrade_legitimacy:
            result.legitima = LegitimacyAnswer.SUSPICIOUS
            result.requiere_revision_manual = True
            downgrade_flag = "La legitimidad actual no queda confirmada por discontinuidad operativa o inconsistencia dominio-entidad."
            if downgrade_flag not in result.banderas_rojas:
                result.banderas_rojas.append(downgrade_flag)

        return result

    def _apply_conservative_score_caps(self, result: CompanyVerificationResult) -> CompanyVerificationResult:
        max_score = 100
        markers = self._extract_conservative_markers(result)

        if result.operativa == TernaryAnswer.NO:
            max_score = min(max_score, 59)
        if result.requiere_revision_manual and result.riesgo_fraude != RiskLevel.LOW:
            max_score = min(max_score, 69)
        if markers["severe_discontinuity"]:
            max_score = min(max_score, 35)
        if result.operativa == TernaryAnswer.NO and markers["domain_failure"]:
            max_score = min(max_score, 30)
        if markers["mismatch"] and markers["absent_presence"]:
            max_score = min(max_score, 45)

        result.score_confianza = min(result.score_confianza, max_score)
        return result

    def _fallback(
        self,
        company: CompanyInput,
        web_evidence: dict[str, Any],
        envelope: LlmEnvelope | None,
        manual_review_threshold: int,
        web_search_enabled: bool,
    ) -> CompanyVerificationResult:
        reachable = isinstance(web_evidence.get("status_code"), int) and int(web_evidence["status_code"]) < 500
        company_match = bool(web_evidence.get("company_name_match"))
        has_contacts = bool(web_evidence.get("contact_emails") or web_evidence.get("contact_phones"))
        has_legal = bool(web_evidence.get("legal_identifiers"))
        parked = bool(web_evidence.get("parked_domain"))
        empty_site = bool(web_evidence.get("empty_site"))
        ssl_valid = web_evidence.get("ssl_valid") is True

        score = 10
        if reachable:
            score += 25
        if ssl_valid:
            score += 10
        if company_match:
            score += 18
        if has_contacts:
            score += 12
        if has_legal:
            score += 10
        if parked:
            score -= 25
        if empty_site:
            score -= 10
        if web_evidence.get("error"):
            score -= 10
        score = max(0, min(100, score))

        existe = TernaryAnswer.YES if reachable else TernaryAnswer.UNDETERMINED
        operativa = TernaryAnswer.YES if reachable and not empty_site and not parked else TernaryAnswer.UNDETERMINED
        legitima = LegitimacyAnswer.YES if company_match and not parked else LegitimacyAnswer.SUSPICIOUS
        risk = RiskLevel.LOW if score >= 75 else RiskLevel.MEDIUM if score >= 45 else RiskLevel.HIGH
        if parked:
            legitima = LegitimacyAnswer.NO
            operativa = TernaryAnswer.NO
            risk = RiskLevel.HIGH

        flags_red: list[str] = []
        flags_green: list[str] = []
        risk_types: list[str] = []
        if parked:
            flags_red.append("La web parece parqueada o en venta.")
            risk_types.append("dominio_parqueado")
        if empty_site:
            flags_red.append("El sitio contiene muy poco contenido útil.")
        if not company_match:
            flags_red.append("No hay coincidencia fuerte entre empresa y contenido del sitio.")
        if web_evidence.get("error"):
            flags_red.append(f"Error al acceder a la web: {web_evidence['error']}")
        if ssl_valid:
            flags_green.append("El certificado SSL es válido.")
        if has_contacts:
            flags_green.append("Se detectaron datos de contacto en el sitio.")
        if has_legal:
            flags_green.append("Se detectaron identificadores legales en el contenido.")
        if company_match:
            flags_green.append("El nombre de la empresa aparece alineado con el contenido del sitio.")

        steps = self._build_steps(company, web_evidence, web_search_enabled)
        justification = self._build_justification(company, web_evidence, steps, web_search_enabled)
        result = CompanyVerificationResult(
            nombre_empresa=company.nombre_empresa,
            web_input=company.web,
            web_verificada=web_evidence.get("final_url") or company.web_normalized,
            existe=existe,
            operativa=operativa,
            absorbida_adquirida=BinaryAnswer.NO,
            rebranded=BinaryAnswer.NO,
            legitima=legitima,
            riesgo_fraude=risk,
            tipologia_riesgo=risk_types,
            score_confianza=score,
            pasos_verificados=steps,
            justificacion_detallada=justification,
            fuentes=list(dict.fromkeys(web_evidence.get("source_urls") or [company.web_normalized]))[:12],
            banderas_rojas=flags_red,
            banderas_verdes=flags_green,
            requiere_revision_manual=score < manual_review_threshold or any(step.status != StepStatus.COMPLETED for step in steps),
            prompt_enviado=envelope.prompt if envelope else "",
            respuesta_llm_cruda=envelope.raw_response if envelope else "",
            processing_status=ProcessingStatus.COMPLETED,
        )
        return result

    def _build_steps(
        self,
        company: CompanyInput,
        web_evidence: dict[str, Any],
        web_search_enabled: bool,
    ) -> list[VerificationStepResult]:
        status_code = web_evidence.get("status_code")
        step1_status = StepStatus.COMPLETED if status_code else StepStatus.NOT_VERIFIABLE
        step2_status = StepStatus.COMPLETED if web_evidence.get("text_snippet") else StepStatus.NOT_VERIFIABLE
        step3_status = StepStatus.COMPLETED if web_evidence.get("text_snippet") else StepStatus.NOT_VERIFIABLE
        step4_status = StepStatus.NOT_VERIFIABLE
        step5_status = StepStatus.COMPLETED if web_evidence.get("status_code") or web_evidence.get("error") else StepStatus.NOT_VERIFIABLE
        step6_status = StepStatus.COMPLETED if web_evidence.get("text_snippet") else StepStatus.NOT_VERIFIABLE
        step7_status = StepStatus.COMPLETED if web_evidence.get("final_url") else StepStatus.NOT_VERIFIABLE
        sources = list(dict.fromkeys(web_evidence.get("source_urls") or [company.web_normalized]))[:6]
        return [
            VerificationStepResult(
                step_number=1,
                name=STEP_NAMES[1],
                status=step1_status,
                finding=(
                    f"HTTP {status_code}; redirecciones: {len(web_evidence.get('redirect_chain', []))}; SSL válido: {web_evidence.get('ssl_valid')}; WHOIS: no verificable."
                    if status_code
                    else f"No fue posible resolver el dominio con evidencia concluyente. Error: {web_evidence.get('error', 'sin detalle')}"
                ),
                evidence=[
                    f"URL final: {web_evidence.get('final_url')}",
                    f"SSL válido: {web_evidence.get('ssl_valid')}",
                ],
                sources=sources,
            ),
            VerificationStepResult(
                step_number=2,
                name=STEP_NAMES[2],
                status=step2_status,
                finding=(
                    f"Título: {web_evidence.get('html_title') or 'no disponible'}; meta descripción: {web_evidence.get('meta_description') or 'no disponible'}; contactos detectados: {len(web_evidence.get('contact_emails', [])) + len(web_evidence.get('contact_phones', []))}."
                ),
                evidence=[web_evidence.get("text_snippet") or "Sin contenido extraíble."],
                sources=sources,
            ),
            VerificationStepResult(
                step_number=3,
                name=STEP_NAMES[3],
                status=step3_status,
                finding=(
                    "La web parece corresponder a la empresa indicada."
                    if web_evidence.get("company_name_match")
                    else "No se encontró una coincidencia clara entre el nombre de la empresa y el contenido del sitio."
                ),
                evidence=[
                    f"Coincidencia de nombre: {web_evidence.get('company_name_match')}",
                    f"Dominio parqueado: {web_evidence.get('parked_domain')}",
                ],
                sources=sources,
            ),
            VerificationStepResult(
                step_number=4,
                name=STEP_NAMES[4],
                status=step4_status,
                finding=(
                    "El modelo no pudo usar búsqueda externa o no había evidencia suficiente; paso marcado como no verificable."
                    if web_search_enabled
                    else "La búsqueda externa está deshabilitada para este modelo; paso marcado como no verificable."
                ),
                evidence=[],
                sources=[],
            ),
            VerificationStepResult(
                step_number=5,
                name=STEP_NAMES[5],
                status=step5_status,
                finding=(
                    "No se observaron señales graves de fraude en la evidencia local."
                    if not web_evidence.get("parked_domain")
                    else "Se detectó patrón de dominio parqueado o en venta, compatible con riesgo elevado."
                ),
                evidence=[
                    f"Dominio parqueado: {web_evidence.get('parked_domain')}",
                    f"Sitio vacío: {web_evidence.get('empty_site')}",
                ],
                sources=sources,
            ),
            VerificationStepResult(
                step_number=6,
                name=STEP_NAMES[6],
                status=step6_status,
                finding=(
                    "Hay indicios limitados de operación actual a partir del contenido disponible."
                    if web_evidence.get("text_snippet")
                    else "No se encontraron señales suficientes para concluir actividad reciente."
                ),
                evidence=[web_evidence.get("text_snippet") or "Sin contenido suficiente."],
                sources=sources,
            ),
            VerificationStepResult(
                step_number=7,
                name=STEP_NAMES[7],
                status=step7_status,
                finding=(
                    f"No hay evidencia de una web alternativa mejor que {web_evidence.get('final_url') or company.web_normalized}."
                ),
                evidence=[f"Web final observada: {web_evidence.get('final_url') or company.web_normalized}"],
                sources=sources,
            ),
        ]

    def _build_justification(
        self,
        company: CompanyInput,
        web_evidence: dict[str, Any],
        steps: list[VerificationStepResult],
        web_search_enabled: bool,
    ) -> str:
        sentences = [
            f"Para {company.nombre_empresa}, el paso 1 revisó el dominio {company.web_normalized} y observó estado HTTP {web_evidence.get('status_code') or 'no verificable'}, con SSL {web_evidence.get('ssl_valid')} y sin dato fiable de WHOIS.",
            f"En el paso 2 se extrajo el título '{web_evidence.get('html_title') or 'no disponible'}' y un fragmento del contenido: '{(web_evidence.get('text_snippet') or 'sin contenido')[:180]}'.",
            f"El paso 3 concluye que la coincidencia empresa-web es {'consistente' if web_evidence.get('company_name_match') else 'débil o contradictoria'}, lo que afecta a la legitimidad estimada.",
            (
                "El paso 4 quedó como no verificable porque no hubo búsqueda externa disponible o no devolvió evidencia auditable."
                if web_search_enabled
                else "El paso 4 quedó como no verificable porque la búsqueda externa estaba deshabilitada para esta ejecución."
            ),
            f"Los pasos 5 a 7 revisaron señales de fraude, actividad y web alternativa; las banderas más relevantes fueron: {', '.join((steps[4].evidence + steps[5].evidence)[:3]) or 'sin hallazgos adicionales'}.",
        ]
        return " ".join(sentences)



def dump_result_json(result: CompanyVerificationResult) -> str:
    return json.dumps(result.model_dump(mode="json"), ensure_ascii=False)
