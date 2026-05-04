"""LangGraph-based verification orchestrator."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, Literal
from typing_extensions import TypedDict

import streamlit as st
from langgraph.checkpoint.memory import MemorySaver
from langgraph.graph import END, START, StateGraph

from company_verifier.models import AppSettings, CompanyInput, CompanyVerificationResult, LlmEnvelope
from company_verifier.services.openrouter_client import OpenRouterClient
from company_verifier.services.prompt_builder import SYSTEM_PROMPT, build_verification_prompt
from company_verifier.services.result_validation import ResultValidationService
from company_verifier.services.web_evidence import WebEvidenceService

LogCallback = Callable[[str], None]


class VerificationGraphState(TypedDict, total=False):
    company: CompanyInput
    settings: AppSettings
    web_evidence: dict[str, Any]
    llm_envelope: LlmEnvelope | None
    llm_error: str | None
    final_result: CompanyVerificationResult


@st.cache_data(ttl=86_400, show_spinner=False)
def _verify_company_cached(
    row_data: dict[str, object],
    settings_data: dict[str, object],
    api_key: str | None,
) -> dict[str, object]:
    row = CompanyInput.model_validate(row_data)
    settings = AppSettings.model_validate(settings_data)
    workflow = VerificationWorkflow(api_key)
    result = workflow.invoke(row, settings)
    return result.model_dump(mode="json")


class VerificationWorkflow:
    """Encapsulates the LangGraph workflow for a single company."""

    def __init__(self, api_key: str | None) -> None:
        self._client = OpenRouterClient(api_key)
        self._web_evidence_service = WebEvidenceService()
        self._result_validation_service = ResultValidationService()
        self._graph = self._build_graph()

    def invoke(self, company: CompanyInput, settings: AppSettings) -> CompanyVerificationResult:
        state = self._graph.invoke(
            {
                "company": company,
                "settings": settings,
                "llm_envelope": None,
                "llm_error": None,
            },
            config={"configurable": {"thread_id": company.record_hash}},
        )
        return state["final_result"]

    def _build_graph(self):
        builder = StateGraph(VerificationGraphState)
        builder.add_node("collect_web_evidence", self._collect_web_evidence)
        builder.add_node("call_primary_model", self._call_primary_model)
        builder.add_node("call_fallback_model", self._call_fallback_model)
        builder.add_node("normalize_result", self._normalize_result)
        builder.add_edge(START, "collect_web_evidence")
        builder.add_conditional_edges(
            "collect_web_evidence",
            self._route_after_evidence,
            {
                "call_primary_model": "call_primary_model",
                "normalize_result": "normalize_result",
            },
        )
        builder.add_conditional_edges(
            "call_primary_model",
            self._route_after_primary,
            {
                "call_fallback_model": "call_fallback_model",
                "normalize_result": "normalize_result",
            },
        )
        builder.add_edge("call_fallback_model", "normalize_result")
        builder.add_edge("normalize_result", END)
        return builder.compile(checkpointer=MemorySaver())

    def _collect_web_evidence(self, state: VerificationGraphState) -> VerificationGraphState:
        company = state["company"]
        web_evidence = self._web_evidence_service.collect(company.nombre_empresa, company.web_normalized)
        return {"web_evidence": web_evidence}

    def _route_after_evidence(self, state: VerificationGraphState) -> Literal["call_primary_model", "normalize_result"]:
        return "call_primary_model" if self._client.is_configured else "normalize_result"

    def _call_primary_model(self, state: VerificationGraphState) -> VerificationGraphState:
        company = state["company"]
        settings = state["settings"]
        web_evidence = state["web_evidence"]
        prompt = build_verification_prompt(company, web_evidence, enable_web_search=settings.enable_web_search)
        try:
            envelope = self._client.complete(
                model=settings.model,
                system_prompt=SYSTEM_PROMPT,
                user_prompt=prompt,
                temperature=settings.temperature,
                max_tokens=settings.max_tokens,
                enable_web_search=settings.enable_web_search,
                web_search_options=_build_web_search_options(settings),
            )
            return {"llm_envelope": envelope, "llm_error": None}
        except Exception as exc:  # noqa: BLE001
            return {"llm_envelope": None, "llm_error": str(exc)}

    def _route_after_primary(self, state: VerificationGraphState) -> Literal["call_fallback_model", "normalize_result"]:
        settings = state["settings"]
        envelope = state.get("llm_envelope")
        if envelope and envelope.parsed_json:
            return "normalize_result"
        if settings.fallback_model and settings.fallback_model != settings.model:
            return "call_fallback_model"
        return "normalize_result"

    def _call_fallback_model(self, state: VerificationGraphState) -> VerificationGraphState:
        company = state["company"]
        settings = state["settings"]
        web_evidence = state["web_evidence"]
        prompt = build_verification_prompt(company, web_evidence, enable_web_search=settings.enable_web_search)
        try:
            envelope = self._client.complete(
                model=str(settings.fallback_model),
                system_prompt=SYSTEM_PROMPT,
                user_prompt=prompt,
                temperature=settings.temperature,
                max_tokens=settings.max_tokens,
                enable_web_search=settings.enable_web_search,
                web_search_options=_build_web_search_options(settings),
            )
            return {"llm_envelope": envelope, "llm_error": None}
        except Exception as exc:  # noqa: BLE001
            return {"llm_envelope": None, "llm_error": str(exc)}

    def _normalize_result(self, state: VerificationGraphState) -> VerificationGraphState:
        company = state["company"]
        settings = state["settings"]
        web_evidence = state["web_evidence"]
        result = self._result_validation_service.normalize(
            company,
            web_evidence,
            state.get("llm_envelope"),
            manual_review_threshold=settings.manual_review_threshold,
            web_search_enabled=settings.enable_web_search,
        )
        llm_error = state.get("llm_error")
        if llm_error:
            result.banderas_rojas.append(f"LLM no disponible o respuesta inválida: {llm_error}")
            result.requiere_revision_manual = True
        return {"final_result": result}


class VerificationOrchestrator:
    """Executes the mandatory 7-step workflow for each company."""

    def __init__(self, api_key: str | None) -> None:
        self._api_key = api_key

    def process_batch(
        self,
        rows: list[CompanyInput],
        settings: AppSettings,
        *,
        log_callback: LogCallback | None = None,
    ) -> list[CompanyVerificationResult]:
        results: list[CompanyVerificationResult] = []
        for row in rows:
            if log_callback:
                log_callback(f"Verificando {row.nombre_empresa} ({row.web_normalized}) con LangGraph")
            result = self.process_company(row, settings=settings, log_callback=log_callback)
            results.append(result)
        return results

    def process_company(
        self,
        row: CompanyInput,
        *,
        settings: AppSettings,
        log_callback: LogCallback | None = None,
    ) -> CompanyVerificationResult:
        if log_callback:
            log_callback(f"Ejecutando workflow LangGraph para {row.nombre_empresa}")
            if settings.enable_web_search:
                log_callback("Web search de OpenRouter solicitado para esta ejecución.")
        payload = _verify_company_cached(row.model_dump(mode="json"), settings.model_dump(mode="json"), self._api_key)
        return CompanyVerificationResult.model_validate(payload)


def _build_web_search_options(settings: AppSettings) -> dict[str, Any]:
    return {
        "engine": settings.web_search_engine,
        "max_results": settings.web_search_max_results,
        "max_total_results": settings.web_search_max_total_results,
        "search_context_size": settings.web_search_context_size,
        "allowed_domains": settings.web_search_allowed_domains,
        "excluded_domains": settings.web_search_excluded_domains,
    }
