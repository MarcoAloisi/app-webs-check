"""OpenRouter client built on LangChain chat models."""

from __future__ import annotations

import json
from typing import Any

from langchain_core.messages import HumanMessage, SystemMessage
from langchain_openai import ChatOpenAI

from company_verifier.config import DEFAULT_RETRY_ATTEMPTS, DEFAULT_TIMEOUT_SECONDS
from company_verifier.models import LlmEnvelope
from company_verifier.utils.retry import RetryableError, retry_with_backoff

OPENROUTER_BASE_URL = "https://openrouter.ai/api/v1"
DEFAULT_HEADERS = {
    "HTTP-Referer": "https://streamlit.io",
    "X-Title": "app-webs-check",
}


class OpenRouterClient:
    """LangChain chat client configured for OpenRouter."""

    def __init__(self, api_key: str | None) -> None:
        self._api_key = api_key

    @property
    def is_configured(self) -> bool:
        return bool(self._api_key)

    def complete(
        self,
        *,
        model: str,
        system_prompt: str,
        user_prompt: str,
        temperature: float,
        max_tokens: int,
        enable_web_search: bool = False,
        web_search_options: dict[str, Any] | None = None,
    ) -> LlmEnvelope:
        if not self._api_key:
            raise ValueError("OpenRouter API key no configurada.")

        def _request() -> LlmEnvelope:
            llm = ChatOpenAI(
                model=model,
                api_key=self._api_key,
                base_url=OPENROUTER_BASE_URL,
                temperature=temperature,
                max_completion_tokens=max_tokens,
                timeout=DEFAULT_TIMEOUT_SECONDS,
                default_headers=DEFAULT_HEADERS,
                extra_body=_build_extra_body(enable_web_search, web_search_options),
            )
            response = llm.invoke(
                [
                    SystemMessage(content=system_prompt),
                    HumanMessage(content=user_prompt),
                ],
                response_format={"type": "json_object"},
            )
            raw_content = _extract_message_content(response.content)
            parsed_json = _try_parse_json(raw_content)
            return LlmEnvelope(
                model=model,
                prompt=user_prompt,
                raw_response=raw_content,
                parsed_json=parsed_json,
                used_web_search=enable_web_search,
                provider_metadata=getattr(response, "response_metadata", {}) or {},
            )

        return retry_with_backoff(_request, max_attempts=DEFAULT_RETRY_ATTEMPTS, retryable_exceptions=(RetryableError, Exception))


def _build_extra_body(enable_web_search: bool, web_search_options: dict[str, Any] | None = None) -> dict[str, Any]:
    if not enable_web_search:
        return {}
    tool_payload: dict[str, Any] = {"type": "openrouter:web_search"}
    parameters = {
        key: value
        for key, value in (web_search_options or {}).items()
        if value not in (None, "", [], {})
    }
    if parameters:
        tool_payload["parameters"] = parameters
    return {
        "tools": [tool_payload],
    }


def _extract_message_content(content: Any) -> str:
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts: list[str] = []
        for item in content:
            if isinstance(item, str):
                parts.append(item)
            elif isinstance(item, dict):
                if item.get("type") == "text":
                    parts.append(str(item.get("text", "")))
                elif "content" in item:
                    parts.append(str(item.get("content", "")))
        return "\n".join(parts)
    return json.dumps(content, ensure_ascii=False)



def _try_parse_json(raw: str) -> dict[str, Any] | None:
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        pass
    start = raw.find("{")
    end = raw.rfind("}")
    if start >= 0 and end > start:
        try:
            return json.loads(raw[start : end + 1])
        except json.JSONDecodeError:
            return None
    return None
