"""Application configuration defaults."""

from __future__ import annotations

from dataclasses import dataclass

APP_TITLE = "Verificador masivo de empresas"
DEFAULT_MODEL = "openai/gpt-4o-mini"
DEFAULT_FALLBACK_MODEL = "anthropic/claude-3.5-haiku"
DEFAULT_TEMPERATURE = 0.2
DEFAULT_MAX_TOKENS = 1800
DEFAULT_BATCH_SIZE = 30
MIN_BATCH_SIZE = 30
MAX_BATCH_SIZE = 50
CHECKPOINT_INTERVAL = 500
DEFAULT_MANUAL_REVIEW_THRESHOLD = 70
DEFAULT_TIMEOUT_SECONDS = 35
DEFAULT_RETRY_ATTEMPTS = 4
USER_AGENT = "app-webs-check/0.1 (+Streamlit Community Cloud)"

DEFAULT_MODEL_OPTIONS = [
    "openai/gpt-4o-mini",
    "anthropic/claude-3.5-haiku",
    "google/gemini-2.0-flash-001",
    "meta-llama/llama-3.3-70b-instruct",
    "x-ai/grok-3-mini",
]


@dataclass(frozen=True)
class CapabilityProfile:
    """Static model capability hints used by the UI."""

    supports_web_search: bool
    supports_json_mode: bool


MODEL_CAPABILITIES: dict[str, CapabilityProfile] = {
    "openai/gpt-4o-mini": CapabilityProfile(supports_web_search=False, supports_json_mode=True),
    "anthropic/claude-3.5-haiku": CapabilityProfile(supports_web_search=False, supports_json_mode=False),
    "google/gemini-2.0-flash-001": CapabilityProfile(supports_web_search=True, supports_json_mode=True),
    "meta-llama/llama-3.3-70b-instruct": CapabilityProfile(supports_web_search=False, supports_json_mode=False),
    "x-ai/grok-3-mini": CapabilityProfile(supports_web_search=True, supports_json_mode=True),
}


def get_model_capability(model: str) -> CapabilityProfile:
    """Return static capability hints for known and pattern-matched models."""
    if model in MODEL_CAPABILITIES:
        return MODEL_CAPABILITIES[model]
    lowered = model.lower()
    if lowered.startswith("x-ai/grok"):
        return CapabilityProfile(supports_web_search=True, supports_json_mode=True)
    if lowered.startswith("openai/"):
        return CapabilityProfile(supports_web_search=False, supports_json_mode=True)
    if lowered.startswith("google/") or lowered.startswith("gemini"):
        return CapabilityProfile(supports_web_search=True, supports_json_mode=True)
    return CapabilityProfile(supports_web_search=False, supports_json_mode=False)
