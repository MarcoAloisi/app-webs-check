from __future__ import annotations

import streamlit as st

from company_verifier.config import DEFAULT_MODEL_OPTIONS, MAX_PARALLEL_WORKERS, MIN_PARALLEL_WORKERS, get_model_capability
from company_verifier.models import AppSettings
from company_verifier.session import get_settings, update_settings


def _has_api_key() -> bool:
    try:
        return bool(st.secrets.get("OPENROUTER_API_KEY", ""))
    except Exception:  # noqa: BLE001
        return False


st.subheader("Configuración de modelos y ejecución")
settings = get_settings()
model_choices = [*DEFAULT_MODEL_OPTIONS, "Otro (ID manual)"]
selected_model_choice = settings.model if settings.model in DEFAULT_MODEL_OPTIONS else "Otro (ID manual)"
selected_fallback_choice = settings.fallback_model if settings.fallback_model in DEFAULT_MODEL_OPTIONS else ("Otro (ID manual)" if settings.fallback_model else "")

with st.form("settings_form"):
    model_choice = st.selectbox("Modelo principal", options=model_choices, index=model_choices.index(selected_model_choice))
    custom_model = st.text_input("ID manual modelo principal", value="" if settings.model in DEFAULT_MODEL_OPTIONS else settings.model, placeholder="Ej. x-ai/grok-3-mini o cualquier ID de OpenRouter")
    fallback = st.selectbox("Modelo fallback", options=["", *model_choices], index=(["", *model_choices].index(selected_fallback_choice) if selected_fallback_choice in ["", *model_choices] else 0))
    custom_fallback = st.text_input("ID manual modelo fallback", value="" if not settings.fallback_model or settings.fallback_model in DEFAULT_MODEL_OPTIONS else settings.fallback_model, placeholder="Opcional")
    temperature = st.slider("Temperatura", min_value=0.0, max_value=1.0, value=float(settings.temperature), step=0.05)
    max_tokens = st.slider("Máx. tokens", min_value=500, max_value=4000, value=int(settings.max_tokens), step=100)
    batch_size = st.slider("Tamaño de batch", min_value=30, max_value=50, value=int(settings.batch_size), step=1)
    parallel_workers = st.slider(
        "Filas en paralelo",
        min_value=MIN_PARALLEL_WORKERS,
        max_value=MAX_PARALLEL_WORKERS,
        value=int(settings.parallel_workers),
        step=1,
        help="Cantidad máxima de empresas procesándose al mismo tiempo.",
    )
    manual_threshold = st.slider("Umbral revisión manual", min_value=1, max_value=100, value=int(settings.manual_review_threshold), step=1)
    checkpoint_interval = st.number_input("Intervalo de checkpoint", min_value=100, max_value=2000, value=int(settings.checkpoint_interval), step=100)
    resolved_model = custom_model.strip() if model_choice == "Otro (ID manual)" else model_choice
    model_capability = get_model_capability(resolved_model) if resolved_model else get_model_capability(settings.model)
    default_web_search = bool(model_capability.supports_web_search)
    enable_web_search = st.checkbox(
        "Habilitar OpenRouter web search",
        value=settings.enable_web_search,
        disabled=not default_web_search,
        help="Usa la herramienta server-side openrouter:web_search. En Grok y otros modelos compatibles puede aprovechar búsqueda nativa o fallback según el engine.",
    )
    web_engine = st.selectbox("Engine web search", options=["auto", "native", "exa", "firecrawl", "parallel"], index=["auto", "native", "exa", "firecrawl", "parallel"].index(settings.web_search_engine if settings.web_search_engine in {"auto", "native", "exa", "firecrawl", "parallel"} else "auto"), disabled=not enable_web_search)
    web_max_results = st.slider("Max results por búsqueda", min_value=1, max_value=25, value=int(settings.web_search_max_results), disabled=not enable_web_search)
    web_max_total_results = st.number_input("Max total results", min_value=0, max_value=200, value=int(settings.web_search_max_total_results or 0), step=1, disabled=not enable_web_search, help="0 = sin límite explícito")
    web_context_size = st.selectbox("Search context size", options=["low", "medium", "high"], index=["low", "medium", "high"].index(settings.web_search_context_size if settings.web_search_context_size in {"low", "medium", "high"} else "medium"), disabled=not enable_web_search)
    allowed_domains = st.text_input("Allowed domains", value=", ".join(settings.web_search_allowed_domains), disabled=not enable_web_search, help="Separados por coma")
    excluded_domains = st.text_input("Excluded domains", value=", ".join(settings.web_search_excluded_domains), disabled=not enable_web_search, help="Separados por coma")
    submitted = st.form_submit_button("Guardar configuración", type="primary")

if submitted:
    final_model = custom_model.strip() if model_choice == "Otro (ID manual)" else model_choice
    fallback_model = None
    if fallback == "Otro (ID manual)":
        fallback_model = custom_fallback.strip() or None
    elif fallback:
        fallback_model = fallback
    updated = AppSettings(
        model=final_model,
        fallback_model=fallback_model,
        temperature=temperature,
        max_tokens=max_tokens,
        batch_size=batch_size,
        parallel_workers=parallel_workers,
        manual_review_threshold=manual_threshold,
        enable_web_search=enable_web_search if default_web_search else False,
        web_search_engine=web_engine,
        web_search_max_results=web_max_results,
        web_search_max_total_results=int(web_max_total_results) or None,
        web_search_context_size=web_context_size,
        web_search_allowed_domains=[item.strip() for item in allowed_domains.split(",") if item.strip()],
        web_search_excluded_domains=[item.strip() for item in excluded_domains.split(",") if item.strip()],
        checkpoint_interval=int(checkpoint_interval),
    )
    update_settings(updated)
    st.success("Configuración guardada en sesión.")
    settings = updated

status = "detectada" if _has_api_key() else "no detectada"
st.info(f"API key OpenRouter en secrets: {status}.")

capability = get_model_capability(settings.model)
col1, col2 = st.columns(2)
with col1:
    st.metric("Web search soportado", "Sí" if capability and capability.supports_web_search else "No")
with col2:
    st.metric("JSON mode estimado", "Sí" if capability and capability.supports_json_mode else "No")

st.caption("Los modelos Grok se pueden usar con su ID de OpenRouter. Si el modelo no aporta evidencia auditable, la app degrada el resultado a no verificable y fuerza revisión manual.")
