# app-webs-check

Aplicación Streamlit para verificar masivamente empresas a partir de un CSV y generar una salida auditable, compatible con Streamlit Community Cloud.

## Qué incluye esta primera implementación

- Carga y validación previa de CSV con columnas `nombre_empresa` y `web`.
- Normalización de dominios, detección de duplicados y URLs malformadas.
- Procesamiento por batches de 30 a 50 registros.
- Integración con OpenRouter mediante `st.secrets`, usando LangChain y LangGraph.
- Workflow obligatorio de 7 pasos por empresa con salida JSON estructurada.
- Fallback conservador si el LLM falla o no hay API key.
- Progreso, ETA, logs, filtros, auditoría por empresa y exportación CSV/Excel/JSONL.
- Checkpoint descargable en CSV y JSON para reanudar subiendo el mismo archivo.
- Orquestación por grafo con LangGraph, con nodo de evidencia web, nodo de modelo principal, nodo fallback y normalización final.

## Estructura

- [streamlit_app.py](streamlit_app.py): entrada principal y navegación.
- [app_pages/upload_run.py](app_pages/upload_run.py): carga, validación, ejecución y checkpoint.
- [app_pages/results.py](app_pages/results.py): tabla, filtros y exportaciones.
- [app_pages/audit.py](app_pages/audit.py): trazabilidad por empresa.
- [app_pages/settings.py](app_pages/settings.py): modelos y parámetros.
- [company_verifier/](company_verifier): modelos, servicios, utilidades y almacenamiento de checkpoint.

## Arquitectura LLM

- `LangGraph` coordina el flujo por empresa.
- `LangChain` ejecuta el modelo contra OpenRouter usando su endpoint compatible con OpenAI.
- Cuando se activa búsqueda web, la integración envía una petición de server-side web search a OpenRouter desde la capa de cliente.
- Si el modelo o gateway no devuelve JSON auditable, la app cae al modo conservador y fuerza revisión manual.

## Desarrollo local

1. Crear entorno virtual.
1. Instalar dependencias desde `pyproject.toml`.
1. Crear `.streamlit/secrets.toml` con la clave de OpenRouter:

```toml
OPENROUTER_API_KEY = "tu_api_key"
```

1. Ejecutar la app:

```bash
streamlit run streamlit_app.py
```

## Despliegue en Streamlit Community Cloud

- Sube el repo a GitHub.
- Configura el archivo principal como [streamlit_app.py](streamlit_app.py).
- Añade `OPENROUTER_API_KEY` en la sección de Secrets.
- La app está pensada para ejecutar por tramos y descargar checkpoints periódicamente.

## Limitaciones conocidas de esta fase

- WHOIS/RDAP queda en modo best-effort y actualmente se marca como no verificable si no hay evidencia fiable.
- La búsqueda externa depende de que el modelo seleccionado soporte web search real vía OpenRouter.
- El payload de web search está aislado en [company_verifier/services/openrouter_client.py](company_verifier/services/openrouter_client.py) para poder ajustarlo rápidamente si OpenRouter cambia su contrato.
- En Streamlit Cloud no hay persistencia durable del servidor; la reanudación robusta se hace re-subiendo el checkpoint descargado.
- El checkpoint se genera automáticamente en memoria, pero la descarga sigue requiriendo acción del usuario en el navegador.
