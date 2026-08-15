"""Configuración del agente copiloto.

Re-exporta desde app.settings (H-008): todas las env vars del proyecto se
centralizan en app/settings.py, este módulo es la puerta de entrada para el
paquete app/agent.
"""

from __future__ import annotations

from app.settings import (
    AGENT_LLM_API_KEY as LLM_API_KEY,
    AGENT_LLM_BASE_URL as LLM_BASE_URL,
    AGENT_LLM_MODEL as LLM_MODEL,
    AGENT_LLM_TIMEOUT_S as LLM_TIMEOUT_S,
    AGENT_MAX_CONCURRENCY as MAX_CONCURRENCY,
    AGENT_MAX_RETRIES as MAX_RETRIES,
    AGENT_RETRY_BASE_S as RETRY_BASE_S,
    AGENT_RETRY_JITTER as RETRY_JITTER,
    AGENT_RETRY_MAX_S as RETRY_MAX_S,
)
