"""Configuración centralizada del proyecto (env vars).

H-008: consolida los `os.getenv` dispersos (agente, db, logger, rate limits) en
un solo punto. Cualquier env var nueva se declara aquí.

Nota sobre el modelo del copiloto (dev vs prod):
- **Dev (default):** modelo local chico vía llama-swap — gratis, privado, ~5GB
  (Qwen3-8B-Instruct, cabe en RAM/VRAM sin OOM del navegador).
  AGENT_LLM_BASE_URL=http://localhost:8080/v1 · AGENT_LLM_MODEL=qwen3_8b_instruct
- **Producción:** API LLM externa (OpenAI-compatible) vía env vars del docker-compose.
  AGENT_LLM_BASE_URL=https://api.openai.com/v1 (o Gemini) · AGENT_LLM_MODEL=gpt-4o-mini · AGENT_LLM_API_KEY=...
"""

from __future__ import annotations

import os


def env(name: str, default: str) -> str:
    value = os.getenv(name)
    return value if value not in (None, "") else default


# ── Copiloto / LLM ────────────────────────────────────────────────────────────
AGENT_LLM_BASE_URL = env("AGENT_LLM_BASE_URL", "http://localhost:8080/v1")  # dev: llama-swap local
AGENT_LLM_MODEL = env("AGENT_LLM_MODEL", "qwen3_8b_instruct")  # dev: modelo chico
AGENT_LLM_API_KEY = env("AGENT_LLM_API_KEY", "")  # llama-swap no exige key; API sí
AGENT_LLM_TIMEOUT_S = float(env("AGENT_LLM_TIMEOUT_S", "60"))
AGENT_MAX_CONCURRENCY = int(env("AGENT_MAX_CONCURRENCY", "8"))
AGENT_MAX_RETRIES = int(env("AGENT_MAX_RETRIES", "2"))
AGENT_MAX_WIDGETS = int(env("AGENT_MAX_WIDGETS", "20"))
AGENT_MAX_MESSAGES = int(env("AGENT_MAX_MESSAGES", "100"))
AGENT_RATE_LIMIT = int(env("AGENT_RATE_LIMIT", "30"))
AGENT_RATE_WINDOW = int(env("AGENT_RATE_WINDOW", "60"))
