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

try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass


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
AGENT_RETRY_BASE_S = float(env("AGENT_RETRY_BASE_S", "1.0"))
AGENT_RETRY_MAX_S = float(env("AGENT_RETRY_MAX_S", "5.0"))
AGENT_RETRY_JITTER = float(env("AGENT_RETRY_JITTER", "0.5"))

# ── Circuit breaker del copiloto (H-030) ──────────────────────────────────────
AGENT_BREAKER_FAIL_THRESHOLD = int(env("AGENT_BREAKER_FAIL_THRESHOLD", "3"))
AGENT_BREAKER_COOLDOWN_S = float(env("AGENT_BREAKER_COOLDOWN_S", "60"))
AGENT_BREAKER_MAX_RETRIES = int(env("AGENT_BREAKER_MAX_RETRIES", "1"))

# ── Comunes (H-045: consolidar os.getenv dispersos) ──────────────────────────
DATABASE_URL = env("DATABASE_URL", "sqlite+pysqlite:///./inventario.db")
DB_POOL_SIZE = int(env("DB_POOL_SIZE", "10"))
DB_MAX_OVERFLOW = int(env("DB_MAX_OVERFLOW", "20"))
SESSION_SECRET = env("SESSION_SECRET", "")
SESSION_HTTPS_ONLY = env("SESSION_HTTPS_ONLY", "0") == "1"
HOST = env("HOST", "127.0.0.1")
PORT = int(env("PORT", "10000"))
RELOAD = env("RELOAD", "1") == "1"
LOG_LEVEL = env("LOG_LEVEL", "INFO").upper()
BUSINESS_CONFIG_PATH = env("BUSINESS_CONFIG_PATH", "")
BUSINESS_CONFIG_DIR = env("BUSINESS_CONFIG_DIR", "")
LOGIN_RATE_LIMIT = int(env("LOGIN_RATE_LIMIT", "10"))
LOGIN_RATE_WINDOW = int(env("LOGIN_RATE_WINDOW", "300"))
UPLOAD_MAX_MB = int(env("UPLOAD_MAX_MB", "10"))
DEV_ACTIONS_ENABLED = env("DEV_ACTIONS_ENABLED", "1") == "1"
INVENTORY_METRICS_TTL = int(env("INVENTORY_METRICS_TTL", "300"))

# ── Sync de escritorio (H-007: hardening del token de dispositivo) ───────────
DEVICE_RATE_LIMIT = int(env("DEVICE_RATE_LIMIT", "20"))
DEVICE_RATE_WINDOW = int(env("DEVICE_RATE_WINDOW", "300"))
DEVICE_TOKEN_MAX_INACTIVITY_DAYS = int(env("DEVICE_TOKEN_MAX_INACTIVITY_DAYS", "90"))
