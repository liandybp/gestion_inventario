"""Circuit breaker del copiloto (H-030).

Protege al LLM: tras N fallos consecutivos abre (OPEN) por un cooldown; las
consultas degradan sin llamar al LLM; tras el cooldown una sonda (HALF-OPEN)
decide si vuelve a CLOSED o reabre. Thread-safe (se usa desde el threadpool).
"""
from __future__ import annotations

import threading
import time

from app import settings

_log = settings  # noop: mantiene import; el log se usa en graph.py


class LLMCircuitBreaker:
    def __init__(self, fail_threshold: int, cooldown_s: float, max_retries: int) -> None:
        self._fail_threshold = fail_threshold
        self._cooldown_s = cooldown_s
        self._max_retries = max_retries
        self._lock = threading.Lock()
        self._state = "CLOSED"  # CLOSED | OPEN | HALF-OPEN
        self._failures = 0
        self._opened_at: float | None = None

    @property
    def state(self) -> str:
        with self._lock:
            return self._state

    def allow_request(self) -> bool:
        with self._lock:
            if self._state == "OPEN":
                if self._opened_at is not None and time.monotonic() - self._opened_at >= self._cooldown_s:
                    self._state = "HALF-OPEN"
                    return True
                return False
            return True  # CLOSED y HALF-OPEN permiten 1 sonda

    def record_success(self) -> None:
        with self._lock:
            self._state = "CLOSED"
            self._failures = 0

    def record_failure(self) -> None:
        with self._lock:
            self._failures += 1
            if self._state == "HALF-OPEN" or self._failures >= self._fail_threshold:
                self._state = "OPEN"
                self._opened_at = time.monotonic()


breaker = LLMCircuitBreaker(
    fail_threshold=settings.AGENT_BREAKER_FAIL_THRESHOLD,
    cooldown_s=settings.AGENT_BREAKER_COOLDOWN_S,
    max_retries=settings.AGENT_BREAKER_MAX_RETRIES,
)
