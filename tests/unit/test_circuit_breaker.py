from __future__ import annotations

from app.agent.circuit_breaker import LLMCircuitBreaker


def test_closed_allows_and_success_closes():
    b = LLMCircuitBreaker(fail_threshold=2, cooldown_s=60, max_retries=1)
    assert b.allow_request() is True
    b.record_success()
    assert b.state == "CLOSED"


def test_opens_after_threshold():
    b = LLMCircuitBreaker(fail_threshold=2, cooldown_s=60, max_retries=1)
    b.record_failure()
    assert b.state == "CLOSED"
    b.record_failure()
    assert b.state == "OPEN"
    assert b.allow_request() is False


def test_half_open_after_cooldown():
    b = LLMCircuitBreaker(fail_threshold=2, cooldown_s=0, max_retries=1)
    b.record_failure()
    b.record_failure()
    assert b.state == "OPEN"
    assert b.allow_request() is True
    assert b.state == "HALF-OPEN"


def test_failure_in_half_open_reopens():
    b = LLMCircuitBreaker(fail_threshold=2, cooldown_s=0, max_retries=1)
    b.record_failure()
    b.record_failure()
    assert b.allow_request() is True  # HALF-OPEN
    b.record_failure()
    assert b.state == "OPEN"
