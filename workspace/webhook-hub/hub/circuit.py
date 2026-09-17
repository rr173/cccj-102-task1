"""Per-endpoint circuit breaker / backoff.

Each endpoint owns one instance, so a sick receiver only pauses its own
pipeline — other tenants and endpoints are never touched by it.
"""

import random
import threading
import time


class CircuitBreaker:
    def __init__(self, base_backoff_ms: int, max_backoff_ms: int):
        self._base = base_backoff_ms / 1000.0
        self._max = max_backoff_ms / 1000.0
        self._lock = threading.Lock()
        self.failures = 0
        self.open_until = 0.0
        self.last_error = None

    def on_success(self):
        with self._lock:
            self.failures = 0
            self.open_until = 0.0
            self.last_error = None

    def on_failure(self, retry_after=None, error=None) -> float:
        """Record a failure and return the backoff (seconds) to wait."""
        with self._lock:
            self.failures += 1
            backoff = min(self._max, self._base * (2 ** (self.failures - 1)))
            backoff *= random.uniform(0.5, 1.5)  # jitter against thundering herd
            if retry_after is not None:
                backoff = max(backoff, float(retry_after))  # honor 429/503 Retry-After
            self.open_until = time.time() + backoff
            self.last_error = error
            return backoff

    def snapshot(self) -> dict:
        with self._lock:
            return {
                "failures": self.failures,
                "open": self.open_until > time.time(),
                "open_until": self.open_until or None,
                "last_error": self.last_error,
            }
