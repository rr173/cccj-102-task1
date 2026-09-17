import os
from dataclasses import dataclass


def _int(name, default):
    return int(os.environ.get(name, default))


def _str(name, default):
    return os.environ.get(name, default)


@dataclass
class Config:
    host: str = "127.0.0.1"
    port: int = 8080
    db_path: str = os.path.join("data", "hub.db")
    delivery_timeout_ms: int = 5000
    base_backoff_ms: int = 500
    max_backoff_ms: int = 30000
    max_attempts: int = 8
    default_max_concurrency: int = 4

    @classmethod
    def from_env(cls):
        return cls(
            host=_str("HUB_HOST", cls.host),
            port=_int("HUB_PORT", cls.port),
            db_path=_str("HUB_DB", cls.db_path),
            delivery_timeout_ms=_int("HUB_DELIVERY_TIMEOUT_MS", cls.delivery_timeout_ms),
            base_backoff_ms=_int("HUB_BASE_BACKOFF_MS", cls.base_backoff_ms),
            max_backoff_ms=_int("HUB_MAX_BACKOFF_MS", cls.max_backoff_ms),
            max_attempts=_int("HUB_MAX_ATTEMPTS", cls.max_attempts),
            default_max_concurrency=_int("HUB_DEFAULT_MAX_CONCURRENCY", cls.default_max_concurrency),
        )
