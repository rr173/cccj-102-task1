"""Hub facade: validation + wiring between store, dispatcher and deliverer."""

import json
import re

from .deliverer import Deliverer
from .dispatcher import Dispatcher
from .errors import BadRequest
from .store import Store

_URL_RE = re.compile(r"^https?://[^\s/]+(?::\d+)?(/|$)")


class Hub:
    def __init__(self, cfg):
        self.cfg = cfg
        self.store = Store(cfg.db_path)
        self.deliverer = Deliverer(cfg.delivery_timeout_ms)
        self.dispatcher = Dispatcher(self)
        # resume scheduling for endpoints persisted before a restart
        for ep in self.store.list_endpoints():
            self.dispatcher.ensure_runner(ep["id"])

    def close(self):
        self.dispatcher.shutdown()
        self.store.close()

    # ---------------------------------------------------------- tenants
    def create_tenant(self, name: str) -> dict:
        if not name or not isinstance(name, str):
            raise BadRequest("name is required")
        return self.store.create_tenant(name)

    # ---------------------------------------------------------- endpoints
    def create_endpoint(self, tenant_id: str, url: str, max_concurrency: int = None) -> dict:
        if not url or not _URL_RE.match(url):
            raise BadRequest("url must be a valid http(s) URL")
        limit = max_concurrency or self.cfg.default_max_concurrency
        if not 1 <= limit <= 1000:
            raise BadRequest("max_concurrency must be between 1 and 1000")
        endpoint, secret = self.store.create_endpoint(tenant_id, url, limit)
        self.dispatcher.ensure_runner(endpoint["id"])
        endpoint["signing_secret"] = secret  # shown once at creation
        return endpoint

    def get_endpoint(self, endpoint_id: str) -> dict:
        ep = self.store.get_endpoint(endpoint_id)
        ep["circuit"] = self.dispatcher.circuit_state(endpoint_id)
        ep["queue"] = self.store.queue_stats(endpoint_id)
        return ep

    def patch_endpoint(self, endpoint_id: str, max_concurrency=None, status=None) -> dict:
        if max_concurrency is not None and not 1 <= max_concurrency <= 1000:
            raise BadRequest("max_concurrency must be between 1 and 1000")
        if status is not None and status not in ("active", "disabled"):
            raise BadRequest("status must be 'active' or 'disabled'")
        ep = self.store.update_endpoint(endpoint_id, max_concurrency, status)
        if max_concurrency is not None:
            self.dispatcher.update_limit(endpoint_id, max_concurrency)
        self.dispatcher.notify(endpoint_id)
        return ep

    def rotate_key(self, endpoint_id: str) -> dict:
        version, secret = self.store.rotate_key(endpoint_id)
        return {"endpoint_id": endpoint_id, "active_key_version": version, "signing_secret": secret}

    def list_keys(self, endpoint_id: str) -> list:
        return self.store.list_keys(endpoint_id)

    # ---------------------------------------------------------- events
    def enqueue(
        self,
        endpoint_id: str,
        object_key: str,
        event_type: str = None,
        payload=None,
        event_id: str = None,
        idempotency_key: str = None,
        max_attempts: int = None,
    ):
        if not object_key or not isinstance(object_key, str):
            raise BadRequest("object_key is required")
        try:
            payload_str = json.dumps(payload if payload is not None else {}, separators=(",", ":"))
        except (TypeError, ValueError):
            raise BadRequest("payload must be JSON-serializable")
        event, created = self.store.enqueue_event(
            endpoint_id=endpoint_id,
            object_key=object_key,
            event_type=event_type or "event",
            payload=payload_str,
            event_id=event_id,
            idempotency_key=idempotency_key,
            max_attempts=max_attempts or self.cfg.max_attempts,
        )
        if created:
            self.dispatcher.notify(endpoint_id)
        return self._public_event(event), created

    def get_event(self, event_id: str) -> dict:
        event = self._public_event(self.store.get_event(event_id))
        event["attempts_log"] = self.store.list_attempts(event_id)
        return event

    def list_events(self, endpoint_id: str, status: str = None, limit: int = 100) -> list:
        self.store.get_endpoint(endpoint_id)
        return [self._public_event(e) for e in self.store.list_events(endpoint_id, status, limit)]

    def replay(self, event_id: str) -> dict:
        event, requeued, receipt = self.store.replay(event_id)
        if requeued:
            self.dispatcher.notify(event["endpoint_id"])
        return {
            "event": self._public_event(event),
            "replayed": requeued,
            "receipt": receipt,
            "reason": None if requeued else "already_delivered",
        }

    @staticmethod
    def _public_event(event: dict) -> dict:
        event = dict(event)
        event["payload"] = json.loads(event["payload"])
        return event
