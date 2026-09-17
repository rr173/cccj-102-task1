"""HTTP delivery: signs and POSTs one event to an endpoint URL."""

import http.client
import time
from urllib.parse import urlparse

from . import signing


class DeliveryResult:
    def __init__(self, http_status=None, error=None, retry_after=None, duration_ms=0.0):
        self.http_status = http_status
        self.error = error
        self.retry_after = retry_after
        self.duration_ms = duration_ms

    @property
    def ok(self) -> bool:
        return self.http_status is not None and 200 <= self.http_status < 300


def _parse_retry_after(value):
    if value is None:
        return None
    try:
        return max(0.0, float(value))
    except ValueError:
        return None  # HTTP-date form not needed for the demo scope


class Deliverer:
    def __init__(self, timeout_ms: int):
        self.timeout = timeout_ms / 1000.0

    def send(self, url: str, secret: str, key_version: int, event: dict) -> DeliveryResult:
        body = event["payload"].encode("utf-8")  # exact bytes that get signed
        ts = int(time.time())
        headers = {
            "Content-Type": "application/json",
            "User-Agent": "webhook-hub/1.0",
            "X-Webhook-Id": event["id"],
            "Idempotency-Key": event["id"],
            "X-Webhook-Event": event["event_type"],
            "X-Webhook-Object-Key": event["object_key"],
            "X-Webhook-Timestamp": str(ts),
            "X-Webhook-Key-Version": str(key_version),
            "X-Webhook-Signature": f"v{key_version}=" + signing.sign(secret, ts, body),
        }
        parsed = urlparse(url)
        conn_cls = http.client.HTTPSConnection if parsed.scheme == "https" else http.client.HTTPConnection
        path = parsed.path or "/"
        if parsed.query:
            path += "?" + parsed.query
        conn = conn_cls(parsed.hostname, parsed.port, timeout=self.timeout)
        start = time.monotonic()
        try:
            conn.request("POST", path, body=body, headers=headers)
            resp = conn.getresponse()
            resp.read()
            retry_after = None
            if resp.status in (429, 503):
                retry_after = _parse_retry_after(resp.getheader("Retry-After"))
            return DeliveryResult(
                http_status=resp.status,
                retry_after=retry_after,
                duration_ms=(time.monotonic() - start) * 1000,
            )
        except (OSError, http.client.HTTPException) as exc:
            # timeout, connection refused/reset, receiver dropped the connection...
            return DeliveryResult(
                error=f"{type(exc).__name__}: {exc}",
                duration_ms=(time.monotonic() - start) * 1000,
            )
        finally:
            conn.close()
