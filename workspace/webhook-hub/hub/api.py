"""HTTP control-plane API (stdlib http.server, no dependencies)."""

import json
import re
import traceback
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from .errors import HubError, NotFound


class _Router:
    def __init__(self, hub):
        self.hub = hub
        self.routes = [
            ("POST", r"^/v1/tenants$", self.create_tenant),
            ("POST", r"^/v1/tenants/(?P<tid>[^/]+)/endpoints$", self.create_endpoint),
            ("GET", r"^/v1/endpoints/(?P<eid>[^/]+)$", self.get_endpoint),
            ("PATCH", r"^/v1/endpoints/(?P<eid>[^/]+)$", self.patch_endpoint),
            ("POST", r"^/v1/endpoints/(?P<eid>[^/]+)/keys/rotate$", self.rotate_key),
            ("GET", r"^/v1/endpoints/(?P<eid>[^/]+)/keys$", self.list_keys),
            ("POST", r"^/v1/endpoints/(?P<eid>[^/]+)/events$", self.enqueue),
            ("GET", r"^/v1/endpoints/(?P<eid>[^/]+)/events$", self.list_events),
            ("GET", r"^/v1/events/(?P<eid>[^/]+)$", self.get_event),
            ("POST", r"^/v1/events/(?P<eid>[^/]+)/replay$", self.replay),
            ("GET", r"^/healthz$", lambda body: (200, {"ok": True})),
        ]
        self.routes = [(m, re.compile(p), fn) for m, p, fn in self.routes]

    def dispatch(self, method: str, path: str, body: dict):
        for m, pattern, fn in self.routes:
            if m != method:
                continue
            match = pattern.match(path)
            if match:
                return fn(body, **match.groupdict())
        raise NotFound(f"no route for {method} {path}")

    # ---------------------------------------------------------- handlers
    def create_tenant(self, body):
        return 201, self.hub.create_tenant(body.get("name"))

    def create_endpoint(self, body, tid):
        return 201, self.hub.create_endpoint(tid, body.get("url"), body.get("max_concurrency"))

    def get_endpoint(self, body, eid):
        return 200, self.hub.get_endpoint(eid)

    def patch_endpoint(self, body, eid):
        return 200, self.hub.patch_endpoint(eid, body.get("max_concurrency"), body.get("status"))

    def rotate_key(self, body, eid):
        return 200, self.hub.rotate_key(eid)

    def list_keys(self, body, eid):
        return 200, {"keys": self.hub.list_keys(eid)}

    def enqueue(self, body, eid):
        event, created = self.hub.enqueue(
            eid,
            object_key=body.get("object_key"),
            event_type=body.get("type"),
            payload=body.get("payload"),
            event_id=body.get("id"),
            idempotency_key=body.get("idempotency_key"),
            max_attempts=body.get("max_attempts"),
        )
        return (201 if created else 200), {"event": event, "deduplicated": not created}

    def list_events(self, body, eid):
        return 200, {"events": self.hub.list_events(eid, body.get("status"), body.get("limit", 100))}

    def get_event(self, body, eid):
        return 200, self.hub.get_event(eid)

    def replay(self, body, eid):
        return 200, self.hub.replay(eid)


def make_server(hub, host: str, port: int) -> ThreadingHTTPServer:
    router = _Router(hub)

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, *args):
            pass

        def _handle(self, method):
            try:
                length = int(self.headers.get("Content-Length") or 0)
                raw = self.rfile.read(length) if length else b""
                body = json.loads(raw) if raw else {}
                if not isinstance(body, dict):
                    raise ValueError
            except (ValueError, json.JSONDecodeError):
                return self._send(400, {"error": {"code": "bad_request", "message": "invalid JSON body"}})
            try:
                status, payload = router.dispatch(method, self.path.split("?")[0], body)
            except HubError as exc:
                status, payload = exc.status, {"error": {"code": exc.code, "message": exc.message}}
            except Exception as exc:  # noqa: BLE001 - last-resort guard
                traceback.print_exc()
                status, payload = 500, {"error": {"code": "internal", "message": str(exc)}}
            self._send(status, payload)

        def _send(self, status, payload):
            data = json.dumps(payload).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        do_GET = lambda self: self._handle("GET")  # noqa: E731
        do_POST = lambda self: self._handle("POST")  # noqa: E731
        do_PATCH = lambda self: self._handle("PATCH")  # noqa: E731

    server = ThreadingHTTPServer((host, port), Handler)
    server.daemon_threads = True
    return server


def serve(hub, host: str, port: int):
    server = make_server(hub, host, port)
    print(f"[hub] control plane listening on http://{host}:{port}", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
        hub.close()
