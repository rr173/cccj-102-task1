"""Mock partner receiver, used by the demo and the e2e tests.

Webhook behavior is remotely controllable per path, so failure scenarios
(429 rate limiting, 500s, timeouts, connection drops) can be scripted:

    POST /__config   {"mode": "ok", "latency_ms": 0, "retry_after": 1,
                      "path_modes": {"/sick": "flaky"},
                      "secrets": {"/a": {"1": "<hex>", "2": "<hex>"}}}
    GET  /__receipts           processed deliveries, in arrival order
    GET  /__stats              counters incl. max observed concurrency
    GET  /__order_violations   per-object_key ordering check on payload.n
    POST /__reset              clear receipts/counters (and config)

Modes: ok | slow | flaky (500) | limited (429 + Retry-After) | down (drop conn)

Idempotency: the receiver *reserves* X-Webhook-Id before processing, so a
retry that arrives while the first attempt is still in flight is deduplicated
instead of processed twice.
"""

import argparse
import json
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from . import signing


class ReceiverState:
    def __init__(self):
        self.lock = threading.Lock()
        self.mode = "ok"
        self.path_modes = {}
        self.latency_ms = 0
        self.retry_after = 1.0
        self.secrets = {}          # path -> {version(str): secret}
        self.seen = set()          # reserved X-Webhook-Id values
        self.receipts = []         # processed deliveries
        self.deduped = 0
        self.current = 0
        self.max_concurrent = 0

    def reset(self, keep_config=False):
        with self.lock:
            self.seen.clear()
            self.receipts.clear()
            self.deduped = 0
            self.current = 0
            self.max_concurrent = 0
            if not keep_config:
                self.mode = "ok"
                self.path_modes = {}
                self.latency_ms = 0
                self.retry_after = 1.0
                self.secrets = {}


def make_receiver_server(host="127.0.0.1", port=0, state=None):
    state = state or ReceiverState()

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, *args):
            pass

        # ---------------------------------------------------------- helpers
        def _json(self, status, obj, extra_headers=None):
            data = json.dumps(obj).encode("utf-8")
            try:
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(data)))
                for k, v in (extra_headers or {}).items():
                    self.send_header(k, v)
                self.end_headers()
                self.wfile.write(data)
            except OSError:
                # client may already be gone (e.g. delivery timed out while we
                # were processing) — the receipt was still recorded
                pass

        def _body(self):
            length = int(self.headers.get("Content-Length") or 0)
            return self.rfile.read(length) if length else b""

        # ------------------------------------------------------------- API
        def do_GET(self):
            path = self.path.split("?")[0]
            if path == "/__receipts":
                with state.lock:
                    return self._json(200, {"receipts": list(state.receipts)})
            if path == "/__stats":
                with state.lock:
                    return self._json(200, {
                        "received": len(state.receipts),
                        "deduped": state.deduped,
                        "current": state.current,
                        "max_concurrent": state.max_concurrent,
                    })
            if path == "/__order_violations":
                return self._json(200, {"violations": self._order_violations()})
            return self._json(404, {"error": "unknown route"})

        def do_POST(self):
            path = self.path.split("?")[0]
            if path == "/__config":
                body = json.loads(self._body() or b"{}")
                with state.lock:
                    for key in ("mode", "latency_ms", "retry_after"):
                        if key in body:
                            setattr(state, key, body[key])
                    if "path_modes" in body:
                        state.path_modes = dict(body["path_modes"])
                    if "secrets" in body:
                        for p, versions in body["secrets"].items():
                            state.secrets.setdefault(p, {}).update(versions)
                return self._json(200, {"ok": True})
            if path == "/__reset":
                body = json.loads(self._body() or b"{}")
                state.reset(keep_config=body.get("keep_config", False))
                return self._json(200, {"ok": True})
            return self._webhook(path)

        # ---------------------------------------------------------- webhook
        def _webhook(self, path):
            body = self._body()
            with state.lock:
                mode = state.path_modes.get(path, state.mode)
                latency = state.latency_ms
                retry_after = state.retry_after

            if mode == "down":
                # simulate the receiver being offline: drop the connection
                self.close_connection = True
                try:
                    self.connection.shutdown(2)  # SHUT_RDWR
                    self.connection.close()
                except OSError:
                    pass
                return
            if mode == "flaky":
                return self._json(500, {"error": "internal boom"})
            if mode == "limited":
                return self._json(429, {"error": "rate limited"},
                                  extra_headers={"Retry-After": str(retry_after)})

            # verify signature when secrets are configured for this path
            with state.lock:
                secrets_for_path = dict(state.secrets.get(path, {}))
            if secrets_for_path:
                version = self.headers.get("X-Webhook-Key-Version", "")
                ts = self.headers.get("X-Webhook-Timestamp", "0")
                signature = self.headers.get("X-Webhook-Signature", "")
                secret = secrets_for_path.get(version)
                expected = f"v{version}=" + signing.sign(secret, int(ts), body) if secret else ""
                if not secret or signature != expected:
                    return self._json(401, {"error": "bad signature"})

            # idempotency: reserve the event id *before* processing
            event_id = self.headers.get("X-Webhook-Id", "")
            with state.lock:
                if event_id in state.seen:
                    state.deduped += 1
                    return self._json(200, {"ok": True, "deduplicated": True})
                state.seen.add(event_id)
                state.current += 1
                state.max_concurrent = max(state.max_concurrent, state.current)
            try:
                if mode == "slow" or latency:
                    time.sleep(max(latency, 1) / 1000.0)
                try:
                    payload = json.loads(body) if body else {}
                except json.JSONDecodeError:
                    payload = {}
                receipt = {
                    "event_id": event_id,
                    "path": path,
                    "object_key": self.headers.get("X-Webhook-Object-Key"),
                    "key_version": int(self.headers.get("X-Webhook-Key-Version", "0")),
                    "n": payload.get("n"),
                    "received_at": time.time(),
                }
                with state.lock:
                    state.receipts.append(receipt)
            finally:
                with state.lock:
                    state.current -= 1
            return self._json(200, {"ok": True})

        def _order_violations(self):
            with state.lock:
                receipts = list(state.receipts)
            last_n = {}
            violations = []
            for r in receipts:
                key, n = r["object_key"], r["n"]
                if key is None or n is None:
                    continue
                if key in last_n and n <= last_n[key]:
                    violations.append({"object_key": key, "after": last_n[key], "got": n})
                last_n[key] = max(n, last_n.get(key, n))
            return violations

    server = ThreadingHTTPServer((host, port), Handler)
    server.daemon_threads = True
    server.state = state
    return server, state


def main():
    parser = argparse.ArgumentParser(prog="hub.receiver")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=9100)
    args = parser.parse_args()
    server, _ = make_receiver_server(args.host, args.port)
    print(f"[receiver] mock partner listening on http://{args.host}:{args.port}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
