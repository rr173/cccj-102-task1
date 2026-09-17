"""Shared fixtures: an in-process hub + mock receiver on ephemeral ports."""

import json
import os
import shutil
import tempfile
import threading
import time
import urllib.error
import urllib.request

from hub.api import make_server
from hub.config import Config
from hub.core import Hub
from hub.receiver import make_receiver_server


def http(method, url, body=None, timeout=10):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(
        url, data=data, method=method, headers={"Content-Type": "application/json"}
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, json.loads(resp.read() or b"{}")
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read() or b"{}")


def wait_until(fn, timeout=10.0, interval=0.02, msg="condition"):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if fn():
            return
        time.sleep(interval)
    raise AssertionError(f"timeout waiting for: {msg}")


class Env:
    """A full runtime: hub (API + dispatcher) and a controllable receiver."""

    def __init__(self, **cfg_overrides):
        self.tmp = tempfile.mkdtemp(prefix="hub-test-")
        cfg = Config(
            host="127.0.0.1",
            port=0,
            db_path=os.path.join(self.tmp, "hub.db"),
            delivery_timeout_ms=800,
            base_backoff_ms=40,
            max_backoff_ms=400,
            max_attempts=8,
            default_max_concurrency=4,
        )
        for key, value in cfg_overrides.items():
            setattr(cfg, key, value)
        self.hub = Hub(cfg)
        self.rcv_server, self.rcv = make_receiver_server("127.0.0.1", 0)
        threading.Thread(target=self.rcv_server.serve_forever, daemon=True).start()
        self.api_server = make_server(self.hub, "127.0.0.1", 0)
        threading.Thread(target=self.api_server.serve_forever, daemon=True).start()
        self.hub_url = f"http://127.0.0.1:{self.api_server.server_address[1]}"
        self.rcv_url = f"http://127.0.0.1:{self.rcv_server.server_address[1]}"

    def close(self):
        self.api_server.shutdown()
        self.api_server.server_close()
        self.rcv_server.shutdown()
        self.rcv_server.server_close()
        self.hub.close()
        shutil.rmtree(self.tmp, ignore_errors=True)

    # ------------------------------------------------------------- helpers
    def make_endpoint(self, path="/ok", max_concurrency=4):
        status, tenant = http("POST", f"{self.hub_url}/v1/tenants", {"name": "acme"})
        assert status == 201, tenant
        status, ep = http(
            "POST",
            f"{self.hub_url}/v1/tenants/{tenant['id']}/endpoints",
            {"url": f"{self.rcv_url}{path}", "max_concurrency": max_concurrency},
        )
        assert status == 201, ep
        return ep

    def enqueue(self, endpoint_id, object_key, n, **extra):
        body = {"object_key": object_key, "type": "test", "payload": {"n": n}}
        body.update(extra)
        status, resp = http("POST", f"{self.hub_url}/v1/endpoints/{endpoint_id}/events", body)
        assert status in (200, 201), resp
        return resp

    def event(self, event_id):
        status, resp = http("GET", f"{self.hub_url}/v1/events/{event_id}")
        assert status == 200, resp
        return resp

    def endpoint(self, endpoint_id):
        status, resp = http("GET", f"{self.hub_url}/v1/endpoints/{endpoint_id}")
        assert status == 200, resp
        return resp

    def receipts(self):
        status, resp = http("GET", f"{self.rcv_url}/__receipts")
        assert status == 200
        return resp["receipts"]

    def stats(self):
        status, resp = http("GET", f"{self.rcv_url}/__stats")
        assert status == 200
        return resp

    def configure_receiver(self, **cfg):
        status, resp = http("POST", f"{self.rcv_url}/__config", cfg)
        assert status == 200, resp

    def wait_delivered(self, event_id, timeout=10):
        wait_until(lambda: self.event(event_id)["status"] == "delivered",
                   timeout=timeout, msg=f"{event_id} delivered")
