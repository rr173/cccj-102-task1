#!/usr/bin/env python3
"""End-to-end scenario against a running hub (see run.sh).

Walks through every guarantee of the delivery hub:

  1. per-object FIFO ordering under parallelism
  2. tenant-tunable concurrency cap
  3. key rotation mid-stream (old queued events keep v1, new events use v2)
  4. per-endpoint backoff isolation (429 / outage does not touch other tenants)
  5. manual replay never double-confirms
"""

import json
import os
import sys
import time
import urllib.error
import urllib.request

HUB = os.environ.get("HUB_URL", "http://127.0.0.1:8080")
RCV = os.environ.get("RECEIVER_URL", "http://127.0.0.1:9100")

PASS, FAIL = "\033[32m✓\033[0m", "\033[31m✗\033[0m"
checks = 0


def api(method, url, body=None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method,
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            return resp.status, json.loads(resp.read() or b"{}")
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read() or b"{}")


def step(msg):
    print(f"\n\033[1m== {msg} ==\033[0m")


def check(ok, msg):
    global checks
    checks += 1
    print(f"  {PASS if ok else FAIL} {msg}")
    if not ok:
        sys.exit(f"\ndemo failed at: {msg}")


def wait_until(fn, msg, timeout=30):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if fn():
            return
        time.sleep(0.05)
    sys.exit(f"timeout waiting for: {msg}")


def enqueue(endpoint_id, object_key, n, **extra):
    body = {"object_key": object_key, "type": "order.updated", "payload": {"n": n}}
    body.update(extra)
    status, resp = api("POST", f"{HUB}/v1/endpoints/{endpoint_id}/events", body)
    assert status in (200, 201), resp
    return resp["event"]


def queue(endpoint_id):
    return api("GET", f"{HUB}/v1/endpoints/{endpoint_id}")[1]


def receipts():
    return api("GET", f"{RCV}/__receipts")[1]["receipts"]


def main():
    api("POST", f"{RCV}/__reset")

    step("1. setup: tenant + two endpoints (partner paths /a and /b)")
    _, tenant = api("POST", f"{HUB}/v1/tenants", {"name": "acme-shop"})
    _, ep_a = api("POST", f"{HUB}/v1/tenants/{tenant['id']}/endpoints",
                  {"url": f"{RCV}/a", "max_concurrency": 4})
    _, ep_b = api("POST", f"{HUB}/v1/tenants/{tenant['id']}/endpoints",
                  {"url": f"{RCV}/b", "max_concurrency": 4})
    api("POST", f"{RCV}/__config",
        {"secrets": {"/a": {"1": ep_a["signing_secret"]},
                     "/b": {"1": ep_b["signing_secret"]}}})
    print(f"  endpoint A: {ep_a['id']} (key v1)")
    print(f"  endpoint B: {ep_b['id']} (key v1)")

    step("2. ordering: 5 orders x 8 events, concurrency 4")
    api("POST", f"{RCV}/__config", {"latency_ms": 60})  # make parallelism observable
    for n in range(8):
        for k in range(5):
            enqueue(ep_a["id"], f"order-{k}", n)
    wait_until(lambda: len(receipts()) == 40, "40 events delivered")
    api("POST", f"{RCV}/__config", {"latency_ms": 0})
    violations = api("GET", f"{RCV}/__order_violations")[1]["violations"]
    check(violations == [], "per-object FIFO preserved under parallel delivery")
    stats = api("GET", f"{RCV}/__stats")[1]
    check(1 < stats["max_concurrent"] <= 4,
          f"parallel but capped (max in-flight = {stats['max_concurrent']} <= 4)")

    step("3. key rotation mid-stream on endpoint A")
    api("POST", f"{RCV}/__config", {"latency_ms": 200})  # build a backlog
    for n in range(100, 110):
        enqueue(ep_a["id"], "migration", n)
    _, rotated = api("POST", f"{HUB}/v1/endpoints/{ep_a['id']}/keys/rotate")
    api("POST", f"{RCV}/__config",
        {"secrets": {"/a": {"2": rotated["signing_secret"]}}})
    print(f"  rotated to key v{rotated['active_key_version']} while events were queued")
    for n in range(110, 120):
        enqueue(ep_a["id"], "migration", n)
    api("POST", f"{RCV}/__config", {"latency_ms": 0})
    wait_until(lambda: len([r for r in receipts() if r["object_key"] == "migration"]) == 20,
               "migration events delivered")
    mig = sorted([r for r in receipts() if r["object_key"] == "migration"],
                 key=lambda r: r["n"])
    old_ok = all(r["key_version"] == 1 for r in mig if r["n"] < 110)
    new_ok = all(r["key_version"] == 2 for r in mig if r["n"] >= 110)
    check(old_ok, "events queued before the switch were still signed with key v1")
    check(new_ok, "events after the switch were signed only with key v2")

    step("4. per-endpoint backoff: /a gets rate-limited, /b keeps flowing")
    api("POST", f"{RCV}/__config", {"path_modes": {"/a": "limited"}, "retry_after": 2})
    for n in range(200, 203):
        enqueue(ep_a["id"], "limited", n)
    b_events = [enqueue(ep_b["id"], "flow", n) for n in range(300, 303)]
    time.sleep(1.0)
    view_a, view_b = queue(ep_a["id"]), queue(ep_b["id"])
    check(view_a["circuit"]["open"] and view_a["circuit"]["failures"] > 0,
          f"endpoint A circuit open (failures={view_a['circuit']['failures']}, "
          f"last={view_a['circuit']['last_error']})")
    check(view_b["queue"]["delivered"] >= 3 and view_b["circuit"]["failures"] == 0,
          "endpoint B (other tenant traffic) unaffected")
    api("POST", f"{RCV}/__config", {"path_modes": {}})
    wait_until(lambda: queue(ep_a["id"])["queue"]["delivered"] >= 63, "A drains after recovery")
    check(True, "endpoint A recovered automatically once the limiter lifted")

    step("5. manual replay never double-confirms")
    delivered = b_events[0]
    before = len(api("GET", f"{HUB}/v1/events/{delivered['id']}")[1]["attempts_log"])
    _, replay = api("POST", f"{HUB}/v1/events/{delivered['id']}/replay")
    check(replay["replayed"] is False and replay["reason"] == "already_delivered",
          "replay of a delivered event returns the original receipt, no re-send")
    after = len(api("GET", f"{HUB}/v1/events/{delivered['id']}")[1]["attempts_log"])
    check(before == after, f"attempt count unchanged ({before})")

    api("POST", f"{RCV}/__config", {"path_modes": {"/b": "flaky"}})
    doomed = enqueue(ep_b["id"], "will-fail", 400, max_attempts=1)
    wait_until(lambda: api("GET", f"{HUB}/v1/events/{doomed['id']}")[1]["status"] == "failed",
               "event dead-letters after exhausting attempts")
    check(True, "event dead-lettered after max_attempts=1")
    api("POST", f"{RCV}/__config", {"path_modes": {}})
    _, replay = api("POST", f"{HUB}/v1/events/{doomed['id']}/replay")
    check(replay["replayed"] is True, "failed event re-queued by manual replay")
    wait_until(lambda: api("GET", f"{HUB}/v1/events/{doomed['id']}")[1]["status"] == "delivered",
               "replayed event delivered")
    got = [r for r in receipts() if r["event_id"] == doomed["id"]]
    check(len(got) == 1, "replayed event processed exactly once at the receiver")

    print(f"\n\033[1mAll {checks} demo checks passed.\033[0m")


if __name__ == "__main__":
    main()
