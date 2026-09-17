"""End-to-end behavior tests against a real hub + mock receiver."""

import http.client
import threading
import time
import unittest

from tests.helpers import Env, http, wait_until


class OrderingTest(unittest.TestCase):
    """Same business object stays FIFO; unrelated objects run in parallel."""

    def setUp(self):
        self.env = Env()

    def tearDown(self):
        self.env.close()

    def test_per_key_fifo_with_parallelism(self):
        ep = self.env.make_endpoint("/wh", max_concurrency=8)
        self.env.configure_receiver(latency_ms=15)

        keys, per_key = 6, 10
        started = time.monotonic()
        for n in range(per_key):
            for k in range(keys):
                self.env.enqueue(ep["id"], f"order-{k}", n)

        total = keys * per_key
        wait_until(lambda: len(self.env.receipts()) == total, timeout=20,
                   msg="all events delivered")
        elapsed = time.monotonic() - started

        status, resp = http("GET", f"{self.env.rcv_url}/__order_violations")
        self.assertEqual(resp["violations"], [])
        # serial delivery would take total * 15ms = 900ms; parallelism must help
        self.assertLess(elapsed, 0.9 * total * 0.015)
        self.assertGreater(self.env.stats()["max_concurrent"], 1)


class ConcurrencyLimitTest(unittest.TestCase):
    def setUp(self):
        self.env = Env()

    def tearDown(self):
        self.env.close()

    def test_endpoint_concurrency_cap_is_respected(self):
        ep = self.env.make_endpoint("/wh", max_concurrency=2)
        self.env.configure_receiver(latency_ms=120)
        for n in range(10):
            self.env.enqueue(ep["id"], f"k{n}", n)  # distinct keys: all eligible
        wait_until(lambda: len(self.env.receipts()) == 10, timeout=20,
                   msg="all events delivered")
        self.assertLessEqual(self.env.stats()["max_concurrent"], 2)

    def test_limit_is_adjustable_at_runtime(self):
        ep = self.env.make_endpoint("/wh", max_concurrency=1)
        status, _ = http("PATCH", f"{self.env.hub_url}/v1/endpoints/{ep['id']}",
                         {"max_concurrency": 6})
        self.assertEqual(status, 200)
        self.env.configure_receiver(latency_ms=120)
        for n in range(12):
            self.env.enqueue(ep["id"], f"k{n}", n)
        wait_until(lambda: len(self.env.receipts()) == 12, timeout=20,
                   msg="all events delivered")
        stats = self.env.stats()
        self.assertGreater(stats["max_concurrent"], 2)
        self.assertLessEqual(stats["max_concurrent"], 6)


class KeyRotationTest(unittest.TestCase):
    """Events queued before the switch keep the old key; new ones use only the new key."""

    def setUp(self):
        self.env = Env()

    def tearDown(self):
        self.env.close()

    def test_rotation_mid_stream(self):
        ep = self.env.make_endpoint("/wh", max_concurrency=1)
        # slow receiver so a backlog builds up while we rotate
        self.env.configure_receiver(latency_ms=150)
        self.env.configure_receiver(secrets={"/wh": {"1": ep["signing_secret"]}})

        for n in range(10):
            self.env.enqueue(ep["id"], "migration", n)

        status, rotated = http("POST", f"{self.env.hub_url}/v1/endpoints/{ep['id']}/keys/rotate")
        self.assertEqual(status, 200)
        self.assertEqual(rotated["active_key_version"], 2)
        # partner learns both secrets (overlap window during migration)
        self.env.configure_receiver(secrets={"/wh": {"2": rotated["signing_secret"]}})

        for n in range(10, 20):
            self.env.enqueue(ep["id"], "migration", n)

        wait_until(lambda: len(self.env.receipts()) == 20, timeout=30,
                   msg="all events delivered")
        receipts = sorted(self.env.receipts(), key=lambda r: r["n"])
        for r in receipts:
            expected_version = 1 if r["n"] < 10 else 2
            self.assertEqual(r["key_version"], expected_version,
                             f"n={r['n']} signed with wrong key version")
        # nothing was rejected for a bad signature (401 would mean no receipt)
        self.assertEqual(len(receipts), 20)


class BackoffIsolationTest(unittest.TestCase):
    """A sick endpoint backs off on its own and never drags other tenants down."""

    def setUp(self):
        # wider backoff windows so circuit state is observable without races
        self.env = Env(base_backoff_ms=200, max_backoff_ms=1000)

    def tearDown(self):
        self.env.close()

    def test_sick_endpoint_isolated_from_healthy_one(self):
        sick = self.env.make_endpoint("/sick")
        healthy = self.env.make_endpoint("/ok")
        self.env.configure_receiver(path_modes={"/sick": "flaky"})  # 500s

        sick_events = [self.env.enqueue(sick["id"], f"s{k}", k)["event"]["id"] for k in range(3)]
        ok_events = [self.env.enqueue(healthy["id"], f"o{k}", 100 + k)["event"]["id"] for k in range(3)]

        # healthy endpoint keeps flowing despite the sick neighbor
        for eid in ok_events:
            self.env.wait_delivered(eid, timeout=5)

        # sick endpoint is circuit-broken, its events still pending
        wait_until(lambda: self.env.endpoint(sick["id"])["circuit"]["failures"] >= 2,
                   timeout=5, msg="sick endpoint circuit open")
        sick_view = self.env.endpoint(sick["id"])
        self.assertTrue(sick_view["circuit"]["open"])
        self.assertEqual(sick_view["queue"]["delivered"], 0)
        self.assertEqual(self.env.endpoint(healthy["id"])["circuit"]["failures"], 0)

        # receiver recovers -> backoff expires -> sick endpoint drains
        self.env.configure_receiver(path_modes={})
        for eid in sick_events:
            self.env.wait_delivered(eid, timeout=10)

    def test_rate_limit_retry_after_is_honored(self):
        ep = self.env.make_endpoint("/limited")
        self.env.configure_receiver(path_modes={"/limited": "limited"}, retry_after=1)
        eid = self.env.enqueue(ep["id"], "k", 1)["event"]["id"]

        wait_until(lambda: self.env.endpoint(ep["id"])["circuit"]["failures"] >= 1,
                   timeout=5, msg="429 observed")
        # while limited, the event must not be hammered: attempts stay low
        attempts_during = len(self.env.event(eid)["attempts_log"])
        self.assertLessEqual(attempts_during, 3)

        self.env.configure_receiver(path_modes={})
        self.env.wait_delivered(eid, timeout=10)

    def test_receiver_down_connection_error_backs_off(self):
        ep = self.env.make_endpoint("/down")
        self.env.configure_receiver(path_modes={"/down": "down"})
        eid = self.env.enqueue(ep["id"], "k", 1)["event"]["id"]
        wait_until(lambda: self.env.endpoint(ep["id"])["circuit"]["failures"] >= 1,
                   timeout=5, msg="connection failure observed")
        self.assertIn(self.env.event(eid)["status"], ("pending", "delivering"))
        self.env.configure_receiver(path_modes={})
        self.env.wait_delivered(eid, timeout=10)


class ReplayTest(unittest.TestCase):
    def setUp(self):
        self.env = Env()

    def tearDown(self):
        self.env.close()

    def test_replay_of_delivered_event_never_redelivers(self):
        ep = self.env.make_endpoint("/wh")
        eid = self.env.enqueue(ep["id"], "k", 1)["event"]["id"]
        self.env.wait_delivered(eid)

        attempts_before = len(self.env.event(eid)["attempts_log"])
        receipts_before = len(self.env.receipts())

        status, resp = http("POST", f"{self.env.hub_url}/v1/events/{eid}/replay")
        self.assertEqual(status, 200)
        self.assertFalse(resp["replayed"])
        self.assertEqual(resp["reason"], "already_delivered")
        self.assertEqual(resp["receipt"]["http_status"], 200)

        time.sleep(0.3)  # give any erroneous redelivery a chance to happen
        self.assertEqual(len(self.env.event(eid)["attempts_log"]), attempts_before)
        self.assertEqual(len(self.env.receipts()), receipts_before)

    def test_replay_of_failed_event_redelivers_once(self):
        ep = self.env.make_endpoint("/flaky")
        self.env.configure_receiver(path_modes={"/flaky": "flaky"})
        eid = self.env.enqueue(ep["id"], "k", 1, max_attempts=1)["event"]["id"]
        wait_until(lambda: self.env.event(eid)["status"] == "failed",
                   timeout=5, msg="event dead-lettered")

        self.env.configure_receiver(path_modes={})
        status, resp = http("POST", f"{self.env.hub_url}/v1/events/{eid}/replay")
        self.assertTrue(resp["replayed"])
        self.env.wait_delivered(eid)
        self.assertEqual(len([r for r in self.env.receipts() if r["event_id"] == eid]), 1)

    def test_receiver_dedupes_redelivery_after_timeout(self):
        # receiver processes slowly; hub times out and retries with the same
        # X-Webhook-Id — the receiver must process it exactly once
        ep = self.env.make_endpoint("/slow")
        self.env.configure_receiver(path_modes={"/slow": "slow"}, latency_ms=900)
        eid = self.env.enqueue(ep["id"], "k", 1)["event"]["id"]
        self.env.wait_delivered(eid, timeout=15)

        # the original (timed-out) request finishes processing slightly later;
        # it must still result in exactly one receipt
        wait_until(lambda: len([r for r in self.env.receipts() if r["event_id"] == eid]) == 1,
                   timeout=5, msg="exactly one receipt recorded")
        time.sleep(0.3)
        receipts = [r for r in self.env.receipts() if r["event_id"] == eid]
        self.assertEqual(len(receipts), 1, "receiver processed the event twice")
        self.assertGreaterEqual(self.env.stats()["deduped"], 1)


class IdempotentIngestTest(unittest.TestCase):
    def setUp(self):
        self.env = Env()

    def tearDown(self):
        self.env.close()

    def test_duplicate_idempotency_key_enqueues_once(self):
        ep = self.env.make_endpoint("/wh")
        body = {"object_key": "k", "type": "t", "payload": {"n": 1},
                "idempotency_key": "req-42"}
        s1, r1 = http("POST", f"{self.env.hub_url}/v1/endpoints/{ep['id']}/events", body)
        s2, r2 = http("POST", f"{self.env.hub_url}/v1/endpoints/{ep['id']}/events", body)
        self.assertEqual(s1, 201)
        self.assertEqual(s2, 200)
        self.assertEqual(r1["event"]["id"], r2["event"]["id"])
        self.assertTrue(r2["deduplicated"])

        self.env.wait_delivered(r1["event"]["id"])
        time.sleep(0.2)
        self.assertEqual(len(self.env.receipts()), 1)


if __name__ == "__main__":
    unittest.main()
