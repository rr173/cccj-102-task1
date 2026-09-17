"""Store-level invariants: key stamping, idempotent ingest, replay transitions."""

import os
import shutil
import tempfile
import unittest

from hub.errors import Conflict
from hub.store import Store


class StoreTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="hub-store-")
        self.store = Store(os.path.join(self.tmp, "test.db"))
        self.tenant = self.store.create_tenant("acme")
        self.endpoint, self.secret = self.store.create_endpoint(
            self.tenant["id"], "http://receiver.test/wh", 4
        )

    def tearDown(self):
        self.store.close()
        shutil.rmtree(self.tmp, ignore_errors=True)

    def enqueue(self, object_key="order-1", **kw):
        kw.setdefault("event_type", "test")
        kw.setdefault("payload", "{}")
        event, created = self.store.enqueue_event(self.endpoint["id"], object_key, **kw)
        self.assertTrue(created)
        return event

    def test_events_are_stamped_with_active_key_version(self):
        e1 = self.enqueue()
        self.assertEqual(e1["key_version"], 1)

        new_version, _ = self.store.rotate_key(self.endpoint["id"])
        self.assertEqual(new_version, 2)

        e2 = self.enqueue()
        self.assertEqual(e2["key_version"], 2)
        # the pre-rotation event keeps its original version
        self.assertEqual(self.store.get_event(e1["id"])["key_version"], 1)
        # old secret is still retrievable for signing queued events
        self.assertEqual(self.store.get_secret(self.endpoint["id"], 1), self.secret)

    def test_ingest_is_idempotent_on_id_and_key(self):
        e1 = self.enqueue(event_id="evt_fixed")
        again, created = self.store.enqueue_event(
            self.endpoint["id"], "order-1", "test", "{}", event_id="evt_fixed"
        )
        self.assertFalse(created)
        self.assertEqual(again["id"], e1["id"])

        e2 = self.enqueue(idempotency_key="abc-123")
        again, created = self.store.enqueue_event(
            self.endpoint["id"], "order-2", "test", "{}", idempotency_key="abc-123"
        )
        self.assertFalse(created)
        self.assertEqual(again["id"], e2["id"])

    def test_seq_is_monotonic_per_endpoint(self):
        seqs = [self.enqueue(object_key=f"k{i}")["seq"] for i in range(5)]
        self.assertEqual(seqs, sorted(seqs))
        self.assertEqual(len(set(seqs)), 5)

    def test_next_eligible_returns_only_head_of_line_per_key(self):
        first = self.enqueue(object_key="k1")
        self.enqueue(object_key="k1")  # second event of same key
        other = self.enqueue(object_key="k2")

        eligible = self.store.next_eligible(self.endpoint["id"], set(), now=10**12)
        self.assertEqual(eligible["id"], first["id"])

        # while k1's head is in flight, only k2 is eligible
        eligible = self.store.next_eligible(self.endpoint["id"], {"k1"}, now=10**12)
        self.assertEqual(eligible["id"], other["id"])

    def test_next_eligible_respects_retry_time(self):
        event = self.enqueue(object_key="k1")
        self.store.claim(event["id"])
        self.store.schedule_retry(event["id"], attempts=1, next_attempt_at=10**12)
        self.assertIsNone(self.store.next_eligible(self.endpoint["id"], set(), now=0))
        eligible = self.store.next_eligible(self.endpoint["id"], set(), now=10**12)
        self.assertEqual(eligible["id"], event["id"])

    def test_replay_transitions(self):
        event = self.enqueue()
        # in-flight events cannot be replayed
        with self.assertRaises(Conflict):
            self.store.replay(event["id"])

        # delivered events: replay is a no-op returning the original receipt
        self.store.claim(event["id"])
        self.store.record_attempt(event["id"], 1, 200, None, 12.0)
        self.store.mark_delivered(event["id"])
        _, requeued, receipt = self.store.replay(event["id"])
        self.assertFalse(requeued)
        self.assertEqual(receipt["http_status"], 200)
        self.assertEqual(self.store.get_event(event["id"])["status"], "delivered")

        # failed events: replay re-queues them
        failed = self.enqueue(object_key="k2")
        self.store.claim(failed["id"])
        self.store.mark_failed(failed["id"])
        _, requeued, _ = self.store.replay(failed["id"])
        self.assertTrue(requeued)
        self.assertEqual(self.store.get_event(failed["id"])["status"], "pending")


if __name__ == "__main__":
    unittest.main()
