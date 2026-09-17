"""Delivery scheduling.

One EndpointRunner thread per endpoint. Isolation guarantees:

  * per-business-object FIFO: at most one in-flight event per object_key, and
    only the oldest pending event of a key is ever eligible (head-of-line
    blocking per key — a failed head blocks its key until it succeeds or
    dead-letters, never lets a newer event overtake it);
  * per-endpoint parallelism: a CapacityGate sized by the tenant-configured
    max_concurrency, adjustable at runtime;
  * per-endpoint backoff: the circuit breaker pauses only this endpoint's
    runner, so a sick receiver never slows other tenants down.
"""

import threading
import time

from .circuit import CircuitBreaker


class CapacityGate:
    """In-flight counter with a runtime-adjustable limit."""

    def __init__(self, limit: int):
        self._cond = threading.Condition()
        self._limit = max(1, limit)
        self._used = 0

    def set_limit(self, limit: int):
        with self._cond:
            self._limit = max(1, limit)
            self._cond.notify_all()

    def acquire(self, stop: threading.Event) -> bool:
        with self._cond:
            while self._used >= self._limit:
                if stop.is_set():
                    return False
                self._cond.wait(0.1)
            self._used += 1
            return True

    def release(self):
        with self._cond:
            self._used -= 1
            self._cond.notify_all()


class EndpointRunner(threading.Thread):
    def __init__(self, hub, endpoint_id: str):
        super().__init__(name=f"runner-{endpoint_id}", daemon=True)
        self.hub = hub
        self.endpoint_id = endpoint_id
        ep = hub.store.get_endpoint(endpoint_id)
        self.gate = CapacityGate(ep["max_concurrency"])
        self.circuit = CircuitBreaker(hub.cfg.base_backoff_ms, hub.cfg.max_backoff_ms)
        self.wake = threading.Event()
        self._lock = threading.Lock()
        self._in_flight = set()  # object_keys currently being delivered

    def run(self):
        store = self.hub.store
        stop = self.hub.dispatcher.stop_event
        while not stop.is_set():
            ep = store.get_endpoint(self.endpoint_id)
            if ep is None:
                return
            if ep["status"] != "active":
                self._wait(0.5)
                continue
            self.gate.set_limit(ep["max_concurrency"])
            now = time.time()
            if self.circuit.open_until > now:
                self._wait(min(self.circuit.open_until - now, 5.0))
                continue
            with self._lock:
                event = store.next_eligible(self.endpoint_id, self._in_flight, now)
                if event is not None:
                    # reserve the key before releasing the lock so the next
                    # loop iteration cannot pick another event of the same key
                    self._in_flight.add(event["object_key"])
            if event is None:
                nxt = store.next_retry_time(self.endpoint_id)
                self._wait(0.5 if nxt is None else max(0.02, min(nxt - now, 0.5)))
                continue
            if not self.gate.acquire(stop):
                with self._lock:
                    self._in_flight.discard(event["object_key"])
                return
            if not store.claim(event["id"]):
                with self._lock:
                    self._in_flight.discard(event["object_key"])
                self.gate.release()
                continue
            threading.Thread(
                target=self._deliver, args=(event,), name=f"deliver-{event['id']}", daemon=True
            ).start()

    def _wait(self, seconds: float):
        self.wake.wait(seconds)
        self.wake.clear()

    def _deliver(self, event: dict):
        try:
            store = self.hub.store
            ep = store.get_endpoint(self.endpoint_id)
            # sign with the key version stamped at enqueue time — rotation-safe
            secret = store.get_secret(self.endpoint_id, event["key_version"])
            result = self.hub.deliverer.send(ep["url"], secret, event["key_version"], event)
            attempts = event["attempts"] + 1
            store.record_attempt(
                event["id"], attempts, result.http_status, result.error, result.duration_ms
            )
            if result.ok:
                store.mark_delivered(event["id"])
                self.circuit.on_success()
            else:
                backoff = self.circuit.on_failure(
                    result.retry_after, result.error or f"http {result.http_status}"
                )
                if attempts >= event["max_attempts"]:
                    store.mark_failed(event["id"])
                else:
                    store.schedule_retry(event["id"], attempts, time.time() + backoff)
        finally:
            with self._lock:
                self._in_flight.discard(event["object_key"])
            self.gate.release()
            self.wake.set()


class Dispatcher:
    def __init__(self, hub):
        self.hub = hub
        self.stop_event = threading.Event()
        self._lock = threading.Lock()
        self._runners = {}

    def ensure_runner(self, endpoint_id: str) -> EndpointRunner:
        with self._lock:
            runner = self._runners.get(endpoint_id)
            if runner is None:
                runner = EndpointRunner(self.hub, endpoint_id)
                runner.start()
                self._runners[endpoint_id] = runner
            return runner

    def notify(self, endpoint_id: str):
        self.ensure_runner(endpoint_id).wake.set()

    def update_limit(self, endpoint_id: str, limit: int):
        self.ensure_runner(endpoint_id).gate.set_limit(limit)
        self.notify(endpoint_id)

    def circuit_state(self, endpoint_id: str) -> dict:
        with self._lock:
            runner = self._runners.get(endpoint_id)
        if runner is None:
            return {"failures": 0, "open": False, "open_until": None, "last_error": None}
        return runner.circuit.snapshot()

    def shutdown(self):
        self.stop_event.set()
        with self._lock:
            runners = list(self._runners.values())
        for runner in runners:
            runner.wake.set()
        for runner in runners:
            runner.join(timeout=2)
