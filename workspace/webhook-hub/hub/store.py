"""SQLite-backed persistence.

One shared connection guarded by an RLock; WAL + busy_timeout for durability.
All scheduling invariants live here so the dispatcher stays dumb:

  * every event is stamped with the endpoint's *active key version* at enqueue
    time — events queued before a rotation keep signing with the old key;
  * ``seq`` is a per-endpoint monotonic sequence, and only the oldest pending
    event of each ``object_key`` is ever eligible for delivery (per-key FIFO);
  * ingest is idempotent on caller-supplied event id or idempotency key.
"""

import os
import secrets
import sqlite3
import threading
import time
import uuid

from .errors import Conflict, NotFound

SCHEMA = """
CREATE TABLE IF NOT EXISTS tenants(
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  created_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS endpoints(
  id TEXT PRIMARY KEY,
  tenant_id TEXT NOT NULL,
  url TEXT NOT NULL,
  max_concurrency INTEGER NOT NULL,
  active_key_version INTEGER NOT NULL DEFAULT 1,
  next_seq INTEGER NOT NULL DEFAULT 1,
  status TEXT NOT NULL DEFAULT 'active',
  created_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS endpoint_keys(
  endpoint_id TEXT NOT NULL,
  version INTEGER NOT NULL,
  secret TEXT NOT NULL,
  status TEXT NOT NULL,               -- active | rotated
  created_at REAL NOT NULL,
  PRIMARY KEY(endpoint_id, version)
);
CREATE TABLE IF NOT EXISTS events(
  id TEXT PRIMARY KEY,
  tenant_id TEXT NOT NULL,
  endpoint_id TEXT NOT NULL,
  object_key TEXT NOT NULL,
  event_type TEXT NOT NULL,
  payload TEXT NOT NULL,              -- canonical JSON, signed & sent verbatim
  key_version INTEGER NOT NULL,       -- signing key version stamped at enqueue
  seq INTEGER NOT NULL,               -- per-endpoint monotonic
  status TEXT NOT NULL,               -- pending | delivering | delivered | failed
  attempts INTEGER NOT NULL DEFAULT 0,
  max_attempts INTEGER NOT NULL,
  next_attempt_at REAL NOT NULL DEFAULT 0,
  idempotency_key TEXT,
  created_at REAL NOT NULL,
  updated_at REAL NOT NULL,
  delivered_at REAL
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_events_idem
  ON events(endpoint_id, idempotency_key) WHERE idempotency_key IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_events_sched ON events(endpoint_id, status, object_key, seq);
CREATE TABLE IF NOT EXISTS attempts(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  event_id TEXT NOT NULL,
  n INTEGER NOT NULL,
  http_status INTEGER,
  error TEXT,
  duration_ms REAL,
  created_at REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_attempts_event ON attempts(event_id);
"""


def _now() -> float:
    return time.time()


def _new_id(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:20]}"


def _as_dict(row):
    return dict(row) if row is not None else None


class Store:
    def __init__(self, path: str):
        os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
        self._db = sqlite3.connect(path, check_same_thread=False, isolation_level=None)
        self._db.row_factory = sqlite3.Row
        self._lock = threading.RLock()
        with self._lock:
            self._db.execute("PRAGMA journal_mode=WAL")
            self._db.execute("PRAGMA busy_timeout=5000")
            self._db.executescript(SCHEMA)
            # crash recovery: anything interrupted mid-delivery becomes pending
            self._db.execute("UPDATE events SET status='pending' WHERE status='delivering'")

    def close(self):
        with self._lock:
            self._db.close()

    # ---------------------------------------------------------------- tenants
    def create_tenant(self, name: str) -> dict:
        with self._lock:
            tenant = {"id": _new_id("ten"), "name": name, "created_at": _now()}
            self._db.execute(
                "INSERT INTO tenants(id, name, created_at) VALUES (:id, :name, :created_at)",
                tenant,
            )
            return tenant

    def get_tenant(self, tenant_id: str) -> dict:
        with self._lock:
            row = self._db.execute("SELECT * FROM tenants WHERE id=?", (tenant_id,)).fetchone()
            if row is None:
                raise NotFound(f"tenant {tenant_id} not found")
            return dict(row)

    # --------------------------------------------------------------- endpoints
    def create_endpoint(self, tenant_id: str, url: str, max_concurrency: int):
        """Create an endpoint together with signing key v1. Returns (endpoint, secret)."""
        with self._lock:
            self.get_tenant(tenant_id)
            endpoint = {
                "id": _new_id("ep"),
                "tenant_id": tenant_id,
                "url": url,
                "max_concurrency": max_concurrency,
                "active_key_version": 1,
                "next_seq": 1,
                "status": "active",
                "created_at": _now(),
            }
            secret = secrets.token_hex(32)
            self._db.execute(
                "INSERT INTO endpoints(id, tenant_id, url, max_concurrency, active_key_version,"
                " next_seq, status, created_at) VALUES (:id, :tenant_id, :url, :max_concurrency,"
                " :active_key_version, :next_seq, :status, :created_at)",
                endpoint,
            )
            self._db.execute(
                "INSERT INTO endpoint_keys(endpoint_id, version, secret, status, created_at)"
                " VALUES (?,?,?,?,?)",
                (endpoint["id"], 1, secret, "active", _now()),
            )
            return endpoint, secret

    def get_endpoint(self, endpoint_id: str) -> dict:
        with self._lock:
            row = self._db.execute("SELECT * FROM endpoints WHERE id=?", (endpoint_id,)).fetchone()
            if row is None:
                raise NotFound(f"endpoint {endpoint_id} not found")
            return dict(row)

    def list_endpoints(self) -> list:
        with self._lock:
            return [dict(r) for r in self._db.execute("SELECT * FROM endpoints").fetchall()]

    def update_endpoint(self, endpoint_id: str, max_concurrency=None, status=None) -> dict:
        with self._lock:
            self.get_endpoint(endpoint_id)
            if max_concurrency is not None:
                self._db.execute(
                    "UPDATE endpoints SET max_concurrency=? WHERE id=?",
                    (max_concurrency, endpoint_id),
                )
            if status is not None:
                self._db.execute("UPDATE endpoints SET status=? WHERE id=?", (status, endpoint_id))
            return self.get_endpoint(endpoint_id)

    # -------------------------------------------------------------- key rotation
    def rotate_key(self, endpoint_id: str):
        """Rotate the signing key. Returns (new_version, new_secret).

        The old secret is kept (status 'rotated') so events already queued with
        the old version still verify at the receiver; new events are stamped
        with the new version from this point on.
        """
        with self._lock:
            ep = self.get_endpoint(endpoint_id)
            new_version = ep["active_key_version"] + 1
            secret = secrets.token_hex(32)
            now = _now()
            self._db.execute("BEGIN IMMEDIATE")
            try:
                self._db.execute(
                    "UPDATE endpoint_keys SET status='rotated'"
                    " WHERE endpoint_id=? AND status='active'",
                    (endpoint_id,),
                )
                self._db.execute(
                    "INSERT INTO endpoint_keys(endpoint_id, version, secret, status, created_at)"
                    " VALUES (?,?,?,?,?)",
                    (endpoint_id, new_version, secret, "active", now),
                )
                self._db.execute(
                    "UPDATE endpoints SET active_key_version=? WHERE id=?",
                    (new_version, endpoint_id),
                )
                self._db.execute("COMMIT")
            except Exception:
                self._db.execute("ROLLBACK")
                raise
            return new_version, secret

    def list_keys(self, endpoint_id: str) -> list:
        with self._lock:
            self.get_endpoint(endpoint_id)
            rows = self._db.execute(
                "SELECT version, status, created_at FROM endpoint_keys"
                " WHERE endpoint_id=? ORDER BY version",
                (endpoint_id,),
            ).fetchall()
            return [dict(r) for r in rows]

    def get_secret(self, endpoint_id: str, version: int) -> str:
        with self._lock:
            row = self._db.execute(
                "SELECT secret FROM endpoint_keys WHERE endpoint_id=? AND version=?",
                (endpoint_id, version),
            ).fetchone()
            if row is None:
                raise NotFound(f"key v{version} for endpoint {endpoint_id} not found")
            return row["secret"]

    # ------------------------------------------------------------------ events
    def enqueue_event(
        self,
        endpoint_id: str,
        object_key: str,
        event_type: str,
        payload: str,
        event_id: str = None,
        idempotency_key: str = None,
        max_attempts: int = 8,
    ):
        """Insert an event, stamping the *current* active key version.

        Returns (event, created). If the caller-supplied id or idempotency key
        already exists, returns the existing event with created=False — no
        duplicate is ever queued.
        """
        with self._lock:
            ep = self.get_endpoint(endpoint_id)
            if ep["status"] == "disabled":
                raise Conflict(f"endpoint {endpoint_id} is disabled")
            if event_id is not None:
                existing = self._event_by_id(event_id)
                if existing is not None:
                    return existing, False
            if idempotency_key is not None:
                existing = self._event_by_idem(endpoint_id, idempotency_key)
                if existing is not None:
                    return existing, False

            eid = event_id or _new_id("evt")
            now = _now()
            self._db.execute("BEGIN IMMEDIATE")
            try:
                seq = self._db.execute(
                    "SELECT next_seq FROM endpoints WHERE id=?", (endpoint_id,)
                ).fetchone()["next_seq"]
                self._db.execute(
                    "UPDATE endpoints SET next_seq=next_seq+1 WHERE id=?", (endpoint_id,)
                )
                self._db.execute(
                    "INSERT INTO events(id, tenant_id, endpoint_id, object_key, event_type,"
                    " payload, key_version, seq, status, attempts, max_attempts,"
                    " next_attempt_at, idempotency_key, created_at, updated_at, delivered_at)"
                    " VALUES (?,?,?,?,?,?,?,?, 'pending', 0, ?, 0, ?, ?, ?, NULL)",
                    (
                        eid,
                        ep["tenant_id"],
                        endpoint_id,
                        object_key,
                        event_type,
                        payload,
                        ep["active_key_version"],
                        seq,
                        max_attempts,
                        idempotency_key,
                        now,
                        now,
                    ),
                )
                self._db.execute("COMMIT")
            except sqlite3.IntegrityError:
                # lost a race on id / idempotency key — return the winner
                self._db.execute("ROLLBACK")
                existing = self._event_by_id(eid) or self._event_by_idem(endpoint_id, idempotency_key)
                return existing, False
            return self._event_by_id(eid), True

    def _event_by_id(self, event_id: str):
        return _as_dict(self._db.execute("SELECT * FROM events WHERE id=?", (event_id,)).fetchone())

    def _event_by_idem(self, endpoint_id: str, idempotency_key: str):
        if idempotency_key is None:
            return None
        return _as_dict(
            self._db.execute(
                "SELECT * FROM events WHERE endpoint_id=? AND idempotency_key=?",
                (endpoint_id, idempotency_key),
            ).fetchone()
        )

    def get_event(self, event_id: str) -> dict:
        with self._lock:
            event = self._event_by_id(event_id)
            if event is None:
                raise NotFound(f"event {event_id} not found")
            return event

    def list_events(self, endpoint_id: str, status: str = None, limit: int = 100) -> list:
        with self._lock:
            if status:
                rows = self._db.execute(
                    "SELECT * FROM events WHERE endpoint_id=? AND status=? ORDER BY seq LIMIT ?",
                    (endpoint_id, status, limit),
                ).fetchall()
            else:
                rows = self._db.execute(
                    "SELECT * FROM events WHERE endpoint_id=? ORDER BY seq LIMIT ?",
                    (endpoint_id, limit),
                ).fetchall()
            return [dict(r) for r in rows]

    # -------------------------------------------------------------- scheduling
    def next_eligible(self, endpoint_id: str, excluded_keys, now: float):
        """Oldest pending head-of-line event whose object_key is not in flight.

        Only the minimum-seq pending event per object_key is a candidate, which
        is what enforces per-business-object FIFO.
        """
        with self._lock:
            rows = self._db.execute(
                """
                SELECT * FROM events e
                WHERE endpoint_id=? AND status='pending' AND next_attempt_at<=?
                  AND seq = (SELECT MIN(seq) FROM events
                             WHERE endpoint_id=e.endpoint_id
                               AND object_key=e.object_key
                               AND status='pending')
                ORDER BY seq LIMIT 32
                """,
                (endpoint_id, now),
            ).fetchall()
            for row in rows:
                if row["object_key"] not in excluded_keys:
                    return dict(row)
            return None

    def next_retry_time(self, endpoint_id: str):
        with self._lock:
            row = self._db.execute(
                "SELECT MIN(next_attempt_at) AS t FROM events"
                " WHERE endpoint_id=? AND status='pending' AND next_attempt_at>0",
                (endpoint_id,),
            ).fetchone()
            return row["t"] if row and row["t"] else None

    def claim(self, event_id: str) -> bool:
        with self._lock:
            cur = self._db.execute(
                "UPDATE events SET status='delivering', updated_at=? WHERE id=? AND status='pending'",
                (_now(), event_id),
            )
            return cur.rowcount == 1

    def record_attempt(self, event_id: str, n: int, http_status, error, duration_ms):
        with self._lock:
            self._db.execute(
                "INSERT INTO attempts(event_id, n, http_status, error, duration_ms, created_at)"
                " VALUES (?,?,?,?,?,?)",
                (event_id, n, http_status, error, duration_ms, _now()),
            )

    def mark_delivered(self, event_id: str):
        with self._lock:
            now = _now()
            self._db.execute(
                "UPDATE events SET status='delivered', delivered_at=?, updated_at=? WHERE id=?",
                (now, now, event_id),
            )

    def schedule_retry(self, event_id: str, attempts: int, next_attempt_at: float):
        with self._lock:
            self._db.execute(
                "UPDATE events SET status='pending', attempts=?, next_attempt_at=?, updated_at=?"
                " WHERE id=?",
                (attempts, next_attempt_at, _now(), event_id),
            )

    def mark_failed(self, event_id: str):
        with self._lock:
            self._db.execute(
                "UPDATE events SET status='failed', updated_at=? WHERE id=?",
                (_now(), event_id),
            )

    # ------------------------------------------------------------------ replay
    def replay(self, event_id: str):
        """Manual replay. Returns (event, requeued, receipt).

        A delivered event is a no-op: the original receipt is returned and no
        new delivery is triggered, so replay can never double-confirm. A failed
        event is re-queued under the *same* event id, so the receiver's
        idempotency check still deduplicates it.
        """
        with self._lock:
            event = self._event_by_id(event_id)
            if event is None:
                raise NotFound(f"event {event_id} not found")
            if event["status"] == "delivered":
                receipt = _as_dict(
                    self._db.execute(
                        "SELECT * FROM attempts WHERE event_id=?"
                        " AND http_status BETWEEN 200 AND 299 ORDER BY id DESC LIMIT 1",
                        (event_id,),
                    ).fetchone()
                )
                return event, False, receipt
            if event["status"] in ("pending", "delivering"):
                raise Conflict(f"event {event_id} is already in flight")
            self._db.execute(
                "UPDATE events SET status='pending', attempts=0, next_attempt_at=0, updated_at=?"
                " WHERE id=?",
                (_now(), event_id),
            )
            return self._event_by_id(event_id), True, None

    # ------------------------------------------------------------------ stats
    def list_attempts(self, event_id: str) -> list:
        with self._lock:
            rows = self._db.execute(
                "SELECT * FROM attempts WHERE event_id=? ORDER BY id", (event_id,)
            ).fetchall()
            return [dict(r) for r in rows]

    def queue_stats(self, endpoint_id: str) -> dict:
        with self._lock:
            stats = {"pending": 0, "delivering": 0, "delivered": 0, "failed": 0}
            rows = self._db.execute(
                "SELECT status, COUNT(*) AS c FROM events WHERE endpoint_id=? GROUP BY status",
                (endpoint_id,),
            ).fetchall()
            for row in rows:
                stats[row["status"]] = row["c"]
            return stats
