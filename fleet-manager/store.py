"""SQLite state for the fleet manager (stdlib only).

Holds what Firestore used to: the 50h/week quota ledger, the job queue
(claim/lease/checkpoint/complete/fail/reap), node liveness, and an event log
for the status endpoint. The manager is the single writer, so plain SQL
transactions replace fence tokens."""
import json
import sqlite3
import threading
import time

SCHEMA = """
CREATE TABLE IF NOT EXISTS nodes (
  node TEXT PRIMARY KEY,
  week_start REAL,
  active_seconds REAL NOT NULL DEFAULT 0,
  active_since REAL,
  last_seen REAL,
  vm_state TEXT
);
CREATE TABLE IF NOT EXISTS jobs (
  id TEXT PRIMARY KEY,
  type TEXT NOT NULL,
  payload TEXT NOT NULL DEFAULT '{}',
  status TEXT NOT NULL DEFAULT 'queued',
  attempts INTEGER NOT NULL DEFAULT 0,
  max_attempts INTEGER NOT NULL DEFAULT 3,
  checkpoint TEXT, result TEXT, error TEXT,
  created_at REAL, claimed_at REAL,
  lease_expires_at REAL, claimed_by TEXT, completed_at REAL
);
CREATE TABLE IF NOT EXISTS kv (k TEXT PRIMARY KEY, v TEXT);
CREATE TABLE IF NOT EXISTS events (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  at REAL, kind TEXT, detail TEXT
);
"""


class Store:
    def __init__(self, path):
        self._lock = threading.RLock()
        self._db = sqlite3.connect(path, check_same_thread=False)
        self._db.row_factory = sqlite3.Row
        with self._lock:
            self._db.executescript(SCHEMA)
            self._db.execute("PRAGMA journal_mode=WAL")
            self._db.commit()
        self.job_available = threading.Condition(self._lock)

    # ---- kv / events --------------------------------------------------

    def kv_get(self, k):
        with self._lock:
            row = self._db.execute("SELECT v FROM kv WHERE k=?", (k,)).fetchone()
        return row["v"] if row else None

    def kv_set(self, k, v):
        with self._lock:
            self._db.execute("INSERT INTO kv(k,v) VALUES(?,?) "
                             "ON CONFLICT(k) DO UPDATE SET v=excluded.v", (k, str(v)))
            self._db.commit()

    def event(self, kind, detail):
        with self._lock:
            self._db.execute("INSERT INTO events(at, kind, detail) VALUES(?,?,?)",
                             (time.time(), kind, str(detail)[:500]))
            self._db.commit()

    def recent_events(self, n=15):
        with self._lock:
            rows = self._db.execute(
                "SELECT at, kind, detail FROM events ORDER BY id DESC LIMIT ?", (n,)).fetchall()
        return [{"at": r["at"], "kind": r["kind"], "detail": r["detail"]} for r in rows]

    # ---- nodes: liveness, quota ledger ---------------------------------

    def heartbeat(self, node):
        now = time.time()
        with self._lock:
            self._db.execute("INSERT INTO nodes(node, last_seen) VALUES(?,?) "
                             "ON CONFLICT(node) DO UPDATE SET last_seen=excluded.last_seen",
                             (node, now))
            self._db.commit()

    def heartbeat_age(self, node):
        with self._lock:
            row = self._db.execute("SELECT last_seen FROM nodes WHERE node=?", (node,)).fetchone()
        if not row or row["last_seen"] is None:
            return None
        return time.time() - row["last_seen"]

    def set_vm_state(self, node, state):
        with self._lock:
            self._db.execute("INSERT INTO nodes(node, vm_state) VALUES(?,?) "
                             "ON CONFLICT(node) DO UPDATE SET vm_state=excluded.vm_state",
                             (node, state))
            self._db.commit()

    def active_since(self, node):
        with self._lock:
            row = self._db.execute("SELECT active_since FROM nodes WHERE node=?", (node,)).fetchone()
        return row["active_since"] if row else None

    def set_active(self, node):
        now = time.time()
        with self._lock:
            self._db.execute("UPDATE nodes SET active_since=NULL")
            self._db.execute("INSERT INTO nodes(node, active_since) VALUES(?,?) "
                             "ON CONFLICT(node) DO UPDATE SET active_since=excluded.active_since",
                             (node, now))
            self._db.commit()

    def add_active_time(self, node, seconds, week_seconds):
        """Accrue active time; resets the node's week when it expires."""
        now = time.time()
        with self._lock:
            row = self._db.execute(
                "SELECT week_start FROM nodes WHERE node=?", (node,)).fetchone()
            if row is None or row["week_start"] is None:
                self._db.execute("INSERT INTO nodes(node, week_start, active_seconds) "
                                 "VALUES(?,?,?) ON CONFLICT(node) DO UPDATE SET "
                                 "week_start=excluded.week_start, active_seconds=excluded.active_seconds",
                                 (node, now, seconds))
            elif now - row["week_start"] >= week_seconds:
                self._db.execute("UPDATE nodes SET week_start=?, active_seconds=? WHERE node=?",
                                 (now, seconds, node))
            else:
                self._db.execute("UPDATE nodes SET active_seconds=active_seconds+? WHERE node=?",
                                 (seconds, node))
            self._db.commit()

    def remaining(self, node, quota_s, week_s):
        now = time.time()
        with self._lock:
            row = self._db.execute(
                "SELECT week_start, active_seconds FROM nodes WHERE node=?", (node,)).fetchone()
        if not row or row["week_start"] is None or now - row["week_start"] >= week_s:
            return quota_s
        return max(0.0, quota_s - float(row["active_seconds"] or 0))

    def nodes_summary(self, ring, quota_s, week_s):
        now = time.time()
        with self._lock:
            rows = {r["node"]: dict(r)
                    for r in self._db.execute("SELECT * FROM nodes").fetchall()}
        out = []
        for n in ring:
            d = rows.get(n) or {}
            left = self.remaining(n, quota_s, week_s)
            seen = d.get("last_seen")
            out.append({"node": n,
                        "quotaLeftH": round(left / 3600, 1),
                        "active": bool(d.get("active_since")),
                        "lastSeenAgeS": None if not seen else int(now - seen),
                        "vmState": d.get("vm_state")})
        return out

    # ---- jobs -----------------------------------------------------------

    def enqueue_job(self, jid, jtype, payload, max_attempts=3):
        with self.job_available:
            self._db.execute(
                "INSERT INTO jobs(id, type, payload, status, attempts, max_attempts, created_at) "
                "VALUES(?,?,?,'queued',0,?,?)",
                (jid, jtype, json.dumps(payload), max_attempts, time.time()))
            self._db.commit()
            self.job_available.notify_all()

    def get_job(self, jid):
        with self._lock:
            row = self._db.execute("SELECT * FROM jobs WHERE id=?", (jid,)).fetchone()
        if not row:
            return None
        d = dict(row)
        for k in ("payload", "checkpoint", "result"):
            if d.get(k):
                try:
                    d[k] = json.loads(d[k])
                except json.JSONDecodeError:
                    pass
        return d

    def job_counts(self):
        with self._lock:
            rows = self._db.execute(
                "SELECT status, COUNT(*) AS n FROM jobs GROUP BY status").fetchall()
        return {r["status"]: r["n"] for r in rows}

    def claim_job(self, node, active_node, allow_any, lease_s, wait_s):
        """Long-poll claim. Only the active node gets jobs unless allow_any."""
        deadline = time.time() + wait_s
        with self.job_available:
            while True:
                job = self._claim_one(node, active_node, allow_any, lease_s)
                if job is not None:
                    return job
                remaining = deadline - time.time()
                if remaining <= 0:
                    return None
                self.job_available.wait(timeout=min(remaining, 2.0))

    def _claim_one(self, node, active_node, allow_any, lease_s):
        if not allow_any and node != active_node:
            return None
        row = self._db.execute(
            "SELECT id FROM jobs WHERE status='queued' ORDER BY created_at LIMIT 1").fetchone()
        if row is None:
            return None
        now = time.time()
        self._db.execute(
            "UPDATE jobs SET status='processing', claimed_by=?, claimed_at=?, "
            "lease_expires_at=?, attempts=attempts+1 WHERE id=?",
            (node, now, now + lease_s, row["id"]))
        self._db.commit()
        jid = row["id"]
        return {k: (json.loads(v) if k in ("payload", "checkpoint") and v else v)
                for k, v in dict(self._db.execute(
                    "SELECT * FROM jobs WHERE id=?", (jid,)).fetchone()).items()}

    def complete_job(self, jid, node, result):
        now = time.time()
        with self._lock:
            row = self._db.execute(
                "SELECT status, claimed_by FROM jobs WHERE id=?", (jid,)).fetchone()
            if not row or row["status"] != "processing" or row["claimed_by"] != node:
                return False
            self._db.execute(
                "UPDATE jobs SET status='completed', result=?, completed_at=?, "
                "claimed_by=NULL, lease_expires_at=NULL WHERE id=?",
                (json.dumps(result, default=str), now, jid))
            self._db.commit()
            return True

    def fail_job(self, jid, node, error, retryable=True):
        now = time.time()
        with self._lock:
            row = self._db.execute(
                "SELECT status, claimed_by, attempts, max_attempts FROM jobs WHERE id=?",
                (jid,)).fetchone()
            if not row or row["status"] != "processing" or row["claimed_by"] != node:
                return False
            if retryable and row["attempts"] < row["max_attempts"]:
                self._db.execute(
                    "UPDATE jobs SET status='queued', claimed_by=NULL, "
                    "lease_expires_at=NULL, error=? WHERE id=?", (error, jid))
            else:
                self._db.execute(
                    "UPDATE jobs SET status='failed', claimed_by=NULL, "
                    "lease_expires_at=NULL, error=?, completed_at=? WHERE id=?",
                    (error, now, jid))
            self._db.commit()
            return True

    def checkpoint_job(self, jid, node, checkpoint, lease_s):
        now = time.time()
        with self._lock:
            row = self._db.execute(
                "SELECT status, claimed_by FROM jobs WHERE id=?", (jid,)).fetchone()
            if not row or row["status"] != "processing" or row["claimed_by"] != node:
                return False
            self._db.execute(
                "UPDATE jobs SET checkpoint=?, lease_expires_at=? WHERE id=?",
                (json.dumps(checkpoint, default=str), now + lease_s, jid))
            self._db.commit()
            return True

    def reap_expired(self, now=None):
        """Return expired 'processing' jobs to the queue (or fail at max attempts)."""
        now = now or time.time()
        requeued = failed = 0
        with self._lock:
            rows = self._db.execute(
                "SELECT id, attempts, max_attempts FROM jobs "
                "WHERE status='processing' AND lease_expires_at < ?", (now,)).fetchall()
            for r in rows:
                if r["attempts"] < r["max_attempts"]:
                    self._db.execute(
                        "UPDATE jobs SET status='queued', claimed_by=NULL, "
                        "lease_expires_at=NULL, error='lease expired; requeued' WHERE id=?",
                        (r["id"],))
                    requeued += 1
                else:
                    self._db.execute(
                        "UPDATE jobs SET status='failed', claimed_by=NULL, "
                        "lease_expires_at=NULL, error='lease expired' WHERE id=?", (r["id"],))
                    failed += 1
            self._db.commit()
        return requeued, failed
