"""Lease lock with fence tokens and atomic takeover via Firestore transactions.
Implements outline sections 6, 7, 11."""
import logging
import time
from datetime import timedelta

from google.api_core.exceptions import Aborted, FailedPrecondition
from google.cloud.firestore import transactional

from common import utcnow, as_aware

log = logging.getLogger("lease")


class LeaseManager:
    def __init__(self, db, node_id, collection="server", doc_id="lease",
                 ttl_seconds=180, grace_seconds=45, ring=None):
        self.db = db
        self.node_id = node_id
        self.ref = db.collection(collection).document(doc_id)
        self.ttl = int(ttl_seconds)
        self.grace = int(grace_seconds)
        self.ring = ring or []

    def _read_in(self, txn):
        snap = self.ref.get(transaction=txn)
        return snap.to_dict() or {}

    def _run_txn(self, mutate, retries=5):
        import random
        from google.api_core.exceptions import DeadlineExceeded, ServiceUnavailable

        for attempt in range(retries):
            txn = self.db.transaction()

            @transactional
            def wrapper(txn):
                return mutate(txn)

            try:
                return wrapper(txn)
            except (Aborted, FailedPrecondition, DeadlineExceeded, ServiceUnavailable):
                time.sleep(random.uniform(0, 0.3 * (2 ** attempt)))
        raise RuntimeError("lease transaction kept aborting")

    def read(self):
        snap = self.ref.get()
        return snap.to_dict() if snap.exists else None

    def try_acquire(self):
        """Acquire if expired, renew if I hold it, else None. Atomic. Returns lease dict or None."""
        now = utcnow()

        def mutate(txn):
            data = self._read_in(txn)
            holder = data.get("activeNode")
            expires = as_aware(data.get("leaseExpiresAt"))
            pref = data.get("preferredNextNode")
            pref_at = as_aware(data.get("preferredAt"))

            if holder == self.node_id:
                # I already hold it -> renew (section 6 supervisor loop)
                data.update(
                    leaseExpiresAt=now + timedelta(seconds=self.ttl),
                    lastHeartbeat=now,
                    status="active",
                )
                txn.set(self.ref, data)
                return data

            # planned-rotation fairness (section 11.1): defer to preferred node
            # for a grace window unless the window passed or it is dead.
            if (pref and pref != self.node_id and pref_at
                    and (now - pref_at).total_seconds() < self.grace):
                return None

            if expires is None or expires <= now:
                # atomic takeover: only this transaction wins, fence increments
                data.update(
                    activeNode=self.node_id,
                    leaseExpiresAt=now + timedelta(seconds=self.ttl),
                    lastHeartbeat=now,
                    status="active",
                    acquiredAt=now,
                    fenceToken=int(data.get("fenceToken", 0)) + 1,
                    preferredNextNode=None,
                    preferredAt=None,
                )
                log.info("acquired lease fenceToken=%s", data["fenceToken"])
                txn.set(self.ref, data)
                return data

            return None

        return self._run_txn(mutate)

    def release(self, reason="planned", preferred=None):
        """Graceful handoff. preferred=X gives X first claim during grace.

        - quota-exhausted handoff: preferred = next shell with quota left.
        - clean restart/shutdown: preferred = self, so the SAME shell may
          reclaim within reclaim_grace_seconds; others take over after.
        - preferred=None: legacy next-in-ring behaviour.
        """
        now = utcnow()

        def mutate(txn):
            data = self._read_in(txn)
            if data.get("activeNode") != self.node_id:
                return False
            nxt = preferred
            if nxt is None and self.node_id in self.ring:
                i = self.ring.index(self.node_id)
                nxt = self.ring[(i + 1) % len(self.ring)]
            data.update(
                activeNode=None,
                status="standby",
                leaseExpiresAt=now,
                releasedAt=now,
                releaseReason=reason,
                requestRotation=False,
                preferredNextNode=nxt,
                preferredAt=now,
            )
            txn.set(self.ref, data)
            return True

        ok = self._run_txn(mutate)
        if ok:
            log.info("released lease (%s)", reason)
        return ok

    def force_expire(self):
        """Manual failover helper for the runbook (section 32)."""
        now = utcnow()

        def mutate(txn):
            data = self._read_in(txn)
            data.update(leaseExpiresAt=now - timedelta(seconds=1))
            txn.set(self.ref, data)

        self._run_txn(mutate)
        log.info("lease forced to expire")