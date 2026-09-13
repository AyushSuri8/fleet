"""Firestore-backed job queue: claim, checkpoint, complete, fail, reap.
Implements outline sections 15, 16, 29, 30.

Note: queries avoid composite indexes on purpose (client-side sort/filter)
so the system works with zero manual index setup."""
import logging
import time
from datetime import timedelta

from google.api_core.exceptions import Aborted, FailedPrecondition
from google.cloud.firestore import transactional

from common import utcnow, as_aware

log = logging.getLogger("jobs")


class StaleWorker(Exception):
    """Raised when this worker's fence token is stale (section 7)."""


def _assert_mine(data, node_id, fence_token):
    if data.get("leaseOwner") != node_id or int(data.get("fenceToken", -1)) != int(fence_token):
        raise StaleWorker(
            f"stale write rejected: owner={data.get('leaseOwner')} "
            f"fence={data.get('fenceToken')} (mine={node_id}/{fence_token})"
        )


class JobQueue:
    def __init__(self, db, cfg):
        q = cfg["queue"]
        self.db = db
        self.node_id = cfg["node"]["id"]
        self.col = db.collection(q["requests_collection"])
        self.lease_ref = db.collection(cfg["lease"]["collection"]).document(cfg["lease"]["doc_id"])
        self.job_lease = int(q["job_lease_seconds"])
        self.max_attempts = int(q["max_attempts"])

    def _txn(self, mutate, retries=5):
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
        raise RuntimeError("job transaction kept aborting")

    def list_queued(self, limit=5):
        # Bounded query; requires no index if limit small but order_by needs index for large scale.
        # Keep zero-index mode by fetching limit*2 and client-sorting as fallback.
        try:
            return list(self.col.where("status", "==", "queued").order_by("createdAt").limit(limit).stream())
        except Exception:
            docs = [s for s in self.col.where("status", "==", "queued").limit(limit * 10).stream()]
            docs.sort(key=lambda s: as_aware(s.to_dict().get("createdAt")) or utcnow())
            return docs[:limit]

    def claim(self, job_ref, fence_token):
        """Atomically claim a queued job, verifying we still hold the lease (section 7)."""
        now = utcnow()

        def mutate(txn):
            ldata = self.lease_ref.get(transaction=txn).to_dict() or {}
            if (ldata.get("activeNode") != self.node_id
                    or int(ldata.get("fenceToken", -1)) != int(fence_token)):
                return None  # lost lease between loop start and claim; do not take work

            snap = job_ref.get(transaction=txn)
            data = snap.to_dict() or {}
            if data.get("status") != "queued":
                return None  # idempotency: only queued jobs are claimable

            attempts = int(data.get("attempts", 0))
            if attempts >= self.max_attempts:
                data.update(status="failed", failedAt=now, leaseOwner=None,
                            lastError="max attempts exceeded")
                txn.set(job_ref, data)
                return None

            data.update(
                status="processing",
                leaseOwner=self.node_id,
                fenceToken=fence_token,
                leaseExpiresAt=now + timedelta(seconds=self.job_lease),
                claimedAt=now,
                attempts=attempts + 1,
            )
            txn.set(job_ref, data)
            return data

        return self._txn(mutate)

    def checkpoint(self, job_ref, fence_token, checkpoint):
        """Persist resumable progress and extend lease (section 15)."""
        now = utcnow()

        def mutate(txn):
            ldata = self.lease_ref.get(transaction=txn).to_dict() or {}
            if ldata.get("activeNode") != self.node_id or int(ldata.get("fenceToken", -1)) != int(fence_token):
                raise StaleWorker(f"stale checkpoint: lease {ldata.get('activeNode')}/{ldata.get('fenceToken')} mine {self.node_id}/{fence_token}")
            data = job_ref.get(transaction=txn).to_dict() or {}
            _assert_mine(data, self.node_id, fence_token)
            data.update(checkpoint=checkpoint, checkpointAt=now,
                        leaseExpiresAt=now + timedelta(seconds=self.job_lease))
            txn.set(job_ref, data)

        self._txn(mutate)

    def complete(self, job_ref, fence_token, result):
        now = utcnow()

        def mutate(txn):
            ldata = self.lease_ref.get(transaction=txn).to_dict() or {}
            if ldata.get("activeNode") != self.node_id or int(ldata.get("fenceToken", -1)) != int(fence_token):
                raise StaleWorker(f"stale complete: lease {ldata.get('activeNode')}/{ldata.get('fenceToken')} mine {self.node_id}/{fence_token}")
            data = job_ref.get(transaction=txn).to_dict() or {}
            _assert_mine(data, self.node_id, fence_token)
            if data.get("status") == "completed":
                return data  # idempotent completion
            data.update(status="completed", result=result, completedAt=now,
                        leaseOwner=None, leaseExpiresAt=None)
            txn.set(job_ref, data)
            return data

        return self._txn(mutate)

    def fail(self, job_ref, fence_token, error, retryable=True):
        now = utcnow()

        def mutate(txn):
            ldata = self.lease_ref.get(transaction=txn).to_dict() or {}
            if ldata.get("activeNode") != self.node_id or int(ldata.get("fenceToken", -1)) != int(fence_token):
                raise StaleWorker(f"stale fail: lease {ldata.get('activeNode')}/{ldata.get('fenceToken')} mine {self.node_id}/{fence_token}")
            data = job_ref.get(transaction=txn).to_dict() or {}
            _assert_mine(data, self.node_id, fence_token)
            attempts = int(data.get("attempts", 0))
            if retryable and attempts < self.max_attempts:
                data.update(status="queued", lastError=str(error)[:1000],
                            leaseOwner=None, leaseExpiresAt=None)
            else:
                data.update(status="failed", failedAt=now, lastError=str(error)[:1000],
                            leaseOwner=None, leaseExpiresAt=None)
            txn.set(job_ref, data)

        self._txn(mutate)

    def reap_stale(self):
        """Return expired 'processing' jobs to the queue (sections 11.2, 28.1).
        Safe for any node to run; requeue is transactional and re-checks expiry."""
        now = utcnow()
        requeued = 0
        # bounded scan to avoid O(N) cost
        for snap in self.col.where("status", "==", "processing").limit(50).stream():
            d = snap.to_dict() or {}
            exp = as_aware(d.get("leaseExpiresAt"))
            if exp is None or exp >= now:
                continue

            def mutate(txn, ref=snap.reference):
                cur = ref.get(transaction=txn).to_dict() or {}
                cur_exp = as_aware(cur.get("leaseExpiresAt"))
                # re-check with fresh now inside txn to handle lease extension via checkpoint
                if cur.get("status") == "processing" and cur_exp and cur_exp < utcnow():
                    cur.update(status="queued", leaseOwner=None, leaseExpiresAt=None,
                               lastError="job lease expired; requeued")
                    txn.set(ref, cur)
                    return True
                return False

            try:
                if self._txn(mutate):
                    requeued += 1
                    log.info("reaped stale job %s", snap.id)
            except Exception as e:
                log.warning("reap failed for %s: %s", snap.id, e)
        return requeued