"""Weekly quota accounting for 24/7 rotation across 4 Cloud Shell accounts.

Model (per user request):
- Same shell keeps running; if it stops (12h cap / disconnect), restart THE SAME
  shell until its 50h weekly quota is exhausted.
- Only then hand off to the next shell with remaining quota.
- After 168h from weekStart, a shell's quota refreshes and it is eligible again.
- 4 x 50h = 200h > 168h needed for full-week coverage.

Stored in Firestore `server/usage-<node>`:
    {weekStart: timestamp, activeSeconds: float, quotaSeconds: int}
"""
import logging

from common import utcnow, as_aware

log = logging.getLogger("usage")


def _usage_ref(db, cfg, node_id):
    col = cfg["lease"]["collection"]
    # reuse lease collection ("server") so no new collection / rules needed
    return db.collection(col).document(f"usage-{node_id}")


def load(db, cfg, node_id):
    """Return (week_start, active_seconds). Resets week if expired."""
    rot = cfg.get("rotation", {})
    week_seconds = int(rot.get("week_seconds", 604800))
    quota = int(rot.get("weekly_quota_seconds", 180000))
    ref = _usage_ref(db, cfg, node_id)
    snap = ref.get()
    now = utcnow()
    if not snap.exists:
        ref.set({"weekStart": now, "activeSeconds": 0.0, "quotaSeconds": quota})
        return now, 0.0
    d = snap.to_dict() or {}
    start = as_aware(d.get("weekStart")) or now
    active = float(d.get("activeSeconds", 0.0) or 0.0)
    if (now - start).total_seconds() >= week_seconds:
        ref.set({"weekStart": now, "activeSeconds": 0.0, "quotaSeconds": quota})
        log.info("quota week reset for %s", node_id)
        return now, 0.0
    return start, active


def remaining_seconds(db, cfg, node_id):
    quota = int(cfg.get("rotation", {}).get("weekly_quota_seconds", 180000))
    _, active = load(db, cfg, node_id)
    return max(0.0, quota - active)


def add_active(db, cfg, node_id, seconds):
    """Accumulate active time; caller passes loop elapsed seconds."""
    if seconds <= 0:
        return
    quota = int(cfg.get("rotation", {}).get("weekly_quota_seconds", 180000))
    ref = _usage_ref(db, cfg, node_id)
    # load() first so an expired week resets before we increment
    load(db, cfg, node_id)
    from google.cloud.firestore import transactional
    from google.api_core.exceptions import Aborted, FailedPrecondition
    import time, random

    def mutate(txn):
        snap = ref.get(transaction=txn)
        d = snap.to_dict() or {}
        cur = float(d.get("activeSeconds", 0.0) or 0.0)
        d.update(activeSeconds=cur + float(seconds), quotaSeconds=quota)
        if "weekStart" not in d:
            d["weekStart"] = utcnow()
        txn.set(ref, d)

    for attempt in range(5):
        txn = db.transaction()

        @transactional
        def wrapper(txn):
            return mutate(txn)

        try:
            wrapper(txn)
            return
        except (Aborted, FailedPrecondition):
            time.sleep(random.uniform(0, 0.3 * (2 ** attempt)))
        except Exception as e:
            log.warning("usage increment failed for %s: %s", node_id, e)
            return
    log.warning("usage increment aborted after retries for %s", node_id)


def pick_next_with_quota(db, cfg, ring, exclude=None):
    """Return next node in ring order with remaining quota, or None."""
    if not ring:
        return None
    best = None
    for node in ring:
        if node == exclude:
            continue
        try:
            if remaining_seconds(db, cfg, node) > 0:
                return node  # ring order, first eligible wins (predictable)
        except Exception as e:
            log.warning("quota check failed for %s: %s", node, e)
            if best is None:
                best = node
    return best
