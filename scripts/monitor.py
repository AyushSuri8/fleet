"""External monitor: lease exists & fresh, heartbeat fresh, exactly one active
node, backlog below limit. Exit 1 -> red X on GitHub + optional webhook alert."""
import json, os, sys
from datetime import datetime, timedelta, timezone
from google.cloud import firestore

HEARTBEAT_MAX_AGE = int(os.environ.get("HEARTBEAT_MAX_AGE", 90))
LEASE_MAX_AGE = int(os.environ.get("LEASE_MAX_AGE", 60))
BACKLOG_LIMIT = int(os.environ.get("BACKLOG_LIMIT", 50))
WEEK_SECONDS = int(os.environ.get("WEEK_SECONDS", 604800))
QUOTA_SECONDS = int(os.environ.get("WEEKLY_QUOTA_SECONDS", 180000))


def aware(dt):
    return dt.replace(tzinfo=timezone.utc) if dt and dt.tzinfo is None else dt


def main():
    problems, notes = [], []
    db = firestore.Client(project=os.environ["GOOGLE_CLOUD_PROJECT"])
    now = datetime.now(timezone.utc)

    lease = db.collection("server").document("lease").get().to_dict() or {}
    active = lease.get("activeNode")
    fence = int(lease.get("fenceToken", 0))
    expires = aware(lease.get("leaseExpiresAt"))

    if not active:
        problems.append("NO-ACTIVE-NODE: lease has no activeNode")
    else:
        if not expires or expires < now - timedelta(seconds=LEASE_MAX_AGE):
            problems.append(f"LEASE-STALE: lease of {active} expired at {expires}")
        hb = db.collection("server").document(f"health-{active}").get().to_dict() or {}
        ts = aware(hb.get("timestamp"))
        if not ts or (now - ts).total_seconds() > HEARTBEAT_MAX_AGE:
            problems.append(f"HEARTBEAT-STALE: nothing from {active} in {HEARTBEAT_MAX_AGE}s")
        elif not hb.get("appHealthy", False):
            problems.append(f"APP-UNHEALTHY: {active} reports unhealthy")

    roster = []
    for snap in db.collection("server").stream():
        if not snap.id.startswith("health-"):
            continue
        d = snap.to_dict() or {}
        ts = aware(d.get("timestamp"))
        fresh = bool(ts) and (now - ts).total_seconds() <= HEARTBEAT_MAX_AGE
        node = d.get("node") or snap.id[len("health-"):]
        roster.append((node, fresh))
        # split-brain: any fresh healthy node besides active is suspect
        if fresh and active and node != active and d.get("appHealthy"):
            problems.append(f"SPLIT-BRAIN: {node} reports healthy but lease says {active}")

    notes.append("roster: " + ", ".join(f"{n}({'up' if f else 'down'})" for n, f in roster))

    # --- weekly quota: 4 x 50h must cover the 168h week (sum over ring, not just roster) ---
    try:
        quota_notes = []
        successors = 0
        # prefer ring order if lease has ring info; fallback to discovered usage docs
        ring = (lease.get("ring") or []) if isinstance(lease.get("ring"), list) else []
        usage_nodes = set()
        for snap in db.collection("server").stream():
            if not snap.id.startswith("usage-"):
                continue
            node = snap.id[len("usage-"):]
            usage_nodes.add(node)
            d = snap.to_dict() or {}
            start = aware(d.get("weekStart"))
            used = float(d.get("activeSeconds", 0.0) or 0.0)
            left = max(0.0, QUOTA_SECONDS - used)
            reset = ""
            if start and (now - start).total_seconds() >= WEEK_SECONDS:
                left = QUOTA_SECONDS
                reset = " (week expired, resets on next loop)"
            quota_notes.append(f"{node}: {left/3600:.1f}h left{reset}")
            if left > 0 and node != active:
                successors += 1
        # also consider ring members that never created usage doc yet (full quota)
        for rn in ring:
            if rn not in usage_nodes:
                quota_notes.append(f"{rn}: 50.0h left (no usage doc)")
                if rn != active:
                    successors += 1
        if quota_notes:
            notes.append("quota: " + ", ".join(quota_notes))
            if active:
                # active-node low quota with no successor = coming outage
                au = db.collection("server").document(f"usage-{active}").get().to_dict() or {}
                aleft_start = aware(au.get("weekStart"))
                au_used = float(au.get("activeSeconds", 0.0) or 0.0)
                if aleft_start and (now - aleft_start).total_seconds() >= WEEK_SECONDS:
                    aleft = QUOTA_SECONDS
                else:
                    aleft = QUOTA_SECONDS - au_used
                if aleft < 3600 and successors == 0:
                    problems.append(
                        f"QUOTA-LOW: {active} has {aleft/3600:.1f}h left and no successor with quota")
            # fleet low: sum over all known nodes (usage + ring)
            all_nodes = usage_nodes | set(ring) if ring else usage_nodes
            if not all_nodes:
                all_nodes = {n for n, _ in roster} or usage_nodes
            total_left = 0.0
            for n in all_nodes:
                ud = db.collection("server").document(f"usage-{n}").get().to_dict() or {}
                st = aware(ud.get("weekStart"))
                used = float(ud.get("activeSeconds", 0.0) or 0.0)
                if st and (now - st).total_seconds() >= WEEK_SECONDS:
                    total_left += QUOTA_SECONDS
                else:
                    total_left += max(0.0, QUOTA_SECONDS - used)
                # nodes with no doc count as full quota
                if n not in usage_nodes:
                    total_left = QUOTA_SECONDS if not ud else total_left
            if total_left < 24 * 3600:
                problems.append(f"QUOTA-FLEET-LOW: only {total_left/3600:.1f}h left across fleet")
    except Exception as e:
        notes.append(f"quota check skipped: {e}")

    try:
        # bounded scan for backlog
        queued = sum(1 for _ in db.collection("requests").where("status", "==", "queued").limit(BACKLOG_LIMIT + 1).stream())
    except Exception:
        queued = 0
    notes.append(f"queued jobs: {queued}")
    if queued > BACKLOG_LIMIT:
        problems.append(f"BACKLOG: {queued} queued > limit {BACKLOG_LIMIT}")

    print(json.dumps({"ok": not problems, "problems": problems, "notes": notes}, indent=2))
    if problems:
        url = os.environ.get("ALERT_WEBHOOK_URL")
        if url:
            try:
                import urllib.request
                req = urllib.request.Request(
                    url,
                    data=json.dumps({"content": "fleet alert: " + "; ".join(problems)}).encode(),
                    headers={"content-type": "application/json"})
                urllib.request.urlopen(req, timeout=10)
            except Exception as e:
                print("alert webhook failed:", e)
        sys.exit(1)


if __name__ == "__main__":
    main()