"""Supervisor: lease lifecycle, server subprocess, heartbeats, quota rotation.

24/7 model: STICKY-QUOTA across 4 Cloud Shell accounts (4 x 50h = 200h/week
covers the 168h week).
- Same shell keeps the server; if it stops (12h session cap / disconnect),
  restart THE SAME shell — it reclaims the lease within the reclaim grace.
- Only when its weekly quota is exhausted does it hand off to the next
  shell with quota left. After week_seconds (168h) its quota refreshes.
- Timed round-robin (max_active_seconds) is legacy; set >0 to re-enable.
"""
import json
import logging
import signal
import subprocess
import sys
import time
from pathlib import Path

from common import load_config, setup_logging, get_db, utcnow
from lease_manager import LeaseManager
import heartbeat
import usage_tracker

running = True


def _stop(signum, frame):
    global running
    running = False


def status_path(cfg):
    p = cfg.get("server", {}).get("status_file") or cfg["worker"]["status_file"]
    return str(Path(p).expanduser()) if p.startswith("~") else p


def read_local_health(cfg):
    f = Path(status_path(cfg))
    if not f.exists():
        return None
    try:
        return json.loads(f.read_text())
    except Exception:
        return None


def server_cmd(cfg):
    """Configurable server workload: 'keepalive' or 'worker' (default)."""
    mode = (cfg.get("server", {}).get("command") or "worker").lower()
    base = Path(__file__).parent
    if mode == "keepalive":
        return [sys.executable, str(base / "keepalive.py")]
    return [sys.executable, str(base / "worker.py")]


def main():
    cfg = load_config()
    setup_logging(cfg["node"]["id"])
    log = logging.getLogger("supervisor")
    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)

    db = get_db(cfg)
    ring = cfg["rotation"].get("order", [])
    reclaim_grace = int(cfg["rotation"].get("reclaim_grace_seconds", 90))
    take_grace = int(cfg["lease"].get("takeover_grace_seconds", 45))
    lease = LeaseManager(
        db, cfg["node"]["id"],
        cfg["lease"]["collection"], cfg["lease"]["doc_id"],
        cfg["lease"]["ttl_seconds"],
        # FIX: defer-to-preferred window must cover the SAME-SHELL reclaim
        # grace (90s), not just takeover grace (45s) — otherwise another node
        # grabs the lease 45s into the documented 90s reclaim window.
        # (reclaim_grace was previously read here and never used.)
        max(take_grace, reclaim_grace),
        ring=ring,
    )
    poll = int(cfg["lease"]["poll_interval_seconds"])
    hb_every = int(cfg["heartbeat"]["interval_seconds"])
    rotation_on = bool(cfg["rotation"].get("enabled", True))
    max_active = int(cfg["rotation"].get("max_active_seconds", 0) or 0)
    quota = int(cfg["rotation"].get("weekly_quota_seconds", 180000))

    proc = None
    proc_started_at = 0.0
    fence = 0
    last_hb = 0.0
    last_idle_log = 0.0
    active_since = None
    draining = False
    last_loop = time.monotonic()
    usage_flush_every = 60.0   # FIX: batch usage writes (1/min, not 1/10s)
    pending_active = 0.0

    def flush_usage():
        nonlocal pending_active
        if pending_active > 0:
            try:
                usage_tracker.add_active(db, cfg, cfg["node"]["id"], pending_active)
            except Exception:
                log.warning("usage flush failed", exc_info=True)
            pending_active = 0.0

    def start_server():
        cmd = server_cmd(cfg)
        log.info("starting server: %s", " ".join(cmd))
        return subprocess.Popen(cmd)

    def stop_server(timeout=60):
        nonlocal proc
        if proc and proc.poll() is None:
            proc.terminate()  # child finishes current work, then exits
            try:
                proc.wait(timeout=timeout)
            except subprocess.TimeoutExpired:
                log.warning("server did not exit in %ss; killing", timeout)
                proc.kill()
                proc.wait(timeout=10)
        proc = None

    log.info("supervisor starting node=%s mode=%s", cfg["node"]["id"],
             cfg.get("server", {}).get("command", "worker"))

    while running:
        now = time.monotonic()
        elapsed = now - last_loop
        last_loop = now
        try:
            status = read_local_health(cfg)
            try:
                my_remaining = usage_tracker.remaining_seconds(db, cfg, cfg["node"]["id"])
            except Exception as e:
                log.warning("quota check failed, assuming quota left: %s", e)
                my_remaining = quota

            if rotation_on and my_remaining <= 0:
                # My week is spent: stay standby, let next shell serve.
                if active_since is not None:
                    log.info("quota exhausted; handing off")
                    stop_server()
                    flush_usage()
                    nxt = usage_tracker.pick_next_with_quota(
                        db, cfg, ring, exclude=cfg["node"]["id"])
                    lease.release(reason="quota-exhausted", preferred=nxt)
                    active_since = None
                    fence = 0
                data = None
            else:
                data = lease.try_acquire()  # renew-if-mine / take-if-expired

            if data and data.get("activeNode") == cfg["node"]["id"]:
                fence = int(data.get("fenceToken", 0))
                if active_since is None:
                    active_since = utcnow()
                    draining = False
                    log.info("ACTIVE fenceToken=%s quota_left_h=%.1f",
                             fence, my_remaining / 3600.0)
                # account active time toward weekly quota (batched)
                pending_active += max(0.0, min(elapsed, poll * 2))
                if pending_active >= usage_flush_every:
                    try:
                        usage_tracker.add_active(db, cfg, cfg["node"]["id"], pending_active)
                        pending_active = 0.0
                        my_remaining = usage_tracker.remaining_seconds(
                            db, cfg, cfg["node"]["id"])
                    except Exception as e:
                        log.warning("usage accounting failed: %s", e)

                overdue = (rotation_on and max_active > 0 and active_since and
                           (utcnow() - active_since).total_seconds() >= max_active)
                requested = bool(data.get("requestRotation"))
                quota_hit = rotation_on and my_remaining <= 0

                if (overdue or requested or quota_hit) and not draining:
                    log.info("rotation due (overdue=%s requested=%s quota_hit=%s): draining",
                             overdue, requested, quota_hit)
                    draining = True
                    stop_server()

                if draining:
                    if proc is None:
                        reason = "quota-exhausted" if quota_hit else "planned-rotation"
                        nxt = usage_tracker.pick_next_with_quota(
                            db, cfg, ring, exclude=cfg["node"]["id"])
                        if nxt is None and not quota_hit:
                            nxt = cfg["node"]["id"]  # no successor; allow self-reclaim
                        flush_usage()
                        lease.release(reason=reason, preferred=nxt)
                        active_since = None
                        fence = 0
                else:
                    if proc is None or proc.poll() is not None:
                        proc = start_server()
                        proc_started_at = time.monotonic()
                    elif time.monotonic() - proc_started_at > 30:
                        state = (status or {}).get("state")
                        if state in ("error", "stopped", None):
                            log.warning("server unhealthy (state=%s); restarting", state)
                            stop_server(timeout=15)
                            proc = start_server()
                            proc_started_at = time.monotonic()
            else:
                if active_since is not None:
                    log.info("lost lease; going standby")
                flush_usage()
                active_since = None
                fence = 0
                if proc is not None:
                    stop_server(timeout=15)
        except Exception:
            log.exception("supervisor loop error")

        if now - last_hb >= hb_every:
            try:
                try:
                    qleft = usage_tracker.remaining_seconds(db, cfg, cfg["node"]["id"])
                except Exception:
                    qleft = -1
                # app_healthy = proc alive AND status file says active (not just proc exists)
                try:
                    st = read_local_health(cfg) or {}
                    app_ok = st.get("state") == "active" and proc is not None
                except Exception:
                    app_ok = proc is not None
                heartbeat.send(db, cfg, cfg["node"]["id"], fence,
                               app_healthy=app_ok,
                               extra={"quotaRemainingSec": qleft})
                last_hb = now
            except Exception:
                log.exception("heartbeat failed")

        # idle keepalive: ensure at least one stdout line per 120s even with no state change
        # so Cloud Shell / tmux pty is not considered idle (see idle audit P0)
        if active_since is not None and now - last_idle_log >= 120:
            try:
                qleft = usage_tracker.remaining_seconds(db, cfg, cfg["node"]["id"])
            except Exception:
                qleft = -1
            log.info("still active fence=%s quota_left_h=%.1f", fence, qleft / 3600.0 if qleft >= 0 else -1)
            last_idle_log = now

        time.sleep(poll)

    log.info("supervisor stopping")
    stop_server(timeout=60)
    flush_usage()
    try:
        # prefer self so a quick restart of THIS shell reclaims the server
        lease.release(reason="supervisor-shutdown", preferred=cfg["node"]["id"])
    except Exception:
        pass


if __name__ == "__main__":
    main()
