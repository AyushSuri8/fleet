"""Worker process: polls Firestore for jobs while this node holds the lease.
Started/stopped by supervisor.py. Sections 9, 13 (outbound-only), 16."""
import json
import logging
import os
import signal
import time
from pathlib import Path

from common import load_config, setup_logging, get_db, utcnow
from lease_manager import LeaseManager
from jobs import JobQueue, StaleWorker
import handlers

stop_requested = False


def _on_sigterm(signum, frame):
    global stop_requested
    stop_requested = True  # finish current job, then exit -> clean rotation


def write_status(cfg, state, extra=None):
    payload = {"node": cfg["node"]["id"], "state": state,
               "timestamp": utcnow().isoformat()}
    if extra:
        payload.update(extra)
    p = cfg["worker"]["status_file"]
    p = str(Path(p).expanduser()) if p.startswith("~") else p
    Path(p).parent.mkdir(parents=True, exist_ok=True)
    tmp = p + ".tmp"
    Path(tmp).write_text(json.dumps(payload))
    # ensure durability on persistent HOME (not /tmp)
    try:
        import os as _os
        _os.fsync(open(tmp, "r").fileno())
    except Exception:
        pass
    os.replace(tmp, p)


def main():
    cfg = load_config()
    setup_logging(cfg["node"]["id"])
    log = logging.getLogger("worker")
    signal.signal(signal.SIGTERM, _on_sigterm)
    signal.signal(signal.SIGINT, _on_sigterm)

    db = get_db(cfg)
    lease = LeaseManager(db, cfg["node"]["id"], cfg["lease"]["collection"],
                         cfg["lease"]["doc_id"], cfg["lease"]["ttl_seconds"])
    queue = JobQueue(db, cfg)
    poll = int(cfg["queue"]["poll_interval_seconds"])
    batch = int(cfg["queue"]["batch_size"])
    log.info("worker up")

    while not stop_requested:
        try:
            data = lease.read()
            if not data or data.get("activeNode") != cfg["node"]["id"]:
                write_status(cfg, "standby")
                time.sleep(poll)
                continue

            fence = int(data.get("fenceToken", 0))
            reaped = queue.reap_stale()
            if reaped:
                log.info("reaped %d stale jobs", reaped)

            processed = 0
            for snap in queue.list_queued(limit=batch):
                if stop_requested:
                    break
                claimed = queue.claim(snap.reference, fence)
                if not claimed:
                    continue
                jid = snap.id
                started = time.monotonic()
                ctx = {
                    "node_id": cfg["node"]["id"],
                    "checkpoint": (lambda cp, r=snap.reference, f=fence:
                                   queue.checkpoint(r, f, cp)),
                }
                try:
                    handler = handlers.get_handler(claimed.get("type"))
                    if handler is None:
                        raise KeyError(f"no handler for type {claimed.get('type')!r}")
                    result = handler(claimed.get("payload") or {}, claimed, ctx)
                    queue.complete(snap.reference, fence, result)
                    log.info("job %s completed in %dms",
                             jid, int((time.monotonic() - started) * 1000))
                except StaleWorker as e:
                    log.warning("job %s: %s", jid, e)
                    break  # lost the lease; stop touching jobs this loop
                except (KeyError, ValueError) as e:
                    log.warning("job %s permanent failure (no retry): %s", jid, e)
                    try:
                        queue.fail(snap.reference, fence, e, retryable=False)
                    except StaleWorker:
                        break
                except Exception as e:
                    log.exception("job %s failed", jid)
                    try:
                        queue.fail(snap.reference, fence, e, retryable=True)
                    except StaleWorker:
                        break
                processed += 1

            write_status(cfg, "draining" if stop_requested else "active",
                         {"fenceToken": fence, "processedLastLoop": processed})
            time.sleep(0.2 if processed else poll)
        except Exception:
            log.exception("worker loop error")
            write_status(cfg, "error")
            time.sleep(poll)

    write_status(cfg, "stopped")
    log.info("worker exiting cleanly")


if __name__ == "__main__":
    main()