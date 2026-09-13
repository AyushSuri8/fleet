"""Minimal 24/7 server-equivalent workload.

Runs only on the ACTIVE node (started/stopped by supervisor.py).
Its only job: prove the Cloud Shell server equivalent is alive by
refreshing the local status file. The supervisor's Firestore heartbeat
then advertises this node as the live server.

Use when you don't need the job queue: set in config/default.json
    {"server": {"command": "keepalive"}}
Set back to "worker" to run the Firestore job poller instead.
"""
import json
import logging
import os
import signal
import time
from pathlib import Path

from common import load_config, setup_logging, utcnow

stop_requested = False


def _on_sigterm(signum, frame):
    global stop_requested
    stop_requested = True


def write_status(cfg, state, extra=None):
    payload = {"node": cfg["node"]["id"], "state": state,
               "timestamp": utcnow().isoformat()}
    if extra:
        payload.update(extra)
    path = cfg.get("server", {}).get("status_file") or cfg["worker"]["status_file"]
    path = str(Path(path).expanduser()) if path.startswith("~") else path
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    tmp = path + ".tmp"
    Path(tmp).write_text(json.dumps(payload))
    os.replace(tmp, path)


def main():
    cfg = load_config()
    setup_logging(cfg["node"]["id"])
    log = logging.getLogger("keepalive")
    signal.signal(signal.SIGTERM, _on_sigterm)
    signal.signal(signal.SIGINT, _on_sigterm)
    log.info("keepalive server up (node=%s)", cfg["node"]["id"])
    while not stop_requested:
        try:
            write_status(cfg, "active", {"mode": "keepalive"})
        except Exception:
            log.exception("keepalive status write failed")
        time.sleep(5)
    write_status(cfg, "stopped")
    log.info("keepalive exiting cleanly")


if __name__ == "__main__":
    main()
