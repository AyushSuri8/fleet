"""Heartbeat writer to Firestore. Implements outline section 10."""
import logging
import shutil
import socket
from pathlib import Path

from common import utcnow

log = logging.getLogger("heartbeat")


def _disk_ok():
    for p in ["/", str(Path.home()), "/tmp"]:
        try:
            total, _used, free = shutil.disk_usage(p)
            if free < 500 * 1024 * 1024 or (free / total) < 0.10:
                return False
        except Exception:
            continue
    return True


def _network_ok():
    try:
        socket.create_connection(("8.8.8.8", 53), timeout=2).close()
        return True
    except Exception:
        return False


def send(db, cfg, node_id, fence_token, app_healthy, extra=None):
    col = cfg["heartbeat"]["collection"]
    prefix = cfg["heartbeat"]["doc_prefix"]
    payload = {
        "node": node_id,
        "timestamp": utcnow(),
        "appHealthy": bool(app_healthy),
        "diskOk": _disk_ok(),
        "networkOk": _network_ok(),
        "fenceToken": fence_token,
    }
    if extra:
        payload.update(extra)
    db.collection(col).document(f"{prefix}{node_id}").set(payload, merge=True)