"""Shared utilities: config, logging, time helpers, Firestore client.
Implements outline sections 5, 17, 22."""
import json
import logging
import os
import socket
import sys
from datetime import datetime, timezone
from pathlib import Path

CONFIG_PATH = os.environ.get(
    "FLEET_CONFIG", str(Path(__file__).parent / "config" / "default.json")
)

_DEFAULTS = {
    "gcp": {"project_id": ""},
    "node": {"id": ""},
    "lease": {
        "collection": "server",
        "doc_id": "lease",
        "ttl_seconds": 90,
        "poll_interval_seconds": 10,
        "takeover_grace_seconds": 45,
    },
    "heartbeat": {"collection": "server", "doc_prefix": "health-", "interval_seconds": 30},
    "rotation": {
        "enabled": True,
        # sticky-quota: same shell keeps the server until it stops + is
        # restarted, or its weekly quota is exhausted. max_active_seconds=0
        # disables timed rotation (legacy round-robin mode).
        "mode": "sticky-quota",
        "max_active_seconds": 0,
        "order": [],
        "weekly_quota_seconds": 180000,  # 50h per Cloud Shell account/week
        "week_seconds": 604800,  # 168h sliding window per shell
        "reclaim_grace_seconds": 90,  # restarted same shell may reclaim lease
    },
    "queue": {
        "requests_collection": "requests",
        "poll_interval_seconds": 5,
        "batch_size": 5,
        "job_lease_seconds": 300,
        "max_attempts": 3,
    },
    "worker": {"status_file": str(Path.home() / ".fleet" / "status.json")},
    "server": {"command": "worker", "status_file": str(Path.home() / ".fleet" / "status.json")},
}


def _deep_merge(base, override):
    out = dict(base)
    for k, v in (override or {}).items():
        if isinstance(v, dict) and isinstance(out.get(k), dict):
            out[k] = _deep_merge(out[k], v)
        else:
            out[k] = v
    return out


def utcnow():
    return datetime.now(timezone.utc)


def as_aware(dt):
    """Firestore may hand back naive datetimes depending on client version."""
    if dt is None:
        return None
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def load_config(path=None):
    path = path or CONFIG_PATH
    cfg = _DEFAULTS
    # if caller explicitly set FLEET_CONFIG to a missing file, fail fast
    if os.environ.get("FLEET_CONFIG") and not os.path.exists(path):
        raise RuntimeError(f"FLEET_CONFIG {path} missing")
    if os.path.exists(path):
        with open(path) as f:
            cfg = _deep_merge(_DEFAULTS, json.load(f))

    env_project = os.environ.get("GOOGLE_CLOUD_PROJECT") or os.environ.get("GCP_PROJECT_ID")
    if env_project:
        cfg["gcp"]["project_id"] = env_project
    env_node = os.environ.get("FLEET_NODE_ID")
    if env_node:
        cfg["node"]["id"] = env_node
    if not cfg["node"]["id"]:
        raise RuntimeError("FLEET_NODE_ID not set and no node.id in config — run bootstrap.sh to set identity")

    if not cfg["gcp"]["project_id"]:
        raise RuntimeError(
            "No project id. Set GOOGLE_CLOUD_PROJECT env var or gcp.project_id in config."
        )
    ring = cfg.get("rotation", {}).get("order", [])
    if ring and cfg["node"]["id"] not in ring:
        raise RuntimeError(f"node {cfg['node']['id']} not in rotation.order {ring} — update config order before joining")
    return cfg


def setup_logging(node_id, level=logging.INFO):
    """Structured JSON logs to stdout (outline section 22)."""

    class JsonFormatter(logging.Formatter):
        def format(self, record):
            payload = {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "level": record.levelname,
                "node": node_id,
                "component": record.name,
                "message": record.getMessage(),
            }
            if record.exc_info:
                payload["error"] = self.formatException(record.exc_info)
            return json.dumps(payload)

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    root = logging.getLogger()
    root.handlers = [handler]
    root.setLevel(level)


def get_db(cfg):
    from google.cloud import firestore

    return firestore.Client(project=cfg["gcp"]["project_id"])