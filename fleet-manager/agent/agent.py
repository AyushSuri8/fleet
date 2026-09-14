#!/usr/bin/env python3
"""Fleet agent: runs on each Cloud Shell node, managed by the Oracle manager.

Pure stdlib — no venv, no Google credentials, nothing to install. Pulls jobs
from the manager over outbound HTTPS (long-poll), runs handlers from
handlers.py next to this file, posts results/checkpoints.

Checkpoints live on the MANAGER, so a job killed by a session cap resumes on
the next node from its last checkpoint. Survives manager restarts (retries).

Installed by provision.py into ~/fleet-agent/ with agent_config.json:
    {"server": "https://ORACLE_IP:8443", "token": "...",
     "node": "shell-a", "insecure_tls": true}
"""
import json
import signal
import ssl
import sys
import threading
import time
import urllib.error
import urllib.request
from pathlib import Path

HERE = Path(__file__).resolve().parent
stop = threading.Event()

try:
    import handlers
except ImportError:
    handlers = None


def http(cfg, path, body=None, method=None, timeout=40):
    url = cfg["server"].rstrip("/") + path
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data,
                                 method=method or ("POST" if data else "GET"))
    req.add_header("x-fleet-token", cfg["token"])
    ctx = ssl._create_unverified_context() if cfg.get("insecure_tls") else None
    with urllib.request.urlopen(req, timeout=timeout, context=ctx) as r:
        raw = r.read()
    return r.status, (json.loads(raw) if raw else None)


def post(cfg, path, body, timeout=20):
    """Never raises: returns (0, None) on network failure (manager rebooting)."""
    try:
        return http(cfg, path, body, timeout=timeout)
    except urllib.error.HTTPError as e:
        return e.code, None
    except Exception:
        return 0, None


def heartbeat_loop(cfg):
    while not stop.is_set():
        post(cfg, "/agent/heartbeat", {"node": cfg["node"], "state": "alive"})
        stop.wait(20)


def run_job(cfg, job):
    jid = job["id"]
    payload = job.get("payload") or {}
    if isinstance(payload, str):
        try:
            payload = json.loads(payload)
        except json.JSONDecodeError:
            payload = {}

    def checkpoint(cp):
        if not isinstance(cp, dict):
            cp = {"value": cp}
        post(cfg, f"/agent/checkpoint/{jid}",
             {"node": cfg["node"], "checkpoint": cp})

    ctx = {"node_id": cfg["node"], "checkpoint": checkpoint}
    try:
        if handlers is None:
            raise RuntimeError("handlers.py missing next to agent.py")
        handler = handlers.get_handler(job.get("type"))
        if handler is None:
            raise KeyError(f"no handler for type {job.get('type')!r}")
        result = handler(payload, job, ctx)
        st, _ = post(cfg, f"/agent/result/{jid}",
                     {"node": cfg["node"], "result": result}, timeout=30)
        if st != 200:
            print(f"job {jid}: result post rejected (HTTP {st})", flush=True)
    except (KeyError, ValueError, RuntimeError) as e:
        post(cfg, f"/agent/result/{jid}",
             {"node": cfg["node"], "error": str(e), "retryable": False})
    except Exception as e:
        print(f"job {jid} failed: {e}", flush=True)
        post(cfg, f"/agent/result/{jid}",
             {"node": cfg["node"], "error": str(e), "retryable": True})


def main():
    cfg = json.loads((HERE / "agent_config.json").read_text())
    node = cfg["node"]
    print(f"agent up node={node} server={cfg['server']}", flush=True)

    def _sig(signum, frame):
        stop.set()  # finish current job, then exit

    signal.signal(signal.SIGTERM, _sig)
    signal.signal(signal.SIGINT, _sig)
    threading.Thread(target=heartbeat_loop, args=(cfg,), daemon=True).start()

    while not stop.is_set():
        try:
            st, job = http(cfg, "/agent/claim", {"node": node}, timeout=40)
        except urllib.error.HTTPError as e:
            st, job = e.code, None
        except Exception:
            st, job = 0, None

        if st != 200 or not job:
            if st == 0:
                stop.wait(5)  # manager unreachable — retry shortly
            continue

        print(f"job {job['id']} type={job.get('type')} claimed", flush=True)
        started = time.monotonic()
        run_job(cfg, job)
        print(f"job {job['id']} done in {time.monotonic() - started:.1f}s", flush=True)

    print("agent exiting cleanly", flush=True)


if __name__ == "__main__":
    main()
