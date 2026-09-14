"""Fleet manager: the always-on control plane on the Oracle VM.

Replaces Firestore (lease/heartbeat/usage/queue), the Cloudflare Worker
watchdog, and the GitHub Actions keeper with one local loop:

  - exactly one shell is ACTIVE; only its agent gets jobs
  - 50h/week quota ledger per node, locally in SQLite
  - pre-rotates before the ~12h session cap (cap_hours, default 11h):
    boots + bootstraps the NEXT node first, then switches — near-zero gap
  - reactive failover when the active node dies (heartbeat + SSH probes)
  - reaps expired job leases back into the queue

Stdlib only. Run under systemd (fleet-manager.service)."""
import json
import logging
import signal
import sys
import threading
import time
from pathlib import Path

from cloudshell import CloudShell, CloudShellError
from sshctl import NodeSSH, SSHError
import frontdoor
from store import Store

HERE = Path(__file__).resolve().parent


class Fleet:
    def __init__(self, cfg):
        self.cfg = cfg
        self.store = Store(cfg.get("db_path", str(HERE / "fleet.db")))
        self.ring = cfg["ring"]
        self.poll = float(cfg.get("poll_seconds", 30))
        self.cap_s = float(cfg.get("cap_hours", 11)) * 3600
        self.quota_s = float(cfg.get("quota_seconds", 180000))
        self.week_s = float(cfg.get("week_seconds", 604800))
        self.hb_stale_s = float(cfg.get("heartbeat_stale_seconds", 90))
        self.log = logging.getLogger("fleet")

        self.sshs = {n: NodeSSH(n, cfg["ssh_key"], HERE / "runtime")
                     for n in self.ring}
        self.cloudshells = {}
        cid, csec = self._load_oauth_client()
        for n in self.ring:
            tok = self._refresh_token(n)
            if tok and cid:
                self.cloudshells[n] = CloudShell(cid, csec, tok)
            elif n in self.ring:
                self.log.warning("%s: no refresh token — cannot boot/manage it", n)

        self.active = self.store.kv_get("active_node")
        self._restart_attempts = {}

    # ---- credentials ----------------------------------------------------

    def _load_oauth_client(self):
        f = Path(self.cfg["oauth_client_file"]).expanduser()
        if not f.exists():
            return None, None
        installed = json.loads(f.read_text())["installed"]
        return installed["client_id"], installed["client_secret"]

    def _refresh_token(self, node):
        suffix = node.split("-")[1].upper()
        f = Path(self.cfg["secrets_dir"]).expanduser() / f"SHELL_{suffix}_REFRESH.txt"
        return f.read_text().strip() if f.exists() else None

    # ---- helpers ----------------------------------------------------------

    def event(self, kind, detail):
        self.log.info("%s: %s", kind, detail)
        self.store.event(kind, detail)

    def _resolve(self, node):
        """Refresh the node's SSH endpoint from the Cloud Shell API (endpoints
        change when a VM restarts). No side effects."""
        cs = self.cloudshells.get(node)
        if not cs:
            return False
        try:
            env = cs.get_env()
        except CloudShellError as e:
            self.log.warning("%s: env resolve failed: %s", node, e)
            return False
        self.store.set_vm_state(node, env.get("state", "?"))
        if env.get("sshHost") and env.get("sshPort") and env.get("sshUsername"):
            self.sshs[node].set_endpoint(env["sshHost"], env["sshPort"],
                                         env["sshUsername"])
            return True
        return False

    def _candidates(self, exclude):
        """Ring order after `exclude`, skipping nodes we can't boot or that
        are out of quota."""
        ex = exclude or self.active
        start = self.ring.index(ex) if ex in self.ring else -1
        for i in range(1, len(self.ring) + 1):
            n = self.ring[(start + i) % len(self.ring)]
            if n == ex:
                continue
            if n not in self.cloudshells:
                continue
            if self.store.remaining(n, self.quota_s, self.week_s) <= 0:
                continue
            yield n

    def _activate(self, node, reason):
        """Boot the VM if needed, (re)start the agent, wait for its heartbeat."""
        cs = self.cloudshells.get(node)
        if not cs:
            self.event("activate-failed", f"{node}: no refresh token")
            return False
        try:
            env = cs.get_env()
            self.store.set_vm_state(node, env.get("state", "?"))
            if env.get("state") != "RUNNING":
                self.event("vm-start", f"{node}: state={env.get('state')}; starting")
                env = cs.start()
            host, port, user = env.get("sshHost"), env.get("sshPort"), env.get("sshUsername")
            if not (host and port and user):
                self.event("activate-failed", f"{node}: no SSH endpoint in env")
                return False
            ssh = self.sshs[node]
            ssh.set_endpoint(host, port, user)
            ok, out = ssh.try_run("test -f ~/fleet-agent/agent.py && echo yes", timeout=30)
            if not ok or "yes" not in out:
                self.event("activate-failed",
                           f"{node}: agent not provisioned — run: python3 provision.py {node}")
                return False
            ssh.start_agent()
        except (CloudShellError, SSHError) as e:
            self.event("activate-failed", f"{node}: {e}")
            return False

        for _ in range(24):  # up to 120s for the agent to phone home
            age = self.store.heartbeat_age(node)
            if age is not None and age < 60:
                self.event("activated", f"{node} agent heartbeating ({reason})")
                return True
            time.sleep(5)
        self.event("activate-failed", f"{node}: agent never heartbeated")
        return False

    def _set_active(self, node, reason):
        self.active = node
        self.store.kv_set("active_node", node)
        self.store.set_active(node)
        self.event("active", f"{node} is now the active node ({reason})")

    def _activate_next(self, exclude, reason):
        """Warm-rotate: activate a successor FIRST, then demote the old node."""
        for nxt in self._candidates(exclude):
            if self._activate(nxt, f"{reason}"):
                old = self.active
                if old and old != nxt:
                    try:
                        self.sshs[old].stop_agent()
                    except SSHError:
                        pass
                    # leave the old VM running; Google idle-suspends it
                self._set_active(nxt, reason)
                return True
            self.event("skip", f"{nxt} failed activation; trying next")
        self.event("no-successor",
                   f"no node could be activated (reason={reason}, excluded={exclude})")
        return False

    # ---- main loop -------------------------------------------------------

    def tick(self):
        requeued, failed = self.store.reap_expired()
        if requeued or failed:
            self.event("reap", f"requeued={requeued} failed={failed}")

        active = self.active
        if not active:
            self.event("activate", "no active node; activating next")
            self._activate_next(exclude=None, reason="no-active")
            return

        self.store.add_active_time(active, self.poll, self.week_s)

        hb_age = self.store.heartbeat_age(active)
        hb_stale = hb_age is None or hb_age > self.hb_stale_s
        # SSH probe doubles as anti-idle activity for the VM
        ssh_ok, _ = self.sshs[active].try_run("echo ok", timeout=30)
        if not ssh_ok and not hb_stale:
            # endpoint may have rotated (VM restarted): re-resolve once
            self._resolve(active)
            ssh_ok, _ = self.sshs[active].try_run("echo ok", timeout=30)

        if hb_stale:
            if not ssh_ok:
                self.event("node-dead",
                           f"{active}: heartbeat silent {hb_age or 'never'}, ssh down")
                self._activate_next(exclude=active, reason="dead-node")
                return
            attempts = self._restart_attempts.get(active, 0)
            if attempts < 3:
                self._restart_attempts[active] = attempts + 1
                self.event("agent-restart",
                           f"{active}: heartbeat silent {hb_age or 'never'}, ssh ok; "
                           f"restarting agent (attempt {attempts + 1}/3)")
                try:
                    self.sshs[active].start_agent()
                except SSHError as e:
                    self.event("agent-restart-failed", str(e))
            else:
                self.event("node-dead", f"{active}: agent unresponsive after restarts")
                self._activate_next(exclude=active, reason="agent-stuck")
            return
        self._restart_attempts[active] = 0

        uptime = time.time() - (self.store.active_since(active) or time.time())
        quota_left = self.store.remaining(active, self.quota_s, self.week_s)
        if uptime >= self.cap_s:
            self.event("rotate",
                       f"{active} uptime {uptime/3600:.1f}h >= cap; pre-rotating")
            self._activate_next(exclude=active, reason="session-cap")
        elif quota_left <= 0:
            self.event("rotate", f"{active} weekly quota exhausted; rotating")
            self._activate_next(exclude=active, reason="quota-exhausted")

    def resume(self):
        """On manager restart: reconnect to the active node without rotating
        away from it just because heartbeats went stale while we were down."""
        node = self.active
        if not node:
            return
        self.log.info("resuming with active node %s", node)
        if not self._resolve(node):
            self._activate_next(exclude=None, reason="resume-no-endpoint")
            return
        for _ in range(6):  # agent heartbeats within ~20s of the manager's return
            age = self.store.heartbeat_age(node)
            if age is not None and age < 60:
                self.event("resumed", f"{node} agent reconnected")
                return
            time.sleep(10)
        if not self._activate(node, "resume-agent-restart"):
            self._activate_next(exclude=node, reason="resume-failed")

    def summary(self):
        now = time.time()
        nodes = self.store.nodes_summary(self.ring, self.quota_s, self.week_s)
        counts = self.store.job_counts()
        problems = []
        if not self.active:
            problems.append("NO-ACTIVE-NODE")
        else:
            age = self.store.heartbeat_age(self.active)
            if age is None or age > self.hb_stale_s:
                problems.append(f"HEARTBEAT-STALE: {self.active} silent "
                                f"{int(age) if age else 'never'}s")
            uptime = now - (self.store.active_since(self.active) or now)
            if uptime >= self.cap_s:
                problems.append(f"CAP-DUE: {self.active} uptime {uptime/3600:.1f}h")
        total_left = sum(self.store.remaining(n, self.quota_s, self.week_s)
                         for n in self.ring)
        if total_left < 24 * 3600:
            problems.append(f"QUOTA-FLEET-LOW: {total_left/3600:.1f}h left")
        if counts.get("queued", 0) > 50:
            problems.append(f"BACKLOG: {counts['queued']} queued")
        return {"ok": not problems,
                "activeNode": self.active,
                "uptimeS": int(now - (self.store.active_since(self.active) or now))
                           if self.active else None,
                "problems": problems,
                "nodes": nodes,
                "jobs": counts,
                "events": self.store.recent_events(15),
                "at": now}


def main():
    cfg_path = sys.argv[1] if len(sys.argv) > 1 else str(HERE / "manager_config.json")
    cfg = json.loads(Path(cfg_path).read_text())
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s %(message)s")
    fleet = Fleet(cfg)

    srv = frontdoor.make_server(fleet, cfg)
    threading.Thread(target=srv.serve_forever, daemon=True, name="http").start()
    fleet.log.info("manager up on %s:%s (public %s)",
                   cfg.get("listen_host"), cfg.get("listen_port"), cfg.get("public_url"))

    stop = {"flag": False}

    def _sig(signum, frame):
        stop["flag"] = True

    signal.signal(signal.SIGTERM, _sig)
    signal.signal(signal.SIGINT, _sig)

    fleet.resume()
    while not stop["flag"]:
        try:
            fleet.tick()
        except Exception:
            fleet.log.exception("tick failed")
        time.sleep(fleet.poll)
    srv.shutdown()
    fleet.log.info("manager stopped")


if __name__ == "__main__":
    main()
