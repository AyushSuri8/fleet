"""Fleet manager: the always-on control plane on the Oracle VM.

Replaces Firestore (lease/heartbeat/usage/queue), the Cloudflare Worker
watchdog, and the GitHub Actions keeper with one local loop:

  - exactly one shell is ACTIVE; only its agent gets jobs
  - STRICT LINEAR SUCCESSION with STICKY RESTARTS (4 x 50h covers the 168h
    week with ~32h buffer):
      * active node goes silent (12h session cap / idle suspend) -> restart
        the SAME node via the Cloud Shell :start API. Never rotate for this;
        a 12h suspension does NOT consume the weekly quota.
      * rotate to the strictly-next ring node ONLY when Google officially
        refuses :start with HTTP 429 / RESOURCE_EXHAUSTED (weekly quota gone).
        Google's API is the source of truth for quota; the local SQLite
        ledger is informational only and never gates activation.
  - SSH-readiness wait after every VM start (the API reports RUNNING before
    sshd accepts connections); a single immediate SSH probe is not trusted.
  - failed activations are cleaned up (agent stopped so Google idle-suspend
    can reclaim the VM — there is no suspend/stop API) and transient
    failures are retried on the SAME node next tick, never cascading through
    the ring and booting every VM.
  - reaps expired job leases back into the queue.

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


class QuotaExhausted(Exception):
    """Google officially refused to start a node (HTTP 429 / RESOURCE_EXHAUSTED).

    The account's weekly quota is gone. This — and only this — advances
    strict linear succession to the next ring node.
    """
    def __init__(self, node, detail=""):
        super().__init__(f"{node}: weekly quota exhausted per Google ({detail})")
        self.node = node


def _is_quota_error(exc):
    """True when an exception is Google's official quota refusal."""
    s = str(exc)
    return ("QUOTA_EXHAUSTED" in s or "RESOURCE_EXHAUSTED" in s
            or "429" in s)


class Fleet:
    def __init__(self, cfg):
        self.cfg = cfg
        self.store = Store(cfg.get("db_path", str(HERE / "fleet.db")))
        self.ring = cfg["ring"]
        self.poll = float(cfg.get("poll_seconds", 30))
        # cap_hours is IGNORED (kept for config compat): the 12h session cap
        # is handled by sticky restart of the SAME node, never by rotation.
        # A 12h suspension does not consume weekly quota.
        self.cap_s = float(cfg.get("cap_hours", 11)) * 3600
        # quota_s / week_s are DISPLAY-ONLY now (fleet/status). Activation
        # never consults them; Google's :start 429 is the source of truth.
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

    def _ring_order(self, exclude):
        """Strict ring order after `exclude`, skipping nodes we cannot boot.

        NOTE: no local quota filtering here — Google's :start 429 is the
        only source of truth for quota exhaustion. A wrong local guess must
        never skip a node that Google would still start.
        """
        ex = exclude or self.active
        start = self.ring.index(ex) if ex in self.ring else -1
        for i in range(1, len(self.ring) + 1):
            n = self.ring[(start + i) % len(self.ring)]
            if n == ex:
                continue
            if n not in self.cloudshells:
                continue
            yield n

    def _candidates(self, exclude):
        """Back-compat alias for _ring_order (no quota pre-filter)."""
        yield from self._ring_order(exclude)

    def _wait_ssh(self, node, tries=12, delay=10, timeout=15):
        """Poll SSH until sshd accepts connections (post-:start readiness).

        The Cloud Shell API reports state=RUNNING before sshd is up; a
        single immediate probe gives false "agent not provisioned" failures.
        Mirrors provision.py's wait_ssh. Returns True when ready.
        """
        ssh = self.sshs[node]
        for _ in range(tries):
            ok, _ = ssh.try_run("echo ok", timeout=timeout)
            if ok:
                return True
            time.sleep(delay)
        return False

    def _cleanup_failed_activation(self, node):
        """Best-effort cleanup so a failed activation stops burning quota.

        There is NO suspend/stop API for Cloud Shell — the only lever is to
        stop the agent (and any keepalive SSH traffic) so Google's idle
        detection can suspend the VM on its own. Never raises.
        """
        try:
            self.sshs[node].stop_agent()
        except Exception:
            pass
        self.event("cleanup", f"{node}: stopped agent after failed activation "
                              f"so Google idle-suspend can reclaim the VM")

    def _activate(self, node, reason):
        """Boot the VM if needed, (re)start the agent, wait for its heartbeat.

        Strict-linear contract:
          * raises QuotaExhausted when Google officially refuses :start
            (HTTP 429 / RESOURCE_EXHAUSTED) — the ONLY signal that advances
            succession to the next ring node.
          * returns True on success.
          * returns False on ANY transient failure (SSH not ready yet,
            endpoint missing, agent never heartbeated, ...). The caller must
            NOT cascade to the next node on False — it retries THIS node next
            tick. Before returning False the VM is cleaned up (agent stopped)
            so Google idle-suspend can reclaim quota. There is no
            suspend/stop API.
        """
        cs = self.cloudshells.get(node)
        if not cs:
            self.event("activate-failed", f"{node}: no refresh token")
            return False
        booted = False
        try:
            env = cs.get_env()
            self.store.set_vm_state(node, env.get("state", "?"))
            if env.get("state") != "RUNNING":
                self.event("vm-start", f"{node}: state={env.get('state')}; starting")
                try:
                    env = cs.start()
                except CloudShellError as e:
                    if _is_quota_error(e):
                        raise QuotaExhausted(node, str(e)[:200])
                    raise
                booted = True
                self.store.set_vm_state(node, env.get("state", "?"))
            host, port, user = env.get("sshHost"), env.get("sshPort"), env.get("sshUsername")
            if not (host and port and user):
                # Endpoint not in the (possibly stale) env — re-resolve once.
                if self._resolve(node):
                    host, port, user = self.sshs[node].endpoint
                if not (host and port and user):
                    self.event("activate-failed", f"{node}: no SSH endpoint in env")
                    if booted:
                        self._cleanup_failed_activation(node)
                    return False
            ssh = self.sshs[node]
            ssh.set_endpoint(host, port, user)
            if booted:
                # API reports RUNNING before sshd is up — wait, don't fail fast.
                self.event("ssh-wait", f"{node}: VM just started; waiting for sshd")
                if not self._wait_ssh(node):
                    self.event("activate-failed", f"{node}: SSH never became ready")
                    self._cleanup_failed_activation(node)
                    return False
            else:
                # VM was already RUNNING: still tolerate a slow sshd, but with
                # a shorter wait so the tick loop stays responsive.
                if not self._wait_ssh(node, tries=3, delay=10, timeout=15):
                    self.event("activate-failed", f"{node}: SSH not ready")
                    return False
            # If the agent is already heartbeating it is by definition
            # provisioned — skip the file check and just restart it.
            hb_age = self.store.heartbeat_age(node)
            if hb_age is None or hb_age >= 120:
                ok, out = ssh.try_run("test -f ~/fleet-agent/agent.py && echo yes",
                                      timeout=30)
                if not ok or "yes" not in out:
                    self.event("activate-failed",
                               f"{node}: agent not provisioned — run: python3 provision.py {node}")
                    if booted:
                        self._cleanup_failed_activation(node)
                    return False
            try:
                ssh.start_agent()
            except SSHError as e:
                self.event("activate-failed", f"{node}: agent start failed: {e}")
                if booted:
                    self._cleanup_failed_activation(node)
                return False
        except QuotaExhausted:
            raise
        except (CloudShellError, SSHError) as e:
            if _is_quota_error(e):
                raise QuotaExhausted(node, str(e)[:200])
            self.event("activate-failed", f"{node}: {e}")
            if booted:
                try:
                    self._cleanup_failed_activation(node)
                except Exception:
                    pass
            return False

        for _ in range(24):  # up to 120s for the agent to phone home
            age = self.store.heartbeat_age(node)
            if age is not None and age < 60:
                self.event("activated", f"{node} agent heartbeating ({reason})")
                return True
            time.sleep(5)
        self.event("activate-failed", f"{node}: agent never heartbeated")
        try:
            self._cleanup_failed_activation(node)
        except Exception:
            pass
        return False

    def _set_active(self, node, reason):
        self.active = node
        self.store.kv_set("active_node", node)
        self.store.set_active(node)
        self.event("active", f"{node} is now the active node ({reason})")

    def _next_in_ring(self, current):
        """The strictly-next ring node after `current` (A->B->C->D->A)."""
        idx = self.ring.index(current) if current in self.ring else -1
        return self.ring[(idx + 1) % len(self.ring)]

    def _activate_next_linear(self, current, reason):
        """Strict linear succession: walk forward ONLY past quota-exhausted nodes.

        Called ONLY when Google officially refused :start for `current`
        (QuotaExhausted). Walks the ring one step at a time:
          * successor raises QuotaExhausted (:start refused, VM never booted,
            zero quota burned) -> record it and CONTINUE to the next node in
            the same tick. Skipping exhausted nodes costs nothing.
          * successor returns True -> set it active, stop the old agent, done.
          * successor returns False (transient: SSH not ready, no endpoint,
            agent never heartbeated, ...) -> STOP immediately and return
            False WITHOUT touching any further node. The successor is retried
            next tick, so we never boot the whole ring in one tick.
        """
        nxt = self._next_in_ring(current)
        seen = set()
        while nxt not in seen:
            seen.add(nxt)
            if nxt not in self.cloudshells:
                self.event("no-successor",
                           f"next node {nxt} has no refresh token (reason={reason})")
                return False
            self.event("rotate",
                       f"weekly quota exhausted for {current} per Google; booting {nxt}")
            try:
                ok = self._activate(nxt, f"{reason}")
            except QuotaExhausted as qe:
                # Refused by Google BEFORE boot: costs zero quota, so it is
                # safe to step past it in the same tick (e.g. manager was
                # down across a quota boundary and several nodes are spent).
                self.event("quota-exhausted", str(qe))
                current = nxt
                nxt = self._next_in_ring(nxt)
                continue
            if ok:
                old = self.active
                if old and old != nxt:
                    try:
                        self.sshs[old].stop_agent()
                    except SSHError:
                        pass
                    # No suspend/stop API exists: stopping the agent (and all
                    # keepalive SSH traffic) lets Google idle-suspend the old VM.
                self._set_active(nxt, reason)
                return True
            self.event("skip", f"{nxt} failed activation (transient); retrying next tick")
            return False
        self.event("no-successor",
                   f"all nodes quota-exhausted per Google (reason={reason})")
        return False

    def _activate_next(self, exclude, reason):
        """Legacy entry point, now strict-linear.

        Activates ONLY the next ring node after `exclude`. Transient failure
        returns False without touching any other node (no cascade). Kept as
        a wrapper so resume()/tick() call sites and any external callers keep
        working; new code should call _activate_next_linear directly.
        """
        current = exclude or self.active or self.ring[-1]
        return self._activate_next_linear(current, reason)

    # ---- main loop -------------------------------------------------------

    def tick(self):
        """One control-loop pass: sticky-restart, linear succession, leases.

        Strict linear succession with sticky restarts:
          * active node healthy -> just accrue informational ledger time.
          * active node silent (12h cap / idle suspend / reboot) -> STICKY
            RESTART the SAME node via _activate(). Any transient failure
            returns False and is retried next tick — never rotate for this.
          * _activate() raises QuotaExhausted ONLY when Google officially
            refuses :start (429 / RESOURCE_EXHAUSTED) -> advance ONE step to
            the strictly-next ring node.
        There is deliberately NO cap_hours pre-rotation and NO local-quota
        gating: both violated the linear duty cycle and burned VMs.
        """
        requeued, failed = self.store.reap_expired()
        if requeued or failed:
            self.event("reap", f"requeued={requeued} failed={failed}")

        active = self.active
        if not active:
            self.event("activate", "no active node; activating next")
            self._activate_next(exclude=None, reason="no-active")
            return

        # Informational ledger only (fleet/status display). Google's actual
        # RUNNING time may differ (failed-activation VMs, refills); never
        # used to gate activation decisions.
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
                # VM likely suspended (12h cap / idle) — STICKY RESTART the
                # same node. Only a QuotaExhausted advances succession.
                self.event("node-silent",
                           f"{active}: heartbeat silent {hb_age if hb_age is not None else 'never'}s, "
                           f"ssh down; sticky-restarting same node")
                try:
                    if self._activate(active, "sticky-restart"):
                        self._restart_attempts[active] = 0
                        self.event("restarted",
                                   f"{active} sticky-restarted after silence")
                except QuotaExhausted as qe:
                    self.event("quota-exhausted", str(qe))
                    self._activate_next_linear(active, reason="quota-exhausted")
                return
            attempts = self._restart_attempts.get(active, 0)
            if attempts < 3:
                self._restart_attempts[active] = attempts + 1
                self.event("agent-restart",
                           f"{active}: heartbeat silent {hb_age if hb_age is not None else 'never'}s, ssh ok; "
                           f"restarting agent (attempt {attempts + 1}/3)")
                try:
                    self.sshs[active].start_agent()
                except SSHError as e:
                    self.event("agent-restart-failed", str(e))
            else:
                # SSH is up but the agent never phones home after 3 restarts.
                # The VM itself is fine — reboot the agent via a full sticky
                # _activate (which re-waits for heartbeat) rather than
                # abandoning a healthy VM for the next shell.
                self.event("agent-stuck",
                           f"{active}: agent unresponsive after restarts; "
                           f"sticky-reactivating same node")
                try:
                    if self._activate(active, "sticky-reactivate"):
                        self._restart_attempts[active] = 0
                except QuotaExhausted as qe:
                    self.event("quota-exhausted", str(qe))
                    self._activate_next_linear(active, reason="quota-exhausted")
            return
        self._restart_attempts[active] = 0

        # Healthy path: nothing else to do. No cap pre-rotation (the 12h
        # session suspend is handled above by sticky restart) and no local
        # quota rotation (only Google's 429 advances succession, and that
        # surfaces from _activate during a sticky restart).

    def resume(self):
        """On manager restart: reconnect to the active node without rotating
        away from it just because heartbeats went stale while we were down.

        Strict-linear rule: resume STICKS to the recorded active node (the
        16h-downtime bug rotated away and cascade-booted the whole ring).
        Only Google's official QuotaExhausted on the sticky _activate()
        advances succession — one step, no cascade.
        """
        node = self.active
        if not node:
            return
        self.log.info("resuming with active node %s", node)
        if not self._resolve(node):
            # No endpoint yet (VM suspended while we were down?) — sticky
            # re-activate the SAME node, which boots it via :start.
            self.event("resume-sticky",
                       f"{node}: no endpoint; sticky-reactivating same node")
            try:
                self._activate(node, "resume-sticky-restart")
            except QuotaExhausted as qe:
                self.event("quota-exhausted", str(qe))
                self._activate_next_linear(node, reason="resume-quota-exhausted")
            return
        for _ in range(6):  # agent heartbeats within ~20s of the manager's return
            age = self.store.heartbeat_age(node)
            if age is not None and age < 60:
                self.event("resumed", f"{node} agent reconnected")
                return
            time.sleep(10)
        try:
            if self._activate(node, "resume-agent-restart"):
                return
            # Transient failure — stay sticky; next tick retries the same node.
            self.event("resume-retry",
                       f"{node}: sticky reactivation pending; retrying next tick")
        except QuotaExhausted as qe:
            self.event("quota-exhausted", str(qe))
            self._activate_next_linear(node, reason="resume-quota-exhausted")

    def summary(self):
        now = time.time()
        nodes = self.store.nodes_summary(self.ring, self.quota_s, self.week_s)
        # Mark the local ledger as estimated: Google's backend is the source
        # of truth (actual RUNNING time, real weekly refills).
        for nd in nodes:
            nd["quotaLeftH"] = nd.get("quotaLeftH")
            nd["quotaEstimated"] = True
        counts = self.store.job_counts()
        problems = []
        if not self.active:
            problems.append("NO-ACTIVE-NODE")
        else:
            age = self.store.heartbeat_age(self.active)
            if age is None or age > self.hb_stale_s:
                problems.append(f"HEARTBEAT-STALE: {self.active} silent "
                                f"{int(age) if age else 'never'}s")
            # NOTE: no CAP-DUE check — the 12h session cap is handled by
            # sticky restart, not rotation, so uptime-vs-cap is not a problem.
        total_left = sum(self.store.remaining(n, self.quota_s, self.week_s)
                         for n in self.ring)
        if total_left < 24 * 3600:
            problems.append(f"QUOTA-FLEET-LOW(estimated): {total_left/3600:.1f}h left")
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
