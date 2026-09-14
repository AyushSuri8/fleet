"""SSH control to a Cloud Shell node via openssh + ControlMaster (stdlib only).

The manager's key is registered permanently on each environment by
provision.py, so this is a plain key login. Host keys are NOT verified
(Cloud Shell endpoints change host keys on VM restart); accepted tradeoff
because this key only opens shells we provisioned ourselves.

The ControlMaster socket keeps one authenticated connection alive per node,
so the every-30s keepalive probe is cheap — and doubles as the anti-idle
activity that stops Google from suspending an "idle" VM."""
import subprocess
import time
from pathlib import Path


class SSHError(Exception):
    pass


class NodeSSH:
    def __init__(self, node, key_path, runtime_dir):
        self.node = node
        self.key = str(Path(key_path).expanduser())
        self.runtime = Path(runtime_dir)
        self.runtime.mkdir(parents=True, exist_ok=True)
        self.endpoint = None  # (host, port, user)

    def set_endpoint(self, host, port, user):
        new = (host, int(port), user)
        if self.endpoint and self.endpoint != new:
            self._close_master()
        self.endpoint = new

    def _ctl(self):
        return f"{self.runtime}/{self.node}-{self.endpoint[0]}:{self.endpoint[1]}"

    def _opts(self, for_scp=False):
        opts = ["-i", self.key,
                "-o", "StrictHostKeyChecking=no",
                "-o", "UserKnownHostsFile=/dev/null",
                "-o", "ControlMaster=auto",
                "-o", f"ControlPath={self._ctl()}",
                "-o", "ControlPersist=600",
                "-o", "ConnectTimeout=20",
                "-o", "BatchMode=yes"]
        if not for_scp:
            # stdin from /dev/null: never hold the channel open.
            # (scp on some OpenSSH builds rejects -n, so it is ssh-only.)
            opts.append("-n")
        return opts

    def _close_master(self):
        subprocess.run(["ssh", "-o", f"ControlPath={self._ctl()}", "-O", "exit"],
                       capture_output=True, timeout=15)

    def run(self, cmd, timeout=60):
        if not self.endpoint:
            raise SSHError(f"{self.node}: no endpoint set")
        host, port, user = self.endpoint
        argv = ["ssh", *self._opts(), "-p", str(port), f"{user}@{host}", cmd]
        try:
            r = subprocess.run(argv, capture_output=True, text=True, timeout=timeout)
        except subprocess.TimeoutExpired:
            raise SSHError(f"{self.node}: ssh timed out after {timeout}s")
        if r.returncode != 0:
            raise SSHError(f"{self.node}: ssh rc={r.returncode}: {r.stderr.strip()[:200]}")
        return r.stdout

    def try_run(self, cmd, timeout=60):
        try:
            return True, self.run(cmd, timeout)
        except SSHError:
            return False, ""

    def upload(self, local, remote):
        host, port, user = self.endpoint
        argv = ["scp", *self._opts(for_scp=True), "-P", str(port),
                str(Path(local).expanduser()), f"{user}@{host}:{remote}"]
        r = subprocess.run(argv, capture_output=True, text=True, timeout=120)
        if r.returncode != 0:
            raise SSHError(f"{self.node}: scp rc={r.returncode}: {r.stderr.strip()[:200]}")

    # ---- agent lifecycle -----------------------------------------------

    def agent_up(self):
        ok, out = self.try_run("pgrep -f '[a]gent.py' >/dev/null && echo up")
        return ok and "up" in out

    def stop_agent(self):
        self.try_run("pkill -f '[a]gent.py' || true")

    def start_agent(self):
        """(Re)start the pull agent. Config comes from ~/fleet-agent/agent_config.json.

        Launches via a double-fork (setsid, all fds closed): shell `&`
        backgrounding hangs the SSH channel on Cloud Shell's proxy even with
        output redirected and stdin from /dev/null. If the channel still
        hangs, falls back to verifying by process presence — the agent is
        what matters, not the launch command's exit status."""
        self.try_run("pkill -f '[a]gent.py' || true; sleep 1")
        launch = ("cd ~/fleet-agent && python3 -c "
                  "'import subprocess;"
                  "log=open(\"agent.log\",\"ab\",0);"
                  "p=subprocess.Popen([\"python3\",\"agent.py\"],"
                  "stdin=subprocess.DEVNULL,stdout=log,"
                  "stderr=subprocess.STDOUT,"
                  "start_new_session=True,close_fds=True);"
                  "print(\"PID=%d\"%p.pid)'")
        out = ""
        try:
            out = self.run(launch, timeout=30).strip()
        except SSHError as e:
            if "timed out" not in str(e):
                raise
            # channel hung — the agent may still have started; verify below
        for _ in range(6):
            ok, ps = self.try_run("pgrep -f '[a]gent.py'")
            if ok and ps.strip():
                return out or f"PID=? ({ps.strip().splitlines()[0][:60]} via pgrep)"
            time.sleep(5)
        raise SSHError(f"{self.node}: agent did not appear after launch")
