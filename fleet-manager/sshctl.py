"""SSH control to a Cloud Shell node via openssh + ControlMaster (stdlib only).

The manager's key is registered permanently on each environment by
provision.py, so this is a plain key login. Host keys are NOT verified
(Cloud Shell endpoints change host keys on VM restart); accepted tradeoff
because this key only opens shells we provisioned ourselves.

The ControlMaster socket keeps one authenticated connection alive per node,
so the every-30s keepalive probe is cheap — and doubles as the anti-idle
activity that stops Google from suspending an "idle" VM."""
import subprocess
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

    def _opts(self):
        return ["-i", self.key,
                "-o", "StrictHostKeyChecking=no",
                "-o", "UserKnownHostsFile=/dev/null",
                "-o", "ControlMaster=auto",
                "-o", f"ControlPath={self._ctl()}",
                "-o", "ControlPersist=600",
                "-o", "ConnectTimeout=20",
                "-o", "BatchMode=yes"]

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
        argv = ["scp", *self._opts(), "-P", str(port),
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
        """(Re)start the pull agent. Config comes from ~/fleet-agent/agent_config.json."""
        self.try_run("pkill -f '[a]gent.py' || true; sleep 1")
        out = self.run(
            "cd ~/fleet-agent && nohup python3 agent.py >> agent.log 2>&1 & echo PID=$!",
            timeout=30)
        return out.strip()
