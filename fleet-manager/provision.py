#!/usr/bin/env python3
"""One-time (per node) provisioning, run on the Oracle manager.

For each node: mint that account's token -> resolve environment -> boot if
needed -> register the MANAGER's public SSH key (permanent) -> upload
agent.py + handlers.py + agent_config.json -> start the agent.

Idempotent: re-running refreshes files and restarts the agent (also how you
push handler updates or a rotated agent_token).

Usage:
  python3 provision.py                 # all nodes in ring
  python3 provision.py shell-b shell-c
"""
import base64
import json
import subprocess
import sys
import time
from pathlib import Path

from cloudshell import CloudShell
from sshctl import NodeSSH

HERE = Path(__file__).resolve().parent


def sh(cmd):
    r = subprocess.run(cmd, capture_output=True, text=True)
    if r.returncode != 0:
        raise SystemExit(f"command failed: {' '.join(cmd)}\n{r.stderr}")
    return r.stdout.strip()


def wait_ssh(ssh, tries=30, delay=10):
    for _ in range(tries):
        ok, _ = ssh.try_run("echo ok", timeout=25)
        if ok:
            return True
        time.sleep(delay)
    return False


def main():
    cfg = json.loads((HERE / "manager_config.json").read_text())
    ring = cfg["ring"]
    targets = sys.argv[1:] or ring
    for t in targets:
        if t not in ring:
            raise SystemExit(f"unknown node {t} (ring: {ring})")

    key = Path(cfg["ssh_key"]).expanduser()
    if not key.exists():
        key.parent.mkdir(parents=True, exist_ok=True)
        print(f"generating manager SSH key at {key}")
        sh(["ssh-keygen", "-t", "ed25519", "-N", "", "-f", str(key)])
    pub = Path(str(key) + ".pub").read_text().strip()

    client_file = Path(cfg["oauth_client_file"]).expanduser()
    installed = json.loads(client_file.read_text())["installed"]
    cid, csec = installed["client_id"], installed["client_secret"]

    for node in targets:
        print(f"\n=== provisioning {node} ===")
        suffix = node.split("-")[1].upper()
        rt = Path(cfg["secrets_dir"]).expanduser() / f"SHELL_{suffix}_REFRESH.txt"
        if not rt.exists():
            print(f"  SKIP: refresh token missing: {rt}")
            continue
        cs = CloudShell(cid, csec, rt.read_text().strip())

        env = cs.get_env()
        print(f"  VM state: {env.get('state')}")
        if env.get("state") != "RUNNING":
            print("  starting VM ...")
            env = cs.start()
        host, port, user = env.get("sshHost"), env.get("sshPort"), env.get("sshUsername")
        if not (host and port and user):
            raise SystemExit(f"  no SSH endpoint for {node}")

        cs.add_public_key(pub)
        print("  manager key registered (or already present)")

        ssh = NodeSSH(node, cfg["ssh_key"], HERE / "runtime")
        ssh.set_endpoint(host, port, user)
        print(f"  waiting for SSH {user}@{host}:{port} ...")
        if not wait_ssh(ssh):
            raise SystemExit(f"  SSH never came up for {node}")
        print("  SSH ok")

        ssh.run("mkdir -p ~/fleet-agent")
        ssh.upload(HERE / "agent" / "agent.py", "fleet-agent/agent.py")
        ssh.upload(HERE / "agent" / "handlers.py", "fleet-agent/handlers.py")

        agent_cfg = {
            "server": cfg["public_url"].rstrip("/"),
            "token": cfg["agent_token"],
            "node": node,
            "insecure_tls": bool(cfg.get("tls_cert")),
        }
        b64 = base64.b64encode(json.dumps(agent_cfg).encode()).decode()
        ssh.run(f"echo {b64} | base64 -d > ~/fleet-agent/agent_config.json")
        ssh.run("chmod 600 ~/fleet-agent/agent_config.json")

        ssh.start_agent()
        print(f"  agent started on {node}")

    print(f"\ndone. verify with: curl -sk {cfg['public_url']}/fleet/status "
          f"| python3 -m json.tool   (manager must be running)")


if __name__ == "__main__":
    main()
