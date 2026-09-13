#!/usr/bin/env python3
"""Rescue a wedged fleet node over its Cloud Shell API + SSH (terminal-only).

Why this exists: `.customize_environment` (the auto-rejoin hook) only runs when
a REAL session attaches to a Cloud Shell VM. The watchdog's API `:start` boots
the VM but never triggers the hook, so a node that was suspended mid-lease (or
rebuilt) never rejoins and the fleet can go dark. This tool reproduces what
attaching a session does, from the terminal:

  1. mint an access token from that node's refresh token (Testing-mode tokens
     die every 7 days -> rotate first with scripts/refresh_token.py if dead)
  2. GET users/me/environments/default (resolves the account + SSH endpoint;
     POST :start if the VM is not RUNNING, and wait)
  3. generate a throwaway ECDSA keypair in /tmp and register the public half
     via :addPublicKey
  4. SSH in — host/port/username are read from the same environment response
  5. launch ./bootstrap.sh detached (FLEET_NODE_ID=<node>, nohup) — exactly
     what the hook would have done; it starts the supervisor in a tmux session
  6. watch the bootstrap log until the supervisor reports ACTIVE / lease claim
  7. :removePublicKey and delete the throwaway key (on success)

If server/lease is still held by a dead node, the watchdog reap fix in
frontdoor/cf-worker.js must be deployed first (or clear activeNode in
Firestore by hand) — this tool does not touch the lease document.

Usage:
  .venv/bin/python scripts/rescue_node.py shell-a
  .venv/bin/python scripts/rescue_node.py shell-c --refresh .secrets/SHELL_C_REFRESH.txt
  .venv/bin/python scripts/rescue_node.py shell-a --plan-only   # read-only

Requires: paramiko + cryptography (this repo's .venv has them — on a fresh
box: python3 -m ensurepip --user, then pip install paramiko).
"""
import argparse
import json
import os
import stat
import sys
import time
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path

TOKEN_URL = "https://oauth2.googleapis.com/token"
CLOUDSHELL_API = "https://cloudshell.googleapis.com/v1"
NODES = ["shell-a", "shell-b", "shell-c", "shell-d"]

REPO = Path(__file__).resolve().parent.parent
DEFAULT_CLIENT = REPO / ".secrets" / "oauth-client.json"

# --- continued in PART2 ---


def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def die(msg, code=1):
    print(f"ERROR: {msg}", file=sys.stderr)
    sys.exit(code)


def _fresh_active_line(tail, boot_ref):
    """True only if a supervisor JSON log line NEWER than boot_ref reports ACTIVE.

    The log tail mixes lines from earlier sessions; matching any 'ACTIVE'
    substring false-positives on stale lines (seen live 2026-09-13: shell-b
    rescue matched 'still active fence=' from a 2.5h-old session and declared
    the node rejoined before the new supervisor had even started). Lines with
    unparseable timestamps are never trusted.
    """
    for line in (tail or "").splitlines():
        line = line.strip()
        if not line.startswith("{"):
            continue
        try:
            d = json.loads(line)
        except json.JSONDecodeError:
            continue
        msg = str(d.get("message", ""))
        if "ACTIVE fenceToken=" not in msg and "still active fence=" not in msg:
            continue
        raw = d.get("timestamp")
        try:
            ts = datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
        except (ValueError, TypeError):
            continue
        if ts >= boot_ref:
            return True
    return False


def load_client(path):
    p = Path(path).expanduser()
    if not p.exists():
        die(f"OAuth client file not found: {p} (expected {{'installed':{{...}}}} shape)")
    try:
        installed = json.loads(p.read_text())["installed"]
        return installed["client_id"], installed["client_secret"]
    except (KeyError, json.JSONDecodeError) as e:
        die(f"{p} is not a valid 'installed' OAuth client JSON: {e}")


def post_form(fields):
    req = urllib.request.Request(TOKEN_URL, data=urllib.parse.urlencode(fields).encode())
    try:
        with urllib.request.urlopen(req, timeout=25) as r:
            return 200, json.loads(r.read().decode())
    except urllib.error.HTTPError as e:
        try:
            return e.code, json.loads(e.read().decode())
        except Exception:
            return e.code, {}


def mint_access_token(client, refresh_file):
    """Mint an access token from the node's refresh token; returns access_token."""
    p = Path(refresh_file).expanduser()
    if not p.exists():
        die(f"refresh-token file not found: {p}\n"
            "  rotate first: .venv/bin/python scripts/refresh_token.py (then save the token)")
    token = p.read_text().strip()
    if not token:
        die(f"{p} is empty")
    status, resp = post_form(
        {
            "client_id": client[0],
            "client_secret": client[1],
            "refresh_token": token,
            "grant_type": "refresh_token",
        }
    )
    if status != 200 or not resp.get("access_token"):
        err = resp.get("error", "?")
        if err == "invalid_grant":
            die(f"refresh token in {p.name} is dead (invalid_grant: 7-day Testing expiry).\n"
                f"  verify with:  .venv/bin/python scripts/refresh_token.py --check {p}")
        die(f"token mint failed (HTTP {status}): {err}")
    return resp["access_token"]


def api(token, method, path, body=None, ok=(200,)):
    """One Cloud Shell REST call. Returns (status, parsed-json)."""
    url = path if path.startswith("http") else f"{CLOUDSHELL_API}/{path}"
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method, headers={
        "authorization": f"Bearer {token}",
        "content-type": "application/json",
    })
    try:
        with urllib.request.urlopen(req, timeout=25) as r:
            return r.status, json.loads(r.read().decode() or "{}")
    except urllib.error.HTTPError as e:
        detail = e.read().decode(errors="replace")[:300]
        if e.code in ok:
            return e.code, {}
        die(f"{method} {url} -> HTTP {e.code}: {detail}")


def wait_operation(token, op_name, what, timeout_s=180):
    """Poll a longrunning operation until done; die() on error."""
    for i in range(timeout_s // 3):
        st, op = api(token, "GET", op_name)
        if op.get("done"):
            if "error" in op:
                die(f"{what} failed: {json.dumps(op['error'])[:300]}")
            log(f"{what}: done")
            return op.get("response", {})
        if i % 5 == 0:
            log(f"{what}: waiting ...")
        time.sleep(3)
    die(f"{what} did not finish within {timeout_s}s")


def make_keypair(tmpdir="/tmp"):
    """Generate a throwaway ECDSA keypair; returns (pub_openssh, priv_path)."""
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.asymmetric import ec
    key = ec.generate_private_key(ec.SECP256R1())
    priv_path = Path(tmpdir) / f"fleet_rescue_{os.getpid()}"
    priv_path.write_bytes(key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.OpenSSH,  # the format paramiko parses
        serialization.NoEncryption(),
    ))
    os.chmod(priv_path, stat.S_IRUSR | stat.S_IWUSR)  # 0600
    pub = key.public_key().public_bytes(
        serialization.Encoding.OpenSSH, serialization.PublicFormat.OpenSSH
    ).decode()
    return pub, str(priv_path)

def main():
    ap = argparse.ArgumentParser(
        description="Rescue a wedged fleet node: boot VM, attach a session, rejoin the fleet.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="Run with just the node for the guided path. --plan-only changes nothing.",
    )
    ap.add_argument("node", choices=NODES, help="which node to rescue (shell-a..d)")
    ap.add_argument("--refresh", help="refresh-token file for that node (default .secrets/SHELL_X_REFRESH.txt)")
    ap.add_argument("--client", default=str(DEFAULT_CLIENT), help="path to oauth-client.json (installed)")
    ap.add_argument("--email", help="account email override (normally resolved via users/me)")
    ap.add_argument("--host", help="SSH host override (normally read from the environment)")
    ap.add_argument("--port", type=int, help="SSH port override")
    ap.add_argument("--user", help="SSH username override")
    ap.add_argument("--timeout", type=int, default=420, help="seconds to wait for VM start + supervisor rejoin")
    ap.add_argument("--keep-key", action="store_true", help="do NOT remove the throwaway SSH key on success")
    ap.add_argument("--plan-only", action="store_true", help="mint + inspect only; start no VM, add no key")
    ap.add_argument("--yes", action="store_true", help="skip the confirmation prompt")
    args = ap.parse_args()

    node = args.node
    refresh = args.refresh or str(REPO / ".secrets" / f"{node.replace('-', '_').upper()}_REFRESH.txt")
    client = load_client(args.client)

    log(f"rescuing {node}")
    access = mint_access_token(client, refresh)

    env_path = "users/me/environments/default"
    st, env = api(access, "GET", env_path)
    name = env.get("name", "")
    email = name.split("/environments/")[0].split("users/")[-1] if name else ""
    if not email or "/" in email:
        die("could not resolve the account from users/me/environments/default; pass --email")
    if args.email:
        email = args.email
    log(f"mint OK; account {email}")
    state = env.get("state", "?")
    log(f"environment state: {state}")

    plan = (
        f"\nPLAN for {node} ({email}):"
        + ("VM already RUNNING; " if state == "RUNNING" else "start VM; ")
        + "add throwaway key -> ssh -> ./bootstrap.sh -> wait for ACTIVE -> remove key"
        f"\n  account     : {email}"
        f"\n  environment : {env_path} (state={state})"
        f"\n  ssh         : {env.get('sshUsername', '?')}@{args.host or env.get('sshHost', '?')}"
        f":{args.port or env.get('sshPort', '?')}"
        f"\n  would do    : addPublicKey -> ssh -> ./bootstrap.sh (FLEET_NODE_ID={node})"
        " -> wait for ACTIVE -> removePublicKey"
    )

    if args.plan_only:
        print(plan)
        return

    if args.yes:
        proceed = True
    elif not sys.stdin.isatty():
        print(f"non-interactive run without --yes for {node}; nothing executed.")
        proceed = False
    else:
        print(plan)
        try:
            proceed = input("Proceed? [y/N]: ").strip().lower() == "y"
        except EOFError:
            proceed = False

    if not proceed:
        return

    if state != "RUNNING":
        log("starting VM (:start) ...")
        st, op = api(access, "POST", f"{env_path}:start", body={})
        wait_operation(access, op.get("name", ""), "start")
        for i in range(max(1, args.timeout // 5)):
            st, env = api(access, "GET", env_path)
            if env.get("state") == "RUNNING":
                log("VM is RUNNING")
                break
            time.sleep(5)
        else:
            die(f"VM did not reach RUNNING within {args.timeout}s (state={env.get('state')})")

    host = args.host or env.get("sshHost")
    port = args.port or env.get("sshPort")
    user = args.user or env.get("sshUsername")
    if not host or not port:
        die(f"environment response lacks SSH endpoint (sshHost={host} sshPort={port}); pass --host/--port")
    log(f"ssh endpoint: {user}@{host}:{port}")

    pub, priv_path = make_keypair()
    st, op = api(access, "POST", f"{env_path}:addPublicKey", body={"key": pub})
    wait_operation(access, op.get("name", ""), "addPublicKey")

    try:
        try:
            import paramiko
        except ImportError:
            die("paramiko not installed in this venv (no ssh binary on Cloud Shell).\n"
                "  .venv/bin/python -m pip install paramiko")
        pk = paramiko.ECDSAKey.from_private_key_file(priv_path)
        sshc = paramiko.SSHClient()
        sshc.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        log("connecting ...")
        sshc.connect(host, port=port, username=user, pkey=pk, timeout=25, banner_timeout=25)
        log("connected")

        def run(cmd):
            return sshc.exec_command(cmd, timeout=40)[1].read().decode(errors="replace")

        if not run("test -d ~/fleet && echo ok").strip():
            log("no ~/fleet clone — cloning ...")
            print(run("git clone https://github.com/AyushSuri8/fleet.git ~/fleet 2>&1 | tail -2").strip())
        if not run("ls ~/fleet/.secrets/shell-project-d2b93-*.json 2>/dev/null").strip():
            sshc.close()
            die("shared SA key missing on the node (~/fleet/.secrets/shell-project-d2b93-*.json).\n"
                "  copy it there first (DEPLOY.md §6), then re-run.")
        if not run("test -x ~/fleet/.venv/bin/python && echo ok").strip():
            log("node .venv missing — bootstrap will rebuild it (first run takes a few minutes)")

        log("launching ./bootstrap.sh detached (same as .customize_environment would) ...")
        print(run(
            "cd ~/fleet && FLEET_NODE_ID=%s nohup ./bootstrap.sh > /tmp/bootstrap-rescue-%s.log 2>&1 & echo PID=$!"
            % (node, node)
        ).strip())

        # Accept only supervisor log lines written AFTER the bootstrap launch
        # (30s skew allowance for node-vs-admin clock drift), never stale ones.
        boot_ref = datetime.now(timezone.utc) - timedelta(seconds=30)
        deadline = time.time() + args.timeout
        joined = False
        while time.time() < deadline:
            time.sleep(15)
            tail = run(f"tail -n 3 /tmp/bootstrap-rescue-{node}.log 2>/dev/null; "
                       f"tail -n 2 ~/fleet/logs/{node}.log 2>/dev/null")
            last = " | ".join(l.strip() for l in tail.strip().splitlines()[-2:])
            print(f"    {last[:160]}")
            if _fresh_active_line(tail, boot_ref):
                joined = True
                break
            if "ERROR:" in tail or "invalid" in tail.lower():
                log("bootstrap log shows an error; inspect /tmp/bootstrap-rescue-%s.log on the node" % node)
        sshc.close()
        if not joined:
            die("supervisor did not log ACTIVE in time — throwaway key NOT removed "
                "(remove it later via :removePublicKey or re-run once fixed)", 3)
        log(f"supervisor ACTIVE — {node} has rejoined the fleet")
    finally:
        if not args.keep_key:
            try:
                api(access, "POST", f"{env_path}:removePublicKey", body={"key": pub}, ok=(200, 404))
                log("throwaway SSH key removed from the environment")
            except SystemExit:
                log("WARN: could not remove the throwaway key — remove it via :removePublicKey")
            Path(priv_path).unlink(missing_ok=True)
        else:
            print(f"throwaway key kept: private={priv_path}")

    print(f"\nVerify: curl -s https://fleet-frontdoor.ayushsuri37.workers.dev/fleet/status "
          f"| python3 -m json.tool   (expect activeNode: {node}, fenceToken bumped)")


if __name__ == "__main__":
    main()