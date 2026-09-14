#!/usr/bin/env bash
# Oracle VM one-shot setup for fleet-manager (Ubuntu, 1 GB box).
# Run ON the Oracle VM, from /opt/fleet-manager, as the normal user (not root).
#
# Usage:
#   ./oracle-setup.sh                 # full run: config + tls + firewall + provision + service
#   ./oracle-setup.sh --skip-provision
#   ./oracle-setup.sh --skip-service
#   ./oracle-setup.sh --yes           # same, assume yes to prompts
#
# What it DOES (idempotent, safe to re-run):
#   1. system check (os, mem, disk, python3, ssh, openssl)
#   2. manager_config.json: auto-sets public_url from detected public IP
#      (unless already set) and auto-generates agent_token (unless set)
#   3. TLS self-signed cert in tls/ (skips if present)
#   4. firewall: ufw or iptables rule for 8443/tcp (+ persist hint)
#   5. secrets presence check — FAILS here with exact filenames if missing
#   6. python3 provision.py [targets...]  (skipped with --skip-provision)
#   7. installs fleet-manager.service + enable --now (skipped with --skip-service)
#   8. verifies: GET /fleet/status via curl -sk
#
# What it does NOT do (one manual click):
#   OCI Console -> VCN -> Security List -> Ingress 0.0.0.0/0 TCP 8443.
#   Without that, agents outside can never reach :8443.
set -euo pipefail

SKIP_PROVISION=0
SKIP_SERVICE=0
ASSUME_YES=0
TARGETS=()
for a in "$@"; do
  case "$a" in
    --skip-provision) SKIP_PROVISION=1 ;;
    --skip-service) SKIP_SERVICE=1 ;;
    --yes|-y) ASSUME_YES=1 ;;
    shell-*) TARGETS+=("$a") ;;
    -h|--help) sed -n '2,/^set /p' "$0"; exit 0 ;;
    *) echo "unknown arg: $a (try --help)" >&2; exit 1 ;;
  esac
done

cd /opt/fleet-manager 2>/dev/null || { echo "ERROR: run from /opt/fleet-manager on the Oracle VM" >&2; exit 1; }

say() { printf '==> %s\n' "$*"; }
die() { printf 'ERROR: %s\n' "$*" >&2; exit 1; }

say "1/8 system check"
cat /etc/os-release | head -2
free -h | head -2; df -h / | tail -1
python3 --version
command -v ssh openssl curl python3 >/dev/null || die "missing ssh/openssl/curl/python3"
for f in manager.py store.py frontdoor.py cloudshell.py sshctl.py provision.py agent/agent.py agent/handlers.py manager_config.json; do
  [[ -f "$f" ]] || die "missing $f — copy fleet-manager/ contents to /opt/fleet-manager first"
done
python3 -m py_compile store.py manager.py frontdoor.py cloudshell.py sshctl.py provision.py agent/agent.py agent/handlers.py
echo "    files OK"

say "2/8 config (public_url + agent_token)"
PUBIP=$(curl -s --max-time 10 ifconfig.me || true)
PUBIP=$(printf '%s' "$PUBIP" | tr -d '[:space:]')
[[ -n "$PUBIP" ]] || PUBIP=$(hostname -I 2>/dev/null | awk '{print $1}')
[[ -n "$PUBIP" ]] || die "could not detect public IP; set public_url manually in manager_config.json"
python3 - "$PUBIP" <<'PY'
import json, secrets, sys
pubip = sys.argv[1]
p = "manager_config.json"
c = json.load(open(p))
changed = []
if "YOUR.ORACLE" in c.get("public_url", ""):
    c["public_url"] = f"https://{pubip}:8443"
    changed.append(f"public_url={c['public_url']}")
if not c.get("agent_token") or "PASTE_OUTPUT" in c["agent_token"] or len(c["agent_token"]) < 32:
    c["agent_token"] = secrets.token_hex(32)
    changed.append("agent_token=<generated>")
json.dump(c, open(p, "w"), indent=2)
print("    " + (", ".join(changed) if changed else "already set: " + c["public_url"]))
PY

say "3/8 TLS self-signed cert"
if [[ -f tls/fleet.crt && -f tls/fleet.key ]]; then
  echo "    tls/fleet.crt exists, skipping"
else
  mkdir -p tls
  openssl req -x509 -newkey rsa:2048 -nodes -days 3650 \
    -keyout tls/fleet.key -out tls/fleet.crt -subj "/CN=fleet" 2>/dev/null
  chmod 600 tls/fleet.key
  echo "    created tls/fleet.crt + tls/fleet.key"
fi

say "4/8 firewall (host layer)"
if command -v ufw >/dev/null && sudo -n true 2>/dev/null && sudo ufw status 2>/dev/null | grep -q "Status: active"; then
  sudo ufw allow 8443/tcp || true
  echo "    ufw: allowed 8443/tcp"
else
  sudo iptables -C INPUT -p tcp --dport 8443 -j ACCEPT 2>/dev/null \
    || sudo iptables -I INPUT -p tcp --dport 8443 -j ACCEPT
  echo "    iptables: 8443/tcp ensured"
  if command -v netfilter-persistent >/dev/null; then
    sudo netfilter-persistent save || true
  else
    echo "    NOTE: 'sudo apt-get install -y iptables-persistent' to survive reboot"
  fi
fi
echo "    STILL REQUIRED (manual): OCI Console -> VCN -> Security List -> Ingress 0.0.0.0/0 TCP 8443"

say "5/8 secrets check"
MISSING=0
for f in secrets/oauth-client.json secrets/SHELL_A_REFRESH.txt secrets/SHELL_B_REFRESH.txt secrets/SHELL_C_REFRESH.txt secrets/SHELL_D_REFRESH.txt; do
  if [[ -f "$f" ]]; then echo "    OK $f"; else echo "    MISSING $f"; MISSING=1; fi
done
chmod 600 secrets/* 2>/dev/null || true
if [[ "$MISSING" -eq 1 ]]; then
  die "put the 5 files above into secrets/ (see ORACLE.md §2), then re-run ./oracle-setup.sh"
fi
python3 - <<'PY'
import json
c = json.load(open("manager_config.json"))
assert c["public_url"].startswith("https://") and "YOUR.ORACLE" not in c["public_url"], "public_url not set"
assert len(c.get("agent_token", "")) >= 32, "agent_token not set"
print("    config OK:", c["public_url"])
PY

if [[ "$SKIP_PROVISION" -eq 0 ]]; then
  say "6/8 provision shells (boots VMs, registers SSH key, uploads agent)"
  python3 provision.py "${TARGETS[@]}"
else
  say "6/8 provision SKIPPED (--skip-provision)"
fi

if [[ "$SKIP_SERVICE" -eq 0 ]]; then
  say "7/8 install + start systemd service"
  sudo cp fleet-manager.service /etc/systemd/system/
  sudo systemctl daemon-reload
  sudo systemctl enable --now fleet-manager
  sleep 3
  sudo systemctl is-active --quiet fleet-manager && echo "    fleet-manager active" \
    || { sudo journalctl -u fleet-manager -n 50 --no-pager; die "service failed to start (log above)"; }
else
  say "7/8 service install SKIPPED (--skip-service)"
fi

say "8/8 verify"
PUBURL=$(python3 -c "import json; print(json.load(open('manager_config.json'))['public_url'])")
if curl -sk --max-time 15 "$PUBURL/fleet/status" | python3 -m json.tool; then
  echo "    verify OK"
else
  echo "    verify FAILED — check: journalctl -u fleet-manager -n 50; OCI security list 8443; secrets"
  exit 1
fi
echo "DONE. follow logs with: journalctl -u fleet-manager -f"
